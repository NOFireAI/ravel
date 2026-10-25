//! Column-statistics load for the query-time metadata-only path (ADR-1413).
//! Like [`crate::covering_postings::load_covering_postings`] it fetches HEAD
//! and follows the statistics refs it carries, but it fetches NO snapshot part
//! body: it needs only each part's blake3 (already carried in
//! `SnapshotHead.parts`) to bind an object to this HEAD's part set. Every GET
//! runs through the caller's accounted, semaphore-bounded funnel (issue #850).
//!
//! `.cstat` is split per snapshot part (ADR-1413): a covered part with a
//! `column_stats` ref (field 7, v3) is read as its own small object, keyed by
//! content hash. A part with no such ref, or whose v3 object fails to load, is
//! simply left uncovered and the query scans it (ADR-1413 decision 6: the
//! whole-tenant v1/v2 objects and the fallback ladder that once read them are
//! retired; the fold no longer publishes them at any size).
//!
//! # One keying scheme
//!
//! Records key by content hash (`SegmentRef::content_hash` /
//! `SnapshotEntry::content_hash`, ADR-0942), carried in
//! `ColumnStatsSegment.writer_id` at 32 bytes. Each part entry keeps a
//! legacy map keyed by the five-field `EntryIdentity` tuple
//! (`fold::entry_identity`) for the retired v1 keying scheme; nothing
//! populates it from a fetched object anymore, so it is empty there, but
//! [`LoadedColumnStats::from_maps`] still accepts one and
//! [`LoadedColumnStats::stat_for`] still consults it.
//!
//! # Summaries and dictionaries
//!
//! A `ColumnStat`'s per-value dictionary is most of its weight, and only the
//! q02 and q08 shapes read one. A load therefore takes a
//! [`ColumnStatsRequest`] naming the columns whose dictionaries the statement
//! needs, and every other column's dictionary is cleared from what the load
//! holds (issue #3244). A reader asks
//! [`LoadedColumnStats::dictionary_loaded`] before it reads a dictionary: a
//! cleared one is empty, and an empty dictionary is a wrong answer, not an
//! absent one.
//!
//! # Degrade-to-`None`, loud exceptions
//!
//! Column statistics are an OPTIONAL metadata artifact, so every failure short
//! of a store outage, an unparseable HEAD or an isolation breach degrades: no
//! HEAD yet, no ref at all, a `NotFound` GET, a blake3 mismatch, a decode
//! error, or a
//! part-binding mismatch. A DECODE failure on an object a covered part
//! actually references means the fold wrote an object the reader cannot
//! open, so the fetch path surfaces it as [`FetchOutcome::DecodeRefused`]
//! (rather than folding it into a bare miss) for the caller to log once and
//! count (issue #1400). A read CPU gate job that failed takes the same
//! variant but says nothing about the bytes; the caller accounts for it
//! separately (`Catalog::note_column_stats_decode_failure`). A GET failure on either the HEAD or a resolved stats
//! object degrades like a miss when [`StoreError::is_retryable`] reports it
//! throttling, a timeout, or otherwise transient: the artifact is optional
//! and the query is correct without it, so a blip is not worth failing a
//! whole query over. A GET failure for any other reason (issue #1976: most
//! commonly `AccessDenied` from a missing IAM read grant) is a store outage
//! rather than a blip or an absence and surfaces as
//! [`LoadColumnStatsError::Store`]. And a genuinely unparseable HEAD (a real
//! catalog defect, not an optional artifact) or a column-stats object
//! declaring a foreign `tenant_hash` (an ADR-0050 §2 isolation breach) are
//! hard errors too: neither is absorbed into a silent degrade. The query
//! still scans on every degrade; only visibility changes.
//! `decode_column_stats` does not itself check part-binding against a
//! caller-supplied part list (unlike `decode_postings`); this loader performs
//! that check itself, exactly as
//! [`crate::column_stats_build::decode_previous_column_stats`] does on the
//! fold side.

use prost::Message;
use ravel_cpu_gate::ReadSite;
use ravel_object_store::StoreError;
use ravel_proto::catalog::v1::{ColumnStatsSegment, SnapshotPartRef};
use ravel_types::{Signal, TenantHash};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use crate::EntryIdentity;
use crate::charged::DecodeReserver;
use crate::provisioning::AccountedRecordGet;
use crate::read_gate::run_snapshot_decode;
#[cfg(test)]
use crate::snapshot_format::decode_column_stats;
use crate::snapshot_format::{
    ColumnStatsLimits, SegmentMeter, SnapshotFormatError, decode_column_stats_header,
    decode_column_stats_streamed, decode_head,
};

/// The per-value dictionaries a statement needs from the column statistics it
/// loads, by declared column name (`ColumnStat.name`). Empty (the
/// [`Default`]) is a summary-only load: counts, extrema and sums, no
/// dictionary at all. Every column it does not name has its dictionary
/// cleared from the load, and [`LoadedColumnStats::dictionary_loaded`] reports
/// it absent.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ColumnStatsRequest {
    dictionaries: BTreeSet<String>,
}

impl ColumnStatsRequest {
    /// A request for no dictionary, the shape SUM, AVG and an unfiltered
    /// count need.
    pub fn summary_only() -> Self {
        Self::default()
    }

    /// This request plus `column`'s dictionary.
    pub fn with_dictionary(mut self, column: impl Into<String>) -> Self {
        self.add_dictionary(column);
        self
    }

    /// Add `column`'s dictionary to this request.
    pub fn add_dictionary(&mut self, column: impl Into<String>) {
        self.dictionaries.insert(column.into());
    }

    /// Add every dictionary `other` names to this request.
    pub fn extend(&mut self, other: &ColumnStatsRequest) {
        self.dictionaries.extend(other.dictionaries.iter().cloned());
    }

    /// Whether this request names `column`'s dictionary.
    pub fn wants_dictionary(&self, column: &str) -> bool {
        self.dictionaries.contains(column)
    }

    /// The columns whose dictionaries this request names, in name order.
    pub fn dictionary_columns(&self) -> impl Iterator<Item = &str> {
        self.dictionaries.iter().map(String::as_str)
    }

    /// Whether this request names no dictionary at all.
    pub fn is_summary_only(&self) -> bool {
        self.dictionaries.is_empty()
    }
}

/// Which columns' dictionaries a [`PartColumnStats`] holds. `All` is a
/// hand-built carrier ([`LoadedColumnStats::from_maps`]), which strips
/// nothing; a fetched part always holds an explicit set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum HeldDictionaries {
    All,
    Columns(BTreeSet<String>),
}

impl HeldDictionaries {
    fn holds(&self, column: &str) -> bool {
        match self {
            HeldDictionaries::All => true,
            HeldDictionaries::Columns(columns) => columns.contains(column),
        }
    }

    fn covers(&self, request: &ColumnStatsRequest) -> bool {
        request
            .dictionary_columns()
            .all(|column| self.holds(column))
    }
}

/// One covered part's decoded column statistics, the unit the catalog's
/// column-statistics cache holds and evicts (ADR-1413 decision 5, issue
/// #3244): every segment the part's `.cstat` object carries, with the
/// dictionary of every column outside [`Self::dictionaries`] cleared.
/// Immutable once built; a load that needs a dictionary an entry lacks
/// builds a new entry from a fresh decode rather than editing a shared one.
#[derive(Debug)]
pub(crate) struct PartColumnStats {
    /// Retired v1 keying scheme
    /// (`(ingest_hour_bucket, shard, writer_id, writer_epoch, writer_seq)`,
    /// `fold::entry_identity`). Empty for a fetched part (ADR-1413 decision
    /// 6); only [`LoadedColumnStats::from_maps`] fills it.
    segments: HashMap<EntryIdentity, ColumnStatsSegment>,
    /// v3 per-part records, keyed by content hash (`SegmentRef::content_hash`,
    /// ADR-0942).
    by_content_hash: HashMap<[u8; 32], ColumnStatsSegment>,
    dictionaries: HeldDictionaries,
    heap_bytes: u64,
}

impl PartColumnStats {
    /// Build an entry from one decoded object, keeping the dictionaries
    /// `request` names plus those `previous` (the same object's earlier entry)
    /// already held, so a new statement's request never drops a dictionary an
    /// earlier one paid to fetch.
    pub(crate) fn from_decoded(
        decoded: DecodedStats,
        request: &ColumnStatsRequest,
        previous: Option<&PartColumnStats>,
    ) -> Self {
        let dictionaries = Self::dictionaries_to_keep(request, previous);
        Self::new(decoded.segments, decoded.by_content_hash, dictionaries)
    }

    /// The dictionaries an entry built for `request` holds: those `request`
    /// names plus those `previous` already held. [`fetch_stats_object`] keeps
    /// exactly these while decoding, and [`Self::from_decoded`] builds the
    /// entry with them.
    pub(crate) fn dictionaries_to_keep(
        request: &ColumnStatsRequest,
        previous: Option<&PartColumnStats>,
    ) -> HeldDictionaries {
        match previous.map(|entry| &entry.dictionaries) {
            Some(HeldDictionaries::All) => HeldDictionaries::All,
            Some(HeldDictionaries::Columns(held)) => {
                let mut columns = held.clone();
                columns.extend(request.dictionaries.iter().cloned());
                HeldDictionaries::Columns(columns)
            }
            None => HeldDictionaries::Columns(request.dictionaries.clone()),
        }
    }

    fn new(
        mut segments: HashMap<EntryIdentity, ColumnStatsSegment>,
        mut by_content_hash: HashMap<[u8; 32], ColumnStatsSegment>,
        dictionaries: HeldDictionaries,
    ) -> Self {
        if let HeldDictionaries::Columns(columns) = &dictionaries {
            for segment in segments.values_mut().chain(by_content_hash.values_mut()) {
                strip_dictionaries(segment, columns);
            }
        }
        let heap_bytes = segments
            .values()
            .chain(by_content_hash.values())
            .map(|segment| segment.encoded_len() as u64)
            .sum();
        PartColumnStats {
            segments,
            by_content_hash,
            dictionaries,
            heap_bytes,
        }
    }

    /// The exact byte weight this entry charges against the column-statistics
    /// cache budget: the summed encoded protobuf length of every segment it
    /// holds, as held (cleared dictionaries cost nothing, held ones count in
    /// full). Computed once at construction.
    pub(crate) fn heap_bytes(&self) -> u64 {
        self.heap_bytes
    }

    /// Whether this entry holds every dictionary `request` names.
    pub(crate) fn covers(&self, request: &ColumnStatsRequest) -> bool {
        self.dictionaries.covers(request)
    }

    /// Whether this entry holds `column`'s dictionary.
    pub(crate) fn holds_dictionary(&self, column: &str) -> bool {
        self.dictionaries.holds(column)
    }

    fn is_empty(&self) -> bool {
        self.segments.is_empty() && self.by_content_hash.is_empty()
    }
}

/// Clear the dictionary of every column of `segment` not named in `keep`.
/// Every other field, `dictionary_present` included, is left as decoded:
/// `dictionary_present` describes the object, and whether the load holds the
/// dictionary is [`LoadedColumnStats::dictionary_loaded`]'s answer.
pub(crate) fn strip_dictionaries(segment: &mut ColumnStatsSegment, keep: &BTreeSet<String>) {
    for column in &mut segment.columns {
        if !keep.contains(&column.name) {
            column.dictionary = Vec::new();
        }
    }
}

/// The column-statistics inputs a query-time metadata-only plan needs: exact
/// per-segment statistics, assembled from every covered part a query's
/// window touches. Each part is a shared cache entry (issue #3244), so
/// building this costs no copy of any part's records. A live segment no part
/// carries has no exact statistics and the query scans it.
///
/// Every column's summary (counts, extrema, sum) is present; a column's
/// dictionary is present only where [`Self::dictionary_loaded`] says so.
#[derive(Clone, Debug, Default)]
pub struct LoadedColumnStats {
    parts: Vec<Arc<PartColumnStats>>,
}

/// The entry for column `name` in `segment`, and only when the segment carries
/// EXACTLY one such entry. `None` covers both "no entry" (the ordinary
/// not-covered case: the fold never built this column, so the reader declines
/// and the scan answers) and "more than one entry", which is a malformed
/// record: `decode_column_stats` refuses an object carrying a duplicate column
/// name outright, so a record reaching a reader with two entries under one name
/// arrived through some other carrier and no rule could pick the right one.
///
/// Duplicates cannot be resolved silently by position, because the two shapes
/// of "silently" disagree with each other: an `iter().find()` lookup keeps the
/// FIRST entry while collecting the same list into a by-name map keeps the
/// LAST, so one malformed record answers one query two ways depending on which
/// reader ran. Fail closed instead; the column is simply uncovered.
pub fn unique_column_stat<'a>(
    segment: &'a ColumnStatsSegment,
    name: &str,
) -> Option<&'a ravel_proto::catalog::v1::ColumnStat> {
    let mut found = None;
    for column in &segment.columns {
        if column.name == name {
            if found.is_some() {
                return None;
            }
            found = Some(column);
        }
    }
    found
}

impl LoadedColumnStats {
    /// A carrier built from records already in hand rather than from a load:
    /// content-hash-keyed records in `by_content_hash`, retired v1
    /// tuple-keyed records in `segments`. It holds every dictionary its
    /// records carry, so [`Self::dictionary_loaded`] is true for every column.
    pub fn from_maps(
        segments: HashMap<EntryIdentity, ColumnStatsSegment>,
        by_content_hash: HashMap<[u8; 32], ColumnStatsSegment>,
    ) -> Self {
        LoadedColumnStats {
            parts: vec![Arc::new(PartColumnStats::new(
                segments,
                by_content_hash,
                HeldDictionaries::All,
            ))],
        }
    }

    /// Assemble a load from its covered parts' entries, in `SnapshotHead.parts`
    /// order. Parts carrying no segment are dropped.
    pub(crate) fn from_parts(parts: Vec<Arc<PartColumnStats>>) -> Self {
        LoadedColumnStats {
            parts: parts.into_iter().filter(|part| !part.is_empty()).collect(),
        }
    }

    /// Whether no part carries any segment: nothing to join against.
    pub fn is_empty(&self) -> bool {
        self.parts.iter().all(|part| part.is_empty())
    }

    /// The exact bytes the part entries this load shares hold: the sum of
    /// each part's encoded segment bytes, as held (issue #905, #3244).
    ///
    /// It counts the decoded `.cstat` payload itself, the term that scales with
    /// a tenant's declared typed columns times its live segments and with the
    /// number of tenants a process serves, and the term the cache budget
    /// exists to bound. It deliberately does NOT count `HashMap` slot overhead
    /// or the `Arc` control blocks, so the figure is an exact, reproducible
    /// function of the held data rather than an estimate; it is a lower bound
    /// on the process RSS the load costs. The bytes counted are
    /// decoded-payload bytes with unrequested dictionaries cleared, not the
    /// object's on-wire compressed size.
    pub fn heap_bytes(&self) -> u64 {
        self.parts.iter().map(|part| part.heap_bytes()).sum()
    }

    /// Whether `column`'s dictionary was loaded for every part this load
    /// holds. When it is not, every record's dictionary for `column` is empty
    /// whatever `dictionary_present` says, and a reader must decline rather
    /// than count from it.
    pub fn dictionary_loaded(&self, column: &str) -> bool {
        self.parts.iter().all(|part| part.holds_dictionary(column))
    }

    /// The exact statistics for a live segment: the content-hash keyed record
    /// (a per-part v3 object, ADR-0942), falling back to the retired
    /// identity-keyed v1 map. A later part wins over an earlier one for the
    /// same key, as when one map was extended part by part. Returns `None`
    /// when no part carries an entry: the segment has no exact statistics and
    /// the caller must scan it.
    pub fn stat_for(
        &self,
        content_hash: &[u8; 32],
        identity: &EntryIdentity,
    ) -> Option<&ColumnStatsSegment> {
        self.parts
            .iter()
            .rev()
            .find_map(|part| part.by_content_hash.get(content_hash))
            .or_else(|| {
                self.parts
                    .iter()
                    .rev()
                    .find_map(|part| part.segments.get(identity))
            })
    }

    /// Every content-hash keyed record this load holds, with its key.
    pub fn content_hash_segments(&self) -> impl Iterator<Item = (&[u8; 32], &ColumnStatsSegment)> {
        self.parts
            .iter()
            .flat_map(|part| part.by_content_hash.iter())
    }

    /// The content-hash keyed records as one map, a later part winning a key
    /// the way [`Self::stat_for`] does.
    #[cfg(test)]
    pub(crate) fn by_content_hash(&self) -> HashMap<[u8; 32], &ColumnStatsSegment> {
        self.content_hash_segments()
            .map(|(hash, segment)| (*hash, segment))
            .collect()
    }

    /// The part entries this load shares, in covered-part order.
    #[cfg(test)]
    pub(crate) fn parts(&self) -> &[Arc<PartColumnStats>] {
        &self.parts
    }
}

/// A resolved reference to one per-part column-statistics object: its key,
/// content hash, and the part it must be bound against
/// (`SnapshotPartRef.column_stats`, field 7, always v3).
#[derive(Clone, Debug)]
pub(crate) struct ResolvedStatsRef {
    /// Object key of the `.cstat` object.
    pub key: String,
    /// Content hash of the referenced object, the fetch's blake3 gate and the
    /// cache's primary key.
    pub blake3: [u8; 32],
    /// The part set this object is bound to: exactly the one covered part. A
    /// fetch is valid only against this exact part set (ADR-0942's binding
    /// check).
    pub expected_part_blake3: Vec<[u8; 32]>,
    /// The envelope `format_version` a part's field 7 always promises: 3.
    /// `decode_column_stats` only checks the envelope byte against the
    /// object's OWN header, never against which slot the caller read it from
    /// -- so without this field, a stale object of some other version left
    /// under a v3 ref could decode clean and be served as this part's
    /// statistics.
    pub expected_version: u32,
}

/// HEAD's part list, resolved from one HEAD GET. The per-part v3 refs live on
/// each `SnapshotPartRef` itself (`parts[i].column_stats`) and are read
/// directly by the caller via [`resolve_part_stats_ref`] for whichever parts
/// its query window covers.
pub(crate) struct ResolvedStatsHead {
    pub parts: Vec<SnapshotPartRef>,
}

/// Outcome of [`fetch_stats_object`]: an object the reader decoded, or one of
/// the two degrade-to-miss kinds the caller must tell apart. A store read
/// that fails with `NotFound` or retryably, and a stale binding, are
/// legitimately "no statistics" ([`Self::Absent`]); a DECODE failure on the
/// object HEAD (or a covered part) points at is not ([`Self::DecodeRefused`])
/// and the caller logs and counts it (issue #1400), apart from a failed gate
/// job, which `Catalog::note_column_stats_decode_failure` keeps out of the
/// refusal accounting.
pub(crate) enum FetchOutcome {
    /// Fetched, hash-verified, tenant-checked, part-bound, and decoded, with
    /// the reservations charging the kept segments (ADR-1702 decision 6), one
    /// per segment.
    Loaded(DecodedStats, Vec<ravel_memory::Reservation>),
    /// No usable object, silently: the store GET returned `NotFound` or a
    /// retryable failure (`StoreError::is_retryable`: throttling, a timeout,
    /// a transient blip), the content hash did not match, a part hash was
    /// malformed, or the part binding was stale. Every one of these is an
    /// ordinary "no statistics" and stays quiet. A non-retryable GET failure
    /// is not folded in here; it surfaces as `Err(LoadColumnStatsError::Store)`
    /// (issue #1976).
    Absent,
    /// HEAD (or a covered part) references an object the reader refused to
    /// DECODE: the fold wrote it and the ref points at it, but the bytes will
    /// not open (an oversized declared body, corruption, a crc or header
    /// failure). Also carries a read CPU gate job that failed
    /// ([`SnapshotFormatError::DecodeJob`]), which did not judge the bytes.
    /// Never normal. Carries the decode error so the caller can name
    /// the cause and, for [`SnapshotFormatError::ColumnStatsDecompressedTooLarge`],
    /// the declared and cap bytes.
    DecodeRefused(SnapshotFormatError),
}

/// One fetched-and-decoded object's records, split by keying scheme, holding
/// only the dictionaries [`fetch_stats_object`] was asked to keep.
/// [`PartColumnStats::from_decoded`] turns it into the cached entry.
#[derive(Default)]
pub(crate) struct DecodedStats {
    pub segments: HashMap<EntryIdentity, ColumnStatsSegment>,
    pub by_content_hash: HashMap<[u8; 32], ColumnStatsSegment>,
    /// The decoded object's declared decompressed body length, the figure
    /// [`crate::Catalog::column_stats_decoded_bytes`] sums.
    pub body_uncompressed_len: u64,
}

/// A genuinely unparseable HEAD, or an isolation breach, encountered while
/// loading column statistics: the only conditions this path
/// surfaces as an error rather than degrading to `Ok(None)`.
#[derive(Debug, thiserror::Error)]
pub enum LoadColumnStatsError {
    #[error("HEAD at {key} is corrupt: {source}")]
    HeadCorrupt {
        key: String,
        #[source]
        source: SnapshotFormatError,
    },
    /// The column-stats object declares a `tenant_hash` naming a different
    /// tenant (ADR-0050 §2 isolation breach): a hard error, never a silent
    /// degrade.
    #[error(
        "column-stats object {key} declares tenant_hash {actual}, expected {expected} \
         (ADR-0050 §2 isolation breach)"
    )]
    TenantHashMismatch {
        key: String,
        expected: String,
        actual: String,
    },
    /// A GET (the HEAD, or a resolved per-part stats object) failed for a
    /// reason other than `NotFound` or a retryable failure
    /// (`StoreError::is_retryable`) (issue #1976): most commonly a missing
    /// IAM read grant, surfaced as `StoreError::AccessDenied`. This is a
    /// store outage, not "no statistics yet", so it is a hard error the
    /// caller can count and retry, never a silent `Ok(None)`/`Absent` degrade
    /// indistinguishable from an ordinary miss.
    #[error("store read at {key} failed: {source}")]
    Store {
        key: String,
        #[source]
        source: StoreError,
    },
    /// The process memory budget refused the reservation for a stats object's
    /// declared uncompressed body (ADR-1702 decision 6). Carries only the
    /// budget's figures and the class that refused.
    #[error("column-stats decode refused: {0}")]
    MemoryExhausted(#[from] ravel_memory::MemoryExhausted),
}

/// HEAD object key (docs/catalog-and-mvcc.md key layout, frozen format).
/// Duplicated the same way [`crate::covering_postings`] and
/// [`crate::seal_divergence`] duplicate it.
fn head_key(tenant: &TenantHash, signal: Signal) -> String {
    format!("t/{}/catalog/{}/HEAD", tenant.to_hex(), signal.key_prefix())
}

/// Read the current folded snapshot HEAD for `(tenant, signal)` and return its
/// part list, or `Ok(None)` when no HEAD exists yet. The first GET of the
/// load: it never fetches a statistics object itself, so a caller can consult
/// a reuse cache before paying for any per-part fetch.
///
/// The HEAD GET is issued through `getter`, so it is credited to the caller's
/// [`QueryAccounting`](ravel_types::accounting::QueryAccounting) and bounded
/// by the catalog request semaphore, the same funnel every other query read
/// path uses (issue #850). It charges to `AccountedOp::Get`.
pub(crate) async fn resolve_stats_head(
    getter: &impl AccountedRecordGet,
    tenant: &TenantHash,
    signal: Signal,
) -> Result<Option<ResolvedStatsHead>, LoadColumnStatsError> {
    let key = head_key(tenant, signal);

    // `NotFound` is the ordinary "nothing folded yet" case. A retryable GET
    // failure (throttling, a timeout, a transient blip) degrades the same
    // way: the artifact is optional and the query is correct without it, so
    // a single blip should not fail the whole query. Any other GET failure
    // (issue #1976: most commonly AccessDenied on a missing IAM read grant)
    // is a store outage, not an absence, and must surface rather than read
    // as "no statistics yet".
    let head_bytes = match getter.accounted_get_full(&key).await {
        Ok(got) => got.data,
        Err(StoreError::NotFound) => return Ok(None),
        Err(source) if source.is_retryable() => {
            tracing::debug!(
                key = %key,
                error = %source,
                "column-stats HEAD GET failed retryably, degrading to no statistics"
            );
            return Ok(None);
        }
        Err(source) => {
            return Err(LoadColumnStatsError::Store {
                key: key.clone(),
                source,
            });
        }
    };
    let head = decode_head(&head_bytes).map_err(|source| LoadColumnStatsError::HeadCorrupt {
        key: key.clone(),
        source,
    })?;

    Ok(Some(ResolvedStatsHead { parts: head.parts }))
}

/// The per-part v3 ref carried on `part.column_stats` (field 7), or `None`
/// when the part has no such ref or its fields are malformed. Pure: no GET,
/// no accounting. `expected_part_blake3` binds to exactly this one part's own
/// blake3, matching how the fold writes it
/// (`SnapshotColumnStatsPartRef.part_blake3 = vec![part_hash]`).
pub(crate) fn resolve_part_stats_ref(part: &SnapshotPartRef) -> Option<ResolvedStatsRef> {
    let stats_ref = part.column_stats.as_ref()?;
    let blake3 = <[u8; 32]>::try_from(stats_ref.blake3.as_slice()).ok()?;
    let part_blake3 = <[u8; 32]>::try_from(part.blake3.as_slice()).ok()?;
    Some(ResolvedStatsRef {
        key: stats_ref.key.clone(),
        blake3,
        expected_part_blake3: vec![part_blake3],
        expected_version: 3,
    })
}

/// GET, blake3-verify, tenant-check, part-bind, and decode the object named by
/// `resolved` (always a per-part v3 object). Every degrade-to-miss case from
/// the [module docs](self) is enforced here against `resolved`'s content hash
/// and part binding.
///
/// The decode streams one segment at a time and keeps only the dictionaries
/// `keep` holds, so the memory it reserves peaks at the largest segment's
/// encoded length plus the bytes kept, never the declared body.
///
/// A record's `writer_id` selects which map it lands in: 16 bytes is the
/// retired v1 `EntryIdentity` tuple (never produced anymore), 32 bytes is a
/// v3 content hash (ADR-0942's overloaded slot). Any other length is a
/// malformed record and is dropped.
pub(crate) async fn fetch_stats_object(
    getter: &impl AccountedRecordGet,
    tenant: &TenantHash,
    resolved: &ResolvedStatsRef,
    keep: &HeldDictionaries,
    reserver: &impl DecodeReserver,
    gate: Option<&ravel_cpu_gate::ReadGate>,
) -> Result<FetchOutcome, LoadColumnStatsError> {
    let data = match getter.accounted_get_full(&resolved.key).await {
        Ok(got) => got.data,
        // Store read: the object may simply not exist for this HEAD. Silent.
        Err(StoreError::NotFound) => return Ok(FetchOutcome::Absent),
        // A retryable failure (throttling, a timeout, a transient blip)
        // degrades the same way as NotFound: the artifact is optional and
        // the query is correct without it.
        Err(source) if source.is_retryable() => {
            tracing::debug!(
                key = %resolved.key,
                error = %source,
                "column-stats object GET failed retryably, degrading to no statistics"
            );
            return Ok(FetchOutcome::Absent);
        }
        // Any other GET failure (issue #1976: most commonly AccessDenied on a
        // missing IAM read grant) is a store outage, not an absence, and must
        // surface rather than be read as "not covered".
        Err(source) => {
            return Err(LoadColumnStatsError::Store {
                key: resolved.key.clone(),
                source,
            });
        }
    };
    let digest = blake3::hash(&data);
    if *digest.as_bytes() != resolved.blake3 {
        return Ok(FetchOutcome::Absent);
    }

    let limits = ColumnStatsLimits::default();
    // Every header check runs on the peeked header, before the body is charged
    // or decoded, so a foreign-tenant or stale-binding object is reported as
    // what it is rather than as a budget refusal. A header that does not parse
    // fails `decode_column_stats` the same way, so it takes the same refusal
    // arm.
    let header = match decode_column_stats_header(&data) {
        Ok(peek) => peek.header,
        Err(err) => return Ok(FetchOutcome::DecodeRefused(err)),
    };

    if header.tenant_hash != tenant.0.to_vec() {
        return Err(LoadColumnStatsError::TenantHashMismatch {
            key: resolved.key.clone(),
            expected: tenant.to_hex(),
            actual: hex::encode(&header.tenant_hash),
        });
    }

    // A part's field 7 promises a v3 envelope; `decode_column_stats` only
    // checks the envelope byte against the object's OWN header, never against
    // which slot named it. A mismatch here means the object at `resolved.key`
    // is not v3 (a stale object left behind by a downgrade, or a future writer
    // bug), so it must not be accepted: degrade like any other stale binding.
    if header.format_version != resolved.expected_version {
        return Ok(FetchOutcome::Absent);
    }

    let actual_part_blake3: Result<Vec<[u8; 32]>, _> = header
        .part_blake3
        .iter()
        .map(|h| <[u8; 32]>::try_from(h.as_slice()))
        .collect();
    let Ok(actual_part_blake3) = actual_part_blake3 else {
        return Ok(FetchOutcome::Absent);
    };
    if actual_part_blake3 != resolved.expected_part_blake3 {
        return Ok(FetchOutcome::Absent);
    }

    // ADR-1702 decision 6: charge decoded bytes before decoding them. Through
    // `reserver` rather than against the budget directly, so a refusal takes
    // the decoded-output caches' evict-then-retry pass (issue #2107): those
    // caches hold parts and postings together with their reservations, so
    // under a finite budget they can hold the whole of it and a bare-budget
    // reservation here fails with memory nothing else would have released.
    //
    // The decode streams (issue #3244), so the charge follows it: each
    // segment's encoded length before it is read, shrunk to its kept bytes
    // once its unkept dictionaries are cleared. A body declared over the
    // ceiling is charged 0 here, as `decoded_charge` charges it, and the
    // decoder then refuses it before reading any segment.
    let declared = header.body_uncompressed_len;
    let ceiling = limits.max_column_stats_bytes;
    let over_ceiling = if declared > ceiling {
        Some(reserver.reserve_decoded(declared, ceiling)?)
    } else {
        None
    };
    let mut meter = WorkingReservation {
        reserver: reserver.owned(),
        ceiling,
        working: None,
        kept: Vec::new(),
    };
    let keep = keep.clone();
    let decoded = run_snapshot_decode(gate, ReadSite::CatalogColumnStats, declared, move || {
        let holds = |name: &str| keep.holds(name);
        match decode_column_stats_streamed(&data, &limits, &holds, &mut meter) {
            Ok(decoded) => Ok(Ok((decoded, meter.kept))),
            Err(MeteredDecodeError::Memory(refusal)) => Ok(Err(refusal)),
            Err(MeteredDecodeError::Format(err)) => Err(err),
        }
    })
    .await;
    drop(over_ceiling);
    let (decoded, reservations) = match decoded {
        Ok(Ok(decoded)) => decoded,
        Ok(Err(refusal)) => return Err(refusal.into()),
        // Decode of an object the ref points at: the fold wrote it, so a
        // failure to open it is never the ordinary not-covered case. Surface
        // it for the caller to log once and count (issue #1400), or, for a
        // failed gate job, to account for separately; the query still
        // degrades to a miss and scans.
        Err(err) => return Ok(FetchOutcome::DecodeRefused(err)),
    };

    let mut segments = HashMap::new();
    let mut by_content_hash = HashMap::new();
    for segment in decoded.segments {
        match segment.writer_id.len() {
            16 => {
                let Ok(writer_id) = <[u8; 16]>::try_from(segment.writer_id.as_slice()) else {
                    continue;
                };
                let identity: EntryIdentity = (
                    segment.ingest_hour_bucket,
                    segment.shard,
                    writer_id,
                    segment.writer_epoch,
                    segment.writer_seq,
                );
                segments.insert(identity, segment);
            }
            32 => {
                let Ok(content_hash) = <[u8; 32]>::try_from(segment.writer_id.as_slice()) else {
                    continue;
                };
                by_content_hash.insert(content_hash, segment);
            }
            _ => continue,
        }
    }

    Ok(FetchOutcome::Loaded(
        DecodedStats {
            segments,
            by_content_hash,
            body_uncompressed_len: header.body_uncompressed_len,
        },
        reservations,
    ))
}

/// The [`SegmentMeter`] [`fetch_stats_object`] decodes under: a working
/// reservation for the segment being read, and one kept reservation per
/// segment already read. Reserving a segment's kept bytes before releasing
/// its working charge keeps every decoded byte charged, so the most it holds
/// is the largest segment's encoded length plus the kept bytes.
struct WorkingReservation {
    reserver: Box<dyn DecodeReserver + Send>,
    ceiling: u64,
    working: Option<ravel_memory::Reservation>,
    kept: Vec<ravel_memory::Reservation>,
}

enum MeteredDecodeError {
    Format(SnapshotFormatError),
    Memory(ravel_memory::MemoryExhausted),
}

impl From<SnapshotFormatError> for MeteredDecodeError {
    fn from(err: SnapshotFormatError) -> Self {
        MeteredDecodeError::Format(err)
    }
}

impl SegmentMeter for WorkingReservation {
    type Error = MeteredDecodeError;

    fn before_segment(&mut self, len: u64) -> Result<(), MeteredDecodeError> {
        let working = self
            .reserver
            .reserve_decoded(len, self.ceiling)
            .map_err(MeteredDecodeError::Memory)?;
        self.working = Some(working);
        Ok(())
    }

    fn after_segment(&mut self, kept: &ColumnStatsSegment) -> Result<(), MeteredDecodeError> {
        let reservation = self
            .reserver
            .reserve_decoded(kept.encoded_len() as u64, self.ceiling)
            .map_err(MeteredDecodeError::Memory)?;
        self.kept.push(reservation);
        self.working = None;
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::collections::BTreeMap;

    use proptest::prelude::*;
    use ravel_proto::catalog::v1::{ColumnStat, ColumnValue, DictEntry, column_value::Kind};

    use super::*;
    use crate::snapshot_format::encode_column_stats_v3;

    const NAMES: [&str; 4] = ["a", "b", "c", "d"];

    fn i64_value(v: i64) -> ColumnValue {
        ColumnValue {
            kind: Some(Kind::I64(v)),
        }
    }

    /// A valid I64 column over `values`: exact counts, extrema and sum, and a
    /// dictionary when `present`.
    fn column(name: &str, values: &[i64], nulls: u64, present: bool) -> ColumnStat {
        let mut counts = BTreeMap::new();
        for v in values {
            *counts.entry(*v).or_insert(0u64) += 1;
        }
        ColumnStat {
            name: name.to_string(),
            declared_type: 2,
            non_null_count: values.len() as u64,
            null_count: nulls,
            min: values.iter().min().map(|v| i64_value(*v)),
            max: values.iter().max().map(|v| i64_value(*v)),
            dictionary_present: present,
            dictionary: if present {
                counts
                    .into_iter()
                    .map(|(v, count)| DictEntry {
                        value: Some(i64_value(v)),
                        count,
                    })
                    .collect()
            } else {
                Vec::new()
            },
            sum: Some(values.iter().sum()),
        }
    }

    fn arb_column(name: &'static str) -> impl Strategy<Value = ColumnStat> {
        (
            proptest::collection::vec(-3i64..4, 0..6),
            0u64..3,
            any::<bool>(),
        )
            .prop_map(move |(values, nulls, present)| column(name, &values, nulls, present))
    }

    /// One to four segments, each keyed by a distinct ascending 32-byte content
    /// hash, each carrying every column of [`NAMES`].
    fn arb_segments() -> impl Strategy<Value = Vec<ColumnStatsSegment>> {
        proptest::collection::vec(
            (
                arb_column(NAMES[0]),
                arb_column(NAMES[1]),
                arb_column(NAMES[2]),
                arb_column(NAMES[3]),
            ),
            1..5,
        )
        .prop_map(|rows| {
            rows.into_iter()
                .enumerate()
                .map(|(i, (a, b, c, d))| ColumnStatsSegment {
                    ingest_hour_bucket: 1,
                    shard: 0,
                    writer_id: vec![i as u8; 32],
                    writer_epoch: 1,
                    writer_seq: 1,
                    columns: vec![a, b, c, d],
                })
                .collect()
        })
    }

    fn arb_request() -> impl Strategy<Value = ColumnStatsRequest> {
        proptest::sample::subsequence(NAMES.to_vec(), 0..=NAMES.len()).prop_map(|names| {
            names
                .into_iter()
                .fold(ColumnStatsRequest::summary_only(), |request, name| {
                    request.with_dictionary(name)
                })
        })
    }

    /// `bytes` decoded and keyed the way [`fetch_stats_object`] keys them.
    fn decode(bytes: &[u8]) -> DecodedStats {
        let decoded =
            decode_column_stats(bytes, &ColumnStatsLimits::default()).expect("a fresh encode");
        DecodedStats {
            segments: HashMap::new(),
            by_content_hash: decoded
                .segments
                .into_iter()
                .map(|segment| {
                    let hash = <[u8; 32]>::try_from(segment.writer_id.as_slice())
                        .expect("a 32-byte content hash");
                    (hash, segment)
                })
                .collect(),
            body_uncompressed_len: decoded.header.body_uncompressed_len,
        }
    }

    proptest! {
        /// Issue #3244: stripping clears dictionaries and nothing else. An
        /// entry built from one decode for `first`, then rebuilt from a second
        /// decode for `second` with the first as `previous`, holds exactly
        /// the union; with every cleared dictionary copied back from a third
        /// decode it equals the encoded segments, so no summary field, and no
        /// `dictionary_present`, was touched. Its `heap_bytes` is the stripped
        /// segments' encoded length.
        ///
        /// FLIP: make `strip_dictionaries` also clear `dictionary_present`;
        /// the restored segments differ (the checked-in seed is that case).
        #[test]
        fn strip_then_reattach_restores_the_decoded_segments(
            segments in arb_segments(),
            first in arb_request(),
            second in arb_request(),
        ) {
            let bytes = encode_column_stats_v3([7; 16], 1, [9; 32], &segments, u64::MAX)
                .expect("distinct ascending segments encode");
            let earlier = PartColumnStats::from_decoded(decode(&bytes), &first, None);
            let entry = PartColumnStats::from_decoded(decode(&bytes), &second, Some(&earlier));
            let full = decode(&bytes).by_content_hash;

            let mut union = first.clone();
            union.extend(&second);
            for name in NAMES {
                prop_assert_eq!(entry.holds_dictionary(name), union.wants_dictionary(name));
            }
            prop_assert!(entry.covers(&union));

            let held_len: u64 = entry
                .by_content_hash
                .values()
                .map(|segment| segment.encoded_len() as u64)
                .sum();
            prop_assert_eq!(entry.heap_bytes(), held_len);

            let mut restored = entry.by_content_hash.clone();
            for (hash, segment) in &mut restored {
                for (held, original) in segment.columns.iter_mut().zip(&full[hash].columns) {
                    if union.wants_dictionary(&held.name) {
                        prop_assert_eq!(&held.dictionary, &original.dictionary);
                    } else {
                        prop_assert!(held.dictionary.is_empty());
                        held.dictionary = original.dictionary.clone();
                    }
                }
            }
            let mut restored: Vec<_> = restored.into_values().collect();
            restored.sort_by(|x, y| x.writer_id.cmp(&y.writer_id));
            prop_assert_eq!(restored, segments);
        }
    }

    /// A reserver over a bare budget that records the most the budget held
    /// after any of its reservations.
    #[derive(Clone)]
    struct HighWaterReserver {
        budget: Arc<ravel_memory::MemoryBudget>,
        high_water: Arc<std::sync::atomic::AtomicU64>,
    }

    impl DecodeReserver for HighWaterReserver {
        fn reserve_decoded(
            &self,
            declared: u64,
            ceiling: u64,
        ) -> Result<ravel_memory::Reservation, ravel_memory::MemoryExhausted> {
            let reservation = crate::charged::reserve_decoded(&self.budget, declared, ceiling)?;
            self.high_water
                .fetch_max(self.budget.reserved(), std::sync::atomic::Ordering::SeqCst);
            Ok(reservation)
        }

        fn owned(&self) -> Box<dyn DecodeReserver + Send> {
            Box::new(self.clone())
        }
    }

    /// A store holding one object, whatever key is asked for.
    struct OneObject(bytes::Bytes);

    impl AccountedRecordGet for OneObject {
        fn accounted_get_full(
            &self,
            _key: &str,
        ) -> impl Future<Output = Result<ravel_object_store::GetOutcome, StoreError>> + Send
        {
            let data = self.0.clone();
            async move {
                Ok(ravel_object_store::GetOutcome {
                    total_size: data.len() as u64,
                    data,
                    etag: ravel_object_store::Etag(String::new()),
                    version: ravel_object_store::Version(String::new()),
                })
            }
        }
    }

    const WIDE_SEGMENTS: usize = 64;
    const WIDE_ENTRIES: usize = 512;
    const WIDE_VALUE_LEN: usize = 1024;

    /// A segment holding one bytes column whose dictionary is about half a
    /// megabyte: `WIDE_ENTRIES` distinct `WIDE_VALUE_LEN`-byte values. Every
    /// segment encodes to the same length.
    fn wide_segment(index: usize) -> ColumnStatsSegment {
        let value = |entry: usize| {
            let mut bytes = Vec::with_capacity(WIDE_VALUE_LEN);
            bytes.extend_from_slice(&(index as u32).to_be_bytes());
            bytes.extend_from_slice(&(entry as u32).to_be_bytes());
            bytes.resize(WIDE_VALUE_LEN, 0x5A);
            ColumnValue {
                kind: Some(Kind::BytesVal(bytes)),
            }
        };
        let mut writer_id = vec![0u8; 32];
        writer_id[..4].copy_from_slice(&(index as u32).to_be_bytes());
        ColumnStatsSegment {
            ingest_hour_bucket: 1,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq: 1,
            columns: vec![ColumnStat {
                name: "wide".to_string(),
                declared_type: 4,
                non_null_count: WIDE_ENTRIES as u64,
                null_count: 0,
                min: Some(value(0)),
                max: Some(value(WIDE_ENTRIES - 1)),
                dictionary_present: true,
                dictionary: (0..WIDE_ENTRIES)
                    .map(|entry| DictEntry {
                        value: Some(value(entry)),
                        count: 1,
                    })
                    .collect(),
                sum: None,
            }],
        }
    }

    /// Issue #3244: a summary-only fetch of an object whose body is over
    /// 32 MiB never holds more than one segment's encoded bytes plus the
    /// bytes it keeps. Its reservations peak at exactly the segment length
    /// plus every kept byte (the last segment's working charge is still held
    /// when its kept bytes are reserved), it fits a budget of exactly that
    /// size, and what it hands back holds exactly the kept bytes.
    ///
    /// FLIP: reserve the declared body before decoding, as the bulk decode
    /// did, or strip the dictionaries only after every segment is read;
    /// either is refused by this budget, failing the `expect`.
    #[tokio::test]
    async fn a_summary_only_fetch_reserves_one_segment_plus_the_kept_bytes() {
        let segments: Vec<ColumnStatsSegment> = (0..WIDE_SEGMENTS).map(wide_segment).collect();
        let segment_len = segments[0].encoded_len() as u64;
        assert!(
            segments
                .iter()
                .all(|segment| segment.encoded_len() as u64 == segment_len)
        );
        let kept: u64 = segments
            .iter()
            .map(|segment| {
                let mut stripped = segment.clone();
                stripped.columns[0].dictionary.clear();
                stripped.encoded_len() as u64
            })
            .sum();
        let part_blake3 = [0x22; 32];
        let tenant = TenantHash([0x11; 16]);
        let bytes = crate::snapshot_format::encode_column_stats_v3(
            tenant.0,
            1,
            part_blake3,
            &segments,
            u64::MAX,
        )
        .expect("distinct ascending segments encode");
        let declared = decode_column_stats_header(&bytes)
            .expect("header")
            .header
            .body_uncompressed_len;
        assert!(declared >= 32 << 20, "declared body {declared}");
        // A stripped segment is 2116 bytes: 34 of writer_id, 6 of hour, epoch
        // and seq, and a 2073-byte column (min and max at 1030 each) behind a
        // 3-byte tag and length. The dictionary adds 512 entries of 1035.
        assert_eq!(segment_len, 532_037);
        assert_eq!(kept, 64 * 2116);

        let peak = segment_len + kept;
        let reserver = HighWaterReserver {
            budget: Arc::new(ravel_memory::MemoryBudget::new(peak)),
            high_water: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        };
        let resolved = ResolvedStatsRef {
            key: "stats.cstat".to_string(),
            blake3: *blake3::hash(&bytes).as_bytes(),
            expected_part_blake3: vec![part_blake3],
            expected_version: 3,
        };
        let outcome = fetch_stats_object(
            &OneObject(bytes::Bytes::from(bytes)),
            &tenant,
            &resolved,
            &HeldDictionaries::Columns(BTreeSet::new()),
            &reserver,
            None,
        )
        .await
        .expect("fits a budget of one segment plus the kept bytes");
        let FetchOutcome::Loaded(decoded, reservations) = outcome else {
            panic!("the object loads");
        };

        let high_water = reserver
            .high_water
            .load(std::sync::atomic::Ordering::SeqCst);
        assert!(high_water <= segment_len + kept);
        assert_eq!(high_water, peak);
        let held: u64 = reservations.iter().map(|r| r.size()).sum();
        assert_eq!(held, kept);
        assert_eq!(reserver.budget.reserved(), kept);
        let decoded_len: u64 = decoded
            .by_content_hash
            .values()
            .map(|segment| segment.encoded_len() as u64)
            .sum();
        assert_eq!(decoded_len, kept);
        assert_eq!(decoded.by_content_hash.len(), WIDE_SEGMENTS);
    }
}
