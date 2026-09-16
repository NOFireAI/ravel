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
//! `ColumnStatsSegment.writer_id` at 32 bytes. [`LoadedColumnStats`] keeps a
//! legacy `segments` map keyed by the five-field `EntryIdentity` tuple
//! (`fold::entry_identity`) for the retired v1 keying scheme; nothing
//! populates it anymore, so it is always empty, but its removal would ripple
//! into `ravel-sql`'s consumption of this type and is out of this change's
//! scope.
//!
//! # Degrade-to-`None`, one loud exception
//!
//! Column statistics are an OPTIONAL metadata artifact, so every failure short
//! of an isolation breach degrades: no HEAD yet, no ref at all, any GET error,
//! a blake3 mismatch, a decode error, or a part-binding mismatch. One of those
//! degrades is not silent: a DECODE failure on an object a covered part
//! actually references means the fold wrote an object the reader cannot open,
//! so the fetch path surfaces it as [`FetchOutcome::DecodeRefused`] (rather
//! than folding it into a bare miss) for the caller to log once and count
//! (issue #1400). The query still scans; only its visibility changes.
//! `decode_column_stats` does not itself check part-binding against a
//! caller-supplied part list (unlike `decode_postings`); this loader performs
//! that check itself, exactly as
//! [`crate::column_stats_build::decode_previous_column_stats`] does on the
//! fold side. The only two loud exceptions are a genuinely unparseable HEAD (a
//! real catalog defect, not an optional artifact) and a column-stats object
//! declaring a foreign `tenant_hash` (an ADR-0050 §2 isolation breach): neither
//! is absorbed into a silent degrade.

use std::collections::HashMap;

use prost::Message;
use ravel_proto::catalog::v1::{ColumnStatsSegment, SnapshotPartRef};
use ravel_types::{Signal, TenantHash};

use crate::EntryIdentity;
use crate::provisioning::AccountedRecordGet;
use crate::snapshot_format::{
    ColumnStatsLimits, SnapshotFormatError, decode_column_stats, decode_head,
};

/// The owned column-statistics inputs a query-time metadata-only plan needs:
/// exact per-segment statistics, assembled from every covered part a query's
/// window touches. v1 (whole-object, tuple-keyed) records live in `segments`;
/// v2 (whole-object) and v3 (per-part) records, both content-hash-keyed, live
/// in `by_content_hash`. A live segment absent from both has no exact
/// statistics and the query scans it.
#[derive(Clone, Debug, Default)]
pub struct LoadedColumnStats {
    /// Retired v1 keying scheme
    /// (`(ingest_hour_bucket, shard, writer_id, writer_epoch, writer_seq)`,
    /// `fold::entry_identity`). Nothing populates this map anymore (ADR-1413
    /// decision 6): it is always empty. Kept rather than removed because
    /// removing it ripples into `ravel-sql`'s consumption of this type,
    /// outside this change's scope.
    pub segments: HashMap<EntryIdentity, ColumnStatsSegment>,
    /// v3 per-part records, keyed by content hash (`SegmentRef::content_hash`,
    /// ADR-0942).
    pub by_content_hash: HashMap<[u8; 32], ColumnStatsSegment>,
    /// The covered parts' blake3 hashes this load was assembled for, in
    /// `SnapshotHead.parts` order (the window a cache entry built from this
    /// load remains valid against).
    pub part_blake3: Vec<[u8; 32]>,
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
    /// The exact byte weight this object charges against the catalog's
    /// column-statistics cache budget (issue #905): the sum of every held
    /// segment's encoded protobuf length (both maps) plus 32 bytes per covered
    /// part hash.
    ///
    /// It counts the decoded `.cstat` payload itself, the term that scales with
    /// a tenant's declared typed columns times its live segments and with the
    /// number of tenants a process serves, and the term the budget exists to
    /// bound. It deliberately does NOT count `HashMap` slot overhead or the
    /// `Arc` control block, so the figure is an exact, reproducible function of
    /// the cached data (identical contents in, identical bytes out) rather than
    /// an estimate of the entry count; it is a lower bound on the process RSS
    /// the entry costs. The bytes counted are decoded-payload bytes, not the
    /// object's on-wire compressed size.
    pub fn heap_bytes(&self) -> u64 {
        let segment_bytes: u64 = self
            .segments
            .values()
            .map(|segment| segment.encoded_len() as u64)
            .sum::<u64>()
            + self
                .by_content_hash
                .values()
                .map(|segment| segment.encoded_len() as u64)
                .sum::<u64>();
        let part_bytes = self.part_blake3.len() as u64 * 32;
        segment_bytes + part_bytes
    }

    /// The exact statistics for a live segment: the content-hash keyed record
    /// (a per-part v3 object, ADR-0942), falling back to the retired
    /// identity-keyed v1 map (always empty since ADR-1413 decision 6). Returns
    /// `None` when neither map carries an entry: the segment has no exact
    /// statistics and the caller must scan it.
    pub fn stat_for(
        &self,
        content_hash: &[u8; 32],
        identity: &EntryIdentity,
    ) -> Option<&ColumnStatsSegment> {
        self.by_content_hash
            .get(content_hash)
            .or_else(|| self.segments.get(identity))
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
/// the two degrade-to-miss kinds the caller must tell apart. A store read that
/// fails and a stale binding are legitimately "no statistics" ([`Self::Absent`]);
/// a DECODE failure on the object HEAD (or a covered part) points at is not
/// ([`Self::DecodeRefused`]) and the caller logs and counts it (issue #1400).
pub(crate) enum FetchOutcome {
    /// Fetched, hash-verified, tenant-checked, part-bound, and decoded.
    Loaded(DecodedStats),
    /// No usable object, silently: the store GET failed (the object may simply
    /// not exist for this HEAD yet), the content hash did not match, a part
    /// hash was malformed, or the part binding was stale. Every one of these is
    /// an ordinary "no statistics" and stays quiet.
    Absent,
    /// HEAD (or a covered part) references an object the reader refused to
    /// DECODE: the fold wrote it and the ref points at it, but the bytes will
    /// not open (an oversized declared body, corruption, a crc or header
    /// failure). Never normal. Carries the decode error so the caller can name
    /// the cause and, for [`SnapshotFormatError::ColumnStatsDecompressedTooLarge`],
    /// the declared and cap bytes.
    DecodeRefused(SnapshotFormatError),
}

/// One fetched-and-decoded object's records, split by keying scheme. Kept
/// separate from the public [`LoadedColumnStats`] because a single query
/// merges records from up to several such fetches (one per covered part, plus
/// at most one whole-object fallback) before it has a final result.
#[derive(Default)]
pub(crate) struct DecodedStats {
    pub segments: HashMap<EntryIdentity, ColumnStatsSegment>,
    pub by_content_hash: HashMap<[u8; 32], ColumnStatsSegment>,
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

    let head_bytes = match getter.accounted_get_full(&key).await {
        Ok(got) => got.data,
        Err(_) => return Ok(None),
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
/// A record's `writer_id` selects which map it lands in: 16 bytes is the
/// retired v1 `EntryIdentity` tuple (never produced anymore), 32 bytes is a
/// v3 content hash (ADR-0942's overloaded slot). Any other length is a
/// malformed record and is dropped.
pub(crate) async fn fetch_stats_object(
    getter: &impl AccountedRecordGet,
    tenant: &TenantHash,
    resolved: &ResolvedStatsRef,
) -> Result<FetchOutcome, LoadColumnStatsError> {
    let data = match getter.accounted_get_full(&resolved.key).await {
        Ok(got) => got.data,
        // Store read: the object may simply not exist for this HEAD. Silent.
        Err(_) => return Ok(FetchOutcome::Absent),
    };
    let digest = blake3::hash(&data);
    if *digest.as_bytes() != resolved.blake3 {
        return Ok(FetchOutcome::Absent);
    }

    let limits = ColumnStatsLimits::default();
    let decoded = match decode_column_stats(&data, &limits) {
        Ok(decoded) => decoded,
        // Decode of an object the ref points at: the fold wrote it, so a
        // failure to open it is never the ordinary not-covered case. Surface
        // it for the caller to log once and count (issue #1400); the query
        // still degrades to a miss and scans.
        Err(err) => return Ok(FetchOutcome::DecodeRefused(err)),
    };

    // A part's field 7 promises a v3 envelope; `decode_column_stats` only
    // checks the envelope byte against the object's OWN header, never against
    // which slot named it. A mismatch here means the object at `resolved.key`
    // is not v3 (a stale object left behind by a downgrade, or a future writer
    // bug), so it must not be accepted: degrade like any other stale binding.
    if decoded.header.tenant_hash != tenant.0.to_vec() {
        return Err(LoadColumnStatsError::TenantHashMismatch {
            key: resolved.key.clone(),
            expected: tenant.to_hex(),
            actual: hex::encode(&decoded.header.tenant_hash),
        });
    }

    if decoded.header.format_version != resolved.expected_version {
        return Ok(FetchOutcome::Absent);
    }

    let actual_part_blake3: Result<Vec<[u8; 32]>, _> = decoded
        .header
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

    Ok(FetchOutcome::Loaded(DecodedStats {
        segments,
        by_content_hash,
    }))
}
