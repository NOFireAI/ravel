//! Column-statistics load for the query-time metadata-only path (ADR-0850).
//! Like [`crate::covering_postings::load_covering_postings`] it fetches HEAD,
//! follows its `column_stats` ref, and GET/blake3-verifies/tenant-checks/
//! decodes the object, but it fetches NO snapshot part: it needs only each
//! part's blake3 (already carried in `SnapshotHead.parts`) to bind the stats
//! object to this HEAD's part set. Every GET runs through the caller's
//! accounted, semaphore-bounded funnel (issue #850), so the two GETs this
//! path issues are counted and rate-limited exactly like every other query
//! read. A query engine (`ravel-sql`'s `LogsScanExec`) consumes the result by
//! joining its own resolved snapshot's live segments against
//! [`LoadedColumnStats::segments`] by identity; a segment with no entry there
//! (never built, build failed, or the segment postdates this stats object)
//! has no exact statistics and the query must fall back to scanning it.
//!
//! # Degrade-to-`None`, one loud exception
//!
//! Column statistics are an OPTIONAL metadata artifact, so every failure
//! short of an isolation breach degrades to `Ok(None)` and the query scans:
//! no HEAD yet, no column-stats ref, any GET error, a blake3 mismatch, a
//! decode error, or a part-binding mismatch against the current HEAD's parts.
//! `decode_column_stats` does not itself check part-binding against a
//! caller-supplied part list (unlike `decode_postings`, which takes the
//! expected part_blake3 as an argument); this loader performs that check
//! itself, exactly as
//! [`crate::column_stats_build::decode_previous_column_stats`] does on the
//! fold side. The only two loud exceptions are a genuinely unparseable HEAD
//! (a real catalog defect, not an optional artifact) and a column-stats
//! object declaring a foreign `tenant_hash` (an ADR-0050 §2 isolation
//! breach): neither is absorbed into a silent degrade.

use std::collections::HashMap;

use prost::Message;
use ravel_proto::catalog::v1::ColumnStatsSegment;
use ravel_types::{Signal, TenantHash};

use crate::EntryIdentity;
use crate::provisioning::AccountedRecordGet;
use crate::snapshot_format::{
    ColumnStatsLimits, SnapshotFormatError, decode_column_stats, decode_head,
};

/// The owned column-statistics inputs a query-time metadata-only plan needs:
/// exact per-segment statistics keyed by the same
/// `(ingest_hour_bucket, shard, writer_id, writer_epoch, writer_seq)`
/// identity `fold::entry_identity` uses, so a resolved snapshot's live
/// segments can be looked up directly with no ordinal bookkeeping.
#[derive(Clone, Debug)]
pub struct LoadedColumnStats {
    /// Every segment this column-stats object carries an entry for, keyed by
    /// identity. A live segment absent from this map has no exact
    /// statistics.
    pub segments: HashMap<EntryIdentity, ColumnStatsSegment>,
    /// The covered parts' blake3 hashes, in `SnapshotHead.parts` order (the
    /// binding this loader verifies before returning `Some`).
    pub part_blake3: Vec<[u8; 32]>,
}

impl LoadedColumnStats {
    /// The exact byte weight this object charges against the catalog's
    /// column-statistics cache budget (issue #905): the sum of every held
    /// segment's encoded protobuf length plus 32 bytes per covered part hash.
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
            .sum();
        let part_bytes = self.part_blake3.len() as u64 * 32;
        segment_bytes + part_bytes
    }
}

/// The column-stats object the current folded HEAD points at, resolved from
/// HEAD alone (one GET) without yet fetching the object itself. Splitting the
/// load in two lets a caller consult a reuse cache keyed by what actually
/// identifies the resolved object -- its content hash (`blake3`) plus the
/// HEAD's covered part set (`expected_part_blake3`, the binding a stale stats
/// object fails) -- and skip the second GET when both still match a cached
/// entry. A changed fold produces a different HEAD, hence a different `blake3`
/// or part set, so it always misses and re-resolves (issue #888).
#[derive(Clone, Debug)]
pub(crate) struct ResolvedStatsRef {
    /// Object key of the `.cstat` object HEAD references.
    pub key: String,
    /// Content hash of the referenced object (`SnapshotColumnStatsRef.blake3`),
    /// the fetch's blake3 gate and the cache's primary key.
    pub blake3: [u8; 32],
    /// The covered parts' blake3 hashes, in `SnapshotHead.parts` order. The
    /// binding this HEAD imposes on the stats object; a fetch (or a cache hit)
    /// is valid only against this exact part set.
    pub expected_part_blake3: Vec<[u8; 32]>,
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

/// Read the current folded snapshot HEAD for `(tenant, signal)` and return the
/// column-stats object it points at, or `Ok(None)` when none exists right now
/// (nothing folded yet, no configured typed columns, or the last fold's
/// column-stats build/PUT failed). The first of the two-GET load: it issues
/// the HEAD GET only, and never fetches the stats object -- that is
/// [`fetch_stats_object`], so a caller can consult a reuse cache in between.
///
/// The HEAD GET is issued through `getter`, so it is credited to the caller's
/// [`QueryAccounting`](ravel_types::accounting::QueryAccounting) and bounded
/// by the catalog request semaphore, the same funnel every other query read
/// path uses (issue #850). It charges to `AccountedOp::Get`.
///
/// Unlike `load_covering_postings`, this path fetches no snapshot part: it
/// needs only each part's blake3 (already carried in `SnapshotHead.parts`) for
/// the binding check, and callers join against their own resolved snapshot
/// rather than a rebuilt covered-entry universe. See the [module docs](self)
/// for the full degrade-to-`Ok(None)` contract.
pub(crate) async fn resolve_stats_ref(
    getter: &impl AccountedRecordGet,
    tenant: &TenantHash,
    signal: Signal,
) -> Result<Option<ResolvedStatsRef>, LoadColumnStatsError> {
    let key = head_key(tenant, signal);

    let head_bytes = match getter.accounted_get_full(&key).await {
        Ok(got) => got.data,
        Err(_) => return Ok(None),
    };
    let head = decode_head(&head_bytes).map_err(|source| LoadColumnStatsError::HeadCorrupt {
        key: key.clone(),
        source,
    })?;

    let Some(stats_ref) = head.column_stats.clone() else {
        return Ok(None);
    };
    let Ok(blake3) = <[u8; 32]>::try_from(stats_ref.blake3.as_slice()) else {
        return Ok(None);
    };

    // The parts are NOT fetched: this loader needs only their blake3 (which
    // HEAD already carries) to bind the stats object to this HEAD's part set.
    // A per-part GET here would GET every snapshot part in full only to
    // discard it, and would turn a fault in an optional metadata artifact into
    // a hard error on the logs query path.
    let mut expected_part_blake3: Vec<[u8; 32]> = Vec::with_capacity(head.parts.len());
    for part_ref in &head.parts {
        let Ok(part_blake3) = <[u8; 32]>::try_from(part_ref.blake3.as_slice()) else {
            return Ok(None);
        };
        expected_part_blake3.push(part_blake3);
    }

    Ok(Some(ResolvedStatsRef {
        key: stats_ref.key,
        blake3,
        expected_part_blake3,
    }))
}

/// GET, blake3-verify, tenant-check, part-bind, and decode the object named by
/// `resolved`. The second GET of the two-GET load; kept separate from
/// [`resolve_stats_ref`] so a caller can skip it on a reuse-cache hit. Every
/// degrade-to-`Ok(None)` case from the [module docs](self) is enforced here
/// against `resolved`'s content hash and part binding, so a cache that keys on
/// those two values and skips this call gets the identical answer this call
/// would have produced.
pub(crate) async fn fetch_stats_object(
    getter: &impl AccountedRecordGet,
    tenant: &TenantHash,
    resolved: &ResolvedStatsRef,
) -> Result<Option<LoadedColumnStats>, LoadColumnStatsError> {
    let data = match getter.accounted_get_full(&resolved.key).await {
        Ok(got) => got.data,
        Err(_) => return Ok(None),
    };
    let digest = blake3::hash(&data);
    if *digest.as_bytes() != resolved.blake3 {
        return Ok(None);
    }

    let limits = ColumnStatsLimits::default();
    let decoded = match decode_column_stats(&data, &limits) {
        Ok(decoded) => decoded,
        Err(_) => return Ok(None),
    };

    if decoded.header.tenant_hash != tenant.0.to_vec() {
        return Err(LoadColumnStatsError::TenantHashMismatch {
            key: resolved.key.clone(),
            expected: tenant.to_hex(),
            actual: hex::encode(&decoded.header.tenant_hash),
        });
    }

    let actual_part_blake3: Result<Vec<[u8; 32]>, _> = decoded
        .header
        .part_blake3
        .iter()
        .map(|h| <[u8; 32]>::try_from(h.as_slice()))
        .collect();
    let Ok(actual_part_blake3) = actual_part_blake3 else {
        return Ok(None);
    };
    if actual_part_blake3 != resolved.expected_part_blake3 {
        return Ok(None);
    }

    let mut segments = HashMap::with_capacity(decoded.segments.len());
    for segment in decoded.segments {
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

    Ok(Some(LoadedColumnStats {
        segments,
        part_blake3: resolved.expected_part_blake3.clone(),
    }))
}
