//! Column-statistics load for the query-time metadata-only path (ADR-0850).
//! Mirrors [`crate::covering_postings::load_covering_postings`] exactly:
//! fetch HEAD, follow its `column_stats` ref, GET/blake3-verify/tenant-check/
//! decode. A query engine (`ravel-sql`'s `LogsScanExec`) consumes the result
//! by joining its own resolved snapshot's live segments against
//! [`LoadedColumnStats::segments`] by identity; a segment with no entry there
//! (never built, build failed, or the segment postdates this stats object)
//! has no exact statistics and the query must fall back to scanning it.
//!
//! # Degrade-to-`None`, one loud exception
//!
//! Same discipline as postings: every failure short of an isolation breach
//! degrades to `Ok(None)` (no HEAD yet, no column-stats ref, any GET error, a
//! blake3 mismatch, a decode error, a part-binding mismatch against the
//! current HEAD's parts). `decode_column_stats` does not itself check
//! part-binding against a caller-supplied part list (unlike
//! `decode_postings`, which takes the expected part_blake3 as an argument);
//! this loader performs that check itself, exactly as
//! [`crate::column_stats_build::decode_previous_column_stats`] does on the
//! fold side. A genuinely unparseable HEAD or part is a real catalog defect,
//! surfaced the same way `load_covering_postings` surfaces one; a
//! column-stats object declaring a foreign `tenant_hash` is an ADR-0050 §2
//! isolation breach, never absorbed into a silent degrade.

use std::collections::HashMap;

use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_proto::catalog::v1::ColumnStatsSegment;
use ravel_types::{Signal, TenantHash};

use crate::EntryIdentity;
use crate::snapshot_format::{
    ColumnStatsLimits, PartLimits, SnapshotFormatError, decode_column_stats, decode_head,
    decode_part,
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

/// A genuinely unparseable HEAD/part, or an isolation breach, encountered
/// while loading column statistics: the only conditions
/// [`load_column_stats`] surfaces as an error rather than degrading to
/// `Ok(None)`.
#[derive(Debug, thiserror::Error)]
pub enum LoadColumnStatsError {
    #[error("HEAD at {key} is corrupt: {source}")]
    HeadCorrupt {
        key: String,
        #[source]
        source: SnapshotFormatError,
    },
    #[error("snapshot part {key} is corrupt: {source}")]
    PartCorrupt {
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

/// Resolve exact per-segment column statistics for `(tenant, signal)` from
/// the current folded snapshot HEAD, or `Ok(None)` when no usable
/// column-stats object exists right now (nothing folded yet, no configured
/// typed columns, or the last fold's column-stats build/PUT failed).
///
/// Metadata-cost: reads the HEAD and the one column-stats object, never a
/// data segment or a snapshot part (unlike `load_covering_postings`, this
/// loader does not need the covered-entry universe -- callers already have
/// their own resolved snapshot to join against). See the [module
/// docs](self) for the full degrade-to-`Ok(None)` contract.
pub async fn load_column_stats(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
) -> Result<Option<LoadedColumnStats>, LoadColumnStatsError> {
    let key = head_key(tenant, signal);

    let head_bytes = match store.get(&key, GetRange::Full).await {
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

    let part_limits = PartLimits::default();
    let mut expected_part_blake3: Vec<[u8; 32]> = Vec::with_capacity(head.parts.len());
    for part_ref in &head.parts {
        let Ok(blake3) = <[u8; 32]>::try_from(part_ref.blake3.as_slice()) else {
            return Ok(None);
        };
        expected_part_blake3.push(blake3);
        // Parts are fetched only to prove liveness/decodability of the HEAD
        // this stats ref is bound to, mirroring `load_covering_postings`'s
        // hard-error-on-corrupt-part precedent; their entries are not needed
        // here.
        let got = match store.get(&part_ref.key, GetRange::Full).await {
            Ok(got) => got,
            Err(_) => return Ok(None),
        };
        if let Err(source) = decode_part(&got.data, &part_limits) {
            return Err(LoadColumnStatsError::PartCorrupt {
                key: part_ref.key.clone(),
                source,
            });
        }
    }

    let data = match store.get(&stats_ref.key, GetRange::Full).await {
        Ok(got) => got.data,
        Err(_) => return Ok(None),
    };
    let digest = blake3::hash(&data);
    if digest.as_bytes().as_slice() != stats_ref.blake3.as_slice() {
        return Ok(None);
    }

    let limits = ColumnStatsLimits::default();
    let decoded = match decode_column_stats(&data, &limits) {
        Ok(decoded) => decoded,
        Err(_) => return Ok(None),
    };

    if decoded.header.tenant_hash != tenant.0.to_vec() {
        return Err(LoadColumnStatsError::TenantHashMismatch {
            key: stats_ref.key.clone(),
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
    if actual_part_blake3 != expected_part_blake3 {
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
        part_blake3: expected_part_blake3,
    }))
}
