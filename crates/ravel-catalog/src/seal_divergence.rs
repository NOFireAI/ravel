//! Seal-divergence verification (ADR-0059 decision 2): re-list a
//! (tenant, signal)'s sealed commit records directly from the store and diff
//! them against the folded snapshot, catching under-counting from clock-skew
//! seal divergence.
//!
//! This is the comparison logic `ravel-cli catalog verify`
//! (`services/ravel-cli/src/catalog.rs`) has always run inline, factored out so
//! both the CLI (manual/ad-hoc use) and the scheduled scrubber
//! (`ravel_server::scrub`, on the fold cadence) drive one implementation. It is
//! metadata-cost: it reads commit records and the snapshot parts, never a data
//! object. It detects and reports; it never repairs (ADR-0059 consequences).
//!
//! The check reconstructs two maps keyed by the same entry identity
//! ([`EntryIdentity`], the dedup key) and
//! classifies every difference:
//!
//! - `missing`: a sealed commit record with no matching snapshot entry. The
//!   folder under-counted; a real divergence.
//! - `mismatched`: present in both, different `content_hash`. Also a real
//!   divergence.
//! - `orphaned`: a snapshot entry with no matching sealed commit record. This
//!   is *expected* once retention deletes a commit record after it has been
//!   folded (reconciliation), so it is
//!   reported but never treated as a failure by any caller.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

#[cfg(test)]
use prost::Message;
use ravel_commit::erasure::{
    compute_compaction_input_set_hash, compute_superseding_compaction_input_set_hash,
};
use ravel_commit::keys::{self, KeyError};
use ravel_commit::record::{self, RecordError};
use ravel_object_store::{GetRange, ObjectMeta, ObjectStoreBackend, StoreError};
use ravel_proto::catalog::v1::SnapshotHead;
use ravel_proto::commit::v1::{CompactionRecord, RewriteRecord};
use ravel_types::{Signal, TenantHash};
use uuid::Uuid;

use crate::error::CatalogError;
use crate::snapshot_format::{PartLimits, SnapshotFormatError, decode_head, decode_part};

/// Entry identity, the dedup key:
/// `(shard, ingest_hour_bucket, writer_id, writer_epoch, writer_seq)`. The same
/// tuple the CLI's `catalog verify` has always used; kept public so callers can
/// render the individual diverging entries.
pub type EntryIdentity = (u32, u32, [u8; 16], u64, u64);

/// The result of one seal-divergence comparison. Carries the full identity
/// lists (not just counts) because `ravel-cli catalog verify` prints each
/// diverging entry, and callers that only need counts read `.len()`.
///
/// `missing` and `mismatched` are the two divergence classes that indicate the
/// folder under-counted; `orphaned` is the expected retention-after-fold shape
/// and is never a failure (see the [module docs](self)).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SealDivergenceReport {
    /// The snapshot HEAD's watermark hour: sealed commit records past this hour
    /// are not expected in the snapshot yet and are excluded from the diff.
    pub watermark_hour: u32,
    /// Count of sealed commit records re-listed from the store (the ground
    /// truth the snapshot is compared against). Records superseded by a
    /// compaction or rewrite record are excluded, matching what the fold
    /// contributes to the snapshot.
    pub sealed_record_count: usize,
    /// Count of entries in the folded snapshot, all levels. Only the level-0
    /// (L0 commit) entries participate in the diff below; level-1 compaction
    /// and rewrite parts are counted here but not compared, since they have no
    /// L0 commit record in the ground truth.
    pub snapshot_entry_count: usize,
    /// Sealed commit records absent from the snapshot (an under-count).
    pub missing: Vec<EntryIdentity>,
    /// Entries present in both but with a different `content_hash`.
    pub mismatched: Vec<EntryIdentity>,
    /// Snapshot entries with no matching sealed commit record. Expected once
    /// retention deletes a folded commit record; never a failure.
    pub orphaned: Vec<EntryIdentity>,
}

impl SealDivergenceReport {
    /// Whether this report indicates the folder under-counted: any `missing` or
    /// `mismatched` entry. `orphaned` is deliberately excluded (expected).
    pub fn has_divergence(&self) -> bool {
        !self.missing.is_empty() || !self.mismatched.is_empty()
    }
}

/// A failure reading or decoding the objects the comparison needs. Distinct
/// from a *divergence* (which is a successful comparison whose result is a
/// [`SealDivergenceReport`]): a caller treats these as transient/skip
/// (the scrubber) or as a hard error to surface (the CLI), never as corruption
/// of the data corpus itself. Messages mirror the CLI's original inline
/// `anyhow` strings so surfacing one is behavior-preserving.
#[derive(Debug, thiserror::Error)]
pub enum SealDivergenceError {
    #[error("HEAD at {key} is corrupt: {source}")]
    HeadCorrupt {
        key: String,
        #[source]
        source: SnapshotFormatError,
    },
    /// The HEAD GET failed for a reason other than `NotFound` (issue #1976):
    /// a `NotFound` there still means "nothing folded yet" and stays an
    /// `Ok(None)` degrade, but any other failure (most commonly
    /// `AccessDenied` on a missing IAM read grant) is a store outage, not an
    /// absence.
    #[error("failed to fetch HEAD {key}: {source}")]
    HeadFetch {
        key: String,
        #[source]
        source: StoreError,
    },
    #[error("failed to fetch part {key}: {source}")]
    PartFetch {
        key: String,
        #[source]
        source: StoreError,
    },
    #[error("part {key} is corrupt: {source}")]
    PartCorrupt {
        key: String,
        #[source]
        source: SnapshotFormatError,
    },
    #[error("part {key} has a malformed writer_id entry")]
    PartWriterId { key: String },
    #[error("failed to build shard prefix: {source}")]
    ShardPrefix {
        #[source]
        source: KeyError,
    },
    #[error("failed to list {prefix}: {source}")]
    ListShard {
        prefix: String,
        #[source]
        source: StoreError,
    },
    #[error("failed to fetch {key}: {source}")]
    RecordFetch {
        key: String,
        #[source]
        source: StoreError,
    },
    #[error("commit record at {key} is corrupt: {source}")]
    RecordCorrupt {
        key: String,
        #[source]
        source: RecordError,
    },
    #[error("commit record at {key} has an invalid writer_id")]
    RecordWriterId { key: String },
    #[error("compaction record at {key} is corrupt: {source}")]
    CompactionRecordCorrupt {
        key: String,
        #[source]
        source: RecordError,
    },
    #[error(
        "compaction record at {key} declares input_set_hash {declared} but its own inputs \
         hash to {computed}: the record is corrupt or forged, not a routine condition; its \
         declared inputs are not trusted to suppress anything"
    )]
    CompactionInputSetHashMismatch {
        key: String,
        declared: String,
        computed: String,
    },
    #[error("rewrite record at {key} is corrupt: {source}")]
    RewriteRecordCorrupt {
        key: String,
        #[source]
        source: ravel_commit::erasure::ErasureError,
    },
    #[error("could not resolve the rewrite supersession in {bucket}: {source}")]
    Supersession {
        bucket: String,
        #[source]
        source: Box<CatalogError>,
    },
}

/// HEAD object key (docs/catalog-and-mvcc.md key layout, frozen format).
/// Duplicated here rather than exposing the `pub(crate)` helper, the same way
/// `ravel-cli`'s `catalog` module and `ravel_server::fold` duplicate it.
fn head_key(tenant: &TenantHash, signal: Signal) -> String {
    format!("t/{}/catalog/{}/HEAD", tenant.to_hex(), signal.key_prefix())
}

/// Re-list `(tenant, signal)`'s sealed commit records directly from `store` and
/// diff them against the current folded snapshot.
///
/// Returns `Ok(None)` when there is no HEAD yet (nothing folded, so nothing to
/// verify): fetching the HEAD returned `NotFound`, exactly the "nothing
/// folded yet" case the CLI has always treated as success. `Ok(Some(report))`
/// carries the classified diff (see [`SealDivergenceReport`]). `Err` is a
/// read/decode failure of the objects the comparison needs -- including a
/// non-`NotFound` HEAD GET failure (issue #1976) -- never the presence of a
/// divergence.
pub async fn verify_seal_divergence(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
    signal: Signal,
) -> Result<Option<SealDivergenceReport>, SealDivergenceError> {
    let key = head_key(tenant_hash, signal);

    // A `NotFound` fetching HEAD means nothing has been folded yet: there is
    // no snapshot to verify against, the "nothing folded yet, nothing to
    // verify" case, not a divergence. Any other GET failure (issue #1976:
    // most commonly AccessDenied on a missing IAM read grant) is a store
    // outage and must surface, not be swallowed as though nothing were
    // folded.
    let head_bytes = match store.get(&key, GetRange::Full).await {
        Ok(outcome) => outcome.data,
        Err(StoreError::NotFound) => return Ok(None),
        Err(source) => {
            return Err(SealDivergenceError::HeadFetch {
                key: key.clone(),
                source,
            });
        }
    };
    let head = decode_head(&head_bytes).map_err(|source| SealDivergenceError::HeadCorrupt {
        key: key.clone(),
        source,
    })?;

    // Decode the folded snapshot's entries into the shared L0 identity shape.
    //
    // This comparison is scoped to sealed L0 commit records: the ground truth
    // re-listed below is the L0 commit history, and the divergence it catches
    // is the folder under-counting those records. A snapshot carries two entry
    // levels (snapshot_format::part validate_entries): a level-0 L0 commit,
    // whose writer_id is the 16-byte flush uuid, and a level-1 compaction or
    // rewrite part, whose writer_id slot instead carries the parent record's
    // 32-byte input_set_hash (fold.rs build_l1_snapshot_entry). An L1 part has
    // no L0 commit record to match here: it represents the L0 records the fold
    // superseded, which are handled on the ground-truth side below. So only
    // level-0 entries enter the identity map; a level-1 entry's 32-byte
    // input_set_hash is never coerced into the 16-byte L0 tuple, which would
    // silently truncate and collide. `decode_part` has already checked the
    // per-level width, so a level-0 writer_id is guaranteed 16 bytes; the
    // fallible convert stays as defense against an entry that bypassed decode.
    let SnapshotEntries {
        level0: snapshot_entries,
        entry_count: snapshot_entry_count,
        ..
    } = read_snapshot_entries(store, &head).await?;

    // Re-list every sealed commit record directly from the store (the ground
    // truth), decoded into the same identity shape.
    //
    // A compaction or rewrite record supersedes a set of L0 commit records: the
    // fold folds those L0s into a level-1 part and drops them from the snapshot
    // (fold.rs contributed_bucket skips any L0 whose identity is in the
    // `excluded` set built from every compaction/rewrite record's `inputs`).
    // The superseded L0 commit records remain on the store until a later sweep
    // deletes them, so between fold and sweep a superseded L0 record is present
    // in the commit history but legitimately absent from the snapshot. To avoid
    // flagging it as `missing`, mirror the fold's exclusion here: build the same
    // superseded set from the shard's compaction and rewrite records, and skip
    // any L0 record it names. Matching the fold on raw `inputs` alone is
    // sufficient: a rewrite that supersedes a whole compaction record by key
    // adds no new L0s, since that compaction record's own `inputs` are already
    // collected here.
    let mut ground_truth: BTreeMap<EntryIdentity, Vec<u8>> = BTreeMap::new();
    for shard in 0..head.shard_count {
        let prefix = keys::commit_shard_prefix(tenant_hash, signal, shard)
            .map_err(|source| SealDivergenceError::ShardPrefix { source })?;
        let objects = ravel_object_store::list_all(store, &prefix)
            .await
            .map_err(|source| SealDivergenceError::ListShard {
                prefix: prefix.clone(),
                source,
            })?;

        // Pass one: the L0 identities superseded by a compaction or rewrite
        // record in this shard, keyed exactly as the fold keys `excluded`:
        // the raw `(writer_id string, epoch, seq)` triple.
        let records = load_superseding_records(store, &objects).await?;
        let mut superseded: HashSet<(String, u64, u64)> = HashSet::new();
        let compaction_inputs = records.compaction.into_iter().map(|(_, rec)| rec.inputs);
        let rewrite_inputs = records.rewrite.into_iter().map(|(_, rec)| rec.inputs);
        for input in compaction_inputs.chain(rewrite_inputs).flatten() {
            superseded.insert((input.writer_id, input.writer_epoch, input.writer_seq));
        }

        // Pass two: every non-superseded sealed L0 commit record.
        for object in &objects {
            let Ok(parsed) = keys::parse_commit_key(&object.key) else {
                continue;
            };
            if parsed.ingest_hour_bucket > head.watermark_hour {
                continue;
            }
            let got = store
                .get(&object.key, GetRange::Full)
                .await
                .map_err(|source| SealDivergenceError::RecordFetch {
                    key: object.key.clone(),
                    source,
                })?;
            let rec =
                record::decode(&got.data).map_err(|source| SealDivergenceError::RecordCorrupt {
                    key: object.key.clone(),
                    source,
                })?;
            if superseded.contains(&(rec.writer_id.clone(), rec.writer_epoch, rec.writer_seq)) {
                continue;
            }
            let writer_id = *Uuid::parse_str(&rec.writer_id)
                .map_err(|_| SealDivergenceError::RecordWriterId {
                    key: object.key.clone(),
                })?
                .as_bytes();
            let identity = (
                rec.shard,
                rec.ingest_hour_bucket,
                writer_id,
                rec.writer_epoch,
                rec.writer_seq,
            );
            ground_truth.insert(identity, rec.content_hash);
        }
    }

    let mut missing = Vec::new();
    let mut mismatched = Vec::new();
    for (identity, hash) in &ground_truth {
        match snapshot_entries.get(identity) {
            None => missing.push(*identity),
            Some(snap_hash) if snap_hash != hash => mismatched.push(*identity),
            Some(_) => {}
        }
    }
    let orphaned: Vec<EntryIdentity> = snapshot_entries
        .keys()
        .filter(|id| !ground_truth.contains_key(*id))
        .copied()
        .collect();

    Ok(Some(SealDivergenceReport {
        watermark_hour: head.watermark_hour,
        sealed_record_count: ground_truth.len(),
        snapshot_entry_count,
        missing,
        mismatched,
        orphaned,
    }))
}

/// The entries of the snapshot a decoded HEAD names, read part by part.
struct SnapshotEntries {
    /// Level-0 entries by identity, with their `content_hash`.
    level0: BTreeMap<EntryIdentity, Vec<u8>>,
    /// Level-1 entries as `(shard, ingest_hour_bucket, input_set_hash)`: the
    /// compaction and rewrite records whose parts the snapshot holds.
    level1_records: HashSet<(u32, u32, Vec<u8>)>,
    /// Entries across every part, all levels.
    entry_count: usize,
    /// Part GETs that returned an object.
    parts_read: usize,
}

/// GET and decode every part `head` references. A part that cannot be fetched
/// or decoded is an error, never an empty part.
async fn read_snapshot_entries(
    store: &dyn ObjectStoreBackend,
    head: &SnapshotHead,
) -> Result<SnapshotEntries, SealDivergenceError> {
    let limits = PartLimits::default();
    let mut entries = SnapshotEntries {
        level0: BTreeMap::new(),
        level1_records: HashSet::new(),
        entry_count: 0,
        parts_read: 0,
    };
    for part_ref in &head.parts {
        let got = store
            .get(&part_ref.key, GetRange::Full)
            .await
            .map_err(|source| SealDivergenceError::PartFetch {
                key: part_ref.key.clone(),
                source,
            })?;
        entries.parts_read += 1;
        let decoded =
            decode_part(&got.data, &limits).map_err(|source| SealDivergenceError::PartCorrupt {
                key: part_ref.key.clone(),
                source,
            })?;
        for entry in decoded.entries {
            entries.entry_count += 1;
            if entry.level != 0 {
                entries.level1_records.insert((
                    entry.shard,
                    entry.ingest_hour_bucket,
                    entry.writer_id,
                ));
                continue;
            }
            let writer_id: [u8; 16] = entry.writer_id.as_slice().try_into().map_err(|_| {
                SealDivergenceError::PartWriterId {
                    key: part_ref.key.clone(),
                }
            })?;
            let identity = (
                entry.shard,
                entry.ingest_hour_bucket,
                writer_id,
                entry.writer_epoch,
                entry.writer_seq,
            );
            entries.level0.insert(identity, entry.content_hash);
        }
    }
    Ok(entries)
}

/// The compaction and rewrite records among a listing, decoded and keyed by
/// their object key.
struct SupersedingRecords {
    compaction: Vec<(String, CompactionRecord)>,
    rewrite: Vec<(String, RewriteRecord)>,
}

/// GET and decode every compaction and rewrite record in `objects`, skipping
/// every other key.
///
/// Each compaction record's hash is recomputed for its own format_version
/// before a single input is trusted to supersede an L0 commit record: the
/// version 1 hash over `inputs` for a version 1 record, the version 2 hash
/// over `inputs` and `superseded_record_key` for a version 2 record. A
/// mismatch under that per-version rule means the record is corrupt or
/// forged, and failing loud here is what makes `catalog verify` notice instead
/// of silently under-checking real L0 entries.
async fn load_superseding_records(
    store: &dyn ObjectStoreBackend,
    objects: &[ObjectMeta],
) -> Result<SupersedingRecords, SealDivergenceError> {
    let mut records = SupersedingRecords {
        compaction: Vec::new(),
        rewrite: Vec::new(),
    };
    for object in objects {
        let is_compaction = keys::parse_compaction_record_key(&object.key).is_ok();
        if !is_compaction && keys::parse_rewrite_record_key(&object.key).is_err() {
            continue;
        }
        let got = store
            .get(&object.key, GetRange::Full)
            .await
            .map_err(|source| SealDivergenceError::RecordFetch {
                key: object.key.clone(),
                source,
            })?;
        if !is_compaction {
            let rec = ravel_commit::erasure::decode_rewrite(&got.data).map_err(|source| {
                SealDivergenceError::RewriteRecordCorrupt {
                    key: object.key.clone(),
                    source,
                }
            })?;
            records.rewrite.push((object.key.clone(), rec));
            continue;
        }
        let rec = record::decode_compaction(got.data.as_ref()).map_err(|source| {
            SealDivergenceError::CompactionRecordCorrupt {
                key: object.key.clone(),
                source,
            }
        })?;
        let computed = if rec.format_version == record::COMPACTION_SUPERSEDING_FORMAT_VERSION {
            compute_superseding_compaction_input_set_hash(&rec.inputs, &rec.superseded_record_key)
        } else {
            compute_compaction_input_set_hash(&rec.inputs)
        };
        if rec.input_set_hash.as_slice() != computed.as_slice() {
            return Err(SealDivergenceError::CompactionInputSetHashMismatch {
                key: object.key.clone(),
                declared: hex::encode(&rec.input_set_hash),
                computed: hex::encode(computed),
            });
        }
        records.compaction.push((object.key.clone(), rec));
    }
    Ok(records)
}

/// The result of [`snapshot_coverage`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SnapshotCoverage {
    /// The HEAD's watermark hour. `None` when there is no HEAD, in which case
    /// every identity asked about is missing.
    pub watermark_hour: Option<u32>,
    /// The identities asked about that the snapshot does not cover, in the
    /// order given, each listed once.
    pub missing: Vec<EntryIdentity>,
    /// Part GETs made: one per part the HEAD references, `0` with no HEAD.
    pub parts_read: usize,
    /// `(shard, hour)` buckets listed: only those holding an identity that is
    /// not a level-0 entry of the snapshot.
    pub buckets_listed: usize,
    /// Compaction and rewrite records fetched from those buckets.
    pub records_read: usize,
}

/// Whether the snapshot the `(tenant, signal)` HEAD names covers each of
/// `identities`, the L0 commits one writer knows it published.
///
/// An identity is covered when it is a level-0 entry of a part the HEAD
/// references, or when a compaction or rewrite record whose parts the
/// snapshot holds as level-1 entries supersedes it: a compaction record by
/// naming it among its `inputs`, a rewrite record by its own `inputs` or
/// through the predecessor chain [`crate::resolve_rewrite_supersession`]
/// chases, the same rule the fold applies. A superseding record on the store
/// whose parts the snapshot does not hold covers nothing. A rewrite with no
/// output parts, and a retention tombstone, remove their inputs from the
/// snapshot without a level-1 entry, so those inputs are reported missing.
///
/// Cost: one HEAD GET and one GET per part the HEAD references. Only for an
/// identity found in no level-0 entry is its `(shard, hour)` bucket listed and
/// its compaction and rewrite records fetched, never the tenant's whole commit
/// history. A HEAD or part that cannot be fetched or decoded is an `Err`; an
/// absent HEAD covers nothing.
pub async fn snapshot_coverage(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
    signal: Signal,
    identities: &[EntryIdentity],
) -> Result<SnapshotCoverage, SealDivergenceError> {
    let key = head_key(tenant_hash, signal);
    let head_bytes = match store.get(&key, GetRange::Full).await {
        Ok(outcome) => outcome.data,
        Err(StoreError::NotFound) => {
            let mut missing = identities.to_vec();
            dedup_in_order(&mut missing);
            return Ok(SnapshotCoverage {
                missing,
                ..SnapshotCoverage::default()
            });
        }
        Err(source) => return Err(SealDivergenceError::HeadFetch { key, source }),
    };
    let head = decode_head(&head_bytes)
        .map_err(|source| SealDivergenceError::HeadCorrupt { key, source })?;
    let snapshot = read_snapshot_entries(store, &head).await?;
    let mut coverage = SnapshotCoverage {
        watermark_hour: Some(head.watermark_hour),
        parts_read: snapshot.parts_read,
        ..SnapshotCoverage::default()
    };

    let mut candidates: Vec<EntryIdentity> = identities
        .iter()
        .filter(|id| !snapshot.level0.contains_key(*id))
        .copied()
        .collect();
    dedup_in_order(&mut candidates);
    let buckets: BTreeSet<(u32, u32)> = candidates.iter().map(|id| (id.0, id.1)).collect();

    let mut superseded: HashSet<EntryIdentity> = HashSet::new();
    for (shard, hour) in buckets {
        let prefix = keys::commit_shard_hour_prefix(tenant_hash, signal, shard, hour)
            .map_err(|source| SealDivergenceError::ShardPrefix { source })?;
        let objects = ravel_object_store::list_all(store, &prefix)
            .await
            .map_err(|source| SealDivergenceError::ListShard {
                prefix: prefix.clone(),
                source,
            })?;
        coverage.buckets_listed += 1;
        let records = load_superseding_records(store, &objects).await?;
        coverage.records_read += records.compaction.len() + records.rewrite.len();
        let held = |rec_hash: &[u8]| {
            snapshot
                .level1_records
                .contains(&(shard, hour, rec_hash.to_vec()))
        };

        let mut excluded: HashSet<(String, u64, u64)> = HashSet::new();
        for (_, rec) in &records.compaction {
            if held(&rec.input_set_hash) {
                for input in &rec.inputs {
                    excluded.insert((
                        input.writer_id.clone(),
                        input.writer_epoch,
                        input.writer_seq,
                    ));
                }
            }
        }
        let compaction_by_key: HashMap<&str, &CompactionRecord> = records
            .compaction
            .iter()
            .map(|(k, r)| (k.as_str(), r))
            .collect();
        let rewrite_by_key: HashMap<&str, &RewriteRecord> = records
            .rewrite
            .iter()
            .map(|(k, r)| (k.as_str(), r))
            .collect();
        let mut superseded_records: HashSet<String> = HashSet::new();
        for (rkey, rec) in &records.rewrite {
            if held(&rec.input_set_hash) {
                crate::catalog::resolve_rewrite_supersession(
                    rkey,
                    rec,
                    &prefix,
                    &compaction_by_key,
                    &rewrite_by_key,
                    &mut excluded,
                    &mut superseded_records,
                )
                .map_err(|source| SealDivergenceError::Supersession {
                    bucket: prefix.clone(),
                    source: Box::new(source),
                })?;
            }
        }
        for (writer_id, epoch, seq) in excluded {
            // An input whose writer_id is not a uuid names no L0 commit.
            if let Ok(writer) = Uuid::parse_str(&writer_id) {
                superseded.insert((shard, hour, *writer.as_bytes(), epoch, seq));
            }
        }
    }

    coverage.missing = candidates
        .into_iter()
        .filter(|id| !superseded.contains(id))
        .collect();
    Ok(coverage)
}

/// Drop every repeat of an earlier element, keeping first-seen order.
fn dedup_in_order(ids: &mut Vec<EntryIdentity>) {
    let mut seen = HashSet::new();
    ids.retain(|id| seen.insert(*id));
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use bytes::Bytes;
    use ravel_commit::publish::{self, RetryPolicy};
    use ravel_commit::record::NewCommitRecord;
    use ravel_object_store::PutOptions;
    use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault};
    use ravel_object_store::memory::MemoryStore;
    use ravel_types::TenantId;

    const NS_PER_HOUR: i64 = 3_600_000_000_000;
    // Comfortably past the default seal margins (matches the CLI test's
    // SEALED_AGE_NS), so a published record is sealed by fold time.
    const SEALED_AGE_NS: i64 = 3 * NS_PER_HOUR;

    async fn publish_segment(
        store: &MemoryStore,
        tenant: &str,
        seq: u64,
        created_unix_ns: i64,
    ) -> ravel_proto::commit::v1::CommitRecord {
        let tenant_hash = TenantId::new(tenant).hash();
        let ingest_hour_bucket = u32::try_from(created_unix_ns / NS_PER_HOUR).expect("fits u32");
        let payload = format!("seg-{seq}").into_bytes();
        let content_hash = *blake3::hash(&payload).as_bytes();
        let rec = record::build(NewCommitRecord {
            tenant_hash,
            signal: Signal::Metrics,
            shard: 0,
            writer_id: Uuid::new_v4(),
            writer_epoch: 1,
            writer_seq: seq,
            object_size: payload.len() as u64,
            content_hash,
            sample_count: 1,
            series_count: 1,
            min_event_ts_ns: created_unix_ns - 1_000,
            max_event_ts_ns: created_unix_ns,
            min_ingest_ts_ns: created_unix_ns - 1_000,
            max_ingest_ts_ns: created_unix_ns,
            segment_format_version: 1,
            created_unix_ns,
            ingest_hour_bucket,
        })
        .expect("valid record");
        let data_key = keys::reconstruct_data_key(&rec).expect("data key");
        publish::put_data_object(store, &data_key, Bytes::from(payload))
            .await
            .expect("put data object");
        publish::publish(store, &rec, &RetryPolicy::default())
            .await
            .expect("publish");
        rec
    }

    /// Publish an L1 compaction record over `inputs` into `(shard 0,
    /// ingest_hour_bucket)`, contributing one L1 part. The record supersedes
    /// its inputs, so the fold drops those L0 entries from the snapshot and
    /// folds this part in as a level-1 entry (32-byte `input_set_hash` in the
    /// writer_id slot). Only the record object is written; the fold reads the
    /// record, not the L1 part object.
    async fn publish_compaction(
        store: &MemoryStore,
        tenant: &str,
        ingest_hour_bucket: u32,
        inputs: &[&ravel_proto::commit::v1::CommitRecord],
        created_unix_ns: i64,
    ) {
        // The canonical hash, exactly as a real compactor computes it: a
        // fixture with any other value is the mismatch fixture below, not
        // this one.
        publish_compaction_with_hash(
            store,
            tenant,
            ingest_hour_bucket,
            inputs,
            created_unix_ns,
            None,
        )
        .await;
    }

    /// Same as [`publish_compaction`], but `forged_hash` (when `Some`)
    /// overrides the stored `input_set_hash` with a value that does not
    /// match the canonical hash of `inputs` -- the mismatch fixture for
    /// [`compaction_record_with_mismatched_input_set_hash_fails_loudly`].
    async fn publish_compaction_with_hash(
        store: &MemoryStore,
        tenant: &str,
        ingest_hour_bucket: u32,
        inputs: &[&ravel_proto::commit::v1::CommitRecord],
        created_unix_ns: i64,
        forged_hash: Option<[u8; 32]>,
    ) -> String {
        let mut record = compaction_record(tenant, ingest_hour_bucket, inputs, created_unix_ns);
        if let Some(forged) = forged_hash {
            record.input_set_hash = forged.to_vec();
        }
        put_compaction_record(store, &record).await
    }

    /// A version 1 compaction record over `inputs` in `(shard 0,
    /// ingest_hour_bucket)` with one L1 part and the canonical version 1 hash.
    fn compaction_record(
        tenant: &str,
        ingest_hour_bucket: u32,
        inputs: &[&ravel_proto::commit::v1::CommitRecord],
        created_unix_ns: i64,
    ) -> ravel_proto::commit::v1::CompactionRecord {
        use ravel_commit::signal;
        use ravel_proto::commit::v1::{CompactionInputIdentity, CompactionPart, CompactionRecord};

        let tenant_hash = TenantId::new(tenant).hash();
        let input_ids: Vec<CompactionInputIdentity> = inputs
            .iter()
            .map(|r| CompactionInputIdentity {
                writer_id: r.writer_id.clone(),
                writer_epoch: r.writer_epoch,
                writer_seq: r.writer_seq,
            })
            .collect();
        let input_set_hash = compute_compaction_input_set_hash(&input_ids);
        let part_payload = format!("l1-{ingest_hour_bucket}").into_bytes();
        let part_content_hash = *blake3::hash(&part_payload).as_bytes();
        let part = CompactionPart {
            part_index: 0,
            first_series_id: vec![0u8; 16],
            last_series_id: vec![0xffu8; 16],
            content_hash: part_content_hash.to_vec(),
            object_size: part_payload.len() as u64,
            sample_count: 1,
            series_count: 1,
            run_count: 1,
            min_event_ts_ns: created_unix_ns - 1_000,
            max_event_ts_ns: created_unix_ns,
            segment_format_version: 3,
            declared_column_stats: Vec::new(),
        };
        CompactionRecord {
            format_version: 1,
            tenant_hash: tenant_hash.0.to_vec(),
            signal: signal::to_proto(Signal::Metrics).into(),
            shard: 0,
            ingest_hour_bucket,
            level: 1,
            inputs: input_ids,
            input_set_hash: input_set_hash.to_vec(),
            parts: vec![part],
            created_unix_ns,
            superseded_record_key: String::new(),
        }
    }

    /// A version 2 record re-encoding `predecessor`: same bucket and inputs,
    /// the predecessor's key in `superseded_record_key`, and the version 2
    /// hash over both.
    fn superseding_compaction_record(
        predecessor: ravel_proto::commit::v1::CompactionRecord,
    ) -> ravel_proto::commit::v1::CompactionRecord {
        let superseded_record_key =
            keys::compaction_record_key_for(&predecessor).expect("predecessor key");
        let input_set_hash = ravel_commit::erasure::compute_superseding_compaction_input_set_hash(
            &predecessor.inputs,
            &superseded_record_key,
        );
        ravel_proto::commit::v1::CompactionRecord {
            format_version: record::COMPACTION_SUPERSEDING_FORMAT_VERSION,
            input_set_hash: input_set_hash.to_vec(),
            superseded_record_key,
            created_unix_ns: predecessor.created_unix_ns + 1,
            ..predecessor
        }
    }

    /// Write `record` at its own key, bypassing encode-side validation so a
    /// fixture can store a record the decoder refuses.
    async fn put_compaction_record(
        store: &MemoryStore,
        record: &ravel_proto::commit::v1::CompactionRecord,
    ) -> String {
        let key = keys::compaction_record_key_for(record).expect("compaction key");
        store
            .put(
                &key,
                Bytes::from(record.encode_to_vec()),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("put compaction record");
        key
    }

    async fn fold(store: Arc<dyn ObjectStoreBackend>, tenant: &str, now_ns: i64) {
        let tenant_hash = TenantId::new(tenant).hash();
        let catalog = crate::Catalog::new(
            store,
            crate::CatalogConfig {
                shard_count: 1,
                ..crate::CatalogConfig::default()
            },
        )
        .expect("catalog")
        .with_provisioning_enforcement();
        catalog
            .fold(
                &tenant_hash,
                Signal::Metrics,
                Uuid::new_v4(),
                now_ns,
                &[],
                None,
            )
            .await
            .expect("fold");
    }

    #[tokio::test]
    async fn clean_snapshot_reports_no_divergence() {
        let store = Arc::new(MemoryStore::new());
        let tenant = "clean";
        let now = 600_000 * NS_PER_HOUR;
        let created = now - SEALED_AGE_NS;
        publish_segment(store.as_ref(), tenant, 1, created).await;
        publish_segment(store.as_ref(), tenant, 2, created).await;
        fold(store.clone(), tenant, now).await;

        let report = verify_seal_divergence(
            store.as_ref(),
            &TenantId::new(tenant).hash(),
            Signal::Metrics,
        )
        .await
        .expect("no read error")
        .expect("HEAD present after fold");
        assert!(!report.has_divergence(), "clean snapshot must not diverge");
        assert!(report.missing.is_empty());
        assert!(report.mismatched.is_empty());
        assert!(report.orphaned.is_empty());
        assert_eq!(report.sealed_record_count, 2);
        assert_eq!(report.snapshot_entry_count, 2);
    }

    /// Issue #1976: the catalog HEAD GET must surface a non-`NotFound`
    /// failure as a real error, not read as "nothing folded yet". Callers:
    /// `ravel-cli`'s `catalog verify` (`services/ravel-cli/src/catalog.rs`,
    /// which maps any `Err` through `anyhow` and exits nonzero) and
    /// `ravel_server::scrub::run_seal_divergence_tick`, which already logs
    /// any `Err` from this function at `error!` and skips the tick.
    #[tokio::test]
    async fn head_get_permanent_failure_surfaces_as_error() {
        let inner = Arc::new(MemoryStore::new());
        let tenant = "head-fault";
        let now = 600_000 * NS_PER_HOUR;
        let created = now - SEALED_AGE_NS;
        publish_segment(inner.as_ref(), tenant, 1, created).await;
        fold(inner.clone(), tenant, now).await;

        let key = head_key(&TenantId::new(tenant).hash(), Signal::Metrics);
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::Get,
                ScriptedFault::Permanent("simulated AccessDenied on idx/*".into()),
            )
            .with_key_contains(key.clone()),
        );
        let faulty = FaultStore::new(inner.clone(), plan);

        let err = verify_seal_divergence(&faulty, &TenantId::new(tenant).hash(), Signal::Metrics)
            .await
            .expect_err("a non-NotFound GET failure on the HEAD must surface as an error");
        match err {
            SealDivergenceError::HeadFetch { key: err_key, .. } => {
                assert_eq!(err_key, key, "error names the failing key");
            }
            other => panic!("expected SealDivergenceError::HeadFetch, got {other:?}"),
        }
        assert!(
            faulty.fault_count(Op::Get, FaultKind::Permanent) >= 1,
            "the injected fault must actually have fired"
        );
    }

    /// The counterpart to `head_get_permanent_failure_surfaces_as_error`: a
    /// genuine `NotFound` on the HEAD (modeled with `NotFoundBlip`) is the
    /// ordinary "nothing folded yet" case and must still degrade quietly to
    /// `Ok(None)`, exactly as before this fix.
    #[tokio::test]
    async fn head_get_not_found_blip_still_degrades_to_none() {
        let inner = Arc::new(MemoryStore::new());
        let tenant = "head-fault-blip";
        let now = 600_000 * NS_PER_HOUR;
        let created = now - SEALED_AGE_NS;
        publish_segment(inner.as_ref(), tenant, 1, created).await;
        fold(inner.clone(), tenant, now).await;

        let key = head_key(&TenantId::new(tenant).hash(), Signal::Metrics);
        let plan = FaultPlan::empty()
            .with_rule(Rule::new(Op::Get, ScriptedFault::NotFoundBlip).with_key_contains(key));
        let faulty = FaultStore::new(inner.clone(), plan);

        let report =
            verify_seal_divergence(&faulty, &TenantId::new(tenant).hash(), Signal::Metrics)
                .await
                .expect("a NotFound on the HEAD must degrade to Ok(None), never an error");
        assert!(report.is_none());
        assert!(
            faulty.fault_count(Op::Get, FaultKind::NotFoundBlip) >= 1,
            "the injected fault must actually have fired"
        );
    }

    #[tokio::test]
    async fn l1_compaction_entry_is_verifiable_and_not_missing() {
        // Regression for issue #819. An L1 compaction over a sealed tenant
        // leaves the snapshot carrying a level-1 entry whose writer_id slot
        // holds the 32-byte input_set_hash, and leaves the superseded L0
        // commit record on the store until a later sweep. `catalog verify`
        // must (1) not reject the 32-byte writer_id as malformed, and (2) not
        // report the superseded L0 record as missing from the snapshot.
        let store = Arc::new(MemoryStore::new());
        let tenant = "compacted";
        let now = 600_000 * NS_PER_HOUR;
        let created = now - SEALED_AGE_NS;
        let hour = u32::try_from(created / NS_PER_HOUR).expect("fits u32");

        let l0 = publish_segment(store.as_ref(), tenant, 1, created).await;
        publish_compaction(store.as_ref(), tenant, hour, &[&l0], created).await;
        fold(store.clone(), tenant, now).await;

        let report = verify_seal_divergence(
            store.as_ref(),
            &TenantId::new(tenant).hash(),
            Signal::Metrics,
        )
        .await
        .expect("an L1 snapshot entry must not be a malformed writer_id (issue #819)")
        .expect("HEAD present after fold");

        assert!(
            !report.has_divergence(),
            "a superseded L0 record folded into an L1 part is not a divergence"
        );
        assert!(
            report.missing.is_empty(),
            "the superseded L0 record must not be reported missing"
        );
        assert!(report.mismatched.is_empty());
        // The L1 part is counted in the snapshot but not compared; the
        // superseded L0 record is excluded from the ground truth, so nothing
        // is diffed and nothing is orphaned.
        assert_eq!(report.snapshot_entry_count, 1, "the L1 part is counted");
        assert_eq!(
            report.sealed_record_count, 0,
            "the superseded L0 record is excluded from the ground truth"
        );
        assert!(report.orphaned.is_empty());
    }

    #[tokio::test]
    async fn l1_compaction_present_does_not_hide_unrelated_missing_record() {
        // The exclusion set built from compaction/rewrite `inputs` must only
        // ever shrink the ground truth by the records those inputs actually
        // name. An L0 record with no relation to any compaction must still be
        // caught as missing, even while an unrelated L1 compaction entry sits
        // in the same snapshot (issue #819 regression: a broad exclusion set
        // would silently swallow this).
        let store = Arc::new(MemoryStore::new());
        let tenant = "compacted-plus-undercount";
        let now = 600_000 * NS_PER_HOUR;
        let created = now - SEALED_AGE_NS;
        let hour = u32::try_from(created / NS_PER_HOUR).expect("fits u32");

        let compacted_l0 = publish_segment(store.as_ref(), tenant, 1, created).await;
        publish_compaction(store.as_ref(), tenant, hour, &[&compacted_l0], created).await;
        fold(store.clone(), tenant, now).await;
        // Sealed but published after the fold, and never part of any
        // compaction: the snapshot under-counts this one specifically.
        publish_segment(store.as_ref(), tenant, 2, created).await;

        let report = verify_seal_divergence(
            store.as_ref(),
            &TenantId::new(tenant).hash(),
            Signal::Metrics,
        )
        .await
        .expect("no read error")
        .expect("HEAD present");
        assert!(report.has_divergence());
        assert_eq!(
            report.missing.len(),
            1,
            "the unrelated post-fold record is missing, despite the L1 entry present"
        );
        assert!(report.mismatched.is_empty());
        assert!(report.orphaned.is_empty());
    }

    #[tokio::test]
    async fn l1_compaction_present_does_not_hide_unrelated_orphaned_entry() {
        // Mirrors the missing-record case above for the orphaned side: an L0
        // snapshot entry unrelated to any compaction, whose backing commit
        // record retention has since deleted, must still be reported
        // orphaned even while an unrelated L1 compaction entry sits in the
        // same snapshot.
        let store = Arc::new(MemoryStore::new());
        let tenant = "compacted-plus-orphan";
        let now = 600_000 * NS_PER_HOUR;
        let created = now - SEALED_AGE_NS;
        let hour = u32::try_from(created / NS_PER_HOUR).expect("fits u32");

        let compacted_l0 = publish_segment(store.as_ref(), tenant, 1, created).await;
        publish_compaction(store.as_ref(), tenant, hour, &[&compacted_l0], created).await;
        let uncompacted_l0 = publish_segment(store.as_ref(), tenant, 2, created).await;
        fold(store.clone(), tenant, now).await;

        // Retention deletes the uncompacted record's commit record once its
        // snapshot entry is folded in. The compacted record's own commit
        // record is left alone here (a later sweep would remove it), so it
        // stays excluded rather than becoming a second orphan.
        let tenant_hash = TenantId::new(tenant).hash();
        let key = keys::commit_key_for_record(&uncompacted_l0).expect("commit key");
        store.delete(&key).await.expect("delete record");

        let report = verify_seal_divergence(store.as_ref(), &tenant_hash, Signal::Metrics)
            .await
            .expect("no read error")
            .expect("HEAD present");
        assert!(
            !report.has_divergence(),
            "orphaned entries must never count as a divergence"
        );
        assert_eq!(
            report.orphaned.len(),
            1,
            "the unrelated deleted record is orphaned, despite the L1 entry present"
        );
        assert!(report.missing.is_empty());
        assert!(report.mismatched.is_empty());
    }

    #[tokio::test]
    async fn compaction_record_with_mismatched_input_set_hash_fails_loudly() {
        // Issue #830. A compaction record whose declared `input_set_hash`
        // does not correspond to its own `inputs` must not be trusted to
        // suppress those inputs from the ground truth: `verify_seal_divergence`
        // must fail loud, naming the record, rather than silently excluding
        // real L0 entries because a forged or corrupt record said so.
        let store = Arc::new(MemoryStore::new());
        let tenant = "forged-hash";
        let now = 600_000 * NS_PER_HOUR;
        let created = now - SEALED_AGE_NS;
        let hour = u32::try_from(created / NS_PER_HOUR).expect("fits u32");

        let l0 = publish_segment(store.as_ref(), tenant, 1, created).await;
        let forged = [0x42u8; 32];
        let record_key = publish_compaction_with_hash(
            store.as_ref(),
            tenant,
            hour,
            &[&l0],
            created,
            Some(forged),
        )
        .await;
        fold(store.clone(), tenant, now).await;

        let tenant_hash = TenantId::new(tenant).hash();
        let err = verify_seal_divergence(store.as_ref(), &tenant_hash, Signal::Metrics)
            .await
            .expect_err(
                "a compaction record whose input_set_hash disagrees with its own \
                         inputs must be a hard error, not a divergence result",
            );
        match &err {
            SealDivergenceError::CompactionInputSetHashMismatch {
                key,
                declared,
                computed,
            } => {
                assert_eq!(key, &record_key, "the error must name the offending record");
                assert_eq!(declared, &hex::encode(forged));
                assert_ne!(
                    declared, computed,
                    "the whole point of the fixture is that these disagree"
                );
            }
            other => panic!("expected CompactionInputSetHashMismatch, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn valid_version_two_compaction_record_passes() {
        let store = Arc::new(MemoryStore::new());
        let tenant = "superseding";
        let now = 600_000 * NS_PER_HOUR;
        let created = now - SEALED_AGE_NS;
        let hour = u32::try_from(created / NS_PER_HOUR).expect("fits u32");

        let l0 = publish_segment(store.as_ref(), tenant, 1, created).await;
        let v2 = superseding_compaction_record(compaction_record(tenant, hour, &[&l0], created));
        put_compaction_record(store.as_ref(), &v2).await;
        fold(store.clone(), tenant, now).await;

        let report = verify_seal_divergence(
            store.as_ref(),
            &TenantId::new(tenant).hash(),
            Signal::Metrics,
        )
        .await
        .expect("a valid version 2 record's stored hash is the version 2 hash")
        .expect("HEAD present after fold");
        assert!(!report.has_divergence());
        assert!(report.missing.is_empty());
        assert!(report.mismatched.is_empty());
        assert!(report.orphaned.is_empty());
        assert_eq!(report.snapshot_entry_count, 1, "the L1 part is counted");
        assert_eq!(
            report.sealed_record_count, 0,
            "the version 2 record's input is excluded from the ground truth"
        );
    }

    #[tokio::test]
    async fn version_two_compaction_record_carrying_the_version_one_hash_is_refused() {
        let store = Arc::new(MemoryStore::new());
        let tenant = "superseding-v1-hash";
        let now = 600_000 * NS_PER_HOUR;
        let created = now - SEALED_AGE_NS;
        let hour = u32::try_from(created / NS_PER_HOUR).expect("fits u32");

        let l0 = publish_segment(store.as_ref(), tenant, 1, created).await;
        fold(store.clone(), tenant, now).await;
        let mut v2 =
            superseding_compaction_record(compaction_record(tenant, hour, &[&l0], created));
        v2.input_set_hash = compute_compaction_input_set_hash(&v2.inputs).to_vec();
        let record_key = put_compaction_record(store.as_ref(), &v2).await;

        let err = verify_seal_divergence(
            store.as_ref(),
            &TenantId::new(tenant).hash(),
            Signal::Metrics,
        )
        .await
        .expect_err("a version 2 record carrying the version 1 hash must not pass");
        match &err {
            SealDivergenceError::CompactionRecordCorrupt { key, source } => {
                assert_eq!(key, &record_key);
                assert_eq!(source, &RecordError::SupersedingInputSetHashMismatch);
            }
            other => panic!("expected CompactionRecordCorrupt, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn absent_head_returns_none() {
        let store = Arc::new(MemoryStore::new());
        let report = verify_seal_divergence(
            store.as_ref(),
            &TenantId::new("empty").hash(),
            Signal::Metrics,
        )
        .await
        .expect("no read error");
        assert!(
            report.is_none(),
            "no HEAD yet must be None, not a divergence"
        );
    }

    #[tokio::test]
    async fn record_sealed_after_fold_is_missing() {
        let store = Arc::new(MemoryStore::new());
        let tenant = "under-count";
        let now = 600_000 * NS_PER_HOUR;
        let created = now - SEALED_AGE_NS;
        publish_segment(store.as_ref(), tenant, 1, created).await;
        fold(store.clone(), tenant, now).await;
        // Sealed but published after the fold: the snapshot under-counts.
        publish_segment(store.as_ref(), tenant, 2, created).await;

        let report = verify_seal_divergence(
            store.as_ref(),
            &TenantId::new(tenant).hash(),
            Signal::Metrics,
        )
        .await
        .expect("no read error")
        .expect("HEAD present");
        assert!(report.has_divergence());
        assert_eq!(report.missing.len(), 1, "the post-fold record is missing");
        assert!(report.mismatched.is_empty());
        assert!(report.orphaned.is_empty());
    }

    #[tokio::test]
    async fn deleted_commit_record_is_orphaned_not_a_failure() {
        let store = Arc::new(MemoryStore::new());
        let tenant = "retention";
        let now = 600_000 * NS_PER_HOUR;
        let created = now - SEALED_AGE_NS;
        publish_segment(store.as_ref(), tenant, 1, created).await;
        publish_segment(store.as_ref(), tenant, 2, created).await;
        fold(store.clone(), tenant, now).await;

        // Delete every sealed commit record, as retention does once its entry
        // is folded in: the snapshot entries become orphaned ground-truth-side.
        let tenant_hash = TenantId::new(tenant).hash();
        let prefix = keys::commit_shard_prefix(&tenant_hash, Signal::Metrics, 0).expect("prefix");
        for object in ravel_object_store::list_all(store.as_ref(), &prefix)
            .await
            .expect("list")
        {
            if keys::parse_commit_key(&object.key).is_ok() {
                store.delete(&object.key).await.expect("delete record");
            }
        }

        let report = verify_seal_divergence(store.as_ref(), &tenant_hash, Signal::Metrics)
            .await
            .expect("no read error")
            .expect("HEAD present");
        assert!(
            !report.has_divergence(),
            "orphaned entries must never count as a divergence"
        );
        assert_eq!(report.orphaned.len(), 2, "both folded entries are orphaned");
        assert!(report.missing.is_empty());
        assert!(report.mismatched.is_empty());
    }

    #[tokio::test]
    async fn corrupt_head_is_a_read_error_not_a_divergence() {
        let store = Arc::new(MemoryStore::new());
        let tenant_hash = TenantId::new("corrupt").hash();
        store
            .put(
                &head_key(&tenant_hash, Signal::Metrics),
                Bytes::from_static(b"not a valid HEAD"),
                PutOptions::default(),
            )
            .await
            .expect("seed corrupt HEAD");
        let err = verify_seal_divergence(store.as_ref(), &tenant_hash, Signal::Metrics)
            .await
            .expect_err("a corrupt HEAD must be a typed read error");
        assert!(matches!(err, SealDivergenceError::HeadCorrupt { .. }));
    }

    fn identity(rec: &ravel_proto::commit::v1::CommitRecord) -> EntryIdentity {
        (
            rec.shard,
            rec.ingest_hour_bucket,
            *Uuid::parse_str(&rec.writer_id).expect("uuid").as_bytes(),
            rec.writer_epoch,
            rec.writer_seq,
        )
    }

    async fn coverage(
        store: &dyn ObjectStoreBackend,
        tenant: &str,
        ids: &[EntryIdentity],
    ) -> Result<SnapshotCoverage, SealDivergenceError> {
        snapshot_coverage(store, &TenantId::new(tenant).hash(), Signal::Metrics, ids).await
    }

    /// The first part key the tenant's HEAD references.
    async fn first_part_key(store: &MemoryStore, tenant: &str) -> String {
        let key = head_key(&TenantId::new(tenant).hash(), Signal::Metrics);
        let got = store.get(&key, GetRange::Full).await.expect("HEAD");
        let head = decode_head(&got.data).expect("decodes");
        head.parts.first().expect("one part").key.clone()
    }

    /// A level-0 entry covers its commit, and a commit published after the
    /// fold is missing. Only the missing commit's bucket is listed; a set the
    /// level-0 entries cover lists nothing.
    #[tokio::test]
    async fn coverage_finds_level0_entries_and_names_the_unfolded_commit() {
        let store = Arc::new(MemoryStore::new());
        let tenant = "coverage-l0";
        let now = 600_000 * NS_PER_HOUR;
        let created = now - SEALED_AGE_NS;
        let folded = publish_segment(store.as_ref(), tenant, 1, created).await;
        fold(store.clone(), tenant, now).await;
        let late = publish_segment(store.as_ref(), tenant, 2, created).await;

        let ids = [identity(&folded), identity(&late), identity(&folded)];
        let found = coverage(store.as_ref(), tenant, &ids)
            .await
            .expect("readable");
        assert_eq!(found.missing, vec![identity(&late)]);
        assert_eq!(found.watermark_hour, Some(folded.ingest_hour_bucket));
        assert_eq!(found.parts_read, 1);
        assert_eq!(found.buckets_listed, 1);
        assert_eq!(found.records_read, 0);

        let covered = coverage(store.as_ref(), tenant, &[identity(&folded)])
            .await
            .expect("readable");
        assert!(covered.missing.is_empty(), "{covered:?}");
        assert_eq!(covered.buckets_listed, 0, "nothing to look up past level 0");
    }

    /// A commit a compaction record superseded is not a level-0 entry of the
    /// snapshot, and is covered by the level-1 part the snapshot holds.
    #[tokio::test]
    async fn coverage_counts_a_commit_a_held_compaction_supersedes() {
        let store = Arc::new(MemoryStore::new());
        let tenant = "coverage-l1";
        let now = 600_000 * NS_PER_HOUR;
        let created = now - SEALED_AGE_NS;
        let hour = u32::try_from(created / NS_PER_HOUR).expect("fits u32");
        let compacted = publish_segment(store.as_ref(), tenant, 1, created).await;
        let raw = publish_segment(store.as_ref(), tenant, 2, created).await;
        publish_compaction(store.as_ref(), tenant, hour, &[&compacted], created).await;
        fold(store.clone(), tenant, now).await;

        let found = coverage(
            store.as_ref(),
            tenant,
            &[identity(&compacted), identity(&raw)],
        )
        .await
        .expect("readable");
        assert!(
            found.missing.is_empty(),
            "the compacted commit is covered by the level-1 part: {found:?}"
        );
        assert_eq!(found.buckets_listed, 1);
        assert_eq!(found.records_read, 1);
    }

    /// A rewrite record over `inputs` in `(shard 0, ingest_hour_bucket)` with
    /// one output part, in the input order and hash `validate_rewrite` checks.
    async fn publish_rewrite(
        store: &MemoryStore,
        tenant: &str,
        ingest_hour_bucket: u32,
        inputs: &[&ravel_proto::commit::v1::CommitRecord],
        created_unix_ns: i64,
    ) {
        use ravel_commit::{erasure, signal};
        use ravel_proto::commit::v1::{CompactionInputIdentity, CompactionPart, RewriteDrop};

        let mut input_ids: Vec<CompactionInputIdentity> = inputs
            .iter()
            .map(|r| CompactionInputIdentity {
                writer_id: r.writer_id.clone(),
                writer_epoch: r.writer_epoch,
                writer_seq: r.writer_seq,
            })
            .collect();
        input_ids.sort_by(|a, b| {
            (a.writer_id.as_str(), a.writer_epoch, a.writer_seq).cmp(&(
                b.writer_id.as_str(),
                b.writer_epoch,
                b.writer_seq,
            ))
        });
        let request_ids = vec![Uuid::new_v4().to_string()];
        let input_set_hash =
            erasure::compute_rewrite_input_set_hash(&input_ids, None, &request_ids).to_vec();
        let part_payload = format!("rw-{ingest_hour_bucket}").into_bytes();
        let part = CompactionPart {
            part_index: 0,
            first_series_id: vec![0u8; 16],
            last_series_id: vec![0xffu8; 16],
            content_hash: blake3::hash(&part_payload).as_bytes().to_vec(),
            object_size: part_payload.len() as u64,
            sample_count: 1,
            series_count: 1,
            run_count: 1,
            min_event_ts_ns: created_unix_ns - 1_000,
            max_event_ts_ns: created_unix_ns,
            segment_format_version: 3,
            declared_column_stats: Vec::new(),
        };
        let record = RewriteRecord {
            format_version: 1,
            tenant_hash: TenantId::new(tenant).hash().0.to_vec(),
            signal: signal::to_proto(Signal::Metrics).into(),
            shard: 0,
            ingest_hour_bucket,
            inputs: input_ids,
            input_set_hash,
            parts: vec![part],
            drops: request_ids
                .iter()
                .map(|request_id| RewriteDrop {
                    request_id: request_id.clone(),
                    dropped_count: 1,
                })
                .collect(),
            created_unix_ns,
            superseded_record_key: String::new(),
        };
        let key = keys::rewrite_record_key_for(&record).expect("rewrite key");
        store
            .put(
                &key,
                erasure::encode_rewrite(&record),
                PutOptions::default(),
            )
            .await
            .expect("put rewrite record");
    }

    /// A commit a rewrite record superseded is covered by the rewrite's part
    /// the snapshot holds, and not by a rewrite published after the fold.
    #[tokio::test]
    async fn coverage_counts_a_commit_a_held_rewrite_supersedes() {
        let store = Arc::new(MemoryStore::new());
        let tenant = "coverage-rewrite";
        let now = 600_000 * NS_PER_HOUR;
        let created = now - SEALED_AGE_NS;
        let hour = u32::try_from(created / NS_PER_HOUR).expect("fits u32");
        let rewritten = publish_segment(store.as_ref(), tenant, 1, created).await;
        publish_rewrite(store.as_ref(), tenant, hour, &[&rewritten], created).await;
        fold(store.clone(), tenant, now).await;
        let late = publish_segment(store.as_ref(), tenant, 2, created).await;
        publish_rewrite(store.as_ref(), tenant, hour, &[&late], created).await;

        let found = coverage(
            store.as_ref(),
            tenant,
            &[identity(&rewritten), identity(&late)],
        )
        .await
        .expect("readable");
        assert_eq!(
            found.missing,
            vec![identity(&late)],
            "only the rewrite the snapshot holds covers its input: {found:?}"
        );
        assert_eq!(found.buckets_listed, 1);
        assert_eq!(found.records_read, 2);
    }

    /// A compaction record on the store whose parts the snapshot does not
    /// hold covers nothing, even beside a held compaction in the same
    /// `(shard, hour)`: a level-1 entry covers only the record whose
    /// `input_set_hash` it carries.
    #[tokio::test]
    async fn coverage_ignores_a_compaction_the_snapshot_does_not_hold() {
        let store = Arc::new(MemoryStore::new());
        let tenant = "coverage-unheld";
        let now = 600_000 * NS_PER_HOUR;
        let created = now - SEALED_AGE_NS;
        let hour = u32::try_from(created / NS_PER_HOUR).expect("fits u32");
        let compacted = publish_segment(store.as_ref(), tenant, 1, created).await;
        publish_compaction(store.as_ref(), tenant, hour, &[&compacted], created).await;
        fold(store.clone(), tenant, now).await;
        let late = publish_segment(store.as_ref(), tenant, 2, created).await;
        publish_compaction(store.as_ref(), tenant, hour, &[&late], created).await;

        let found = coverage(
            store.as_ref(),
            tenant,
            &[identity(&compacted), identity(&late)],
        )
        .await
        .expect("readable");
        assert_eq!(
            found.missing,
            vec![identity(&late)],
            "only the compaction the snapshot holds covers its input: {found:?}"
        );
        assert_eq!(found.buckets_listed, 1);
        assert_eq!(found.records_read, 2, "both compaction records were read");
    }

    /// No HEAD: nothing is covered.
    #[tokio::test]
    async fn coverage_without_a_head_covers_nothing() {
        let store = Arc::new(MemoryStore::new());
        let tenant = "coverage-no-head";
        let created = 600_000 * NS_PER_HOUR - SEALED_AGE_NS;
        let rec = publish_segment(store.as_ref(), tenant, 1, created).await;
        let found = coverage(store.as_ref(), tenant, &[identity(&rec)])
            .await
            .expect("an absent HEAD is not a read error");
        assert_eq!(found.missing, vec![identity(&rec)]);
        assert_eq!(found.watermark_hour, None);
    }

    /// A part that cannot be fetched, or does not decode, is an error: the
    /// coverage of the commits it may hold was not checked.
    #[tokio::test]
    async fn coverage_with_an_unreadable_part_is_an_error() {
        let inner = Arc::new(MemoryStore::new());
        let tenant = "coverage-bad-part";
        let now = 600_000 * NS_PER_HOUR;
        let created = now - SEALED_AGE_NS;
        let rec = publish_segment(inner.as_ref(), tenant, 1, created).await;
        fold(inner.clone(), tenant, now).await;
        let part_key = first_part_key(inner.as_ref(), tenant).await;

        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Get, ScriptedFault::Permanent("part unreadable".into()))
                .with_key_contains(part_key.clone()),
        );
        let faulty = FaultStore::new(inner.clone(), plan);
        let err = coverage(&faulty, tenant, &[identity(&rec)])
            .await
            .expect_err("an unreadable part is not coverage");
        assert!(
            matches!(&err, SealDivergenceError::PartFetch { key, .. } if *key == part_key),
            "{err:?}"
        );
        assert_eq!(faulty.fault_count(Op::Get, FaultKind::Permanent), 1);

        inner
            .put(
                &part_key,
                Bytes::from_static(b"not a part"),
                PutOptions::default(),
            )
            .await
            .expect("overwrite part");
        let err = coverage(inner.as_ref(), tenant, &[identity(&rec)])
            .await
            .expect_err("a corrupt part is not coverage");
        assert!(
            matches!(err, SealDivergenceError::PartCorrupt { .. }),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn coverage_with_a_corrupt_head_is_an_error() {
        let store = Arc::new(MemoryStore::new());
        let tenant = "coverage-bad-head";
        let tenant_hash = TenantId::new(tenant).hash();
        store
            .put(
                &head_key(&tenant_hash, Signal::Metrics),
                Bytes::from_static(b"not a valid HEAD"),
                PutOptions::default(),
            )
            .await
            .expect("seed corrupt HEAD");
        let err = coverage(store.as_ref(), tenant, &[(0, 1, [0; 16], 1, 1)])
            .await
            .expect_err("a corrupt HEAD is not coverage");
        assert!(
            matches!(err, SealDivergenceError::HeadCorrupt { .. }),
            "{err:?}"
        );
    }
}
