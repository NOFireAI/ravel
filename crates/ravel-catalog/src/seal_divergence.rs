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

use std::collections::{BTreeMap, HashSet};

use prost::Message;
use ravel_commit::keys::{self, KeyError};
use ravel_commit::record::{self, RecordError};
use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError};
use ravel_proto::commit::v1::CompactionRecord;
use ravel_types::{Signal, TenantHash};
use uuid::Uuid;

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
        source: prost::DecodeError,
    },
    #[error(
        "compaction record at {key} declares input_set_hash {declared} but its \
         inputs hash to {computed}"
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
/// verify): fetching the HEAD failed with a store error, exactly the "nothing
/// folded yet" case the CLI has always treated as success. `Ok(Some(report))`
/// carries the classified diff (see [`SealDivergenceReport`]). `Err` is a
/// read/decode failure of the objects the comparison needs, never the presence
/// of a divergence.
pub async fn verify_seal_divergence(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
    signal: Signal,
) -> Result<Option<SealDivergenceReport>, SealDivergenceError> {
    let key = head_key(tenant_hash, signal);

    // A store error fetching HEAD means nothing has been folded yet (or the
    // HEAD is unreadable): there is no snapshot to verify against. This is the
    // "nothing folded yet, nothing to verify" case, not a divergence.
    let head_bytes = match store.get(&key, GetRange::Full).await {
        Ok(outcome) => outcome.data,
        Err(_) => return Ok(None),
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
    let limits = PartLimits::default();
    let mut snapshot_entries: BTreeMap<EntryIdentity, Vec<u8>> = BTreeMap::new();
    let mut snapshot_entry_count = 0usize;
    for part_ref in &head.parts {
        let got = store
            .get(&part_ref.key, GetRange::Full)
            .await
            .map_err(|source| SealDivergenceError::PartFetch {
                key: part_ref.key.clone(),
                source,
            })?;
        let decoded =
            decode_part(&got.data, &limits).map_err(|source| SealDivergenceError::PartCorrupt {
                key: part_ref.key.clone(),
                source,
            })?;
        for entry in decoded.entries {
            snapshot_entry_count += 1;
            if entry.level != 0 {
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
            snapshot_entries.insert(identity, entry.content_hash);
        }
    }

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
        let mut superseded: HashSet<(String, u64, u64)> = HashSet::new();
        for object in &objects {
            if keys::parse_compaction_record_key(&object.key).is_ok() {
                let got = store
                    .get(&object.key, GetRange::Full)
                    .await
                    .map_err(|source| SealDivergenceError::RecordFetch {
                        key: object.key.clone(),
                        source,
                    })?;
                let rec = CompactionRecord::decode(got.data.as_ref()).map_err(|source| {
                    SealDivergenceError::CompactionRecordCorrupt {
                        key: object.key.clone(),
                        source,
                    }
                })?;
                // A compaction record's declared `input_set_hash` must match
                // the canonical hash of its own `inputs`: every input named
                // here is removed from the ground-truth set below, so a
                // record whose declared hash disagrees with its inputs could
                // make this check silently skip real L0 entries instead of
                // catching them as missing (issue #830). Recomputed the same
                // way `ravel-maintain`'s compaction loader derives the hash
                // it publishes, so a well-formed record always agrees.
                // A compaction record's declared `input_set_hash` must match
                // the canonical hash of its own `inputs`: every input named
                // here is removed from the ground-truth set below, so a
                // record whose declared hash disagrees with its inputs could
                // make this check silently skip real L0 entries instead of
                // catching them as missing (issue #830). Recomputed the same
                // way `ravel-maintain`'s compaction loader derives the hash
                // it publishes, so a well-formed record always agrees.
                let computed =
                    ravel_commit::erasure::compute_compaction_input_set_hash(&rec.inputs);
                if rec.input_set_hash.as_slice() != computed.as_slice() {
                    return Err(SealDivergenceError::CompactionInputSetHashMismatch {
                        key: object.key.clone(),
                        declared: hex::encode(&rec.input_set_hash),
                        computed: hex::encode(computed),
                    });
                }
                for input in rec.inputs {
                    superseded.insert((input.writer_id, input.writer_epoch, input.writer_seq));
                }
            } else if keys::parse_rewrite_record_key(&object.key).is_ok() {
                let got = store
                    .get(&object.key, GetRange::Full)
                    .await
                    .map_err(|source| SealDivergenceError::RecordFetch {
                        key: object.key.clone(),
                        source,
                    })?;
                let rec = ravel_commit::erasure::decode_rewrite(&got.data).map_err(|source| {
                    SealDivergenceError::RewriteRecordCorrupt {
                        key: object.key.clone(),
                        source,
                    }
                })?;
                for input in rec.inputs {
                    superseded.insert((input.writer_id, input.writer_epoch, input.writer_seq));
                }
            }
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

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use bytes::Bytes;
    use ravel_commit::publish::{self, RetryPolicy};
    use ravel_commit::record::NewCommitRecord;
    use ravel_object_store::PutOptions;
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
    ///
    /// `tamper_hash`: when true, stores a `input_set_hash` that does not
    /// match `inputs` (a corrupt/malicious record), to exercise the
    /// [`SealDivergenceError::CompactionInputSetHashMismatch`] check
    /// (issue #830). Never set for a well-formed fixture.
    async fn publish_compaction(
        store: &MemoryStore,
        tenant: &str,
        ingest_hour_bucket: u32,
        inputs: &[&ravel_proto::commit::v1::CommitRecord],
        created_unix_ns: i64,
        tamper_hash: bool,
    ) {
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
        // The canonical hash: `verify_seal_divergence` now recomputes and
        // checks this (issue #830), so a well-formed test fixture must carry
        // the real thing, not a placeholder 32 bytes.
        let mut input_set_hash =
            ravel_commit::erasure::compute_compaction_input_set_hash(&input_ids);
        if tamper_hash {
            input_set_hash[0] ^= 0xff;
        }
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
        };
        let record = CompactionRecord {
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
        };
        let key = keys::compaction_record_key_for(&record).expect("compaction key");
        store
            .put(
                &key,
                Bytes::from(record.encode_to_vec()),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("put compaction record");
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
        publish_compaction(store.as_ref(), tenant, hour, &[&l0], created, false).await;
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
    async fn tampered_compaction_input_set_hash_fails_loud_not_quiet() {
        // Regression for issue #830. Before the fix, `verify_seal_divergence`
        // trusted a compaction record's `inputs` at face value: it built the
        // `superseded` set straight from `rec.inputs` and never checked that
        // `rec.input_set_hash` is actually the canonical hash of those inputs.
        // A record whose declared inputs don't match its hash could then make
        // this check silently exclude real L0 entries from the ground truth,
        // reporting a clean, no-divergence result it never actually
        // established -- verify going quiet instead of loud. This asserts the
        // opposite: a tampered record is a hard `Err`, not a clean report.
        let store = Arc::new(MemoryStore::new());
        let tenant = "tampered-compaction";
        let now = 600_000 * NS_PER_HOUR;
        let created = now - SEALED_AGE_NS;
        let hour = u32::try_from(created / NS_PER_HOUR).expect("fits u32");

        let l0 = publish_segment(store.as_ref(), tenant, 1, created).await;
        publish_compaction(store.as_ref(), tenant, hour, &[&l0], created, true).await;
        fold(store.clone(), tenant, now).await;

        let err = verify_seal_divergence(
            store.as_ref(),
            &TenantId::new(tenant).hash(),
            Signal::Metrics,
        )
        .await
        .expect_err(
            "a compaction record whose declared input_set_hash disagrees with its \
             inputs must be a hard error, not a silent clean report",
        );
        assert!(
            matches!(
                err,
                SealDivergenceError::CompactionInputSetHashMismatch { .. }
            ),
            "expected CompactionInputSetHashMismatch, got {err:?}"
        );
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
        publish_compaction(
            store.as_ref(),
            tenant,
            hour,
            &[&compacted_l0],
            created,
            false,
        )
        .await;
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
        publish_compaction(
            store.as_ref(),
            tenant,
            hour,
            &[&compacted_l0],
            created,
            false,
        )
        .await;
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
}
