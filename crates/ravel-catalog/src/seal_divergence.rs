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

use std::collections::BTreeMap;

use ravel_commit::keys::{self, KeyError};
use ravel_commit::record::{self, RecordError};
use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError};
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
    /// truth the snapshot is compared against).
    pub sealed_record_count: usize,
    /// Count of entries in the folded snapshot.
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

    // Decode the folded snapshot's entries into the shared identity shape.
    let limits = PartLimits::default();
    let mut snapshot_entries: BTreeMap<EntryIdentity, Vec<u8>> = BTreeMap::new();
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
        for object in objects {
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
        snapshot_entry_count: snapshot_entries.len(),
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

    async fn publish_segment(store: &MemoryStore, tenant: &str, seq: u64, created_unix_ns: i64) {
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
