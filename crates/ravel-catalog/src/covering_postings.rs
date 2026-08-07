//! Covering-postings load for the at-rest scrubber's postings tier (ADR-0059
//! decision 1, issue #708). The library scrubber
//! (`ravel_maintain::scrub::scrub_one_object`) already implements the S2-09
//! false-negative check against a covering name-postings object, but it takes
//! the postings bytes, the covered parts' blake3, and the covered-entry list
//! as inputs it cannot fetch itself. This module resolves those inputs from a
//! (tenant, signal)'s current folded snapshot HEAD, so the scheduled scrubber
//! (`ravel_server::scrub`) can wire the postings tier on.
//!
//! It is the natural next step after [`crate::seal_divergence`]: it does the
//! same metadata-cost HEAD/parts fetch (fetch HEAD, decode every referenced
//! part into a flat entry list, no data-object reads), then goes one step
//! further and also loads the postings object the HEAD references, following
//! the exact procedure `Catalog::load_snapshot_postings` uses on the query
//! path (GET, blake3 verify, ADR-0050 §2 tenant-hash check, decode against the
//! covered parts' blake3, entry-count check).
//!
//! # Degrade-to-None, one loud exception
//!
//! Postings are a pure pruning/verification optimization, never a correctness
//! gate: every failure short of an isolation breach degrades to `Ok(None)`
//! (no HEAD yet, no postings ref, any GET error, a blake3 mismatch, a decode
//! error, an entry-count mismatch), exactly as `load_snapshot_postings`
//! documents. The scrubber then simply runs the structural and content tiers
//! with no postings tier this tick and retries next tick. The two loud
//! exceptions returned as a real [`LoadPostingsError`] are a genuinely
//! unparseable HEAD or part (a real catalog defect, matching
//! [`crate::seal_divergence`]) and a postings object whose declared
//! `tenant_hash` names a different tenant (an isolation breach, ADR-0050 §2,
//! never absorbed into a silent degrade).

use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_proto::catalog::v1::SnapshotEntry;
use ravel_types::{Signal, TenantHash};

use crate::snapshot_format::{
    self, DEFAULT_MAX_POSTINGS_BYTES, PartLimits, PostingsLimits, SnapshotFormatError, decode_head,
    decode_part,
};

/// The owned covering-postings inputs the scrubber's postings tier needs, in
/// the shape `ravel_maintain::CoveringPostings<'a>` borrows from. Owned (not
/// borrowed) because the server-side caller loads this once per (tenant,
/// signal) per tick and must hold it alive across every shard's tick for that
/// signal, handing each shard a borrowing `CoveringPostings` built from these
/// fields.
#[derive(Clone, Debug)]
pub struct LoadedCoveringPostings {
    /// The RNP1 postings object's full bytes.
    pub bytes: Vec<u8>,
    /// The covered parts' blake3 hashes, in `SnapshotHead.parts` order (the
    /// binding `decode_postings` verifies).
    pub part_blake3: Vec<[u8; 32]>,
    /// Every covered part's entries, concatenated in `SnapshotHead.parts`
    /// order (the order postings ordinals index into).
    pub covered_entries: Vec<SnapshotEntry>,
}

/// A genuinely unparseable object or an isolation breach encountered while
/// loading covering postings: the only two conditions [`load_covering_postings`]
/// surfaces as an error rather than degrading to `Ok(None)`. Every other
/// failure mode (absent HEAD or postings ref, any GET error, a blake3 or
/// entry-count mismatch, a postings decode error) is a `Ok(None)` degrade, not
/// a variant here.
#[derive(Debug, thiserror::Error)]
pub enum LoadPostingsError {
    /// The HEAD object is present but does not decode: a real catalog defect,
    /// surfaced the same way [`crate::seal_divergence`] surfaces a corrupt HEAD.
    #[error("HEAD at {key} is corrupt: {source}")]
    HeadCorrupt {
        key: String,
        #[source]
        source: SnapshotFormatError,
    },
    /// A snapshot part the HEAD references is present but does not decode.
    #[error("snapshot part {key} is corrupt: {source}")]
    PartCorrupt {
        key: String,
        #[source]
        source: SnapshotFormatError,
    },
    /// The postings object declares a `tenant_hash` naming a different tenant
    /// (ADR-0050 §2 isolation breach): a hard error, never a silent degrade,
    /// matching `Catalog::load_snapshot_postings`.
    #[error(
        "postings object {key} declares tenant_hash {actual}, expected {expected} \
         (ADR-0050 §2 isolation breach)"
    )]
    TenantHashMismatch {
        key: String,
        expected: String,
        actual: String,
    },
}

/// HEAD object key (docs/catalog-and-mvcc.md key layout, frozen format).
/// Duplicated here rather than exposing the `pub(crate)` helper, the same way
/// [`crate::seal_divergence`], `ravel-cli`'s `catalog` module, and
/// `ravel_server::fold` all duplicate it.
fn head_key(tenant: &TenantHash, signal: Signal) -> String {
    format!("t/{}/catalog/{}/HEAD", tenant.to_hex(), signal.key_prefix())
}

/// Resolve the covering name-postings inputs for `(tenant, signal)` from the
/// current folded snapshot HEAD, or `Ok(None)` when no usable postings object
/// exists right now.
///
/// Metadata-cost: it reads the HEAD, every snapshot part, and the one postings
/// object, never a data segment. Returns `Ok(Some(..))` only when a postings
/// object is present, hash-verified, tenant-bound to this tenant, decodes
/// against the covered parts' blake3, and its entry count matches the covered
/// entries. See the [module docs](self) for the full degrade-to-`Ok(None)`
/// contract and the two loud [`LoadPostingsError`] exceptions.
pub async fn load_covering_postings(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
) -> Result<Option<LoadedCoveringPostings>, LoadPostingsError> {
    let key = head_key(tenant, signal);

    // No HEAD yet (nothing folded): there is no snapshot to load postings from.
    // A store error fetching HEAD is the "nothing folded yet" case, not an
    // error, exactly as verify_seal_divergence treats it.
    let head_bytes = match store.get(&key, GetRange::Full).await {
        Ok(got) => got.data,
        Err(_) => return Ok(None),
    };
    let head = decode_head(&head_bytes).map_err(|source| LoadPostingsError::HeadCorrupt {
        key: key.clone(),
        source,
    })?;

    // No postings ref built yet (or the last fold's postings build failed): a
    // pure degrade-to-None, the documented "no postings ref yet" case
    // (ADR-0059), never an error.
    let Some(postings_ref) = head.postings.clone() else {
        return Ok(None);
    };

    // Rebuild the covered-entry universe from every part, in SnapshotHead.parts
    // order (the order postings ordinals index into), collecting the covered
    // parts' blake3 (the binding decode_postings verifies) alongside. A part
    // GET failure disables the postings tier this tick (retried next tick); a
    // part present but genuinely unparseable is a real catalog defect surfaced
    // as an error, matching verify_seal_divergence.
    let part_limits = PartLimits::default();
    let mut part_blake3: Vec<[u8; 32]> = Vec::with_capacity(head.parts.len());
    let mut covered_entries: Vec<SnapshotEntry> = Vec::new();
    for part_ref in &head.parts {
        // validate_head already guaranteed a 32-byte part blake3; degrade
        // rather than panic if that invariant ever changes.
        let Ok(blake3) = <[u8; 32]>::try_from(part_ref.blake3.as_slice()) else {
            return Ok(None);
        };
        part_blake3.push(blake3);
        let got = match store.get(&part_ref.key, GetRange::Full).await {
            Ok(got) => got,
            Err(_) => return Ok(None),
        };
        let decoded = decode_part(&got.data, &part_limits).map_err(|source| {
            LoadPostingsError::PartCorrupt {
                key: part_ref.key.clone(),
                source,
            }
        })?;
        covered_entries.extend(decoded.entries);
    }

    // Load and validate the covering postings object, following
    // load_snapshot_postings exactly: GET, verify blake3 against the ref, check
    // the declared tenant_hash BEFORE decode (ADR-0050 §2 -- a foreign tenant
    // is a hard error even for an object that would also fail to bind), decode
    // against the covered parts' blake3, then check the entry count. Every
    // failure short of the tenant-hash breach degrades to Ok(None).
    let data = match store.get(&postings_ref.key, GetRange::Full).await {
        Ok(got) => got.data,
        Err(_) => return Ok(None),
    };
    let digest = blake3::hash(&data);
    if digest.as_bytes().as_slice() != postings_ref.blake3.as_slice() {
        return Ok(None);
    }
    match snapshot_format::postings_declared_tenant_hash(&data) {
        Ok(declared) if declared != tenant.0 => {
            return Err(LoadPostingsError::TenantHashMismatch {
                key: postings_ref.key.clone(),
                expected: tenant.to_hex(),
                actual: hex::encode(declared),
            });
        }
        Ok(_) => {}
        Err(_) => return Ok(None),
    }
    let limits = PostingsLimits {
        max_postings_bytes: DEFAULT_MAX_POSTINGS_BYTES,
    };
    let decoded = match snapshot_format::decode_postings(&data, &limits, &part_blake3) {
        Ok(decoded) => decoded,
        Err(_) => return Ok(None),
    };
    if decoded.header.entry_count != covered_entries.len() as u64 {
        return Ok(None);
    }

    Ok(Some(LoadedCoveringPostings {
        bytes: data.to_vec(),
        part_blake3,
        covered_entries,
    }))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use ravel_commit::keys;
    use ravel_commit::publish::{self, RetryPolicy};
    use ravel_commit::record::{self, NewCommitRecord};
    use ravel_object_store::PutOptions;
    use ravel_object_store::memory::MemoryStore;
    use ravel_proto::commit::v1::CommitRecord;
    use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
    use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, TenantId};
    use uuid::Uuid;

    use super::*;
    use crate::snapshot_format::encode_head;

    const NS_PER_HOUR: i64 = 3_600_000_000_000;
    // Comfortably past the default seal margins (matches the seal-divergence
    // suite's SEALED_AGE_NS), so a published record is sealed by fold time.
    const SEALED_AGE_NS: i64 = 3 * NS_PER_HOUR;

    fn tenant_id() -> TenantId {
        TenantId::new("covering-postings-test")
    }

    /// Publish a real RSEG segment plus its commit record into `(tenant,
    /// Metrics, shard 0)`, exactly as ingest would, so a fold over it can build
    /// a genuine postings object (a fake payload would make the postings build
    /// fail and the fold proceed with no postings ref).
    async fn publish_real_segment(
        store: &MemoryStore,
        seq: u64,
        created_unix_ns: i64,
        metrics: &[&str],
    ) -> CommitRecord {
        let tid = tenant_id();
        let tenant_hash = tid.hash();
        let writer_id = Uuid::from_u128(u128::from(seq));
        let ingest_hour_bucket = u32::try_from(created_unix_ns / NS_PER_HOUR).expect("fits u32");
        let series: Vec<SeriesInput> = metrics
            .iter()
            .map(|metric| {
                let labels = LabelSet::new(vec![Label {
                    name: METRIC_NAME_LABEL.to_string(),
                    value: (*metric).to_string(),
                }])
                .expect("valid labels");
                let series_id = SeriesId::compute(&tid, metric, &labels).expect("series id");
                SeriesInput {
                    series_id,
                    labels,
                    samples: vec![Sample {
                        ts_ns: created_unix_ns,
                        value: 1.0,
                    }],
                }
            })
            .collect();
        let identity = SegmentIdentity {
            tenant_hash: tenant_hash.0,
            shard: 0,
            writer_id: writer_id.to_string(),
            writer_epoch: 1,
            writer_seq: seq,
        };
        let min_ingest_ts_ns = created_unix_ns - 1_000;
        let max_ingest_ts_ns = created_unix_ns;
        let bounds = IngestBounds {
            min_ingest_ts_ns,
            max_ingest_ts_ns,
        };
        let written = SegmentWriter::write(series, identity, bounds).expect("write segment");
        let rec = record::build(NewCommitRecord {
            tenant_hash,
            signal: Signal::Metrics,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq: seq,
            object_size: written.bytes.len() as u64,
            content_hash: written.summary.blake3,
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            min_ingest_ts_ns,
            max_ingest_ts_ns,
            segment_format_version: 1,
            created_unix_ns,
            ingest_hour_bucket,
        })
        .expect("valid record");
        let data_key = keys::reconstruct_data_key(&rec).expect("data key");
        publish::put_data_object(store, &data_key, written.bytes)
            .await
            .expect("put data object");
        publish::publish(store, &rec, &RetryPolicy::default())
            .await
            .expect("publish commit record");
        rec
    }

    async fn fold(store: Arc<dyn ObjectStoreBackend>, now_ns: i64) {
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
                &tenant_id().hash(),
                Signal::Metrics,
                Uuid::new_v4(),
                now_ns,
                &[],
            )
            .await
            .expect("fold produces a HEAD");
    }

    #[tokio::test]
    async fn absent_head_returns_none() {
        let store = MemoryStore::new();
        let loaded = load_covering_postings(&store, &tenant_id().hash(), Signal::Metrics)
            .await
            .expect("no read error");
        assert!(
            loaded.is_none(),
            "no HEAD yet must be Ok(None), not an error"
        );
    }

    #[tokio::test]
    async fn folded_snapshot_with_postings_loads() {
        let store = Arc::new(MemoryStore::new());
        let now = 600_000 * NS_PER_HOUR;
        let created = now - SEALED_AGE_NS;
        publish_real_segment(store.as_ref(), 1, created, &["cpu", "mem"]).await;
        fold(store.clone(), now).await;

        let loaded = load_covering_postings(store.as_ref(), &tenant_id().hash(), Signal::Metrics)
            .await
            .expect("no read error")
            .expect("a fold over one real segment builds a postings object");
        assert_eq!(loaded.covered_entries.len(), 1, "one covered entry");
        assert_eq!(loaded.part_blake3.len(), 1, "one covered part");
        assert!(!loaded.bytes.is_empty(), "the postings bytes are present");
    }

    #[tokio::test]
    async fn blake3_mismatch_degrades_to_none() {
        let store = Arc::new(MemoryStore::new());
        let now = 600_000 * NS_PER_HOUR;
        let created = now - SEALED_AGE_NS;
        publish_real_segment(store.as_ref(), 1, created, &["cpu", "mem"]).await;
        fold(store.clone(), now).await;

        // Overwrite the postings object with different bytes without updating
        // the HEAD's postings.blake3: the ref no longer matches the object, so
        // the load degrades to Ok(None) (pruning/verification disabled), never
        // an error.
        let key = head_key(&tenant_id().hash(), Signal::Metrics);
        let head_bytes = store.get(&key, GetRange::Full).await.expect("head").data;
        let head = decode_head(&head_bytes).expect("decode head");
        let postings_key = head.postings.expect("postings ref").key;
        store
            .put(
                &postings_key,
                Bytes::from_static(b"not the real postings bytes"),
                PutOptions::default(),
            )
            .await
            .expect("overwrite postings object");

        let loaded = load_covering_postings(store.as_ref(), &tenant_id().hash(), Signal::Metrics)
            .await
            .expect("a blake3 mismatch must be Ok(None), never an error");
        assert!(loaded.is_none());
    }

    #[tokio::test]
    async fn foreign_tenant_hash_hard_fails() {
        let store = Arc::new(MemoryStore::new());
        let now = 600_000 * NS_PER_HOUR;
        let created = now - SEALED_AGE_NS;
        publish_real_segment(store.as_ref(), 1, created, &["cpu", "mem"]).await;
        fold(store.clone(), now).await;

        // Re-encode a postings object declaring a foreign tenant_hash but bound
        // to the real covered part(s), then point the HEAD's postings ref at it
        // with a matching blake3 so the ref check passes and the load reaches
        // the ADR-0050 §2 tenant-hash check (mirrors snapshot_resolve's
        // `postings_foreign_tenant_unbound_still_hard_fails`).
        let key = head_key(&tenant_id().hash(), Signal::Metrics);
        let head_bytes = store.get(&key, GetRange::Full).await.expect("head").data;
        let mut head = decode_head(&head_bytes).expect("decode head");
        let part_blake3: Vec<[u8; 32]> = head
            .parts
            .iter()
            .map(|p| <[u8; 32]>::try_from(p.blake3.as_slice()).expect("32-byte part blake3"))
            .collect();
        let wrong_tenant = TenantHash([0xff; 16]);

        let postings_bytes = snapshot_format::encode_postings(
            wrong_tenant.0,
            ravel_commit::signal::to_proto(Signal::Metrics) as u32,
            &part_blake3,
            1,
            &[],
        )
        .expect("encode foreign postings");
        let postings_hash = *blake3::hash(&postings_bytes).as_bytes();

        let postings_ref = head.postings.as_mut().expect("postings ref");
        store
            .put(
                &postings_ref.key,
                Bytes::from(postings_bytes.clone()),
                PutOptions::default(),
            )
            .await
            .expect("overwrite postings object with the foreign-tenant one");
        postings_ref.blake3 = postings_hash.to_vec();
        postings_ref.size = postings_bytes.len() as u64;
        let rewritten = encode_head(&head).expect("re-encode head");
        store
            .put(&key, Bytes::from(rewritten), PutOptions::default())
            .await
            .expect("overwrite HEAD");

        let err = load_covering_postings(store.as_ref(), &tenant_id().hash(), Signal::Metrics)
            .await
            .expect_err("a foreign tenant_hash must hard-fail, never degrade");
        assert!(matches!(err, LoadPostingsError::TenantHashMismatch { .. }));
    }
}
