//! Covering-postings load for the at-rest scrubber's postings tier (ADR-0059
//! decision 1). The library scrubber
//! (`ravel_maintain::scrub::scrub_one_object`) already implements the
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
//! # Degrade-to-None, three loud exceptions
//!
//! Postings are a pure pruning/verification optimization, never a correctness
//! gate: an absent HEAD, an absent postings ref, a `NotFound` on the HEAD, a
//! part, or the postings object, a blake3 mismatch, a decode error, or an
//! entry-count mismatch all degrade to `Ok(None)`, exactly as
//! `load_snapshot_postings` documents. The scrubber then simply runs the
//! structural and content tiers with no postings tier this tick and retries
//! next tick. Three loud exceptions return a real [`LoadPostingsError`]
//! instead: a genuinely unparseable HEAD or part (a real catalog defect,
//! matching [`crate::seal_divergence`]), a postings object whose declared
//! `tenant_hash` names a different tenant (an isolation breach, ADR-0050 §2,
//! never absorbed into a silent degrade), and a GET of the HEAD, a part, or
//! the postings object itself failing for any reason other than `NotFound`
//! (issue #1964 covered the postings object; issue #1976 extends the same
//! treatment to the HEAD and per-part GETs above it, which used to degrade to
//! `Ok(None)` on any error: an `AccessDenied` on any of the three is a
//! missing IAM grant, not an absent object, and must disable the postings
//! tier loudly rather than reading as "nothing there yet").

use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError};
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

/// A genuinely unparseable object, an isolation breach, or a non-`NotFound`
/// GET failure on the HEAD, a part, or the postings object itself: the only
/// conditions [`load_covering_postings`] surfaces as an error rather than
/// degrading to `Ok(None)`. Every other failure mode (absent HEAD or postings
/// ref, a `NotFound` on the HEAD, a part, or the postings object, a blake3 or
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
    /// The postings object GET failed for a reason other than `NotFound`
    /// (issue #1964): most commonly a missing IAM read grant on `idx/*`,
    /// surfaced as `StoreError::AccessDenied`. This disables the whole
    /// postings tier for the tick, so it is a hard error the scrub tick can
    /// count and retry, never a silent `Ok(None)` degrade indistinguishable
    /// from "no postings ref yet".
    #[error("postings object {key} GET failed: {source}")]
    Store {
        key: String,
        #[source]
        source: StoreError,
    },
    /// The catalog HEAD GET failed for a reason other than `NotFound` (issue
    /// #1976): a `NotFound` there still means "nothing folded yet" and stays
    /// an `Ok(None)` degrade, but any other failure is a store outage, not an
    /// absence.
    #[error("HEAD at {key} GET failed: {source}")]
    HeadStore {
        key: String,
        #[source]
        source: StoreError,
    },
    /// A snapshot part the HEAD references failed its GET for a reason other
    /// than `NotFound` (issue #1976): the same store-outage-vs-absence
    /// distinction as [`Self::Store`] and [`Self::HeadStore`], applied to a
    /// covered part's own object.
    #[error("snapshot part {key} GET failed: {source}")]
    PartStore {
        key: String,
        #[source]
        source: StoreError,
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
/// contract and the three loud [`LoadPostingsError`] exceptions.
pub async fn load_covering_postings(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
) -> Result<Option<LoadedCoveringPostings>, LoadPostingsError> {
    let key = head_key(tenant, signal);

    // No HEAD yet (nothing folded): there is no snapshot to load postings
    // from. `NotFound` fetching HEAD is the "nothing folded yet" case, not an
    // error, exactly as verify_seal_divergence treats it. Any other GET
    // failure (issue #1976: most commonly AccessDenied on a missing IAM read
    // grant) is a store outage and must surface, not be swallowed as though
    // nothing were folded.
    let head_bytes = match store.get(&key, GetRange::Full).await {
        Ok(got) => got.data,
        Err(StoreError::NotFound) => return Ok(None),
        Err(source) => {
            return Err(LoadPostingsError::HeadStore {
                key: key.clone(),
                source,
            });
        }
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
    // parts' blake3 (the binding decode_postings verifies) alongside. A
    // `NotFound` part GET disables the postings tier this tick (retried next
    // tick); any other GET failure (issue #1976) is a store outage and
    // surfaces instead; a part present but genuinely unparseable is a real
    // catalog defect surfaced as an error, matching verify_seal_divergence.
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
            Err(StoreError::NotFound) => return Ok(None),
            Err(source) => {
                return Err(LoadPostingsError::PartStore {
                    key: part_ref.key.clone(),
                    source,
                });
            }
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
    // failure short of the tenant-hash breach and a non-NotFound GET error
    // degrades to Ok(None). `NotFound` means "no postings object at this ref
    // yet", a normal degrade; any other GET error (issue #1964, most commonly
    // AccessDenied on a missing idx/* read grant) is a real outage of the
    // postings tier and must not be swallowed as though nothing was there.
    let data = match store.get(&postings_ref.key, GetRange::Full).await {
        Ok(got) => got.data,
        Err(StoreError::NotFound) => return Ok(None),
        // No log here: the returned error carries the key and the source, and
        // the caller reports it. One fault, one line.
        Err(source) => {
            return Err(LoadPostingsError::Store {
                key: postings_ref.key.clone(),
                source,
            });
        }
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
    use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault};
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
                None,
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

    /// Issue #1964: a GET failure on the postings object other than
    /// `NotFound` (an `AccessDenied` on a missing `idx/*` read grant, most
    /// commonly) must surface as a real error, not degrade to `Ok(None)`
    /// indistinguishable from "no postings ref yet". `ScriptedFault` has no
    /// `AccessDenied` kind (a `ravel-object-store` testing gap, out of scope
    /// for this crate), so `Permanent` stands in as a structurally equivalent
    /// non-`NotFound` GET failure: the code path this test pins branches on
    /// `Err(StoreError::NotFound)` versus every other `Err`, not on which
    /// variant the "other" arm carries.
    #[tokio::test]
    async fn postings_get_permanent_failure_surfaces_as_error() {
        let inner = Arc::new(MemoryStore::new());
        let now = 600_000 * NS_PER_HOUR;
        let created = now - SEALED_AGE_NS;
        publish_real_segment(inner.as_ref(), 1, created, &["cpu", "mem"]).await;
        fold(inner.clone(), now).await;

        let key = head_key(&tenant_id().hash(), Signal::Metrics);
        let head_bytes = inner.get(&key, GetRange::Full).await.expect("head").data;
        let head = decode_head(&head_bytes).expect("decode head");
        let postings_key = head.postings.expect("postings ref").key;

        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::Get,
                ScriptedFault::Permanent("simulated AccessDenied on idx/*".into()),
            )
            .with_key_contains(postings_key.clone()),
        );
        let faulty = FaultStore::new(inner.clone(), plan);

        let err = load_covering_postings(&faulty, &tenant_id().hash(), Signal::Metrics)
            .await
            .expect_err(
                "a non-NotFound GET failure on the postings object must surface as an error",
            );
        match err {
            LoadPostingsError::Store { key: err_key, .. } => {
                assert_eq!(err_key, postings_key, "error names the failing key");
            }
            other => panic!("expected LoadPostingsError::Store, got {other:?}"),
        }
        assert!(
            faulty.fault_count(Op::Get, FaultKind::Permanent) >= 1,
            "the injected fault must actually have fired"
        );
    }

    /// The counterpart to `postings_get_permanent_failure_surfaces_as_error`:
    /// a genuine `NotFound` on the postings object (modeled here with
    /// `NotFoundBlip`, an eventual-consistency blip rather than a permission
    /// fault) must still degrade quietly to `Ok(None)`, exactly as before
    /// this fix.
    #[tokio::test]
    async fn postings_get_not_found_blip_still_degrades_to_none() {
        let inner = Arc::new(MemoryStore::new());
        let now = 600_000 * NS_PER_HOUR;
        let created = now - SEALED_AGE_NS;
        publish_real_segment(inner.as_ref(), 1, created, &["cpu", "mem"]).await;
        fold(inner.clone(), now).await;

        let key = head_key(&tenant_id().hash(), Signal::Metrics);
        let head_bytes = inner.get(&key, GetRange::Full).await.expect("head").data;
        let head = decode_head(&head_bytes).expect("decode head");
        let postings_key = head.postings.expect("postings ref").key;

        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Get, ScriptedFault::NotFoundBlip).with_key_contains(postings_key),
        );
        let faulty = FaultStore::new(inner.clone(), plan);

        let loaded = load_covering_postings(&faulty, &tenant_id().hash(), Signal::Metrics)
            .await
            .expect("a NotFound on the postings object must degrade to Ok(None), never an error");
        assert!(loaded.is_none());
        assert!(
            faulty.fault_count(Op::Get, FaultKind::NotFoundBlip) >= 1,
            "the injected fault must actually have fired"
        );
    }

    /// Issue #1976: the catalog HEAD GET itself (the read `#1964` explicitly
    /// left unfixed) must surface a non-`NotFound` failure as a real error,
    /// not read as "nothing folded yet". Caller: `ravel_server::scrub::run_cycle`,
    /// which already logs any `Err` from this function at `error!` and skips
    /// the postings tier for the tick.
    #[tokio::test]
    async fn head_get_permanent_failure_surfaces_as_error() {
        let inner = Arc::new(MemoryStore::new());
        let now = 600_000 * NS_PER_HOUR;
        let created = now - SEALED_AGE_NS;
        publish_real_segment(inner.as_ref(), 1, created, &["cpu", "mem"]).await;
        fold(inner.clone(), now).await;

        let key = head_key(&tenant_id().hash(), Signal::Metrics);
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::Get,
                ScriptedFault::Permanent("simulated AccessDenied on idx/*".into()),
            )
            .with_key_contains(key.clone()),
        );
        let faulty = FaultStore::new(inner.clone(), plan);

        let err = load_covering_postings(&faulty, &tenant_id().hash(), Signal::Metrics)
            .await
            .expect_err("a non-NotFound GET failure on the HEAD must surface as an error");
        match err {
            LoadPostingsError::HeadStore { key: err_key, .. } => {
                assert_eq!(err_key, key, "error names the failing key");
            }
            other => panic!("expected LoadPostingsError::HeadStore, got {other:?}"),
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
        let now = 600_000 * NS_PER_HOUR;
        let created = now - SEALED_AGE_NS;
        publish_real_segment(inner.as_ref(), 1, created, &["cpu", "mem"]).await;
        fold(inner.clone(), now).await;

        let key = head_key(&tenant_id().hash(), Signal::Metrics);
        let plan = FaultPlan::empty()
            .with_rule(Rule::new(Op::Get, ScriptedFault::NotFoundBlip).with_key_contains(key));
        let faulty = FaultStore::new(inner.clone(), plan);

        let loaded = load_covering_postings(&faulty, &tenant_id().hash(), Signal::Metrics)
            .await
            .expect("a NotFound on the HEAD must degrade to Ok(None), never an error");
        assert!(loaded.is_none());
        assert!(
            faulty.fault_count(Op::Get, FaultKind::NotFoundBlip) >= 1,
            "the injected fault must actually have fired"
        );
    }

    /// Issue #1976: a snapshot part's GET, the other read `#1964` left
    /// unfixed, must surface a non-`NotFound` failure as a real error rather
    /// than read as "no postings ref yet". Same caller as the HEAD-GET tests
    /// above.
    #[tokio::test]
    async fn part_get_permanent_failure_surfaces_as_error() {
        let inner = Arc::new(MemoryStore::new());
        let now = 600_000 * NS_PER_HOUR;
        let created = now - SEALED_AGE_NS;
        publish_real_segment(inner.as_ref(), 1, created, &["cpu", "mem"]).await;
        fold(inner.clone(), now).await;

        let key = head_key(&tenant_id().hash(), Signal::Metrics);
        let head_bytes = inner.get(&key, GetRange::Full).await.expect("head").data;
        let head = decode_head(&head_bytes).expect("decode head");
        let part_key = head.parts.first().expect("one covered part").key.clone();

        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::Get,
                ScriptedFault::Permanent("simulated AccessDenied on idx/*".into()),
            )
            .with_key_contains(part_key.clone()),
        );
        let faulty = FaultStore::new(inner.clone(), plan);

        let err = load_covering_postings(&faulty, &tenant_id().hash(), Signal::Metrics)
            .await
            .expect_err("a non-NotFound GET failure on a snapshot part must surface as an error");
        match err {
            LoadPostingsError::PartStore { key: err_key, .. } => {
                assert_eq!(err_key, part_key, "error names the failing key");
            }
            other => panic!("expected LoadPostingsError::PartStore, got {other:?}"),
        }
        assert!(
            faulty.fault_count(Op::Get, FaultKind::Permanent) >= 1,
            "the injected fault must actually have fired"
        );
    }

    /// The counterpart to `part_get_permanent_failure_surfaces_as_error`: a
    /// genuine `NotFound` on a snapshot part (modeled with `NotFoundBlip`)
    /// must still degrade quietly to `Ok(None)`, exactly as before this fix.
    #[tokio::test]
    async fn part_get_not_found_blip_still_degrades_to_none() {
        let inner = Arc::new(MemoryStore::new());
        let now = 600_000 * NS_PER_HOUR;
        let created = now - SEALED_AGE_NS;
        publish_real_segment(inner.as_ref(), 1, created, &["cpu", "mem"]).await;
        fold(inner.clone(), now).await;

        let key = head_key(&tenant_id().hash(), Signal::Metrics);
        let head_bytes = inner.get(&key, GetRange::Full).await.expect("head").data;
        let head = decode_head(&head_bytes).expect("decode head");
        let part_key = head.parts.first().expect("one covered part").key.clone();

        let plan = FaultPlan::empty()
            .with_rule(Rule::new(Op::Get, ScriptedFault::NotFoundBlip).with_key_contains(part_key));
        let faulty = FaultStore::new(inner.clone(), plan);

        let loaded = load_covering_postings(&faulty, &tenant_id().hash(), Signal::Metrics)
            .await
            .expect("a NotFound on a snapshot part must degrade to Ok(None), never an error");
        assert!(loaded.is_none());
        assert!(
            faulty.fault_count(Op::Get, FaultKind::NotFoundBlip) >= 1,
            "the injected fault must actually have fired"
        );
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
