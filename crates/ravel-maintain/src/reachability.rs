//! The HEAD-reachability delete blocker (ADR-0020), shared by every physical
//! delete that can race a fold.
//!
//! A snapshot the live `(tenant, signal)` HEAD names is what a resolver reads;
//! the protection horizon bounds a *pinned in-flight reader*, but it does not
//! on its own prove the *current* HEAD has stopped naming an object. Both the
//! retention sweep (whole tombstoned buckets) and the superseded-input sweep
//! (a compaction or rewrite record's inputs) therefore ask the same question
//! before deleting: does the live HEAD snapshot still reach this object?
//!
//! The three answers are fixed and identical for both callers:
//!
//! - **HEAD absent**: no snapshot names anything, so nothing is blocked
//!   (ADR-0020: the catalog index is a pure optimization; a missing HEAD
//!   degrades to listing).
//! - **HEAD, or a covering snapshot part, present but unreadable**: blocked
//!   fail-closed. Non-reachability cannot be proven from data that cannot be
//!   read, and a wrongly-permitted delete is unrecoverable while a delayed one
//!   is not.
//! - **A decoded snapshot entry names the object**: blocked. The ordinary
//!   lagging-fold case.
//!
//! [`SnapshotReachability`] is the per-pass cache that keeps this affordable:
//! HEAD is read at most once per pass and each covering part at most once, so
//! a pass that gates many buckets or many superseded inputs of one
//! `(tenant, signal)` never pays a HEAD GET per candidate (ADR-0076 request
//! cost).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use ravel_catalog::{DecodedPart, PartLimits, decode_head, decode_part};
use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError};
use ravel_proto::catalog::v1::{SnapshotEntry, SnapshotHead, SnapshotPartRef};
use ravel_types::{Signal, TenantHash};

use crate::bucket::Bucket;
use crate::error::{MaintainError, Result};

/// Why a physical delete was blocked by HEAD reachability (ADR-0020
/// delete-blocker). Both variants delete nothing; they are distinguished so
/// each is separately observable in the maintain counters (a persistent
/// [`SnapshotBlock::Unreadable`] is an operator signal that HEAD or a part
/// cannot be read, not the ordinary lagging-fold case).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotBlock {
    /// A decoded snapshot entry names an object the delete would remove: the
    /// bucket it sits in (retention) or the object itself (superseded inputs).
    /// The ordinary case: the fold has not yet reconciled the hour.
    Named,
    /// HEAD, or a snapshot part covering the relevant hour, was present but
    /// could not be read (undecodable, checksum/hash mismatch, unsupported
    /// version, an entry whose identity fields do not fit the shape a fold
    /// writes, or a HEAD-named part that is missing). Blocked fail-closed:
    /// non-reachability cannot be proven from data that cannot be read, and a
    /// wrongly-permitted delete is unrecoverable while a delayed one is not.
    Unreadable,
}

/// The result of gating one delete candidate on HEAD reachability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SnapshotGate {
    /// No snapshot entry names the candidate: the delete may proceed.
    Clear,
    /// The delete is blocked; the reason distinguishes the counters.
    Blocked(SnapshotBlock),
}

/// The identity of one physical object exactly as a snapshot entry carries it,
/// so "does the live HEAD still name this object" is decided without
/// reconstructing key strings on either side.
///
/// The frozen `SnapshotEntry` has no dedicated level-1 identity field, so the
/// fold overloads the writer slots for a compaction or rewrite output part:
/// `writer_id` carries the parent record's 32-byte `input_set_hash` and
/// `writer_epoch` carries the `part_index` (crates/ravel-catalog's
/// `build_l1_snapshot_entry`). [`snapshot_object`] is the one place that
/// convention is read here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum SnapshotObject {
    /// A raw L0 flush: the `(writer_id, epoch, seq)` identity its commit
    /// record and its data object share.
    L0 {
        shard: u32,
        ingest_hour_bucket: u32,
        writer_id: [u8; 16],
        writer_epoch: u64,
        writer_seq: u64,
    },
    /// A compaction or rewrite output part: its parent record's input-set hash
    /// plus its own part index.
    L1 {
        shard: u32,
        ingest_hour_bucket: u32,
        input_set_hash: [u8; 32],
        part_index: u32,
    },
}

/// Read one decoded snapshot entry's object identity, or `None` if its
/// identity fields do not fit the shape a fold writes (a 16-byte `writer_id`
/// at level 0, a 32-byte `input_set_hash` and a `u32`-ranged `part_index`
/// above it). `None` is a fail-closed signal, never "does not match": an entry
/// whose identity cannot be read cannot be proven not to name the candidate.
fn snapshot_object(entry: &SnapshotEntry) -> Option<SnapshotObject> {
    if entry.level == 0 {
        let writer_id: [u8; 16] = entry.writer_id.as_slice().try_into().ok()?;
        Some(SnapshotObject::L0 {
            shard: entry.shard,
            ingest_hour_bucket: entry.ingest_hour_bucket,
            writer_id,
            writer_epoch: entry.writer_epoch,
            writer_seq: entry.writer_seq,
        })
    } else {
        let input_set_hash: [u8; 32] = entry.writer_id.as_slice().try_into().ok()?;
        let part_index = u32::try_from(entry.writer_epoch).ok()?;
        Some(SnapshotObject::L1 {
            shard: entry.shard,
            ingest_hour_bucket: entry.ingest_hour_bucket,
            input_set_hash,
            part_index,
        })
    }
}

/// Per-sweep-pass cache of the catalog HEAD and the snapshot parts it names,
/// so a pass that gates many candidates of one `(tenant, signal)` reads HEAD at
/// most once and each covering part at most once, rather than once per
/// candidate (a per-candidate HEAD GET would be an S3-request-cost regression
/// against the live cost-reduction epic, ADR-0076). A retention pass creates
/// one per [`crate::scan::scan_and_maintain_with_memo`] call and threads it
/// through [`crate::retention::maintain_bucket_with_reach`]; the
/// superseded-input sweep creates one per pass. The public entry points
/// ([`crate::retention::maintain_bucket`],
/// [`crate::retention::retention_sweep_bucket`],
/// [`crate::sweep::sweep_superseded`]) create a fresh one per call.
///
/// The cache is read-once-per-pass, not a durable cache. For retention it is
/// safe precisely because a retention tombstone is irreversible (ADR-0019
/// decision 2), so a bucket the fold has already dropped from HEAD is never
/// re-added. For superseded inputs it is safe because a fold never starts
/// naming an input a published compaction or rewrite record already
/// superseded. In both directions a HEAD read that DOES name the candidate
/// only ever delays a delete by one pass, the fail-safe direction.
#[derive(Default)]
pub struct SnapshotReachability {
    head: Option<HeadLoad>,
    /// Decoded snapshot parts by object key. `None` = present but unreadable
    /// (fail-closed); `Some` = decoded and usable.
    parts: HashMap<String, Option<Arc<DecodedPart>>>,
}

/// The catalog HEAD as read once for a sweep pass.
enum HeadLoad {
    /// HEAD is absent: no snapshot names anything, so nothing is blocked
    /// (ADR-0020: the index is a pure optimization; a missing HEAD degrades to
    /// listing).
    Absent,
    /// HEAD is present but undecodable (or a newer format this build cannot
    /// read): fail-closed, every candidate is blocked.
    Unreadable,
    /// HEAD is present and decoded. Boxed so this large variant does not
    /// inflate every `HeadLoad` (`clippy::large_enum_variant`): `SnapshotHead`
    /// grew past 300 bytes when ADR-0850 added `column_stats`, while the other
    /// variants are unit-sized.
    Present(Box<SnapshotHead>),
}

impl SnapshotReachability {
    /// A fresh, empty cache for one sweep pass.
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether the live HEAD snapshot still reaches an object inside `bucket`
    /// (ADR-0020 delete-blocker). HEAD and each covering part are loaded at
    /// most once per pass and cached. HEAD absent -> [`SnapshotGate::Clear`];
    /// HEAD or any covering part unreadable -> fail-closed
    /// [`SnapshotBlock::Unreadable`]; a decoded entry naming this bucket's
    /// shard+hour -> [`SnapshotBlock::Named`].
    pub(crate) async fn bucket_gate(
        &mut self,
        store: &dyn ObjectStoreBackend,
        bucket: &Bucket,
    ) -> Result<SnapshotGate> {
        let (covering, skipped) = match self
            .covering_parts(
                store,
                &bucket.tenant_hash,
                bucket.signal,
                bucket.ingest_hour_bucket,
            )
            .await?
        {
            Covering::Clear => return Ok(SnapshotGate::Clear),
            Covering::Blocked(reason) => return Ok(SnapshotGate::Blocked(reason)),
            Covering::Parts { covering, skipped } => (covering, skipped),
        };

        for part_ref in &covering {
            match self.ensure_part(store, part_ref).await? {
                // A covering part could not be read: cannot prove
                // non-reachability, fail closed.
                None => return Ok(SnapshotGate::Blocked(SnapshotBlock::Unreadable)),
                Some(part) => {
                    // An object is physically inside this bucket iff its entry's
                    // shard and ingest hour match the bucket. Any such entry
                    // means the snapshot still names an object the sweep would
                    // delete.
                    if part.entries.iter().any(|e| {
                        e.shard == bucket.shard && e.ingest_hour_bucket == bucket.ingest_hour_bucket
                    }) {
                        return Ok(SnapshotGate::Blocked(SnapshotBlock::Named));
                    }
                }
            }
        }
        self.clear_or_block_on_skipped(store, &skipped).await
    }

    /// Whether the live HEAD snapshot still names any of `objects`, all of
    /// which sit in `ingest_hour_bucket`. The object-granular counterpart of
    /// [`Self::bucket_gate`]: the superseded-input sweep deletes individual
    /// objects out of a bucket whose other objects (the compaction or rewrite
    /// outputs that superseded them) the snapshot legitimately still names, so
    /// a bucket-granular question would block it forever.
    ///
    /// Same three answers, same cache, same fail-closed direction. An empty
    /// `objects` is [`SnapshotGate::Clear`] without reading anything, so a pass
    /// with no delete candidate issues no HEAD GET at all.
    ///
    /// `objects` is indexed into a set once per call, so the cost is one hash
    /// lookup per snapshot entry rather than a scan of every candidate: a group
    /// holding a long supersession chain gates in time linear in the covering
    /// parts' entry count, not in its product with the group's size.
    pub(crate) async fn object_gate(
        &mut self,
        store: &dyn ObjectStoreBackend,
        tenant: &TenantHash,
        signal: Signal,
        ingest_hour_bucket: u32,
        objects: &[SnapshotObject],
    ) -> Result<SnapshotGate> {
        if objects.is_empty() {
            return Ok(SnapshotGate::Clear);
        }
        let wanted: HashSet<SnapshotObject> = objects.iter().copied().collect();
        let (covering, skipped) = match self
            .covering_parts(store, tenant, signal, ingest_hour_bucket)
            .await?
        {
            Covering::Clear => return Ok(SnapshotGate::Clear),
            Covering::Blocked(reason) => return Ok(SnapshotGate::Blocked(reason)),
            Covering::Parts { covering, skipped } => (covering, skipped),
        };

        for part_ref in &covering {
            let Some(part) = self.ensure_part(store, part_ref).await? else {
                return Ok(SnapshotGate::Blocked(SnapshotBlock::Unreadable));
            };
            for entry in &part.entries {
                let Some(named) = snapshot_object(entry) else {
                    // An entry whose identity fields cannot be read: the same
                    // fail-closed answer an unreadable part gets.
                    return Ok(SnapshotGate::Blocked(SnapshotBlock::Unreadable));
                };
                if wanted.contains(&named) {
                    return Ok(SnapshotGate::Blocked(SnapshotBlock::Named));
                }
            }
        }
        self.clear_or_block_on_skipped(store, &skipped).await
    }

    /// The last step of both gates, reached only when every covering part came
    /// back clear and the gate is about to permit a delete: prove that the
    /// parts the hour-range filter skipped really were outside the hour.
    ///
    /// [`Self::covering_parts`] reads each part's range from the HEAD-level
    /// reference, not from the part itself. A reference whose declared range is
    /// narrower than the part it names would silently exclude a part that does
    /// hold entries for the gated hour, so a skip is only sound once
    /// [`Self::ensure_part`] has confirmed the two agree; it returns `None`
    /// when they do not, and that is the fail-closed answer here too.
    ///
    /// Done last, and only on the clearing path, so the ordinary held pass
    /// still reads nothing beyond HEAD and the covering parts: a delete this
    /// pass would not have performed anyway never pays for the proof.
    async fn clear_or_block_on_skipped(
        &mut self,
        store: &dyn ObjectStoreBackend,
        skipped: &[SnapshotPartRef],
    ) -> Result<SnapshotGate> {
        for part_ref in skipped {
            if self.ensure_part(store, part_ref).await?.is_none() {
                return Ok(SnapshotGate::Blocked(SnapshotBlock::Unreadable));
            }
        }
        Ok(SnapshotGate::Clear)
    }

    /// Load HEAD once for the pass and split its part refs into the ones whose
    /// declared hour range covers `ingest_hour_bucket` and the ones it does
    /// not, or return the terminal answer when HEAD is absent or unreadable.
    /// The refs are owned clones so the borrow of `self.head` is released
    /// before the callers' part loads borrow `self` mutably.
    async fn covering_parts(
        &mut self,
        store: &dyn ObjectStoreBackend,
        tenant: &TenantHash,
        signal: Signal,
        ingest_hour_bucket: u32,
    ) -> Result<Covering> {
        match self.ensure_head(store, tenant, signal).await? {
            HeadStatus::Absent => Ok(Covering::Clear),
            HeadStatus::Unreadable => Ok(Covering::Blocked(SnapshotBlock::Unreadable)),
            HeadStatus::Present => match &self.head {
                Some(HeadLoad::Present(head)) => {
                    let (covering, skipped) = head.parts.iter().cloned().partition(|p| {
                        p.min_hour <= ingest_hour_bucket && ingest_hour_bucket <= p.watermark_hour
                    });
                    Ok(Covering::Parts { covering, skipped })
                }
                // Unreachable after `ensure_head` returned `Present`; block
                // fail-closed rather than panic (no unwrap/expect on a
                // production path).
                _ => Ok(Covering::Blocked(SnapshotBlock::Unreadable)),
            },
        }
    }

    /// Load HEAD once for the pass, caching the result. Returns a lightweight
    /// status; the decoded HEAD itself stays in `self.head` for the covering
    /// part-ref extraction in [`Self::covering_parts`].
    async fn ensure_head(
        &mut self,
        store: &dyn ObjectStoreBackend,
        tenant: &TenantHash,
        signal: Signal,
    ) -> Result<HeadStatus> {
        if self.head.is_none() {
            let head_key = catalog_head_key(tenant, signal);
            let load = match store.get(&head_key, GetRange::Full).await {
                Ok(got) => match decode_head(got.data.as_ref()) {
                    Ok(head) => HeadLoad::Present(Box::new(head)),
                    Err(err) => {
                        // Present but undecodable/newer: fail-closed. Cannot
                        // prove non-reachability from a HEAD we cannot read.
                        tracing::warn!(
                            error = %err,
                            key = %head_key,
                            "maintain sweep: catalog HEAD failed to decode; blocking deletes \
                             fail-closed this pass rather than proving non-reachability from an \
                             unreadable HEAD"
                        );
                        HeadLoad::Unreadable
                    }
                },
                Err(StoreError::NotFound) => HeadLoad::Absent,
                Err(err) => return Err(MaintainError::Store(err)),
            };
            self.head = Some(load);
        }
        Ok(match &self.head {
            Some(HeadLoad::Absent) => HeadStatus::Absent,
            Some(HeadLoad::Unreadable) => HeadStatus::Unreadable,
            Some(HeadLoad::Present(_)) => HeadStatus::Present,
            None => HeadStatus::Unreadable,
        })
    }

    /// Load, verify (blake3 against the HEAD ref, and the ref's hour range
    /// against the decoded header's), and decode one snapshot part once per
    /// pass, caching the result. `Ok(None)` is the fail-closed "present but
    /// unreadable" case (a missing HEAD-named part, a hash mismatch, a bounds
    /// mismatch, or a decode failure); a transient store fault propagates.
    ///
    /// The bounds check is what makes [`Self::covering_parts`]' range filter
    /// safe to trust. That filter skips every part whose
    /// `[min_hour, watermark_hour]` excludes the hour being gated, reading
    /// those bounds from the HEAD-level reference; a reference whose range is
    /// narrower than the part it names would then let the gate skip a part
    /// that does name the candidate, and clear a delete the snapshot still
    /// reaches.
    async fn ensure_part(
        &mut self,
        store: &dyn ObjectStoreBackend,
        part_ref: &SnapshotPartRef,
    ) -> Result<Option<Arc<DecodedPart>>> {
        if let Some(cached) = self.parts.get(&part_ref.key) {
            return Ok(cached.clone());
        }
        let load: Option<Arc<DecodedPart>> = match store.get(&part_ref.key, GetRange::Full).await {
            Ok(got) => {
                let data = got.data;
                if blake3::hash(&data).as_bytes().as_slice() != part_ref.blake3.as_slice() {
                    tracing::warn!(
                        key = %part_ref.key,
                        "maintain sweep: snapshot part hash mismatch; blocking deletes fail-closed"
                    );
                    None
                } else {
                    let limits = PartLimits {
                        max_snapshot_part_bytes: ravel_catalog::DEFAULT_MAX_SNAPSHOT_PART_BYTES,
                    };
                    match decode_part(data.as_ref(), &limits) {
                        Ok(part)
                            if part.header.min_hour != part_ref.min_hour
                                || part.header.watermark_hour != part_ref.watermark_hour =>
                        {
                            tracing::warn!(
                                key = %part_ref.key,
                                ref_min_hour = part_ref.min_hour,
                                ref_watermark_hour = part_ref.watermark_hour,
                                header_min_hour = part.header.min_hour,
                                header_watermark_hour = part.header.watermark_hour,
                                "maintain sweep: snapshot part reference range disagrees with the \
                                 part header; blocking deletes fail-closed"
                            );
                            None
                        }
                        Ok(part) => Some(Arc::new(part)),
                        Err(err) => {
                            tracing::warn!(
                                error = %err,
                                key = %part_ref.key,
                                "maintain sweep: snapshot part failed to decode; blocking deletes \
                                 fail-closed"
                            );
                            None
                        }
                    }
                }
            }
            // HEAD names a part that is not present. Anomalous (a HEAD-named
            // part is only deleted after HEAD stops naming it plus the
            // horizon): cannot read its entries, so fail closed.
            Err(StoreError::NotFound) => {
                tracing::warn!(
                    key = %part_ref.key,
                    "maintain sweep: HEAD-named snapshot part is missing; blocking deletes \
                     fail-closed"
                );
                None
            }
            Err(err) => return Err(MaintainError::Store(err)),
        };
        self.parts.insert(part_ref.key.clone(), load.clone());
        Ok(load)
    }
}

/// What [`SnapshotReachability::covering_parts`] resolved for one hour: either
/// a terminal gate answer that needs no part reads, or the parts to inspect.
enum Covering {
    /// HEAD is absent: nothing is blocked.
    Clear,
    /// HEAD itself decided the answer (unreadable).
    Blocked(SnapshotBlock),
    /// The HEAD-named parts split by the declared hour range: `covering` is
    /// read for entries naming the candidate, `skipped` only has its declared
    /// range checked against the part header, and only when the gate is
    /// otherwise about to clear.
    Parts {
        covering: Vec<SnapshotPartRef>,
        skipped: Vec<SnapshotPartRef>,
    },
}

/// Lightweight status returned by [`SnapshotReachability::ensure_head`], so the
/// decoded HEAD can stay owned in the cache while the caller decides how to
/// proceed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HeadStatus {
    Absent,
    Unreadable,
    Present,
}

/// `t/<tenant_hash_hex>/catalog/<signal>/HEAD` -- the mutable head pointer for
/// one `(tenant, signal)` (docs/catalog-and-mvcc.md key layout, a frozen
/// contract). No public builder is exported from ravel-catalog, so it is
/// constructed here from the same pieces. This is the crate's only copy; the
/// catalog-object sweep in [`crate::sweep`] uses it too.
pub(crate) fn catalog_head_key(tenant: &TenantHash, signal: Signal) -> String {
    format!("t/{}/catalog/{}/HEAD", tenant.to_hex(), signal.key_prefix())
}
