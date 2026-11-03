//! The tier-2 key-index router (ADR-3692 decision 2): the one implementation
//! of ADR-2707 decision 5's per-part leaf procedure, shared by the spans and
//! logs read paths.
//!
//! For every segment of a pinned snapshot the router answers
//! [`SegmentRoute::Pruned`] (the part's leaves prove every key of the set
//! absent), [`SegmentRoute::Blocks`] (the blocks holding the set), or
//! [`SegmentRoute::Uncovered`] (tier 1 decides). It never reads a part object
//! or HEAD: a segment's `(part, ordinal)` from [`SnapshotParts`] is the only
//! link between a leaf answer and a segment.
//!
//! Parts are admitted in descending `entry_count`, ties by descending
//! `watermark_hour`, while the next part's worst-case leaf GET count fits
//! under [`KEY_INDEX_LEAF_GET_CEILING`] and the admitted count stays within
//! [`KEY_INDEX_MAX_PROBED_PARTS`]. A part's worst case is `1 + min(k,
//! 2^bucket_bits, B_run)` per leaf slice the key set touches; once its probe
//! has run, the GETs it did not issue go back to the budget and the first
//! part that did not fit is evaluated again. Parts left over are
//! [`SegmentRoute::Uncovered`], never sent to a scan.

use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use futures::future::BoxFuture;
use ravel_catalog::{
    Catalog, CatalogError, KEY_INDEX_LEAF_GET_CEILING, KEY_INDEX_LEAF_READER_MAX_BUCKET_BITS,
    KEY_INDEX_MAX_PROBED_PARTS, KEY_INDEX_SET_PROBE_MAX_RUNS, KeyIndexKeyAnswer,
    KeyIndexLeafLimits, KeyIndexLeafRejection, KeyIndexLeafRequest, KeyIndexSetAnswer,
    KeyIndexSetDecline, KeyIndexSetPolicy, Snapshot, SnapshotPartHandle, SnapshotParts,
    select_key_index_slice,
};
use ravel_codec::key_idx::{Key8, KeyType};
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_proto::catalog::v1::SnapshotKeyIndexLeafRef;
use ravel_types::accounting::{AccountedOp, QueryAccounting};
use ravel_types::{Signal, TenantHash};

use crate::phase_accounting::PhaseAccounting;

/// The most keys one [`KeySet`] holds (ADR-2983 decision 5's k).
pub const KEY_SET_MAX_KEYS: usize = 1_024;

/// Parts whose leaves are probed at once.
const PROBE_CONCURRENCY: usize = 16;

/// One lookup's keys: a field, its key type, and 1 to [`KEY_SET_MAX_KEYS`]
/// sorted, distinct `key8`s. A set of one is the equality case. The full
/// values the residual filter needs stay with the caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeySet {
    field: String,
    key_type: KeyType,
    keys: Vec<Key8>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KeySetError {
    #[error("a key set needs at least one key")]
    Empty,
    #[error("a key set holds at most {KEY_SET_MAX_KEYS} distinct keys, got {0}")]
    TooMany(usize),
}

impl KeySet {
    /// Sorts and deduplicates `keys`, then checks the count.
    pub fn new(
        field: impl Into<String>,
        key_type: KeyType,
        mut keys: Vec<Key8>,
    ) -> Result<Self, KeySetError> {
        keys.sort_unstable();
        keys.dedup();
        if keys.is_empty() {
            return Err(KeySetError::Empty);
        }
        if keys.len() > KEY_SET_MAX_KEYS {
            return Err(KeySetError::TooMany(keys.len()));
        }
        Ok(KeySet {
            field: field.into(),
            key_type,
            keys,
        })
    }

    pub fn field(&self) -> &str {
        &self.field
    }

    pub fn key_type(&self) -> KeyType {
        self.key_type
    }

    /// Sorted ascending, distinct.
    pub fn keys(&self) -> &[Key8] {
        &self.keys
    }
}

/// What the router says about one segment of the snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SegmentRoute {
    /// The part's leaves prove every key of the set absent from the segment.
    Pruned,
    /// The union of the blocks holding any key of the set, ascending and
    /// distinct: read these instead of the object's SKIP_IDX candidates.
    Blocks(Vec<u32>),
    /// No tier-2 answer: tier 1 decides.
    Uncovered,
}

/// The router's figures (ADR-3692 decision 7's `stats.keyIndex`, snake_case,
/// plus `leaf_wire_bytes` and `skipped_by_budget`). Every part with at
/// least one segment in the snapshot counts in exactly one of the six part
/// outcomes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RouteFigures {
    /// Parts whose leaves answered the whole key set.
    pub parts_covered: u64,
    /// Parts with no leaf for the field.
    pub parts_uncovered: u64,
    /// Parts the leaf GET or part ceiling left unprobed.
    pub parts_beyond_ceiling: u64,
    /// Parts whose leaf, or whose leaf refs, did not answer.
    pub leaves_rejected: u64,
    /// Parts whose frame decode reservation was refused.
    pub reservations_refused: u64,
    /// GETs the reader charged to the leaf reads, in the `keyIndex` phase.
    pub leaf_get_requests: u64,
    /// Wire bytes of those GETs.
    pub leaf_wire_bytes: u64,
    /// The key set's size.
    pub keys_probed: u64,
    /// Parts whose bridged bucket read exceeded `entry_count x T1_BYTES`.
    pub skipped_by_budget: u64,
}

/// Held while a frame decodes; releases its reservation when dropped.
pub struct LeafDecodeGuard {
    _held: Box<dyn Any + Send + Sync>,
}

impl LeafDecodeGuard {
    /// A guard owning `held`, whose drop releases the reservation.
    pub fn new(held: impl Any + Send + Sync) -> Self {
        LeafDecodeGuard {
            _held: Box::new(held),
        }
    }
}

impl std::fmt::Debug for LeafDecodeGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("LeafDecodeGuard")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("a {bytes}-byte leaf decode reservation was refused")]
pub struct BudgetRefused {
    pub bytes: u64,
}

/// The memory handle a frame's decoded bound is reserved through: the
/// statement's pool for SQL, the server's [`ravel_memory::MemoryBudget`]
/// for the trace-by-id endpoint.
pub trait LeafDecodeBudget: Sync {
    fn try_reserve(&self, bytes: u64) -> Result<LeafDecodeGuard, BudgetRefused>;
}

impl LeafDecodeBudget for Arc<ravel_memory::MemoryBudget> {
    fn try_reserve(&self, bytes: u64) -> Result<LeafDecodeGuard, BudgetRefused> {
        self.reserve(bytes)
            .map(LeafDecodeGuard::new)
            .map_err(|_| BudgetRefused { bytes })
    }
}

/// The ranged byte source for leaf objects: the fetcher's cached ranged
/// read in production. A read that reaches the store charges its GET and
/// wire bytes to `accounting`; a cache hit charges none.
pub trait LeafRangeReader: Sync {
    fn read_range<'a>(
        &'a self,
        leaf: &'a SnapshotKeyIndexLeafRef,
        range: Range<u64>,
        accounting: &'a QueryAccounting,
    ) -> BoxFuture<'a, Result<Bytes, String>>;
}

/// An uncached [`LeafRangeReader`]: one ranged store GET per read.
pub struct StoreLeafReader {
    store: Arc<dyn ObjectStoreBackend>,
}

impl StoreLeafReader {
    pub fn new(store: Arc<dyn ObjectStoreBackend>) -> Self {
        StoreLeafReader { store }
    }
}

impl LeafRangeReader for StoreLeafReader {
    fn read_range<'a>(
        &'a self,
        leaf: &'a SnapshotKeyIndexLeafRef,
        range: Range<u64>,
        accounting: &'a QueryAccounting,
    ) -> BoxFuture<'a, Result<Bytes, String>> {
        Box::pin(async move {
            accounting.record_s3_request(AccountedOp::Get);
            let got = self
                .store
                .get(&leaf.key, GetRange::Range(range.start, range.end))
                .await
                .map_err(|e| e.to_string())?;
            accounting.add_s3_bytes(AccountedOp::Get, got.data.len() as u64);
            Ok(got.data)
        })
    }
}

/// Everything one routing pass reads.
pub struct RouteInput<'a> {
    /// Counts a tenant or signal breach in its isolation counter.
    pub catalog: &'a Catalog,
    pub snapshot: &'a Snapshot,
    /// From the same `resolve_pruned_with_parts` call as `snapshot`.
    pub parts: &'a SnapshotParts,
    pub signal: Signal,
    pub tenant_hash: TenantHash,
    pub key_set: &'a KeySet,
    /// Leaf reads are charged to its `keyIndex` handle, under that handle's
    /// access intent.
    pub accounting: &'a PhaseAccounting,
    pub reader: &'a dyn LeafRangeReader,
    pub budget: &'a dyn LeafDecodeBudget,
    pub limits: KeyIndexLeafLimits,
}

#[derive(Debug, thiserror::Error)]
pub enum RouteError {
    /// A leaf under this tenant's prefix names another tenant or signal.
    /// The statement fails; the catalog's breach counter has moved.
    #[error("key-index leaf {leaf_key}: {source}")]
    Isolation {
        leaf_key: String,
        #[source]
        source: CatalogError,
    },
    /// `parts` does not belong to `snapshot`.
    #[error("key-index route input: {0}")]
    InvalidInput(String),
}

/// The admission bounds. [`route`] uses the ADR constants; tests use smaller
/// ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Ceiling {
    leaf_gets: u64,
    parts: usize,
}

const ADR_CEILING: Ceiling = Ceiling {
    leaf_gets: KEY_INDEX_LEAF_GET_CEILING as u64,
    parts: KEY_INDEX_MAX_PROBED_PARTS,
};

/// Routes every segment of `input.snapshot` for `input.key_set`. The routes
/// are parallel to `snapshot.segments`. A leaf breach is the only
/// statement-failing outcome besides a `parts`/`snapshot` mismatch; every
/// other failure leaves the part [`SegmentRoute::Uncovered`] and counts it.
pub async fn route(input: RouteInput<'_>) -> Result<(Vec<SegmentRoute>, RouteFigures), RouteError> {
    route_under(input, ADR_CEILING).await
}

async fn route_under(
    input: RouteInput<'_>,
    ceiling: Ceiling,
) -> Result<(Vec<SegmentRoute>, RouteFigures), RouteError> {
    let segment_count = input.snapshot.segments.len();
    if input.parts.origin.len() != segment_count {
        return Err(RouteError::InvalidInput(format!(
            "{} part origins for {segment_count} segments",
            input.parts.origin.len()
        )));
    }
    let mut has_segment = vec![false; input.parts.parts.len()];
    for origin in input.parts.origin.iter().flatten() {
        let Some(slot) = has_segment.get_mut(origin.part as usize) else {
            return Err(RouteError::InvalidInput(format!(
                "segment origin names part {} of {}",
                origin.part,
                input.parts.parts.len()
            )));
        };
        *slot = true;
    }
    let mut figures = RouteFigures {
        keys_probed: input.key_set.keys.len() as u64,
        ..RouteFigures::default()
    };
    // Leaf reads are charged here first, so the figures count leaf GETs
    // only, and merged into the caller's `keyIndex` handle on every exit.
    let leaf_accounting = QueryAccounting::new();
    leaf_accounting.set_access_intent(input.accounting.key_index().access_intent());
    let answered = route_parts(
        &input,
        ceiling,
        &has_segment,
        &leaf_accounting,
        &mut figures,
    )
    .await;
    let leaf = leaf_accounting.snapshot();
    input.accounting.key_index().merge_snapshot(&leaf);
    figures.leaf_get_requests = leaf.s3_requests(AccountedOp::Get);
    figures.leaf_wire_bytes = leaf.s3_bytes(AccountedOp::Get);
    let answered = answered?;

    let mut routes = vec![SegmentRoute::Uncovered; segment_count];
    for (route, origin) in routes.iter_mut().zip(&input.parts.origin) {
        let Some(origin) = origin else {
            continue;
        };
        let Some(answer) = answered.get(&(origin.part as usize)) else {
            continue;
        };
        let ordinal = u64::from(origin.ordinal);
        let entry_count = input.parts.parts[origin.part as usize].entry_count;
        if ordinal >= entry_count || answer.uncovered.contains(&ordinal) {
            continue;
        }
        *route = match answer.blocks.get(&ordinal) {
            Some(blocks) => SegmentRoute::Blocks(blocks.iter().copied().collect()),
            None => SegmentRoute::Pruned,
        };
    }
    Ok((routes, figures))
}

/// A covered part's answer over its covered entries.
struct PartAnswer {
    /// Entry ordinal -> the union of the key set's blocks in it.
    blocks: BTreeMap<u64, BTreeSet<u32>>,
    uncovered: BTreeSet<u64>,
}

enum PartOutcome {
    Answered(PartAnswer),
    Rejected,
    OverBudget,
    ReservationRefused,
}

/// One admissible part: the leaf slices the key set touches, each with its
/// keys, and the worst-case GET count admission charges before probing.
struct PartPlan<'a> {
    part: usize,
    handle: &'a SnapshotPartHandle,
    slices: Vec<(&'a SnapshotKeyIndexLeafRef, Vec<Key8>)>,
    worst_gets: u64,
}

fn plan_part<'a>(
    part: usize,
    handle: &'a SnapshotPartHandle,
    key_set: &KeySet,
) -> Result<Option<PartPlan<'a>>, KeyIndexLeafRejection> {
    let mut slices: Vec<(&'a SnapshotKeyIndexLeafRef, Vec<Key8>)> = Vec::new();
    for key8 in &key_set.keys {
        let Some(leaf_ref) = select_key_index_slice(&handle.key_index, &key_set.field, key8)?
        else {
            return Ok(None);
        };
        // Keys are ascending and slices tile the key space, so a key either
        // joins the last slice or opens the next one.
        match slices.last_mut() {
            Some((last, keys)) if std::ptr::eq(*last, leaf_ref) => keys.push(*key8),
            _ => slices.push((leaf_ref, vec![*key8])),
        }
    }
    let b_run = KEY_INDEX_SET_PROBE_MAX_RUNS as u64;
    let worst_gets = slices
        .iter()
        .map(|(leaf_ref, keys)| {
            let bits = leaf_ref
                .bucket_bits
                .min(u32::from(KEY_INDEX_LEAF_READER_MAX_BUCKET_BITS));
            1 + (keys.len() as u64).min(1u64 << bits).min(b_run)
        })
        .sum();
    Ok(Some(PartPlan {
        part,
        handle,
        slices,
        worst_gets,
    }))
}

async fn route_parts(
    input: &RouteInput<'_>,
    ceiling: Ceiling,
    has_segment: &[bool],
    leaf_accounting: &QueryAccounting,
    figures: &mut RouteFigures,
) -> Result<BTreeMap<usize, PartAnswer>, RouteError> {
    let mut plans: Vec<PartPlan<'_>> = Vec::new();
    for (part, handle) in input.parts.parts.iter().enumerate() {
        if !has_segment[part] {
            continue;
        }
        match plan_part(part, handle, input.key_set) {
            Ok(Some(plan)) => plans.push(plan),
            Ok(None) => figures.parts_uncovered += 1,
            Err(rejection) => {
                tracing::warn!(
                    part = hex::encode(&handle.blake3),
                    reason = %rejection,
                    "key-index leaf refs rejected; the part is left to tier 1"
                );
                figures.leaves_rejected += 1;
            }
        }
    }
    plans.sort_by(|a, b| {
        b.handle
            .entry_count
            .cmp(&a.handle.entry_count)
            .then(b.handle.watermark_hour.cmp(&a.handle.watermark_hour))
            .then(a.part.cmp(&b.part))
    });

    let mut answered = BTreeMap::new();
    // GETs charged against the ceiling: the exact count for a probed part,
    // the worst case for one in flight.
    let mut charged = 0u64;
    let mut next = 0usize;
    loop {
        let wave_start = next;
        while next < plans.len()
            && next < ceiling.parts
            && charged.saturating_add(plans[next].worst_gets) <= ceiling.leaf_gets
        {
            charged += plans[next].worst_gets;
            next += 1;
        }
        if next == wave_start {
            break;
        }
        // Indexes rather than `&PartPlan` items: a closure over a borrowed
        // item is not general over its lifetime, and the future would not be
        // `Send`.
        let plans_ref = &plans;
        let mut probes = futures::stream::iter(wave_start..next)
            .map(|i| async move {
                let plan = &plans_ref[i];
                probe_part(input, plan, leaf_accounting)
                    .await
                    .map(|(gets, outcome)| (plan, gets, outcome))
            })
            .buffer_unordered(PROBE_CONCURRENCY);
        while let Some(probed) = probes.next().await {
            let (plan, gets, outcome) = probed?;
            charged = charged.saturating_sub(plan.worst_gets).saturating_add(gets);
            match outcome {
                PartOutcome::Answered(answer) => {
                    figures.parts_covered += 1;
                    answered.insert(plan.part, answer);
                }
                PartOutcome::Rejected => figures.leaves_rejected += 1,
                PartOutcome::OverBudget => figures.skipped_by_budget += 1,
                PartOutcome::ReservationRefused => figures.reservations_refused += 1,
            }
        }
    }
    figures.parts_beyond_ceiling = (plans.len() - next) as u64;
    Ok(answered)
}

/// Probes every slice of one part, stopping at the first that does not
/// answer. Returns the reads issued and the outcome.
async fn probe_part(
    input: &RouteInput<'_>,
    plan: &PartPlan<'_>,
    leaf_accounting: &QueryAccounting,
) -> Result<(u64, PartOutcome), RouteError> {
    let request = KeyIndexLeafRequest {
        tenant_hash: input.tenant_hash,
        signal: input.signal,
        part_blake3: &plan.handle.blake3,
        part_entry_count: plan.handle.entry_count,
        field: &input.key_set.field,
        key_type: input.key_set.key_type,
    };
    let policy = KeyIndexSetPolicy::for_part(plan.handle.entry_count);
    let mut gets = 0u64;
    let mut answer = PartAnswer {
        blocks: BTreeMap::new(),
        uncovered: BTreeSet::new(),
    };
    for (leaf_ref, keys) in &plan.slices {
        let leaf_ref: &SnapshotKeyIndexLeafRef = leaf_ref;
        let probe = input
            .catalog
            .probe_key_index_leaf_set_with(
                leaf_ref,
                &request,
                keys,
                &input.limits,
                |range| input.reader.read_range(leaf_ref, range, leaf_accounting),
                &policy,
                |bytes| input.budget.try_reserve(bytes).ok(),
            )
            .await
            .map_err(|source| RouteError::Isolation {
                leaf_key: leaf_ref.key.clone(),
                source,
            })?;
        gets += u64::from(probe.get_requests);
        leaf_accounting.add_decompressed_bytes(probe.decompressed_bytes);
        match probe.answer {
            KeyIndexSetAnswer::Answered {
                per_key,
                uncovered_entry_ordinals,
            } => {
                answer.uncovered.extend(uncovered_entry_ordinals);
                for key_answer in per_key {
                    if let KeyIndexKeyAnswer::Matches(matches) = key_answer {
                        for m in matches {
                            answer
                                .blocks
                                .entry(m.entry_ordinal)
                                .or_default()
                                .extend(m.blocks);
                        }
                    }
                }
            }
            KeyIndexSetAnswer::Rejected(rejection) => {
                tracing::warn!(
                    leaf = %leaf_ref.key,
                    reason = %rejection,
                    "key-index leaf rejected; the part is left to tier 1"
                );
                return Ok((gets, PartOutcome::Rejected));
            }
            KeyIndexSetAnswer::Declined(KeyIndexSetDecline::OverBudget { .. }) => {
                return Ok((gets, PartOutcome::OverBudget));
            }
            KeyIndexSetAnswer::Declined(KeyIndexSetDecline::ReservationRefused { .. }) => {
                return Ok((gets, PartOutcome::ReservationRefused));
            }
        }
    }
    Ok((gets, PartOutcome::Answered(answer)))
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests;
