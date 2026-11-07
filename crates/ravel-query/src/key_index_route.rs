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
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use futures::StreamExt;
use futures::future::BoxFuture;
use ravel_catalog::{
    Catalog, CatalogError, KEY_INDEX_LEAF_GET_CEILING, KEY_INDEX_LEAF_READER_MAX_BUCKET_BITS,
    KEY_INDEX_MAX_PROBED_PARTS, KEY_INDEX_SET_PROBE_MAX_RUNS, KeyIndexKeyAnswer,
    KeyIndexLeafBreach, KeyIndexLeafError, KeyIndexLeafLimits, KeyIndexLeafRejection,
    KeyIndexLeafRequest, KeyIndexSetAnswer, KeyIndexSetDecline, KeyIndexSetDecoder,
    KeyIndexSetPlan, KeyIndexSetPolicy, KeyIndexSetRunError, LEAF_READ_CONCURRENCY, Snapshot,
    SnapshotPartHandle, SnapshotParts, key_index_leaf_prefix_range, plan_key_index_leaf_set,
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

/// Why a [`LeafRangeReader`] returned no bytes.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LeafReadError {
    /// The reader's fetch reservation refused the range before any GET; the
    /// part counts as [`KeyIndexPartOutcome::ReservationRefused`].
    #[error("{0}")]
    ReservationRefused(String),
    /// The part counts as [`KeyIndexPartOutcome::Rejected`].
    #[error("{0}")]
    Failed(String),
}

impl From<String> for LeafReadError {
    fn from(message: String) -> Self {
        LeafReadError::Failed(message)
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
    ) -> BoxFuture<'a, Result<Bytes, LeafReadError>>;
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
    ) -> BoxFuture<'a, Result<Bytes, LeafReadError>> {
        Box::pin(async move {
            accounting.record_s3_request(AccountedOp::Get);
            let got = self
                .store
                .get(&leaf.key, GetRange::Range(range.start, range.end))
                .await
                .map_err(|e| LeafReadError::Failed(e.to_string()))?;
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
    /// The statement fails, and the catalog's breach counter has moved by
    /// one for it: every breach the route counts fails its statement.
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

/// The `outcome` label of `ravel_query_key_index_parts_total`: the six part
/// outcomes of [`RouteFigures`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum KeyIndexPartOutcome {
    Covered,
    Uncovered,
    BeyondCeiling,
    Rejected,
    ReservationRefused,
    SkippedByBudget,
}

impl KeyIndexPartOutcome {
    pub const ALL: [KeyIndexPartOutcome; 6] = [
        KeyIndexPartOutcome::Covered,
        KeyIndexPartOutcome::Uncovered,
        KeyIndexPartOutcome::BeyondCeiling,
        KeyIndexPartOutcome::Rejected,
        KeyIndexPartOutcome::ReservationRefused,
        KeyIndexPartOutcome::SkippedByBudget,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            KeyIndexPartOutcome::Covered => "covered",
            KeyIndexPartOutcome::Uncovered => "uncovered",
            KeyIndexPartOutcome::BeyondCeiling => "beyond_ceiling",
            KeyIndexPartOutcome::Rejected => "rejected",
            KeyIndexPartOutcome::ReservationRefused => "reservation_refused",
            KeyIndexPartOutcome::SkippedByBudget => "skipped_by_budget",
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

impl RouteFigures {
    /// This pass's count for `outcome`.
    pub fn parts(&self, outcome: KeyIndexPartOutcome) -> u64 {
        match outcome {
            KeyIndexPartOutcome::Covered => self.parts_covered,
            KeyIndexPartOutcome::Uncovered => self.parts_uncovered,
            KeyIndexPartOutcome::BeyondCeiling => self.parts_beyond_ceiling,
            KeyIndexPartOutcome::Rejected => self.leaves_rejected,
            KeyIndexPartOutcome::ReservationRefused => self.reservations_refused,
            KeyIndexPartOutcome::SkippedByBudget => self.skipped_by_budget,
        }
    }
}

/// The signals the router serves, as metric rows.
const ROUTED_SIGNALS: [Signal; 2] = [Signal::Spans, Signal::Logs];

/// Process totals behind `ravel_query_key_index_parts_total{signal,
/// outcome}` and `ravel_query_key_index_leaf_gets_total{signal}`.
struct RouteTotals {
    parts: [[AtomicU64; 6]; ROUTED_SIGNALS.len()],
    leaf_gets: [AtomicU64; ROUTED_SIGNALS.len()],
}

impl RouteTotals {
    const fn new() -> Self {
        RouteTotals {
            parts: [const { [const { AtomicU64::new(0) }; 6] }; ROUTED_SIGNALS.len()],
            leaf_gets: [const { AtomicU64::new(0) }; ROUTED_SIGNALS.len()],
        }
    }

    fn row(signal: Signal) -> Option<usize> {
        ROUTED_SIGNALS.iter().position(|s| *s == signal)
    }

    fn add_leaf_gets(&self, signal: Signal, gets: u64) {
        if let Some(row) = Self::row(signal) {
            self.leaf_gets[row].fetch_add(gets, Ordering::Relaxed);
        }
    }

    fn add_parts(&self, signal: Signal, figures: &RouteFigures) {
        if let Some(row) = Self::row(signal) {
            for outcome in KeyIndexPartOutcome::ALL {
                self.parts[row][outcome.index()]
                    .fetch_add(figures.parts(outcome), Ordering::Relaxed);
            }
        }
    }

    fn parts(&self, signal: Signal, outcome: KeyIndexPartOutcome) -> u64 {
        Self::row(signal).map_or(0, |row| {
            self.parts[row][outcome.index()].load(Ordering::Relaxed)
        })
    }

    fn leaf_gets(&self, signal: Signal) -> u64 {
        Self::row(signal).map_or(0, |row| self.leaf_gets[row].load(Ordering::Relaxed))
    }
}

static ROUTE_TOTALS: RouteTotals = RouteTotals::new();

/// The signals [`key_index_parts_total`] and [`key_index_leaf_gets_total`]
/// carry rows for.
pub fn key_index_routed_signals() -> &'static [Signal] {
    &ROUTED_SIGNALS
}

/// Parts this process's routing passes counted under `outcome`, for
/// `signal`, since start.
pub fn key_index_parts_total(signal: Signal, outcome: KeyIndexPartOutcome) -> u64 {
    ROUTE_TOTALS.parts(signal, outcome)
}

/// Leaf GETs this process's routing passes issued for `signal` since start,
/// including those of a pass dropped before it finished.
pub fn key_index_leaf_gets_total(signal: Signal) -> u64 {
    ROUTE_TOTALS.leaf_gets(signal)
}

/// Routes every segment of `input.snapshot` for `input.key_set`. The routes
/// are parallel to `snapshot.segments`. A leaf breach is the only
/// statement-failing outcome besides a `parts`/`snapshot` mismatch; every
/// other failure leaves the part [`SegmentRoute::Uncovered`] and counts it.
pub async fn route(input: RouteInput<'_>) -> Result<(Vec<SegmentRoute>, RouteFigures), RouteError> {
    route_under(input, ADR_CEILING, &ROUTE_TOTALS).await
}

/// [`route`] with the part ceiling lowered to `max_parts`, never raised past
/// the ADR's `P_max`: a test reaches the beyond-ceiling outcome with a
/// handful of parts instead of a thousand.
pub async fn route_with_max_parts(
    input: RouteInput<'_>,
    max_parts: usize,
) -> Result<(Vec<SegmentRoute>, RouteFigures), RouteError> {
    let ceiling = Ceiling {
        parts: max_parts.min(ADR_CEILING.parts),
        ..ADR_CEILING
    };
    route_under(input, ceiling, &ROUTE_TOTALS).await
}

/// Owns the leaf reads' accounting for one pass and merges it into the
/// caller's `keyIndex` handle when dropped, so a pass that errors, or whose
/// future is dropped mid-probe, still charges every GET it issued.
struct LeafCharge<'a> {
    leaf: QueryAccounting,
    into: &'a PhaseAccounting,
    totals: &'a RouteTotals,
    signal: Signal,
}

impl Drop for LeafCharge<'_> {
    fn drop(&mut self) {
        let leaf = self.leaf.snapshot();
        self.into.key_index().merge_snapshot(&leaf);
        self.totals
            .add_leaf_gets(self.signal, leaf.s3_requests(AccountedOp::Get));
    }
}

async fn route_under(
    input: RouteInput<'_>,
    ceiling: Ceiling,
    totals: &RouteTotals,
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
    // only.
    let charge = LeafCharge {
        leaf: QueryAccounting::new(),
        into: input.accounting,
        totals,
        signal: input.signal,
    };
    charge
        .leaf
        .set_access_intent(input.accounting.key_index().access_intent());
    let answered = route_parts(&input, ceiling, &has_segment, &charge.leaf, &mut figures).await;
    let leaf = charge.leaf.snapshot();
    drop(charge);
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
    totals.add_parts(input.signal, &figures);
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

/// How one slice's GET 1 ended.
enum SliceStart {
    /// The plan, and GET 1's bytes, which carry a production reader's fetch
    /// reservation for as long as they are alive.
    Planned(Box<KeyIndexSetPlan>, Bytes),
    /// The reader's fetch reservation refused GET 1.
    ReadRefused,
    Rejected(KeyIndexLeafRejection),
    Breach(KeyIndexLeafBreach),
}

/// What decides a part at the first slice that does not go on to phase 3,
/// unless an earlier slice's run decides it first.
enum SliceStop<'p> {
    Outcome(PartOutcome),
    /// Logged only if it decides the part.
    Rejected(&'p SnapshotKeyIndexLeafRef, KeyIndexLeafRejection),
}

fn leaf_rejected(leaf_ref: &SnapshotKeyIndexLeafRef, rejection: &KeyIndexLeafRejection) {
    tracing::warn!(
        leaf = %leaf_ref.key,
        reason = %rejection,
        "key-index leaf rejected; the part is left to tier 1"
    );
}

fn merge_answer(answer: &mut PartAnswer, slice: KeyIndexSetAnswer) -> Result<(), PartOutcome> {
    match slice {
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
            Ok(())
        }
        KeyIndexSetAnswer::Rejected(_) => Err(PartOutcome::Rejected),
        KeyIndexSetAnswer::Declined(KeyIndexSetDecline::OverBudget { .. }) => {
            Err(PartOutcome::OverBudget)
        }
        KeyIndexSetAnswer::Declined(KeyIndexSetDecline::ReservationRefused { .. }) => {
            Err(PartOutcome::ReservationRefused)
        }
    }
}

/// Probes every slice of one part in three phases, and returns the reads
/// issued and the outcome.
///
/// 1. GET 1 of every slice, at most [`LEAF_READ_CONCURRENCY`] in flight, and
///    each slice's plan from it.
/// 2. No reads: if GET 1 of any slice showed a breach, the first breaching
///    slice in slice order is counted once by
///    [`Catalog::key_index_leaf_breach`] and the part fails the statement
///    with [`RouteError::Isolation`]; phase 3 does not start. Otherwise the
///    slices in order, each plan's bridged bytes charged against the part's
///    allowance. The stopping slice is the first that did not plan (a
///    refused read or a rejection) or whose bridged bytes exceed what the
///    earlier slices left.
/// 3. The bucket runs of the slices before the stopping one, at most
///    [`LEAF_READ_CONCURRENCY`] in flight across the part, decoded in body
///    order.
///
/// Without a breach, the outcome is the one probing the slices one after
/// another gives: the first slice in slice order whose run fails to read or
/// decode, or whose decode reservation is refused, decides; failing that,
/// the stopping slice does (a refused read is a refused reservation, a
/// rejection rejects, an exceeded allowance declines the part over budget);
/// failing that, the part answers.
///
/// A breach is the one departure from that order. Phase 1 reads GET 1 of
/// slices the serial probe would never have reached (those past a failing
/// run or past the stopping slice), and ADR-0050 section 2 makes a
/// tenant_hash mismatch fail the query wherever it is seen, so a breach
/// fails the statement even where an earlier slice's run or an earlier
/// stopping slice would have decided the part.
///
/// The bytes read, and so the GET count, wire bytes and decompressed bytes,
/// are those of probing the slices one after another whenever the part
/// answers. A part the stopping slice decides, or a breach fails, has also
/// read GET 1 of the slices after it. A part decided in phase 3 has also read GET 1 of the
/// slices after the failing one and may have issued up to
/// `LEAF_READ_CONCURRENCY - 1` runs past the failing run. All of those
/// reads are counted. [`PartPlan::worst_gets`] still bounds the count:
/// every slice issues GET 1 and at most its worst case of runs.
///
/// Memory: at the end of phase 1 the part holds every slice's GET 1 bytes
/// and plan. From phase 2 on it holds those of the slices before the
/// stopping one, each until that slice's last run decodes (a slice with no
/// runs, until the part's last run does); a production reader's fetch
/// reservation for GET 1 is released with its bytes. On top of that it
/// holds at most `LEAF_READ_CONCURRENCY` runs' bytes, in flight or waiting
/// their turn to decode, and the decode reservations of the slices not yet
/// finished.
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
    let part_policy = KeyIndexSetPolicy::for_part(plan.handle.entry_count);
    let gets = AtomicU64::new(0);

    let starts: Vec<SliceStart> = {
        let (request, gets) = (&request, &gets);
        futures::stream::iter(0..plan.slices.len())
            .map(|s| async move {
                let (leaf_ref, keys) = &plan.slices[s];
                let range = match key_index_leaf_prefix_range(leaf_ref) {
                    Ok(range) => range,
                    Err(rejection) => return SliceStart::Rejected(rejection),
                };
                gets.fetch_add(1, Ordering::Relaxed);
                let bytes = match input
                    .reader
                    .read_range(leaf_ref, range, leaf_accounting)
                    .await
                {
                    Ok(bytes) => bytes,
                    Err(LeafReadError::ReservationRefused(_)) => return SliceStart::ReadRefused,
                    Err(LeafReadError::Failed(e)) => {
                        return SliceStart::Rejected(KeyIndexLeafRejection::Read(e));
                    }
                };
                match plan_key_index_leaf_set(&bytes, leaf_ref, request, keys, part_policy.b_run) {
                    Ok(set_plan) => SliceStart::Planned(Box::new(set_plan), bytes),
                    Err(KeyIndexLeafError::Breach(breach)) => SliceStart::Breach(breach),
                    Err(KeyIndexLeafError::Rejected(rejection)) => SliceStart::Rejected(rejection),
                }
            })
            .buffered(LEAF_READ_CONCURRENCY)
            .collect()
            .await
    };

    // The bridged-byte allowance is the part's, `entry_count x
    // KEY_INDEX_T1_BYTES` (the tier-1 cost the part's segments would
    // otherwise pay), shared by every slice the key set tiles, so a set
    // across S slices cannot read S times the part's allowance.
    let mut bridged_left = part_policy.max_bridged_bytes;
    let mut set_plans: Vec<KeyIndexSetPlan> = Vec::with_capacity(starts.len());
    // Parallel to `set_plans`: each slice's GET 1 bytes, dropped when the
    // slice finishes.
    let mut prefixes: Vec<Option<Bytes>> = Vec::with_capacity(starts.len());
    let mut stop = None;
    for ((leaf_ref, _), start) in plan.slices.iter().zip(starts) {
        let leaf_ref: &SnapshotKeyIndexLeafRef = leaf_ref;
        match start {
            SliceStart::Breach(breach) => {
                let source = input.catalog.key_index_leaf_breach(leaf_ref, breach);
                tracing::warn!(
                    leaf = %leaf_ref.key,
                    error = %source,
                    "key-index leaf binds another tenant or signal"
                );
                return Err(RouteError::Isolation {
                    leaf_key: leaf_ref.key.clone(),
                    source,
                });
            }
            _ if stop.is_some() => {}
            SliceStart::Planned(set_plan, prefix) => {
                if set_plan.bridged_bytes() > bridged_left {
                    stop = Some(SliceStop::Outcome(PartOutcome::OverBudget));
                } else {
                    bridged_left -= set_plan.bridged_bytes();
                    set_plans.push(*set_plan);
                    prefixes.push(Some(prefix));
                }
            }
            SliceStart::ReadRefused => {
                stop = Some(SliceStop::Outcome(PartOutcome::ReservationRefused));
            }
            SliceStart::Rejected(rejection) => {
                stop = Some(SliceStop::Rejected(leaf_ref, rejection));
            }
        }
    }

    let mut decoders: Vec<Option<KeyIndexSetDecoder<'_, LeafDecodeGuard>>> =
        set_plans.iter().map(|p| Some(p.decoder())).collect();
    let mut answer = PartAnswer {
        blocks: BTreeMap::new(),
        uncovered: BTreeSet::new(),
    };
    let mut decompressed = 0u64;
    let mut reserve = |bytes| input.budget.try_reserve(bytes).ok();
    // Finishing a slice as soon as its last run decodes releases its decode
    // reservations before the rest of the part's runs arrive.
    let finish = |s: usize,
                  decoder: KeyIndexSetDecoder<'_, LeafDecodeGuard>,
                  decompressed: &mut u64,
                  answer: &mut PartAnswer|
     -> Result<(), PartOutcome> {
        *decompressed += decoder.decompressed_bytes();
        let slice = decoder.finish();
        if let KeyIndexSetAnswer::Rejected(rejection) = &slice {
            leaf_rejected(plan.slices[s].0, rejection);
        }
        merge_answer(answer, slice)
    };
    let mut failure = None;
    {
        let runs: Vec<(usize, Range<u64>)> = set_plans
            .iter()
            .enumerate()
            .flat_map(|(s, p)| p.runs().iter().map(move |run| (s, run.clone())))
            .collect();
        let gets = &gets;
        let mut reads = futures::stream::iter(runs)
            .map(|(s, range)| {
                gets.fetch_add(1, Ordering::Relaxed);
                let read = input
                    .reader
                    .read_range(plan.slices[s].0, range, leaf_accounting);
                async move { (s, read.await) }
            })
            .buffered(LEAF_READ_CONCURRENCY);
        while let Some((s, read)) = reads.next().await {
            let leaf_ref = plan.slices[s].0;
            let decoded = match read {
                Err(LeafReadError::ReservationRefused(_)) => Err(PartOutcome::ReservationRefused),
                Err(LeafReadError::Failed(e)) => {
                    leaf_rejected(leaf_ref, &KeyIndexLeafRejection::Read(e));
                    Err(PartOutcome::Rejected)
                }
                Ok(bytes) => {
                    let slot = &mut decoders[s];
                    let step = match slot.as_mut() {
                        Some(decoder) => {
                            decoder.decode_next_run(&bytes, &input.limits, &mut reserve)
                        }
                        // A slice's decoder is finished only after its last
                        // run, so this is a run its plan does not hold.
                        None => Err(KeyIndexSetRunError::Rejected(
                            KeyIndexLeafRejection::RequestMismatch(
                                "a bucket run past the slice's plan".to_string(),
                            ),
                        )),
                    };
                    match step {
                        Ok(()) => match slot.take_if(|d| d.is_done()) {
                            Some(decoder) => {
                                prefixes[s] = None;
                                finish(s, decoder, &mut decompressed, &mut answer)
                            }
                            None => Ok(()),
                        },
                        Err(KeyIndexSetRunError::Rejected(rejection)) => {
                            leaf_rejected(leaf_ref, &rejection);
                            Err(PartOutcome::Rejected)
                        }
                        Err(KeyIndexSetRunError::ReservationRefused { .. }) => {
                            Err(PartOutcome::ReservationRefused)
                        }
                    }
                }
            };
            if let Err(outcome) = decoded {
                failure = Some(outcome);
                break;
            }
        }
    }
    let outcome = match (failure, stop) {
        (Some(outcome), _) => outcome,
        (None, Some(SliceStop::Outcome(outcome))) => outcome,
        (None, Some(SliceStop::Rejected(leaf_ref, rejection))) => {
            leaf_rejected(leaf_ref, &rejection);
            PartOutcome::Rejected
        }
        (None, None) => {
            // Slices whose keys all fell in empty buckets have no runs.
            let mut finished = Ok(());
            for (s, slot) in decoders.iter_mut().enumerate() {
                if let Some(decoder) = slot.take() {
                    finished = finish(s, decoder, &mut decompressed, &mut answer);
                    if finished.is_err() {
                        break;
                    }
                }
            }
            match finished {
                Ok(()) => PartOutcome::Answered(answer),
                Err(outcome) => outcome,
            }
        }
    };
    decompressed += decoders
        .iter()
        .flatten()
        .map(|d| d.decompressed_bytes())
        .sum::<u64>();
    leaf_accounting.add_decompressed_bytes(decompressed);
    drop(prefixes);
    Ok((gets.load(Ordering::Relaxed), outcome))
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests;
