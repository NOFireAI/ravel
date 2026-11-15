//! The metrics series route (ADR-4146 decisions 3 and 4), tried before the
//! label-value route on every part that carries the three series fields.
//!
//! A routed plan is a selector plan with at least two distinct positive
//! equality pairs. Per part, three passes run, each one set probe shared by
//! the plans admitted to it:
//!
//! - pass A probes `series_label` with the plans' pair keys, and each plan
//!   intersects its own pairs' postings into its series keys `S`;
//! - pass B probes `series_class` with the union of the admitted `S`, giving
//!   each plan its classes `C`;
//! - pass C probes `class_objects` with the union of the admitted `C`, and
//!   each plan keeps the entries its `C` names plus every entry a probed
//!   leaf lists as uncovered.
//!
//! A plan whose `S` is empty keeps only pass A's uncovered entries. A plan a
//! pass does not admit, and every plan still in flight on a part that
//! declines as a whole, goes to the label-value route for that part.
//!
//! A posting element of `series_label` is a series key read as a big-endian
//! `u64`, so that ascending elements are ascending `key8`s; a class ordinal is
//! looked up in `class_objects` as `key8_i64` of the ordinal.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

use bytes::Bytes;
use futures::StreamExt;
use ravel_catalog::{
    KEY_INDEX_LEAF_READER_MAX_BUCKET_BITS, KEY_INDEX_MAX_PROBED_PARTS, KEY_INDEX_SET_PROBE_MAX_RUNS,
    KeyIndexKeyAnswer, KeyIndexLeafError, KeyIndexLeafLimits, KeyIndexLeafRejection,
    KeyIndexLeafRequest, KeyIndexPostingSetAnswer, KeyIndexPostingSetDecoder, KeyIndexSetAnswer,
    KeyIndexSetDecline, KeyIndexSetDecoder, KeyIndexSetPlan, KeyIndexSetRunError,
    LEAF_READ_CONCURRENCY, METRICS_CLASS_OBJECTS_FIELD, METRICS_SERIES_CLASS_FIELD,
    METRICS_SERIES_LABEL_FIELD, Snapshot, SnapshotPartHandle, SnapshotParts,
    key_index_leaf_prefix_range, plan_key_index_leaf_set, select_key_index_slice,
    series_label_pair_key8,
};
use ravel_codec::key_idx::{Key8, KeyType, key8_i64};
use ravel_proto::catalog::v1::SnapshotKeyIndexLeafRef;
use ravel_types::accounting::{AccountedOp, QueryAccounting};

use super::{
    ADR_CEILING, LeafCharge, LeafDecodeGuard, LeafReadError, PROBE_CONCURRENCY, ROUTE_TOTALS,
    RouteError, RouteTotals, SliceStart, TIER_ONE_GETS_PER_ENTRY, ValueRouteFigures,
    ValueRouteInput, ValueRouting, leaf_rejected, route_values_masked,
};

/// The most series keys the plans admitted to pass B may hold together
/// (ADR-4146 decision 3 step 2).
pub const SERIES_ROUTE_S_MAX: usize = 192;

/// Pass A's on-object byte allowance past its prefixes, per part.
pub const SERIES_ROUTE_Q_A: u64 = 8 * 1024 * 1024;

/// The on-object byte allowance passes B and C share past their prefixes,
/// per part.
pub const SERIES_ROUTE_Q_BC: u64 = 4 * 1024 * 1024;

const B_RUN: usize = KEY_INDEX_SET_PROBE_MAX_RUNS;

/// The series route's bounds. [`Default`] is the ADR's; the engine overrides
/// `s_max` only for tests and the acceptance run's large-intersection check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeriesRouteOptions {
    pub s_max: usize,
    pub q_a: u64,
    pub q_bc: u64,
    /// The most parts the route probes, never raised past
    /// [`KEY_INDEX_MAX_PROBED_PARTS`].
    pub max_parts: usize,
}

impl Default for SeriesRouteOptions {
    fn default() -> Self {
        SeriesRouteOptions {
            s_max: SERIES_ROUTE_S_MAX,
            q_a: SERIES_ROUTE_Q_A,
            q_bc: SERIES_ROUTE_Q_BC,
            max_parts: KEY_INDEX_MAX_PROBED_PARTS,
        }
    }
}

/// One of the series route's three passes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum SeriesPass {
    A,
    B,
    C,
}

impl SeriesPass {
    pub const ALL: [SeriesPass; 3] = [SeriesPass::A, SeriesPass::B, SeriesPass::C];

    pub fn as_str(self) -> &'static str {
        match self {
            SeriesPass::A => "A",
            SeriesPass::B => "B",
            SeriesPass::C => "C",
        }
    }

    /// The leaf field the pass probes.
    pub fn field(self) -> &'static str {
        match self {
            SeriesPass::A => METRICS_SERIES_LABEL_FIELD,
            SeriesPass::B => METRICS_SERIES_CLASS_FIELD,
            SeriesPass::C => METRICS_CLASS_OBJECTS_FIELD,
        }
    }

    fn key_type(self) -> KeyType {
        match self {
            SeriesPass::A => KeyType::Bytes,
            SeriesPass::B => KeyType::Id16,
            SeriesPass::C => KeyType::I64,
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

/// One series pass's figures, summed over the parts it ran on.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SeriesPassFigures {
    /// Parts on which the pass read its prefixes.
    pub parts_probed: u64,
    /// Plans offered to the pass.
    pub plans_offered: u64,
    /// Plans the pass admitted.
    pub plans_admitted: u64,
    /// Distinct keys of the offered plans' union: `K` for pass A, the `M`
    /// and `M_c` offered for B and C.
    pub keys_offered: u64,
    /// Distinct keys of the admitted plans' union, probed past the prefixes.
    pub keys_probed: u64,
    /// [`KeyIndexSetPlan::distinct_buckets`] of the offered union, summed
    /// over the slices it falls in.
    pub distinct_buckets: u64,
    /// [`KeyIndexSetPlan::bridged_bytes`] of the offered union, summed over
    /// its slices: 0 where a slice's buckets are read one by one.
    pub bridged_bytes: u64,
    /// On-object bytes of the prefixes read (GET 1 of each slice).
    pub prefix_bytes: u64,
    /// On-object bytes read past the prefixes: the admitted union's runs.
    pub bytes_past_prefix: u64,
    /// Leaf GETs the pass issued, prefixes included, charged to the
    /// `keyIndex` phase.
    pub leaf_get_requests: u64,
    /// Wire bytes of those GETs.
    pub leaf_wire_bytes: u64,
    /// Leaf reads the reader's cache served with no GET.
    pub leaf_cache_hits: u64,
}

impl SeriesPassFigures {
    fn add(&mut self, other: &SeriesPassFigures) {
        self.parts_probed += other.parts_probed;
        self.plans_offered += other.plans_offered;
        self.plans_admitted += other.plans_admitted;
        self.keys_offered += other.keys_offered;
        self.keys_probed += other.keys_probed;
        self.distinct_buckets += other.distinct_buckets;
        self.bridged_bytes += other.bridged_bytes;
        self.prefix_bytes += other.prefix_bytes;
        self.bytes_past_prefix += other.bytes_past_prefix;
    }
}

/// Why a routed plan went to the label-value route on a part.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum SeriesDecline {
    /// The part lacks a ref for one of the three series fields.
    NoSeriesRefs,
    /// A series leaf or its refs did not answer, or the three fields
    /// disagree (a series key with no class, a class with no entry).
    Rejected,
    /// A pass's worst-case leaf GETs exceed 2 for each of the part's
    /// segments.
    OverTierOne,
    /// The route's part or leaf GET ceiling left the part unprobed.
    BeyondCeiling,
    /// Pass A did not admit the plan under `Q_A`.
    OverQA,
    /// Pass B's `S_max` pre-check did not admit the plan.
    OverSMax,
    /// Pass B or pass C did not admit the plan under its share of `Q_BC`.
    OverQBC,
    /// A shared probe's runs exceeded what its pass had left.
    OverBudget,
    /// A leaf read's fetch reservation or a frame's decode reservation was
    /// refused.
    ReservationRefused,
}

impl SeriesDecline {
    pub const ALL: [SeriesDecline; 9] = [
        SeriesDecline::NoSeriesRefs,
        SeriesDecline::Rejected,
        SeriesDecline::OverTierOne,
        SeriesDecline::BeyondCeiling,
        SeriesDecline::OverQA,
        SeriesDecline::OverSMax,
        SeriesDecline::OverQBC,
        SeriesDecline::OverBudget,
        SeriesDecline::ReservationRefused,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            SeriesDecline::NoSeriesRefs => "no_series_refs",
            SeriesDecline::Rejected => "rejected",
            SeriesDecline::OverTierOne => "over_tier_one",
            SeriesDecline::BeyondCeiling => "beyond_ceiling",
            SeriesDecline::OverQA => "over_q_a",
            SeriesDecline::OverSMax => "over_s_max",
            SeriesDecline::OverQBC => "over_q_bc",
            SeriesDecline::OverBudget => "over_budget",
            SeriesDecline::ReservationRefused => "reservation_refused",
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

/// The series route's figures for one statement. Every (routed plan, part
/// with a segment in the snapshot) pair counts once: answered, or under one
/// decline.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SeriesRouteFigures {
    /// Plans with at least two distinct positive equality pairs.
    pub plans_routed: u64,
    /// (plan, part) pairs the series route answered.
    pub plan_parts_answered: u64,
    pub(crate) declines: [u64; SeriesDecline::ALL.len()],
    pub(crate) passes: [SeriesPassFigures; SeriesPass::ALL.len()],
}

impl SeriesRouteFigures {
    /// (plan, part) pairs sent to the label-value route for `reason`.
    pub fn declined(&self, reason: SeriesDecline) -> u64 {
        self.declines[reason.index()]
    }

    /// Every (plan, part) pair sent to the label-value route.
    pub fn plan_parts_declined(&self) -> u64 {
        self.declines.iter().sum()
    }

    pub fn pass(&self, pass: SeriesPass) -> &SeriesPassFigures {
        &self.passes[pass.index()]
    }
}

/// What [`route_metric_statement`] decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricRouting {
    /// Per plan, `Some(keep)` parallel to the snapshot's segments when the
    /// plan was routed, `None` when it reads every segment.
    pub plan_keeps: Vec<Option<Vec<bool>>>,
    /// The label-value passes, as [`ValueRouting::values`].
    pub values: Vec<ValueRouteFigures>,
    /// `None` when the series route did not run: no plan had two positive
    /// pairs, or no part with a segment in the snapshot has a series ref.
    pub series: Option<SeriesRouteFigures>,
    /// Segments no plan reads, on parts where the series route answered
    /// every plan of the statement.
    pub segments_pruned_by_series_index: u64,
    /// Segments no plan reads, on every other part. The two counters add up
    /// to the segments no plan reads, and both are zero when a plan reads
    /// every segment.
    pub segments_pruned_by_label_index: u64,
}

impl MetricRouting {
    pub fn unrouted(plans: usize) -> Self {
        MetricRouting {
            plan_keeps: vec![None; plans],
            values: Vec::new(),
            series: None,
            segments_pruned_by_series_index: 0,
            segments_pruned_by_label_index: 0,
        }
    }
}

/// Routes a metrics statement's plans: the series route on every part that
/// carries the series fields, then [`route_values`](super::route_values)
/// over the parts and plans the series route did not answer. `input.field`
/// is the label-value field. A plan the series route answers on a part keeps
/// that answer there and is not probed on the label-value field for that
/// part. With no series ref on any part a snapshot segment comes from, the
/// result is [`route_values`](super::route_values)'s. Nothing is routed while
/// the snapshot carries a pending erasure.
pub async fn route_metric_statement(
    input: ValueRouteInput<'_>,
    plans: &[Vec<(&str, &str)>],
    options: SeriesRouteOptions,
) -> Result<MetricRouting, RouteError> {
    route_metric_statement_under(input, plans, options, &ROUTE_TOTALS).await
}

pub(super) async fn route_metric_statement_under(
    input: ValueRouteInput<'_>,
    plans: &[Vec<(&str, &str)>],
    options: SeriesRouteOptions,
    totals: &RouteTotals,
) -> Result<MetricRouting, RouteError> {
    let snapshot = input.snapshot;
    let parts = input.parts;
    if !snapshot.pending_erasure.is_empty() || snapshot.segments.is_empty() {
        return Ok(MetricRouting::unrouted(plans.len()));
    }
    let series = route_series(&input, plans, options, totals).await?;
    let masks = series.as_ref().map(|series| series.label_value_masks(parts));
    let values = route_values_masked(input, plans, masks.as_deref(), ADR_CEILING, totals).await?;
    Ok(combine(snapshot, parts, series, values))
}

/// The series route's answers for one statement.
struct SeriesOutcome {
    figures: SeriesRouteFigures,
    /// Per plan: part -> the entry ordinals the plan keeps there.
    answers: Vec<BTreeMap<usize, BTreeSet<u64>>>,
}

impl SeriesOutcome {
    /// Per plan and part, whether the plan still needs the label-value route.
    fn label_value_masks(&self, parts: &SnapshotParts) -> Vec<Vec<bool>> {
        self.answers
            .iter()
            .map(|answers| {
                (0..parts.parts.len())
                    .map(|part| !answers.contains_key(&part))
                    .collect()
            })
            .collect()
    }
}

fn combine(
    snapshot: &Snapshot,
    parts: &SnapshotParts,
    series: Option<SeriesOutcome>,
    values: ValueRouting,
) -> MetricRouting {
    let segment_count = snapshot.segments.len();
    let mut plan_keeps = values.plan_keeps;
    if let Some(series) = &series {
        for (keep, answers) in plan_keeps.iter_mut().zip(&series.answers) {
            if answers.is_empty() {
                continue;
            }
            let keep = keep.get_or_insert_with(|| vec![true; segment_count]);
            for (kept, origin) in keep.iter_mut().zip(&parts.origin) {
                let Some(origin) = origin else {
                    continue;
                };
                let Some(entries) = answers.get(&(origin.part as usize)) else {
                    continue;
                };
                let ordinal = u64::from(origin.ordinal);
                let entry_count = parts.parts[origin.part as usize].entry_count;
                *kept = ordinal >= entry_count || entries.contains(&ordinal);
            }
        }
    }
    let mut read = vec![false; segment_count];
    let mut every_plan_routed = true;
    for keep in &plan_keeps {
        match keep {
            None => every_plan_routed = false,
            Some(keep) => {
                for (read, kept) in read.iter_mut().zip(keep) {
                    *read |= *kept;
                }
            }
        }
    }
    let (mut by_series, mut by_label) = (0u64, 0u64);
    if every_plan_routed {
        for (s, read) in read.iter().enumerate() {
            if *read {
                continue;
            }
            let series_answered = parts.origin[s].as_ref().is_some_and(|origin| {
                series.as_ref().is_some_and(|series| {
                    series
                        .answers
                        .iter()
                        .all(|answers| answers.contains_key(&(origin.part as usize)))
                })
            });
            if series_answered {
                by_series += 1;
            } else {
                by_label += 1;
            }
        }
    }
    MetricRouting {
        plan_keeps,
        values: values.values,
        series: series.map(|series| series.figures),
        segments_pruned_by_series_index: by_series,
        segments_pruned_by_label_index: by_label,
    }
}

/// The series keys of a plan's pairs, sorted and distinct, when it has at
/// least two distinct positive pairs; `None` for a plan the route skips.
fn plan_series_keys(pairs: &[(&str, &str)]) -> Option<Vec<Key8>> {
    let distinct: BTreeSet<(&str, &str)> = pairs
        .iter()
        .copied()
        .filter(|(_, value)| !value.is_empty())
        .collect();
    if distinct.len() < 2 {
        return None;
    }
    let keys: BTreeSet<Key8> = distinct
        .iter()
        .map(|(name, value)| series_label_pair_key8(name, value).ok())
        .collect::<Option<_>>()?;
    Some(keys.into_iter().collect())
}

/// Whether `handle` has a ref for each of the three series fields, and for
/// any of them.
fn series_refs(handle: &SnapshotPartHandle) -> (bool, bool) {
    let has = SeriesPass::ALL.map(|pass| {
        handle
            .key_index
            .iter()
            .any(|leaf| leaf.field == pass.field())
    });
    (has.iter().all(|h| *h), has.iter().any(|h| *h))
}

fn slice_worst_gets(leaf_ref: &SnapshotKeyIndexLeafRef, keys: u64) -> u64 {
    let bits = leaf_ref
        .bucket_bits
        .min(u32::from(KEY_INDEX_LEAF_READER_MAX_BUCKET_BITS));
    1 + keys.min(1u64 << bits).min(B_RUN as u64)
}

/// `keys` (sorted) grouped by the slice of `field` that holds each, in key
/// order.
type Slices<'a> = Vec<(&'a SnapshotKeyIndexLeafRef, Vec<Key8>)>;

fn slices_for<'a>(
    handle: &'a SnapshotPartHandle,
    field: &str,
    keys: &[Key8],
) -> Result<Option<Slices<'a>>, KeyIndexLeafRejection> {
    let mut slices: Slices<'a> = Vec::new();
    for key8 in keys {
        let Some(leaf_ref) = select_key_index_slice(&handle.key_index, field, key8)? else {
            return Ok(None);
        };
        match slices.last_mut() {
            Some((last, keys)) if std::ptr::eq(*last, leaf_ref) => keys.push(*key8),
            _ => slices.push((leaf_ref, vec![*key8])),
        }
    }
    Ok(Some(slices))
}

/// A part's worst-case leaf GETs per pass, for the GET ceiling: pass A over
/// the statement's pair keys, B and C over every slice of their field at
/// [`B_RUN`] keys or fewer, since their keys are not known yet.
fn worst_gets(
    handle: &SnapshotPartHandle,
    pair_keys: &[Key8],
) -> Result<[u64; 3], KeyIndexLeafRejection> {
    let a = match slices_for(handle, SeriesPass::A.field(), pair_keys)? {
        Some(slices) => slices
            .iter()
            .map(|(leaf_ref, keys)| slice_worst_gets(leaf_ref, keys.len() as u64))
            .sum(),
        None => 0,
    };
    let whole_field = |pass: SeriesPass| -> u64 {
        handle
            .key_index
            .iter()
            .filter(|leaf| leaf.field == pass.field())
            .map(|leaf| slice_worst_gets(leaf, B_RUN as u64))
            .sum()
    };
    Ok([a, whole_field(SeriesPass::B), whole_field(SeriesPass::C)])
}

/// On-object body bytes of every slice of `field` on the part.
fn field_body_bytes(handle: &SnapshotPartHandle, field: &str) -> u64 {
    handle
        .key_index
        .iter()
        .filter(|leaf| leaf.field == field)
        .map(|leaf| leaf.size.saturating_sub(u64::from(leaf.prefix_len)))
        .sum()
}

struct SeriesCtx<'a, 'b> {
    input: &'a ValueRouteInput<'b>,
    options: SeriesRouteOptions,
    charges: [&'a QueryAccounting; 3],
}

async fn route_series(
    input: &ValueRouteInput<'_>,
    plans: &[Vec<(&str, &str)>],
    options: SeriesRouteOptions,
    totals: &RouteTotals,
) -> Result<Option<SeriesOutcome>, RouteError> {
    let parts = input.parts;
    let segment_count = input.snapshot.segments.len();
    if parts.origin.len() != segment_count {
        return Err(RouteError::InvalidInput(format!(
            "{} part origins for {segment_count} segments",
            parts.origin.len()
        )));
    }
    let mut segments_in_snapshot = vec![0u64; parts.parts.len()];
    for origin in parts.origin.iter().flatten() {
        let Some(slot) = segments_in_snapshot.get_mut(origin.part as usize) else {
            return Err(RouteError::InvalidInput(format!(
                "segment origin names part {} of {}",
                origin.part,
                parts.parts.len()
            )));
        };
        *slot += 1;
    }
    let routed: Vec<(usize, Vec<Key8>)> = plans
        .iter()
        .enumerate()
        .filter_map(|(plan, pairs)| plan_series_keys(pairs).map(|keys| (plan, keys)))
        .collect();
    if routed.is_empty() {
        return Ok(None);
    }
    let any_series_ref = parts
        .parts
        .iter()
        .zip(&segments_in_snapshot)
        .any(|(handle, segments)| *segments > 0 && series_refs(handle).1);
    if !any_series_ref {
        return Ok(None);
    }

    let mut outcome = SeriesOutcome {
        figures: SeriesRouteFigures {
            plans_routed: routed.len() as u64,
            ..SeriesRouteFigures::default()
        },
        answers: vec![BTreeMap::new(); plans.len()],
    };
    let decline_part = |figures: &mut SeriesRouteFigures, reason: SeriesDecline| {
        figures.declines[reason.index()] += routed.len() as u64;
    };
    let pair_keys: Vec<Key8> = routed
        .iter()
        .flat_map(|(_, keys)| keys.iter().copied())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let mut candidates: Vec<(usize, [u64; 3])> = Vec::new();
    for (part, handle) in parts.parts.iter().enumerate() {
        if segments_in_snapshot[part] == 0 {
            continue;
        }
        if !series_refs(handle).0 {
            decline_part(&mut outcome.figures, SeriesDecline::NoSeriesRefs);
            continue;
        }
        match worst_gets(handle, &pair_keys) {
            Ok(worst) => candidates.push((part, worst)),
            Err(rejection) => {
                tracing::warn!(
                    part = hex::encode(&handle.blake3),
                    reason = %rejection,
                    "series leaf refs rejected; the part is left to the label-value route"
                );
                decline_part(&mut outcome.figures, SeriesDecline::Rejected);
            }
        }
    }
    candidates.sort_by(|(a, _), (b, _)| {
        let (a_handle, b_handle) = (&parts.parts[*a], &parts.parts[*b]);
        b_handle
            .entry_count
            .cmp(&a_handle.entry_count)
            .then(b_handle.watermark_hour.cmp(&a_handle.watermark_hour))
            .then(a.cmp(b))
    });
    // Each pass's ceiling is the label-value route's for its key count, the
    // keys of passes B and C taken at `B_run`.
    let max_parts = options.max_parts.min(KEY_INDEX_MAX_PROBED_PARTS);
    let per_part = |keys: usize| 1 + keys.min(B_RUN) as u64;
    let ceilings = [
        max_parts as u64 * per_part(pair_keys.len()),
        max_parts as u64 * per_part(B_RUN),
        max_parts as u64 * per_part(B_RUN),
    ];
    let mut charged = [0u64; 3];
    let mut admitted: Vec<usize> = Vec::new();
    for (i, (part, worst)) in candidates.iter().enumerate() {
        let fits = (0..3).all(|p| charged[p].saturating_add(worst[p]) <= ceilings[p]);
        if admitted.len() >= max_parts || !fits {
            for _ in i..candidates.len() {
                decline_part(&mut outcome.figures, SeriesDecline::BeyondCeiling);
            }
            break;
        }
        for p in 0..3 {
            charged[p] += worst[p];
        }
        admitted.push(*part);
    }

    let charges = SeriesPass::ALL.map(|_| {
        let charge = LeafCharge {
            leaf: QueryAccounting::new(),
            into: input.accounting,
            totals,
            signal: input.signal,
        };
        charge
            .leaf
            .set_access_intent(input.accounting.key_index().access_intent());
        charge
    });
    let ctx = SeriesCtx {
        input,
        options,
        charges: [&charges[0].leaf, &charges[1].leaf, &charges[2].leaf],
    };
    let probed: Result<Vec<PartSeries>, RouteError> = async {
        let (ctx, routed, admitted, segments) = (&ctx, &routed, &admitted, &segments_in_snapshot);
        let mut probes = futures::stream::iter(0..admitted.len())
            .map(|i| async move {
                let part = admitted[i];
                route_series_part(ctx, part, &parts.parts[part], segments[part], routed).await
            })
            .buffer_unordered(PROBE_CONCURRENCY);
        let mut probed = Vec::with_capacity(admitted.len());
        while let Some(part) = probes.next().await {
            probed.push(part?);
        }
        Ok(probed)
    }
    .await;
    for (pass, charge) in SeriesPass::ALL.iter().zip(&charges) {
        let leaf = charge.leaf.snapshot();
        let figures = &mut outcome.figures.passes[pass.index()];
        figures.leaf_get_requests = leaf.s3_requests(AccountedOp::Get);
        figures.leaf_wire_bytes = leaf.s3_bytes(AccountedOp::Get);
        figures.leaf_cache_hits = leaf.cache_hits;
    }
    drop(charges);
    for part in probed? {
        for (pass, figures) in part.passes.iter().enumerate() {
            outcome.figures.passes[pass].add(figures);
        }
        for (plan, entries) in part.answered {
            outcome.figures.plan_parts_answered += 1;
            outcome.answers[plan].insert(part.part, entries);
        }
        for (_, reason) in part.declined {
            outcome.figures.declines[reason.index()] += 1;
        }
    }
    Ok(Some(outcome))
}

/// One part's series answers.
struct PartSeries {
    part: usize,
    /// Plan -> the entry ordinals it keeps on the part.
    answered: Vec<(usize, BTreeSet<u64>)>,
    declined: Vec<(usize, SeriesDecline)>,
    /// GET figures are filled in by the caller from the pass's accounting.
    passes: [SeriesPassFigures; 3],
}

impl PartSeries {
    fn decline_all(&mut self, plans: &[usize], reason: SeriesDecline) {
        self.declined
            .extend(plans.iter().map(|plan| (*plan, reason)));
    }
}

/// The ascending values every posting holds.
fn intersect(postings: &[&[u64]]) -> Vec<u64> {
    let Some((first, rest)) = postings.split_first() else {
        return Vec::new();
    };
    let mut out: Vec<u64> = first.to_vec();
    out.sort_unstable();
    out.dedup();
    for posting in rest {
        let posting: BTreeSet<u64> = posting.iter().copied().collect();
        out.retain(|value| posting.contains(value));
    }
    out
}

async fn route_series_part(
    ctx: &SeriesCtx<'_, '_>,
    part: usize,
    handle: &SnapshotPartHandle,
    segments: u64,
    routed: &[(usize, Vec<Key8>)],
) -> Result<PartSeries, RouteError> {
    let mut out = PartSeries {
        part,
        answered: Vec::new(),
        declined: Vec::new(),
        passes: [SeriesPassFigures::default(); 3],
    };
    let options = ctx.options;
    let all: Vec<usize> = routed.iter().map(|(plan, _)| *plan).collect();

    let a = match run_pass(
        ctx,
        handle,
        SeriesPass::A,
        routed,
        options.q_a,
        segments,
        &mut out.passes[0],
    )
    .await?
    {
        PassRun::PartDeclined(reason) => {
            out.decline_all(&all, reason);
            return Ok(out);
        }
        PassRun::Ran(a) => a,
    };
    out.decline_all(&a.declined, SeriesDecline::OverQA);
    let mut uncovered = a.uncovered.clone();
    let mut with_s: Vec<(usize, Vec<Key8>)> = Vec::new();
    for plan in &a.admitted {
        let Some((_, pair_keys)) = routed.iter().find(|(p, _)| p == plan) else {
            continue;
        };
        let postings: Vec<&[u64]> = pair_keys
            .iter()
            .map(|key| a.answers.get(key).map_or(&[][..], Vec::as_slice))
            .collect();
        let s = intersect(&postings);
        if s.is_empty() {
            out.answered.push((*plan, uncovered.clone()));
        } else {
            with_s.push((*plan, s.into_iter().map(u64::to_be_bytes).collect()));
        }
    }
    if with_s.is_empty() {
        return Ok(out);
    }

    // Pass B's share of `Q_BC` leaves room for the whole `class_objects`
    // body, the most pass C can read.
    let b_share = options
        .q_bc
        .saturating_sub(field_body_bytes(handle, METRICS_CLASS_OBJECTS_FIELD));
    let offered_b = if field_body_bytes(handle, METRICS_SERIES_CLASS_FIELD) <= b_share {
        with_s
    } else {
        let mut order = with_s;
        order.sort_by_key(|(plan, s)| (s.len(), *plan));
        let mut union: BTreeSet<Key8> = BTreeSet::new();
        let mut admitted = Vec::new();
        let mut over: Vec<usize> = Vec::new();
        for (plan, s) in order {
            if over.is_empty() {
                let mut candidate = union.clone();
                candidate.extend(s.iter().copied());
                if candidate.len() <= options.s_max {
                    union = candidate;
                    admitted.push((plan, s));
                    continue;
                }
            }
            over.push(plan);
        }
        out.decline_all(&over, SeriesDecline::OverSMax);
        admitted.sort_by_key(|(plan, _)| *plan);
        admitted
    };
    if offered_b.is_empty() {
        return Ok(out);
    }
    let pending: Vec<usize> = offered_b.iter().map(|(plan, _)| *plan).collect();
    let b = match run_pass(
        ctx,
        handle,
        SeriesPass::B,
        &offered_b,
        b_share,
        segments,
        &mut out.passes[1],
    )
    .await?
    {
        PassRun::PartDeclined(reason) => {
            out.decline_all(&pending, reason);
            return Ok(out);
        }
        PassRun::Ran(b) => b,
    };
    out.decline_all(&b.declined, SeriesDecline::OverQBC);
    uncovered.extend(&b.uncovered);
    let mut with_c: Vec<(usize, Vec<Key8>)> = Vec::new();
    for plan in &b.admitted {
        let Some((_, s)) = offered_b.iter().find(|(p, _)| p == plan) else {
            continue;
        };
        let mut classes: BTreeSet<Key8> = BTreeSet::new();
        for skey in s {
            // Pass A named the series, so the leaf must give it a class.
            let Some(posting) = b.answers.get(skey) else {
                out.decline_all(&b.admitted, SeriesDecline::Rejected);
                return Ok(out);
            };
            for class in posting {
                let Ok(class) = i64::try_from(*class) else {
                    out.decline_all(&b.admitted, SeriesDecline::Rejected);
                    return Ok(out);
                };
                classes.insert(key8_i64(class));
            }
        }
        with_c.push((*plan, classes.into_iter().collect()));
    }
    if with_c.is_empty() {
        return Ok(out);
    }

    let pending: Vec<usize> = with_c.iter().map(|(plan, _)| *plan).collect();
    let c = match run_pass(
        ctx,
        handle,
        SeriesPass::C,
        &with_c,
        options.q_bc.saturating_sub(b.bytes_read),
        segments,
        &mut out.passes[2],
    )
    .await?
    {
        PassRun::PartDeclined(reason) => {
            out.decline_all(&pending, reason);
            return Ok(out);
        }
        PassRun::Ran(c) => c,
    };
    out.decline_all(&c.declined, SeriesDecline::OverQBC);
    uncovered.extend(&c.uncovered);
    let mut kept: Vec<(usize, BTreeSet<u64>)> = Vec::new();
    for plan in &c.admitted {
        let Some((_, classes)) = with_c.iter().find(|(p, _)| p == plan) else {
            continue;
        };
        let mut entries = uncovered.clone();
        for class in classes {
            // Pass B named the class, so the leaf must list its entries.
            let Some(ordinals) = c.answers.get(class) else {
                out.decline_all(&c.admitted, SeriesDecline::Rejected);
                return Ok(out);
            };
            entries.extend(ordinals.iter().copied());
        }
        kept.push((*plan, entries));
    }
    out.answered.extend(kept);
    Ok(out)
}

enum PassRun {
    /// Every plan offered to the pass declines on the part.
    PartDeclined(SeriesDecline),
    Ran(PassResult),
}

#[derive(Default)]
struct PassResult {
    admitted: Vec<usize>,
    declined: Vec<usize>,
    /// Probed key -> its posting (passes A and B) or the entry ordinals
    /// holding it (pass C). A key the leaf does not hold has no entry.
    answers: BTreeMap<Key8, Vec<u64>>,
    uncovered: BTreeSet<u64>,
    /// On-object bytes read past the prefixes.
    bytes_read: u64,
}

fn run_bytes(plan: &KeyIndexSetPlan) -> u64 {
    plan.runs().iter().map(|run| run.end - run.start).sum()
}

fn in_slice(leaf_ref: &SnapshotKeyIndexLeafRef, key8: &Key8) -> bool {
    let key = u64::from_be_bytes(*key8);
    leaf_ref.key8_lo <= key && key <= leaf_ref.key8_hi
}

/// A pass's decoder over one slice: the posting decoder for passes A and B,
/// the entry decoder for pass C.
enum PassDecoder<'p> {
    Posting(KeyIndexPostingSetDecoder<'p, LeafDecodeGuard>),
    Entry(KeyIndexSetDecoder<'p, LeafDecodeGuard>),
}

impl PassDecoder<'_> {
    fn decode_next_run<R>(
        &mut self,
        bytes: &[u8],
        limits: &KeyIndexLeafLimits,
        reserve: &mut R,
    ) -> Result<(), KeyIndexSetRunError>
    where
        R: FnMut(u64) -> Option<LeafDecodeGuard>,
    {
        match self {
            PassDecoder::Posting(d) => d.decode_next_run(bytes, limits, reserve),
            PassDecoder::Entry(d) => d.decode_next_run(bytes, limits, reserve),
        }
    }

    fn is_done(&self) -> bool {
        match self {
            PassDecoder::Posting(d) => d.is_done(),
            PassDecoder::Entry(d) => d.is_done(),
        }
    }

    fn decompressed_bytes(&self) -> u64 {
        match self {
            PassDecoder::Posting(d) => d.decompressed_bytes(),
            PassDecoder::Entry(d) => d.decompressed_bytes(),
        }
    }

    /// Merges the slice's answer for `keys` (the plan's keys, in order).
    fn finish(
        self,
        leaf_ref: &SnapshotKeyIndexLeafRef,
        keys: &[Key8],
        result: &mut PassResult,
    ) -> Result<(), SeriesDecline> {
        let declined = |decline: KeyIndexSetDecline| match decline {
            KeyIndexSetDecline::OverBudget { .. } => SeriesDecline::OverBudget,
            KeyIndexSetDecline::ReservationRefused { .. } => SeriesDecline::ReservationRefused,
        };
        match self {
            PassDecoder::Posting(d) => match d.finish() {
                KeyIndexPostingSetAnswer::Answered {
                    per_key,
                    uncovered_entry_ordinals,
                } => {
                    result.uncovered.extend(uncovered_entry_ordinals);
                    for (key, posting) in keys.iter().zip(per_key) {
                        if let Some(posting) = posting {
                            result.answers.insert(*key, posting);
                        }
                    }
                    Ok(())
                }
                KeyIndexPostingSetAnswer::Declined(decline) => Err(declined(decline)),
                KeyIndexPostingSetAnswer::Rejected(rejection) => {
                    leaf_rejected(leaf_ref, &rejection);
                    Err(SeriesDecline::Rejected)
                }
            },
            PassDecoder::Entry(d) => match d.finish() {
                KeyIndexSetAnswer::Answered {
                    per_key,
                    uncovered_entry_ordinals,
                } => {
                    result.uncovered.extend(uncovered_entry_ordinals);
                    for (key, answer) in keys.iter().zip(per_key) {
                        if let KeyIndexKeyAnswer::Matches(matches) = answer {
                            result.answers.insert(
                                *key,
                                matches.iter().map(|m| m.entry_ordinal).collect(),
                            );
                        }
                    }
                    Ok(())
                }
                KeyIndexSetAnswer::Declined(decline) => Err(declined(decline)),
                KeyIndexSetAnswer::Rejected(rejection) => {
                    leaf_rejected(leaf_ref, &rejection);
                    Err(SeriesDecline::Rejected)
                }
            },
        }
    }
}

/// One pass on one part: GET 1 of every slice the offered keys fall in,
/// admission of the offered plans (ascending own bytes, ties in plan order)
/// while the admitted union's runs stay within `allowance`, then the
/// admitted union's runs, decoded.
async fn run_pass(
    ctx: &SeriesCtx<'_, '_>,
    handle: &SnapshotPartHandle,
    pass: SeriesPass,
    offered: &[(usize, Vec<Key8>)],
    allowance: u64,
    segments: u64,
    figures: &mut SeriesPassFigures,
) -> Result<PassRun, RouteError> {
    let input = ctx.input;
    let field = pass.field();
    let mut offered: Vec<(usize, BTreeSet<Key8>)> = offered
        .iter()
        .map(|(plan, keys)| (*plan, keys.iter().copied().collect()))
        .collect();
    offered.sort_by_key(|(plan, _)| *plan);
    let union: Vec<Key8> = offered
        .iter()
        .flat_map(|(_, keys)| keys.iter().copied())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let slices = match slices_for(handle, field, &union) {
        Ok(Some(slices)) => slices,
        Ok(None) => return Ok(PassRun::PartDeclined(SeriesDecline::NoSeriesRefs)),
        Err(rejection) => {
            tracing::warn!(
                part = hex::encode(&handle.blake3),
                reason = %rejection,
                "series leaf refs rejected; the part is left to the label-value route"
            );
            return Ok(PassRun::PartDeclined(SeriesDecline::Rejected));
        }
    };
    let worst: u64 = slices
        .iter()
        .map(|(leaf_ref, keys)| slice_worst_gets(leaf_ref, keys.len() as u64))
        .sum();
    if worst > segments.saturating_mul(TIER_ONE_GETS_PER_ENTRY) {
        return Ok(PassRun::PartDeclined(SeriesDecline::OverTierOne));
    }
    let request = KeyIndexLeafRequest {
        tenant_hash: input.tenant_hash,
        signal: input.signal,
        part_blake3: &handle.blake3,
        part_entry_count: handle.entry_count,
        field,
        key_type: pass.key_type(),
    };
    let leaf_accounting = ctx.charges[pass.index()];
    figures.parts_probed += 1;
    figures.plans_offered += offered.len() as u64;
    figures.keys_offered += union.len() as u64;

    let starts: Vec<SliceStart> = {
        let (request, slices) = (&request, &slices);
        futures::stream::iter(0..slices.len())
            .map(|s| async move {
                let (leaf_ref, keys) = &slices[s];
                let range = match key_index_leaf_prefix_range(leaf_ref) {
                    Ok(range) => range,
                    Err(rejection) => return SliceStart::Rejected(rejection),
                };
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
                match plan_key_index_leaf_set(&bytes, leaf_ref, request, keys, B_RUN) {
                    Ok(set_plan) => SliceStart::Planned(Box::new(set_plan), bytes),
                    Err(KeyIndexLeafError::Breach(breach)) => SliceStart::Breach(breach),
                    Err(KeyIndexLeafError::Rejected(rejection)) => SliceStart::Rejected(rejection),
                }
            })
            .buffered(LEAF_READ_CONCURRENCY)
            .collect()
            .await
    };
    let mut prefixes: Vec<Bytes> = Vec::with_capacity(starts.len());
    let mut stop = None;
    for ((leaf_ref, _), start) in slices.iter().zip(starts) {
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
                figures.prefix_bytes += prefix.len() as u64;
                figures.distinct_buckets += set_plan.distinct_buckets() as u64;
                figures.bridged_bytes += set_plan.bridged_bytes();
                prefixes.push(prefix);
            }
            SliceStart::ReadRefused => stop = Some(SeriesDecline::ReservationRefused),
            SliceStart::Rejected(rejection) => {
                leaf_rejected(leaf_ref, &rejection);
                stop = Some(SeriesDecline::Rejected);
            }
        }
    }
    if let Some(reason) = stop {
        return Ok(PassRun::PartDeclined(reason));
    }

    // The plans of `keys` over every slice, from the prefixes already read.
    let plan_keys = |keys: &BTreeSet<Key8>| -> Result<Vec<(usize, KeyIndexSetPlan, Vec<Key8>)>, ()> {
        let mut plans = Vec::new();
        for (s, ((leaf_ref, _), prefix)) in slices.iter().zip(&prefixes).enumerate() {
            let in_this: Vec<Key8> = keys
                .iter()
                .filter(|key| in_slice(leaf_ref, key))
                .copied()
                .collect();
            if in_this.is_empty() {
                continue;
            }
            let plan = plan_key_index_leaf_set(prefix, leaf_ref, &request, &in_this, B_RUN)
                .map_err(|_| ())?;
            plans.push((s, plan, in_this));
        }
        Ok(plans)
    };
    let bytes_of = |keys: &BTreeSet<Key8>| -> Result<u64, ()> {
        Ok(plan_keys(keys)?
            .iter()
            .map(|(_, plan, _)| run_bytes(plan))
            .sum())
    };

    let mut order: Vec<(u64, usize)> = Vec::with_capacity(offered.len());
    for (i, (_, keys)) in offered.iter().enumerate() {
        let Ok(own) = bytes_of(keys) else {
            return Ok(PassRun::PartDeclined(SeriesDecline::Rejected));
        };
        order.push((own, i));
    }
    order.sort_unstable();
    let mut result = PassResult::default();
    let mut admitted_keys: BTreeSet<Key8> = BTreeSet::new();
    for (_, i) in order {
        let (plan, keys) = &offered[i];
        if result.declined.is_empty() {
            let mut candidate = admitted_keys.clone();
            candidate.extend(keys.iter().copied());
            let Ok(bytes) = bytes_of(&candidate) else {
                return Ok(PassRun::PartDeclined(SeriesDecline::Rejected));
            };
            if bytes <= allowance {
                admitted_keys = candidate;
                result.admitted.push(*plan);
                continue;
            }
        }
        result.declined.push(*plan);
    }
    if result.admitted.is_empty() {
        return Ok(PassRun::Ran(result));
    }
    let Ok(probes) = plan_keys(&admitted_keys) else {
        return Ok(PassRun::PartDeclined(SeriesDecline::Rejected));
    };
    // The probe's own backstop: each slice's runs within what the earlier
    // slices of the pass left of the allowance.
    let mut left = allowance;
    for (_, plan, _) in &probes {
        let bytes = run_bytes(plan);
        if bytes > left {
            return Ok(PassRun::PartDeclined(SeriesDecline::OverBudget));
        }
        left -= bytes;
    }
    result.bytes_read = allowance - left;
    figures.plans_admitted += result.admitted.len() as u64;
    figures.keys_probed += admitted_keys.len() as u64;
    figures.bytes_past_prefix += result.bytes_read;

    let mut decoders: Vec<Option<PassDecoder<'_>>> = probes
        .iter()
        .map(|(_, plan, _)| {
            Some(match pass {
                SeriesPass::A | SeriesPass::B => PassDecoder::Posting(plan.posting_decoder()),
                SeriesPass::C => PassDecoder::Entry(plan.decoder()),
            })
        })
        .collect();
    let mut decompressed = 0u64;
    let mut reserve = |bytes| input.budget.try_reserve(bytes).ok();
    let mut failure = None;
    {
        let runs: Vec<(usize, Range<u64>)> = probes
            .iter()
            .enumerate()
            .flat_map(|(i, (_, plan, _))| plan.runs().iter().map(move |run| (i, run.clone())))
            .collect();
        let (probes, slices) = (&probes, &slices);
        let mut reads = futures::stream::iter(runs)
            .map(|(i, range)| {
                let read = input
                    .reader
                    .read_range(slices[probes[i].0].0, range, leaf_accounting);
                async move { (i, read.await) }
            })
            .buffered(LEAF_READ_CONCURRENCY);
        while let Some((i, read)) = reads.next().await {
            let leaf_ref = slices[probes[i].0].0;
            let decoded = match read {
                Err(LeafReadError::ReservationRefused(_)) => Err(SeriesDecline::ReservationRefused),
                Err(LeafReadError::Failed(e)) => {
                    leaf_rejected(leaf_ref, &KeyIndexLeafRejection::Read(e));
                    Err(SeriesDecline::Rejected)
                }
                Ok(bytes) => {
                    let slot = &mut decoders[i];
                    let step = match slot.as_mut() {
                        Some(decoder) => {
                            decoder.decode_next_run(&bytes, &input.limits, &mut reserve)
                        }
                        None => Err(KeyIndexSetRunError::Rejected(
                            KeyIndexLeafRejection::RequestMismatch(
                                "a bucket run past the slice's plan".to_string(),
                            ),
                        )),
                    };
                    match step {
                        Ok(()) => match slot.take_if(|d| d.is_done()) {
                            Some(decoder) => {
                                decompressed += decoder.decompressed_bytes();
                                decoder.finish(leaf_ref, &probes[i].2, &mut result)
                            }
                            None => Ok(()),
                        },
                        Err(KeyIndexSetRunError::Rejected(rejection)) => {
                            leaf_rejected(leaf_ref, &rejection);
                            Err(SeriesDecline::Rejected)
                        }
                        Err(KeyIndexSetRunError::ReservationRefused { .. }) => {
                            Err(SeriesDecline::ReservationRefused)
                        }
                    }
                }
            };
            if let Err(reason) = decoded {
                failure = Some(reason);
                break;
            }
        }
    }
    if failure.is_none() {
        // Slices whose keys all fell in empty buckets have no runs.
        for (i, slot) in decoders.iter_mut().enumerate() {
            if let Some(decoder) = slot.take() {
                decompressed += decoder.decompressed_bytes();
                let leaf_ref = slices[probes[i].0].0;
                if let Err(reason) = decoder.finish(leaf_ref, &probes[i].2, &mut result) {
                    failure = Some(reason);
                    break;
                }
            }
        }
    }
    decompressed += decoders
        .iter()
        .flatten()
        .map(|d| d.decompressed_bytes())
        .sum::<u64>();
    leaf_accounting.add_decompressed_bytes(decompressed);
    drop(prefixes);
    match failure {
        Some(reason) => Ok(PassRun::PartDeclined(reason)),
        None => Ok(PassRun::Ran(result)),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests;
