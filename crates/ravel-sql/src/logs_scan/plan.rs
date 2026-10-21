use super::*;

/// The shared per-segment block plan (ADR-0102): for each segment, in snapshot
/// order, its [`PlannedUnit`], or `None` for a ts-irrelevant segment (no GET,
/// no blocks) and for an unplanned one. Computed once; [`Self::publish`] turns
/// it into the units partitions take from a [`UnitQueue`].
pub(super) struct PlanCounts {
    pub(super) segs: Vec<Option<PlannedUnit>>,
    /// Per segment, in snapshot order: relevant and accepted by
    /// [`may_skip_plan`] in a scan that [`skips_sub_bound_plans`], so not
    /// planned (ADR-2677 decision 2), and not dropped by a KEY_IDX probe
    /// ([`LogSegmentFetcher::probe_segment_keyed`]). All
    /// false otherwise. Its owner opens it whole, once, and prunes its
    /// blocks from that read ([`open_segment_whole`]); its `segs` entry is
    /// `None`.
    unplanned: Vec<bool>,
    /// Relevant segments the plan phase read whole instead of counting from the
    /// skip index (#761). A segment planned from footer alone or from the skip
    /// index carries its plan footer forward (`PlannedUnit::footer` is `Some`); the
    /// whole-object fallback carries none, so this is the count of relevant
    /// segments with no footer, published as the `plan_full_reads` metric (see
    /// [`BlockMetrics::plan_full_reads`] for the two causes).
    pub(super) full_reads: usize,
    /// The global time-ordered publication order ([`global_order`]) when the
    /// scan feeds a confirmed ascending `ts` TopK and stripes blocks; `None`
    /// otherwise. [`Self::publish`] follows it instead of the visit order.
    global: Option<Vec<GlobalUnit>>,
    /// What the tier-1 KEY_IDX probe pruned over every planned segment
    /// (ADR-2707 decision 5), published once by partition 0.
    pub(super) key_index: KeyIndexPruning,
}

/// What the plan phase learned about a segment with at least one surviving
/// block (ADR-2414 decision A1).
struct PlannedBlocks {
    /// Whole-object block indices surviving this query's pruning, ascending:
    /// the list every per-partition open's own pruning must reproduce.
    indices: Arc<Vec<usize>>,
    /// The blocks [`PlanCounts::publish`] publishes, grouped into whole row
    /// groups ([`row_groups`]): every survivor, or the ones the KEY_IDX probe
    /// left (ADR-2707 decision 5). Computed here so publication does not need
    /// the directories, which the segment's last owner may already have
    /// released.
    groups: Vec<Vec<usize>>,
    /// Per entry of `groups`, its [`group_key`]: the order [`global_order`]
    /// sorts the group in.
    group_min_ts: Vec<i64>,
    /// This segment's directories and carried whole object, one handle per
    /// published [`OwnedSeg`] of it.
    carried: CarrySlot,
}

impl PlannedBlocks {
    #[cfg(test)]
    fn new(
        indices: Vec<usize>,
        dirs: Arc<SegmentDirectories>,
        reservation: ravel_memory::Reservation,
    ) -> Self {
        Self::restricted(indices, None, dirs, reservation, None)
    }

    /// A segment's plan with no owners counted yet: until
    /// [`CarrySlot::set_owners`] runs, the directories and the whole object
    /// stay held for as long as the plan counts live. Only `dealt` (a
    /// subsequence of `indices`) is published to partitions when it is
    /// `Some`.
    fn restricted(
        indices: Vec<usize>,
        dealt: Option<&[usize]>,
        dirs: Arc<SegmentDirectories>,
        reservation: ravel_memory::Reservation,
        whole_object: Option<CarriedWholeObject>,
    ) -> Self {
        let groups = row_groups(dealt.unwrap_or(&indices), dirs.page_dir());
        let l0 = &dirs.skip_index().l0;
        let group_min_ts = groups
            .iter()
            .map(|group| group_key(group, |block| l0.get(block).map(|e| (e.min_ts, e.max_ts))))
            .collect();
        PlannedBlocks {
            indices: Arc::new(indices),
            groups,
            group_min_ts,
            carried: CarrySlot::new(
                CarriedDirs {
                    dirs,
                    _reservation: reservation,
                },
                whole_object,
            ),
        }
    }
}

/// A segment's directories, decoded once by the plan phase
/// ([`LogSegmentFetcher::plan_segment`]) and carried to every subset open
/// through [`OwnedSeg`] so the open reuses them via
/// `RlogReader::from_decoded` instead of decoding them again, together with
/// the fetch memory budget's reservation for them
/// ([`LogSegmentFetcher::reserve_carried_directories`]). Both are released
/// when the last handle drops.
pub(super) struct CarriedDirs {
    pub(super) dirs: Arc<SegmentDirectories>,
    _reservation: ravel_memory::Reservation,
}

/// What one owner of a segment takes from its [`CarrySlot`].
struct Carry {
    dirs: Option<Arc<CarriedDirs>>,
    whole_object: Option<CarriedWholeObject>,
}

/// Hands one handle on a segment's [`CarriedDirs`] and carried whole object to
/// each [`OwnedSeg`] published for it, and keeps its own until the last of
/// them has taken one. Each partition drops its handles when it finishes that
/// [`OwnedSeg`], so both reservations are released once every owner has
/// finished the segment rather than when the statement ends.
struct CarrySlot {
    state: std::sync::Mutex<CarrySlotState>,
}

struct CarrySlotState {
    dirs: Option<Arc<CarriedDirs>>,
    whole_object: Option<CarriedWholeObject>,
    /// Owners that have not taken their handles yet. `usize::MAX` until
    /// publication counts them.
    takers_left: usize,
}

impl CarrySlot {
    fn new(dirs: CarriedDirs, whole_object: Option<CarriedWholeObject>) -> Self {
        CarrySlot {
            state: std::sync::Mutex::new(CarrySlotState {
                dirs: Some(Arc::new(dirs)),
                whole_object,
                takers_left: usize::MAX,
            }),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, CarrySlotState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// How many [`OwnedSeg`]s are published for this segment: each calls
    /// [`Self::take`] once.
    fn set_owners(&self, owners: usize) {
        let mut state = self.lock();
        state.takers_left = owners;
        if owners == 0 {
            state.dirs = None;
            state.whole_object = None;
        }
    }

    /// One owner's handles. The slot lets its own go once every owner has
    /// taken one.
    fn take(&self) -> Carry {
        let mut state = self.lock();
        let carry = Carry {
            dirs: state.dirs.clone(),
            whole_object: state.whole_object.clone(),
        };
        state.takers_left = state.takers_left.saturating_sub(1);
        if state.takers_left == 0 {
            state.dirs = None;
            state.whole_object = None;
        }
        carry
    }
}

/// One relevant segment's contribution to [`PlanCounts`].
pub(super) struct PlannedUnit {
    /// The surviving blocks, grouped by row group, with the carried
    /// directories and whole object, or `None` when no block of this segment
    /// survives: no partition opens such a segment, so it keeps nothing
    /// resident and reserves nothing.
    planned: Option<PlannedBlocks>,
    /// The whole-segment prune stats ([`BlockMetrics::record_segment_totals`]
    /// consumes `blocks_total` and the postings drop). `blocks_scanned`/`pages`
    /// are zero here -- planning decodes nothing.
    pub(super) stats: ScanStats,
    /// The footer the plan fast path read for this segment (#693 part 3,
    /// deliverable 2), or `None` when the plan slow branch opened the scan
    /// instead. Carried to each subset open through [`OwnedSeg`] so the open
    /// reuses it and skips its own suffix probe.
    footer: Option<LogFooter>,
}

pub(super) type CountsFuture = Pin<Box<dyn Future<Output = DFResult<Arc<PlanCounts>>> + Send>>;

/// A future resolving the shared [`PlanCounts`] through `cell`: the first
/// partition to poll computes it (pruning every segment once), the rest await
/// that one computation. Errors are not cached, so a transient fetch failure
/// can be retried by a later poll. `by_min_ts` is whether the scan feeds a
/// confirmed ascending `ts` TopK ([`LogsScanExec::active_topk`]); with
/// `stripe_blocks` it selects [`global_order`].
pub(super) fn plan_counts_future(
    cell: Arc<OnceCell<Arc<PlanCounts>>>,
    ctx: Arc<PartitionCtx>,
    segments: Arc<Vec<SegmentRef>>,
    plan_concurrency: usize,
    stripe_blocks: bool,
    by_min_ts: bool,
    plan_init_elapsed: Time,
) -> CountsFuture {
    Box::pin(async move {
        let counts = cell
            .get_or_try_init(|| async {
                // Only the initializing partition runs this closure, so the
                // metric it records is the barrier's cost counted once.
                let started = Instant::now();
                let counts = compute_plan_counts(
                    &ctx,
                    &segments,
                    plan_concurrency,
                    stripe_blocks,
                    by_min_ts,
                )
                .await;
                plan_init_elapsed.add_elapsed(started);
                counts
            })
            .await?;
        Ok(Arc::clone(counts))
    })
}

/// One segment's [`LogSegmentFetcher::plan_segment`] result: the surviving
/// whole-object block indices and this segment's decoded directories
/// (ADR-2414 decision A1), stats, a carried footer (skip-decidable
/// branches), and a carried whole-object body (the fallback branch, issue
/// #835). `None` when the segment was pruned by ts bounds alone and never
/// reached the fetcher.
type PlanSegmentResult = Option<(
    Vec<usize>,
    Arc<SegmentDirectories>,
    ScanStats,
    Option<LogFooter>,
    Option<CarriedWholeObject>,
)>;

/// Whether this scan opens its segments at or under
/// [`LogSegmentFetcher::plan_whole_object_bound`] unplanned (ADR-2677
/// decision 2): true when the scan holds at least `target_partitions` of them
/// that are relevant to the query window ([`may_skip_plan`]). A keyed query
/// counts the ones above the block-range threshold too: the plan pass probes
/// their KEY_IDX instead of planning them, and leaves unplanned every one the
/// probe does not drop. Counted over the whole segment list
/// once, inside the shared plan pass, so every partition routes every segment
/// the same way.
///
/// An unplanned segment has one owner, so with fewer such segments than
/// partitions the unplanned route would leave partitions idle that the planned
/// route stripes blocks over when a read cache is wired (ADR-0102). Those
/// scans keep planning every segment.
fn skips_sub_bound_plans(
    ctx: &PartitionCtx,
    segments: &[SegmentRef],
    target_partitions: usize,
) -> bool {
    let sub_bound = segments
        .iter()
        .filter(|seg| {
            may_skip_plan(ctx, seg)
                && LogSegmentFetcher::ts_range_relevant(
                    seg,
                    ctx.query.ts_min_ns,
                    ctx.query.ts_max_ns,
                )
        })
        .count();
    sub_bound >= target_partitions.max(1)
}

/// Whether `seg` is one [`skips_sub_bound_plans`] may open unplanned: at or
/// under [`LogSegmentFetcher::plan_whole_object_bound`]. For a query with
/// KEY_IDX equalities one above [`LogSegmentFetcher::block_range_threshold`]
/// is probed first ([`LogSegmentFetcher::probe_segment_keyed`]) and opened
/// only when no key is absent.
fn may_skip_plan(ctx: &PartitionCtx, seg: &SegmentRef) -> bool {
    !ctx.fetcher.plans_segment(seg)
}

/// One plan-pass read: the plan of a planned segment, or the KEY_IDX probe of
/// a keyed segment the scan opens unplanned.
enum PlanPassRead {
    Planned(Box<ravel_query::KeyedSegmentPlan>),
    Probed(KeyIndexPruning),
}

/// Prune every segment once (no block decode) to build the shared block plan.
///
/// The prunes run `plan_concurrency` at a time (`buffer_unordered`, consumed
/// as each completes rather than after the whole pass), and results are
/// written into a position-indexed `Vec` so `segs` keeps snapshot order and
/// [`PlanCounts::publish`] can still index it by segment position even though
/// completion order does not match it. Every partition awaits this whole pass
/// before it drains anything, so the plan phase sits alone on the query's
/// critical path: run serially it costs one object-store round trip per
/// segment in sequence (issue #691 measured about 20 minutes per statement on
/// 8424 objects, one GET in flight for the whole time). Concurrency here
/// changes only how many of those reads are in flight, never their count
/// (still one plan read sequence per segment) or their semantics (the first
/// error aborts the plan, and `get_or_try_init` does not cache it). Which
/// error that is became scheduling-dependent when the pass moved to
/// completion order: a query whose segments fail differently can surface
/// either one. The
/// fetcher's own in-flight GET semaphore remains the global bound, so an
/// oversized `plan_concurrency` is safe.
///
/// `plan_concurrency` here is `self.target_partitions`, the query's SQL
/// partition count -- not `--store-get-concurrency`'s object-store GET
/// limiter (ADR-1195), which bounds in-flight wire requests process-wide
/// and, since ADR-1195 T2, is a separate knob from this one.
///
/// `plan_concurrency` also bounds how many carried whole objects (issue #835)
/// this pass retains at once (issue #835 follow-up): the first
/// `plan_concurrency` segments to COMPLETE with a carried whole object (not
/// the first `plan_concurrency` by segment position -- completion order under
/// `buffer_unordered` is scheduling-dependent) keep their bytes; every later
/// one has its `whole_object` forced to `None` before it is stored, so its
/// subset open pays a real re-fetch in the scan phase instead of holding
/// bytes the rest of the pass has not yet caught up to consuming. See this
/// module's doc, "Whole-object fallback carry, and its memory bound".
// Issue #693 part 3: the predicate-free full-window fast path in `execute`
// skips this whole pass; see `owned_whole_segments` and `plan_segment_fast`.
async fn compute_plan_counts(
    ctx: &PartitionCtx,
    segments: &[SegmentRef],
    plan_concurrency: usize,
    stripe_blocks: bool,
    by_min_ts: bool,
) -> DFResult<Arc<PlanCounts>> {
    // Not-yet-polled futures, one per segment tagged with its snapshot
    // position, so `buffer_unordered` decides how many run at once and the
    // consumer loop below can still store each result at its original index.
    // Built with a loop rather than a `map` closure: a closure returning a
    // future that borrows its argument cannot satisfy the higher-ranked bound
    // this `Send` boxed future needs.
    let mut prunes = Vec::with_capacity(segments.len());
    let mut unplanned = vec![false; segments.len()];
    let skip_sub_bound = skips_sub_bound_plans(ctx, segments, plan_concurrency);
    for (idx, seg) in segments.iter().enumerate() {
        // Refuse an unreadable version BEFORE the plan probe, not after. This
        // is a second entry point into the fetch layer alongside the three
        // route choices below, and `plan_segment` issues its footer probe
        // before the open phase can reject the version -- so without this the
        // ordering guarantee holds on the scan path and quietly fails here.
        refuse_unreadable_version(seg)?;
        // ADR-2677 decision 2: at or under the bound the plan would read the
        // object whole only to count its survivors, so the scan reads it once
        // instead, when the scan has enough such segments to fill its
        // partitions (see `skips_sub_bound_plans`).
        // ADR-2707 decision 5: a keyed one above the block-range threshold is
        // probed here instead, and left unplanned unless a key is absent.
        let probe_only = skip_sub_bound && may_skip_plan(ctx, seg);
        if probe_only {
            if !LogSegmentFetcher::ts_range_relevant(seg, ctx.query.ts_min_ns, ctx.query.ts_max_ns)
            {
                continue;
            }
            if ctx.query.key_equals.is_empty()
                || seg.object_size <= ctx.fetcher.block_range_threshold()
            {
                unplanned[idx] = true;
                continue;
            }
        }
        prunes.push(async move {
            let (plan, key_index) = (
                ctx.phase_accounting.plan(),
                ctx.phase_accounting.key_index(),
            );
            let read = if probe_only {
                ctx.fetcher
                    .probe_segment_keyed(seg, ctx.tenant_hash, &ctx.query, plan, key_index)
                    .await
                    .map(PlanPassRead::Probed)
            } else {
                ctx.fetcher
                    .plan_segment_keyed(seg, ctx.tenant_hash, &ctx.query, plan, key_index)
                    .await
                    .map(|plan| PlanPassRead::Planned(Box::new(plan)))
            };
            (idx, read)
        });
    }
    let budget = plan_concurrency.max(1);
    let mut stream = futures::stream::iter(prunes).buffer_unordered(budget);
    let mut segs: Vec<Option<PlannedUnit>> = std::iter::repeat_with(|| None)
        .take(segments.len())
        .collect();
    let mut full_reads = 0usize;
    // Only the RETAINED carried bytes -- the first `budget` segments to
    // complete with a whole object -- are summed here, so this is the true
    // peak `compute_plan_counts` holds at once, not the whole corpus. A
    // dropped whole object never reaches this sum: its `whole_object` is
    // `None` before `PlannedUnit` is built below.
    let mut carried_bytes = 0u64;
    let mut carried_seen = 0usize;
    let mut key_index = KeyIndexPruning::default();
    while let Some((idx, entry)) = stream.next().await {
        let keyed = match entry.map_err(SqlError::from)? {
            PlanPassRead::Planned(keyed) => *keyed,
            PlanPassRead::Probed(probe) => {
                key_index.segments_pruned += probe.segments_pruned;
                unplanned[idx] = probe.segments_pruned == 0;
                continue;
            }
        };
        key_index.segments_pruned += keyed.key_index.segments_pruned;
        key_index.blocks_pruned += keyed.key_index.blocks_pruned;
        let entry: PlanSegmentResult = keyed.plan;
        if let Some((indices, dirs, stats, footer, whole_object)) = entry {
            // The blocks some partition reads: the KEY_IDX probe's subset of
            // the survivors when it named one.
            let survivors = keyed.key_blocks.as_ref().map_or(indices.len(), Vec::len);
            // A relevant segment planned from the skip index carries its
            // footer forward; the whole-object fallback (#761) carries none.
            if footer.is_none() {
                full_reads += 1;
            }
            let whole_object = match whole_object {
                // `survivors > 0` mirrors the condition `PlanCounts::publish`
                // uses to decide whether any partition ever opens this
                // segment: a zero-survivor segment is dropped there, so
                // spending the budget on its carry here would retain bytes
                // no partition ever reuses and starve a segment that would
                // have.
                Some(w) if survivors > 0 && carried_seen < budget => {
                    carried_seen += 1;
                    carried_bytes += w.byte_len();
                    Some(w)
                }
                // Either no whole object to begin with, no surviving block to
                // spend it on, or one arrived after the budget was already
                // spent by earlier completions: drop it before it is ever
                // stored, so it is never retained and never charged as
                // reused.
                _ => None,
            };
            // Only a segment some partition will open keeps its directories,
            // and only then are they reserved: `PlanCounts::publish` skips a
            // zero-survivor segment, so carrying them would hold decoded
            // bytes nothing reuses.
            let planned = if survivors > 0 {
                let reservation = ctx
                    .fetcher
                    .reserve_carried_directories(&dirs)
                    .map_err(SqlError::from)?;
                Some(PlannedBlocks::restricted(
                    indices,
                    keyed.key_blocks.as_deref(),
                    dirs,
                    reservation,
                    whole_object,
                ))
            } else {
                None
            };
            segs[idx] = Some(PlannedUnit {
                planned,
                stats,
                footer,
            });
        }
    }
    // A single call with the final sum is only correct because `carried_bytes`
    // is monotonically non-decreasing across this loop: nothing here ever
    // releases a retained buffer mid-pass, so the last value is also the
    // peak. If a future change lets retention shrink before this function
    // returns (an eviction, an early release), this call must move inside
    // the loop so `observe_intermediate_bytes` sees every local maximum, not
    // just the end state.
    ctx.phase_accounting
        .plan()
        .observe_intermediate_bytes(carried_bytes);
    Ok(Arc::new(PlanCounts::assemble(
        segs,
        unplanned,
        full_reads,
        key_index,
        segments,
        stripe_blocks,
        by_min_ts,
    )))
}

/// Which of a segment's blocks one published unit carries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum UnitBlocks {
    /// An unplanned segment ([`PlanCounts::unplanned`]), opened whole.
    Unplanned,
    /// A run of whole row groups, opened once: positions into the segment's
    /// [`PlannedBlocks::groups`], ascending.
    Groups(Vec<usize>),
    /// Every block the plan publishes for the segment.
    All,
}

/// How many row groups one run of a segment with `groups` surviving row
/// groups holds when the scan has `partitions` partitions: `ceil(groups /
/// partitions)`, so the segment splits into at most `min(groups, partitions)`
/// runs and is opened at most that many times.
fn run_len(groups: usize, partitions: usize) -> usize {
    groups.div_ceil(partitions.max(1)).max(1)
}

impl PlanCounts {
    /// The plan of `segments`, publishing in [`global_order`] when the scan
    /// stripes blocks (`stripe_blocks`, a read cache is wired) and feeds a
    /// confirmed ascending `ts` TopK (`by_min_ts`), and in visit order
    /// otherwise.
    fn assemble(
        segs: Vec<Option<PlannedUnit>>,
        unplanned: Vec<bool>,
        full_reads: usize,
        key_index: KeyIndexPruning,
        segments: &[SegmentRef],
        stripe_blocks: bool,
        by_min_ts: bool,
    ) -> Self {
        let global =
            (stripe_blocks && by_min_ts).then(|| global_order(&segs, &unplanned, segments));
        PlanCounts {
            segs,
            unplanned,
            full_reads,
            global,
            key_index,
        }
    }

    fn planned(&self, seg_idx: usize) -> Option<&PlannedBlocks> {
        self.segs
            .get(seg_idx)
            .and_then(Option::as_ref)
            .and_then(|unit| unit.planned.as_ref())
    }

    fn is_unplanned(&self, seg_idx: usize) -> bool {
        self.unplanned.get(seg_idx).copied().unwrap_or(false)
    }

    /// The units this plan publishes for `partitions` partitions, in
    /// publication order, as `(snapshot segment index, blocks)` (ADR-0102
    /// decision 1):
    ///
    /// - Under [`global_order`] (a read cache with a confirmed ascending `ts`
    ///   TopK, at any partition count), the dealt runs of [`global_runs`].
    /// - With `stripe_blocks` otherwise (a read cache and more than one
    ///   partition), segments in `order` (see [`visit_order`]), each planned
    ///   one of `G` surviving row groups as runs of [`run_len`]`(G,
    ///   partitions)` consecutive row groups in group order, so it is opened
    ///   at most `min(G, partitions)` times, and each unplanned one as one
    ///   unit.
    /// - Without `stripe_blocks`, one unit per segment in `order` that has a
    ///   surviving block or is unplanned.
    ///
    /// A segment with no surviving block publishes nothing.
    pub(super) fn units(
        &self,
        order: &[usize],
        stripe_blocks: bool,
        partitions: usize,
    ) -> Vec<(usize, UnitBlocks)> {
        if let Some(global) = &self.global {
            return global_runs(global, partitions);
        }
        let mut units = Vec::new();
        for &seg_idx in order {
            if self.is_unplanned(seg_idx) {
                units.push((seg_idx, UnitBlocks::Unplanned));
                continue;
            }
            let Some(planned) = self.planned(seg_idx) else {
                continue;
            };
            if stripe_blocks {
                let positions: Vec<usize> = (0..planned.groups.len()).collect();
                let len = run_len(positions.len(), partitions);
                units.extend(
                    positions
                        .chunks(len)
                        .map(|run| (seg_idx, UnitBlocks::Groups(run.to_vec()))),
                );
            } else {
                units.push((seg_idx, UnitBlocks::All));
            }
        }
        units
    }

    /// [`Self::units`] as the [`OwnedSeg`]s a [`UnitQueue`] hands out. Each
    /// segment's owners are counted first, so its [`CarrySlot`] lets its own
    /// handles go as the last unit takes one and the carry is then released
    /// when the last owner finishes its unit. A second publication from the
    /// same plan finds the slots empty and carries nothing.
    pub(super) fn publish(
        &self,
        segments: &[SegmentRef],
        order: &[usize],
        stripe_blocks: bool,
        partitions: usize,
    ) -> VecDeque<OwnedSeg> {
        let units = self.units(order, stripe_blocks, partitions);
        let mut owners = vec![0usize; self.segs.len()];
        for &(seg_idx, _) in &units {
            if let Some(count) = owners.get_mut(seg_idx) {
                *count += 1;
            }
        }
        for (seg_idx, &count) in owners.iter().enumerate() {
            if let Some(planned) = self.planned(seg_idx) {
                planned.carried.set_owners(count);
            }
        }
        units
            .into_iter()
            .filter_map(|(seg_idx, blocks)| {
                let seg = segments.get(seg_idx)?.clone();
                if blocks == UnitBlocks::Unplanned {
                    return Some(OwnedSeg {
                        seg,
                        ordinal: seg_idx,
                        indices: Vec::new(),
                        survivors: None,
                        footer: None,
                        whole_object: None,
                        dirs: None,
                        unplanned: true,
                    });
                }
                let unit = self.segs.get(seg_idx)?.as_ref()?;
                let planned = unit.planned.as_ref()?;
                let indices = match blocks {
                    UnitBlocks::Groups(run) => run
                        .iter()
                        .filter_map(|&pos| planned.groups.get(pos))
                        .flatten()
                        .copied()
                        .collect(),
                    UnitBlocks::Unplanned | UnitBlocks::All => planned.groups.concat(),
                };
                let Carry { dirs, whole_object } = planned.carried.take();
                Some(OwnedSeg {
                    seg,
                    ordinal: seg_idx,
                    indices,
                    survivors: Some(Arc::clone(&planned.indices)),
                    footer: unit.footer.clone(),
                    whole_object,
                    dirs,
                    unplanned: false,
                })
            })
            .collect()
    }
}

/// The published units of one execution of a scan's partitions. Each
/// partition takes the next unit when it finishes its current one, so a
/// unit, and with it a row group, has exactly one owner, and each partition
/// opens its units in publication order. The queue's length is fixed when it
/// is published and only shrinks; a partition that finds it empty is done.
pub(super) struct UnitQueue {
    units: std::sync::Mutex<VecDeque<OwnedSeg>>,
}

impl UnitQueue {
    pub(super) fn new(units: VecDeque<OwnedSeg>) -> Self {
        UnitQueue {
            units: std::sync::Mutex::new(units),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, VecDeque<OwnedSeg>> {
        self.units
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The next unit in publication order, or `None` once every unit is
    /// taken.
    pub(super) fn take_next(&self) -> Option<OwnedSeg> {
        self.lock().pop_front()
    }

    /// Drops every unit nobody took, releasing their carries.
    fn close(&self) {
        let left = std::mem::take(&mut *self.lock());
        drop(left);
    }
}

/// Which [`UnitQueue`] each execution of a partition takes from. Every
/// partition's first execution joins round 0; a partition executed again
/// joins the next round, whose queue is published afresh from the same plan,
/// so a re-executed scan reads every unit again instead of finding round 0's
/// queue empty. Once every declared partition has left a round, the units
/// left in its queue are dropped.
pub(super) struct UnitRounds {
    members: usize,
    state: std::sync::Mutex<RoundsState>,
}

#[derive(Default)]
struct RoundsState {
    /// Per partition, how many times it has joined.
    executions: Vec<usize>,
    rounds: Vec<Round>,
}

#[derive(Default)]
struct Round {
    queue: Option<Arc<UnitQueue>>,
    left: usize,
}

impl UnitRounds {
    pub(super) fn new(partitions: usize) -> Self {
        UnitRounds {
            members: partitions.max(1),
            state: std::sync::Mutex::new(RoundsState::default()),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, RoundsState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// `partition`'s place in its next round.
    pub(super) fn join(self: &Arc<Self>, partition: usize) -> RoundTicket {
        let mut state = self.lock();
        if state.executions.len() <= partition {
            state.executions.resize(partition + 1, 0);
        }
        let round = state.executions.get(partition).copied().unwrap_or(0);
        if let Some(count) = state.executions.get_mut(partition) {
            *count += 1;
        }
        if state.rounds.len() <= round {
            state.rounds.resize_with(round + 1, Round::default);
        }
        RoundTicket {
            rounds: Arc::clone(self),
            round,
        }
    }
}

/// One partition execution's membership of a [`UnitRounds`] round; dropping
/// it leaves the round.
pub(super) struct RoundTicket {
    rounds: Arc<UnitRounds>,
    round: usize,
}

impl RoundTicket {
    /// The scan's declared partition count, which sizes the runs the round's
    /// queue publishes ([`PlanCounts::units`]).
    pub(super) fn partitions(&self) -> usize {
        self.rounds.members
    }

    /// The round's queue, published by `publish` on the round's first call.
    pub(super) fn queue(&self, publish: impl FnOnce() -> VecDeque<OwnedSeg>) -> Arc<UnitQueue> {
        let mut state = self.rounds.lock();
        match state.rounds.get_mut(self.round) {
            Some(round) => Arc::clone(
                round
                    .queue
                    .get_or_insert_with(|| Arc::new(UnitQueue::new(publish()))),
            ),
            None => Arc::new(UnitQueue::new(VecDeque::new())),
        }
    }
}

impl Drop for RoundTicket {
    fn drop(&mut self) {
        let closed = {
            let mut state = self.rounds.lock();
            let members = self.rounds.members;
            state.rounds.get_mut(self.round).and_then(|round| {
                round.left += 1;
                if round.left >= members {
                    round.queue.take()
                } else {
                    None
                }
            })
        };
        if let Some(queue) = closed {
            queue.close();
        }
    }
}

/// The order key of a row group whose blocks are `group` (whole-object block
/// indices): the smallest stored minimum `ts` of its blocks, read through
/// `bounds` (a block's skip-index `(min_ts, max_ts)`). A block with no bounds
/// or with inverted ones (`min > max`) proves nothing about where its rows
/// sit, so it keys the group `i64::MIN` and the group sorts ahead of every
/// group it might precede.
fn group_key(group: &[usize], bounds: impl Fn(usize) -> Option<(i64, i64)>) -> i64 {
    group
        .iter()
        .map(|&block| match bounds(block) {
            Some((min, max)) if min <= max => min,
            _ => i64::MIN,
        })
        .min()
        .unwrap_or(i64::MIN)
}

/// One unit of [`global_order`]: a row group of a planned segment (`group` is
/// its position into the segment's [`PlannedBlocks::groups`]), or an unplanned
/// segment (`group` is `None`). Fields are in sort order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct GlobalUnit {
    key: i64,
    pub(super) seg: usize,
    pub(super) group: Option<usize>,
}

/// The publication order under a confirmed ascending `ts` TopK on the striped
/// path (ADR-2677 decision 6, issue #2694): every surviving row group of every
/// planned segment, and every unplanned segment as one unit, sorted ascending
/// by `(key, snapshot segment index, group position)`. A row group's key is
/// its [`group_key`]; an unplanned segment's is its `min_event_ts_ns`.
/// Partitions take the units with the smallest minimum `ts` first, across all
/// segments, so the TopK heap fills from the earliest blocks and the threshold
/// skip passes over the rest, which ordering whole segments by their minimum
/// cannot do when every segment spans most of the time range.
pub(super) fn global_order(
    segs: &[Option<PlannedUnit>],
    unplanned: &[bool],
    segments: &[SegmentRef],
) -> Vec<GlobalUnit> {
    let mut units = Vec::new();
    for (seg_idx, plan) in segs.iter().enumerate() {
        if unplanned.get(seg_idx).copied().unwrap_or(false) {
            if let Some(seg) = segments.get(seg_idx) {
                units.push(GlobalUnit {
                    key: seg.min_event_ts_ns,
                    seg: seg_idx,
                    group: None,
                });
            }
            continue;
        }
        let Some(planned) = plan.as_ref().and_then(|p| p.planned.as_ref()) else {
            continue;
        };
        for (pos, &key) in planned.group_min_ts.iter().enumerate() {
            units.push(GlobalUnit {
                key,
                seg: seg_idx,
                group: Some(pos),
            });
        }
    }
    units.sort_unstable();
    units
}

/// [`global_order`] dealt as runs, as the per-partition deal of ADR-2677
/// decision 6 dealt it: unit `i` goes to queue `i % partitions`, and extends
/// the last run of that queue when the run holds row groups of the same
/// segment ending at a smaller group position, since an open drains its
/// blocks in block order; any other unit starts a new run, so a run is read in
/// its global order. Runs are published in the global order of their first
/// unit, which keeps every queue's runs in that queue's order.
pub(super) fn global_runs(global: &[GlobalUnit], partitions: usize) -> Vec<(usize, UnitBlocks)> {
    let n = partitions.max(1);
    // Per queue, the index into `units` of its last run.
    let mut last: Vec<Option<usize>> = vec![None; n];
    let mut units: Vec<(usize, UnitBlocks)> = Vec::new();
    for (i, unit) in global.iter().enumerate() {
        let Some(tail) = last.get_mut(i % n) else {
            continue;
        };
        let extended = match (unit.group, tail.and_then(|at| units.get_mut(at))) {
            (Some(pos), Some((seg, UnitBlocks::Groups(positions))))
                if *seg == unit.seg && positions.last().is_some_and(|&p| p < pos) =>
            {
                positions.push(pos);
                true
            }
            _ => false,
        };
        if !extended {
            *tail = Some(units.len());
            units.push((
                unit.seg,
                match unit.group {
                    Some(pos) => UnitBlocks::Groups(vec![pos]),
                    None => UnitBlocks::Unplanned,
                },
            ));
        }
    }
    units
}

/// One unit of work: a segment, and the block indices of it the partition
/// that takes this unit drains.
pub(super) struct OwnedSeg {
    pub(super) seg: SegmentRef,
    /// This segment's position in the snapshot's segment list, i.e. the
    /// segment field of every row-ref built from it (ADR-0774).
    pub(super) ordinal: usize,
    pub(super) indices: Vec<usize>,
    /// The segment's whole surviving-block list this query's plan produced
    /// (ascending whole-object block indices), of which `indices` is this
    /// unit's share. A row-ref addresses a block by its position in this
    /// list ([`RowRefRange`]), not by its whole-object index, so the scan
    /// resolves each block's row-ref through it. `None` on the whole-segment
    /// fast path, where every block survives and a block's whole-object index
    /// already is its surviving-block position.
    pub(super) survivors: Option<Arc<Vec<usize>>>,
    /// The plan-phase footer for this segment (#693 part 3, deliverable 2),
    /// carried to the subset open so it skips its own suffix probe. `None` on the
    /// whole-segment fast path (no plan phase) and when the plan slow branch ran.
    pub(super) footer: Option<LogFooter>,
    /// The plan fallback's whole-object bytes for this segment (issue #835),
    /// carried to the subset open so it does not pay a second wire GET: this
    /// unit's handle from the segment's [`CarrySlot`]. `None` when the plan
    /// pass did not retain one, and always `None` on the whole-segment fast
    /// path (no plan phase).
    pub(super) whole_object: Option<CarriedWholeObject>,
    /// This unit's handle on the segment's directories, decoded once by
    /// the plan phase (ADR-2414 decision A1), carried to the subset open so it
    /// reuses them instead of decoding them again. `None` on the whole-segment
    /// fast path (no plan phase ever runs there).
    pub(super) dirs: Option<Arc<CarriedDirs>>,
    /// Never planned ([`PlanCounts::unplanned`]): opened whole through
    /// [`open_segment_whole`], with `indices`, `survivors`, `footer`,
    /// `whole_object` and `dirs` all empty. The open's own pruning supplies
    /// the survivor list a row-ref resolves against.
    pub(super) unplanned: bool,
}

/// Groups `indices` (a segment's surviving whole-object block indices,
/// ascending) into whole row groups by `page_dir`'s own group boundaries, so
/// [`PlanCounts::publish`] can publish a row group's blocks as one unit
/// (ADR-2414 decision A1) instead of splitting them across partitions. Indices
/// are ascending and row groups are contiguous ranges of block numbers, so
/// consecutive indices mapping to the same group's `first_block` are always
/// one unbroken run. A block [`PageDir::locate_block`] cannot place keeps its
/// own singleton group rather than joining one it does not belong to.
fn row_groups(indices: &[usize], page_dir: &PageDir) -> Vec<Vec<usize>> {
    let mut groups: Vec<(Option<u32>, Vec<usize>)> = Vec::new();
    for &idx in indices {
        let block = u32::try_from(idx).unwrap_or(u32::MAX);
        let first_block = page_dir.locate_block(block).map(|(g, _)| g.first_block);
        match groups.last_mut() {
            Some((cur, items)) if cur.is_some() && *cur == first_block => items.push(idx),
            _ => groups.push((first_block, vec![idx])),
        }
    }
    groups.into_iter().map(|(_, items)| items).collect()
}

/// The predicate-free full-window whole-segment assignment (#693 part 3,
/// deliverable 1), computed with NO plan phase at all. Relevant (ts-overlapping) segments in
/// snapshot order are numbered `0..`, and segment `j` goes to partition `j % n`,
/// which then drains the whole segment through [`open_segment_whole`]. `n` is the
/// same `min(target_partitions, relevant)` the caller derives, so every
/// partition gets whole segments and none is split.
///
/// Each relevant segment is proved fully contained in the query window and
/// predicate-free before this runs (see [`LogsScanExec::whole_segment_fast_path`]),
/// so every block survives and the block-index list is unused here.
///
/// Segments are numbered in `order` (see [`visit_order`]).
pub(super) fn owned_whole_segments(
    segments: &[SegmentRef],
    order: &[usize],
    ts_min: i64,
    ts_max: i64,
    partition: usize,
    n: usize,
) -> VecDeque<OwnedSeg> {
    let mut work = VecDeque::new();
    let mut ordinal = 0usize;
    for &seg_idx in order {
        let Some(seg) = segments.get(seg_idx) else {
            continue;
        };
        if !LogSegmentFetcher::ts_range_relevant(seg, ts_min, ts_max) {
            continue;
        }
        if ordinal % n == partition {
            work.push_back(OwnedSeg {
                seg: seg.clone(),
                ordinal: seg_idx,
                indices: Vec::new(),
                survivors: None,
                footer: None,
                whole_object: None,
                dirs: None,
                unplanned: false,
            });
        }
        ordinal += 1;
    }
    work
}

/// The order a scan deals its segments to partitions in, as snapshot indices.
///
/// Snapshot order without a confirmed TopK threshold. With one (ADR-2677
/// decision 6, [`LogsScanExec::active_topk`]),
/// ascending `min_event_ts_ns`, ties broken by snapshot index: each partition
/// then opens its smallest-minimum segment first, so the TopK heap fills from
/// the rows most likely to stay in it and the threshold tightens before the
/// later segments are reached. A segment's minimum covers every row it holds,
/// a late-arriving row included, so a segment written last that holds the
/// smallest `ts` is visited first.
///
/// With a confirmed threshold this order deals the whole-segment fast path
/// ([`owned_whole_segments`]) and orders the units the planned path without a
/// read cache publishes ([`PlanCounts::units`]); the planned path with one
/// publishes row groups in [`global_order`] instead and does not read it.
pub(super) fn visit_order(segments: &[SegmentRef], by_min_ts: bool) -> Vec<usize> {
    let mut order: Vec<usize> = (0..segments.len()).collect();
    if by_min_ts {
        order.sort_by_key(|&i| (segments[i].min_event_ts_ns, i));
    }
    order
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod unit_queue_tests {
    //! ADR-0102 decision 1 and ADR-2414 decision A1: [`PlanCounts::publish`]
    //! publishes runs of whole row groups (whole segments without a read
    //! cache) and partitions take them one at a time from a [`UnitQueue`], so
    //! a row group's blocks never split across partitions. Fixtures are
    //! writer-produced objects of one block per record, `GROUP_BLOCKS` blocks
    //! per row group.

    use super::*;
    use proptest::prelude::*;
    use ravel_catalog::SegmentLevel;
    use ravel_logseg::record::stream_attrs_bytes;
    use ravel_logseg::{ObjectIdentity, RlogConfig, RlogReader, RlogWriter};
    use ravel_types::logstream::log_stream_id;
    use uuid::Uuid;

    const GROUP_BLOCKS: usize = 4;
    const GROUPS: usize = 3;
    const BLOCKS: usize = GROUPS * GROUP_BLOCKS;

    fn object(group_blocks: usize) -> Vec<u8> {
        let cfg = RlogConfig {
            block_target_records: 1,
            group_target_blocks: group_blocks,
            ..RlogConfig::default()
        };
        let identity = ObjectIdentity {
            tenant_hash: [7u8; 16],
            shard: 0,
            writer_id: [2u8; 16],
            writer_epoch: 1,
            writer_seq: 1,
        };
        let resource = vec![("service.name".to_string(), AttrValue::Str("svc".into()))];
        let mut writer = RlogWriter::new(cfg, identity);
        for ts in 0..BLOCKS as i64 {
            writer
                .push(ravel_logseg::LogRecord {
                    stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
                    stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
                    ts_ns: ts,
                    observed_ts_ns: ts,
                    severity_num: 9,
                    severity_text: "INFO".into(),
                    body: format!("row {ts}"),
                    trace_id: None,
                    span_id: None,
                    flags: 0,
                    attrs: Vec::new(),
                })
                .expect("push");
        }
        writer.finish().expect("finish")
    }

    fn dirs() -> Arc<SegmentDirectories> {
        dirs_grouped(GROUP_BLOCKS)
    }

    /// The directories of the fixture object written with `group_blocks`
    /// blocks per row group.
    fn dirs_grouped(group_blocks: usize) -> Arc<SegmentDirectories> {
        let obj = object(group_blocks);
        Arc::new(
            RlogReader::decode_directories(&obj[..], &RlogConfig::default()).expect("directories"),
        )
    }

    fn seg_ref(key: &str) -> SegmentRef {
        SegmentRef {
            data_object_key: key.to_string(),
            object_size: 0,
            min_event_ts_ns: 0,
            max_event_ts_ns: BLOCKS as i64,
            ingest_hour_bucket: 0,
            sample_count: BLOCKS as u64,
            series_count: 0,
            shard: 0,
            content_hash: [0u8; 32],
            writer_id: Uuid::from_u128(1),
            writer_epoch: 1,
            writer_seq: 1,
            created_unix_ns: 0,
            level: SegmentLevel::L0,
            segment_format_version: u32::from(ravel_logseg::footer::VERSION),
            declared_column_stats: Default::default(),
        }
    }

    fn seg_ref_at(key: &str, min_event_ts_ns: i64) -> SegmentRef {
        SegmentRef {
            min_event_ts_ns,
            ..seg_ref(key)
        }
    }

    /// A planned segment surviving `indices` of the fixture object, with its
    /// group keys replaced by `keys` when given, reserving `bytes`.
    fn planned_seg(
        indices: Vec<usize>,
        keys: Option<Vec<i64>>,
        budget: &Arc<ravel_memory::MemoryBudget>,
        bytes: u64,
    ) -> Option<PlannedUnit> {
        planned_seg_in(dirs(), indices, keys, budget, bytes)
    }

    /// [`planned_seg`] over the object whose directories are `dirs`.
    fn planned_seg_in(
        dirs: Arc<SegmentDirectories>,
        indices: Vec<usize>,
        keys: Option<Vec<i64>>,
        budget: &Arc<ravel_memory::MemoryBudget>,
        bytes: u64,
    ) -> Option<PlannedUnit> {
        let mut planned =
            PlannedBlocks::new(indices, dirs, budget.reserve(bytes).expect("reserve"));
        if let Some(keys) = keys {
            assert_eq!(keys.len(), planned.groups.len());
            planned.group_min_ts = keys;
        }
        Some(PlannedUnit {
            planned: Some(planned),
            stats: ScanStats::default(),
            footer: None,
        })
    }

    fn assemble(
        segs: Vec<Option<PlannedUnit>>,
        unplanned: Vec<bool>,
        refs: &[SegmentRef],
        stripe: bool,
        by_min_ts: bool,
    ) -> PlanCounts {
        PlanCounts::assemble(
            segs,
            unplanned,
            0,
            KeyIndexPruning::default(),
            refs,
            stripe,
            by_min_ts,
        )
    }

    /// `segments` identical objects, each with every block surviving, in a
    /// scan with no TopK.
    fn plan(segments: usize, stripe: bool) -> (PlanCounts, Vec<SegmentRef>) {
        let budget = Arc::new(ravel_memory::MemoryBudget::unlimited());
        let segs = (0..segments)
            .map(|_| planned_seg((0..BLOCKS).collect(), None, &budget, 0))
            .collect();
        let refs: Vec<SegmentRef> = (0..segments).map(|s| seg_ref(&format!("seg{s}"))).collect();
        let counts = assemble(segs, vec![false; segments], &refs, stripe, false);
        (counts, refs)
    }

    /// A unit as `(segment ordinal, indices, unplanned)`.
    type Unit = (usize, Vec<usize>, bool);

    fn unit(owned: &OwnedSeg) -> Unit {
        (owned.ordinal, owned.indices.clone(), owned.unplanned)
    }

    /// The units `counts` publishes for `n` partitions, in publication order.
    fn published(
        counts: &PlanCounts,
        refs: &[SegmentRef],
        stripe: bool,
        by_min_ts: bool,
        n: usize,
    ) -> Vec<Unit> {
        counts
            .publish(refs, &visit_order(refs, by_min_ts), stripe, n)
            .iter()
            .map(unit)
            .collect()
    }

    /// Partitions `0..n` taking from `queue` until each has found it empty,
    /// the partition that takes next chosen by `schedule` (cycled, or round
    /// robin when empty; a partition already done passes to the next one).
    /// Returns each partition's units in the order it took them, and every
    /// take in the order it happened with its taker.
    fn take_all(
        queue: &UnitQueue,
        n: usize,
        schedule: &[usize],
    ) -> (Vec<Vec<OwnedSeg>>, Vec<(usize, Unit)>) {
        let mut owned: Vec<Vec<OwnedSeg>> = (0..n).map(|_| Vec::new()).collect();
        let mut log = Vec::new();
        let mut done = vec![false; n];
        let mut step = 0usize;
        while let Some(p) = {
            let pick = schedule
                .get(step % schedule.len().max(1))
                .copied()
                .unwrap_or(step);
            (0..n).map(|k| (pick + k) % n).find(|&p| !done[p])
        } {
            step += 1;
            match queue.take_next() {
                Some(taken) => {
                    log.push((p, unit(&taken)));
                    owned[p].push(taken);
                }
                None => done[p] = true,
            }
        }
        (owned, log)
    }

    fn blocks(owned: &[OwnedSeg]) -> Vec<Vec<usize>> {
        owned.iter().map(|o| o.indices.clone()).collect()
    }

    /// One object with more blocks (12) than partitions (3), striped: with as
    /// many partitions as row groups each run is one row group, so three
    /// partitions taking in turn own one whole group each, and one partition
    /// taking every unit owns the three groups as three whole units while the
    /// other two run empty.
    ///
    /// Fails against publishing one unit per block (twelve single-block units,
    /// so partition 0 owns blocks 0, 3, 6 and 9 of three different groups) and
    /// against publishing the segment as one unit (partition 0 owns all twelve
    /// blocks in one unit and partitions 1 and 2 nothing).
    #[test]
    fn a_row_group_stays_with_one_owner() {
        let groups = vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7], vec![8, 9, 10, 11]];
        let (counts, refs) = plan(1, true);
        let queue = UnitQueue::new(counts.publish(&refs, &[0], true, 3));
        let (owned, _) = take_all(&queue, 3, &[0, 1, 2]);
        assert_eq!(
            owned.iter().map(|o| blocks(o)).collect::<Vec<_>>(),
            vec![
                vec![groups[0].clone()],
                vec![groups[1].clone()],
                vec![groups[2].clone()],
            ],
            "each partition owns exactly one whole row group"
        );

        let (counts, refs) = plan(1, true);
        let queue = UnitQueue::new(counts.publish(&refs, &[0], true, 3));
        let (owned, _) = take_all(&queue, 3, &[0]);
        assert_eq!(blocks(&owned[0]), groups);
        assert!(owned[1].is_empty() && owned[2].is_empty());
    }

    /// Without a TopK the striped plan publishes segments in snapshot order,
    /// each as its runs of consecutive row groups in sequence, and two
    /// partitions taking in turn alternate through that one sequence across
    /// the segment boundary. With two partitions a segment's three row groups
    /// make runs of `ceil(3 / 2) = 2` and 1, so each segment is opened twice,
    /// not three times.
    ///
    /// Fails against publishing in [`global_order`] without a TopK (segment
    /// 1's first run, key 0, would precede segment 0's second, key 8),
    /// against one unit per row group (six units), and against publishing
    /// each segment as one unit.
    #[test]
    fn striped_units_follow_visit_order_with_groups_in_sequence() {
        let (counts, refs) = plan(2, true);
        let order: Vec<Unit> = (0..2)
            .flat_map(|s| {
                [
                    (s, (0..2 * GROUP_BLOCKS).collect(), false),
                    (s, group(2), false),
                ]
            })
            .collect();
        assert_eq!(published(&counts, &refs, true, false, 2), order);

        let (counts, refs) = plan(2, true);
        let queue = UnitQueue::new(counts.publish(&refs, &[0, 1], true, 2));
        let (owned, _) = take_all(&queue, 2, &[]);
        let units = |p: usize| owned[p].iter().map(unit).collect::<Vec<_>>();
        assert_eq!(units(0), vec![order[0].clone(), order[2].clone()]);
        assert_eq!(units(1), vec![order[1].clone(), order[3].clone()]);
    }

    /// A pruned survivor list is grouped by the object's own row-group
    /// boundaries, not by runs of consecutive survivors or by a fixed survivor
    /// count: blocks 3 and 4 are consecutive survivors but sit in different
    /// groups, and 4 and 5 share one.
    ///
    /// Fails against grouping consecutive survivor runs (`[[3,4,5],[9]]`) and
    /// against chunking survivors by `GROUP_BLOCKS` (`[[3,4,5,9]]`).
    #[test]
    fn survivors_group_by_page_dir_boundaries() {
        let dirs = dirs();
        assert_eq!(
            row_groups(&[3, 4, 5, 9], dirs.page_dir()),
            vec![vec![3], vec![4, 5], vec![9]]
        );
        assert_eq!(
            row_groups(&[], dirs.page_dir()),
            Vec::<Vec<usize>>::new(),
            "no survivors, no groups"
        );
    }

    /// Blocks PAGE_DIR cannot place each keep a singleton group, adjacent ones
    /// included: they share no group, so nothing makes them one unit.
    ///
    /// Fails against matching on the `Option` key alone, which merges 100 and
    /// 101 into one group keyed `None`.
    #[test]
    fn unplaceable_blocks_keep_singleton_groups() {
        let dirs = dirs();
        assert!(dirs.page_dir().locate_block(100).is_none());
        assert_eq!(
            row_groups(&[9, 100, 101], dirs.page_dir()),
            vec![vec![9], vec![100], vec![101]]
        );
    }

    /// Without a read cache the units stay segment-granular: each segment is
    /// one unit of every surviving block, and two partitions taking in turn
    /// own segments 0 and 2, and 1.
    #[test]
    fn the_uncached_units_are_whole_segments() {
        let (counts, refs) = plan(3, false);
        let queue = UnitQueue::new(counts.publish(&refs, &[0, 1, 2], false, 2));
        let (owned, _) = take_all(&queue, 2, &[]);
        let all: Vec<usize> = (0..BLOCKS).collect();
        let units: Vec<Vec<Unit>> = owned.iter().map(|o| o.iter().map(unit).collect()).collect();
        assert_eq!(
            units,
            vec![
                vec![(0, all.clone(), false), (2, all.clone(), false)],
                vec![(1, all, false)],
            ]
        );
    }

    // ---- the global order (issue #2694) ----------------------------------

    /// The fixture object's `ts` is its block index, so a group's key is its
    /// first surviving block. A block PAGE_DIR cannot place is also past the
    /// skip index, so it has no bounds and keys its singleton group
    /// `i64::MIN`. Through [`group_key`] directly: a group's key is the least
    /// minimum of its blocks, not its first block's; inverted bounds key a
    /// group `i64::MIN` whatever its other blocks' minima are; an empty group
    /// (never built) keys the same.
    ///
    /// Fails against keying a group by its first block (`[1, 3]` would key
    /// 5), against taking an inverted block's `min` at face value (`[1, 7]`
    /// would key 5), and against leaving bound-less blocks out of the minimum
    /// (`[9, 100]` would key 9).
    #[test]
    fn group_keys_are_the_least_block_minimum_and_boundless_blocks_lead() {
        let budget = Arc::new(ravel_memory::MemoryBudget::unlimited());
        let planned = PlannedBlocks::new(
            vec![1, 2, 3, 4, 9, 100],
            dirs(),
            budget.reserve(0).expect("reserve"),
        );
        assert_eq!(
            planned.groups,
            vec![vec![1, 2, 3], vec![4], vec![9], vec![100]]
        );
        assert_eq!(planned.group_min_ts, vec![1, 4, 9, i64::MIN]);

        let bounds = |block: usize| match block {
            1 => Some((5, 6)),
            3 => Some((2, 4)),
            7 => Some((8, 2)),
            9 => Some((9, 9)),
            _ => None,
        };
        assert_eq!(group_key(&[1, 3], bounds), 2, "the least minimum");
        assert_eq!(group_key(&[1, 7], bounds), i64::MIN, "inverted bounds");
        assert_eq!(group_key(&[9, 100], bounds), i64::MIN, "absent bounds");
        assert_eq!(group_key(&[], bounds), i64::MIN, "no blocks");
    }

    /// An unplanned segment is one unit keyed by its `min_event_ts_ns`, so it
    /// is published between the groups whose keys bracket it; a group keyed
    /// `i64::MIN` is published ahead of every other unit. Over one partition
    /// a segment's units that are not consecutive in the sorted sequence are
    /// separate runs.
    ///
    /// Fails against keying an unplanned segment by snapshot position (it
    /// would sort first or last), against keying it at `i64::MIN`, and
    /// against merging segment 0's units into one run.
    #[test]
    fn unplanned_segments_sort_by_their_minimum_and_boundless_groups_first() {
        let budget = Arc::new(ravel_memory::MemoryBudget::unlimited());
        let segs = vec![
            planned_seg((0..BLOCKS).collect(), None, &budget, 0),
            None,
            planned_seg(vec![0, 1, 2, 3], Some(vec![i64::MIN]), &budget, 0),
        ];
        let refs = vec![seg_ref_at("a", 0), seg_ref_at("u", 5), seg_ref_at("b", 0)];
        let counts = assemble(segs, vec![false, true, false], &refs, true, true);
        assert_eq!(
            published(&counts, &refs, true, true, 1),
            vec![
                (2, vec![0, 1, 2, 3], false),
                (0, (0..8).collect(), false),
                (1, vec![], true),
                (0, vec![8, 9, 10, 11], false),
            ],
            "keys: segment 2 at i64::MIN, segment 0 at 0, 4, 8, the unplanned one at 5"
        );
    }

    /// A queue's consecutive units of one segment share a run only when their
    /// group positions ascend: the open path drains a segment's blocks in
    /// ascending order and binary-searches [`OwnedSeg::indices`] on that
    /// assumption (`ravel-query/src/log_fetcher.rs`). One segment with group
    /// keys `[5, 1]` sorts group 1 (key 1) ahead of group 0 (key 5), so both
    /// for two partitions and for one, where the two units land in the same
    /// queue, they are published as two units, group 1 first.
    ///
    /// Fails against merging a queue's consecutive units of one segment on
    /// the segment alone, which for one partition publishes one unit of both
    /// groups, read with group 0 first.
    #[test]
    fn descending_group_positions_publish_separate_units() {
        let budget = Arc::new(ravel_memory::MemoryBudget::unlimited());
        let counts = |budget: &Arc<ravel_memory::MemoryBudget>| {
            let segs = vec![planned_seg((0..8).collect(), Some(vec![5, 1]), budget, 0)];
            assemble(segs, vec![false], &[seg_ref_at("a", 0)], true, true)
        };
        let refs = vec![seg_ref_at("a", 0)];
        assert_eq!(
            published(&counts(&budget), &refs, true, true, 2),
            vec![(0, vec![4, 5, 6, 7], false), (0, vec![0, 1, 2, 3], false)],
        );
        assert_eq!(
            published(&counts(&budget), &refs, true, true, 1),
            vec![(0, vec![4, 5, 6, 7], false), (0, vec![0, 1, 2, 3], false)],
        );
    }

    /// Segment 0 keyed 0, 2, 4; segment 1 keyed 1, 10, 11; segment 2 keyed
    /// 20, 22; segment 3 keyed 21; segment 4 unplanned with minimum 2;
    /// segment 5 one group with no bounds.
    fn global_fixture(
        budget: &Arc<ravel_memory::MemoryBudget>,
        bytes: [u64; 4],
    ) -> (Vec<Option<PlannedUnit>>, Vec<bool>, Vec<SegmentRef>) {
        let segs = vec![
            planned_seg((0..BLOCKS).collect(), Some(vec![0, 2, 4]), budget, bytes[0]),
            planned_seg(
                (0..BLOCKS).collect(),
                Some(vec![1, 10, 11]),
                budget,
                bytes[1],
            ),
            planned_seg((0..8).collect(), Some(vec![20, 22]), budget, bytes[2]),
            planned_seg((0..4).collect(), Some(vec![21]), budget, bytes[3]),
            None,
            planned_seg((0..4).collect(), Some(vec![i64::MIN]), budget, 0),
        ];
        let mins = [0, 1, 20, 21, 2, -1];
        let refs = mins
            .iter()
            .enumerate()
            .map(|(s, &min)| seg_ref_at(&format!("seg{s}"), min))
            .collect();
        (segs, vec![false, false, false, false, true, false], refs)
    }

    fn group(g: usize) -> Vec<usize> {
        (g * GROUP_BLOCKS..(g + 1) * GROUP_BLOCKS).collect()
    }

    /// With a read cache (`stripe`) and a confirmed ascending `ts` TopK
    /// (`by_min_ts`) the queue's publication order is the group-key order the
    /// global deal sorted by, across segments: ascending `(key, snapshot
    /// segment index, group position)`, an unplanned segment keyed by its
    /// minimum and sorting ahead of a group of its own segment, with the
    /// groups the deal gave one partition consecutively from one segment
    /// merged into one run. Partitions take units in that order, so the
    /// earliest row groups are taken first whichever partition asks. Without
    /// either half of the precondition the plan publishes in visit order
    /// instead.
    ///
    /// Fails against publishing in visit order under the TopK (segment 0's
    /// three groups would come first), against selecting the global order on
    /// the TopK alone (the unstriped expectation below would sort row groups),
    /// against sorting by segment before key, and against leaving the dealt
    /// runs unmerged (two partitions would publish segment 0's groups apart).
    #[test]
    fn topk_units_are_published_in_global_deal_order() {
        let budget = Arc::new(ravel_memory::MemoryBudget::unlimited());
        let (segs, unplanned, refs) = global_fixture(&budget, [0; 4]);
        let counts = assemble(segs, unplanned, &refs, true, true);
        let global = vec![
            (5, group(0), false),
            (0, group(0), false),
            (1, group(0), false),
            (0, group(1), false),
            (4, vec![], true),
            (0, group(2), false),
            (1, group(1), false),
            (1, group(2), false),
            (2, group(0), false),
            (3, group(0), false),
            (2, group(1), false),
        ];
        assert_eq!(
            published(&counts, &refs, true, true, 3),
            global,
            "three partitions: no queue holds two consecutive units of one segment"
        );

        // Two partitions: queue 1 is dealt segment 0's three groups in a row
        // and queue 0 segment 2's two, so each is one run, published where its
        // first group sits in the global order.
        let runs = vec![
            (5, group(0), false),
            (0, (0..3 * GROUP_BLOCKS).collect(), false),
            (1, group(0), false),
            (4, vec![], true),
            (1, group(1), false),
            (1, group(2), false),
            (2, (0..2 * GROUP_BLOCKS).collect(), false),
            (3, group(0), false),
        ];
        let (segs, unplanned, refs) = global_fixture(&budget, [0; 4]);
        let counts = assemble(segs, unplanned, &refs, true, true);
        assert_eq!(published(&counts, &refs, true, true, 2), runs);

        let (segs, unplanned, refs) = global_fixture(&budget, [0; 4]);
        let counts = assemble(segs, unplanned, &refs, true, true);
        let queue = UnitQueue::new(counts.publish(&refs, &visit_order(&refs, true), true, 2));
        let (owned, _) = take_all(&queue, 2, &[]);
        for (p, units) in owned.iter().enumerate() {
            let expected: Vec<Unit> = runs.iter().skip(p).step_by(2).cloned().collect();
            assert_eq!(units.iter().map(unit).collect::<Vec<_>>(), expected);
        }

        let (segs, unplanned, refs) = global_fixture(&budget, [0; 4]);
        let counts = assemble(segs, unplanned, &refs, false, true);
        let all = |n: usize| (0..n * GROUP_BLOCKS).collect::<Vec<usize>>();
        assert_eq!(
            published(&counts, &refs, false, true, 2),
            vec![
                (5, all(1), false),
                (0, all(3), false),
                (1, all(3), false),
                (4, vec![], true),
                (2, all(2), false),
                (3, all(1), false),
            ],
            "no read cache: whole segments in ascending minimum order"
        );

        let (segs, unplanned, refs) = global_fixture(&budget, [0; 4]);
        let counts = assemble(segs, unplanned, &refs, true, false);
        let snapshot: Vec<Unit> = [(0, 3), (1, 3), (2, 2), (3, 1)]
            .into_iter()
            .flat_map(|(s, n)| (0..n).map(move |g| (s, group(g), false)))
            .chain([(4, vec![], true), (5, group(0), false)])
            .collect();
        assert_eq!(
            published(&counts, &refs, true, false, 3),
            snapshot,
            "no TopK: snapshot order, each segment's groups in sequence"
        );
        let (segs, unplanned, refs) = global_fixture(&budget, [0; 4]);
        let counts = assemble(segs, unplanned, &refs, true, false);
        let pair: Vec<usize> = (0..2 * GROUP_BLOCKS).collect();
        assert_eq!(
            published(&counts, &refs, true, false, 2),
            vec![
                (0, pair.clone(), false),
                (0, group(2), false),
                (1, pair, false),
                (1, group(2), false),
                (2, group(0), false),
                (2, group(1), false),
                (3, group(0), false),
                (4, vec![], true),
                (5, group(0), false),
            ],
            "no TopK, two partitions: runs of ceil(G/2) groups in group order"
        );
    }

    /// One partition's run of consecutive units of one segment in
    /// [`main_global_deal`]; `groups` is empty for an unplanned segment.
    #[derive(Clone, Debug, PartialEq, Eq)]
    struct DealtRun {
        seg: usize,
        groups: Vec<usize>,
    }

    /// The per-partition TopK deal as it stood before the shared queue
    /// (ADR-2677 decision 6, issue #2694), kept verbatim as the oracle for
    /// [`global_runs`]: every unit sorted by `(key, segment, position)`, unit
    /// `i` dealt to partition `i % n`, and a partition's consecutive units of
    /// one segment merged into one run while their positions ascend.
    fn main_global_deal(
        segs: &[Option<PlannedUnit>],
        unplanned: &[bool],
        segments: &[SegmentRef],
        n: usize,
    ) -> Vec<Vec<DealtRun>> {
        let mut units: Vec<(i64, usize, Option<usize>)> = Vec::new();
        for (seg_idx, plan) in segs.iter().enumerate() {
            if unplanned.get(seg_idx).copied().unwrap_or(false) {
                if let Some(seg) = segments.get(seg_idx) {
                    units.push((seg.min_event_ts_ns, seg_idx, None));
                }
                continue;
            }
            let Some(planned) = plan.as_ref().and_then(|p| p.planned.as_ref()) else {
                continue;
            };
            for (pos, &key) in planned.group_min_ts.iter().enumerate() {
                units.push((key, seg_idx, Some(pos)));
            }
        }
        units.sort_unstable();
        let n = n.max(1);
        let mut deal: Vec<Vec<DealtRun>> = vec![Vec::new(); n];
        for (i, (_, seg, pos)) in units.into_iter().enumerate() {
            let queue = &mut deal[i % n];
            match (queue.last_mut(), pos) {
                (Some(run), Some(pos))
                    if run.seg == seg && run.groups.last().is_some_and(|&last| last < pos) =>
                {
                    run.groups.push(pos);
                }
                _ => queue.push(DealtRun {
                    seg,
                    groups: pos.into_iter().collect(),
                }),
            }
        }
        deal
    }

    /// `deal`'s runs as one sequence ordered by the global position of each
    /// run's first unit, which for the `k`-th unit of partition `q`'s queue is
    /// `q + k * n`.
    fn main_publication(deal: &[Vec<DealtRun>]) -> Vec<(usize, UnitBlocks)> {
        let n = deal.len();
        let mut runs: Vec<(usize, &DealtRun)> = Vec::new();
        for (q, queue) in deal.iter().enumerate() {
            let mut dealt = 0usize;
            for run in queue {
                runs.push((q + dealt * n, run));
                dealt += run.groups.len().max(1);
            }
        }
        runs.sort_by_key(|&(first, _)| first);
        runs.into_iter()
            .map(|(_, run)| {
                let blocks = if run.groups.is_empty() {
                    UnitBlocks::Unplanned
                } else {
                    UnitBlocks::Groups(run.groups.clone())
                };
                (run.seg, blocks)
            })
            .collect()
    }

    /// Three time-disjoint segments: segment 0 keyed 0, 1, 2; segment 1 keyed
    /// 10, 11, 12; segment 2 keyed 20, 21.
    fn disjoint_fixture(
        budget: &Arc<ravel_memory::MemoryBudget>,
    ) -> (Vec<Option<PlannedUnit>>, Vec<bool>, Vec<SegmentRef>) {
        let segs = vec![
            planned_seg((0..BLOCKS).collect(), Some(vec![0, 1, 2]), budget, 0),
            planned_seg((0..BLOCKS).collect(), Some(vec![10, 11, 12]), budget, 0),
            planned_seg((0..8).collect(), Some(vec![20, 21]), budget, 0),
        ];
        let refs = [0, 10, 20]
            .iter()
            .enumerate()
            .map(|(s, &min)| seg_ref_at(&format!("seg{s}"), min))
            .collect();
        (segs, vec![false; 3], refs)
    }

    /// Under a read cache and a confirmed ascending `ts` TopK the published
    /// units are the per-partition deal's runs exactly, at every partition
    /// count, one partition included: on time-disjoint and on overlapping
    /// segments, for one, two and four partitions, against the ported
    /// [`main_global_deal`] and against sequences derived by hand. Every
    /// segment is opened once per dealt run of it.
    ///
    /// Fails against cutting a segment's groups into `ceil(G / n)` runs (the
    /// time-disjoint segments over two partitions would publish runs of two
    /// consecutive groups, and one partition would publish each overlapping
    /// segment as one unit where its first group sits), and against merging
    /// on the global sequence instead of per queue (over two partitions
    /// segment 1's groups 1 and 2 would merge and segment 0's would not).
    #[test]
    fn topk_units_match_main_global_deal_with_dealt_run_merge() {
        let budget = Arc::new(ravel_memory::MemoryBudget::unlimited());
        let g = |positions: &[usize]| UnitBlocks::Groups(positions.to_vec());
        let disjoint = [
            (
                1,
                vec![(0, g(&[0, 1, 2])), (1, g(&[0, 1, 2])), (2, g(&[0, 1]))],
            ),
            (
                2,
                vec![
                    (0, g(&[0, 2])),
                    (0, g(&[1])),
                    (1, g(&[0, 2])),
                    (1, g(&[1])),
                    (2, g(&[0])),
                    (2, g(&[1])),
                ],
            ),
            (
                4,
                vec![
                    (0, g(&[0])),
                    (0, g(&[1])),
                    (0, g(&[2])),
                    (1, g(&[0])),
                    (1, g(&[1])),
                    (1, g(&[2])),
                    (2, g(&[0])),
                    (2, g(&[1])),
                ],
            ),
        ];
        let unplanned = (4, UnitBlocks::Unplanned);
        let overlapping = [
            (
                1,
                vec![
                    (5, g(&[0])),
                    (0, g(&[0])),
                    (1, g(&[0])),
                    (0, g(&[1])),
                    unplanned.clone(),
                    (0, g(&[2])),
                    (1, g(&[1, 2])),
                    (2, g(&[0])),
                    (3, g(&[0])),
                    (2, g(&[1])),
                ],
            ),
            (
                2,
                vec![
                    (5, g(&[0])),
                    (0, g(&[0, 1, 2])),
                    (1, g(&[0])),
                    unplanned.clone(),
                    (1, g(&[1])),
                    (1, g(&[2])),
                    (2, g(&[0, 1])),
                    (3, g(&[0])),
                ],
            ),
            (
                4,
                vec![
                    (5, g(&[0])),
                    (0, g(&[0, 2])),
                    (1, g(&[0, 1])),
                    (0, g(&[1])),
                    unplanned.clone(),
                    (1, g(&[2])),
                    (2, g(&[0])),
                    (3, g(&[0])),
                    (2, g(&[1])),
                ],
            ),
        ];
        for (name, cases) in [("disjoint", &disjoint), ("overlapping", &overlapping)] {
            for (n, expected) in cases {
                let (segs, unplanned, refs) = if name == "disjoint" {
                    disjoint_fixture(&budget)
                } else {
                    global_fixture(&budget, [0; 4])
                };
                let deal = main_global_deal(&segs, &unplanned, &refs, *n);
                let counts = assemble(segs, unplanned, &refs, true, true);
                let units = counts.units(&[], true, *n);
                assert_eq!(&units, expected, "{name}, {n} partitions, by hand");
                assert_eq!(
                    units,
                    main_publication(&deal),
                    "{name}, {n} partitions, the ported deal"
                );
                let published = counts.publish(&refs, &visit_order(&refs, true), true, *n);
                for (seg, _) in refs.iter().enumerate() {
                    assert_eq!(
                        published.iter().filter(|o| o.ordinal == seg).count(),
                        deal.iter().flatten().filter(|r| r.seg == seg).count(),
                        "{name}, {n} partitions: segment {seg} opens once per dealt run"
                    );
                }
            }
        }
    }

    /// Every published unit of a planned segment carries a handle on its
    /// directories and its whole survivor list, the slot itself keeps none
    /// once publication has handed them out, and a segment's reservation is
    /// released exactly when the last of its units drops, not before: units
    /// are dropped one at a time in take order and the budget is checked
    /// after each against the segments with a unit still alive.
    ///
    /// Fails against a slot that keeps its own handle (nothing is released
    /// while the plan counts live), against a slot that hands its handle out
    /// to the first unit only (`take` instead of `clone`, so the segment is
    /// released while later owners still read it, and they carry nothing),
    /// and against an owner count of one per segment (the same).
    #[test]
    fn every_unit_carries_the_directories_until_its_segment_s_last_owner_finishes() {
        let budget = Arc::new(ravel_memory::MemoryBudget::unlimited());
        let bytes = [100, 1_000, 10_000, 100_000];
        let (segs, unplanned, refs) = global_fixture(&budget, bytes);
        let counts = assemble(segs, unplanned, &refs, true, true);
        assert_eq!(budget.reserved(), 111_100);

        let queue = UnitQueue::new(counts.publish(&refs, &visit_order(&refs, true), true, 2));
        for planned in counts
            .segs
            .iter()
            .flatten()
            .filter_map(|s| s.planned.as_ref())
        {
            let slot = planned.carried.lock();
            assert!(slot.dirs.is_none() && slot.takers_left == 0);
        }
        let (owned, log) = take_all(&queue, 2, &[]);
        let mut alive: Vec<OwnedSeg> = Vec::new();
        let mut owned: Vec<std::collections::VecDeque<OwnedSeg>> =
            owned.into_iter().map(Into::into).collect();
        for (p, _) in &log {
            alive.extend(owned[*p].pop_front());
        }
        for o in &alive {
            assert_eq!(o.dirs.is_some(), !o.unplanned, "segment {}", o.ordinal);
            assert_eq!(
                o.survivors.as_ref().map(|s| s.len()),
                counts.planned(o.ordinal).map(|p| p.indices.len()),
                "every unit carries the segment's whole survivor list"
            );
        }
        assert_eq!(budget.reserved(), 111_100, "every owner holds its handle");
        while !alive.is_empty() {
            drop(alive.remove(0));
            let held: u64 = (0..4)
                .filter(|&s| alive.iter().any(|o| o.ordinal == s))
                .map(|s| bytes[s])
                .sum();
            assert_eq!(budget.reserved(), held, "{} units left", alive.len());
        }
        assert_eq!(budget.reserved(), 0, "with the plan counts alive");
        drop(counts);
    }

    /// A partition executed again joins a new round with a queue published
    /// afresh from the same plan, so it reads every unit again. Once every
    /// declared partition has left a round, the units left in its queue are
    /// dropped and their carries released.
    ///
    /// Fails against one queue for every execution (the re-executed partition
    /// finds it drained) and against a round that closes on its first leaver
    /// (partition 1 would find the queue empty) or never (the untaken units'
    /// reservation outlives the round).
    #[test]
    fn each_round_publishes_afresh_and_closes_when_every_partition_leaves() {
        let budget = Arc::new(ravel_memory::MemoryBudget::unlimited());
        let segs = vec![
            planned_seg((0..BLOCKS).collect(), None, &budget, 10),
            planned_seg((0..BLOCKS).collect(), None, &budget, 100),
        ];
        let refs = vec![seg_ref("a"), seg_ref("b")];
        let counts = assemble(segs, vec![false, false], &refs, true, false);
        let rounds = Arc::new(UnitRounds::new(2));
        let publish = || counts.publish(&refs, &[0, 1], true, 2);
        let run = GROUPS.div_ceil(2);

        let first = rounds.join(0);
        assert_eq!(first.partitions(), 2);
        let queue = first.queue(publish);
        let taken = queue.take_next().expect("a unit");
        let first_run: Vec<usize> = (0..run).flat_map(group).collect();
        assert_eq!((taken.ordinal, taken.indices.clone()), (0, first_run));
        drop(first);
        let second = rounds.join(1);
        let shared = second.queue(|| unreachable!("round 0 is published"));
        assert!(Arc::ptr_eq(&queue, &shared));

        let again = rounds.join(0);
        let fresh = again.queue(publish);
        assert!(!Arc::ptr_eq(&queue, &fresh));
        let mut reread = 0;
        while fresh.take_next().is_some() {
            reread += 1;
        }
        assert_eq!(
            reread,
            2 * 2,
            "the re-execution reads every run, two per segment"
        );

        assert_eq!(budget.reserved(), 110);
        drop(second);
        assert!(queue.take_next().is_none(), "round 0 closed");
        assert_eq!(
            budget.reserved(),
            10,
            "only the unit partition 0 took still holds segment 0"
        );
        drop(taken);
        assert_eq!(budget.reserved(), 0);
        drop(again);
    }

    /// Six segments in snapshot order: planned with every block (minimum 50),
    /// unplanned (10), irrelevant (0), relevant with no survivor (20), planned
    /// with survivors `[3, 4, 5, 9]` in three row groups (0), planned with two
    /// groups (30).
    fn mixed_plan(
        budget: &Arc<ravel_memory::MemoryBudget>,
        stripe: bool,
        by_min_ts: bool,
    ) -> (PlanCounts, Vec<SegmentRef>) {
        let segs = vec![
            planned_seg((0..BLOCKS).collect(), None, budget, 0),
            None,
            None,
            Some(PlannedUnit {
                planned: None,
                stats: ScanStats::default(),
                footer: None,
            }),
            planned_seg(vec![3, 4, 5, 9], None, budget, 0),
            planned_seg((0..8).collect(), None, budget, 0),
        ];
        let refs = [
            ("a", 50),
            ("u", 10),
            ("x", 0),
            ("e", 20),
            ("p", 0),
            ("t", 30),
        ]
        .into_iter()
        .map(|(key, min)| seg_ref_at(key, min))
        .collect::<Vec<_>>();
        let unplanned = vec![false, true, false, false, false, false];
        (assemble(segs, unplanned, &refs, stripe, by_min_ts), refs)
    }

    /// Without both halves of the global precondition the plan publishes in
    /// visit order: striped, each planned segment's surviving row groups in
    /// sequence (three partitions, so every run here is one group); unstriped, each planned segment as one unit of its survivors;
    /// an unplanned segment as one unit either way, and nothing for an
    /// irrelevant segment or one with no survivor. Under a TopK without a read
    /// cache the visit order is ascending segment minimum.
    ///
    /// Fails against building the global order on either half alone, and
    /// against publishing a segment that has no surviving block.
    #[test]
    fn publication_without_the_global_mode_follows_visit_order() {
        let budget = Arc::new(ravel_memory::MemoryBudget::unlimited());
        let unplanned = (1, vec![], true);
        let striped: Vec<Unit> = (0..GROUPS)
            .map(|g| (0, group(g), false))
            .chain([
                unplanned.clone(),
                (4, vec![3], false),
                (4, vec![4, 5], false),
                (4, vec![9], false),
                (5, group(0), false),
                (5, group(1), false),
            ])
            .collect();
        let whole = |s: usize, indices: Vec<usize>| (s, indices, false);
        let cases = [
            (true, false, striped),
            (
                false,
                false,
                vec![
                    whole(0, (0..BLOCKS).collect()),
                    unplanned.clone(),
                    whole(4, vec![3, 4, 5, 9]),
                    whole(5, (0..8).collect()),
                ],
            ),
            (
                false,
                true,
                vec![
                    whole(4, vec![3, 4, 5, 9]),
                    unplanned.clone(),
                    whole(5, (0..8).collect()),
                    whole(0, (0..BLOCKS).collect()),
                ],
            ),
        ];
        for (stripe, by_min_ts, expected) in cases {
            let (counts, refs) = mixed_plan(&budget, stripe, by_min_ts);
            assert!(
                counts.global.is_none(),
                "stripe {stripe} by_min_ts {by_min_ts}"
            );
            let units = counts.publish(&refs, &visit_order(&refs, by_min_ts), stripe, GROUPS);
            assert_eq!(
                units.iter().map(unit).collect::<Vec<_>>(),
                expected,
                "stripe {stripe} by_min_ts {by_min_ts}"
            );
            for o in &units {
                assert_eq!(o.dirs.is_some(), !o.unplanned);
            }
        }
    }

    #[derive(Clone, Debug)]
    enum Shape {
        Irrelevant,
        Unplanned,
        NoSurvivor,
        /// Surviving blocks (ascending), and a key per row group (only the
        /// first as many as there are groups are used).
        Planned(Vec<usize>, Vec<i64>),
    }

    fn shape() -> impl Strategy<Value = Shape> {
        prop_oneof![
            Just(Shape::Irrelevant),
            Just(Shape::Unplanned),
            Just(Shape::NoSurvivor),
            (
                proptest::collection::btree_set(0..BLOCKS, 1..=BLOCKS),
                proptest::collection::vec(-4i64..16, GROUPS),
            )
                .prop_map(|(blocks, keys)| Shape::Planned(blocks.into_iter().collect(), keys)),
        ]
    }

    /// What a plan of `shapes` must publish for `n` partitions, derived
    /// without [`PlanCounts`]: per segment its expected row groups, and the
    /// units in publication order. In visit order a striped segment of `G`
    /// groups is cut into runs of `ceil(G / n)` groups. In the global order
    /// the units sorted by key are dealt round robin to `n` queues, a queue's
    /// consecutive groups of one segment in ascending position form one run,
    /// and each run is published where its first unit sits in the sort.
    fn model(
        shapes: &[Shape],
        mins: &[i64],
        stripe: bool,
        by_min_ts: bool,
        n: usize,
    ) -> (Vec<Vec<Vec<usize>>>, Vec<Unit>) {
        let groups: Vec<Vec<Vec<usize>>> = shapes
            .iter()
            .map(|shape| {
                let mut groups: Vec<Vec<usize>> = Vec::new();
                if let Shape::Planned(blocks, _) = shape {
                    for &b in blocks {
                        match groups.last_mut() {
                            Some(g) if g[0] / GROUP_BLOCKS == b / GROUP_BLOCKS => g.push(b),
                            _ => groups.push(vec![b]),
                        }
                    }
                }
                groups
            })
            .collect();
        let is_unplanned = |s: usize| matches!(shapes[s], Shape::Unplanned);
        let run = |s: usize| groups[s].len().div_ceil(n);
        let units = if stripe && by_min_ts {
            let mut sorted: Vec<(i64, usize, Option<usize>)> = Vec::new();
            for (s, shape) in shapes.iter().enumerate() {
                match shape {
                    Shape::Unplanned => sorted.push((mins[s], s, None)),
                    Shape::Planned(_, keys) => {
                        sorted.extend((0..groups[s].len()).map(|g| (keys[g], s, Some(g))))
                    }
                    Shape::Irrelevant | Shape::NoSurvivor => {}
                }
            }
            sorted.sort();
            // Per partition, its runs as (global index of the first unit,
            // segment, group positions).
            let mut queues: Vec<Vec<(usize, usize, Vec<usize>)>> = vec![Vec::new(); n];
            for (i, (_, s, g)) in sorted.into_iter().enumerate() {
                let queue = &mut queues[i % n];
                match (queue.last_mut(), g) {
                    (Some((_, rs, gs)), Some(g))
                        if *rs == s && gs.last().is_some_and(|&last| last < g) =>
                    {
                        gs.push(g)
                    }
                    _ => queue.push((i, s, g.into_iter().collect())),
                }
            }
            let mut runs: Vec<(usize, usize, Vec<usize>)> = queues.into_iter().flatten().collect();
            runs.sort_by_key(|run| run.0);
            runs.into_iter()
                .map(|(_, s, gs)| {
                    let blocks: Vec<usize> =
                        gs.iter().flat_map(|&g| groups[s][g].clone()).collect();
                    (s, blocks, gs.is_empty())
                })
                .collect()
        } else {
            let mut order: Vec<usize> = (0..shapes.len()).collect();
            if by_min_ts {
                order.sort_by_key(|&s| (mins[s], s));
            }
            order
                .into_iter()
                .flat_map(|s| {
                    if is_unplanned(s) {
                        vec![(s, vec![], true)]
                    } else if groups[s].is_empty() {
                        vec![]
                    } else if stripe {
                        groups[s]
                            .chunks(run(s))
                            .map(|chunk| (s, chunk.concat(), false))
                            .collect()
                    } else {
                        vec![(s, groups[s].concat(), false)]
                    }
                })
                .collect()
        };
        (groups, units)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// Whatever the plan's shape, mode, partition count and the order
        /// partitions happen to take in: the takes, in the order they happen,
        /// are the published units in publication order; every unit is taken
        /// exactly once, by one partition; each partition's units keep
        /// publication order; every surviving row group lies whole inside
        /// exactly one taken unit; the queue is empty once every partition has
        /// found it so; every planned unit carries the directories; and once
        /// every taken unit has dropped, nothing stays reserved while the plan
        /// counts are alive.
        ///
        /// Fails against a queue that hands one unit to two takers (the take
        /// log is longer than the publication), against one that splits a row
        /// group (a group's blocks lie in two units), and against one that
        /// hands units out of publication order.
        #[test]
        fn every_surviving_row_group_is_consumed_exactly_once_by_one_partition(
            shapes in proptest::collection::vec(shape(), 0..7),
            mins in proptest::collection::vec(-4i64..16, 7),
            stripe in any::<bool>(),
            by_min_ts in any::<bool>(),
            n in 1usize..6,
            schedule in proptest::collection::vec(0usize..6, 0..48),
        ) {
            let budget = Arc::new(ravel_memory::MemoryBudget::unlimited());
            let (groups, expected) = model(&shapes, &mins, stripe, by_min_ts, n);
            let refs: Vec<SegmentRef> = (0..shapes.len())
                .map(|s| seg_ref_at(&format!("seg{s}"), mins[s]))
                .collect();
            let segs = shapes
                .iter()
                .zip(&groups)
                .map(|(shape, groups)| match shape {
                    Shape::Irrelevant | Shape::Unplanned => None,
                    Shape::NoSurvivor => Some(PlannedUnit {
                        planned: None,
                        stats: ScanStats::default(),
                        footer: None,
                    }),
                    Shape::Planned(blocks, keys) => planned_seg(
                        blocks.clone(),
                        Some(keys[..groups.len()].to_vec()),
                        &budget,
                        1,
                    ),
                })
                .collect();
            let unplanned = shapes.iter().map(|s| matches!(s, Shape::Unplanned)).collect();
            let counts = assemble(segs, unplanned, &refs, stripe, by_min_ts);

            let queue =
                UnitQueue::new(counts.publish(&refs, &visit_order(&refs, by_min_ts), stripe, n));
            let (owned, log) = take_all(&queue, n, &schedule);
            let taken: Vec<Unit> = log.iter().map(|(_, u)| u.clone()).collect();
            prop_assert_eq!(&taken, &expected);

            let mut seen = vec![0usize; expected.len()];
            for (p, units) in owned.iter().enumerate() {
                let positions: Vec<usize> = units
                    .iter()
                    .map(|o| expected.iter().position(|e| *e == unit(o)).expect("published"))
                    .collect();
                prop_assert!(
                    positions.windows(2).all(|w| w[0] < w[1]),
                    "partition {} took {:?}",
                    p,
                    positions
                );
                for pos in positions {
                    seen[pos] += 1;
                }
                for o in units {
                    prop_assert_eq!(o.dirs.is_some(), !o.unplanned);
                }
            }
            prop_assert!(seen.iter().all(|&c| c == 1), "takes per unit {:?}", seen);

            for (s, segment_groups) in groups.iter().enumerate() {
                for group in segment_groups {
                    let holders: Vec<&OwnedSeg> = owned
                        .iter()
                        .flatten()
                        .filter(|o| o.ordinal == s && group.iter().any(|b| o.indices.contains(b)))
                        .collect();
                    prop_assert_eq!(holders.len(), 1, "segment {} group {:?}", s, group);
                    prop_assert!(group.iter().all(|b| holders[0].indices.contains(b)));
                }
            }
            prop_assert!(queue.take_next().is_none());
            drop(owned);
            prop_assert_eq!(budget.reserved(), 0);
            drop(counts);
        }

        /// Over an object of one-block row groups, so a segment's `G` is its
        /// survivor count (1 to 12), with a read cache and no TopK, with a
        /// read cache and the global order, and without a read cache: with a
        /// read cache and no TopK `n` partitions open each segment at most
        /// `min(G, n)` times, without a read cache exactly once, and in the
        /// global order exactly as many times as [`main_global_deal`] deals
        /// it runs; every surviving row group is read by exactly one owner,
        /// each partition's units keep publication order, and every unit's
        /// indices ascend.
        ///
        /// Fails against one unit per row group (a segment of `G > n` groups
        /// opens `G` times), against runs cut to `floor(G / n)` (`n + 1` runs
        /// when `n` does not divide `G`), and, in the global mode, against
        /// cutting runs to `ceil(G / n)` groups (a segment whose groups the
        /// deal gives one partition in a row opens more than once).
        #[test]
        fn runs_open_each_segment_at_most_min_groups_partitions_times(
            survivors in proptest::collection::vec(
                proptest::collection::btree_set(0..BLOCKS, 1..=BLOCKS),
                1..4,
            ),
            keys in proptest::collection::vec(proptest::collection::vec(-4i64..16, BLOCKS), 3),
            mode in 0usize..3,
            n in 1usize..8,
            schedule in proptest::collection::vec(0usize..8, 0..48),
        ) {
            let (stripe, by_min_ts) = [(true, false), (true, true), (false, false)][mode];
            let budget = Arc::new(ravel_memory::MemoryBudget::unlimited());
            let dirs = dirs_grouped(1);
            let refs: Vec<SegmentRef> = (0..survivors.len())
                .map(|s| seg_ref(&format!("seg{s}")))
                .collect();
            let segs: Vec<Option<PlannedUnit>> = survivors
                .iter()
                .zip(&keys)
                .map(|(blocks, keys)| {
                    planned_seg_in(
                        dirs.clone(),
                        blocks.iter().copied().collect(),
                        Some(keys[..blocks.len()].to_vec()),
                        &budget,
                        1,
                    )
                })
                .collect();
            let dealt = main_global_deal(&segs, &vec![false; survivors.len()], &refs, n);
            let counts = assemble(segs, vec![false; survivors.len()], &refs, stripe, by_min_ts);
            prop_assert_eq!(counts.global.is_some(), stripe && by_min_ts);

            let units = counts.publish(&refs, &visit_order(&refs, by_min_ts), stripe, n);
            let order: Vec<Unit> = units.iter().map(unit).collect();
            let queue = UnitQueue::new(units);
            let (owned, _) = take_all(&queue, n, &schedule);

            for (s, blocks) in survivors.iter().enumerate() {
                let opens = owned.iter().flatten().filter(|o| o.ordinal == s).count();
                if by_min_ts {
                    prop_assert_eq!(
                        opens,
                        dealt.iter().flatten().filter(|run| run.seg == s).count(),
                        "segment {} of {} groups by {} partitions",
                        s,
                        blocks.len(),
                        n
                    );
                } else {
                    let bound = if stripe { blocks.len().min(n) } else { 1 };
                    prop_assert!(
                        (1..=bound).contains(&opens),
                        "segment {} of {} groups opened {} times by {} partitions",
                        s,
                        blocks.len(),
                        opens,
                        n
                    );
                }
                for b in blocks {
                    let owners = owned
                        .iter()
                        .flatten()
                        .filter(|o| o.ordinal == s && o.indices.contains(b))
                        .count();
                    prop_assert_eq!(owners, 1, "segment {} block {}", s, b);
                }
            }
            let read: usize = owned.iter().flatten().map(|o| o.indices.len()).sum();
            prop_assert_eq!(read, survivors.iter().map(|b| b.len()).sum::<usize>());
            for (p, units) in owned.iter().enumerate() {
                let positions: Vec<usize> = units
                    .iter()
                    .map(|o| order.iter().position(|e| *e == unit(o)).expect("published"))
                    .collect();
                prop_assert!(
                    positions.windows(2).all(|w| w[0] < w[1]),
                    "partition {} took {:?}",
                    p,
                    positions
                );
                for o in units {
                    prop_assert!(o.indices.windows(2).all(|w| w[0] < w[1]));
                }
            }
            drop(owned);
            prop_assert_eq!(budget.reserved(), 0);
            drop(counts);
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod carried_directory_reservation_tests {
    //! ADR-2414 decision A1: the plan phase's carried directories are charged to
    //! the fetch memory budget a carried whole object is charged to, for the
    //! segments a partition will open and no others, until every partition
    //! owning one of the segment's row groups has finished it; and an open whose
    //! own pruning disagrees with the plan's survivor list fails closed.

    use super::*;
    use ravel_catalog::SegmentLevel;
    use ravel_logseg::footer::kind;
    use ravel_logseg::record::stream_attrs_bytes;
    use ravel_logseg::{ObjectIdentity, RlogConfig, RlogReader, RlogWriter};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{ObjectStoreBackend, PutOptions};
    use ravel_query::BlockRangeFetcher;
    use ravel_types::logstream::log_stream_id;
    use uuid::Uuid;

    const TENANT: TenantHash = TenantHash([7u8; 16]);
    const BLOCKS: usize = 12;
    /// Spacing of the timestamps of the sparse object, wider than the query
    /// window below, so no block of it can hold a row in the window.
    const SPARSE_STEP: i64 = 10;

    /// One-record blocks with timestamps `ts_step * i`.
    fn object(seq: u64, ts_step: i64) -> Vec<u8> {
        object_from(seq, ts_step, 0)
    }

    /// One-record blocks with timestamps `base + ts_step * i`.
    fn object_from(seq: u64, ts_step: i64, base: i64) -> Vec<u8> {
        let cfg = RlogConfig {
            block_target_records: 1,
            group_target_blocks: 4,
            ..RlogConfig::default()
        };
        let identity = ObjectIdentity {
            tenant_hash: [7u8; 16],
            shard: 0,
            writer_id: [2u8; 16],
            writer_epoch: 1,
            writer_seq: seq,
        };
        let resource = vec![("service.name".to_string(), AttrValue::Str("svc".into()))];
        let mut writer = RlogWriter::new(cfg, identity);
        for i in 0..BLOCKS as i64 {
            let ts = base + ts_step * i;
            writer
                .push(ravel_logseg::LogRecord {
                    stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
                    stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
                    ts_ns: ts,
                    observed_ts_ns: ts,
                    severity_num: 9,
                    severity_text: "INFO".into(),
                    body: format!("row {ts} {}", "payload ".repeat(32)),
                    trace_id: None,
                    span_id: None,
                    flags: 0,
                    attrs: Vec::new(),
                })
                .expect("push");
        }
        writer.finish().expect("finish")
    }

    /// `(stored, decoded)` bytes of the four directory sections of `obj`.
    fn directory_bytes(obj: &[u8]) -> (u64, u64) {
        let footer = ravel_logseg::footer::open(obj).expect("footer");
        [
            kind::STREAM_DIR,
            kind::FIELD_DIR,
            kind::SKIP_IDX,
            kind::PAGE_DIR,
        ]
        .iter()
        .map(|k| footer.section(*k).expect("section"))
        .fold((0, 0), |(stored, decoded), d| {
            (stored + d.len, decoded + d.uncomp_len)
        })
    }

    fn seg_ref(key: &str, obj: &[u8], ts_step: i64, seq: u64) -> SegmentRef {
        SegmentRef {
            data_object_key: key.to_string(),
            object_size: obj.len() as u64,
            min_event_ts_ns: 0,
            max_event_ts_ns: ts_step * (BLOCKS as i64 - 1),
            ingest_hour_bucket: 0,
            sample_count: BLOCKS as u64,
            series_count: 0,
            shard: 0,
            content_hash: [seq as u8; 32],
            writer_id: Uuid::from_u128(1),
            writer_epoch: 1,
            writer_seq: seq,
            created_unix_ns: 0,
            level: SegmentLevel::L0,
            segment_format_version: u32::from(ravel_logseg::footer::VERSION),
            declared_column_stats: Default::default(),
        }
    }

    /// Two segments with a surviving block and one that overlaps the query
    /// window but has none: the budget holds exactly the first two segments'
    /// decoded directory bytes while the plan counts live, and nothing once
    /// they drop.
    ///
    /// Fails against no reservation (`reserved` is 0), against reserving for
    /// every relevant segment (the zero-survivor segment's bytes are added),
    /// and against reserving the stored section lengths (the fixture's
    /// sections compress, so stored and decoded totals differ).
    #[tokio::test]
    async fn carried_directories_reserve_decoded_bytes_for_segments_with_survivors() {
        let store = Arc::new(MemoryStore::new());
        let dense = [object(1, 1), object(2, 1)];
        let sparse = object(3, SPARSE_STEP);
        let mut segments = Vec::new();
        for (i, obj) in dense.iter().enumerate() {
            let key = format!("t/dense{i}.rlog");
            store
                .put(&key, bytes::Bytes::from(obj.clone()), PutOptions::default())
                .await
                .expect("put");
            segments.push(seg_ref(&key, obj, 1, i as u64 + 1));
        }
        store
            .put(
                "t/sparse.rlog",
                bytes::Bytes::from(sparse.clone()),
                PutOptions::default(),
            )
            .await
            .expect("put");
        segments.push(seg_ref("t/sparse.rlog", &sparse, SPARSE_STEP, 3));

        let with_survivors: (u64, u64) = dense
            .iter()
            .map(|o| directory_bytes(o))
            .fold((0, 0), |a, d| (a.0 + d.0, a.1 + d.1));
        let sparse_decoded = directory_bytes(&sparse).1;
        assert_ne!(
            with_survivors.0, with_survivors.1,
            "the fixture's sections compress, so stored and decoded totals differ"
        );
        assert!(sparse_decoded > 0);

        // The window is inside every segment's catalog range and inside a gap
        // of the sparse object's timestamps: it keeps block 11 of each dense
        // object (ts 11) and no block of the sparse one.
        let budget = Arc::new(ravel_memory::MemoryBudget::unlimited());
        let fetcher = LogSegmentFetcher::new(store.clone())
            .with_memory_budget(Arc::clone(&budget))
            .with_block_range(
                BlockRangeFetcher::new(store)
                    .with_suffix_len(256)
                    .with_whole_object_threshold(0),
            )
            .with_block_range_threshold(0);
        let ctx = PartitionCtx {
            fetcher,
            tenant_hash: TENANT,
            query: LogQuery::new(11, 19),
            columns: ColumnSelection::all(),
            projected_fraction: 1.0,
            phase_accounting: PhaseAccounting::new(),
        };
        assert_eq!(budget.reserved(), 0);
        let counts = compute_plan_counts(&ctx, &segments, 4, true, false)
            .await
            .expect("plan counts");
        assert_eq!(
            counts.segs[0].as_ref().map(|s| s.planned.is_some()),
            Some(true)
        );
        assert_eq!(
            counts.segs[2].as_ref().map(|s| s.planned.is_some()),
            Some(false),
            "the zero-survivor segment carries no directories"
        );
        assert_eq!(
            budget.reserved(),
            with_survivors.1,
            "the decoded directory bytes of the segments with survivors, nothing for the other"
        );
        drop(counts);
        assert_eq!(budget.reserved(), 0, "released when the plan counts drop");

        // The refusal, on objects whose directories are much larger than what a
        // plan read buffers (a long repetitive attribute value decodes to far
        // more than its compressed section): a budget of exactly the two
        // segments' decoded directories admits their plan, and one byte less
        // refuses it with the typed fetch memory error a refused whole-object
        // reservation reports, on the reservation that completes the
        // directories. The reads run one at a time, so each reads with only the
        // earlier segments' directories held. On the fixture above a plan
        // read's buffers outweigh a segment's directories, so any limit that
        // refuses there refuses a read's buffer first, with the same error, and
        // a smaller limit here does the same: neither shows that the
        // directories are what trips.
        let store = Arc::new(MemoryStore::new());
        let objects = [overflow_object(1), overflow_object(2)];
        let mut segments = Vec::new();
        for (i, obj) in objects.iter().enumerate() {
            let key = format!("t/large{i}.rlog");
            store
                .put(&key, bytes::Bytes::from(obj.clone()), PutOptions::default())
                .await
                .expect("put");
            segments.push(seg_ref(&key, obj, 1, i as u64 + 1));
        }
        let per_segment: Vec<u64> = objects.iter().map(|o| directory_bytes(o).1).collect();
        let total: u64 = per_segment.iter().sum();
        let with_limit = |limit: u64| PartitionCtx {
            fetcher: LogSegmentFetcher::new(store.clone())
                .with_memory_budget(Arc::new(ravel_memory::MemoryBudget::new(limit)))
                .with_block_range(
                    BlockRangeFetcher::new(store.clone())
                        .with_suffix_len(256)
                        .with_whole_object_threshold(0),
                )
                .with_block_range_threshold(0),
            tenant_hash: TENANT,
            query: LogQuery::new(11, 19),
            columns: ColumnSelection::all(),
            projected_fraction: 1.0,
            phase_accounting: PhaseAccounting::new(),
        };
        let exact = compute_plan_counts(&with_limit(total), &segments, 1, true, false).await;
        assert!(
            exact.is_ok(),
            "the directories fit a budget of exactly their size: {:?}",
            exact.as_ref().err()
        );
        drop(exact);
        let err = compute_plan_counts(&with_limit(total - 1), &segments, 1, true, false)
            .await
            .err()
            .expect("a budget one byte short of the directories refuses");
        let typed = match &err {
            DataFusionError::External(e) => e.downcast_ref::<SqlError>(),
            _ => None,
        };
        let Some(SqlError::LogFetch(LogFetchError::FetchMemoryExhausted {
            requested,
            reserved,
            limit,
            ..
        })) = typed
        else {
            panic!("expected the typed fetch memory error, got: {err:?}");
        };
        assert_eq!(
            (*requested, *reserved, *limit),
            (per_segment[1], per_segment[0], total - 1),
            "the second segment's directories are refused with the first's held"
        );
    }

    /// A sub-bound segment the plan reads whole (fewer such segments than
    /// partitions, so it is planned) carries the whole object to every one of
    /// its three row-group units. The whole object and the directories stay
    /// reserved while any owner holds a unit of the segment, through owner A
    /// finishing one unit and taking another and finishing that too, and are
    /// released exactly when owner B, the last, finishes, with the plan counts
    /// still alive.
    ///
    /// Fails against a slot that hands the whole object to its first taker
    /// only (`take()` instead of `clone()`: units 1 and 2 carry none), against
    /// a slot that keeps its own handle (the reservation outlives every owner),
    /// and against releasing on the first owner's finish.
    #[tokio::test]
    async fn carried_whole_objects_are_released_when_every_owner_finishes_the_segment() {
        let store = Arc::new(MemoryStore::new());
        let obj = object(1, 1);
        store
            .put(
                "t/whole.rlog",
                bytes::Bytes::from(obj.clone()),
                PutOptions::default(),
            )
            .await
            .expect("put");
        let segments = vec![seg_ref("t/whole.rlog", &obj, 1, 1)];
        let budget = Arc::new(ravel_memory::MemoryBudget::unlimited());
        let ctx = PartitionCtx {
            fetcher: LogSegmentFetcher::new(store)
                .with_memory_budget(Arc::clone(&budget))
                .with_block_range_threshold(u64::MAX),
            tenant_hash: TENANT,
            query: LogQuery::new(1, 12),
            columns: ColumnSelection::all(),
            projected_fraction: 1.0,
            phase_accounting: PhaseAccounting::new(),
        };
        let counts = compute_plan_counts(&ctx, &segments, 2, true, false)
            .await
            .expect("plan counts");
        let planned = counts.planned(0).expect("planned");
        assert_eq!(
            planned
                .carried
                .lock()
                .whole_object
                .as_ref()
                .map(CarriedWholeObject::byte_len),
            Some(obj.len() as u64),
            "the plan read the object whole and carries it"
        );
        let held = budget.reserved();
        assert!(
            held >= directory_bytes(&obj).1 + obj.len() as u64,
            "the directories and the whole object are both reserved: {held}"
        );

        // Published for three partitions, so each of the three runs is one
        // row group and owner A can take a second run of the same segment.
        let queue = UnitQueue::new(counts.publish(&segments, &[0], true, 3));
        {
            let slot = planned.carried.lock();
            assert!(slot.whole_object.is_none() && slot.dirs.is_none());
        }
        let a0 = queue.take_next().expect("unit 0");
        let b1 = queue.take_next().expect("unit 1");
        let carries = |unit: &OwnedSeg| {
            assert_eq!(
                unit.whole_object.as_ref().map(CarriedWholeObject::byte_len),
                Some(obj.len() as u64),
                "unit {:?} carries the whole object",
                unit.indices
            );
            assert!(unit.dirs.is_some());
        };
        carries(&a0);
        carries(&b1);
        assert_eq!(budget.reserved(), held);
        drop(a0);
        assert_eq!(budget.reserved(), held, "owner B still holds unit 1");
        let a2 = queue.take_next().expect("unit 2");
        assert!(queue.take_next().is_none());
        carries(&a2);
        assert_eq!(budget.reserved(), held);
        drop(a2);
        assert_eq!(budget.reserved(), held, "owner B still holds unit 1");
        drop(b1);
        assert_eq!(
            budget.reserved(),
            0,
            "released when the last owner finishes, with the plan counts alive"
        );
        drop(counts);
    }

    /// A segment's rows through a scan whose plan counts are seeded with
    /// `plan_indices` as the survivor list, over a ts window that keeps blocks
    /// 6..12 of the object. Returns the rows' timestamps, or the stream's error.
    async fn scan_with_plan(plan_indices: Vec<usize>) -> Result<Vec<i64>, String> {
        use datafusion::arrow::array::Array;
        let store = Arc::new(MemoryStore::new());
        let obj = object(1, 1);
        store
            .put(
                "t/dense0.rlog",
                bytes::Bytes::from(obj.clone()),
                PutOptions::default(),
            )
            .await
            .expect("put");
        let seg = seg_ref("t/dense0.rlog", &obj, 1, 1);
        let dirs = Arc::new(
            RlogReader::decode_directories(&obj[..], &RlogConfig::default()).expect("directories"),
        );
        let budget = Arc::new(ravel_memory::MemoryBudget::unlimited());
        let schema = crate::logs_schema::logs_schema_with_declared(&[]);
        let exec = LogsScanExec::new(
            TENANT,
            LogSegmentFetcher::new(store),
            std::slice::from_ref(&seg),
            1,
            6,
            i64::MAX,
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
            None,
            PhaseAccounting::new(),
            schema,
            Arc::new(Vec::new()),
        )
        .expect("scan");
        let counts = PlanCounts {
            segs: vec![Some(PlannedUnit {
                planned: Some(PlannedBlocks::new(
                    plan_indices,
                    dirs,
                    budget.reserve(0).expect("reserve"),
                )),
                stats: ScanStats::default(),
                footer: None,
            })],
            unplanned: vec![false],
            full_reads: 1,
            global: None,
            key_index: KeyIndexPruning::default(),
        };
        assert!(exec.counts.set(Arc::new(counts)).is_ok());
        let mut stream = exec
            .execute(0, Arc::new(TaskContext::default()))
            .expect("execute");
        let mut ts = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(|e| e.to_string())?;
            let col = batch
                .column(crate::logs_schema::LOG_COL_TS)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("ts type")
                .clone();
            ts.extend((0..col.len()).map(|i| col.value(i)));
        }
        Ok(ts)
    }

    /// A plan whose survivor list is the blocks this open's own pruning keeps
    /// reads them all; a plan listing a block the open pruned (or missing one it
    /// kept) is refused with the typed mismatch error instead of reading the
    /// intersection.
    ///
    /// Fails against an open that intersects the dealt blocks with its own
    /// survivors (it returns rows for the plan that lists blocks 0..12, here 6
    /// of them, with no error), and against an open that only compares the two
    /// lists' lengths (the plan below that lists 5..11 has the same length as
    /// the truth and must still be refused).
    #[tokio::test]
    async fn an_open_that_prunes_differently_from_the_plan_fails_closed() {
        assert_eq!(
            scan_with_plan((6..12).collect()).await,
            Ok((6..12).collect::<Vec<i64>>()),
            "a plan that matches the open reads every surviving block"
        );
        for wrong in [(0..12).collect::<Vec<usize>>(), (5..11).collect()] {
            let err = scan_with_plan(wrong.clone())
                .await
                .expect_err("a plan that differs from the open must refuse");
            assert!(
                err.contains("segment survivor mismatch"),
                "plan {wrong:?} refused with the typed error, got: {err}"
            );
        }
    }

    /// One stream whose resource attribute is this long, so each object's
    /// STREAM_DIR, and with it the carried directories, dwarfs the few page
    /// bytes an open holds for a timestamp projection.
    const RESOURCE_VALUE_LEN: usize = 64 * 1024;

    /// One-record blocks, `ts` 0..12, in groups of four, whose two attributes
    /// overflow a one-column dynamic budget: every block carries an `attrs_raw`
    /// page, so the columnar drain falls back to a row-path reopen.
    fn overflow_object(seq: u64) -> Vec<u8> {
        let cfg = RlogConfig {
            block_target_records: 1,
            group_target_blocks: 4,
            max_dynamic_columns: 1,
            ..RlogConfig::default()
        };
        let identity = ObjectIdentity {
            tenant_hash: [7u8; 16],
            shard: 0,
            writer_id: [2u8; 16],
            writer_epoch: 1,
            writer_seq: seq,
        };
        let resource = vec![(
            "service.name".to_string(),
            AttrValue::Str(format!("{seq}-{}", "r".repeat(RESOURCE_VALUE_LEN))),
        )];
        let mut writer = RlogWriter::new(cfg, identity);
        for ts in 0..BLOCKS as i64 {
            writer
                .push(ravel_logseg::LogRecord {
                    stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
                    stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
                    ts_ns: ts,
                    observed_ts_ns: ts,
                    severity_num: 9,
                    severity_text: "INFO".into(),
                    body: format!("row {ts}"),
                    trace_id: None,
                    span_id: None,
                    flags: 0,
                    attrs: vec![
                        ("a".to_string(), AttrValue::Str(format!("a{ts}"))),
                        ("b".to_string(), AttrValue::Str(format!("b{ts}"))),
                    ],
                })
                .expect("push");
        }
        writer.finish().expect("finish")
    }

    fn rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(RecordBatch::num_rows).sum()
    }

    fn metric_total(exec: &LogsScanExec, name: &str) -> usize {
        exec.metrics()
            .expect("metrics")
            .iter()
            .filter(|m| m.value().name() == name)
            .map(|m| m.value().as_usize())
            .sum()
    }

    /// Three segments of three row groups, striped over two partitions, so
    /// each segment is two runs: groups 0 and 1, then group 2. A segment's
    /// directories stay reserved while any owner is still on one of its runs
    /// and are released once every owner has finished it, not when the
    /// statement ends.
    ///
    /// Partition 0 takes segment 0's first run and yields a batch; partition 1
    /// then takes segment 0's second run and yields a batch, and stays on it.
    /// Partition 0 drains every other unit: segments 1 and 2, which it alone
    /// owned, are released, and segment 0 is still reserved for partition 1,
    /// whose `attrs_raw` row-path reopens of it come after partition 0
    /// finished it. Once partition 1 drains, nothing is reserved while the exec
    /// is still alive.
    ///
    /// Fails against releasing on the first owner's finish (segment 0 is gone
    /// while partition 1 is still on it), against never releasing (segments
    /// 1 and 2 are still reserved once their only owner has finished them),
    /// and against one unit per row group (nine units, 18 reopens).
    #[tokio::test]
    async fn carried_directories_are_released_when_every_owner_finishes_the_segment() {
        use futures::StreamExt;

        let store = Arc::new(MemoryStore::new());
        let objects: Vec<Vec<u8>> = (1..=3).map(overflow_object).collect();
        let mut segments = Vec::new();
        for (i, obj) in objects.iter().enumerate() {
            let key = format!("t/held{i}.rlog");
            store
                .put(&key, bytes::Bytes::from(obj.clone()), PutOptions::default())
                .await
                .expect("put");
            segments.push(seg_ref(&key, obj, 1, i as u64 + 1));
        }
        let dirs: Vec<u64> = objects.iter().map(|o| directory_bytes(o).1).collect();
        let all: u64 = dirs.iter().sum();
        // What an open holds besides the directories (a timestamp projection's
        // pages and the tail sections) is far below this.
        let slack = dirs.iter().min().copied().unwrap_or(0) / 2;
        assert!(slack as usize > RESOURCE_VALUE_LEN / 4);

        let budget = Arc::new(ravel_memory::MemoryBudget::unlimited());
        let cache_bytes = 64u64 << 20;
        let cache: Arc<ravel_cache::Cache<ravel_query::CacheFetchError>> =
            Arc::new(ravel_cache::Cache::new(ravel_cache::CacheLimits::new(
                cache_bytes,
                4096,
                cache_bytes,
            )));
        let fetcher = LogSegmentFetcher::new(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>)
            .with_memory_budget(Arc::clone(&budget))
            .with_cache(cache)
            .with_block_range(
                BlockRangeFetcher::new(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>)
                    .with_suffix_len(256)
                    .with_whole_object_threshold(0),
            )
            .with_block_range_threshold(0);
        let schema = crate::logs_schema::logs_schema_with_declared(&[]);
        // The window starts after each segment's first block, so no segment is
        // contained in it and the striped route runs.
        let exec = LogsScanExec::new(
            TENANT,
            fetcher,
            &segments,
            2,
            1,
            BLOCKS as i64,
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
            Some(&vec![crate::logs_schema::LOG_COL_TS]),
            PhaseAccounting::new(),
            schema,
            Arc::new(Vec::new()),
        )
        .expect("scan");
        let task_ctx = Arc::new(TaskContext::default());
        let mut first = exec.execute(0, Arc::clone(&task_ctx)).expect("execute");
        let mut second = exec.execute(1, task_ctx).expect("execute");

        let head = first
            .next()
            .await
            .expect("a batch")
            .expect("partition 0 batch");
        let mid = second
            .next()
            .await
            .expect("a batch")
            .expect("partition 1 batch");
        assert!(
            budget.reserved() >= all && budget.reserved() < all + slack,
            "both partitions are on segment 0 and nobody has finished a segment: {} of {all}",
            budget.reserved()
        );

        let drained: Vec<RecordBatch> =
            first.map(|b| b.expect("partition 0 batch")).collect().await;
        // Every unit but segment 0's second run (group 2, four blocks); every
        // segment's first run is short of block 0.
        assert_eq!(
            rows(std::slice::from_ref(&head)) + rows(&drained),
            3 * (BLOCKS - 1) - 4
        );
        assert!(
            budget.reserved() >= dirs[0] && budget.reserved() < dirs[0] + slack,
            "partition 1 is still on segment 0, so only segment 0 stays reserved: {} of {}",
            budget.reserved(),
            dirs[0]
        );

        let rest: Vec<RecordBatch> = second
            .map(|b| b.expect("partition 1 batch"))
            .collect()
            .await;
        assert_eq!(rows(std::slice::from_ref(&mid)) + rows(&rest), 4);
        // The read cache keeps what it admitted, with that read's reservation,
        // until it drops with the exec.
        assert!(
            budget.reserved() < slack,
            "every segment's directories released while the exec is alive: {}",
            budget.reserved()
        );
        // Every block overflows, so each unit's open falls back twice in a row
        // and commits the rest of its blocks to the row path: two reopens per
        // unit, and three segments of two runs each.
        assert_eq!(
            metric_total(&exec, "reopens"),
            12,
            "two `attrs_raw` reopens per unit, six units"
        );
        drop(exec);
        assert_eq!(budget.reserved(), 0);
    }

    /// The same release in the global order (issue #2694): a confirmed
    /// ascending `ts` TopK whose filter never publishes, so the scan publishes
    /// row groups in [`global_order`] and skips nothing. Three identical
    /// segments whose groups key 1, 4 and 8 sort every segment's group 0,
    /// then every group 1, then every group 2; dealt over two partitions no
    /// partition is dealt two groups of one segment in a row, so the queue
    /// publishes the nine row groups as nine units in that order. The read
    /// cache admits nothing (no entry fits in one byte).
    ///
    /// Partition 0 takes segment 0's group 0 and partition 1 segment 1's;
    /// partition 0 drains the other seven units. Every slot handed its
    /// handles out at publication, so only segment 1, which partition 1 is
    /// still on, stays reserved; once partition 1 drains too, exactly segment
    /// 1's directories are released while the exec is still alive.
    ///
    /// Fails against an owner count above the number of published units (a
    /// segment stays reserved after both partitions drain), against
    /// publishing in visit order (partition 1 would be on segment 0's group
    /// 1), and against cutting each segment into `ceil(3 / 2)`-group runs (six
    /// units: 12 reopens and 51 block decodes).
    #[tokio::test]
    async fn carried_directories_are_released_in_the_global_order() {
        use futures::StreamExt;

        let store = Arc::new(MemoryStore::new());
        let objects: Vec<Vec<u8>> = (1..=3).map(overflow_object).collect();
        let mut segments = Vec::new();
        for (i, obj) in objects.iter().enumerate() {
            let key = format!("t/global{i}.rlog");
            store
                .put(&key, bytes::Bytes::from(obj.clone()), PutOptions::default())
                .await
                .expect("put");
            segments.push(seg_ref(&key, obj, 1, i as u64 + 1));
        }
        let dirs: Vec<u64> = objects.iter().map(|o| directory_bytes(o).1).collect();
        // What an open holds besides the directories is far below this.
        let slack = dirs.iter().min().copied().unwrap_or(0) / 2;
        assert!(slack as usize > RESOURCE_VALUE_LEN / 4);

        let budget = Arc::new(ravel_memory::MemoryBudget::unlimited());
        let cache: Arc<ravel_cache::Cache<ravel_query::CacheFetchError>> = Arc::new(
            ravel_cache::Cache::new(ravel_cache::CacheLimits::new(64 << 20, 4096, 1)),
        );
        let fetcher = LogSegmentFetcher::new(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>)
            .with_memory_budget(Arc::clone(&budget))
            .with_cache(cache)
            .with_block_range(
                BlockRangeFetcher::new(Arc::clone(&store) as Arc<dyn ObjectStoreBackend>)
                    .with_suffix_len(256)
                    .with_whole_object_threshold(0),
            )
            .with_block_range_threshold(0);
        let schema = crate::logs_schema::logs_schema_with_declared(&[]);
        let ts: Arc<dyn PhysicalExpr> = Arc::new(Column::new("ts", crate::logs_schema::LOG_COL_TS));
        let filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![ts],
            datafusion::physical_expr::expressions::lit(true),
        ));
        // The window starts after each segment's first block, so no segment is
        // contained in it and the striped route runs.
        let exec = LogsScanExec::new(
            TENANT,
            fetcher,
            &segments,
            2,
            1,
            BLOCKS as i64,
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
            Some(&vec![crate::logs_schema::LOG_COL_TS]),
            PhaseAccounting::new(),
            schema,
            Arc::new(Vec::new()),
        )
        .expect("scan")
        .with_topk(Some(TopKThreshold {
            filter,
            confirmed: true,
        }));
        let task_ctx = Arc::new(TaskContext::default());
        let mut first = exec.execute(0, Arc::clone(&task_ctx)).expect("execute");
        let mut second = exec.execute(1, task_ctx).expect("execute");

        let head = first
            .next()
            .await
            .expect("a batch")
            .expect("partition 0 batch");
        let mid = second
            .next()
            .await
            .expect("a batch")
            .expect("partition 1 batch");
        let counts = exec.counts.get().expect("planned");
        let groups = |pos: usize| (0..3).map(move |seg| (seg, UnitBlocks::Groups(vec![pos])));
        assert_eq!(
            counts.units(&[], true, 2),
            (0..3).flat_map(groups).collect::<Vec<_>>(),
            "every segment's group 0, then every group 1, then every group 2"
        );
        let slots: Vec<(bool, bool, usize)> = counts
            .segs
            .iter()
            .flatten()
            .filter_map(|s| s.planned.as_ref())
            .map(|p| {
                let state = p.carried.lock();
                (
                    state.dirs.is_some(),
                    state.whole_object.is_some(),
                    state.takers_left,
                )
            })
            .collect();
        assert_eq!(
            slots,
            vec![(false, false, 0); 3],
            "publication handed every handle out"
        );

        let drained: Vec<RecordBatch> =
            first.map(|b| b.expect("partition 0 batch")).collect().await;
        // Every unit but segment 1's group 0, blocks 1..4.
        assert_eq!(
            rows(std::slice::from_ref(&head)) + rows(&drained),
            3 * (BLOCKS - 1) - 3
        );
        let after_first = budget.reserved();
        assert!(
            after_first >= dirs[1] && after_first < dirs[1] + slack,
            "only segment 1 stays reserved: {after_first} of {}",
            dirs[1]
        );

        let rest: Vec<RecordBatch> = second
            .map(|b| b.expect("partition 1 batch"))
            .collect()
            .await;
        assert_eq!(rows(std::slice::from_ref(&mid)) + rows(&rest), 3);
        assert!(
            budget.reserved() < slack,
            "segment 1's directories released while the exec is alive: {}",
            budget.reserved()
        );
        // Every block overflows, so each unit's open goes columnar on its
        // first block, falls back (reopen, row-path block), goes columnar on
        // its second block, falls back again (reopen, skipping the first block
        // and row-pathing the second) and then commits the rest to the row
        // path: two reopens and three decodes on top of its blocks' own, for
        // every unit, each of which here has at least two blocks. Each segment
        // is three units and decodes its 11 surviving blocks once each, so
        // reopens are 3 * 3 * 2 = 18 and decodes 3 * (11 + 3 * 3) = 60.
        assert_eq!(metric_total(&exec, "reopens"), 18);
        assert_eq!(metric_total(&exec, "blocks_scanned"), 60);
        drop(exec);
        assert_eq!(budget.reserved(), 0);
    }

    /// A cache-wired scan of `segments` with one partition under a confirmed
    /// ascending `ts` TopK whose filter never publishes, over the window
    /// `[ts_min, ts_max]`, run to the end: the exec and the rows it read.
    async fn sole_partition_topk_scan(
        store: &Arc<MemoryStore>,
        segments: &[SegmentRef],
        ts_min: i64,
        ts_max: i64,
    ) -> (LogsScanExec, usize) {
        use futures::StreamExt;

        let cache: Arc<ravel_cache::Cache<ravel_query::CacheFetchError>> = Arc::new(
            ravel_cache::Cache::new(ravel_cache::CacheLimits::new(64 << 20, 4096, 1)),
        );
        let fetcher = LogSegmentFetcher::new(Arc::clone(store) as Arc<dyn ObjectStoreBackend>)
            .with_cache(cache)
            .with_block_range(
                BlockRangeFetcher::new(Arc::clone(store) as Arc<dyn ObjectStoreBackend>)
                    .with_suffix_len(256)
                    .with_whole_object_threshold(0),
            )
            .with_block_range_threshold(0);
        let schema = crate::logs_schema::logs_schema_with_declared(&[]);
        let ts: Arc<dyn PhysicalExpr> = Arc::new(Column::new("ts", crate::logs_schema::LOG_COL_TS));
        let filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![ts],
            datafusion::physical_expr::expressions::lit(true),
        ));
        let exec = LogsScanExec::new(
            TENANT,
            fetcher,
            segments,
            1,
            ts_min,
            ts_max,
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
            Some(&vec![crate::logs_schema::LOG_COL_TS]),
            PhaseAccounting::new(),
            schema,
            Arc::new(Vec::new()),
        )
        .expect("scan")
        .with_topk(Some(TopKThreshold {
            filter,
            confirmed: true,
        }));
        let batches: Vec<RecordBatch> = exec
            .execute(0, Arc::new(TaskContext::default()))
            .expect("execute")
            .map(|b| b.expect("batch"))
            .collect()
            .await;
        let read = rows(&batches);
        (exec, read)
    }

    /// Puts `obj` under `key` and returns its reference, its timestamps
    /// starting at `base`.
    async fn put_segment(
        store: &Arc<MemoryStore>,
        key: &str,
        obj: &[u8],
        seq: u64,
        base: i64,
    ) -> SegmentRef {
        store
            .put(key, bytes::Bytes::from(obj.to_vec()), PutOptions::default())
            .await
            .expect("put");
        let seg = seg_ref(key, obj, 1, seq);
        SegmentRef {
            min_event_ts_ns: base + seg.min_event_ts_ns,
            max_event_ts_ns: base + seg.max_event_ts_ns,
            ..seg
        }
    }

    /// A cache-wired scan with one partition under a confirmed ascending `ts`
    /// TopK builds the global order, as the per-partition deal did at one
    /// partition: two identical segments of three row groups keyed 0, 4 and
    /// 8 alternate in it, so its sole partition opens each segment once per
    /// row group and reads every surviving block.
    ///
    /// Fails against building no global order for one partition (the plan
    /// carries none, and each segment is one unit opened once).
    #[tokio::test]
    async fn topk_with_one_partition_follows_the_global_deal() {
        let store = Arc::new(MemoryStore::new());
        let mut segments = Vec::new();
        for seq in 1..=2u64 {
            let key = format!("t/sole{seq}.rlog");
            segments.push(put_segment(&store, &key, &object(seq, 1), seq, 0).await);
        }
        // The window starts after each segment's first block, so no segment is
        // contained in it and the planned route runs.
        let (exec, read) = sole_partition_topk_scan(&store, &segments, 1, BLOCKS as i64).await;
        assert_eq!(read, 2 * (BLOCKS - 1));
        let counts = exec.counts.get().expect("planned");
        assert!(counts.planned(0).is_some() && counts.planned(1).is_some());
        assert!(
            counts.global.is_some(),
            "one partition builds the global order"
        );
        assert_eq!(
            counts.units(&[], true, 1),
            (0..3)
                .flat_map(|pos| (0..2).map(move |seg| (seg, UnitBlocks::Groups(vec![pos]))))
                .collect::<Vec<_>>(),
            "the segments' row groups alternate"
        );
        assert_eq!(
            metric_total(&exec, "segments_opened"),
            6,
            "each segment opened once per row group"
        );
    }

    /// One partition, a read cache and a confirmed ascending `ts` TopK over
    /// two time-disjoint segments, the later one first in the snapshot: the
    /// global order holds each segment's row groups consecutively, so each
    /// segment is one unit, opened once, the earlier segment first.
    ///
    /// Fails against building no global order for one partition (the plan
    /// carries none) and against one unit per row group (six opens).
    #[tokio::test]
    async fn topk_one_partition_time_disjoint_segments_open_once() {
        let store = Arc::new(MemoryStore::new());
        let segments = vec![
            put_segment(&store, "t/late.rlog", &object_from(1, 1, 100), 1, 100).await,
            put_segment(&store, "t/early.rlog", &object(2, 1), 2, 0).await,
        ];
        // The window excludes the early segment's first block and the late
        // segment's last, so neither is contained in it and the planned route
        // runs.
        let (exec, read) = sole_partition_topk_scan(&store, &segments, 1, 110).await;
        assert_eq!(read, 2 * (BLOCKS - 1));
        let counts = exec.counts.get().expect("planned");
        assert!(counts.planned(0).is_some() && counts.planned(1).is_some());
        assert!(
            counts.global.is_some(),
            "one partition builds the global order"
        );
        assert_eq!(
            counts.units(&[], true, 1),
            vec![
                (1, UnitBlocks::Groups(vec![0, 1, 2])),
                (0, UnitBlocks::Groups(vec![0, 1, 2])),
            ],
            "each segment one unit, the earlier first"
        );
        assert_eq!(
            metric_total(&exec, "segments_opened"),
            2,
            "each segment opened once"
        );
    }
}
