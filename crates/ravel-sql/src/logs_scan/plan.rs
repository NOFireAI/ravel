use super::*;

/// The shared per-segment block plan (ADR-0102): for each segment, in snapshot
/// order, the count of blocks that survive this query's pruning and the
/// whole-segment [`ScanStats`] the prune produced, or `None` for a
/// ts-irrelevant segment (no GET, no blocks) and for an unplanned one. Computed
/// once and reused by every partition.
pub(super) struct PlanCounts {
    pub(super) segs: Vec<Option<SegPlan>>,
    /// Per segment, in snapshot order: relevant and at or under
    /// [`LogSegmentFetcher::plan_whole_object_bound`] in a scan that
    /// [`skips_sub_bound_plans`], so not planned (ADR-2677 decision 2). All
    /// false otherwise. Its owner opens it whole, once, and prunes its
    /// blocks from that read ([`open_segment_whole`]); its `segs` entry is
    /// `None`.
    unplanned: Vec<bool>,
    /// The partition count [`owned_work`] deals over: `target_partitions`,
    /// capped by the surviving block count plus one per unplanned segment
    /// (ADR-0102).
    /// [`compute_plan_counts`] counts each segment's owners against this same
    /// figure.
    pub(super) partitions: usize,
    /// Relevant segments the plan phase read whole instead of counting from the
    /// skip index (#761). A segment planned from footer alone or from the skip
    /// index carries its plan footer forward (`SegPlan::footer` is `Some`); the
    /// whole-object fallback carries none, so this is the count of relevant
    /// segments with no footer, published as the `plan_full_reads` metric (see
    /// [`BlockMetrics::plan_full_reads`] for the two causes).
    pub(super) full_reads: usize,
}

/// What the plan phase learned about a segment with at least one surviving
/// block (ADR-2414 decision A1).
struct PlannedBlocks {
    /// Whole-object block indices surviving this query's pruning, ascending:
    /// the set [`owned_work`] deals, whole row group by whole row group, into
    /// each partition's share, and the list every per-partition open's own
    /// pruning must reproduce.
    indices: Arc<Vec<usize>>,
    /// `indices` grouped into whole row groups ([`row_groups`]), computed
    /// here so the deal does not need the directories, which the segment's
    /// last owner may already have released.
    groups: Vec<Vec<usize>>,
    /// This segment's directories, one handle per owning partition.
    carried: CarriedDirsSlot,
}

impl PlannedBlocks {
    /// A segment's plan with no owners counted yet: until
    /// [`CarriedDirsSlot::set_owners`] runs, the directories stay held for as
    /// long as the plan counts live.
    fn new(
        indices: Vec<usize>,
        dirs: Arc<SegmentDirectories>,
        reservation: ravel_memory::Reservation,
    ) -> Self {
        let groups = row_groups(&indices, dirs.page_dir());
        PlannedBlocks {
            indices: Arc::new(indices),
            groups,
            carried: CarriedDirsSlot::new(CarriedDirs {
                dirs,
                _reservation: reservation,
            }),
        }
    }
}

/// A segment's directories, decoded once by the plan phase
/// ([`LogSegmentFetcher::plan_segment`]) and carried to every per-partition
/// subset open through [`OwnedSeg`] so the open reuses them via
/// `RlogReader::from_decoded` instead of decoding them again, together with
/// the fetch memory budget's reservation for them
/// ([`LogSegmentFetcher::reserve_carried_directories`]). Both are released
/// when the last handle drops.
pub(super) struct CarriedDirs {
    pub(super) dirs: Arc<SegmentDirectories>,
    _reservation: ravel_memory::Reservation,
}

/// Hands one [`CarriedDirs`] handle to each partition that owns a row group of
/// the segment, and keeps its own until the last of them has taken one. Each
/// partition drops its handle when it finishes the segment, so the reservation
/// is released once every owner has finished it rather than when the
/// statement ends.
struct CarriedDirsSlot {
    state: std::sync::Mutex<CarriedDirsState>,
}

struct CarriedDirsState {
    held: Option<Arc<CarriedDirs>>,
    /// Owners that have not taken their handle yet. `usize::MAX` until the
    /// deal counts them.
    takers_left: usize,
}

impl CarriedDirsSlot {
    fn new(carried: CarriedDirs) -> Self {
        CarriedDirsSlot {
            state: std::sync::Mutex::new(CarriedDirsState {
                held: Some(Arc::new(carried)),
                takers_left: usize::MAX,
            }),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, CarriedDirsState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// How many partitions own a row group of this segment.
    fn set_owners(&self, owners: usize) {
        let mut state = self.lock();
        state.takers_left = owners;
        if owners == 0 {
            state.held = None;
        }
    }

    /// One owner's handle. The slot lets its own go once every owner has
    /// taken one.
    fn take(&self) -> Option<Arc<CarriedDirs>> {
        let mut state = self.lock();
        let handle = state.held.clone();
        state.takers_left = state.takers_left.saturating_sub(1);
        if state.takers_left == 0 {
            state.held = None;
        }
        handle
    }
}

/// How many partitions [`owned_work`] gives a row group of a segment with
/// `groups` row groups when it deals over `partitions`: the segment's groups
/// are consecutive in the round-robin order, so they land on `min(groups,
/// partitions)` distinct partitions. Without striping one partition owns the
/// whole segment.
fn segment_owners(groups: usize, partitions: usize, stripe_blocks: bool) -> usize {
    if stripe_blocks {
        groups.min(partitions)
    } else {
        1
    }
}

/// One relevant segment's contribution to [`PlanCounts`].
pub(super) struct SegPlan {
    /// The surviving blocks and carried directories, or `None` when no block
    /// of this segment survives: no partition opens such a segment, so it
    /// keeps no directories resident and reserves nothing.
    planned: Option<PlannedBlocks>,
    /// The whole-segment prune stats ([`BlockMetrics::record_segment_totals`]
    /// consumes `blocks_total` and the postings drop). `blocks_scanned`/`pages`
    /// are zero here -- planning decodes nothing.
    pub(super) stats: ScanStats,
    /// The footer the plan fast path read for this segment (#693 part 3,
    /// deliverable 2), or `None` when the plan slow branch opened the scan
    /// instead. Carried to each per-partition subset open through [`OwnedSeg`] so
    /// the open reuses it and skips its own suffix probe.
    footer: Option<LogFooter>,
    /// The whole-object bytes the plan fallback branch already fetched for
    /// this segment (issue #835), when that branch resolved the entire
    /// object. Carried to the subset open through [`OwnedSeg`] so it does not
    /// pay a second wire GET for bytes the plan phase already holds. `None`
    /// on the fast/skip-decidable footer branches (no block read), on the
    /// fallback's ranged crossover (not the whole object), and when
    /// [`compute_plan_counts`]'s concurrency bound dropped an already-fetched
    /// whole object because `plan_concurrency` other segments' bytes were
    /// retained first -- that segment's subset open re-fetches instead.
    whole_object: Option<CarriedWholeObject>,
}

pub(super) type CountsFuture = Pin<Box<dyn Future<Output = DFResult<Arc<PlanCounts>>> + Send>>;

/// A future resolving the shared [`PlanCounts`] through `cell`: the first
/// partition to poll computes it (pruning every segment once), the rest await
/// that one computation. Errors are not cached, so a transient fetch failure
/// can be retried by a later poll.
pub(super) fn plan_counts_future(
    cell: Arc<OnceCell<Arc<PlanCounts>>>,
    ctx: Arc<PartitionCtx>,
    segments: Arc<Vec<SegmentRef>>,
    plan_concurrency: usize,
    stripe_blocks: bool,
    plan_init_elapsed: Time,
) -> CountsFuture {
    Box::pin(async move {
        let counts = cell
            .get_or_try_init(|| async {
                // Only the initializing partition runs this closure, so the
                // metric it records is the barrier's cost counted once.
                let started = Instant::now();
                let counts =
                    compute_plan_counts(&ctx, &segments, plan_concurrency, stripe_blocks).await;
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
/// that are relevant to the query window. Counted over the whole segment list
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
            !ctx.fetcher.plans_segment(seg)
                && LogSegmentFetcher::ts_range_relevant(
                    seg,
                    ctx.query.ts_min_ns,
                    ctx.query.ts_max_ns,
                )
        })
        .count();
    sub_bound >= target_partitions.max(1)
}

/// Prune every segment once (no block decode) to build the shared block plan.
///
/// The prunes run `plan_concurrency` at a time (`buffer_unordered`, consumed
/// as each completes rather than after the whole pass), and results are
/// written into a position-indexed `Vec` so `segs` keeps snapshot order and
/// [`owned_work`] can still index it by segment position even though
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
// skips this whole pass; see `owned_work` and `plan_segment_fast`.
async fn compute_plan_counts(
    ctx: &PartitionCtx,
    segments: &[SegmentRef],
    plan_concurrency: usize,
    stripe_blocks: bool,
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
        if skip_sub_bound && !ctx.fetcher.plans_segment(seg) {
            unplanned[idx] =
                LogSegmentFetcher::ts_range_relevant(seg, ctx.query.ts_min_ns, ctx.query.ts_max_ns);
            continue;
        }
        let prune = ctx.fetcher.plan_segment(
            seg,
            ctx.tenant_hash,
            &ctx.query,
            ctx.phase_accounting.plan(),
        );
        prunes.push(async move { (idx, prune.await) });
    }
    let budget = plan_concurrency.max(1);
    let mut stream = futures::stream::iter(prunes).buffer_unordered(budget);
    let mut segs: Vec<Option<SegPlan>> = std::iter::repeat_with(|| None)
        .take(segments.len())
        .collect();
    let mut total_blocks = 0usize;
    let mut full_reads = 0usize;
    // Only the RETAINED carried bytes -- the first `budget` segments to
    // complete with a whole object -- are summed here, so this is the true
    // peak `compute_plan_counts` holds at once, not the whole corpus. A
    // dropped whole object never reaches this sum: its `whole_object` is
    // `None` before `SegPlan` is built below.
    let mut carried_bytes = 0u64;
    let mut carried_seen = 0usize;
    while let Some((idx, entry)) = stream.next().await {
        let entry: PlanSegmentResult = entry.map_err(SqlError::from)?;
        if let Some((indices, dirs, stats, footer, whole_object)) = entry {
            let survivors = indices.len();
            total_blocks += survivors;
            // A relevant segment planned from the skip index carries its
            // footer forward; the whole-object fallback (#761) carries none.
            if footer.is_none() {
                full_reads += 1;
            }
            let whole_object = match whole_object {
                // `survivors > 0` mirrors the condition `owned_work` uses to
                // decide whether any partition ever opens this segment: a
                // zero-survivor segment is dropped there regardless, so
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
            // and only then are they reserved: `owned_work` skips a
            // zero-survivor segment, so carrying them would hold decoded
            // bytes nothing reuses.
            let planned = if survivors > 0 {
                let reservation = ctx
                    .fetcher
                    .reserve_carried_directories(&dirs)
                    .map_err(SqlError::from)?;
                Some(PlannedBlocks::new(indices, dirs, reservation))
            } else {
                None
            };
            segs[idx] = Some(SegPlan {
                planned,
                stats,
                footer,
                whole_object,
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
    // Cap the stride by the real block count (ADR-0102): with fewer blocks
    // than partitions the extra partitions get empty work. An unplanned
    // segment's survivors are unknown until it is opened, and it is dealt
    // whole, so it counts as one unit.
    let unplanned_count = unplanned.iter().filter(|&&u| u).count();
    let partitions = plan_concurrency
        .max(1)
        .min((total_blocks + unplanned_count).max(1));
    for planned in segs.iter().flatten().filter_map(|s| s.planned.as_ref()) {
        planned.carried.set_owners(segment_owners(
            planned.groups.len(),
            partitions,
            stripe_blocks,
        ));
    }
    Ok(Arc::new(PlanCounts {
        segs,
        unplanned,
        partitions,
        full_reads,
    }))
}

/// One segment this partition owns blocks in, and the block-index list (into
/// the segment's surviving-block list) it must drain.
pub(super) struct OwnedSeg {
    pub(super) seg: SegmentRef,
    /// This segment's position in the snapshot's segment list, i.e. the
    /// segment field of every row-ref built from it (ADR-0774).
    pub(super) ordinal: usize,
    pub(super) indices: Vec<usize>,
    /// The segment's whole surviving-block list this query's plan produced
    /// (ascending whole-object block indices), of which `indices` is this
    /// partition's share. A row-ref addresses a block by its position in this
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
    /// carried to the subset open so it does not pay a second wire GET.
    /// `None` whenever [`SegPlan::whole_object`] was `None`, and always
    /// `None` on the whole-segment fast path (no plan phase).
    pub(super) whole_object: Option<CarriedWholeObject>,
    /// This partition's handle on the segment's directories, decoded once by
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
/// [`owned_work`] can deal a row group's blocks to one partition as a unit
/// (ADR-2414 decision A1) instead of splitting it across partitions. Indices
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

/// This partition's share of the block assignment, in one of two modes
/// (ADR-0102, amended by #693), both with `n = target_partitions.max(1).
/// min(total.max(1))`.
///
/// - **`stripe_blocks` true** (a read cache is wired): the flattened
///   row-group assignment (ADR-2414 decision A1). Each segment's surviving
///   blocks are grouped into whole row groups by [`row_groups`], and group
///   `i` in the segment-then-group order over all surviving row groups goes
///   to partition `i % n` -- every block of a row group to the same
///   partition, never split. A row group is the unit the format stores
///   column-major, and a column chunk's dictionary page is shared by every
///   block of the group, so splitting a group across partitions makes each of
///   them decode that dictionary again. Dealing round-robin over groups keeps
///   the assignment deterministic, and the number of groups per partition
///   differs by at most one; block counts are as even as the groups are, and
///   a segment's last group may be short. A segment with fewer groups than
///   partitions leaves some of them without a share of it.
/// - **`stripe_blocks` false** (un-cached): the segment-granular assignment.
///   Counting only segments with a surviving block, in snapshot order, segment
///   `j` goes to partition `j % n` and that partition drains all of the
///   segment's surviving block indices. Each segment is opened
///   by exactly one partition, so with nothing to coalesce re-opens the scan
///   still costs one read per segment rather than one per partition per segment.
///
/// Segments are visited in `order` (see [`visit_order`]) rather than in
/// snapshot order when they differ; each [`OwnedSeg::ordinal`] stays the
/// segment's snapshot index either way.
///
/// An unplanned segment ([`PlanCounts::unplanned`]) is one unit in either
/// mode, a row group or a segment in the same round-robin, and goes whole to
/// the partition that unit lands on.
///
/// Returns, per owned segment, the surviving whole-object block indices this
/// partition drains, in ascending order (empty for an unplanned segment).
pub(super) fn owned_work(
    counts: &PlanCounts,
    segments: &[SegmentRef],
    order: &[usize],
    partition: usize,
    n: usize,
    stripe_blocks: bool,
) -> VecDeque<OwnedSeg> {
    let mut work = VecDeque::new();
    let planned_segs = order
        .iter()
        .filter_map(|&seg_idx| counts.segs.get(seg_idx).map(|plan| (seg_idx, plan)));
    let unplanned = |seg_idx: usize| OwnedSeg {
        seg: segments[seg_idx].clone(),
        ordinal: seg_idx,
        indices: Vec::new(),
        survivors: None,
        footer: None,
        whole_object: None,
        dirs: None,
        unplanned: true,
    };
    let is_unplanned = |seg_idx: usize| counts.unplanned.get(seg_idx).copied().unwrap_or(false);
    if stripe_blocks {
        let mut global_group = 0usize;
        for (seg_idx, plan) in planned_segs {
            // An unplanned segment is one unit in the same round-robin.
            if is_unplanned(seg_idx) {
                if global_group % n == partition {
                    work.push_back(unplanned(seg_idx));
                }
                global_group += 1;
                continue;
            }
            let Some(plan) = plan else { continue };
            let Some(planned) = &plan.planned else {
                continue;
            };
            let mut indices = Vec::new();
            for group in &planned.groups {
                if global_group % n == partition {
                    indices.extend_from_slice(group);
                }
                global_group += 1;
            }
            if !indices.is_empty() {
                work.push_back(OwnedSeg {
                    seg: segments[seg_idx].clone(),
                    ordinal: seg_idx,
                    indices,
                    survivors: Some(Arc::clone(&planned.indices)),
                    footer: plan.footer.clone(),
                    whole_object: plan.whole_object.clone(),
                    dirs: planned.carried.take(),
                    unplanned: false,
                });
            }
        }
    } else {
        let mut seg_ordinal = 0usize;
        for (seg_idx, plan) in planned_segs {
            if is_unplanned(seg_idx) {
                if seg_ordinal % n == partition {
                    work.push_back(unplanned(seg_idx));
                }
                seg_ordinal += 1;
                continue;
            }
            let Some(plan) = plan else { continue };
            let Some(planned) = &plan.planned else {
                continue;
            };
            if seg_ordinal % n == partition {
                work.push_back(OwnedSeg {
                    seg: segments[seg_idx].clone(),
                    ordinal: seg_idx,
                    indices: planned.indices.to_vec(),
                    survivors: Some(Arc::clone(&planned.indices)),
                    footer: plan.footer.clone(),
                    whole_object: plan.whole_object.clone(),
                    dirs: planned.carried.take(),
                    unplanned: false,
                });
            }
            seg_ordinal += 1;
        }
    }
    work
}

/// The predicate-free full-window whole-segment assignment (#693 part 3,
/// deliverable 1): the same round-robin [`owned_work`]'s un-cached branch uses,
/// but computed with NO plan phase at all. Relevant (ts-overlapping) segments in
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
pub(super) fn visit_order(segments: &[SegmentRef], by_min_ts: bool) -> Vec<usize> {
    let mut order: Vec<usize> = (0..segments.len()).collect();
    if by_min_ts {
        order.sort_by_key(|&i| (segments[i].min_event_ts_ns, i));
    }
    order
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod owned_work_tests {
    //! ADR-2414 decision A1: `owned_work` deals whole row groups, never a
    //! row group's blocks across partitions. Fixtures are writer-produced
    //! objects of one block per record, `GROUP_BLOCKS` blocks per row group.

    use super::*;
    use ravel_catalog::SegmentLevel;
    use ravel_logseg::record::stream_attrs_bytes;
    use ravel_logseg::{ObjectIdentity, RlogConfig, RlogReader, RlogWriter};
    use ravel_types::logstream::log_stream_id;
    use uuid::Uuid;

    const GROUP_BLOCKS: usize = 4;
    const GROUPS: usize = 3;
    const BLOCKS: usize = GROUPS * GROUP_BLOCKS;

    fn object() -> Vec<u8> {
        let cfg = RlogConfig {
            block_target_records: 1,
            group_target_blocks: GROUP_BLOCKS,
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
        let obj = object();
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

    /// `segments` identical objects, each with every block surviving.
    fn plan(segments: usize) -> (PlanCounts, Vec<SegmentRef>) {
        let dirs = dirs();
        let budget = Arc::new(ravel_memory::MemoryBudget::unlimited());
        let segs = (0..segments)
            .map(|_| {
                Some(SegPlan {
                    planned: Some(PlannedBlocks::new(
                        (0..BLOCKS).collect(),
                        Arc::clone(&dirs),
                        budget.reserve(0).expect("reserve"),
                    )),
                    stats: ScanStats::default(),
                    footer: None,
                    whole_object: None,
                })
            })
            .collect();
        let refs = (0..segments).map(|s| seg_ref(&format!("seg{s}"))).collect();
        (
            PlanCounts {
                segs,
                unplanned: vec![false; segments],
                partitions: segments * BLOCKS,
                full_reads: 0,
            },
            refs,
        )
    }

    /// Every partition's owned `(segment ordinal, blocks)` list.
    fn deal(
        counts: &PlanCounts,
        refs: &[SegmentRef],
        n: usize,
        stripe: bool,
    ) -> Vec<Vec<(usize, Vec<usize>)>> {
        (0..n)
            .map(|p| {
                let order = visit_order(refs, false);
                owned_work(counts, refs, &order, p, n, stripe)
                    .into_iter()
                    .map(|w| (w.ordinal, w.indices))
                    .collect()
            })
            .collect()
    }

    /// One object with more blocks (12) than partitions (3): every row group
    /// lands whole on one partition, every partition owns exactly one group,
    /// and every block is owned exactly once.
    ///
    /// Fails against the per-block deal (`unit i -> partition i % n`), which
    /// puts blocks 0, 3, 6, 9 on partition 0 and so splits every group, and
    /// against a deal that hands every group to partition 0 (partitions 1 and 2
    /// would own nothing).
    #[test]
    fn a_row_group_stays_on_one_partition() {
        let (counts, refs) = plan(1);
        let dealt = deal(&counts, &refs, 3, true);
        assert_eq!(
            dealt,
            vec![
                vec![(0, vec![0, 1, 2, 3])],
                vec![(0, vec![4, 5, 6, 7])],
                vec![(0, vec![8, 9, 10, 11])],
            ],
            "each partition owns exactly one whole row group"
        );
        assert_eq!(dealt, deal(&counts, &refs, 3, true), "the deal is stable");
    }

    /// Group numbering runs on across segments, not from zero per segment:
    /// two objects of three groups over four partitions deal groups
    /// 0..6 to partitions 0, 1, 2, 3, 0, 1, so partition 0 owns segment 0's
    /// first group and segment 1's second, and group counts differ by at most
    /// one.
    ///
    /// Fails against a deal that restarts the group count per segment (segment
    /// 1 would again start at partition 0, leaving partition 3 with one group
    /// and partition 0 with two of segment 1's).
    #[test]
    fn group_numbering_runs_on_across_segments() {
        let (counts, refs) = plan(2);
        let dealt = deal(&counts, &refs, 4, true);
        assert_eq!(
            dealt,
            vec![
                vec![(0, vec![0, 1, 2, 3]), (1, vec![4, 5, 6, 7])],
                vec![(0, vec![4, 5, 6, 7]), (1, vec![8, 9, 10, 11])],
                vec![(0, vec![8, 9, 10, 11])],
                vec![(1, vec![0, 1, 2, 3])],
            ]
        );
        let groups: Vec<usize> = dealt
            .iter()
            .map(|p| p.iter().map(|(_, b)| b.len() / GROUP_BLOCKS).sum())
            .collect();
        let (lo, hi) = (
            groups.iter().min().expect("partitions"),
            groups.iter().max().expect("partitions"),
        );
        assert!(hi - lo <= 1, "group counts are balanced: {groups:?}");
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

    /// Without a read cache the assignment stays segment-granular: segment `j`
    /// goes whole to partition `j % n`.
    #[test]
    fn the_uncached_deal_is_still_per_segment() {
        let (counts, refs) = plan(3);
        let dealt = deal(&counts, &refs, 2, false);
        let all: Vec<usize> = (0..BLOCKS).collect();
        assert_eq!(
            dealt,
            vec![vec![(0, all.clone()), (2, all.clone())], vec![(1, all)]]
        );
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
            let ts = ts_step * i;
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
        let counts = compute_plan_counts(&ctx, &segments, 4, true)
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
        let exact = compute_plan_counts(&with_limit(total), &segments, 1, true).await;
        assert!(
            exact.is_ok(),
            "the directories fit a budget of exactly their size: {:?}",
            exact.as_ref().err()
        );
        drop(exact);
        let err = compute_plan_counts(&with_limit(total - 1), &segments, 1, true)
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
            segs: vec![Some(SegPlan {
                planned: Some(PlannedBlocks::new(
                    plan_indices,
                    dirs,
                    budget.reserve(0).expect("reserve"),
                )),
                stats: ScanStats::default(),
                footer: None,
                whole_object: None,
            })],
            unplanned: vec![false],
            partitions: 1,
            full_reads: 1,
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
    /// both partitions own a group of every segment. A segment's directories
    /// stay reserved while either owner is still on it and are released once
    /// both have finished it, not when the statement ends.
    ///
    /// Partition 0 drains first: every segment is still reserved, because
    /// partition 1 has not finished any. Partition 1 is then held on its first
    /// GET of segment 0, and segment 0 is still reserved. Released, it finishes
    /// segment 0, including the `attrs_raw` row-path reopens of it that come
    /// after partition 0 finished it, and is held again on segment 1: segment
    /// 0's directories are gone from the budget and the other two are not. At
    /// the end nothing is reserved while the exec is still alive.
    ///
    /// Fails against releasing on the first owner's finish (segment 0 is gone
    /// while partition 1 is still on it) and against never releasing (segment
    /// 0 is still reserved once both owners have finished it).
    #[tokio::test]
    async fn carried_directories_are_released_when_every_owner_finishes_the_segment() {
        use futures::StreamExt;
        use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op};

        let store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
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
        let first = exec.execute(0, Arc::clone(&task_ctx)).expect("execute");
        let second = exec.execute(1, task_ctx).expect("execute");

        let drained: Vec<RecordBatch> =
            first.map(|b| b.expect("partition 0 batch")).collect().await;
        // Groups 0, 2, 4, 6, 8 of nine: two groups of segments 0 and 2 (the
        // first group is short of block 0) and one of segment 1.
        assert_eq!(rows(&drained), 18);
        assert!(
            budget.reserved() >= all && budget.reserved() < all + slack,
            "partition 1 has finished no segment, so all three are reserved: {} of {all}",
            budget.reserved()
        );

        let gate = store.hold(Op::Get, Some("t/held".to_string()), Occurrence::Always);
        let task = tokio::spawn(async move {
            second
                .map(|b| b.expect("partition 1 batch"))
                .collect::<Vec<_>>()
                .await
        });
        gate.wait_until_held(1).await;
        let on = |key: &str| gate.held_details().iter().all(|(_, _, k)| k.contains(key));
        assert!(on("held0"), "partition 1 opens segment 0 first");
        assert!(
            budget.reserved() >= all && budget.reserved() < all + slack,
            "partition 1 is still on segment 0: {} of {all}",
            budget.reserved()
        );

        // Let partition 1 through segment 0 until it waits on segment 1.
        loop {
            gate.wait_until_held(1).await;
            if on("held1") {
                break;
            }
            for (id, _, key) in gate.held_details() {
                if key.contains("held0") {
                    gate.release(id);
                }
            }
        }
        let rest = all - dirs[0];
        assert!(
            budget.reserved() >= rest && budget.reserved() < rest + slack,
            "both owners finished segment 0, so only segments 1 and 2 stay reserved: {} of {rest}",
            budget.reserved()
        );

        let releaser = {
            let gate = gate.clone();
            tokio::spawn(async move {
                loop {
                    gate.wait_until_held(1).await;
                    for (id, _, _) in gate.held_details() {
                        gate.release(id);
                    }
                }
            })
        };
        let second_rows = task.await.expect("partition 1 task");
        releaser.abort();
        assert_eq!(rows(&second_rows), 3 * (BLOCKS - 1) - 18);
        // The read cache keeps what it admitted, with that read's reservation,
        // until it drops with the exec.
        assert!(
            budget.reserved() < slack,
            "every segment's directories released while the exec is alive: {}",
            budget.reserved()
        );
        assert_eq!(
            metric_total(&exec, "reopens"),
            12,
            "two `attrs_raw` reopens per partition per segment"
        );
        drop(exec);
        assert_eq!(budget.reserved(), 0);
    }
}
