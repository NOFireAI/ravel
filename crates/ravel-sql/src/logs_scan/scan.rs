use super::*;

/// Everything one partition's fetches need, shared by every per-segment open
/// future so each can be `'static` without cloning the query per segment.
pub(super) struct PartitionCtx {
    pub(super) fetcher: LogSegmentFetcher,
    pub(super) tenant_hash: TenantHash,
    pub(super) query: LogQuery,
    pub(super) columns: ColumnSelection,
    /// [`LogsScanExec::projected_fraction`], carried so the whole-segment fast
    /// path can route each segment as it opens it (issue #862).
    pub(super) projected_fraction: f64,
    pub(super) phase_accounting: PhaseAccounting,
}

impl PartitionCtx {
    /// Whether the whole-segment fast path should open `seg` by column chunk
    /// rather than with one whole-object GET (issue #862).
    ///
    /// The fast path's own conjuncts ([`LogsScanExec::whole_segment_fast_path`])
    /// prove every block of `seg` survives. That does not make the whole-object
    /// read optimal: every block surviving says nothing about how many COLUMNS
    /// the projection wants, and the ranged entry point ([`open_segment_ranged`])
    /// already fetches one coalesced range per surviving `(row group, projected
    /// column)` from the very same [`ColumnSelection`] (ADR-0699 decision 5), so
    /// a narrow projection can leave most of the object unread.
    ///
    /// The arbiter is the fetch layer's request-cost model, not a threshold
    /// invented here: the ranged path pays only when the bytes the projection
    /// skips outweigh the round trips the protocol adds. That keeps a wide
    /// projection (`SELECT *`, or any reference to the merged `attrs` map) on
    /// the unchanged whole-object read, where it belongs -- the ranged path
    /// would fetch the same bytes and pay a probe on top.
    ///
    /// The route is unconditional in the format version (ADR-0892 decision 3):
    /// every readable RLOG object stores pages column-major with a PAGE_DIR, so
    /// the `ColumnSelection` always selects which chunks are FETCHED. An object
    /// whose declared version this build cannot read never reaches here at all;
    /// [`refuse_unreadable_version`] turns it away before any route is chosen.
    fn open_by_column_chunk(&self, seg: &SegmentRef) -> bool {
        self.fetcher
            .ranged_projection_pays(seg.object_size, self.projected_fraction)
    }

    /// Record one whole-segment fast-path open on the route
    /// [`open_by_column_chunk`](Self::open_by_column_chunk) just chose, on this
    /// query's accounting handle (ADR-0904 decision 5).
    ///
    /// The plan metrics carry the same split per partition
    /// ([`BlockMetrics::record_fast_path_route`]), but only for a statement whose
    /// `EXPLAIN ANALYZE` output someone reads. The accounting handle is the
    /// per-query figure a caller can assert on, which is what makes the
    /// request-cost knob's effect on the route provable rather than inferred
    /// from the configured value.
    ///
    /// Called once per segment, beside the metric and under the same rule: an
    /// `attrs_raw` fallback re-opens the same segment the same way and does not
    /// re-count, so the two counters sum to the fast-path segment count.
    fn record_open_shape(&self, by_column_chunk: bool) {
        if by_column_chunk {
            self.phase_accounting.scan().add_logs_ranged_opens(1);
        } else {
            self.phase_accounting.scan().add_logs_whole_object_opens(1);
        }
    }

    /// Record this query's one touch of the data object the caller is about to
    /// open, for the whole-segment fast path (ADR-0996 decision 3, issue
    /// #1006).
    ///
    /// # Why this site is the fast path's single recorder
    ///
    /// The counter is the denominator of `range_amplification =
    /// data_GET_requests / data_objects_touched`, so it must count each data
    /// object once per query however many GETs are issued against it. That
    /// makes it a designated-recorder question rather than a
    /// record-where-you-fetch one: the striped path splits one segment's blocks
    /// across partitions, so a per-partition site would multiply the count by
    /// the partition count, the same way per-partition
    /// [`BlockMetrics::record_segment_totals`] would (ADR-0102's
    /// partition-0-at-planning rule).
    ///
    /// Here the owning partition is the recorder. [`owned_whole_segments`]
    /// assigns relevant segment `j` whole to partition `j % n`, so the caller
    /// below runs exactly once per relevant segment across the whole query, not
    /// once per partition. Both routes it guards fetch blocks bytes (whole
    /// object or coalesced column-chunk ranges), so the object is touched in the
    /// counter's sense and not merely probed; and both are reached only after
    /// [`LogsScanExec::whole_segment_fast_path`] proved the segment relevant on
    /// the same ts bounds the fetch re-checks, so neither can decline the fetch
    /// underneath a recorded touch. An `attrs_raw` fallback re-opens the same
    /// segment without coming back through here, which is what keeps several
    /// opens of one object at one touch, exactly as it does for
    /// [`record_open_shape`](Self::record_open_shape).
    ///
    /// # Route exclusivity: no object can be recorded twice
    ///
    /// The planned route records the same counter in the fetch layer, at
    /// `ravel_query::LogSegmentFetcher::plan_segment`, and no statement reaches
    /// both recorders. [`LogsScanExec::execute`] decides from
    /// [`LogsScanExec::whole_segment_fast_path`]: on `Ok` the partition drains
    /// whole segments through the caller of this method and never builds a
    /// [`plan_counts_future`], and on `Err` it plans and never sets
    /// `fast_whole_segment`, so this method is unreachable. That decision reads
    /// only the resolved snapshot and the query, both fixed for the statement
    /// and neither dependent on the partition, so every partition of one
    /// statement takes the same route and exactly one of the two recorders can
    /// fire for a given object.
    ///
    /// On the planned route a segment [`may_skip_plan`] accepts in a scan
    /// that [`skips_sub_bound_plans`] is not planned (ADR-2677 decision 2):
    /// [`compute_plan_counts`] never calls `plan_segment` for it,
    /// so the fetch-layer recorder cannot fire, and [`owned_work`] gives it to
    /// exactly one partition, whose open records the touch here instead.
    fn record_data_object_touched(&self) {
        self.phase_accounting.scan().add_data_objects_touched(1);
    }
}

/// Refuse a segment whose declared RLOG format version this build cannot read,
/// BEFORE any request is issued for it (ADR-0892 decision 4).
///
/// Every open path below calls this first. Without it an unreadable object is
/// found out by `ravel_logseg::footer::open_from_suffix`, which runs on bytes
/// the ranged path has already paid a suffix probe (and possibly a footer-range
/// GET) to fetch, or that the whole-object path has fetched entirely. The
/// snapshot already carries the version in
/// [`SegmentRef::segment_format_version`], so the round trips buy nothing.
///
/// This is a pre-filter, not the gate: the object's own trailer is still
/// checked on open, so a catalog entry that disagrees with the bytes is caught
/// there. The error it produces is the one that check produces for an object
/// whose bytes match their recorded content hash (ADR-2708 D5a). No content
/// hash is read here: the version is the commit record's, not a trailer byte
/// that could have been damaged.
pub(super) fn refuse_unreadable_version(seg: &SegmentRef) -> Result<(), SqlError> {
    // The catalog carries the version as u32 and an RLOG trailer is u16, so a
    // value that does not fit is one no build could read; it is refused and
    // reported as recorded, never truncated into the supported window.
    let declared = seg.segment_format_version;
    if u16::try_from(declared).is_ok_and(|v| ravel_logseg::footer::SUPPORTED_VERSIONS.contains(v)) {
        return Ok(());
    }
    Err(SqlError::LogFetch(LogFetchError::UnsupportedVersion {
        key: seg.data_object_key.clone(),
        version: declared,
    }))
}

type OpenFuture = Pin<Box<dyn Future<Output = DFResult<Option<LogSegmentScan>>> + Send>>;

/// Fetch one segment's bytes and open its pruned, column-projected scan
/// restricted to the whole-object block indices in `indices` (ADR-2414
/// decision A1: `owned_work` deals whole row groups, so these are raw block
/// indices, not ordinal survivor positions).
/// `Ok(None)` means the catalog summary proved the segment irrelevant, with no
/// GET issued -- which cannot happen for a segment that was already counted
/// with survivors, but is handled as end-of-segment rather than panicking.
///
/// `survivors` is the segment's whole surviving-block list the plan phase
/// produced ([`OwnedSeg::survivors`]), of which `indices` is this partition's
/// share. Every caller of this function reaches it only through `owned_work`
/// (never `owned_whole_segments`, which never calls this function), so
/// `survivors` is architecturally always `Some`; `None` is refused with a
/// typed error rather than silently scanning without the cross-check (issue
/// #2417), since a caller that could reach this function without it is a bug
/// in this module, not a runtime condition to tolerate.
fn open_segment_subset(
    ctx: Arc<PartitionCtx>,
    seg: SegmentRef,
    indices: Vec<usize>,
    survivors: Option<Arc<Vec<usize>>>,
    footer: Option<LogFooter>,
    whole_object: Option<CarriedWholeObject>,
    dirs: Option<Arc<SegmentDirectories>>,
) -> OpenFuture {
    Box::pin(async move {
        refuse_unreadable_version(&seg)?;
        let survivors = survivors.ok_or_else(|| {
            SqlError::Internal("striped segment open with no plan-phase survivor list".into())
        })?;
        let scan = ctx
            .fetcher
            .scan_accounted_with_tenant_subset_raw(
                &seg,
                ctx.tenant_hash,
                &ctx.query,
                &ctx.columns,
                &indices,
                &survivors,
                footer.as_ref(),
                whole_object,
                dirs.as_ref(),
                ctx.phase_accounting.scan(),
            )
            .await
            .map_err(SqlError::from)?;
        Ok(scan)
    })
}

/// Fetch one whole segment's object in a single GET and open its pruned,
/// column-projected scan over all of its blocks (#693 part 3, deliverable 1).
/// Used on the predicate-free full-window whole-segment path, where the
/// segment is assigned to exactly one partition and every block survives, so a
/// whole-object read is optimal for a projection wide enough to want most of
/// the object's bytes ([`PartitionCtx::open_by_column_chunk`]) and no plan phase
/// or suffix probe is needed. Also used for a segment at or under the plan
/// bound (ADR-2677 decision 2), which has one owner and no plan: the open
/// prunes blocks from the directory the whole read holds, and the caller takes
/// the survivor list from [`LogSegmentScan::survivor_block_indices`].
fn open_segment_whole(ctx: Arc<PartitionCtx>, seg: SegmentRef) -> OpenFuture {
    Box::pin(async move {
        refuse_unreadable_version(&seg)?;
        let scan = ctx
            .fetcher
            .scan_whole_accounted_with_tenant(
                &seg,
                ctx.tenant_hash,
                &ctx.query,
                &ctx.columns,
                ctx.phase_accounting.scan(),
            )
            .await
            .map_err(SqlError::from)?;
        Ok(scan)
    })
}

/// The whole-segment fast path's other entry point (issue #862): open the same
/// whole segment through the probe-and-range protocol, which brings one
/// coalesced range per surviving `(row group, projected column)` instead of
/// every byte of the object.
///
/// Taken when [`PartitionCtx::open_by_column_chunk`] judges the skipped bytes
/// worth the extra round trips. It passes the SAME [`ColumnSelection`] the
/// decode uses, which is what ADR-0699 decision 5 requires of a version-4 fetch,
/// and no block-index subset: the fast path's conjuncts already proved every
/// block of this segment survives, so the ranged read's candidate set is the
/// whole segment and the rows it yields are the rows
/// [`open_segment_whole`] would have yielded.
fn open_segment_ranged(ctx: Arc<PartitionCtx>, seg: SegmentRef) -> OpenFuture {
    Box::pin(async move {
        refuse_unreadable_version(&seg)?;
        let scan = ctx
            .fetcher
            .scan_accounted_with_tenant(
                &seg,
                ctx.tenant_hash,
                &ctx.query,
                &ctx.columns,
                ctx.phase_accounting.scan(),
            )
            .await
            .map_err(SqlError::from)?;
        Ok(scan)
    })
}

/// One segment's open on the whole-segment fast path, routed by
/// [`PartitionCtx::open_by_column_chunk`].
fn open_segment_fast(ctx: Arc<PartitionCtx>, seg: SegmentRef, by_column_chunk: bool) -> OpenFuture {
    if by_column_chunk {
        open_segment_ranged(ctx, seg)
    } else {
        open_segment_whole(ctx, seg)
    }
}

/// How many of one partition's ranged fast-path segment opens may be held at
/// once, counting the segment being drained (ADR-2414 decision A2): the
/// partition's share of the process's GET permits, `store_get_concurrency /
/// partitions`, and at least 2 so the next segment's round trips always
/// overlap the current one's decode.
pub(super) fn fast_path_prefetch_share(store_get_concurrency: usize, partitions: usize) -> usize {
    (store_get_concurrency / partitions.max(1)).max(2)
}

/// Whether `e` is the fetcher's memory budget refusing a reservation
/// ([`LogFetchError::FetchMemoryExhausted`]), as an open surfaces it.
fn is_fetch_memory_refusal(e: &DataFusionError) -> bool {
    matches!(
        e,
        DataFusionError::External(inner)
            if matches!(
                inner.downcast_ref::<SqlError>(),
                Some(SqlError::LogFetch(LogFetchError::FetchMemoryExhausted { .. }))
            )
    )
}

/// A ranged fast-path open issued ahead of its partition's turn to drain
/// its segment (ADR-2414 decision A2), held in that partition's
/// [`PrefetchSlot`]. It is the same [`open_segment_fast`] future the
/// sequential open would build, polled from
/// [`LogScanStream::drive_prefetches`] until it resolves; the resolved open
/// is kept, error included, until the segment's turn comes, so an error is
/// reported for the segment that produced it and only once the segments
/// before it have been emitted. Its fetch reservations are owned by the
/// future or by the bytes it resolved to, so dropping it, from whichever
/// partition's task, releases them at once.
struct Prefetch {
    /// The owned work item the open was issued for, kept whole so a
    /// revocation can hand it back to its owner's work.
    owned: OwnedSeg,
    /// When the open was issued: the start of this segment's `open_elapsed`.
    issued: Instant,
    open: PrefetchOpen,
}

enum PrefetchOpen {
    InFlight(OpenFuture),
    Ready {
        opened: Box<DFResult<Option<LogSegmentScan>>>,
        at: Instant,
    },
}

/// One partition's share of a [`PrefetchPool`].
#[derive(Default)]
struct PrefetchSlot {
    /// The issued opens of the owner's segments after its current one, in
    /// owned order.
    prefetched: VecDeque<Prefetch>,
    /// Segments whose prefetch a revocation dropped, in owned order, for the
    /// owner to move back to the front of its work.
    handed_back: VecDeque<OwnedSeg>,
}

/// One statement's prefetched fast-path opens, a slot per partition
/// (ADR-2414 decision A2). The fetch memory budget is process-wide, so a
/// partition whose current open is refused may be refused for bytes another
/// partition's prefetches hold; [`Self::revoke_all`] lets it drop them all
/// synchronously, without waiting for any other stream to be polled.
///
/// A slot's lock is never held across an await, and no code path holds two
/// slot locks at once.
pub(super) struct PrefetchPool {
    /// Set by the first revocation: no partition of the statement issues
    /// another prefetch.
    off: AtomicBool,
    slots: Vec<std::sync::Mutex<PrefetchSlot>>,
    /// Whether each partition's stream has been built, so a second `execute`
    /// of one partition cannot share a slot with the first.
    registered: Vec<AtomicBool>,
}

impl PrefetchPool {
    pub(super) fn new(partitions: usize) -> Self {
        PrefetchPool {
            off: AtomicBool::new(false),
            slots: (0..partitions)
                .map(|_| std::sync::Mutex::new(PrefetchSlot::default()))
                .collect(),
            registered: (0..partitions).map(|_| AtomicBool::new(false)).collect(),
        }
    }

    fn is_off(&self) -> bool {
        self.off.load(Ordering::SeqCst)
    }

    /// Claims `partition`'s slot for the stream `execute` is building.
    pub(super) fn register(&self, partition: usize) -> DFResult<()> {
        let flag = self.registered.get(partition).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "logs scan partition {partition} has no prefetch slot"
            ))
        })?;
        if flag.swap(true, Ordering::SeqCst) {
            return Err(DataFusionError::Internal(format!(
                "logs scan partition {partition} executed twice"
            )));
        }
        Ok(())
    }

    fn slot(&self, partition: usize) -> DFResult<std::sync::MutexGuard<'_, PrefetchSlot>> {
        self.slots
            .get(partition)
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "logs scan partition {partition} has no prefetch slot"
                ))
            })?
            .lock()
            .map_err(|_| {
                DataFusionError::Internal(format!(
                    "logs scan partition {partition}'s prefetch slot lock is poisoned"
                ))
            })
    }

    /// Turns the statement's pipeline off, then drops every unconsumed
    /// prefetch of every partition, one slot at a time in index order:
    /// each is popped from the back and its segment pushed on the front of
    /// that slot's `handed_back`, so owned order is kept. Dropping a
    /// prefetch releases its fetch reservations before this returns.
    /// Returns how many were dropped.
    fn revoke_all(&self) -> DFResult<usize> {
        self.off.store(true, Ordering::SeqCst);
        let mut dropped = 0;
        for partition in 0..self.slots.len() {
            let mut slot = self.slot(partition)?;
            while let Some(Prefetch { owned, open, .. }) = slot.prefetched.pop_back() {
                drop(open);
                slot.handed_back.push_front(owned);
                dropped += 1;
            }
        }
        Ok(dropped)
    }

    /// Drops everything `partition`'s slot holds. Used on the stream's way
    /// out, so it clears a poisoned slot too rather than leaving its
    /// prefetches' bytes reserved.
    fn clear(&self, partition: usize) {
        if let Some(slot) = self.slots.get(partition) {
            let mut slot = slot
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            slot.prefetched.clear();
            slot.handed_back.clear();
        }
    }

    /// Gives `partition`'s slot back when its stream is dropped, so a later
    /// `execute` of the same plan instance (a parent that runs its input
    /// once per iteration) claims it again. Two live streams on one slot
    /// stay refused by [`Self::register`].
    fn unregister(&self, partition: usize) {
        if let Some(flag) = self.registered.get(partition) {
            flag.store(false, Ordering::SeqCst);
        }
    }
}

/// Which open [`LogScanStream::on_open_refused`] retries.
enum Reopen {
    /// The segment's fast-path open, on the route its first open took.
    Fast,
    /// The `attrs_raw` row-path reopen, skipping the `skip` blocks of this
    /// partition's list already emitted.
    Rows { skip: usize },
}

/// Consecutive `attrs_raw`-overflow fallbacks (see
/// [`LogScanStream::consecutive_fallbacks`]) tolerated within one segment
/// before the rest of this partition's block list is committed to the row
/// path in one reopen, rather than reopening again to retry columnar. `1`:
/// the first fallback still gets a columnar retry (the sparse case, where a
/// single overflowing block among clean ones is common and worth resuming
/// columnar for), but a second fallback immediately after it -- with no
/// clean columnar block in between -- escalates. This bounds a segment to at
/// most `MAX_CONSECUTIVE_ATTR_RAW_FALLBACKS + 1` reopens regardless of how
/// many of its blocks overflow.
const MAX_CONSECUTIVE_ATTR_RAW_FALLBACKS: usize = 1;

pub(super) enum LogScanState {
    /// Awaiting the shared per-segment block plan (ADR-0102). Once it resolves,
    /// this partition's owned `(segment, block-index-list)` work is computed and
    /// the stream advances to draining.
    Planning(CountsFuture),
    /// Advance to the next owned segment of this partition, or finish.
    NextSegment,
    /// Awaiting one owned segment's GET and prune (restricted to this
    /// partition's block-index list).
    Opening(OpenFuture),
    /// Draining one segment's surviving blocks through the columnar fast path
    /// (ADR-0099 decision 2). Entered when the scan is statically eligible, and
    /// re-entered (issue #1769) once [`LogScanState::RowFallbackBlock`] has
    /// taken the one block that carried an `attrs_raw` overflow page, so only
    /// that block runs the row path instead of the rest of the segment.
    Columnar(Box<LogSegmentScan>),
    /// Draining one segment's surviving blocks through the row path
    /// ([`LogSegmentScan::next_block`]), rebuilding a [`LogRecord`] per row: the
    /// unchanged pre-ADR-0099 path, taken by a scan that is statically
    /// ineligible for the columnar fast path, OR handed the
    /// still-open scan by [`LogScanState::RowFallbackBlock`] once a segment
    /// has hit [`MAX_CONSECUTIVE_ATTR_RAW_FALLBACKS`] consecutive `attrs_raw`
    /// fallbacks, committing the rest of this partition's block list to the
    /// row path in one reopen instead of retrying columnar block by block.
    /// Never carries a `skip`: a fallback reopen needs one only while it is
    /// still resuming columnar, which is `RowFallbackBlock`'s job.
    Rows(Box<LogSegmentScan>),
    /// Re-opening the current segment to restart it on the row path after a
    /// block turned out to carry an `attrs_raw` overflow page. The re-opened
    /// scan is given the SAME block-index list this partition owns for the
    /// segment (ADR-0102), so `skip` is a position within that list: the number
    /// of this partition's blocks already fully emitted (columnar, or by an
    /// earlier `RowFallbackBlock`). Ready, it becomes
    /// [`LogScanState::RowFallbackBlock`], never `Rows`: this state exists only
    /// for the `attrs_raw` fallback.
    ReopenRows {
        fut: OpenFuture,
        skip: usize,
    },
    /// The re-opened row scan from [`LogScanState::ReopenRows`]: drain and
    /// discard `skip` blocks (already emitted columnar or by an earlier
    /// fallback, so re-decoding them here does not re-emit a row), then take
    /// exactly the next block -- the one that carried the `attrs_raw` overflow
    /// page -- through the row path (issue #1769). That block done, the same
    /// still-open scan is handed back to [`LogScanState::Columnar`] so the
    /// blocks after it keep decoding columnar instead of falling the rest of
    /// the segment to rows -- unless this is this segment's
    /// [`MAX_CONSECUTIVE_ATTR_RAW_FALLBACKS`]-th consecutive fallback with no
    /// clean columnar block in between, in which case it is handed to
    /// [`LogScanState::Rows`] instead, committing the rest of
    /// the block list to the row path with no further reopen. A block-index
    /// list this partition owns is never touched by another partition's
    /// fallback (ADR-0102), so this reopen cannot race one.
    RowFallbackBlock {
        scan: Box<LogSegmentScan>,
        skip: usize,
    },
    /// One columnar block's decode and Arrow build, running as a read-gate
    /// job that owns the scan (ADR-1702 decision 7). Resolved, the scan goes
    /// back to [`LogScanState::Columnar`] with the block's outcome in
    /// [`LogScanStream::decoded`].
    DecodingColumnar {
        job: ColumnarJobFuture,
        /// When the job was submitted, so `decode_build_elapsed` includes the
        /// wait for a gate permit.
        started: Instant,
    },
    /// One row-path block decoding on the read gate through
    /// [`LogSegmentScan::next_block_on_gate`]. Resolved, the scan goes back
    /// to the state `resume` names with the block in
    /// [`LogScanStream::decoded`].
    DecodingRows {
        job: RowJobFuture,
        resume: RowResume,
        started: Instant,
    },
    Done,
}

type ColumnarJobFuture = Pin<
    Box<
        dyn Future<
                Output = Result<
                    (Box<LogSegmentScan>, MemoryReservation, ColumnarStep),
                    CpuGateError,
                >,
            > + Send,
    >,
>;

type RowJobFuture = Pin<
    Box<
        dyn Future<
                Output = (
                    Box<LogSegmentScan>,
                    Option<usize>,
                    Result<Option<Vec<LogRecord>>, LogFetchError>,
                ),
            > + Send,
    >,
>;

/// The row-path state a [`LogScanState::DecodingRows`] job returns its scan
/// to.
#[derive(Clone, Copy)]
pub(super) enum RowResume {
    Rows,
    FallbackBlock { skip: usize },
}

/// A block a gate job decoded, taken by the state the job resumed.
pub(super) enum Decoded {
    Columnar(ColumnarStep),
    Rows {
        /// The block's whole-object index, read before the decode.
        block: Option<usize>,
        next: Result<Option<Vec<LogRecord>>, LogFetchError>,
    },
}

/// What [`LogScanStream::next_row_block`] did.
enum RowNext {
    /// The block's whole-object index, read before the decode, and the block.
    Ready(Option<usize>, Result<Option<Vec<LogRecord>>, LogFetchError>),
    /// The decode went to the read gate; the state is now
    /// [`LogScanState::DecodingRows`].
    Submitted,
    Failed(DataFusionError),
}

/// One columnar block's outcome, owned, so the view (which borrows the scan)
/// is dropped before the stream's state or reservation is touched.
pub(super) enum ColumnarStep {
    Exhausted(ScanStats),
    /// A block carrying an `attrs_raw` overflow page (or, only defensively,
    /// an unexpected pending erasure): fall the rest of this segment back to
    /// the row path.
    Fallback,
    /// A clean block's built batches (possibly empty for a block with no
    /// surviving row). The decoded block itself, which stays resident behind
    /// the reader while those batches drain, is charged by
    /// [`ColumnarBlockJob::run`].
    Held {
        batches: Vec<RecordBatch>,
    },
    /// A decode or build error, carried out of the view's borrow.
    Failed(DataFusionError),
}

/// Everything one columnar block's decode and build read besides the scan,
/// owned so the two can run together as one read-gate job.
struct ColumnarBlockJob {
    schema: SchemaRef,
    projection: Arc<Vec<usize>>,
    declared: Arc<Vec<DeclaredColumn>>,
    attr_keys: Arc<Vec<String>>,
    full_len: usize,
    /// The segment field of the block's row refs.
    segment: usize,
    /// The block's surviving-block position for its row refs, from
    /// [`block_index`]; consulted only when a block is decoded.
    block: DFResult<Option<usize>>,
    /// The scan's shared-dictionary values ([`LogScanStream::str_values`]).
    str_values: SharedStrValuesCache,
    /// Panic inside the gate job instead of running it.
    #[cfg(test)]
    panic_for_test: bool,
}

impl ColumnarBlockJob {
    /// Decodes `scan`'s next block and builds its batches.
    ///
    /// `resident` is the charge for the block the reader held before this
    /// call. The decode frees that block, so the charge is resized to the
    /// block it decoded as soon as the decode returns, and freed when it
    /// decoded none.
    fn run(self, scan: &mut LogSegmentScan, resident: &MemoryReservation) -> ColumnarStep {
        match scan.next_block_columnar() {
            Ok(ColumnarBlockOutcome::Exhausted) => {
                resident.free();
                ColumnarStep::Exhausted(scan.stats())
            }
            // The fast path is only entered with no erasure, so this is
            // unreachable in practice; fall back rather than risk serving an
            // erased record columnar.
            Ok(ColumnarBlockOutcome::ErasurePending) => {
                resident.free();
                ColumnarStep::Fallback
            }
            Ok(ColumnarBlockOutcome::Block(view)) => {
                if let Err(e) = resident.try_resize(view.decoded_bytes()) {
                    return ColumnarStep::Failed(e);
                }
                if view.has_attrs_raw_page() {
                    return ColumnarStep::Fallback;
                }
                let built = self.block.and_then(|block| {
                    build_columnar_batches(
                        &view,
                        &self.schema,
                        &self.projection,
                        &self.declared,
                        &self.attr_keys,
                        self.full_len,
                        block.map(|block| RowRefRange {
                            segment: self.segment,
                            block,
                            first_row: 0,
                        }),
                        &self.str_values,
                    )
                });
                match built {
                    Ok(batches) => ColumnarStep::Held { batches },
                    Err(e) => ColumnarStep::Failed(e),
                }
            }
            Err(e) => {
                resident.free();
                ColumnarStep::Failed(SqlError::from(e).into())
            }
        }
    }
}

/// `job` on `gate` as one [`ReadSite::LogBlock`] job: the block's decode and
/// its Arrow build both leave the runtime worker, and the scan comes back
/// with the outcome. `resident`, the charge for the block the scan's reader
/// holds, goes with it, so a job waiting for a permit keeps that block
/// charged until its decode frees it, and a job that never hands the scan
/// back drops the charge with the scan.
fn columnar_block_on_gate(
    gate: Arc<ReadGate>,
    size: JobSize,
    mut scan: Box<LogSegmentScan>,
    job: ColumnarBlockJob,
    resident: MemoryReservation,
) -> ColumnarJobFuture {
    Box::pin(async move {
        gate.run(ReadSite::LogBlock, size, move || {
            #[cfg(test)]
            assert!(!job.panic_for_test, "injected columnar block job panic");
            let step = job.run(&mut scan, &resident);
            (scan, resident, step)
        })
        .await
    })
}

/// Object keys whose next columnar block job panics on the gate. Keyed by
/// object so a test arming it leaves every other test's scan alone.
#[cfg(test)]
static PANIC_NEXT_COLUMNAR_JOB: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

/// Whether the next columnar block job over `key` panics, consuming the
/// arming.
#[cfg(test)]
fn take_columnar_job_panic(key: &str) -> bool {
    PANIC_NEXT_COLUMNAR_JOB
        .lock()
        .is_ok_and(|mut keys| match keys.iter().position(|k| k == key) {
            Some(i) => {
                keys.swap_remove(i);
                true
            }
            None => false,
        })
}

/// The row path's next block on the read gate, the scan returned with it.
fn row_block_on_gate(mut scan: Box<LogSegmentScan>) -> RowJobFuture {
    Box::pin(async move {
        let block = scan.next_block_index();
        let next = scan.next_block_on_gate().await;
        (scan, block, next)
    })
}

/// A columnar block job that failed on the gate, classified the way the
/// fetcher classifies its own gated block decodes: a panic is the decode's
/// failure, a job that never ran is transient.
fn log_block_gate_failed(key: &str, err: CpuGateError) -> DataFusionError {
    let fetch = match err {
        CpuGateError::Panicked => LogFetchError::Corrupt {
            key: key.to_string(),
            source: LogSegError::Corrupted(format!("read CPU gate: {err}")),
        },
        CpuGateError::Cancelled | CpuGateError::Closed => LogFetchError::Store {
            key: key.to_string(),
            source: StoreError::Transient(format!("read CPU gate: {err}")),
        },
    };
    SqlError::from(fetch).into()
}

/// The block currently being drained into output batches, and the form it is
/// held in. The charge tracked by [`LogScanStream::held`] covers the records or
/// batches of whichever variant is live; on the columnar path the decoded block
/// behind them is charged separately, in `LogScanStream::resident`.
pub(super) enum Pending {
    /// Nothing held.
    None,
    /// Row path: the block's surviving records, drained `BATCH_ROWS` at a time
    /// from `pos`.
    Rows { records: Vec<LogRecord>, pos: usize },
    /// Columnar fast path: the block's already-built output batches, emitted one
    /// per poll. Built whole from the [`ColumnarBlockView`] so the view (which
    /// borrows the scan) is dropped before the next block is decoded. The
    /// decoded block itself is still resident behind the reader until then; it
    /// is charged in `LogScanStream::resident`, not in the charge for these
    /// batches. Each batch carries its own charge ([`columnar_charges`]).
    Batches(VecDeque<(RecordBatch, usize)>),
}

/// The address a run of consecutive output rows carries in the synthetic
/// row-ref column (ADR-0774): the block they were decoded from, plus the
/// surviving-row position of the first of them.
///
/// `segment` is a position in the snapshot's segment list, `block` a position
/// in that segment's surviving-block list for this query, and `first_row` a
/// position in that block's surviving-row list. All three are cursor state the
/// scan already has; nothing is decoded to produce them.
#[derive(Clone, Copy)]
pub(super) struct RowRefRange {
    pub(super) segment: usize,
    pub(super) block: usize,
    pub(super) first_row: usize,
}

/// The surviving-block index of a just-decoded block, or `None` when the scan
/// emits no row refs (ADR-0774).
///
/// `decoded_block` is the whole-object block index the scan itself named as the
/// next block to decode, read before the decode
/// ([`LogSegmentScan::next_block_index`]), not a position into a
/// separately-tracked owned-block list: resolving a row-ref from the scan's
/// own drain order, rather than from a cursor counted alongside it, means a
/// list that has drifted out of step with the scan's actual blocks cannot
/// silently stamp the wrong row-ref (issue #2417). `survivors` is the
/// segment's whole surviving-block list; a row-ref carries a block's position
/// in it, not its whole-object index.
///
/// An unplanned segment ([`PlanCounts::unplanned`]) passes the survivor list
/// its own open pruned to, so it resolves like a planned one.
///
/// The whole-segment fast path passes `None`: it has no plan-phase survivor
/// list, and late materialization resolves the row-ref as a position among the
/// blocks its own reopen keeps. The two agree only while the open has pruned
/// no block before this one, so the decoded block must equal `cursor`, the
/// block's position in this scan's drain. A block that follows one the open
/// pruned (its skip index disagreeing with the catalog's bounds) fails with a
/// typed `Corrupted` error naming `key` rather than stamping a row-ref that
/// would resolve to another block.
///
/// A free function rather than a method because the columnar drain calls it
/// while the block cursor holds a mutable borrow of the stream's state field.
fn block_index(
    row_refs: bool,
    survivors: Option<&[usize]>,
    decoded_block: Option<usize>,
    cursor: usize,
    key: &str,
) -> DFResult<Option<usize>> {
    if !row_refs {
        return Ok(None);
    }
    let owned = decoded_block.ok_or_else(|| {
        DataFusionError::Internal(
            "row-ref requested but the scan has not decoded a block yet".into(),
        )
    })?;
    let Some(survivors) = survivors else {
        if owned != cursor {
            return Err(SqlError::from(LogFetchError::Corrupt {
                key: key.to_string(),
                source: LogSegError::Corrupted(format!(
                    "segment survivor mismatch: whole-segment open decoded block {owned} \
                     at drain position {cursor}"
                )),
            })
            .into());
        }
        return Ok(Some(owned));
    };
    survivors.binary_search(&owned).map(Some).map_err(|_| {
        DataFusionError::Internal(format!(
            "owned block {owned} is not among the segment's surviving blocks"
        ))
    })
}

/// The row-ref column for `rows` consecutive rows starting at `range`.
pub(super) fn row_ref_array(range: RowRefRange, rows: usize) -> DFResult<ArrayRef> {
    let mut packed = Vec::with_capacity(rows);
    for i in 0..rows {
        packed.push(
            RowRef {
                segment: range.segment,
                block: range.block,
                row: range.first_row + i,
            }
            .pack()?,
        );
    }
    Ok(Arc::new(UInt64Array::from(packed)))
}

impl LogScanStream {
    /// Publish one timeline point for the segment being drained: `name` at the
    /// current offset from the exec's origin, labelled with the segment's
    /// snapshot ordinal so a reader can line the partitions up on one clock.
    fn mark_segment(&self, name: &'static str) {
        self.mark_segment_at(self.current_seg_ordinal, name);
    }

    /// [`Self::mark_segment`] for the segment at snapshot ordinal `ordinal`,
    /// which a prefetched open reaches before it is the current segment.
    fn mark_segment_at(&self, ordinal: usize, name: &'static str) {
        if !self.segment_timing {
            return;
        }
        MetricBuilder::new(&self.metrics)
            .with_new_label("segment", ordinal.to_string())
            .subset_time(name, self.partition)
            .add_elapsed(self.origin);
    }

    /// Issue the ranged opens of this partition's next owned segments, in
    /// owned order, into its slot of the statement's [`PrefetchPool`], until
    /// [`Self::prefetch_share`] opens are held counting the current one
    /// (ADR-2414 decision A2). Only behind a ranged current segment, only
    /// while the next owned segment is itself a ranged open (the first
    /// whole-object segment stays in `work` and opens sequentially at its
    /// turn), and never once a revocation has turned the statement's
    /// pipeline off.
    fn top_up_prefetch(&mut self, cx: &mut Context<'_>) -> DFResult<()> {
        if !self.current_by_chunk || self.prefetch_pool.is_off() {
            return Ok(());
        }
        loop {
            let ranged = self
                .work
                .front()
                .is_some_and(|next| self.ctx.open_by_column_chunk(&next.seg));
            if !ranged {
                break;
            }
            let ordinal = {
                let mut slot = self.prefetch_pool.slot(self.partition)?;
                // Re-read under the lock: a revoker sets `off` before it
                // drains this slot, so nothing pushed here outlives a drain.
                if self.prefetch_pool.is_off() || slot.prefetched.len() + 1 >= self.prefetch_share {
                    break;
                }
                let Some(next) = self.work.pop_front() else {
                    break;
                };
                let ordinal = next.ordinal;
                let open = open_segment_fast(Arc::clone(&self.ctx), next.seg.clone(), true);
                slot.prefetched.push_back(Prefetch {
                    owned: next,
                    issued: Instant::now(),
                    open: PrefetchOpen::InFlight(open),
                });
                ordinal
            };
            self.mark_segment_at(ordinal, "seg_open_start_offset");
        }
        self.drive_prefetches(cx)
    }

    /// Poll every in-flight prefetched open in this partition's slot once,
    /// keeping each result until its segment's turn. A pending open has
    /// registered `cx`'s waker, so the stream is polled again when it can
    /// make progress.
    fn drive_prefetches(&mut self, cx: &mut Context<'_>) -> DFResult<()> {
        let mut ready = Vec::new();
        {
            let mut slot = self.prefetch_pool.slot(self.partition)?;
            for prefetch in slot.prefetched.iter_mut() {
                if let PrefetchOpen::InFlight(fut) = &mut prefetch.open
                    && let Poll::Ready(opened) = fut.as_mut().poll(cx)
                {
                    prefetch.open = PrefetchOpen::Ready {
                        opened: Box::new(opened),
                        at: Instant::now(),
                    };
                    ready.push(prefetch.owned.ordinal);
                }
            }
        }
        for ordinal in ready {
            self.mark_segment_at(ordinal, "seg_open_ready_offset");
        }
        Ok(())
    }

    /// Moves the segments a revocation handed back to this partition to the
    /// front of its work, in owned order.
    fn reclaim_handed_back(&mut self) -> DFResult<()> {
        let mut slot = self.prefetch_pool.slot(self.partition)?;
        while let Some(owned) = slot.handed_back.pop_back() {
            self.work.push_front(owned);
        }
        Ok(())
    }

    /// The next owned segment's prefetched open, if its turn has come with
    /// one in this partition's slot. Segments a revocation handed back are
    /// moved to the front of `work` first, under the same lock, so none is
    /// skipped by a revocation landing between the two reads.
    fn take_next_prefetch(&mut self) -> DFResult<Option<Prefetch>> {
        let mut slot = self.prefetch_pool.slot(self.partition)?;
        if slot.handed_back.is_empty() {
            return Ok(slot.prefetched.pop_front());
        }
        while let Some(owned) = slot.handed_back.pop_back() {
            self.work.push_front(owned);
        }
        Ok(None)
    }

    /// Handles a refused current open on the whole-segment fast path: a first
    /// open, a consumed prefetch ([`Reopen::Fast`]), or the `attrs_raw`
    /// row-path reopen ([`Reopen::Rows`]).
    ///
    /// The fetch memory budget is process-wide, so the bytes that refused
    /// the open may be held by prefetches of any partition of the statement.
    /// On a fetch memory refusal this turns the statement's pipeline off,
    /// drops every unconsumed prefetch of every partition
    /// ([`PrefetchPool::revoke_all`]; each segment goes back to the front of
    /// its owner's work, in owned order), moves this partition's own handed
    /// back segments into its work, and retries the refused open once, at
    /// once, on the same route and, for the row-path reopen, with the same
    /// `skip`. It waits for no other stream: the bytes are released by the
    /// drop itself. Any other error, any error off the fast path, and a
    /// refusal of the retry itself are returned untouched.
    ///
    /// The property this gives: within one statement a current open (a first
    /// open, a consumed prefetch, or the `attrs_raw` reopen) is reported
    /// refused only if the budget refused it twice, the second
    /// time after every unconsumed prefetch of every partition of the
    /// statement had been dropped and the statement's pipeline turned off.
    /// Between the drain and the retry a sibling's current open may reserve
    /// first; the retry then meets one open per partition, which is the
    /// sequential walk's own peak. Across statements the budget stays
    /// fail-fast: another statement's prefetches can hold the bytes, and
    /// that refusal is typed and reported like any contended reservation.
    fn on_open_refused(&mut self, e: DataFusionError, reopen: Reopen) -> DFResult<()> {
        if !(self.fast_whole_segment && is_fetch_memory_refusal(&e)) || self.retrying {
            return Err(e);
        }
        let Some(seg) = self.current_seg.clone() else {
            return Err(e);
        };
        let dropped = self.prefetch_pool.revoke_all()?;
        self.reclaim_handed_back()?;
        self.retrying = true;
        self.blocks.prefetch_revocations.add(dropped);
        self.blocks.prefetch_memory_reopens.add(1);
        tracing::debug!(
            partition = self.partition,
            segment = self.current_seg_ordinal,
            dropped,
            error = %e,
            "open refused by the fetch memory budget; dropped the statement's \
             prefetches and retrying once"
        );
        self.state = match reopen {
            Reopen::Fast => {
                self.open_started.get_or_insert_with(Instant::now);
                LogScanState::Opening(open_segment_fast(
                    Arc::clone(&self.ctx),
                    seg,
                    self.current_by_chunk,
                ))
            }
            Reopen::Rows { skip } => {
                self.open_started = Some(Instant::now());
                LogScanState::ReopenRows {
                    fut: self.row_reopen(seg),
                    skip,
                }
            }
        };
        Ok(())
    }

    /// The `attrs_raw` fallback's row-path reopen of `seg`, the current
    /// segment: the same routing decision its first open took (#862) on the
    /// fast path, and on the striped route the same block-index list, survivor
    /// list, footer and directories, with the carried whole object `take()`n,
    /// so it is consumed at most once by construction.
    fn row_reopen(&mut self, seg: SegmentRef) -> OpenFuture {
        if self.fast_whole_segment {
            let by_chunk = self.ctx.open_by_column_chunk(&seg);
            open_segment_fast(Arc::clone(&self.ctx), seg, by_chunk)
        } else if self.current_unplanned {
            open_segment_whole(Arc::clone(&self.ctx), seg)
        } else {
            open_segment_subset(
                Arc::clone(&self.ctx),
                seg,
                self.current_indices.clone(),
                self.current_survivors.clone(),
                self.current_footer.clone(),
                self.current_whole_object.take(),
                self.current_dirs.as_ref().map(|c| Arc::clone(&c.dirs)),
            )
        }
    }

    /// The surviving-block index of the block just decoded, or `None` when
    /// this scan emits no row refs. `decoded_block` is that block's
    /// whole-object index, read from the scan before it decoded the block
    /// ([`LogSegmentScan::next_block_index`]), not the cursor's position in
    /// [`Self::current_indices`] (issue #2417).
    fn current_block(&self, decoded_block: Option<usize>) -> DFResult<Option<usize>> {
        block_index(
            self.row_refs,
            self.current_survivors.as_deref().map(Vec::as_slice),
            decoded_block,
            self.block_cursor,
            self.current_seg
                .as_ref()
                .map_or("", |s| s.data_object_key.as_str()),
        )
    }

    /// Skips, undecoded, the blocks at the front of the open segment whose
    /// minimum ts is above the TopK threshold (ADR-2677 decision 6). Called
    /// before each block decode on the `Rows` state (the `Columnar` arm does
    /// the same inline), never on `RowFallbackBlock`, whose reopened scan
    /// re-drains blocks by position.
    ///
    /// The row-ref cursor moves past the skipped blocks with the scan, so the
    /// whole-segment path's check that the decoded block sits at the drain
    /// position still holds and every later row keeps its address.
    fn skip_threshold_blocks(&mut self) {
        let Some(ceiling) = topk_ceiling(self.topk.as_ref()) else {
            return;
        };
        let (LogScanState::Columnar(scan) | LogScanState::Rows(scan)) = &mut self.state else {
            return;
        };
        let skipped = skip_blocks_above(scan, ceiling);
        self.block_cursor += skipped;
        self.blocks.blocks_skipped_by_threshold.add(skipped);
    }

    /// The row-ref address for the block just decoded (`decoded_block`, its
    /// whole-object index), and advance the cursor past it.
    fn take_block_range(&mut self, decoded_block: Option<usize>) -> DFResult<Option<RowRefRange>> {
        let range = self.current_block(decoded_block)?.map(|block| RowRefRange {
            segment: self.current_seg_ordinal,
            block,
            first_row: 0,
        });
        self.block_cursor += 1;
        Ok(range)
    }

    /// Emit the next row-path batch out of `pending`, moving the reservation
    /// with it: the previous batch's charge is released (it is downstream's
    /// now), the new batch's charge is taken before it is handed over.
    fn emit_next_row_batch(&mut self) -> DFResult<RecordBatch> {
        self.reservation.shrink(std::mem::take(&mut self.emitted));
        let pending_range = self.pending_range;
        let Pending::Rows { records, pos } = &mut self.pending else {
            return Err(DataFusionError::Internal(
                "emit_next_row_batch called without a row block held".into(),
            ));
        };
        let end = (*pos + BATCH_ROWS).min(records.len());
        let batch = build_batch(
            &records[*pos..end],
            Arc::clone(&self.schema),
            &self.projection,
            &self.declared,
            &self.attr_keys,
            self.full_len,
            pending_range.map(|r| RowRefRange {
                first_row: r.first_row + *pos,
                ..r
            }),
        )?;
        *pos = end;
        let bytes = batch.get_array_memory_size();
        self.reservation.try_grow(bytes)?;
        self.emitted = bytes;
        self.blocks.rowpath_batches.add(1);
        Ok(batch)
    }

    /// Emit the next columnar-path batch: pop the front pre-built batch, relabel
    /// its already-reserved bytes from `held` to `emitted`, and release the
    /// batch handed out on the previous poll. No new reservation is taken -- the
    /// block's batches were charged once in [`Self::hold_batches`], and the
    /// decoded block itself stays charged in [`Self::resident`].
    fn emit_next_columnar_batch(&mut self) -> DFResult<RecordBatch> {
        self.reservation.shrink(std::mem::take(&mut self.emitted));
        let Pending::Batches(queue) = &mut self.pending else {
            return Err(DataFusionError::Internal(
                "emit_next_columnar_batch called without columnar batches held".into(),
            ));
        };
        let Some((batch, bytes)) = queue.pop_front() else {
            return Err(DataFusionError::Internal(
                "emit_next_columnar_batch called on an empty queue".into(),
            ));
        };
        // The batch's bytes were reserved as part of `held`; moving it
        // downstream relabels that charge rather than growing or releasing it.
        self.held = self.held.saturating_sub(bytes);
        self.emitted = bytes;
        self.blocks.columnar_batches.add(1);
        Ok(batch)
    }

    /// Take ownership of one decoded block's records (row path), charging the
    /// pool for `records_memory` before it is held. An empty block (every row
    /// filtered out) charges nothing and leaves the stream to ask for the next.
    fn hold_block(&mut self, records: Vec<LogRecord>) -> DFResult<()> {
        let bytes = records_memory(&records);
        self.reservation.try_grow(bytes)?;
        self.held = bytes;
        self.pending = Pending::Rows { records, pos: 0 };
        Ok(())
    }

    /// Take ownership of one block's pre-built columnar batches, charging the
    /// pool for their Arrow footprint as [`columnar_charges`] splits it. The
    /// decoded block they were built from is charged separately, in
    /// [`Self::resident`], and the shared dictionary values the scan's
    /// [`StrValuesCache`] holds in [`Self::str_values_charge`], resized here to
    /// the cache's live entries.
    ///
    /// [`Self::emit_next_columnar_batch`] moves each emitted batch's own
    /// charge from `held` to `emitted`; the block's shared values stay in
    /// `held` until [`Self::release_block`].
    fn hold_batches(&mut self, batches: Vec<RecordBatch>) -> DFResult<()> {
        let (charged, shared, live) = {
            let mut cache = lock_str_values(&self.str_values);
            let live = cache.live_bytes();
            let (charged, shared) = columnar_charges(batches, &cache);
            (charged, shared, live)
        };
        self.release_dead_str_values(live);
        let bytes = charged.iter().map(|(_, c)| c).sum::<usize>() + shared;
        let growth = live.saturating_sub(self.str_values_charge);
        self.reservation.try_grow(bytes + growth)?;
        self.str_values_charge += growth;
        self.held = bytes;
        self.pending = Pending::Batches(charged);
        Ok(())
    }

    /// Shrink [`Self::str_values_charge`] to `live` when the cache's live
    /// entries have fallen below it.
    ///
    /// The batch handed out last keeps only its own keys charged, in
    /// [`Self::emitted`], until the next emit. Its values can lose their charge
    /// before that: a base values array here once the reader has moved past
    /// its row group, a plain page's values in [`Self::release_block`]. The gap
    /// is at most one values array per dictionary column of that one batch,
    /// and it closes at the next emit, which releases the batch's charge in
    /// any case.
    fn release_dead_str_values(&mut self, live: usize) {
        if live < self.str_values_charge {
            self.reservation.shrink(self.str_values_charge - live);
            self.str_values_charge = live;
        }
    }

    /// Drop every shared dictionary values array the scan's cache holds and
    /// release their charge: the stream has nothing left to read them for.
    fn release_str_values(&mut self) {
        lock_str_values(&self.str_values).entries.clear();
        self.release_dead_str_values(0);
    }

    /// True when the current block still has a batch to emit.
    fn has_pending(&self) -> bool {
        match &self.pending {
            Pending::None => false,
            Pending::Rows { records, pos } => *pos < records.len(),
            Pending::Batches(queue) => !queue.is_empty(),
        }
    }

    /// Drop the drained block's records or batches and release their charge,
    /// with the charge of every cached values array no batch reads any more.
    /// The columnar cursor's decoded block stays charged in
    /// [`Self::resident`] until the next decode replaces it.
    fn release_block(&mut self) {
        self.reservation.shrink(std::mem::take(&mut self.held));
        self.pending = Pending::None;
        let live = lock_str_values(&self.str_values).live_bytes();
        self.release_dead_str_values(live);
    }

    /// The scan the state holds reported exhaustion: publish its final
    /// counters and move to the next owned segment.
    fn end_scanned_segment(&mut self) {
        if let LogScanState::Columnar(scan)
        | LogScanState::Rows(scan)
        | LogScanState::RowFallbackBlock { scan, .. } = &self.state
        {
            let stats = scan.stats();
            self.blocks.record_scan(&stats);
            if self.fast_whole_segment || self.current_unplanned {
                self.blocks.record_segment_totals(&stats);
            }
        }
        self.mark_segment("seg_done_offset");
        self.finish_segment();
    }

    /// The next block of the row-path scan the state holds: the one a
    /// [`LogScanState::DecodingRows`] job just decoded, or, with none waiting,
    /// a new decode. That decode goes to the read gate as a job when the
    /// scan's fetcher carries one, leaving the state at
    /// [`LogScanState::DecodingRows`] to return to `resume`, and runs inline
    /// otherwise. Exhaustion decodes nothing and always runs inline.
    fn next_row_block(&mut self, resume: RowResume) -> RowNext {
        match self.decoded.take() {
            Some(Decoded::Rows { block, next }) => return RowNext::Ready(block, next),
            Some(Decoded::Columnar(_)) => {
                return RowNext::Failed(DataFusionError::Internal(
                    "a columnar block resumed on the row path".into(),
                ));
            }
            None => {}
        }
        let (LogScanState::Rows(scan) | LogScanState::RowFallbackBlock { scan, .. }) =
            &mut self.state
        else {
            return RowNext::Failed(DataFusionError::Internal(
                "row block requested off a row-path state".into(),
            ));
        };
        // Unlike the columnar exit, the row exit frees its decoded block before
        // returning the records (`BlockScan::next_block`), so no previous
        // block is resident behind the reader while this job waits for a
        // permit, and there is no block charge to carry into it.
        if scan.remaining_blocks() > 0 && scan.block_gate().is_some() {
            let (LogScanState::Rows(scan) | LogScanState::RowFallbackBlock { scan, .. }) =
                std::mem::replace(&mut self.state, LogScanState::Done)
            else {
                return RowNext::Failed(DataFusionError::Internal(
                    "row block requested off a row-path state".into(),
                ));
            };
            self.state = LogScanState::DecodingRows {
                job: row_block_on_gate(scan),
                resume,
                started: Instant::now(),
            };
            return RowNext::Submitted;
        }
        let decode_started = Instant::now();
        let block = scan.next_block_index();
        let next = scan.next_block();
        self.blocks.decode_build_elapsed.add_elapsed(decode_started);
        RowNext::Ready(block, next)
    }

    /// This partition is done with the current segment: drop its handle on the
    /// segment's directories and move to the next owned segment.
    fn finish_segment(&mut self) {
        self.current_dirs = None;
        self.state = LogScanState::NextSegment;
        let live = lock_str_values(&self.str_values).live_bytes();
        self.release_dead_str_values(live);
    }

    /// This partition has emitted its pushed `fetch` rows: stop, mid-segment
    /// if need be, and release everything the scan still holds.
    ///
    /// A scan still open is dropped here instead of running to its
    /// end-of-segment record, so its counters are published first: the decode
    /// counts as they stand (`record_scan` accumulates), and on the
    /// whole-segment fast path the segment's prune totals, which are final from
    /// the open and so are the whole segment's, not a partial count.
    fn finish_at_fetch(&mut self) {
        match std::mem::replace(&mut self.state, LogScanState::Done) {
            LogScanState::Columnar(scan)
            | LogScanState::Rows(scan)
            | LogScanState::RowFallbackBlock { scan, .. } => {
                let stats = scan.stats();
                self.blocks.record_scan(&stats);
                if self.fast_whole_segment || self.current_unplanned {
                    self.blocks.record_segment_totals(&stats);
                }
            }
            // A block job runs only once the previous block's batches or
            // records are all emitted, so no row has been emitted since the
            // last check and `fetch` cannot be reached with one in flight. Dropping it abandons the job; the scan folds its
            // accounting when it drops, with the future or with the job's
            // discarded result.
            LogScanState::Planning(_)
            | LogScanState::NextSegment
            | LogScanState::Opening(_)
            | LogScanState::ReopenRows { .. }
            | LogScanState::DecodingColumnar { .. }
            | LogScanState::DecodingRows { .. }
            | LogScanState::Done => {}
        }
        self.current_dirs = None;
        self.work.clear();
        self.prefetch_pool.clear(self.partition);
        self.release_block();
        self.resident.free();
        self.release_str_values();
    }

    /// Abandon the stream on error, releasing everything the scan still holds.
    fn fail(&mut self, e: DataFusionError) -> Poll<Option<DFResult<RecordBatch>>> {
        self.state = LogScanState::Done;
        self.current_dirs = None;
        self.work.clear();
        self.prefetch_pool.clear(self.partition);
        self.release_block();
        self.resident.free();
        self.release_str_values();
        self.reservation.shrink(std::mem::take(&mut self.emitted));
        Poll::Ready(Some(Err(e)))
    }

    /// Take one decoded block's surviving records through the row path: apply
    /// scan-layer selective-erasure exclusion (ADR-0064) and hold the records.
    ///
    /// The exclusion here is authoritative because it sees the same merged
    /// `attrs` view the surface returns (resource + scope + record), so a
    /// subject named only in a resource/scope attribute is dropped; the
    /// fetcher-level filter matches per-record attributes alone and cannot see
    /// it. An empty `records` is normal, not end-of-segment: a block can survive
    /// pruning and hold no matching row, or have every matching row erased.
    fn take_row_block(&mut self, mut records: Vec<LogRecord>) -> DFResult<()> {
        retain_unerased(&mut records, &self.erasure)?;
        self.hold_block(records)
    }
}

impl Stream for LogScanStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.blocks.polls.add(1);
        let polled = this.poll_inner(cx);
        match &polled {
            Poll::Pending => this.blocks.polls_pending.add(1),
            Poll::Ready(Some(Ok(_))) if !this.first_batch_seen => {
                this.first_batch_seen = true;
                this.blocks.first_batch_elapsed.add_elapsed(this.origin);
            }
            Poll::Ready(None) if !this.done_seen => {
                this.done_seen = true;
                this.blocks.stream_elapsed.add_elapsed(this.origin);
            }
            Poll::Ready(_) => {}
        }
        polled
    }
}

/// The prefetch pool belongs to the exec and outlives this stream, so a
/// stream dropped mid-statement (a cancelled query, a satisfied limit above
/// it) drops its own slot's prefetches here, releasing their fetch
/// reservations with it rather than when the plan is dropped.
impl Drop for LogScanStream {
    fn drop(&mut self) {
        self.prefetch_pool.clear(self.partition);
        self.prefetch_pool.unregister(self.partition);
    }
}

impl LogScanStream {
    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<DFResult<RecordBatch>>> {
        let this = self;
        if let Err(e) = this
            .reclaim_handed_back()
            .and_then(|()| this.drive_prefetches(cx))
        {
            return this.fail(e);
        }
        loop {
            if this.fetch.is_some_and(|fetch| this.rows_emitted >= fetch) {
                this.finish_at_fetch();
                return Poll::Ready(None);
            }
            // Anything buffered from the current block goes out first.
            if this.has_pending() {
                let emit_started = Instant::now();
                let emitted = match &this.pending {
                    Pending::Rows { .. } => this.emit_next_row_batch(),
                    Pending::Batches(_) => this.emit_next_columnar_batch(),
                    Pending::None => unreachable!("has_pending() ruled this out"),
                };
                this.blocks.emit_elapsed.add_elapsed(emit_started);
                return match emitted {
                    Ok(batch) => {
                        // DataFusion drops the limit above a scan that
                        // accepted `with_fetch`, so the cap is exact here.
                        let batch = match this.fetch {
                            Some(fetch) if this.rows_emitted + batch.num_rows() > fetch => {
                                batch.slice(0, fetch.saturating_sub(this.rows_emitted))
                            }
                            _ => batch,
                        };
                        this.rows_emitted += batch.num_rows();
                        Poll::Ready(Some(Ok(batch)))
                    }
                    Err(e) => this.fail(e),
                };
            }
            // The block is drained: release its records or batches before
            // decoding another. A columnar block's decoded bytes stay charged
            // until the next decode replaces them (`resident`).
            if this.held > 0 || !matches!(this.pending, Pending::None) {
                this.release_block();
            }

            match &mut this.state {
                LogScanState::Planning(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(counts)) => {
                        this.blocks
                            .planning_wait_elapsed
                            .add_elapsed(this.stream_started);
                        // The stride the plan counted each segment's owners
                        // against: `target_partitions` capped by the real block
                        // count (ADR-0102), so with fewer blocks than partitions
                        // the extra partitions get empty work.
                        let n = counts.partitions;
                        // Partition 0 publishes every relevant segment's
                        // whole-segment prune totals, once, so striping a segment
                        // across partitions does not multiply them.
                        if this.partition == 0 {
                            for plan in counts.segs.iter().flatten() {
                                this.blocks.record_segment_totals(&plan.stats);
                            }
                            this.blocks.plan_full_reads.add(counts.full_reads);
                            this.blocks
                                .segments_pruned_by_key_index
                                .add(counts.key_index.segments_pruned as usize);
                            this.blocks
                                .blocks_pruned_by_key_index
                                .add(counts.key_index.blocks_pruned as usize);
                        }
                        let order = visit_order(&this.segments, this.topk.is_some());
                        this.work = owned_work(
                            &counts,
                            &this.segments,
                            &order,
                            this.partition,
                            n,
                            this.stripe_blocks,
                        );
                        this.state = LogScanState::NextSegment;
                    }
                    Poll::Ready(Err(e)) => return this.fail(e),
                    Poll::Pending => return Poll::Pending,
                },
                LogScanState::NextSegment => {
                    let prefetch = match this.take_next_prefetch() {
                        Ok(prefetch) => prefetch,
                        Err(e) => return this.fail(e),
                    };
                    // The next owned segment's open was issued ahead of its
                    // turn (ADR-2414 decision A2). Everything the sequential
                    // walk records for a fast-path segment is recorded here,
                    // at its turn and in owned order; every prefetch is a
                    // ranged open.
                    if let Some(Prefetch {
                        owned: OwnedSeg { seg, ordinal, .. },
                        issued,
                        open,
                    }) = prefetch
                    {
                        this.current_seg = Some(seg);
                        this.current_seg_ordinal = ordinal;
                        this.current_indices = Vec::new();
                        this.current_survivors = None;
                        this.current_footer = None;
                        this.current_dirs = None;
                        this.current_whole_object = None;
                        this.current_unplanned = false;
                        this.current_by_chunk = true;
                        this.retrying = false;
                        this.block_cursor = 0;
                        this.consecutive_fallbacks = 0;
                        this.blocks.segments_opened.add(1);
                        this.blocks.record_fast_path_route(true);
                        this.ctx.record_open_shape(true);
                        this.ctx.record_data_object_touched();
                        match open {
                            PrefetchOpen::InFlight(fut) => {
                                this.open_started = Some(issued);
                                this.state = LogScanState::Opening(fut);
                            }
                            PrefetchOpen::Ready { opened, at } => {
                                this.open_started = None;
                                this.blocks
                                    .open_elapsed
                                    .add_duration(at.saturating_duration_since(issued));
                                match *opened {
                                    Ok(Some(scan)) => {
                                        this.state = if this.columnar_eligible {
                                            LogScanState::Columnar(Box::new(scan))
                                        } else {
                                            LogScanState::Rows(Box::new(scan))
                                        };
                                        if let Err(e) = this.top_up_prefetch(cx) {
                                            return this.fail(e);
                                        }
                                    }
                                    Ok(None) => this.finish_segment(),
                                    Err(e) => {
                                        if let Err(e) = this.on_open_refused(e, Reopen::Fast) {
                                            return this.fail(e);
                                        }
                                    }
                                }
                            }
                        }
                        continue;
                    }
                    match this.work.pop_front() {
                        Some(OwnedSeg {
                            seg,
                            ordinal,
                            indices,
                            survivors,
                            footer,
                            whole_object,
                            dirs,
                            unplanned,
                        }) => {
                            // ADR-2677 decision 6: a segment whose every row
                            // is above the TopK threshold is not opened for
                            // its blocks.
                            if topk_ceiling(this.topk.as_ref()).is_some_and(|ceiling| {
                                above_ceiling(seg.min_event_ts_ns, seg.max_event_ts_ns, ceiling)
                            }) {
                                this.blocks.segments_skipped_by_threshold.add(1);
                                continue;
                            }
                            this.current_seg = Some(seg.clone());
                            this.current_seg_ordinal = ordinal;
                            this.current_indices = indices.clone();
                            this.current_survivors = survivors.clone();
                            this.current_footer = footer.clone();
                            let open_dirs = dirs.as_ref().map(|c| Arc::clone(&c.dirs));
                            this.current_dirs = dirs;
                            // Moved, not cloned: this `OwnedSeg` open consumes its
                            // carried whole object exactly once, by whichever open
                            // below `take()`s it first. Under `global_deal` a
                            // stream can reach this transition several times for
                            // the same segment, one `OwnedSeg` at a time, each
                            // with its own carry. A later `ReopenRows` reopen of
                            // the SAME `OwnedSeg` must see `None` and pay for its
                            // own fetch (real GET or read-cache hit), or
                            // `tenant_bytes_with_footer` charges
                            // `add_bytes_reused` twice for one buffer (issue #835
                            // follow-up).
                            this.current_whole_object = whole_object;
                            this.current_unplanned = unplanned;
                            this.retrying = false;
                            this.block_cursor = 0;
                            this.consecutive_fallbacks = 0;
                            this.blocks.segments_opened.add(1);
                            this.open_started = Some(Instant::now());
                            this.mark_segment("seg_open_start_offset");
                            // Whole-segment fast path reads the object in one GET
                            // (#693 part 3), or by column chunk when the projection
                            // is narrow enough to pay for the extra round trips
                            // (#862); the striped path opens only this partition's
                            // subset, reusing the plan footer if any.
                            this.state = if this.fast_whole_segment {
                                let by_chunk = this.ctx.open_by_column_chunk(&seg);
                                this.current_by_chunk = by_chunk;
                                this.blocks.record_fast_path_route(by_chunk);
                                this.ctx.record_open_shape(by_chunk);
                                // The fast path's owning-partition recorder for
                                // ADR-0996 decision 3; see
                                // `PartitionCtx::record_data_object_touched` for why
                                // this site is once per segment per query and why
                                // the planned route's recorder cannot also fire.
                                this.ctx.record_data_object_touched();
                                LogScanState::Opening(open_segment_fast(
                                    Arc::clone(&this.ctx),
                                    seg,
                                    by_chunk,
                                ))
                            } else if unplanned {
                                // ADR-2677 decision 2: one whole-object read,
                                // pruned in the scan. No plan read touched the
                                // object, and this partition is its only owner,
                                // so the touch is recorded here, once.
                                this.current_by_chunk = false;
                                this.blocks.unplanned_segments.add(1);
                                this.ctx.record_data_object_touched();
                                LogScanState::Opening(open_segment_whole(
                                    Arc::clone(&this.ctx),
                                    seg,
                                ))
                            } else {
                                // `take()`, not the moved-in value directly: this
                                // IS the one consumption of the carried whole
                                // object (see the comment above where it moved
                                // into `current_whole_object`). Taking it here
                                // leaves `None` behind for any later reopen.
                                let whole_object = this.current_whole_object.take();
                                LogScanState::Opening(open_segment_subset(
                                    Arc::clone(&this.ctx),
                                    seg,
                                    indices,
                                    survivors,
                                    footer,
                                    whole_object,
                                    open_dirs,
                                ))
                            };
                        }
                        None => {
                            this.state = LogScanState::Done;
                            this.release_str_values();
                        }
                    }
                }
                // The current open is polled before the opens behind it are
                // topped up, so its first reservation goes first (its later
                // ones can still follow a prefetch's); the top-up then
                // runs whether it resolved or is still in flight, which keeps
                // the next segments' round trips overlapping this one's.
                LogScanState::Opening(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(Some(scan))) => {
                        this.retrying = false;
                        if let Some(started) = this.open_started.take() {
                            this.blocks.open_elapsed.add_elapsed(started);
                        }
                        this.mark_segment("seg_open_ready_offset");
                        // An unplanned segment's survivor list is the one its
                        // open just pruned to, which is also the list a
                        // late-materialization reopen prunes to, so a row-ref's
                        // block position resolves the same way on both.
                        if this.current_unplanned {
                            this.current_survivors = Some(Arc::new(scan.survivor_block_indices()));
                        }
                        this.state = if this.columnar_eligible {
                            LogScanState::Columnar(Box::new(scan))
                        } else {
                            LogScanState::Rows(Box::new(scan))
                        };
                        if let Err(e) = this.top_up_prefetch(cx) {
                            return this.fail(e);
                        }
                    }
                    // The segment's ts span could not satisfy the query: no GET
                    // was issued and there is nothing to drain.
                    Poll::Ready(Ok(None)) => {
                        if let Some(started) = this.open_started.take() {
                            this.blocks.open_elapsed.add_elapsed(started);
                        }
                        this.retrying = false;
                        this.finish_segment();
                    }
                    Poll::Ready(Err(e)) => {
                        if let Err(e) = this.on_open_refused(e, Reopen::Fast) {
                            return this.fail(e);
                        }
                    }
                    Poll::Pending => {
                        this.blocks.open_pending_polls.add(1);
                        if let Err(e) = this.top_up_prefetch(cx) {
                            return this.fail(e);
                        }
                        return Poll::Pending;
                    }
                },
                LogScanState::ReopenRows { fut, skip } => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(Some(scan))) => {
                        this.retrying = false;
                        if let Some(started) = this.open_started.take() {
                            this.blocks.reopen_elapsed.add_elapsed(started);
                        }
                        let skip = *skip;
                        this.state = LogScanState::RowFallbackBlock {
                            scan: Box::new(scan),
                            skip,
                        };
                    }
                    // Cannot happen for a segment already opened once this scan;
                    // treat a vanished segment as end-of-segment rather than
                    // panicking.
                    Poll::Ready(Ok(None)) => {
                        this.retrying = false;
                        this.finish_segment();
                    }
                    Poll::Ready(Err(e)) => {
                        let skip = *skip;
                        if let Err(e) = this.on_open_refused(e, Reopen::Rows { skip }) {
                            return this.fail(e);
                        }
                    }
                    Poll::Pending => return Poll::Pending,
                },
                LogScanState::Columnar(scan) => {
                    let step = match this.decoded.take() {
                        Some(Decoded::Columnar(step)) => step,
                        Some(Decoded::Rows { .. }) => {
                            return this.fail(DataFusionError::Internal(
                                "a row-path block resumed on the columnar path".into(),
                            ));
                        }
                        None => {
                            if let Some(ceiling) = topk_ceiling(this.topk.as_ref()) {
                                let skipped = skip_blocks_above(scan, ceiling);
                                this.block_cursor += skipped;
                                this.blocks.blocks_skipped_by_threshold.add(skipped);
                            }
                            // The row-ref address of the block this call
                            // decodes, resolved from the scan's own next block
                            // (issue #2417), not from the cursor's position in
                            // `current_indices`. Read before the decode, since
                            // the view it returns holds the scan borrowed; only
                            // a decoded block consumes it, and with no next
                            // block none is decoded.
                            let block = match scan.next_block_index() {
                                Some(next) => block_index(
                                    this.row_refs,
                                    this.current_survivors.as_deref().map(Vec::as_slice),
                                    Some(next),
                                    this.block_cursor,
                                    this.current_seg
                                        .as_ref()
                                        .map_or("", |s| s.data_object_key.as_str()),
                                ),
                                None => Ok(None),
                            };
                            let job = ColumnarBlockJob {
                                schema: Arc::clone(&this.schema),
                                projection: Arc::clone(&this.projection),
                                declared: Arc::clone(&this.declared),
                                attr_keys: Arc::clone(&this.attr_keys),
                                full_len: this.full_len,
                                segment: this.current_seg_ordinal,
                                block,
                                str_values: Arc::clone(&this.str_values),
                                #[cfg(test)]
                                panic_for_test: take_columnar_job_panic(
                                    this.current_seg
                                        .as_ref()
                                        .map_or("", |s| s.data_object_key.as_str()),
                                ),
                            };
                            match scan.block_gate() {
                                // Exhaustion decodes nothing, so it submits no
                                // job.
                                Some((gate, size)) if scan.remaining_blocks() > 0 => {
                                    let LogScanState::Columnar(scan) =
                                        std::mem::replace(&mut this.state, LogScanState::Done)
                                    else {
                                        return this.fail(DataFusionError::Internal(
                                            "columnar decode submitted off a non-columnar state"
                                                .into(),
                                        ));
                                    };
                                    // The previous block's charge goes with the
                                    // scan: the reader frees that block only
                                    // when the job's decode runs.
                                    let resident = this.resident.take();
                                    this.state = LogScanState::DecodingColumnar {
                                        job: columnar_block_on_gate(
                                            gate, size, scan, job, resident,
                                        ),
                                        started: Instant::now(),
                                    };
                                    continue;
                                }
                                _ => {
                                    let decode_started = Instant::now();
                                    let step = job.run(scan, &this.resident);
                                    this.blocks.decode_build_elapsed.add_elapsed(decode_started);
                                    step
                                }
                            }
                        }
                    };
                    match step {
                        ColumnarStep::Failed(e) => return this.fail(e),
                        ColumnarStep::Exhausted(stats) => {
                            this.blocks.record_scan(&stats);
                            // The whole-segment fast path has no plan phase, so
                            // the whole-segment prune totals are recorded here,
                            // per segment (one owner per segment, no double
                            // count), instead of by partition 0 during planning.
                            // An unplanned segment has one owner too.
                            if this.fast_whole_segment || this.current_unplanned {
                                this.blocks.record_segment_totals(&stats);
                            }
                            this.mark_segment("seg_done_offset");
                            this.finish_segment();
                        }
                        ColumnarStep::Fallback => {
                            // Re-open the segment on the row path over the SAME
                            // block-index list this partition owns (ADR-0102),
                            // skipping the blocks already fully emitted (columnar,
                            // or row-emitted by an earlier fallback in this same
                            // segment) so no row is emitted twice. `skip` is
                            // `block_cursor`, a position within this partition's
                            // own list, so the count and the list line up even
                            // when the segment's blocks are striped across several
                            // partitions. `RowFallbackBlock` (issue #1769) takes
                            // only the next block -- the one that just failed the
                            // columnar attempt -- through the row path, then hands
                            // this reopened scan back to `Columnar` for the rest.
                            //
                            // Publish the abandoned columnar cursor's partial
                            // counters (issue #474) before it is dropped: the
                            // re-opened row scan below re-decodes this partition's
                            // list from the start up to `skip`, so those blocks'
                            // pages are decoded twice, but the abandoned cursor's
                            // own count of its first pass was previously discarded
                            // along with it. `record_scan` accumulates, so this
                            // and the reopened scan's own eventual `record_scan`
                            // call(s) sum to the real total decode work across all
                            // passes, matching what `EXPLAIN ANALYZE` claims the
                            // counters prove (ADR-0087): that projection reached
                            // the page level.
                            this.blocks.record_scan(&scan.stats());
                            let seg = match this.current_seg.clone() {
                                Some(seg) => seg,
                                None => {
                                    return this.fail(DataFusionError::Internal(
                                        "attrs_raw fallback with no current segment".into(),
                                    ));
                                }
                            };
                            // Re-opened through the same routing decision the
                            // first open took (#862), and deliberately not
                            // re-counted: the route metric and the accounting
                            // handle's opens-by-shape counters both tally
                            // segments, not opens.
                            let fut = this.row_reopen(seg);
                            this.blocks.reopens.add(1);
                            this.consecutive_fallbacks += 1;
                            this.open_started = Some(Instant::now());
                            this.state = LogScanState::ReopenRows {
                                fut,
                                skip: this.block_cursor,
                            };
                            // The abandoned cursor, and the block its reader
                            // held, went with the state it was in.
                            this.resident.free();
                        }
                        ColumnarStep::Held { batches } => {
                            // A clean columnar block: the streak of consecutive
                            // fallbacks that would otherwise escalate to a
                            // full row-path commit is broken.
                            this.consecutive_fallbacks = 0;
                            // Count every consumed clean block, empty or not, so
                            // a later `attrs_raw` fallback's `ReopenRows` skips
                            // exactly the blocks the cursor advanced past. The
                            // row-ref cursor moves with it, so a fallback
                            // re-opens at the same surviving-block position.
                            this.block_cursor += 1;
                            // The decoded block, whether or not a row survived,
                            // is already charged in `resident`: the job that
                            // decoded it resized that charge from the previous
                            // block's as its decode returned, and the next job
                            // resizes it again only once its own decode has
                            // freed this block. Outside the decode calls
                            // themselves there is no interval during which it
                            // is resident and uncharged, a wait for a gate
                            // permit included.
                            if !batches.is_empty()
                                && let Err(e) = this.hold_batches(batches)
                            {
                                return this.fail(e);
                            }
                        }
                    }
                }
                LogScanState::Rows(_) => {
                    if this.decoded.is_none() {
                        this.skip_threshold_blocks();
                    }
                    let (decoded_block, next) = match this.next_row_block(RowResume::Rows) {
                        RowNext::Ready(block, next) => (block, next),
                        RowNext::Submitted => continue,
                        RowNext::Failed(e) => return this.fail(e),
                    };
                    match next {
                        Ok(Some(records)) => {
                            // Stamp the block's row-ref address before the
                            // records are held: the batch builder reads it out
                            // of `pending_range` as it chunks them. Resolved
                            // from the block the scan itself yielded (issue
                            // #2417), not from the cursor's position in
                            // `current_indices`.
                            match this.take_block_range(decoded_block) {
                                Ok(range) => this.pending_range = range,
                                Err(e) => return this.fail(e),
                            }
                            if let Err(e) = this.take_row_block(records) {
                                return this.fail(e);
                            }
                        }
                        // Only `None` ends the segment. Its counters are final
                        // now, so publish them before moving on.
                        Ok(None) => this.end_scanned_segment(),
                        Err(e) => return this.fail(SqlError::from(e).into()),
                    }
                }
                LogScanState::RowFallbackBlock { skip, .. } => {
                    // Drain and discard the blocks already fully emitted
                    // (columnar, or row-emitted by an earlier fallback in this
                    // same segment), then take exactly the next block -- the one
                    // that carried the `attrs_raw` overflow page -- through the
                    // row path.
                    let skip = *skip;
                    let (decoded_block, next) =
                        match this.next_row_block(RowResume::FallbackBlock { skip }) {
                            RowNext::Ready(block, next) => (block, next),
                            RowNext::Submitted => continue,
                            RowNext::Failed(e) => return this.fail(e),
                        };
                    if skip > 0 {
                        match next {
                            Ok(Some(_)) => {
                                if let LogScanState::RowFallbackBlock { skip, .. } = &mut this.state
                                {
                                    *skip -= 1;
                                }
                            }
                            Ok(None) => this.end_scanned_segment(),
                            Err(e) => return this.fail(SqlError::from(e).into()),
                        }
                        continue;
                    }
                    match next {
                        Ok(Some(records)) => {
                            // Stamp the block's row-ref address before the
                            // records are held: the batch builder reads it out
                            // of `pending_range` as it chunks them. Resolved
                            // from the block the scan itself yielded (issue
                            // #2417), not from the cursor's position in
                            // `current_indices`.
                            match this.take_block_range(decoded_block) {
                                Ok(range) => this.pending_range = range,
                                Err(e) => return this.fail(e),
                            }
                            if let Err(e) = this.take_row_block(records) {
                                return this.fail(e);
                            }
                            // Exactly one block goes through the row path per
                            // fallback (issue #1769): hand the still-open scan
                            // back to the columnar cursor for the blocks after
                            // it instead of falling the rest of the segment to
                            // rows -- UNLESS this segment has now hit
                            // `MAX_CONSECUTIVE_ATTR_RAW_FALLBACKS` fallbacks in
                            // a row with no clean columnar block between them
                            // (issue #1769 follow-up): the per-object `attrs_raw` budget
                            // that caused this block to overflow almost
                            // certainly causes the rest of this partition's
                            // block list to as well, so retrying columnar
                            // block by block would pay one reopen per
                            // remaining block for no columnar benefit. Commit
                            // the still-open scan straight to `Rows` instead:
                            // it already sits right after the block just
                            // emitted, so the remaining blocks drain through
                            // the row path with no further reopen, bounding
                            // this segment's total reopens to
                            // `MAX_CONSECUTIVE_ATTR_RAW_FALLBACKS + 1`.
                            let scan = match std::mem::replace(&mut this.state, LogScanState::Done)
                            {
                                LogScanState::RowFallbackBlock { scan, .. } => scan,
                                _ => unreachable!("state just matched as RowFallbackBlock"),
                            };
                            this.state = if this.consecutive_fallbacks
                                > MAX_CONSECUTIVE_ATTR_RAW_FALLBACKS
                            {
                                LogScanState::Rows(scan)
                            } else {
                                LogScanState::Columnar(scan)
                            };
                        }
                        // Cannot happen: the columnar cursor that triggered this
                        // fallback had already decoded this exact block, so a
                        // fresh cursor positioned at the same index (past the
                        // `skip` already discarded) must yield it too. Treated
                        // as end-of-segment rather than panicking, matching
                        // `ReopenRows`'s own `Ok(None)` handling above.
                        Ok(None) => this.end_scanned_segment(),
                        Err(e) => return this.fail(SqlError::from(e).into()),
                    }
                }
                LogScanState::DecodingColumnar { job, started } => {
                    let started = *started;
                    let ran = match job.as_mut().poll(cx) {
                        Poll::Ready(ran) => ran,
                        Poll::Pending => return Poll::Pending,
                    };
                    this.blocks.decode_build_elapsed.add_elapsed(started);
                    match ran {
                        Ok((scan, resident, step)) => {
                            this.resident = resident;
                            this.decoded = Some(Decoded::Columnar(step));
                            this.state = LogScanState::Columnar(scan);
                        }
                        Err(err) => {
                            let key = this
                                .current_seg
                                .as_ref()
                                .map_or("", |s| s.data_object_key.as_str());
                            let e = log_block_gate_failed(key, err);
                            return this.fail(e);
                        }
                    }
                }
                LogScanState::DecodingRows {
                    job,
                    resume,
                    started,
                } => {
                    let (resume, started) = (*resume, *started);
                    let (scan, block, next) = match job.as_mut().poll(cx) {
                        Poll::Ready(ran) => ran,
                        Poll::Pending => return Poll::Pending,
                    };
                    this.blocks.decode_build_elapsed.add_elapsed(started);
                    this.decoded = Some(Decoded::Rows { block, next });
                    this.state = match resume {
                        RowResume::Rows => LogScanState::Rows(scan),
                        RowResume::FallbackBlock { skip } => {
                            LogScanState::RowFallbackBlock { scan, skip }
                        }
                    };
                }
                LogScanState::Done => return Poll::Ready(None),
            }
        }
    }
}

impl RecordBatchStream for LogScanStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod fast_path_prefetch_tests {
    //! ADR-2414 decision A2: on the ranged whole-segment fast path a partition
    //! issues its next owned segments' opens while the current one opens and
    //! decodes, at most its share of the GET permits at once, and consumes
    //! them in owned order.

    use std::time::Duration;

    use super::*;
    use datafusion::arrow::array::TimestampNanosecondArray;
    use datafusion::physical_plan::ExecutionPlan;
    use ravel_catalog::SegmentLevel;
    use ravel_logseg::record::stream_attrs_bytes;
    use ravel_logseg::{ObjectIdentity, RlogConfig, RlogWriter};
    use ravel_object_store::fault::{FaultPlan, FaultStore, GateHandle, Occurrence, Op};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{ObjectStoreBackend, PutOptions};
    use ravel_query::BlockRangeFetcher;
    use ravel_types::accounting::QueryAccountingSnapshot;
    use ravel_types::logstream::log_stream_id;
    use uuid::Uuid;

    const TENANT: TenantHash = TenantHash([9u8; 16]);
    const BLOCKS: i64 = 4;
    const SEGMENTS: usize = 6;
    /// Segment `i` holds timestamps `i * SPAN .. i * SPAN + BLOCKS`.
    const SPAN: i64 = 100;

    /// Segment `seq`'s object: `BLOCKS` one-record blocks. With
    /// `overflow_last`, the last block carries a second attribute past a
    /// one-column dynamic budget, so it alone has an `attrs_raw` page and the
    /// columnar drain falls back to one row-path reopen on it.
    fn object(seq: usize, overflow_last: bool) -> Vec<u8> {
        let cfg = RlogConfig {
            block_target_records: 1,
            max_dynamic_columns: 1,
            ..RlogConfig::default()
        };
        let identity = ObjectIdentity {
            tenant_hash: TENANT.0,
            shard: 0,
            writer_id: [4u8; 16],
            writer_epoch: 1,
            writer_seq: seq as u64 + 1,
        };
        let resource = vec![("service.name".to_string(), AttrValue::Str("svc".into()))];
        let mut writer = RlogWriter::new(cfg, identity);
        for j in 0..BLOCKS {
            let ts = seq as i64 * SPAN + j;
            let mut attrs = vec![("a".to_string(), AttrValue::Str(format!("a{ts}")))];
            if overflow_last && j == BLOCKS - 1 {
                attrs.push(("b".to_string(), AttrValue::Str(format!("b{ts}"))));
            }
            writer
                .push(ravel_logseg::LogRecord {
                    stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
                    stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
                    ts_ns: ts,
                    observed_ts_ns: ts,
                    severity_num: 9,
                    severity_text: "INFO".into(),
                    body: format!("row {ts} {}", "payload ".repeat(16)),
                    trace_id: None,
                    span_id: None,
                    flags: 0,
                    attrs,
                })
                .expect("push");
        }
        writer.finish().expect("finish")
    }

    fn key(seq: usize) -> String {
        format!("t/pf{seq}.rlog")
    }

    /// `SEGMENTS` segments in one store, segment `overflow` (if any) built
    /// with an overflowing last block.
    async fn fixture(overflow: Option<usize>) -> (Arc<FaultStore<MemoryStore>>, Vec<SegmentRef>) {
        let store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
        let mut segments = Vec::new();
        for seq in 0..SEGMENTS {
            let obj = object(seq, overflow == Some(seq));
            store
                .put(
                    &key(seq),
                    bytes::Bytes::from(obj.clone()),
                    PutOptions::default(),
                )
                .await
                .expect("put");
            segments.push(SegmentRef {
                data_object_key: key(seq),
                object_size: obj.len() as u64,
                min_event_ts_ns: seq as i64 * SPAN,
                max_event_ts_ns: seq as i64 * SPAN + BLOCKS - 1,
                ingest_hour_bucket: 0,
                sample_count: BLOCKS as u64,
                series_count: 0,
                shard: 0,
                content_hash: [seq as u8 + 1; 32],
                writer_id: Uuid::from_u128(4),
                writer_epoch: 1,
                writer_seq: seq as u64 + 1,
                created_unix_ns: 0,
                level: SegmentLevel::L0,
                segment_format_version: u32::from(ravel_logseg::footer::VERSION),
                declared_column_stats: Default::default(),
            });
        }
        (store, segments)
    }

    /// A `ts`-only scan over `segments` at `partitions` partitions, whose
    /// fetcher has `permits` GET permits and routes every segment ranged.
    /// The window contains every segment and there is no predicate, so the
    /// whole-segment fast path runs; partition 0 owns segments 0, 2 and 4
    /// at two partitions.
    fn exec(
        store: &Arc<FaultStore<MemoryStore>>,
        segments: &[SegmentRef],
        partitions: usize,
        permits: usize,
        accounting: PhaseAccounting,
    ) -> LogsScanExec {
        exec_with(
            ranged_fetcher(store, permits),
            segments,
            partitions,
            accounting,
        )
    }

    /// The fetcher [`exec`] scans with: `permits` GET permits, every segment
    /// routed ranged.
    fn ranged_fetcher(store: &Arc<FaultStore<MemoryStore>>, permits: usize) -> LogSegmentFetcher {
        let store = Arc::clone(store) as Arc<dyn ObjectStoreBackend>;
        LogSegmentFetcher::new(Arc::clone(&store))
            .with_block_range(
                BlockRangeFetcher::new(store)
                    .with_suffix_len(256)
                    .with_whole_object_threshold(0),
            )
            .with_block_range_threshold(0)
            .with_max_concurrent_gets(permits)
    }

    /// [`exec`] over a caller-built fetcher.
    fn exec_with(
        fetcher: LogSegmentFetcher,
        segments: &[SegmentRef],
        partitions: usize,
        accounting: PhaseAccounting,
    ) -> LogsScanExec {
        LogsScanExec::new(
            TENANT,
            fetcher,
            segments,
            partitions,
            0,
            SEGMENTS as i64 * SPAN,
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
            Some(&vec![crate::logs_schema::LOG_COL_TS]),
            accounting,
            crate::logs_schema::logs_schema_with_declared(&[]),
            Arc::new(Vec::new()),
        )
        .expect("scan")
        .with_segment_timing(true)
    }

    fn timestamps(batches: &[RecordBatch]) -> Vec<i64> {
        batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .expect("ts column")
                    .values()
                    .to_vec()
            })
            .collect()
    }

    fn metric_total(plan: &dyn ExecutionPlan, name: &str) -> usize {
        plan.metrics()
            .expect("metrics")
            .iter()
            .filter(|m| m.value().name() == name && m.labels().is_empty())
            .map(|m| m.value().as_usize())
            .sum()
    }

    /// Whether segment `ordinal`'s open has resolved: its
    /// `seg_open_ready_offset` timeline point exists.
    fn opened(plan: &dyn ExecutionPlan, ordinal: usize) -> bool {
        let label = ordinal.to_string();
        plan.metrics().expect("metrics").iter().any(|m| {
            m.value().name() == "seg_open_ready_offset"
                && m.labels()
                    .iter()
                    .any(|l| l.name() == "segment" && l.value() == label)
        })
    }

    fn held_on(gate: &GateHandle, seq: usize) -> bool {
        let k = key(seq);
        gate.held_details().iter().any(|(_, _, held)| *held == k)
    }

    /// Yields to the partition task until segment `ordinal` has opened.
    async fn wait_opened(plan: &dyn ExecutionPlan, ordinal: usize) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !opened(plan, ordinal) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("segment {ordinal} never opened"));
    }

    /// Releases every held call until the task it is spawned beside ends.
    fn release_all(gate: GateHandle) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                gate.wait_until_held(1).await;
                for id in gate.held() {
                    gate.release(id);
                }
            }
        })
    }

    /// Partition 0's rows and scan-phase accounting with the pipeline off: a
    /// pushed fetch no row count reaches disables it, and stops nothing.
    async fn sequential(
        overflow: Option<usize>,
        permits: usize,
    ) -> (Vec<i64>, usize, QueryAccountingSnapshot) {
        let (store, segments) = fixture(overflow).await;
        let accounting = PhaseAccounting::new();
        let plan = exec(&store, &segments, 2, permits, accounting.clone())
            .with_fetch(Some(usize::MAX))
            .expect("fetch pushdown");
        let batches: Vec<RecordBatch> = plan
            .execute(0, Arc::new(TaskContext::default()))
            .expect("execute")
            .map(|b| b.expect("batch"))
            .collect()
            .await;
        (
            timestamps(&batches),
            metric_total(plan.as_ref(), "segments_opened"),
            accounting.scan().snapshot(),
        )
    }

    /// With the first segment's GETs held, the second segment opens
    /// completely and the third is not issued: two opens, the share, are
    /// held at once. Released, the rows, the segments opened and the
    /// scan-phase accounting equal the sequential walk's, rows first segment
    /// first.
    ///
    /// Fails against no prefetch (segment 2 never opens under the hold),
    /// against unbounded prefetch (segment 4's GET is held), and against
    /// emitting segments in completion order (segment 2, opened first, would
    /// lead the rows).
    async fn prefetches_within_the_share(partitions: usize, permits: usize) {
        let (store, segments) = fixture(None).await;
        let gate = store.hold(Op::Get, Some(key(0)), Occurrence::Always);
        store.hold(Op::Get, Some(key(4)), Occurrence::Always);
        let accounting = PhaseAccounting::new();
        let plan = Arc::new(exec(
            &store,
            &segments,
            partitions,
            permits,
            accounting.clone(),
        ));
        let stream = plan
            .execute(0, Arc::new(TaskContext::default()))
            .expect("execute");
        let task =
            tokio::spawn(
                async move { stream.map(|b| b.expect("batch")).collect::<Vec<_>>().await },
            );

        gate.wait_until_held(1).await;
        assert!(held_on(&gate, 0), "segment 0's first GET is held");
        wait_opened(plan.as_ref(), 2).await;
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        assert!(held_on(&gate, 0), "segment 0 is still held");
        assert!(
            !held_on(&gate, 4) && !opened(plan.as_ref(), 4),
            "segment 4 is not issued while segments 0 and 2 hold the share"
        );

        let releaser = release_all(gate);
        let batches = task.await.expect("partition task");
        releaser.abort();

        let (rows, opened_segments, scan) = sequential(None, permits).await;
        assert_eq!(
            rows,
            vec![0, 1, 2, 3, 200, 201, 202, 203, 400, 401, 402, 403],
            "the sequential walk's rows, segment 0 first"
        );
        assert_eq!(timestamps(&batches), rows);
        assert_eq!(metric_total(plan.as_ref(), "segments_opened"), 3);
        assert_eq!(opened_segments, 3);
        let piped = accounting.scan().snapshot();
        assert_eq!(piped.data_objects_touched, 3);
        assert_eq!(piped.logs_ranged_opens, 3);
        assert_eq!(
            piped, scan,
            "scan-phase accounting equals the sequential walk's"
        );
    }

    #[tokio::test]
    async fn a_partition_prefetches_its_next_segments_ranges() {
        // Four permits at two partitions: a share of 2, with permits to spare
        // so the limiter cannot be what stops segment 4.
        assert_eq!(fast_path_prefetch_share(4, 2), 2);
        prefetches_within_the_share(2, 4).await;
    }

    /// Two permits at two partitions is a share of 1, floored at 2: the next
    /// segment still opens under the hold. Fails against a share without the
    /// floor, where segment 2 never opens.
    #[tokio::test]
    async fn the_share_is_at_least_two() {
        assert_eq!(fast_path_prefetch_share(2, 2), 2);
        prefetches_within_the_share(2, 2).await;
    }

    /// With the second segment's GETs held, every row of the first is emitted:
    /// its decode is not waiting on the prefetch.
    ///
    /// Fails against an implementation that waits for the prefetches before
    /// draining the current segment (the first batch never arrives).
    #[tokio::test]
    async fn the_current_segment_drains_while_the_next_is_held() {
        let (store, segments) = fixture(None).await;
        let gate = store.hold(Op::Get, Some(key(2)), Occurrence::Always);
        let plan = exec(&store, &segments, 2, 4, PhaseAccounting::new());
        let mut stream = plan
            .execute(0, Arc::new(TaskContext::default()))
            .expect("execute");
        let mut rows = Vec::new();
        while rows.len() < BLOCKS as usize {
            let batch = tokio::time::timeout(Duration::from_secs(10), stream.next())
                .await
                .expect("segment 0 drains under the hold")
                .expect("a batch")
                .expect("batch");
            rows.extend(timestamps(&[batch]));
        }
        assert_eq!(rows, vec![0, 1, 2, 3]);
        assert!(held_on(&gate, 2), "segment 2's GET is still held");

        let releaser = release_all(gate);
        let rest: Vec<RecordBatch> = stream.map(|b| b.expect("batch")).collect().await;
        releaser.abort();
        assert_eq!(
            timestamps(&rest),
            vec![200, 201, 202, 203, 400, 401, 402, 403]
        );
    }

    /// The second owned segment falls back to the row path for its last block
    /// while the third's open is in flight behind it: the reopen is the one
    /// sequential open, the third segment's prefetch is consumed after it, and
    /// every row comes once, in order.
    ///
    /// Fails against a reopen that drops the in-flight prefetches (segment 4's
    /// rows are missing) and against counting the reopened segment again
    /// (`segments_opened` reads 4).
    #[tokio::test]
    async fn a_fallback_reopen_drains_the_prefetches_behind_it() {
        let (store, segments) = fixture(Some(2)).await;
        let gate = store.hold(Op::Get, Some(key(4)), Occurrence::Always);
        let plan = exec(&store, &segments, 2, 4, PhaseAccounting::new());
        let mut stream = plan
            .execute(0, Arc::new(TaskContext::default()))
            .expect("execute");
        let mut rows = Vec::new();
        while rows.len() < 2 * BLOCKS as usize {
            let batch = tokio::time::timeout(Duration::from_secs(10), stream.next())
                .await
                .expect("segments 0 and 2 drain under the hold")
                .expect("a batch")
                .expect("batch");
            rows.extend(timestamps(&[batch]));
        }
        assert!(
            held_on(&gate, 4),
            "segment 4's open was issued before segment 2 finished"
        );
        assert_eq!(
            metric_total(&plan, "reopens"),
            1,
            "one reopen, on segment 2"
        );

        let releaser = release_all(gate);
        let rest: Vec<RecordBatch> = stream.map(|b| b.expect("batch")).collect().await;
        releaser.abort();
        rows.extend(timestamps(&rest));
        assert_eq!(
            rows,
            vec![0, 1, 2, 3, 200, 201, 202, 203, 400, 401, 402, 403]
        );
        assert_eq!(metric_total(&plan, "reopens"), 1);
        assert_eq!(metric_total(&plan, "segments_opened"), 3);
        let (sequential_rows, _, _) = sequential(Some(2), 4).await;
        assert_eq!(rows, sequential_rows);
    }

    /// A prefetched open's error surfaces at that segment's turn, after the
    /// segments before it have emitted every row.
    #[tokio::test]
    async fn a_prefetch_error_surfaces_at_its_own_segment() {
        let (store, mut segments) = fixture(None).await;
        segments[2].segment_format_version = u32::MAX;
        let plan = exec(&store, &segments, 2, 4, PhaseAccounting::new());
        let mut stream = plan
            .execute(0, Arc::new(TaskContext::default()))
            .expect("execute");
        let mut rows = Vec::new();
        let err = loop {
            match stream.next().await.expect("an item before the end") {
                Ok(batch) => rows.extend(timestamps(&[batch])),
                Err(e) => break e,
            }
        };
        assert_eq!(rows, vec![0, 1, 2, 3], "segment 0 is emitted whole first");
        assert!(
            err.to_string().contains(&key(2)),
            "the error names segment 2: {err}"
        );
    }

    /// What partition 0 of a two-partition exec produced, executed alone,
    /// under a fetch memory budget of `limit` bytes: its rows,
    /// `segments_opened`, `fast_path_ranged_segments`,
    /// `prefetch_memory_reopens`, and the scan phase's data objects touched,
    /// or the error the stream ended with. `pipelined: false` turns the
    /// pipeline off the way [`sequential`] does. Every reservation is checked
    /// released before an error is returned.
    async fn budgeted(
        limit: u64,
        pipelined: bool,
    ) -> Result<(Vec<i64>, usize, usize, usize, u64), DataFusionError> {
        let (store, segments) = fixture(None).await;
        let accounting = PhaseAccounting::new();
        let budget = Arc::new(ravel_memory::MemoryBudget::new(limit));
        let fetcher = ranged_fetcher(&store, 4).with_memory_budget(Arc::clone(&budget));
        let exec = Arc::new(exec_with(fetcher, &segments, 2, accounting.clone()));
        let plan: Arc<dyn ExecutionPlan> = if pipelined {
            exec
        } else {
            exec.with_fetch(Some(usize::MAX)).expect("fetch pushdown")
        };
        let mut stream = plan
            .execute(0, Arc::new(TaskContext::default()))
            .expect("execute");
        let mut rows = Vec::new();
        let drained = tokio::time::timeout(Duration::from_secs(10), async {
            while let Some(item) = stream.next().await {
                rows.extend(timestamps(&[item?]));
            }
            Ok::<(), DataFusionError>(())
        })
        .await
        .expect("the scan ends rather than retrying a refused open forever");
        drop(stream);
        assert_eq!(
            budget.reserved(),
            0,
            "every fetch reservation is released, on success and on error"
        );
        drained?;
        Ok((
            rows,
            metric_total(plan.as_ref(), "segments_opened"),
            metric_total(plan.as_ref(), "fast_path_ranged_segments"),
            metric_total(plan.as_ref(), "prefetch_memory_reopens"),
            accounting.scan().snapshot().data_objects_touched,
        ))
    }

    /// One partition, partition 0 of a two-partition exec executed alone: a
    /// ranged prefetch the fetch memory budget refuses does not fail a query
    /// the sequential walk runs. At the smallest budget the sequential walk
    /// completes in, the pipelined walk (share 2) is refused its second open,
    /// turns the pipeline off and retries the refused segment once at its
    /// turn, and returns the sequential walk's rows, segments opened,
    /// ranged-route count and data objects touched, with the retry counted
    /// exactly once. One byte below that budget the retry is refused too, and
    /// the query fails with the typed refusal, every reservation released.
    ///
    /// Fails against failing the query on the prefetch's refusal (the first
    /// pipelined run errors) and against retrying every refusal (the run one
    /// byte short never ends, and the timeout fires).
    #[tokio::test]
    async fn a_prefetch_refused_by_the_memory_budget_reopens_sequentially() {
        assert_eq!(fast_path_prefetch_share(4, 2), 2);
        let ceiling: u64 = 1 << 24;
        assert!(
            budgeted(ceiling, false).await.is_ok(),
            "the ceiling fits the sequential walk"
        );
        // The smallest budget the sequential walk completes in.
        let (mut lo, mut hi) = (0u64, ceiling);
        while hi - lo > 1 {
            let mid = lo + (hi - lo) / 2;
            if budgeted(mid, false).await.is_ok() {
                hi = mid;
            } else {
                lo = mid;
            }
        }
        let fits = hi;

        let unlimited = budgeted(ceiling, true)
            .await
            .expect("pipelined, no pressure");
        assert_eq!(unlimited.3, 0, "no refusal, no fallback reopen");

        let sequential = budgeted(fits, false).await.expect("sequential fits");
        let pipelined = budgeted(fits, true)
            .await
            .expect("the pipelined walk falls back instead of failing");
        assert_eq!(
            sequential.0,
            (0..BLOCKS)
                .chain(200..200 + BLOCKS)
                .chain(400..400 + BLOCKS)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            (&pipelined.0, pipelined.1, pipelined.2, pipelined.4),
            (&sequential.0, sequential.1, sequential.2, sequential.4),
            "rows, segments opened, ranged opens and data objects touched match \
             the sequential walk"
        );
        assert_eq!((sequential.1, sequential.2, sequential.4), (3, 3, 3));
        assert_eq!(
            (sequential.3, pipelined.3),
            (0, 1),
            "the fallback is counted once, on the pipelined run only"
        );

        for pipelined in [false, true] {
            let err = budgeted(fits - 1, pipelined)
                .await
                .expect_err("one byte short refuses the sequential open too");
            assert!(
                is_fetch_memory_refusal(&err),
                "the typed refusal surfaces (pipelined {pipelined}): {err}"
            );
        }
    }

    /// The largest budget [`fits_with`] bisects from; every run fits in it.
    const CEILING: u64 = 1 << 24;

    /// How [`run_statement`] drives the partitions of one exec.
    #[derive(Clone)]
    enum Drive {
        /// One item from each live partition in turn, partition 0 first.
        Lockstep,
        /// One spawned task per partition.
        Spawned,
        /// Each listed partition to its end, in the order given.
        Order(Vec<usize>),
    }

    /// What every partition of one statement produced.
    struct Statement {
        /// Each partition's timestamps, in emitted order.
        rows: Vec<Vec<i64>>,
        plan: Arc<dyn ExecutionPlan>,
        scan: QueryAccountingSnapshot,
    }

    impl Statement {
        fn metric(&self, name: &str) -> usize {
            metric_total(self.plan.as_ref(), name)
        }
    }

    /// The fixture's rows for `segments`, in that order.
    fn rows_of(segments: &[usize]) -> Vec<i64> {
        segments
            .iter()
            .flat_map(|&s| (0..BLOCKS).map(move |j| s as i64 * SPAN + j))
            .collect()
    }

    /// Executes every partition of ONE exec over the fixture (segment
    /// `overflow`, if any, falling back to the `attrs_raw` reopen), at
    /// `partitions` partitions and 4 GET permits, under one fetch memory
    /// budget of `limit` bytes, driven by `drive`. Every stream is dropped
    /// and the budget's reserved bytes are asserted 0 before any error is
    /// returned. `pipelined: false` turns the pipeline off the way
    /// [`sequential`] does.
    async fn run_statement(
        limit: u64,
        partitions: usize,
        overflow: Option<usize>,
        pipelined: bool,
        drive: Drive,
    ) -> Result<Statement, DataFusionError> {
        let (store, segments) = fixture(overflow).await;
        let accounting = PhaseAccounting::new();
        let budget = Arc::new(ravel_memory::MemoryBudget::new(limit));
        let fetcher = ranged_fetcher(&store, 4).with_memory_budget(Arc::clone(&budget));
        let exec = Arc::new(exec_with(
            fetcher,
            &segments,
            partitions,
            accounting.clone(),
        ));
        let plan: Arc<dyn ExecutionPlan> = if pipelined {
            exec
        } else {
            exec.with_fetch(Some(usize::MAX)).expect("fetch pushdown")
        };
        let streams: Vec<SendableRecordBatchStream> = (0..partitions)
            .map(|p| {
                plan.execute(p, Arc::new(TaskContext::default()))
                    .expect("execute")
            })
            .collect();
        let mut rows = vec![Vec::new(); partitions];
        let outcome = tokio::time::timeout(
            Duration::from_secs(10),
            drive_streams(streams, drive, &mut rows),
        )
        .await
        .expect("the statement ends rather than waiting or retrying forever");
        assert_eq!(
            budget.reserved(),
            0,
            "every stream dropped releases every fetch reservation"
        );
        outcome?;
        Ok(Statement {
            rows,
            plan,
            scan: accounting.scan().snapshot(),
        })
    }

    /// Drives `streams` (partition `p` at index `p`) to their ends, or to the
    /// first error, collecting each partition's timestamps into `rows`. Every
    /// stream is dropped before this returns.
    async fn drive_streams(
        streams: Vec<SendableRecordBatchStream>,
        drive: Drive,
        rows: &mut [Vec<i64>],
    ) -> Result<(), DataFusionError> {
        match drive {
            Drive::Lockstep => {
                let mut live: Vec<Option<SendableRecordBatchStream>> =
                    streams.into_iter().map(Some).collect();
                while live.iter().any(Option::is_some) {
                    for (p, slot) in live.iter_mut().enumerate() {
                        let Some(stream) = slot.as_mut() else {
                            continue;
                        };
                        match stream.next().await {
                            Some(Ok(batch)) => rows[p].extend(timestamps(&[batch])),
                            Some(Err(e)) => return Err(e),
                            None => *slot = None,
                        }
                    }
                }
                Ok(())
            }
            Drive::Spawned => {
                let tasks: Vec<_> = streams
                    .into_iter()
                    .map(|mut stream| {
                        tokio::spawn(async move {
                            let mut out = Vec::new();
                            while let Some(item) = stream.next().await {
                                out.extend(timestamps(&[item?]));
                            }
                            Ok::<_, DataFusionError>(out)
                        })
                    })
                    .collect();
                let mut first_error = None;
                for (p, task) in tasks.into_iter().enumerate() {
                    match task.await.expect("partition task") {
                        Ok(out) => rows[p] = out,
                        Err(e) => {
                            first_error.get_or_insert(e);
                        }
                    }
                }
                first_error.map_or(Ok(()), Err)
            }
            Drive::Order(order) => {
                let mut live: Vec<Option<SendableRecordBatchStream>> =
                    streams.into_iter().map(Some).collect();
                for p in order {
                    let Some(mut stream) = live[p].take() else {
                        continue;
                    };
                    while let Some(item) = stream.next().await {
                        rows[p].extend(timestamps(&[item?]));
                    }
                }
                Ok(())
            }
        }
    }

    /// The smallest budget at which `partitions` partitions with the
    /// pipeline off, driven by `drive`, complete.
    async fn fits_with(partitions: usize, overflow: Option<usize>, drive: Drive) -> u64 {
        assert!(
            run_statement(CEILING, partitions, overflow, false, drive.clone())
                .await
                .is_ok(),
            "the ceiling fits"
        );
        let (mut lo, mut hi) = (0u64, CEILING);
        while hi - lo > 1 {
            let mid = lo + (hi - lo) / 2;
            if run_statement(mid, partitions, overflow, false, drive.clone())
                .await
                .is_ok()
            {
                hi = mid;
            } else {
                lo = mid;
            }
        }
        hi
    }

    /// Whether partition `p`'s slot holds a prefetch that resolved with an
    /// open scan.
    fn holds_open_prefetch(exec: &LogsScanExec, p: usize) -> bool {
        exec.prefetch_pool
            .slot(p)
            .expect("slot")
            .prefetched
            .iter()
            .any(|f| matches!(&f.open, PrefetchOpen::Ready { opened, .. } if matches!(**opened, Ok(Some(_)))))
    }

    /// What [`reopen_refused`] observed.
    struct ReopenRun {
        rows: [Vec<i64>; 2],
        exec: Arc<LogsScanExec>,
        /// Whether partition 1 held its current open and an open prefetch
        /// while partition 0's `attrs_raw` reopen GET was held.
        sibling_held_both: bool,
        /// Calls the hold gate held for the reopen, counted when partition 0
        /// was parked on it.
        reopen_held: usize,
        /// How the statement ended, observed after every stream was dropped
        /// and the reserved bytes checked.
        outcome: Result<(), DataFusionError>,
    }

    /// The overflow fixture (segment 0's last block falls back to the
    /// `attrs_raw` reopen) at two partitions under a budget of `limit`, in a
    /// forced order: partition 0 drains segment 0's three clean blocks and
    /// parks on its reopen's first GET, held by a gate on segment 0's key;
    /// only then is partition 1 polled until it has emitted its first batch,
    /// which leaves it holding segment 1's open and segment 3's prefetch;
    /// then the gate releases everything and partition 0, then partition 1,
    /// run to their ends. Every stream is dropped and reserved bytes are
    /// asserted 0 before the outcome is returned.
    async fn reopen_refused(limit: u64) -> ReopenRun {
        let (store, segments) = fixture(Some(0)).await;
        let budget = Arc::new(ravel_memory::MemoryBudget::new(limit));
        let fetcher = ranged_fetcher(&store, 4).with_memory_budget(Arc::clone(&budget));
        let exec = Arc::new(exec_with(fetcher, &segments, 2, PhaseAccounting::new()));
        let gate = store.hold(Op::Get, Some(key(0)), Occurrence::Always);
        let mut s0 = exec
            .execute(0, Arc::new(TaskContext::default()))
            .expect("execute 0");
        let mut s1 = exec
            .execute(1, Arc::new(TaskContext::default()))
            .expect("execute 1");
        let mut rows = [Vec::new(), Vec::new()];
        let mut sibling_held_both = false;
        let mut reopen_held = 0;
        let outcome = tokio::time::timeout(Duration::from_secs(10), async {
            reopen_held = loop {
                match futures::poll!(s0.next()) {
                    Poll::Ready(Some(item)) => rows[0].extend(timestamps(&[item?])),
                    Poll::Ready(None) => {
                        return Err(DataFusionError::Internal(
                            "partition 0 ended before its reopen".into(),
                        ));
                    }
                    Poll::Pending => {
                        if rows[0].len() == BLOCKS as usize - 1 && held_on(&gate, 0) {
                            break gate.held_count();
                        }
                        for id in gate.held() {
                            gate.release(id);
                        }
                        tokio::task::yield_now().await;
                    }
                }
            };
            loop {
                match futures::poll!(s1.next()) {
                    Poll::Ready(Some(item)) => {
                        rows[1].extend(timestamps(&[item?]));
                        break;
                    }
                    Poll::Ready(None) => {
                        return Err(DataFusionError::Internal(
                            "partition 1 ended before its first batch".into(),
                        ));
                    }
                    Poll::Pending => tokio::task::yield_now().await,
                }
            }
            sibling_held_both = holds_open_prefetch(&exec, 1);
            let releaser = release_all(gate.clone());
            let drained = async {
                while let Some(item) = s0.next().await {
                    rows[0].extend(timestamps(&[item?]));
                }
                while let Some(item) = s1.next().await {
                    rows[1].extend(timestamps(&[item?]));
                }
                Ok::<(), DataFusionError>(())
            }
            .await;
            releaser.abort();
            drained
        })
        .await
        .expect("the statement ends rather than waiting or retrying forever");
        drop(s0);
        drop(s1);
        assert_eq!(budget.reserved(), 0, "every fetch reservation is released");
        ReopenRun {
            rows,
            exec,
            sibling_held_both,
            reopen_held,
            outcome,
        }
    }

    /// `name`'s value on partition `p` alone.
    fn partition_metric(plan: &dyn ExecutionPlan, name: &str, p: usize) -> usize {
        plan.metrics()
            .expect("metrics")
            .iter()
            .filter(|m| {
                m.value().name() == name && m.labels().is_empty() && m.partition() == Some(p)
            })
            .map(|m| m.value().as_usize())
            .sum()
    }

    /// The smallest budget the one-partition pipeline-off run completes in.
    async fn fits_1() -> u64 {
        fits_with(1, None, Drive::Order(vec![0])).await
    }

    /// The smallest budget the two-partition pipeline-off run, driven in
    /// lockstep, completes in.
    async fn fits_2(overflow: Option<usize>) -> u64 {
        fits_with(2, overflow, Drive::Lockstep).await
    }

    /// At two partitions in lockstep and the budget the pipeline-off run
    /// needs, partition 0 holds its current open and a prefetch when
    /// partition 1's first open is refused: the refusal drops partition 0's
    /// prefetch, hands its segment back, and partition 1's retry succeeds.
    /// Rows per partition, segments opened, the route split and data objects
    /// touched are the pipeline-off run's.
    ///
    /// Fails against the branch before the pool (partition 1 holds nothing
    /// behind its open and fails with no retry), against dropping only the
    /// refused partition's own prefetches (the retry is refused again),
    /// and against dropping without handing the segments back (segment 2's
    /// rows are missing).
    #[tokio::test]
    async fn a_refused_open_drops_every_partitions_prefetches_and_retries_once() {
        assert_eq!(fast_path_prefetch_share(4, 2), 2);
        let fits_1 = fits_1().await;
        let fits_2 = fits_2(None).await;
        assert!(fits_2 >= fits_1, "{fits_2} >= {fits_1}");
        let off = run_statement(fits_2, 2, None, false, Drive::Lockstep)
            .await
            .expect("the pipeline-off run fits");
        assert_eq!(off.rows, [rows_of(&[0, 2, 4]), rows_of(&[1, 3, 5])]);
        let on = run_statement(fits_2, 2, None, true, Drive::Lockstep)
            .await
            .expect("the pipelined run retries instead of failing");
        assert_eq!(on.rows, off.rows);
        assert_eq!(
            (
                on.metric("segments_opened"),
                on.metric("fast_path_ranged_segments"),
                on.scan.data_objects_touched
            ),
            (6, 6, 6)
        );
        let reopens = on.metric("prefetch_memory_reopens");
        assert!(
            (1..=2).contains(&reopens),
            "a refusal occurs and each partition retries at most once: {reopens}"
        );
        let revocations = on.metric("prefetch_revocations");
        assert!(
            revocations <= 2,
            "at most one prefetch per partition: {revocations}"
        );
        assert_eq!(off.metric("prefetch_memory_reopens"), 0);
    }

    /// Two partitions on spawned tasks at the two-partition pipeline-off
    /// budget both complete with the pipeline-off rows. One byte below the
    /// one-partition pipeline-off budget, which no interleaving of the two
    /// tasks completes in, the run ends with the typed refusal inside the
    /// timeout. Both runs release every reservation once their streams are
    /// dropped ([`run_statement`]), and so does dropping both streams after
    /// their first batch, while each slot holds an open prefetch and the
    /// plan is still alive.
    ///
    /// Fails against retrying every refusal (the run one byte short never
    /// ends and the timeout fires) and against a `Drop` that does not clear
    /// the stream's slot (the cancelled prefetches stay reserved).
    #[test]
    fn all_partitions_spawned_match_the_sequential_walk_below_the_pipelined_peak() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("runtime");
        // The body runs on a worker so the timeout below still fires if a
        // refused open retried forever inside one poll; the shutdown then
        // abandons that worker instead of waiting for it.
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            rt.block_on(async {
                let body = tokio::spawn(spawned_partitions());
                match tokio::time::timeout(Duration::from_secs(60), body).await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
                    Ok(Err(e)) => panic!("test body task: {e}"),
                    Err(_) => panic!("the test body never ended"),
                }
            });
        }));
        rt.shutdown_timeout(Duration::from_secs(1));
        if let Err(panic) = outcome {
            std::panic::resume_unwind(panic);
        }
    }

    /// The body of
    /// [`all_partitions_spawned_match_the_sequential_walk_below_the_pipelined_peak`].
    async fn spawned_partitions() {
        let fits_1 = fits_1().await;
        let fits_2 = fits_2(None).await;
        let off = run_statement(fits_2, 2, None, false, Drive::Lockstep)
            .await
            .expect("pipeline off fits");
        let spawned = run_statement(fits_2, 2, None, true, Drive::Spawned)
            .await
            .expect("both spawned partitions complete");
        let sorted = |rows: &[Vec<i64>]| {
            let mut all: Vec<i64> = rows.concat();
            all.sort_unstable();
            all
        };
        assert_eq!(sorted(&spawned.rows), sorted(&off.rows));
        assert_eq!(
            spawned.rows, off.rows,
            "each partition's own rows, in order"
        );
        assert_eq!(spawned.metric("segments_opened"), 6);

        let err = run_statement(fits_1 - 1, 2, None, true, Drive::Spawned)
            .await
            .err()
            .expect("one byte below the one-partition fit refuses");
        assert!(is_fetch_memory_refusal(&err), "typed refusal: {err}");

        let (store, segments) = fixture(None).await;
        let budget = Arc::new(ravel_memory::MemoryBudget::new(CEILING));
        let fetcher = ranged_fetcher(&store, 4).with_memory_budget(Arc::clone(&budget));
        let exec = exec_with(fetcher, &segments, 2, PhaseAccounting::new());
        let mut streams: Vec<SendableRecordBatchStream> = (0..2)
            .map(|p| {
                exec.execute(p, Arc::new(TaskContext::default()))
                    .expect("execute")
            })
            .collect();
        for stream in &mut streams {
            stream
                .next()
                .await
                .expect("a first batch")
                .expect("first batch");
        }
        assert!(
            holds_open_prefetch(&exec, 0) && holds_open_prefetch(&exec, 1),
            "each slot holds an open prefetch when the streams are dropped"
        );
        drop(streams);
        assert_eq!(
            budget.reserved(),
            0,
            "dropped streams release their prefetches with the plan still alive"
        );
    }

    /// Partition 1 alone is polled until it holds its current open and a
    /// prefetch, at the two-partition pipeline-off budget. Then only
    /// partition 0 is polled: its first open is refused, it drops partition
    /// 1's prefetch from its own task, retries and finishes segments 0, 2
    /// and 4 with partition 1 never polled in between. Partition 1 then
    /// reopens segment 3 from the segments handed back to it.
    ///
    /// Fails against any design in which the refused open waits for
    /// partition 1 to be polled (the timeout fires), against dropping only
    /// partition 0's own prefetches (its retry is refused again), and
    /// against handing segment 3 back into partition 0's work (partition 0's
    /// rows gain segment 3, partition 1's lose it).
    #[tokio::test]
    async fn the_refused_partition_completes_without_its_sibling_being_polled() {
        let fits_2 = fits_2(None).await;
        let off = run_statement(fits_2, 2, None, false, Drive::Lockstep)
            .await
            .expect("pipeline off fits");
        let (store, segments) = fixture(None).await;
        let budget = Arc::new(ravel_memory::MemoryBudget::new(fits_2));
        let fetcher = ranged_fetcher(&store, 4).with_memory_budget(Arc::clone(&budget));
        let exec = exec_with(fetcher, &segments, 2, PhaseAccounting::new());
        let mut s0 = exec
            .execute(0, Arc::new(TaskContext::default()))
            .expect("execute 0");
        let mut s1 = exec
            .execute(1, Arc::new(TaskContext::default()))
            .expect("execute 1");

        let mut rows1 = timestamps(&[s1
            .next()
            .await
            .expect("partition 1's first batch")
            .expect("batch")]);
        assert!(
            holds_open_prefetch(&exec, 1),
            "partition 1 holds segment 3's prefetch"
        );

        let rows0 = tokio::time::timeout(Duration::from_secs(10), async {
            let mut rows = Vec::new();
            while let Some(item) = s0.next().await {
                rows.extend(timestamps(&[item.expect("partition 0 completes")]));
            }
            rows
        })
        .await
        .expect("partition 0 completes without partition 1 being polled");
        assert_eq!(rows0, off.rows[0]);
        assert_eq!(partition_metric(&exec, "prefetch_memory_reopens", 0), 1);
        assert_eq!(partition_metric(&exec, "prefetch_revocations", 0), 1);
        drop(s0);
        assert!(
            budget.reserved() > 0,
            "partition 1's current open is still held"
        );

        while let Some(item) = s1.next().await {
            rows1.extend(timestamps(&[item.expect("partition 1 completes")]));
        }
        assert_eq!(rows1, off.rows[1], "segment 3 is reopened from handed_back");
        drop(s1);
        assert_eq!(budget.reserved(), 0);
    }

    /// One partition owning all six segments, share 4, at the budget the
    /// pipeline-off run needs: segment 0 opens, the three prefetches behind
    /// it are refused, and the first of them is consumed refused at its
    /// turn, which drops the other two and retries it once. The dropped
    /// segments go back to work in owned order, so rows come segment 0 to
    /// segment 5, in order.
    ///
    /// Fails against popping from the front in the revoker's drain or in the
    /// owner's hand-back drain (segments 2 and 3 swap), against dropping
    /// without handing back (segments 2 and 3 are missing), against leaving
    /// the pipeline on (the retried segment's prefetches are refused and
    /// retried again, two reopens), and against failing the query at the
    /// consumed prefetch's refusal.
    #[tokio::test]
    async fn released_prefetches_go_back_to_work_in_owned_order() {
        assert_eq!(fast_path_prefetch_share(4, 1), 4);
        let fits_1 = fits_1().await;
        let run = run_statement(fits_1, 1, None, true, Drive::Order(vec![0]))
            .await
            .expect("the pipelined run retries instead of failing");
        assert_eq!(run.rows, [rows_of(&[0, 1, 2, 3, 4, 5])]);
        assert_eq!(run.metric("segments_opened"), 6);
        assert_eq!(run.metric("prefetch_memory_reopens"), 1);
        assert_eq!(
            run.metric("prefetch_revocations"),
            2,
            "segments 2 and 3's refused prefetches"
        );
    }

    /// The fixture variant whose segment 0 falls back to the `attrs_raw`
    /// reopen, in the order [`reopen_refused`] forces: partition 1 takes its
    /// current open and a prefetch while partition 0 is parked on its
    /// reopen's first GET, so the reopen is refused once released. The
    /// refusal drops both partitions' prefetches and retries the row-path
    /// reopen with the same `skip`; rows per partition are the pipeline-off
    /// run's, with no duplicate.
    ///
    /// The budget is the smallest at which partition 1 holds both opens at
    /// that point, bisected.
    ///
    /// Fails against the reopen's refusal still failing the query, against
    /// retrying it as a fast-path open (segment 0's first three rows come
    /// twice), against losing `skip` (the same), and against not handing
    /// segment 3 back (partition 1 loses its rows).
    #[tokio::test]
    async fn a_refused_attrs_raw_reopen_drops_the_prefetches_behind_it() {
        let fits_2 = fits_2(Some(0)).await;
        let off = run_statement(fits_2, 2, Some(0), false, Drive::Lockstep)
            .await
            .expect("pipeline off fits");
        assert_eq!(off.rows, [rows_of(&[0, 2, 4]), rows_of(&[1, 3, 5])]);
        assert_eq!(off.metric("reopens"), 1);

        // Whether partition 1 held both opens at the park point depends only
        // on what happened before it, not on how the run then ends.
        assert!(reopen_refused(CEILING).await.sibling_held_both);
        let (mut lo, mut hi) = (fits_2, CEILING);
        while hi - lo > 1 {
            let mid = lo + (hi - lo) / 2;
            if reopen_refused(mid).await.sibling_held_both {
                hi = mid;
            } else {
                lo = mid;
            }
        }
        let run = reopen_refused(hi).await;
        if let Err(e) = &run.outcome {
            panic!("the refused reopen is retried: {e}");
        }
        assert!(run.sibling_held_both);
        assert_eq!(
            run.reopen_held, 1,
            "the gate parked partition 0 on the reopen"
        );
        assert_eq!(run.rows.to_vec(), off.rows);
        let plan = run.exec.as_ref();
        assert_eq!(metric_total(plan, "reopens"), 1);
        assert_eq!(partition_metric(plan, "prefetch_memory_reopens", 0), 1);
        assert_eq!(metric_total(plan, "prefetch_memory_reopens"), 1);
        assert!(partition_metric(plan, "prefetch_revocations", 0) >= 1);
    }

    /// Two statements over clones of one fetcher share one budget, one
    /// partition each, share 4, at twice the one-partition pipeline-off
    /// budget. Statement 1 is driven to its first batch; statement 2's first
    /// open is then refused, its own pool holds nothing to drop, its one
    /// retry is refused too, and it ends with the typed refusal at once,
    /// leaving the budget where it found it. Statement 1 then completes.
    ///
    /// Fails against a refused open that waits for statement 1 (the timeout
    /// fires) and against retrying more than once (two reopens counted).
    #[tokio::test]
    async fn another_statements_prefetches_fail_an_open_fast_and_typed() {
        let limit = 2 * fits_1().await;
        let (store, segments) = fixture(None).await;
        let budget = Arc::new(ravel_memory::MemoryBudget::new(limit));
        let fetcher = ranged_fetcher(&store, 4).with_memory_budget(Arc::clone(&budget));
        let one = exec_with(fetcher.clone(), &segments, 1, PhaseAccounting::new());
        let two = exec_with(fetcher, &segments, 1, PhaseAccounting::new());
        let mut s1 = one
            .execute(0, Arc::new(TaskContext::default()))
            .expect("execute 1");
        let mut rows1 = timestamps(&[s1
            .next()
            .await
            .expect("statement 1's first batch")
            .expect("batch")]);
        let before = budget.reserved();
        assert!(before > 0, "statement 1 holds its opens");

        let mut s2 = two
            .execute(0, Arc::new(TaskContext::default()))
            .expect("execute 2");
        let ended = tokio::time::timeout(Duration::from_secs(10), async {
            while let Some(item) = s2.next().await {
                item?;
            }
            Ok::<(), DataFusionError>(())
        })
        .await
        .expect("statement 2 ends rather than waiting for statement 1");
        let err = ended.expect_err("statement 2 is refused twice");
        assert!(is_fetch_memory_refusal(&err), "typed refusal: {err}");
        assert_eq!(metric_total(&two, "prefetch_memory_reopens"), 1);
        drop(s2);
        assert_eq!(budget.reserved(), before);

        while let Some(item) = s1.next().await {
            rows1.extend(timestamps(&[item.expect("statement 1 completes")]));
        }
        assert_eq!(rows1, rows_of(&[0, 1, 2, 3, 4, 5]));
        drop(s1);
        assert_eq!(budget.reserved(), 0);
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod read_gate_tests {
    //! ADR-1702 task 8: with a read gate on the fetcher, the logs scan
    //! decodes each block as one `log_block` job. The placement test, that a
    //! parked block decode leaves the runtime free, is
    //! `tests/logs_scan_read_gate.rs`, in a binary of its own.

    use super::*;
    use datafusion::physical_plan::ExecutionPlan;
    use ravel_catalog::SegmentLevel;
    use ravel_cpu_gate::{CpuGateConfig, InstantClock};
    use ravel_logseg::record::stream_attrs_bytes;
    use ravel_logseg::{ObjectIdentity, RlogConfig, RlogWriter};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{ObjectStoreBackend, PutOptions};
    use ravel_types::logstream::log_stream_id;
    use uuid::Uuid;

    const TENANT: TenantHash = TenantHash([7u8; 16]);
    /// Records in the fixture's one object. The writer cuts a block per
    /// record (`block_target_records: 1`), so this is also its block count,
    /// and with no predicate and a window over every record each block
    /// survives pruning and is decoded exactly once.
    const BLOCKS: u64 = 4;
    const KEY: &str = "t/gate0.rlog";

    /// One object of [`BLOCKS`] one-record blocks, each carrying one
    /// attribute, so no block has an `attrs_raw` page and the columnar
    /// drain never falls back.
    fn object() -> Vec<u8> {
        let cfg = RlogConfig {
            block_target_records: 1,
            ..RlogConfig::default()
        };
        let identity = ObjectIdentity {
            tenant_hash: TENANT.0,
            shard: 0,
            writer_id: [5u8; 16],
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
                    body: format!("row {ts} {}", "payload ".repeat(16)),
                    trace_id: None,
                    span_id: None,
                    flags: 0,
                    attrs: vec![("a".to_string(), AttrValue::Str(format!("a{ts}")))],
                })
                .expect("push");
        }
        writer.finish().expect("finish")
    }

    async fn fixture() -> (Arc<dyn ObjectStoreBackend>, Vec<SegmentRef>) {
        fixture_at(KEY).await
    }

    /// The fixture stored under `key`.
    async fn fixture_at(key: &str) -> (Arc<dyn ObjectStoreBackend>, Vec<SegmentRef>) {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let obj = object();
        store
            .put(key, bytes::Bytes::from(obj.clone()), PutOptions::default())
            .await
            .expect("put");
        let segment = SegmentRef {
            data_object_key: key.to_string(),
            object_size: obj.len() as u64,
            min_event_ts_ns: 0,
            max_event_ts_ns: BLOCKS as i64 - 1,
            ingest_hour_bucket: 0,
            sample_count: BLOCKS,
            series_count: 0,
            shard: 0,
            content_hash: [3u8; 32],
            writer_id: Uuid::from_u128(5),
            writer_epoch: 1,
            writer_seq: 1,
            created_unix_ns: 0,
            level: SegmentLevel::L0,
            segment_format_version: u32::from(ravel_logseg::footer::VERSION),
            declared_column_stats: Default::default(),
        };
        (store, vec![segment])
    }

    /// A read gate whose byte floor is 0, so every block is a gate job.
    fn floor_zero_gate() -> Arc<ReadGate> {
        Arc::new(ReadGate::new(
            CpuGateConfig {
                permits: 2,
                inline_floor_bytes: 0,
                eval_floor_samples: 0,
            },
            Arc::new(InstantClock::new()),
        ))
    }

    fn site_counts(gate: &ReadGate, site: ReadSite) -> (u64, u64) {
        gate.snapshot()
            .sites
            .iter()
            .find(|counts| counts.site == site)
            .map_or((0, 0), |counts| (counts.jobs, counts.inline))
    }

    fn total_inline(gate: &ReadGate) -> u64 {
        gate.snapshot().sites.iter().map(|c| c.inline).sum()
    }

    /// `ts` alone is the columnar fast path; adding `attrs` makes the scan
    /// statically ineligible for it, so every block drains the row path.
    fn projection(rows: bool) -> Vec<usize> {
        if rows {
            vec![crate::logs_schema::LOG_COL_TS, LOG_COL_ATTRS]
        } else {
            vec![crate::logs_schema::LOG_COL_TS]
        }
    }

    /// A one-partition scan over the whole fixture window, with `gate` on
    /// the fetcher when given.
    fn exec(
        store: &Arc<dyn ObjectStoreBackend>,
        segments: &[SegmentRef],
        gate: Option<Arc<ReadGate>>,
        rows: bool,
    ) -> LogsScanExec {
        let fetcher = LogSegmentFetcher::new(Arc::clone(store));
        let fetcher = match gate {
            Some(gate) => fetcher.with_read_gate(gate),
            None => fetcher,
        };
        LogsScanExec::new(
            TENANT,
            fetcher,
            segments,
            1,
            0,
            BLOCKS as i64,
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
            Some(&projection(rows)),
            PhaseAccounting::new(),
            crate::logs_schema::logs_schema_with_declared(&[]),
            Arc::new(Vec::new()),
        )
        .expect("scan")
    }

    async fn collect(plan: &LogsScanExec) -> Vec<RecordBatch> {
        plan.execute(0, Arc::new(TaskContext::default()))
            .expect("execute")
            .map(|b| b.expect("batch"))
            .collect()
            .await
    }

    fn metric_total(plan: &dyn ExecutionPlan, name: &str) -> usize {
        plan.metrics()
            .expect("metrics")
            .iter()
            .filter(|m| m.value().name() == name && m.labels().is_empty())
            .map(|m| m.value().as_usize())
            .sum()
    }

    /// With the floor at 0, a scan moves the `log_block` job count by exactly
    /// the fixture's [`BLOCKS`], on the columnar path and on the row path, runs
    /// nothing inline, and returns what the same scan returns with no gate.
    ///
    /// Fails with the columnar path's `gate.run` wrap removed (the columnar
    /// case reads `(0, 0)`), and with the row path back on the inline
    /// `next_block` (the row case reads `(0, 0)`).
    #[tokio::test]
    async fn log_block_decodes_run_through_the_read_gate() {
        let (store, segments) = fixture().await;
        for rows in [false, true] {
            let gate = floor_zero_gate();
            let gated = exec(&store, &segments, Some(Arc::clone(&gate)), rows);
            let got = collect(&gated).await;
            assert_eq!(
                site_counts(&gate, ReadSite::LogBlock),
                (BLOCKS, 0),
                "one log_block job per fixture block (rows path: {rows})"
            );
            assert_eq!(total_inline(&gate), 0, "nothing ran inline (rows: {rows})");
            let path_batches = if rows {
                "rowpath_batches"
            } else {
                "columnar_batches"
            };
            assert!(
                metric_total(&gated, path_batches) > 0,
                "the scan took the path under test (rows: {rows})"
            );
            let want = collect(&exec(&store, &segments, None, rows)).await;
            assert_eq!(got, want, "gated and inline output agree (rows: {rows})");
        }
    }

    /// The late-materialization re-read of one block
    /// ([`RowFetchSource::fetch_block`]) is one `log_block` job per block, and
    /// returns the record the same re-read returns with no gate.
    ///
    /// Fails with `fetch_block` back on the inline `next_block`: the count
    /// reads `(0, 0)`.
    #[tokio::test]
    async fn row_ref_block_fetches_run_through_the_read_gate() {
        let (store, segments) = fixture().await;
        let gate = floor_zero_gate();
        let gated = exec(&store, &segments, Some(Arc::clone(&gate)), false).row_fetch_source();
        let inline = exec(&store, &segments, None, false).row_fetch_source();
        for block in 0..BLOCKS as usize {
            let got = gated
                .fetch_block(0, block, &[(0, block)])
                .await
                .expect("gated fetch");
            let want = inline
                .fetch_block(0, block, &[(0, block)])
                .await
                .expect("inline fetch");
            assert_eq!(got.len(), 1, "one record per one-record block");
            assert_eq!(got, want, "gated and inline re-reads agree (block {block})");
        }
        assert_eq!(site_counts(&gate, ReadSite::LogBlock), (BLOCKS, 0));
        assert_eq!(total_inline(&gate), 0, "nothing ran inline");
    }

    /// A columnar block job that panics on the gate fails the scan, as its
    /// first and only item, with the fetcher's corrupt error for the object:
    /// the class the fetcher's own gated block decode reports for a panic.
    ///
    /// Fails with `log_block_gate_failed` mapping `Panicked` to the transient
    /// store error (the match reads `Store`), and with the job's panic hook
    /// removed (the stream yields the fixture's four batches instead).
    #[tokio::test]
    async fn a_panicking_columnar_block_job_fails_the_scan_as_corrupt() {
        const PANIC_KEY: &str = "t/gate-panic.rlog";
        let (store, segments) = fixture_at(PANIC_KEY).await;
        let gate = floor_zero_gate();
        PANIC_NEXT_COLUMNAR_JOB
            .lock()
            .expect("panic hook")
            .push(PANIC_KEY.to_string());
        let gated = exec(&store, &segments, Some(Arc::clone(&gate)), false);
        let items: Vec<DFResult<RecordBatch>> = gated
            .execute(0, Arc::new(TaskContext::default()))
            .expect("execute")
            .collect()
            .await;
        assert_eq!(items.len(), 1, "the failure ends the stream");
        let Some(Err(DataFusionError::External(err))) = items.into_iter().next() else {
            panic!("the scan's only item is an external error");
        };
        match err.downcast_ref::<SqlError>() {
            Some(SqlError::LogFetch(LogFetchError::Corrupt {
                key,
                source: LogSegError::Corrupted(message),
            })) => {
                assert_eq!(key, PANIC_KEY);
                assert_eq!(
                    message,
                    &format!("read CPU gate: {}", CpuGateError::Panicked)
                );
            }
            other => panic!("expected the corrupt log fetch error, got {other:?}"),
        }
        assert_eq!(
            site_counts(&gate, ReadSite::LogBlock),
            (1, 0),
            "the first block's job took a permit and panicked"
        );
    }

    /// A columnar block job that never ran, cancelled or refused by a closed
    /// gate, maps to the transient store error the fetcher's `gate_not_run`
    /// builds for its own gated decodes: `StoreError::Transient` carrying
    /// "read CPU gate: " and the gate error. Checked on the mapping itself:
    /// the gate reports `Cancelled` only for a blocking task the runtime
    /// cancelled at shutdown, which leaves nothing to poll the scan, and
    /// `ReadGate` has no way to close its semaphore.
    ///
    /// Fails with either variant mapped to `Corrupt`.
    #[test]
    fn a_columnar_block_job_that_never_ran_is_transient() {
        for gate_err in [CpuGateError::Cancelled, CpuGateError::Closed] {
            let err = log_block_gate_failed(KEY, gate_err);
            let DataFusionError::External(err) = err else {
                panic!("an external error for {gate_err:?}");
            };
            match err.downcast_ref::<SqlError>() {
                Some(SqlError::LogFetch(LogFetchError::Store {
                    key,
                    source: StoreError::Transient(message),
                })) => {
                    assert_eq!(key, KEY);
                    assert_eq!(message, &format!("read CPU gate: {gate_err}"));
                }
                other => panic!("expected a transient store error, got {other:?}"),
            }
        }
    }
}
