//! The RSPAN side of the codec seam (ADR-0032 seam, ADR-0041 format):
//! L0-to-L1 span segment compaction.
//!
//! # What the merge does (docs/span-segment-format.md, ADR-0041)
//!
//! An L1 `.rspan` part is the sorted union of its inputs' span records,
//! re-blocked from scratch. Concretely the merge:
//!
//! - flat k-way merges every input's block stream in physical order (already
//!   sorted by `(trace_id, start_ts)`) through one [`BlockCursor`] per input,
//!   picking the minimum `(trace_id, start_ts_ns, input_index)` head at each
//!   step -- see the `# Memory` section below for why there is no per-trace
//!   outer loop the way RLOG has a per-stream one;
//! - pushes the merged records into a fresh [`RspanWriter`] and splits a part
//!   on a *trace boundary* once it reaches the split target (so a trace never
//!   straddles two parts, giving parts disjoint, ascending `trace_id` ranges
//!   -- the span analogue of RLOG's stream-boundary split, and the reason the
//!   target is a split point rather than a bound on the part's heap);
//! - lets the writer re-sort each part by `(trace_id, start_ts)` (a no-op on
//!   already-sorted input), re-chunk the blocks, and rebuild the
//!   interval-aware SKIP_IDX over the merged contents.
//!
//! # No `stream_ref`-equivalent remap (the RLOG departure)
//!
//! RLOG renumbers every input's local `stream_ref` values into one merged
//! STREAM_DIR and checks a cross-object stream-identity invariant, because an
//! RLOG record references its resource+scope identity indirectly through a
//! per-object stream directory. RSPAN has *no such indirection*: `trace_id` is
//! the direct sort key and every field of a [`SpanRecord`] (ids, timestamps,
//! status, the already-merged `attrs` map) is stored inline per row, not as a
//! reference into an object-local table (ADR-0041 sections 1 and 4;
//! `record.rs`: "no FIELD_DIR-style per-key column directory"). So the merge
//! needs no remap and no cross-object identity reconciliation -- a decoded span
//! from any input is already self-contained and re-encodes verbatim. This is a
//! genuine simplification over RLOG, not an omission.
//!
//! # Reuse, not reimplementation
//!
//! Every encode step (sort, block chunking, skip-index build, section framing,
//! footer/trailer) is exactly what [`RspanWriter`] already does for a
//! single-object L0 write. The merge does not re-derive any of it: it decodes
//! each input's blocks back to [`SpanRecord`]s through [`RspanRangeReader`],
//! pushes the merged records into a fresh `RspanWriter`, and calls
//! [`RspanWriter::finish_compacted`] to stamp `level = 1`, the compaction
//! `input_set_hash`, and the `part_index`. An L0 write and an L1 merge share the
//! one writer implementation and so cannot drift.
//!
//! # Memory (mirroring RLOG's k-way merge)
//!
//! The read side ([`SpanCodec::load_input_catalog`]) retains only per-input
//! catalog metadata: an [`RspanRangeReader`] holding the decoded SKIP_IDX plus
//! the object key and footer, never block bytes.
//!
//! The merge itself is a **k-way block-streaming merge**, so its decode-side
//! peak resident memory is bounded independently of any one trace's size, the
//! same fix RLOG got. Its writer-side residency is not: see the split rule
//! below. Unlike RLOG, there is no per-stream (here, per-trace) outer loop:
//! RSPAN's SKIP_IDX has no directory of the distinct `trace_id`s an
//! object holds (a block entry records only its own boundary
//! `min_trace_id`/`max_trace_id`, not every trace_id that starts inside it),
//! so "the next trace_id in this object" cannot be discovered without decoding
//! block contents first -- the point-lookup `trace_block_span`/`decode_trace`
//! API this crate already had for query pruning cannot drive a merge loop.
//!
//! Instead [`build_parts`] walks every input's blocks unconditionally, in
//! physical (already `(trace_id, start_ts)`-sorted) order, through one
//! [`BlockCursor`] per input ([`RspanRangeReader::block_count`] +
//! [`RspanRangeReader::block_loc`] + [`RspanRangeReader::decode_block`]),
//! fetching the next block's bytes by range only once the previous block is
//! drained. Because every input's own on-disk record stream is already sorted
//! by exactly the key the merged output needs, a flat k-way merge over all
//! inputs' cursors -- comparing `(trace_id, start_ts_ns, input_index)`, no
//! grouping by stream/trace first -- reproduces byte-for-byte the ordering the
//! old "gather everything then stable-sort by `(trace_id, start_ts)`" produced:
//! each input is already sorted on that key, so a stable sort of the
//! concatenation orders equal-key records by (input, position), exactly what
//! selecting the minimum `(trace_id, start_ts_ns, input_index)` head does here.
//! A part still splits only on a trace boundary (never mid-trace); since the
//! sort key puts `trace_id` first, a trace's records from every input are
//! contiguous in the merged stream, so the boundary check the old per-trace
//! loop made once per trace group becomes an inline check on each
//! `trace_id` transition in the flat loop. Merged records feed straight into
//! the in-progress part's [`RspanWriter`]; there is no intermediate
//! `Vec<SpanRecord>` batch or `by_trace` map.
//!
//! Peak resident memory is then: per-input catalog metadata (KBs per input),
//! plus at most one decoded block per input (`O(input_count * block_size)`,
//! independent of trace size), plus the in-progress part's writer buffer.
//!
//! That last term is where RSPAN differs from RLOG, and it is not a bound. The
//! split target `l1_part_memory_target_bytes` is compared against the part's
//! record heap ONLY at a `trace_id` transition, because a trace must not
//! straddle two parts, so the writer holds up to the target plus one whole
//! trace, and a single trace larger than the target is held in full however
//! large it grows. A host running span compaction has to survive the largest
//! single trace a tenant sends, not the configured target; `merge`'s
//! [`PartBuilder`] and
//! [`CompactorConfig::l1_part_memory_target_bytes`] say the same thing.
//! RSPAN carries only that one target; the stored-size
//! target the RLOG merge grew (issue #872) has no RSPAN counterpart yet. The
//! [`crate::config::MergeMemoryTracker`]
//! seam accounts these terms at their real allocation/decode points, the same
//! seam RLOG's merge feeds.
//!
//! RSPAN's [`SpanRecord`] has no first-class span links/events in v1 (see
//! `record.rs`'s module doc); both, if decoded at all, live as opaque blob
//! values inside `attrs`, which this merge already carries through verbatim
//! per record. No special handling beyond that pass-through is needed.
//!
//! # One merge, two callers (issue #742)
//!
//! [`merge`] is that k-way block-streaming merge, and both RSPAN writers of L1
//! parts run through it: compaction ([`SpanCodec::build_parts`]) keeping every
//! record, and the ADR-0064 selective-erasure rewrite
//! (`erasure_rewrite::build_rewrite_spans`) keeping the records no pending
//! request's matcher drops. This mirrors the RLOG split (issue #725): the
//! rewrite used to GET every input object whole and push every survivor into
//! one unbounded [`RspanWriter`], so its peak memory scaled with the bucket,
//! not one part. Sharing the driver gives it the same bound the compactor has
//! (one decoded block per input plus the in-progress part) and keeps the two
//! from drifting on merge order, trace-boundary split points, or the per-input
//! footer cross-check. Compaction keeps every record, so its byte output is
//! unchanged: `merge` with a keep-everything filter reproduces exactly the
//! part sequence the inline loop produced.

use std::future::Future;
use std::pin::Pin;

use futures::stream::{StreamExt, TryStreamExt, iter as stream_iter};
use ravel_commit::keys;
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_proto::commit::v1::CompactionPart;
use ravel_rspan::footer::{self, SuffixOutcome, kind};
use ravel_rspan::{
    BlockLoc, ObjectIdentity, RspanConfig, RspanRangeReader, RspanWriter, SpanRecord,
};

use crate::bucket::Bucket;
use crate::build::{BuiltPart, put_part};
use crate::codec::SegmentCodec;
use crate::config::{CompactorConfig, MergeMemoryTracker};
use crate::error::{MaintainError, Result};
use crate::read::InputRecord;

/// The RSPAN output trailer version every L1 part carries. Recorded in each
/// part's `CompactionPart.segment_format_version`, the span analogue of RSEG's
/// [`crate::build::OUTPUT_FORMAT_VERSION`] and RLOG's
/// [`crate::rlog::OUTPUT_FORMAT_VERSION`].
///
/// Read from `ravel_rspan`'s single-sourced supported-version window
/// (`SUPPORTED_VERSIONS.newest()`, ADR-0066 decision 1, slice A), the same
/// single source the reader gate and the CLI `audit-versions`/`migrate` paths
/// use. As a mirrored literal it went stale on the RSPAN v2 bump:
/// `finish_compacted` stamps the trailer from the writer while the literal
/// recorded 1, so the compactor wrote v2 parts that claimed to be v1; routing
/// through the window keeps the recorded number and the emitted trailer in step.
pub const OUTPUT_FORMAT_VERSION: u32 = ravel_rspan::footer::SUPPORTED_VERSIONS.newest() as u32;

/// One RSPAN input's retained catalog metadata: the data-object key, its
/// decoded footer, and an [`RspanRangeReader`] over its SKIP_IDX. Block bytes
/// are never retained here; the merge fetches them by range one block at a
/// time (see the module memory note). Retaining the footer lets a corrupt or
/// missing input fail loud at load time.
#[derive(Debug, Clone)]
pub struct SpanInputCatalog {
    pub object_key: String,
    pub footer: footer::SpanFooter,
    pub reader: RspanRangeReader,
}

/// The spans codec: implements the [`SegmentCodec`] seam for `.rspan` objects.
pub struct SpanCodec;

impl SegmentCodec for SpanCodec {
    type Catalog = SpanInputCatalog;

    async fn load_input_catalog(
        store: &dyn ObjectStoreBackend,
        config: &CompactorConfig,
        input: &InputRecord,
    ) -> Result<Self::Catalog> {
        let object_key = keys::reconstruct_data_key(&input.record)?;
        load_catalog_from_object(store, config, object_key).await
    }

    async fn build_parts(
        store: &dyn ObjectStoreBackend,
        config: &CompactorConfig,
        bucket: &Bucket,
        inputs: &[InputRecord],
        catalogs: Vec<Self::Catalog>,
        input_set_hash: &[u8; 32],
    ) -> Result<Vec<BuiltPart>> {
        if inputs.len() != catalogs.len() {
            return Err(MaintainError::Invariant(
                "inputs and catalogs length mismatch".to_string(),
            ));
        }
        // Compaction drops nothing, so every merged record is kept and the
        // counts the merge returns are the input count twice over; the
        // compaction-side conservation gate reads `part.sample_count`, not
        // these.
        let merged = merge(
            store,
            config,
            bucket,
            &catalogs,
            input_set_hash,
            config.dry_run,
            &mut |_| Ok(true),
        )
        .await?;
        Ok(merged.parts)
    }
}

/// The object-key-parametrized core of [`SpanCodec::load_input_catalog`],
/// generalized so a caller holding an object key from something other than an
/// L0 [`InputRecord`] can decode a catalog too. The erasure rewrite
/// (`erasure_rewrite::build_rewrite_spans`) is that caller: its live input set
/// is a list of whole-object keys (L0 data objects, or the parts of the live
/// compaction/rewrite record), never `InputRecord`s.
pub(crate) async fn load_catalog_from_object(
    store: &dyn ObjectStoreBackend,
    config: &CompactorConfig,
    object_key: String,
) -> Result<SpanInputCatalog> {
    // Locate and validate the footer from a suffix probe: one ranged GET,
    // growing to a second only if the probe missed the whole footer (the
    // RSPAN analogue of the RSEG/RLOG read path). This fails loud here on a
    // missing or corrupt input, before the merge fetches any block bytes.
    let probe = store
        .get(&object_key, GetRange::Suffix(config.footer_probe_bytes))
        .await?;
    let total = probe.total_size;
    let ftr = match footer::open_from_suffix(&probe.data, total)? {
        SuffixOutcome::Ready(f) => f,
        SuffixOutcome::NeedRange { offset, len } => {
            let tail = store
                .get(&object_key, GetRange::Range(offset, offset + len))
                .await?;
            match footer::open_from_suffix(&tail.data, total)? {
                SuffixOutcome::Ready(f) => f,
                SuffixOutcome::NeedRange { .. } => {
                    return Err(MaintainError::Invariant(
                        "rspan footer not covered by ranged fetch".into(),
                    ));
                }
            }
        }
    };
    // Fetch and decode the SKIP_IDX section by range. BLOCKS is never
    // fetched here; the merge streams blocks by range one at a time.
    let skip_desc = ftr
        .section(kind::SKIP_IDX)
        .ok_or_else(|| MaintainError::Invariant("rspan input missing SKIP_IDX section".into()))?;
    let skip_section = store
        .get(
            &object_key,
            GetRange::Range(skip_desc.offset, skip_desc.offset + skip_desc.len),
        )
        .await?;
    let skip_idx_raw = footer::decode_section(
        skip_section.data.as_ref(),
        skip_desc,
        footer::DEFAULT_MAX_SECTION_UNCOMP,
    )?;
    let reader = RspanRangeReader::from_sections(&ftr, &skip_idx_raw)?;

    Ok(SpanInputCatalog {
        object_key,
        footer: ftr,
        reader,
    })
}

/// One catalog per object key, `input_read_concurrency` loads in flight.
///
/// `buffered`, not `buffer_unordered`: the returned catalogs stay aligned
/// one-to-one with `object_keys` in canonical order, which is the k-way
/// merge's tie-break on equal `(trace_id, start_ts_ns)`.
pub(crate) async fn load_catalogs_by_key(
    store: &dyn ObjectStoreBackend,
    config: &CompactorConfig,
    object_keys: &[String],
) -> Result<Vec<SpanInputCatalog>> {
    let mut pending = Vec::with_capacity(object_keys.len());
    for object_key in object_keys {
        pending.push(load_catalog_from_object(store, config, object_key.clone()));
    }
    stream_iter(pending)
        .buffered(config.input_read_concurrency.max(1))
        .try_collect()
        .await
}

/// What one run of [`merge`] produced: the parts, and the record counts the
/// caller's conservation gates need.
pub(crate) struct SpanMergeOutput {
    /// Every part built, in part-index order.
    pub parts: Vec<BuiltPart>,
    /// Records the merge read out of the inputs, kept and dropped alike.
    pub input_record_count: u64,
    /// Records the filter kept, so the ones actually pushed into a part.
    pub output_record_count: u64,
}

/// The merge's running record tallies. Checked addition throughout: an
/// overflowing tally is itself an invariant breach, never a silent wrap that
/// could balance a caller's conservation gate against a wrong total.
#[derive(Default)]
struct RecordCounts {
    input: u64,
    output: u64,
}

impl RecordCounts {
    fn add_input(&mut self) -> Result<()> {
        self.input = self.input.checked_add(1).ok_or_else(|| {
            MaintainError::Invariant("merged input record count overflowed u64".to_string())
        })?;
        Ok(())
    }

    fn add_output(&mut self) -> Result<()> {
        self.output = self.output.checked_add(1).ok_or_else(|| {
            MaintainError::Invariant("merged output record count overflowed u64".to_string())
        })?;
        Ok(())
    }
}

/// The shared flat k-way block-streaming merge: every input's span records, in
/// global `(trace_id, start_ts_ns)` order, filtered through `keep`, partitioned
/// into parts on trace boundaries, at the split target
/// `l1_part_memory_target_bytes`.
///
/// The target is compared against the part's record heap only when the merged
/// stream moves to a new `trace_id`, so a part runs past it by up to one whole
/// trace and a single trace larger than the target is never split at all. It is
/// where parts split, not a ceiling on resident bytes
/// ([`CompactorConfig::l1_part_memory_target_bytes`]).
///
/// Both callers of the RSPAN merge run through here (issue #742). Compaction
/// ([`SpanCodec::build_parts`]) keeps every record; the ADR-0064 erasure
/// rewrite (`erasure_rewrite::build_rewrite_spans`) keeps the records no
/// pending request's matcher drops. Sharing the driver is what gives the
/// rewrite the same split rule the compactor has: resident memory is one
/// decoded block per input plus the in-progress part, never the bucket. It also
/// means the two cannot drift on merge order, on where a part splits, or on the
/// per-input footer cross-check ([`BlockCursor::refill`]).
///
/// `dry_run` decides whether each part is PUT as it closes: compaction PUTs
/// here, while the erasure rewrite defers every PUT to its own publish path,
/// which writes parts only after its conservation gate passes.
///
/// Each input is already sorted by `(trace_id, start_ts_ns)` (RSPAN's on-disk
/// order), so selecting the globally-minimum `(trace_id, start_ts_ns,
/// input_index)` head across all cursors reproduces byte-for-byte the ordering
/// the old "gather everything then stable-sort" produced (see the module memory
/// note). A part flushes on a `trace_id` transition once it reaches the split
/// target, so a trace never straddles two parts; since the sort key puts
/// `trace_id` first, all of one trace's records across every input are
/// contiguous here, so a plain `last_trace != current` check (inside
/// [`PartBuilder::push`]) counts distinct traces correctly without a set. A
/// record `keep` rejects is released here, so the erasure rewrite's peak memory
/// is bounded by its survivors' part, not by everything it read.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn merge(
    store: &dyn ObjectStoreBackend,
    config: &CompactorConfig,
    bucket: &Bucket,
    catalogs: &[SpanInputCatalog],
    input_set_hash: &[u8; 32],
    dry_run: bool,
    keep: &mut (dyn FnMut(&SpanRecord) -> Result<bool> + Send),
) -> Result<SpanMergeOutput> {
    // No whole-object fetch: the per-input ranged readers are already in the
    // catalogs. Blocks are fetched by range one at a time in the flat k-way
    // merge below, so raw resident bytes stay bounded to one block per input,
    // never a whole object or the whole bucket.
    let identity = compactor_identity(bucket, config);
    let tracker = config.merge_memory_tracker.as_ref();
    let concurrency = config.input_read_concurrency.max(1);

    // Open every input's cursor and load its first record, `input_read_concurrency`
    // opens in flight (each open costs one ranged block GET). `buffered` keeps
    // canonical input order, the k-way merge's tie-break on equal
    // `(trace_id, start_ts_ns)`. Box each future with an explicit `+ Send`
    // bound before `buffered`, the workaround `crate::rlog::merge_stream_into_parts`
    // documents for futures that borrow the `&dyn ObjectStoreBackend`.
    type CursorFuture<'f, 'a> = Pin<Box<dyn Future<Output = Result<BlockCursor<'a>>> + Send + 'f>>;
    let opens: Vec<CursorFuture<'_, '_>> = catalogs
        .iter()
        .enumerate()
        .map(|(idx, catalog)| {
            Box::pin(open_cursor(store, catalog, idx, tracker)) as CursorFuture<'_, '_>
        })
        .collect();
    let mut cursors: Vec<BlockCursor> = stream_iter(opens)
        .buffered(concurrency)
        .try_collect()
        .await?;

    let mut parts = Vec::new();
    let mut part_index: u32 = 0;
    let mut current: Option<PartBuilder> = None;
    let mut current_trace: Option<[u8; 16]> = None;
    let mut counts = RecordCounts::default();

    loop {
        let mut best: Option<(usize, [u8; 16], i64, usize)> = None;
        for (i, cursor) in cursors.iter().enumerate() {
            if let Some(head) = &cursor.head {
                let key = (head.trace_id, head.start_ts_ns, cursor.input_index);
                match best {
                    Some((_, bt, bts, bidx)) if (bt, bts, bidx) <= key => {}
                    _ => best = Some((i, key.0, key.1, key.2)),
                }
            }
        }
        let Some((bi, trace_id, _, _)) = best else {
            break;
        };
        let Some(rec) = cursors[bi].take_head() else {
            return Err(MaintainError::Invariant(
                "rspan merge selected a cursor with no head".into(),
            ));
        };

        counts.add_input()?;

        // The trace-boundary flush runs on every `trace_id` transition in the
        // merged stream, independent of `keep`: a part is closed before the
        // first record of a new trace so a trace never straddles two parts.
        // Only surviving records open or push into a part.
        //
        // This transition is the only place the split target is consulted, so
        // the part's heap is whatever the traces it already holds weigh: past
        // the target by up to one trace, and unbounded for one trace larger
        // than the target.
        if current_trace != Some(trace_id) {
            if let Some(part) = &current {
                let over_cap =
                    part.estimate >= config.l1_part_memory_target_bytes && !part.is_empty();
                if over_cap && let Some(builder) = current.take() {
                    let built = builder
                        .finish(store, bucket, input_set_hash, part_index, dry_run)
                        .await?;
                    parts.push(built);
                    part_index += 1;
                    if let Some(t) = tracker {
                        t.set_writer_bytes(0);
                    }
                }
            }
            current_trace = Some(trace_id);
        }

        if keep(&rec)? {
            counts.add_output()?;
            let part = current.get_or_insert_with(|| PartBuilder::new(&identity));
            part.push(rec, tracker);
        }
        cursors[bi].refill(store, tracker).await?;
    }

    if let Some(part) = current
        && !part.is_empty()
    {
        let built = part
            .finish(store, bucket, input_set_hash, part_index, dry_run)
            .await?;
        parts.push(built);
        if let Some(t) = tracker {
            t.set_writer_bytes(0);
        }
    }

    Ok(SpanMergeOutput {
        parts,
        input_record_count: counts.input,
        output_record_count: counts.output,
    })
}

/// Open one input's cursor over its whole block sequence and load its first
/// record. Named (not an inline `async` block) so its future is `Send`-general
/// over the borrowed store; see the call site in [`merge`]. The cursor is
/// retained even when the input carries no record: [`BlockCursor::refill`]
/// still runs its per-input footer cross-check as it drains.
async fn open_cursor<'a>(
    store: &'a dyn ObjectStoreBackend,
    catalog: &'a SpanInputCatalog,
    input_index: usize,
    tracker: Option<&MergeMemoryTracker>,
) -> Result<BlockCursor<'a>> {
    let mut cursor = BlockCursor::open(catalog, input_index);
    cursor.refill(store, tracker).await?;
    Ok(cursor)
}

/// One in-progress L1 part: the shared [`RspanWriter`] merged records push
/// into directly, plus the running record-byte estimate that decides where
/// the part splits (the first trace boundary at or after the estimate reaches
/// the split target `l1_part_memory_target_bytes`) and feeds the
/// [`MergeMemoryTracker`]'s writer term. Between two such boundaries the
/// estimate grows unchecked, which is why the target is a split point rather
/// than a bound on this part's heap.
struct PartBuilder {
    writer: RspanWriter,
    estimate: u64,
    count: usize,
    trace_count: u64,
    last_trace: Option<[u8; 16]>,
}

impl PartBuilder {
    fn new(identity: &ObjectIdentity) -> Self {
        PartBuilder {
            writer: RspanWriter::new(RspanConfig::default(), *identity),
            estimate: 0,
            count: 0,
            trace_count: 0,
            last_trace: None,
        }
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Push one merged record into the part's writer, updating the running
    /// estimate and the distinct-trace count. Since the merge feeds records in
    /// `(trace_id, start_ts_ns)` order, all of one trace's records arrive
    /// contiguously, so a transition on `last_trace` counts distinct traces
    /// exactly.
    fn push(&mut self, r: SpanRecord, tracker: Option<&MergeMemoryTracker>) {
        if self.last_trace != Some(r.trace_id) {
            self.trace_count += 1;
            self.last_trace = Some(r.trace_id);
        }
        self.estimate = self.estimate.saturating_add(estimate_record(&r));
        self.count += 1;
        self.writer.push(r);
        if let Some(t) = tracker {
            t.set_writer_bytes(self.estimate);
        }
    }

    /// Encode and PUT the part through the shared writer pipeline. See
    /// [`finalize_part`] for the encode/summary/PUT detail this reuses.
    async fn finish(
        self,
        store: &dyn ObjectStoreBackend,
        bucket: &Bucket,
        input_set_hash: &[u8; 32],
        part_index: u32,
        dry_run: bool,
    ) -> Result<BuiltPart> {
        finalize_part(
            store,
            bucket,
            self.writer,
            input_set_hash,
            part_index,
            self.trace_count,
            dry_run,
        )
        .await
    }
}

/// One input's cursor over its whole block sequence, yielding records in
/// stored (already `(trace_id, start_ts_ns)`-ascending) order one block at a
/// time. At most one decoded block is resident: `head` is the next record to
/// merge, `block` holds the rest of the current block, and the next block's
/// bytes are fetched by range only once the current block is drained. Unlike
/// RLOG's `StreamCursor`, this walks *every* block of the input
/// unconditionally -- there is no per-trace filter, because RSPAN has no
/// directory of which traces a block holds (see the module memory note).
/// `input_index` is the cursor's canonical position in `catalogs`, the k-way
/// merge's tie-break on equal `(trace_id, start_ts_ns)`.
struct BlockCursor<'a> {
    input_index: usize,
    object_key: &'a str,
    reader: &'a RspanRangeReader,
    next_block: usize,
    total_blocks: usize,
    /// Remaining records of the current decoded block.
    block: std::vec::IntoIter<SpanRecord>,
    /// The current block's decoded-byte estimate, held live in the tracker
    /// until the block is released.
    block_bytes: u64,
    /// The next record to merge, or `None` once the cursor is exhausted.
    head: Option<SpanRecord>,
    /// Running count of records this cursor has decoded and handed to the
    /// merge (bumped in [`Self::take_head`]). Cross-checked against
    /// `declared_record_count` at exhaustion.
    decoded_count: u64,
    /// This input's own footer `record_count`, the independent authority the
    /// decode tally is checked against when the cursor drains. The whole-read
    /// compaction path asserted this per input; the streaming path restores it.
    declared_record_count: u64,
}

impl<'a> BlockCursor<'a> {
    /// Open a cursor over every block of `catalog`. Does not fetch yet; call
    /// [`Self::refill`] once to load the first record.
    fn open(catalog: &'a SpanInputCatalog, input_index: usize) -> Self {
        BlockCursor {
            input_index,
            object_key: &catalog.object_key,
            reader: &catalog.reader,
            next_block: 0,
            total_blocks: catalog.reader.block_count(),
            block: Vec::new().into_iter(),
            block_bytes: 0,
            head: None,
            decoded_count: 0,
            declared_record_count: catalog.footer.record_count,
        }
    }

    /// Take the current head record, counting it toward this cursor's decoded
    /// tally; the caller must [`Self::refill`] before the next merge step.
    fn take_head(&mut self) -> Option<SpanRecord> {
        let rec = self.head.take();
        if rec.is_some() {
            self.decoded_count = self.decoded_count.saturating_add(1);
        }
        rec
    }

    /// Release the current decoded block's residency from the tracker.
    fn release_block(&mut self, tracker: Option<&MergeMemoryTracker>) {
        if self.block_bytes > 0 {
            if let Some(t) = tracker {
                t.block_released(self.block_bytes);
            }
            self.block_bytes = 0;
        }
    }

    /// Load the next record into `head`: the next record of the current
    /// block, or the first record of the next block (fetched by range and
    /// decoded here), or `None` when this input is exhausted. At most one
    /// block's raw bytes and one decoded block are resident at a time.
    async fn refill(
        &mut self,
        store: &dyn ObjectStoreBackend,
        tracker: Option<&MergeMemoryTracker>,
    ) -> Result<()> {
        if let Some(rec) = self.block.next() {
            self.head = Some(rec);
            return Ok(());
        }
        self.release_block(tracker);
        while self.next_block < self.total_blocks {
            let loc: BlockLoc = self.reader.block_loc(self.next_block)?;
            self.next_block += 1;
            let got = store
                .get(self.object_key, GetRange::Range(loc.start(), loc.end()))
                .await?;
            let raw_len = got.data.len() as u64;
            if let Some(t) = tracker {
                t.block_fetched(raw_len);
            }
            let recs = self.reader.decode_block(&loc, got.data.as_ref())?;
            let decoded_bytes: u64 = recs.iter().map(estimate_record).sum();
            if let Some(t) = tracker {
                // Records both raw and decoded resident at the high-water
                // instant, then drops the raw buffer.
                t.block_decoded(raw_len, decoded_bytes);
            }
            drop(got);
            self.block_bytes = decoded_bytes;
            self.block = recs.into_iter();
            if let Some(rec) = self.block.next() {
                self.head = Some(rec);
                return Ok(());
            }
            // An empty block (never produced by this crate's own writer, but
            // not assumed impossible from an untrusted input): release and
            // try the next.
            self.release_block(tracker);
        }
        // This input is drained. Cross-check the records the merge actually
        // decoded against the footer's own `record_count` -- the whole-read
        // compaction path asserted `decoded_record_count == footer.record_count`
        // per input and failed loud on mismatch; the streaming merge dropped it.
        // Only a logically inconsistent but CRC-valid object
        // (truncation kills the suffix footer, mid-object damage trips block
        // CRCs) reaches here with a mismatch, but that is exactly the case the
        // per-input check catches. Fires exactly once per cursor: once `head`
        // is `None` the merge never selects this cursor again.
        if self.decoded_count != self.declared_record_count {
            return Err(MaintainError::SpanInputRecordCountMismatch {
                object_key: self.object_key.to_string(),
                decoded_record_count: self.decoded_count,
                footer_record_count: self.declared_record_count,
            });
        }
        self.head = None;
        Ok(())
    }
}

/// Encode one in-progress part's writer into an L1 object and PUT it: run the
/// shared writer pipeline via [`RspanWriter::finish_compacted`] (stamping
/// `level = 1`, the `input_set_hash`, and `part_index`), then PUT it
/// `CreateIfAbsent`. The part's summary stats are read back from the produced
/// object's own footer, so they describe exactly what was written.
pub(crate) async fn finalize_part(
    store: &dyn ObjectStoreBackend,
    bucket: &Bucket,
    writer: RspanWriter,
    input_set_hash: &[u8; 32],
    part_index: u32,
    trace_count: u64,
    dry_run: bool,
) -> Result<BuiltPart> {
    let object = writer.finish_compacted(1, input_set_hash.to_vec(), part_index)?;
    let object = bytes::Bytes::from(object);

    // Authoritative summary (bounds, counts) from the object we just wrote. The
    // footer's trace_id bounds are the part's inclusive id range; because
    // traces are merged in sorted order and a trace never straddles a part,
    // adjacent parts' ranges are disjoint and ascending.
    let ftr = footer::open(&object)?;
    let content_hash: [u8; 32] = *blake3::hash(&object).as_bytes();

    let input_set_hash16 = hex::encode(&input_set_hash[..8]);
    let hash16 = hex::encode(&content_hash[..8]);
    let key = keys::l1_part_key(
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
        bucket.ingest_hour_bucket,
        &input_set_hash16,
        part_index,
        &hash16,
    )?;

    let mut part = CompactionPart {
        part_index,
        first_series_id: ftr.min_trace_id.to_vec(),
        last_series_id: ftr.max_trace_id.to_vec(),
        content_hash: content_hash.to_vec(),
        object_size: object.len() as u64,
        sample_count: ftr.record_count,
        // A span's "series" identity for the record's series-count field is its
        // trace_id; the footer carries no distinct-trace count, so it is the
        // number of traces accumulated into this part.
        series_count: trace_count,
        // Spans have no run concept (no cross-record dedup runs, ADR-0041); the
        // per-span count already lives in sample_count, like logs.
        run_count: 0,
        min_event_ts_ns: ftr.min_start_ts_ns,
        max_event_ts_ns: ftr.max_end_ts_ns,
        segment_format_version: OUTPUT_FORMAT_VERSION,
        declared_column_stats: Vec::new(),
    };
    // Spans never carry declared typed columns -- they are a logs concept
    // (ADR-0873 decision 3), so this part has no eligible columns and stamps
    // nothing. Routed through the same validated commit-side path the logs
    // compactor uses, so "stamps nothing" is expressed the one way.
    ravel_commit::declared_stats::stamp_compaction_part(&mut part, &[]);
    // RSPAN keeps its current retention behaviour (ADR-0979 decision 3 wraps the
    // bytes in `Some` mechanically; the bounded-memory fix for this codec is a
    // follow-up, #982), so `put_already_existed` stays false and the bytes are
    // never released here.
    let built = BuiltPart {
        key,
        bytes: Some(object),
        part,
        put_already_existed: false,
    };
    if !dry_run {
        put_part(store, &built).await?;
    }
    Ok(built)
}

/// The compactor's object identity for an L1 part. `writer_epoch`/`writer_seq`
/// are zero and `writer_id` is the compactor's uuid: informational only, never
/// part of any identity or dedup order (RSPAN has none), matching the RSEG/RLOG
/// L1 writer identity convention.
pub(crate) fn compactor_identity(bucket: &Bucket, config: &CompactorConfig) -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: bucket.tenant_hash.0,
        shard: bucket.shard,
        writer_id: config.compactor_writer_id.into_bytes(),
        writer_epoch: 0,
        writer_seq: 0,
    }
}

/// A rough uncompressed byte estimate for one span, for the part size cap.
/// Approximate on purpose (it only decides where parts split, never
/// correctness); mirrors the writer's own `row_estimate`. `pub(crate)` so the
/// erasure-rewrite split test can compute a part count arithmetically rather
/// than observe it, the same way the RLOG path exposes `rlog::estimate_record`.
pub(crate) fn estimate_record(r: &SpanRecord) -> u64 {
    let mut est: u64 = 48; // fixed-field overhead (ids, timestamps, status)
    est += r.name.len() as u64;
    if let Some(m) = &r.status_message {
        est += m.len() as u64;
    }
    for (k, v) in &r.attrs {
        est += k.len() as u64 + v.len() as u64 + 4;
    }
    est
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use bytes::Bytes;
    use proptest::prelude::*;
    use prost::Message;
    use ravel_commit::record::{self, NewCommitRecord};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, list_all};
    use ravel_proto::commit::v1::CompactionRecord;
    use ravel_rspan::{
        BloomPredicate, COL_NAME, COL_SERVICE_NAME, RspanConfig, RspanReader, RspanWriter,
        SpanQuery, StatusCode,
    };
    use ravel_rspan::{SpanRecord, writer::ObjectIdentity};
    use ravel_types::{Signal, TenantHash, TenantId};
    use uuid::Uuid;

    use super::*;
    use crate::config::MergeMemoryTracker;
    use crate::{Bucket, CompactionOutcome, CompactorConfig, FixedClock, compact_bucket};

    const TENANT: &str = "acme";
    const SHARD: u32 = 7;
    const HOUR: u32 = 495_000;
    const NS_PER_HOUR: i64 = 3_600_000_000_000;
    const EPOCH: u64 = 10;

    fn tenant_hash() -> TenantHash {
        TenantId::new(TENANT).hash()
    }

    fn bucket() -> Bucket {
        Bucket::new(tenant_hash(), Signal::Spans, SHARD, HOUR)
    }

    /// Past the seal margin for [`HOUR`] under default config.
    fn sealed_now_ns() -> i64 {
        (i64::from(HOUR) + 1) * NS_PER_HOUR + 2 * NS_PER_HOUR
    }

    /// A synthetic span for trace `t`, span `s`, over `[start, end]`, with a
    /// distinct attr so attrs round-trip through the merge.
    fn span(t: u8, s: u8, start: i64, end: i64) -> SpanRecord {
        SpanRecord {
            trace_id: [t; 16],
            span_id: [s; 8],
            parent_span_id: if s == 0 { None } else { Some([s - 1; 8]) },
            name: format!("op-{s}"),
            start_ts_ns: start,
            end_ts_ns: end,
            status_code: StatusCode::Ok,
            status_message: Some(format!("msg-{s}")),
            attrs: vec![("svc".into(), format!("s{t}"))],
        }
    }

    /// Seed one L0 `.rspan` input (data object + commit record) exactly as a
    /// span ingest shard would, returning the object bytes for a direct
    /// differential decode.
    async fn seed(
        store: &dyn ObjectStoreBackend,
        writer_id: Uuid,
        seq: u64,
        records: &[SpanRecord],
    ) -> Bytes {
        seed_l0(store, writer_id, seq, records, RspanConfig::default()).await
    }

    /// [`seed`] with the L0 writer's config under test control: `cfg` so an
    /// input can be blocked differently from the output the compactor always
    /// writes with `RspanConfig::default()` (the RSPAN analogue of RLOG's
    /// `seed_l0`).
    async fn seed_l0(
        store: &dyn ObjectStoreBackend,
        writer_id: Uuid,
        seq: u64,
        records: &[SpanRecord],
        cfg: RspanConfig,
    ) -> Bytes {
        let th = tenant_hash();
        let identity = ObjectIdentity {
            tenant_hash: th.0,
            shard: SHARD,
            writer_id: writer_id.into_bytes(),
            writer_epoch: EPOCH,
            writer_seq: seq,
        };
        let mut w = RspanWriter::new(cfg, identity);
        for r in records {
            w.push(r.clone());
        }
        let bytes = Bytes::from(w.finish().expect("finish L0"));
        let content_hash: [u8; 32] = *blake3::hash(&bytes).as_bytes();
        let data_key = keys::data_key(
            &th,
            Signal::Spans,
            SHARD,
            writer_id,
            EPOCH,
            seq,
            &content_hash,
        )
        .expect("data key");
        store
            .put(&data_key, bytes.clone(), PutOptions::default())
            .await
            .expect("put data");

        let mut traces = std::collections::BTreeSet::new();
        let mut min_start = i64::MAX;
        let mut max_end = i64::MIN;
        for r in records {
            traces.insert(r.trace_id);
            min_start = min_start.min(r.start_ts_ns);
            max_end = max_end.max(r.end_ts_ns);
        }
        let created = i64::from(HOUR) * NS_PER_HOUR + (seq as i64) * 1_000_000;
        let rec = record::build(NewCommitRecord {
            tenant_hash: th,
            signal: Signal::Spans,
            shard: SHARD,
            writer_id,
            writer_epoch: EPOCH,
            writer_seq: seq,
            object_size: bytes.len() as u64,
            content_hash,
            sample_count: records.len() as u64,
            series_count: traces.len() as u64,
            min_event_ts_ns: min_start,
            max_event_ts_ns: max_end,
            min_ingest_ts_ns: created,
            max_ingest_ts_ns: created,
            segment_format_version: OUTPUT_FORMAT_VERSION,
            created_unix_ns: created,
            ingest_hour_bucket: HOUR,
        })
        .expect("build commit record");
        let commit_key = keys::commit_key_for_record(&rec).expect("commit key");
        store
            .put(&commit_key, record::encode(&rec), PutOptions::default())
            .await
            .expect("put commit");
        bytes
    }

    /// Decode every span of an RSPAN object (full-range scan, so nothing is
    /// pruned), in the object's stored `(trace_id, start_ts)` order.
    fn decode_all(bytes: &[u8]) -> Vec<SpanRecord> {
        let cfg = RspanConfig::default();
        let reader = RspanReader::new(bytes, &cfg).expect("open");
        let (rows, _) = reader
            .scan(&SpanQuery::ts_range(i64::MIN, i64::MAX))
            .expect("scan");
        rows
    }

    /// An order-independent canonical key for a span: attrs folded to a sorted
    /// unique-key map (the writer's canonical form), everything else verbatim.
    type Canon = (
        [u8; 16],
        [u8; 8],
        Option<[u8; 8]>,
        String,
        i64,
        i64,
        u8,
        Option<String>,
        Vec<(String, String)>,
    );
    fn canon(r: &SpanRecord) -> Canon {
        let attrs: std::collections::BTreeMap<String, String> = r.attrs.iter().cloned().collect();
        (
            r.trace_id,
            r.span_id,
            r.parent_span_id,
            r.name.clone(),
            r.start_ts_ns,
            r.end_ts_ns,
            r.status_code.to_u8(),
            r.status_message.clone(),
            attrs.into_iter().collect(),
        )
    }

    fn canon_multiset(records: &[SpanRecord]) -> Vec<Canon> {
        let mut v: Vec<Canon> = records.iter().map(canon).collect();
        v.sort();
        v
    }

    /// Fetch the single compaction record in the bucket and every L1 part it
    /// references (parts in record order = ascending trace ranges).
    async fn read_output(store: &dyn ObjectStoreBackend) -> (CompactionRecord, Vec<Bytes>) {
        let b = bucket();
        let prefix =
            keys::commit_shard_hour_prefix(&b.tenant_hash, b.signal, b.shard, b.ingest_hour_bucket)
                .unwrap();
        let metas = list_all(store, &prefix).await.unwrap();
        let mut rec_keys: Vec<String> = metas
            .into_iter()
            .map(|m| m.key)
            .filter(|k| {
                matches!(
                    keys::partition_bucket_entry(k),
                    Ok(keys::BucketEntry::CompactionRecord(_))
                )
            })
            .collect();
        rec_keys.sort();
        assert_eq!(rec_keys.len(), 1, "expected exactly one compaction record");
        let got = store.get(&rec_keys[0], GetRange::Full).await.unwrap();
        let recrd = CompactionRecord::decode(got.data.as_ref()).unwrap();
        let mut parts = Vec::new();
        for p in &recrd.parts {
            let key = keys::reconstruct_l1_part_key(&recrd, p).unwrap();
            parts.push(store.get(&key, GetRange::Full).await.unwrap().data);
        }
        (recrd, parts)
    }

    #[tokio::test]
    async fn compacts_two_l0_rspan_objects_into_one_l1_part_verbatim() {
        let store = MemoryStore::new();
        let a = vec![span(0, 0, 10, 20), span(1, 0, 5, 9)];
        let b = vec![span(0, 1, 15, 18), span(2, 0, 1, 2)];
        let a_bytes = seed(&store, Uuid::from_u128(1), 1, &a).await;
        let b_bytes = seed(&store, Uuid::from_u128(2), 2, &b).await;

        let clock = FixedClock::new(sealed_now_ns());
        let outcome = compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket())
            .await
            .expect("compact");
        assert!(matches!(outcome, CompactionOutcome::Compacted { .. }));

        let (rec, parts) = read_output(&store).await;
        assert_eq!(rec.level, 1);
        assert!(!rec.input_set_hash.is_empty());
        assert_eq!(parts.len(), 1, "small corpus fits one part");
        let ftr = footer::open(&parts[0]).expect("open l1");
        assert_eq!(ftr.level, 1);
        assert!(!ftr.input_set_hash.is_empty());
        // Against the format's constant, not the compactor's: see the same
        // assertion in `rlog.rs`. `open` above rejects any
        // other trailer version, so this pins the recorded number to the
        // bytes rather than to a second constant in this crate.
        assert_eq!(
            rec.parts[0].segment_format_version,
            u32::from(ravel_rspan::footer::VERSION)
        );
        assert_eq!(rec.parts[0].sample_count, 4);
        assert_eq!(rec.parts[0].series_count, 3, "three distinct traces");

        // The L1 records are the union of both inputs, stored in (trace_id,
        // start_ts) order.
        let l1 = decode_all(&parts[0]);
        let order: Vec<([u8; 16], i64)> = l1.iter().map(|r| (r.trace_id, r.start_ts_ns)).collect();
        let mut sorted = order.clone();
        sorted.sort();
        assert_eq!(order, sorted, "L1 records in (trace, start_ts) order");

        let mut expected = a.clone();
        expected.extend(b.clone());
        assert_eq!(canon_multiset(&l1), canon_multiset(&expected));

        // Decoding the inputs directly then concatenating gives the same set.
        let mut direct = decode_all(&a_bytes);
        direct.extend(decode_all(&b_bytes));
        assert_eq!(canon_multiset(&l1), canon_multiset(&direct));
    }

    /// A synthetic span carrying an explicit `service.name` attribute and a
    /// chosen span `name`, so the writer's v3 `service_name` column and the
    /// block bloom over span-name/service-name tokens are populated. The
    /// default [`span`] helper above uses a plain "svc" attr key, which is not
    /// the lifted `service.name` key and so leaves the v3 column empty.
    fn span_svc(t: u8, s: u8, start: i64, end: i64, name: &str, service: &str) -> SpanRecord {
        SpanRecord {
            trace_id: [t; 16],
            span_id: [s; 8],
            parent_span_id: None,
            name: name.to_string(),
            start_ts_ns: start,
            end_ts_ns: end,
            status_code: StatusCode::Ok,
            status_message: None,
            attrs: vec![("service.name".to_string(), service.to_string())],
        }
    }

    /// The v3 keystone (ADR-0054): merge inputs with pairwise
    /// disjoint service names and span names, then prove the compacted output's
    /// BLOOM section and `service_name` column were rebuilt from the merged
    /// union, not copied from whichever input's blooms happened to be around.
    ///
    /// A bloom copied from one input would answer membership for only that
    /// input's tokens; a `service_name` column copied from one input would
    /// decode the wrong value for the other inputs' records. Both would pass a
    /// plain "record union is complete" check (the attrs blob still round-trips
    /// via the reader's re-insertion), so this test probes the derived
    /// artifacts directly.
    #[tokio::test]
    async fn compaction_rebuilds_bloom_and_service_name_column() {
        let store = MemoryStore::new();
        // Three L0 inputs, each one service and one span name over its own
        // traces, all disjoint across inputs.
        let a = vec![
            span_svc(0, 0, 10, 20, "read", "checkout"),
            span_svc(1, 0, 11, 21, "read", "checkout"),
        ];
        let b = vec![
            span_svc(2, 0, 12, 22, "write", "payments"),
            span_svc(3, 0, 13, 23, "write", "payments"),
        ];
        let c = vec![
            span_svc(4, 0, 14, 24, "flush", "inventory"),
            span_svc(5, 0, 15, 25, "flush", "inventory"),
        ];
        seed(&store, Uuid::from_u128(1), 1, &a).await;
        seed(&store, Uuid::from_u128(2), 2, &b).await;
        seed(&store, Uuid::from_u128(3), 3, &c).await;

        let clock = FixedClock::new(sealed_now_ns());
        compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket())
            .await
            .expect("compact");
        let (_rec, parts) = read_output(&store).await;
        assert_eq!(parts.len(), 1, "small corpus fits one part");

        let cfg = RspanConfig::default();
        let reader = RspanReader::new(parts[0].as_ref(), &cfg).expect("open l1");
        let bloom = reader.bloom().expect("bloom parses, not degraded");
        let skip = reader.skip_index();

        // Does *some* block of the part survive the bloom prune for a
        // `field_id = literal` equality predicate? A surviving block means the
        // bloom does not prove the literal absent (present, up to a false
        // positive); zero survivors means the bloom proves it absent across the
        // whole part (a bloom is false-positive-only, so a negative is a
        // proof).
        let survives = |field_id: u32, literal: &str| -> bool {
            let preds = [BloomPredicate { field_id, literal }];
            !skip
                .candidate_blocks_with_bloom(None, i64::MIN, i64::MAX, None, None, &bloom, &preds)
                .expect("bloom prune")
                .is_empty()
        };

        // Every input's service name and span name is provably present in the
        // merged bloom -- not just the last-merged input's. A copied bloom
        // would fail this for two of the three services.
        for service in ["checkout", "payments", "inventory"] {
            assert!(
                survives(COL_SERVICE_NAME, service),
                "merged bloom must answer membership for service {service}"
            );
        }
        for name in ["read", "write", "flush"] {
            assert!(
                survives(COL_NAME, name),
                "merged bloom must answer membership for span name {name}"
            );
        }

        // Negative controls: a service and a span name in no input are proven
        // absent (the bloom prunes every block). This distinguishes a real
        // rebuilt bloom from an all-ones (vacuously-present) or copied bloom,
        // both of which would still pass the positive asserts above.
        assert!(
            !survives(COL_SERVICE_NAME, "billing"),
            "absent service must be pruned by the rebuilt bloom"
        );
        assert!(
            !survives(COL_NAME, "delete"),
            "absent span name must be pruned by the rebuilt bloom"
        );

        // The service_name column decodes the input's value for every merged
        // record. The reader lifts COL_SERVICE_NAME back into attrs on decode
        // (block.rs), so a wrong column value surfaces as a wrong service.name
        // attr here.
        let expected: BTreeMap<[u8; 16], &str> = [
            ([0u8; 16], "checkout"),
            ([1u8; 16], "checkout"),
            ([2u8; 16], "payments"),
            ([3u8; 16], "payments"),
            ([4u8; 16], "inventory"),
            ([5u8; 16], "inventory"),
        ]
        .into_iter()
        .collect();
        let l1 = decode_all(&parts[0]);
        assert_eq!(l1.len(), 6, "all six merged spans present");
        for r in &l1 {
            let got = r
                .attrs
                .iter()
                .find(|(k, _)| k == "service.name")
                .map(|(_, v)| v.as_str());
            assert_eq!(
                got,
                expected.get(&r.trace_id).copied(),
                "service_name column decodes the input's value for trace {:?}",
                r.trace_id
            );
        }
    }

    #[tokio::test]
    async fn part_splitting_keeps_traces_whole_and_disjoint() {
        // Many distinct traces and a tiny part cap force splits on trace
        // boundaries: parts get disjoint, ascending trace_id ranges (a trace
        // never straddles two parts), and the union of records is preserved.
        let store = MemoryStore::new();
        let mk = |name_suffix: u8| -> Vec<SpanRecord> {
            (0..20u8)
                .map(|t| span(t, name_suffix, i64::from(t), i64::from(t) + 1))
                .collect()
        };
        let a = mk(0);
        let b = mk(1);
        seed(&store, Uuid::from_u128(1), 1, &a).await;
        seed(&store, Uuid::from_u128(2), 2, &b).await;

        let clock = FixedClock::new(sealed_now_ns());
        let config = CompactorConfig {
            l1_part_memory_target_bytes: 256,
            ..CompactorConfig::default()
        };
        compact_bucket(&store, &clock, &config, &bucket())
            .await
            .expect("compact");
        let (rec, parts) = read_output(&store).await;
        assert!(parts.len() >= 2, "tiny cap must split into parts");

        // Part trace_id ranges are disjoint and ascending.
        let mut prev_last: Option<[u8; 16]> = None;
        for (i, p) in rec.parts.iter().enumerate() {
            let first: [u8; 16] = p.first_series_id.as_slice().try_into().unwrap();
            let last: [u8; 16] = p.last_series_id.as_slice().try_into().unwrap();
            assert!(first <= last);
            if let Some(pl) = prev_last {
                assert!(pl < first, "part trace ranges must be disjoint and ordered");
            }
            prev_last = Some(last);
            assert_eq!(footer::open(&parts[i]).expect("open").level, 1);
        }

        // Content complete across all parts.
        let mut l1: Vec<SpanRecord> = Vec::new();
        for p in &parts {
            l1.extend(decode_all(p));
        }
        let mut expected = a.clone();
        expected.extend(b.clone());
        assert_eq!(canon_multiset(&l1), canon_multiset(&expected));
    }

    #[tokio::test]
    async fn corrupt_input_footer_fails_loud_at_load() {
        // A truncated/garbled input object must fail the compaction with a typed
        // error, never a panic and never a silent partial merge. Two inputs so
        // the compaction clears the min-inputs gate and reaches the codec; one
        // input's data object is then corrupted.
        let store = MemoryStore::new();
        let a = vec![span(0, 0, 1, 2)];
        let b = vec![span(1, 0, 3, 4)];
        seed(&store, Uuid::from_u128(1), 1, &a).await;
        seed(&store, Uuid::from_u128(2), 2, &b).await;
        // Corrupt one data object's trailer so its footer no longer opens.
        let prefix = format!(
            "t/{}/{}/l0/{:04}/",
            tenant_hash().to_hex(),
            Signal::Spans.key_prefix(),
            SHARD
        );
        let data_key = list_all(&store, &prefix).await.unwrap()[0].key.clone();
        let mut bad = store
            .get(&data_key, GetRange::Full)
            .await
            .unwrap()
            .data
            .to_vec();
        let n = bad.len();
        bad[n - 1] ^= 0xff; // clobber the trailer magic
        store
            .put(&data_key, Bytes::from(bad), PutOptions::default())
            .await
            .unwrap();

        let clock = FixedClock::new(sealed_now_ns());
        let err = compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket())
            .await
            .expect_err("corrupt input must fail the compaction");
        // Any typed error is acceptable; it must not panic or succeed.
        let _ = format!("{err}");
    }

    /// Re-stamp an RSPAN object's footer with a mutated `record_count`, keeping
    /// the body (BLOCKS/SKIP_IDX/BLOOM) byte-identical and the footer CRC valid.
    /// The result opens cleanly (`footer::open` succeeds) but its declared
    /// `record_count` disagrees with the number of records the body decodes to
    /// -- a logically inconsistent but CRC-valid object, the exact case the
    /// per-input cross-check in `BlockCursor::refill` must catch.
    fn rewrite_footer_record_count(bytes: &[u8], new_count: u64) -> Bytes {
        let trailer_len = 16usize; // footer::TRAILER_LEN
        let n = bytes.len();
        let footer_len = u32::from_le_bytes([
            bytes[n - trailer_len],
            bytes[n - trailer_len + 1],
            bytes[n - trailer_len + 2],
            bytes[n - trailer_len + 3],
        ]) as usize;
        // body is everything before the footer protobuf + trailer; its bytes and
        // thus every section offset in the footer stay unchanged.
        let body_end = n - trailer_len - footer_len;
        let mut ftr = footer::open(bytes).expect("open original");
        assert_eq!(
            ftr.record_count,
            new_count.wrapping_sub(1),
            "helper assumes a +1 bump from the real count"
        );
        ftr.record_count = new_count;
        let mut out = bytes[..body_end].to_vec();
        footer::write_footer_and_trailer(&mut out, &ftr);
        // Sanity: the rewritten object still opens and now over-declares.
        let reopened = footer::open(&out).expect("rewritten object still opens");
        assert_eq!(reopened.record_count, new_count);
        Bytes::from(out)
    }

    /// A CRC-valid RSPAN input whose footer `record_count` over-declares its
    /// actual body (footer says N+1, body decodes to N) must fail the
    /// streaming compaction loud with the typed
    /// [`MaintainError::SpanInputRecordCountMismatch`], never a silent merge
    /// that drops the discrepancy.
    ///
    /// Flip proof (non-vacuous): against the pre-fix code -- with the per-cursor
    /// `decoded_count != declared_record_count` check removed from
    /// `BlockCursor::refill` -- this compaction succeeds and `expect_err` panics
    /// here, so the `assert!(matches!(err, ...SpanInputRecordCountMismatch ...))`
    /// below is what fails first. Verified by deleting the check: the test then
    /// reports the compaction returned Ok instead of the mismatch error.
    #[tokio::test]
    async fn over_declared_footer_record_count_fails_loud() {
        let store = MemoryStore::new();
        // Input A: honest footer, distinct trace so both drain independently.
        let a = vec![span(0, 0, 1, 2)];
        seed(&store, Uuid::from_u128(1), 1, &a).await;
        // Input B: two real records, footer will be bumped to claim three.
        let b = vec![span(5, 0, 1, 2), span(5, 1, 3, 4)];
        let b_bytes = seed(&store, Uuid::from_u128(2), 2, &b).await;

        // Locate B's data object and overwrite it with a footer that claims one
        // extra record than the body holds. The commit record is left untouched:
        // it still points at this key and carries the honest sample_count, so the
        // ADR-0048 conservation gate would pass -- only the per-input footer
        // cross-check catches this.
        let honest = footer::open(&b_bytes).expect("open b");
        let declared = honest.record_count; // 2
        let data_key = keys::data_key(
            &tenant_hash(),
            Signal::Spans,
            SHARD,
            Uuid::from_u128(2),
            EPOCH,
            2,
            blake3::hash(&b_bytes).as_bytes(),
        )
        .expect("data key");
        let bad = rewrite_footer_record_count(&b_bytes, declared + 1);
        store
            .put(&data_key, bad, PutOptions::default())
            .await
            .expect("overwrite b");

        let clock = FixedClock::new(sealed_now_ns());
        let err = compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket())
            .await
            .expect_err("over-declared footer must fail the compaction");
        match err {
            MaintainError::SpanInputRecordCountMismatch {
                decoded_record_count,
                footer_record_count,
                ..
            } => {
                assert_eq!(decoded_record_count, declared, "decoded the honest N");
                assert_eq!(footer_record_count, declared + 1, "footer over-declared");
            }
            other => panic!("expected SpanInputRecordCountMismatch, got {other:?}"),
        }
    }

    // --- bounded-memory k-way merge -------------------------------

    /// L0 writer config for the tests below: 1000-record blocks, so an input
    /// is blocked differently from the 8192-record output (the compactor
    /// always writes with `RspanConfig::default()`).
    fn l0_blocked_cfg() -> RspanConfig {
        RspanConfig {
            block_target_records: 1000,
            ..RspanConfig::default()
        }
    }

    /// A synthetic span for a hot trace `t`, span `s`, tagged with `tag` so its
    /// origin is observable in decoded output.
    fn span_tagged(t: u8, s: u32, start: i64, tag: &str) -> SpanRecord {
        SpanRecord {
            trace_id: [t; 16],
            span_id: (s as u64).to_be_bytes(),
            parent_span_id: None,
            name: format!("op-{tag}-{s}"),
            start_ts_ns: start,
            end_ts_ns: start + 1,
            status_code: StatusCode::Ok,
            status_message: None,
            attrs: vec![("tag".into(), tag.to_string())],
        }
    }

    /// The split target is a target, not a bound: one trace larger than it is
    /// buffered whole, because the target is only consulted at a `trace_id`
    /// transition and a trace never straddles two parts.
    ///
    /// This is the claim
    /// [`CompactorConfig::l1_part_memory_target_bytes`] makes about the RSPAN
    /// path, pinned as a magnitude rather than a direction: one hot trace at a
    /// 4 KiB target produces a single part whose record heap, and whose recorded
    /// residency, are both more than 10x the target. If RSPAN ever grew a
    /// mid-trace split, this fails and the doc it pins has to change with it.
    #[tokio::test]
    async fn one_trace_runs_past_the_memory_split_target() {
        const INPUTS: u32 = 2;
        const PER_INPUT: i64 = 600;
        const TARGET: u64 = 4 * 1024;

        let store = MemoryStore::new();
        for j in 0..INPUTS {
            // All one trace (trace 0), start_ts interleaved across inputs.
            let recs: Vec<SpanRecord> = (0..PER_INPUT)
                .map(|i| {
                    let ts = i * i64::from(INPUTS) + i64::from(j);
                    span_tagged(0, ts as u32, ts, "hot")
                })
                .collect();
            seed(
                &store,
                Uuid::from_u128(u128::from(j) + 1),
                u64::from(j) + 1,
                &recs,
            )
            .await;
        }

        let tracker = MergeMemoryTracker::new();
        let config = CompactorConfig {
            l1_part_memory_target_bytes: TARGET,
            merge_memory_tracker: Some(tracker.clone()),
            ..CompactorConfig::default()
        };
        let clock = FixedClock::new(sealed_now_ns());
        compact_bucket(&store, &clock, &config, &bucket())
            .await
            .expect("compact");

        let (_rec, parts) = read_output(&store).await;
        assert_eq!(parts.len(), 1, "one trace is one part at any target");
        let heap: u64 = decode_all(&parts[0]).iter().map(estimate_record).sum();
        assert!(
            heap > 10 * TARGET,
            "the single part's heap {heap} must run well past the {TARGET}-byte target, \
             which is what makes the target a split point and not a bound"
        );
        assert!(
            tracker.peak_total_bytes() > 10 * TARGET,
            "the overshoot is real residency, not only an estimate: peak {} vs target {TARGET}",
            tracker.peak_total_bytes()
        );
    }

    /// Acceptance test (mirroring RLOG's): the RSPAN
    /// compaction merge's peak resident decode memory is bounded by block size
    /// times input count, independent of how large one hot trace grows.
    ///
    /// A single hot trace (many spans in one trace, e.g. a fanned-out request)
    /// is grown 10x across two runs. The [`MergeMemoryTracker`]'s recorded
    /// *transient* high-water -- the merge's own decode-side buffers, at most
    /// one fetched raw block plus one decoded block per input -- must stay
    /// under a fixed bound and must NOT grow with the trace. Flipped-line
    /// proof: reverting `BlockCursor::refill`'s one-block-at-a-time discipline
    /// to whole-object decode (e.g. by making `refill` eagerly decode every
    /// remaining block into `self.block` on first call instead of one block
    /// per call) makes `decoded` scale with the trace and pushes `transient`
    /// over `TRANSIENT_BOUND` at the 10x scale -- this test fails against that
    /// whole-read shape.
    ///
    /// Deterministic: it asserts on the tracker's accounting, never on process
    /// RSS or allocator hooks, and runs against [`MemoryStore`].
    #[tokio::test]
    async fn merge_peak_memory_is_bounded_independently_of_trace_size() {
        const INPUTS: u32 = 3;
        // l0_blocked_cfg's block target: each input is re-blocked at 1000
        // records, so a grown trace spans many blocks per input.
        const L0_BLOCK: u64 = 1000;
        // A generous per-record upper bound (estimate_record for these tiny
        // records is well under 100 bytes); the bound is intentionally loose
        // so it tracks block size and input count, not the exact estimate.
        const PER_RECORD_BOUND: u64 = 256;
        // At most one raw block plus one decoded block resident per input.
        const TRANSIENT_BOUND: u64 = INPUTS as u64 * 2 * L0_BLOCK * PER_RECORD_BOUND;
        // Rough bytes-per-record for the "trace size" the peak is compared
        // against (only used for reporting and the not-scaling assertion).
        const PER_RECORD_APPROX: i64 = 70;

        // records-per-input at the base scale and 10x, all one hot trace.
        let scales = [3_000i64, 30_000i64];
        let mut peaks = Vec::new();
        for &records_per_input in &scales {
            let store = MemoryStore::new();
            for j in 0..INPUTS {
                // One hot trace (trace 0); start_ts interleaves across inputs
                // so the k-way merge genuinely interleaves them.
                let recs: Vec<SpanRecord> = (0..records_per_input)
                    .map(|i| {
                        span_tagged(
                            0,
                            (i * i64::from(INPUTS) + i64::from(j)) as u32,
                            i * i64::from(INPUTS) + i64::from(j),
                            "x",
                        )
                    })
                    .collect();
                seed_l0(
                    &store,
                    Uuid::from_u128(u128::from(j) + 1),
                    u64::from(j) + 1,
                    &recs,
                    l0_blocked_cfg(),
                )
                .await;
            }

            let tracker = MergeMemoryTracker::new();
            let config = CompactorConfig {
                merge_memory_tracker: Some(tracker.clone()),
                ..CompactorConfig::default()
            };
            let clock = FixedClock::new(sealed_now_ns());
            compact_bucket(&store, &clock, &config, &bucket())
                .await
                .expect("compact");

            // One hot trace => exactly one part (a trace never straddles).
            let (_rec, parts) = read_output(&store).await;
            assert_eq!(parts.len(), 1, "one trace is one part");
            assert_eq!(
                decode_all(&parts[0]).len() as i64,
                records_per_input * i64::from(INPUTS),
                "every record survived the merge"
            );

            let transient = tracker.peak_transient_bytes();
            let total = tracker.peak_total_bytes();
            let trace_bytes =
                (records_per_input * i64::from(INPUTS)) as u64 * PER_RECORD_APPROX as u64;
            println!(
                "[mem:rspan #908] records/input={records_per_input} trace~={trace_bytes}B \
                 peak_transient={transient}B peak_total={total}B transient_bound={TRANSIENT_BOUND}B \
                 mem_bound={}B",
                config.l1_part_memory_target_bytes
            );

            // The decode-side peak is under the fixed bound.
            assert!(
                transient > 0,
                "the tracker must record real decode residency"
            );
            assert!(
                transient < TRANSIENT_BOUND,
                "transient peak {transient} exceeded the fixed bound {TRANSIENT_BOUND} \
                 (block size x input count)"
            );
            // The only trace-dependent residency is the writer's in-progress
            // part (content-addressing needs the whole part before its key
            // exists). This fixture is one trace, so the part holds all of it:
            // the assertion passes because the default target is far above this
            // corpus, not because the target caps the part. See
            // `one_trace_runs_past_the_memory_split_target`.
            assert!(
                total < TRANSIENT_BOUND + config.l1_part_memory_target_bytes,
                "total peak {total} exceeded transient bound + memory split target"
            );
            peaks.push(transient);
        }

        // The trace grew 10x; the decode-side peak did not (the same handful
        // of blocks stay resident regardless of trace size).
        let (base, ten_x) = (peaks[0], peaks[1]);
        assert!(
            ten_x <= base * 2,
            "decode-side peak scaled with the trace: base={base} 10x={ten_x}"
        );
        // And concretely, at 10x the peak is a small fraction of the trace.
        let trace_bytes_10x = (scales[1] * i64::from(INPUTS)) as u64 * PER_RECORD_APPROX as u64;
        assert!(
            ten_x < trace_bytes_10x / 5,
            "10x peak {ten_x} is not far below the trace size {trace_bytes_10x}"
        );
    }

    /// Byte-identical-output test: the new k-way streaming
    /// merge produces the exact same part bytes as the old "concatenate every
    /// input's decoded records by trace_id in canonical input order, then
    /// stable sort by (trace_id, start_ts)" path. Parts are content-addressed,
    /// so a reordering bug would silently change every downstream hash.
    ///
    /// The corpus carries the same `(trace_id, start_ts)` in two different
    /// inputs with *different span names*, so a wrong tie-break would reorder
    /// those records and change the bytes -- the exact failure this guards
    /// against.
    #[tokio::test]
    async fn streaming_merge_output_is_byte_identical_to_stable_sort_order() {
        let store = MemoryStore::new();
        // Three inputs over three traces. `a` and `b` share an even start_ts
        // grid (cross-input ties on every trace); `c` uses an odd grid (it
        // interleaves). Names are tagged by input so order is observable.
        let mk = |tag: &str, odd: i64| -> Vec<SpanRecord> {
            let mut v = Vec::new();
            for t in 0..3u8 {
                for k in 0..40u32 {
                    v.push(span_tagged(
                        t,
                        k,
                        i64::from(k) * 2 + odd,
                        &format!("{tag}-{t}-{k}"),
                    ));
                }
            }
            v
        };
        let a = mk("a", 0);
        let b = mk("b", 0); // same start_ts grid as `a` => cross-input ties
        let c = mk("c", 1); // odd grid => interleaves
        // writer_ids 1,2,3 with seqs 1,2,3 sort to canonical order [a, b, c],
        // the order the merge breaks start_ts ties in.
        let a_bytes = seed_l0(&store, Uuid::from_u128(1), 1, &a, l0_blocked_cfg()).await;
        let b_bytes = seed_l0(&store, Uuid::from_u128(2), 2, &b, l0_blocked_cfg()).await;
        let c_bytes = seed_l0(&store, Uuid::from_u128(3), 3, &c, l0_blocked_cfg()).await;

        let clock = FixedClock::new(sealed_now_ns());
        compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket())
            .await
            .expect("compact");
        let (_rec, parts) = read_output(&store).await;
        assert_eq!(parts.len(), 1, "small corpus fits one part");
        let actual = &parts[0];

        // Reference: the OLD logical order. For each trace in sorted id order,
        // concatenate each input's records for that trace in canonical (seed)
        // order, then stable-sort by start_ts; concatenate across traces.
        let per_input: Vec<Vec<SpanRecord>> = [&a_bytes, &b_bytes, &c_bytes]
            .iter()
            .map(|b| decode_all(b))
            .collect();
        let mut all_ids: BTreeMap<[u8; 16], ()> = BTreeMap::new();
        for recs in &per_input {
            for r in recs {
                all_ids.insert(r.trace_id, ());
            }
        }
        let mut old_order: Vec<SpanRecord> = Vec::new();
        for id in all_ids.keys() {
            let mut recs: Vec<SpanRecord> = Vec::new();
            for recs_in in &per_input {
                recs.extend(recs_in.iter().filter(|r| &r.trace_id == id).cloned());
            }
            recs.sort_by_key(|r| r.start_ts_ns); // stable: exactly the old path
            old_order.extend(recs);
        }

        // Build the reference part through the same writer, identity,
        // input_set_hash, and part_index the compaction used.
        let ftr = footer::open(actual).expect("open actual part");
        assert_eq!(ftr.input_set_hash.len(), 32, "input_set_hash is 32 bytes");
        let mut ish = [0u8; 32];
        ish.copy_from_slice(&ftr.input_set_hash);
        let identity = compactor_identity(&bucket(), &CompactorConfig::default());
        let mut writer = RspanWriter::new(RspanConfig::default(), identity);
        for r in old_order {
            writer.push(r);
        }
        let reference = writer
            .finish_compacted(1, ish.to_vec(), 0)
            .expect("finish reference part");

        assert_eq!(
            reference.as_slice(),
            actual.as_ref(),
            "streaming merge output must be byte-identical to the stable-sort ordering"
        );
    }

    // --- keystone differential property test ---------------------------------

    #[derive(Debug, Clone)]
    struct SpanSpec {
        trace: u8,
        span: u8,
        start: i64,
        dur: i64,
        name: String,
        msg: Option<String>,
        attrs: Vec<(String, String)>,
    }

    fn attr_strategy() -> impl Strategy<Value = (String, String)> {
        let key = prop::sample::select(vec!["k0", "k1", "k2", "svc"]).prop_map(String::from);
        let val = prop::sample::select(vec!["p", "q", "r", "s"]).prop_map(String::from);
        (key, val)
    }

    fn spec_strategy() -> impl Strategy<Value = SpanSpec> {
        (
            0u8..4,
            any::<u8>(),
            0i64..40,
            0i64..1000,
            prop::sample::select(vec!["get", "put", "query", "flush"]),
            prop::option::of("[a-z ]{0,8}"),
            prop::collection::vec(attr_strategy(), 0..4),
        )
            .prop_map(|(trace, span, start, dur, name, msg, attrs)| SpanSpec {
                trace,
                span,
                start,
                dur,
                name: name.into(),
                msg,
                attrs,
            })
    }

    /// Attribute keys are drawn from a small set, so a generated spec can name
    /// one twice. The writer keeps one entry per key, so a record built with a
    /// repeated key decodes back narrower than it was written and its
    /// [`estimate_record`] shrinks with it. Dedup here, where specs become
    /// records, so the records seeded into L0 are the records the merge sees and
    /// the part-count arithmetic in [`expected_part_count`] is exact.
    fn spec_to_record(s: &SpanSpec) -> SpanRecord {
        let mut seen: BTreeSet<&String> = BTreeSet::new();
        let mut attrs: Vec<(String, String)> = Vec::with_capacity(s.attrs.len());
        for (k, v) in &s.attrs {
            if seen.insert(k) {
                attrs.push((k.clone(), v.clone()));
            }
        }
        SpanRecord {
            trace_id: [s.trace; 16],
            span_id: [s.span; 8],
            parent_span_id: None,
            name: s.name.clone(),
            start_ts_ns: s.start,
            end_ts_ns: s.start.saturating_add(s.dur),
            status_code: StatusCode::Unset,
            status_message: s.msg.clone(),
            attrs,
        }
    }

    fn corpus_strategy() -> impl Strategy<Value = Vec<Vec<SpanSpec>>> {
        // 2..=5 inputs, each 1..=15 spans.
        prop::collection::vec(prop::collection::vec(spec_strategy(), 1..15), 2..6)
    }

    /// The part cap the boundary test runs under: small enough that a corpus of
    /// a few dozen spans splits, large enough that a part still holds several
    /// traces.
    const BOUNDARY_MEMORY_CAP: u64 = 512;

    /// How many L1 parts [`merge`] must produce for `corpus` at
    /// `l1_part_memory_target_bytes = memory_cap`, derived from the split rule
    /// rather than observed: records merge in `(trace_id, start_ts_ns)` order,
    /// so one trace's records are contiguous, and the in-progress part is closed
    /// at a trace transition once its [`estimate_record`] total has reached the
    /// cap. Only whole traces and their heap totals decide the boundaries, so
    /// the intra-trace merge order does not enter this count.
    fn expected_part_count(corpus: &[Vec<SpanSpec>], memory_cap: u64) -> usize {
        let mut by_trace: BTreeMap<[u8; 16], u64> = BTreeMap::new();
        for input in corpus {
            for spec in input {
                let r = spec_to_record(spec);
                let e = by_trace.entry(r.trace_id).or_insert(0);
                *e = e.saturating_add(estimate_record(&r));
            }
        }
        let mut parts = 0usize;
        let mut open: u64 = 0;
        for trace_bytes in by_trace.into_values() {
            if open >= memory_cap {
                parts += 1;
                open = 0;
            }
            open = open.saturating_add(trace_bytes);
        }
        if open > 0 {
            parts += 1;
        }
        parts
    }

    /// Compact `corpus` under `memory_cap` and return the number of L1 parts it
    /// produced.
    ///
    /// The cap goes to `l1_part_memory_target_bytes`, the field [`merge`] reads.
    /// It used to be written to `max_l1_part_bytes`, which the RSPAN merge does
    /// not consult at all, so every corpus produced one part and the
    /// part-boundary case below crossed no boundary. The part-count assertion
    /// keeps that silent: a cap that stops reaching the split rule shows up as a
    /// count mismatch, not as a property that holds vacuously.
    async fn differential_check(corpus: Vec<Vec<SpanSpec>>, memory_cap: u64) -> usize {
        let store = MemoryStore::new();
        let mut all_input_records: Vec<SpanRecord> = Vec::new();
        for (i, input) in corpus.iter().enumerate() {
            let records: Vec<SpanRecord> = input.iter().map(spec_to_record).collect();
            all_input_records.extend(records.clone());
            seed(
                &store,
                Uuid::from_u128((i + 1) as u128),
                (i + 1) as u64,
                &records,
            )
            .await;
        }

        let clock = FixedClock::new(sealed_now_ns());
        let config = CompactorConfig {
            l1_part_memory_target_bytes: memory_cap,
            ..CompactorConfig::default()
        };
        compact_bucket(&store, &clock, &config, &bucket())
            .await
            .expect("compact");
        let (rec, parts) = read_output(&store).await;
        assert_eq!(rec.level, 1);
        assert_eq!(
            parts.len(),
            expected_part_count(&corpus, memory_cap),
            "the cap must reach the merge's split rule: part count is arithmetic over \
             the corpus's per-trace heap"
        );

        // Decode every L1 part (concatenated in part order) and compare its
        // record set to the inputs decoded directly, as an order-independent
        // canonical multiset (the correctness core).
        let mut l1: Vec<SpanRecord> = Vec::new();
        for p in &parts {
            l1.extend(decode_all(p));
        }
        assert_eq!(
            canon_multiset(&l1),
            canon_multiset(&all_input_records),
            "L1 decoded set must equal the input union"
        );

        // Within each part, records are in (trace_id, start_ts) order.
        for p in &parts {
            let recs = decode_all(p);
            let order: Vec<([u8; 16], i64)> =
                recs.iter().map(|r| (r.trace_id, r.start_ts_ns)).collect();
            let mut sorted = order.clone();
            sorted.sort();
            assert_eq!(order, sorted, "part records in (trace, start_ts) order");
        }

        parts.len()
    }

    /// A corpus whose per-trace heap totals actually cross
    /// [`BOUNDARY_MEMORY_CAP`], so the boundary property below has a boundary to
    /// cross. Corpora of a couple of tiny single-trace spans cannot split (a
    /// trace never straddles a part) and are filtered out rather than asserted
    /// against.
    fn splitting_corpus_strategy() -> impl Strategy<Value = Vec<Vec<SpanSpec>>> {
        corpus_strategy().prop_filter(
            "corpus must split into more than one part at the boundary cap",
            |c| expected_part_count(c, BOUNDARY_MEMORY_CAP) > 1,
        )
    }

    /// A fixed corpus at [`BOUNDARY_MEMORY_CAP`] splits into a known number of
    /// parts, so the boundary-crossing differential above is provably armed at
    /// one concrete input rather than only at whatever the generator samples.
    ///
    /// Demonstrated red by writing the cap to `max_l1_part_bytes` (the field the
    /// RSPAN merge does not read) in `differential_check`: the merge then runs
    /// at the 256 MiB memory default, the whole corpus lands in one part, and
    /// this fails with `1 != 4`.
    #[tokio::test]
    async fn boundary_corpus_splits_into_several_parts() {
        // 3 inputs x 4 traces x 5 spans: every trace's heap is a few hundred
        // bytes, so a 512-byte cap closes a part every second trace or so.
        let corpus: Vec<Vec<SpanSpec>> = (0..3)
            .map(|input| {
                (0..4u8)
                    .flat_map(|trace| {
                        (0..5u8).map(move |s| SpanSpec {
                            trace,
                            span: s,
                            start: i64::from(s) * 10 + input,
                            dur: 5,
                            name: "query".into(),
                            msg: Some("msg-abcd".into()),
                            attrs: vec![("k0".into(), "p".into()), ("k1".into(), "q".into())],
                        })
                    })
                    .collect()
            })
            .collect();

        let expected = expected_part_count(&corpus, BOUNDARY_MEMORY_CAP);
        assert_eq!(
            expected, 4,
            "fixture must cross several part boundaries at the boundary cap"
        );
        let parts = differential_check(corpus, BOUNDARY_MEMORY_CAP).await;
        assert_eq!(parts, expected, "observed part count must match the rule");
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 24, ..ProptestConfig::default() })]

        /// The correctness core (ADR-0041): for a random corpus of span records
        /// split across N L0 objects, the full decoded record set is identical
        /// whether the N L0 inputs are decoded and concatenated or the single
        /// compacted L1 output is decoded. Default part cap: a single L1 part.
        #[test]
        fn differential_l0_union_equals_l1_output(corpus in corpus_strategy()) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(differential_check(
                corpus,
                CompactorConfig::default().l1_part_memory_target_bytes,
            ));
        }

        /// The same union-equality property under a tiny part cap that forces
        /// the merge to split across multiple parts on trace boundaries, so the
        /// "concatenate all parts" side of the differential actually crosses
        /// part boundaries. The generator only yields corpora that do split, and
        /// the part count is asserted here, so a cap that stops reaching the
        /// merge fails this test instead of making it pass vacuously.
        #[test]
        fn differential_holds_across_part_boundaries(corpus in splitting_corpus_strategy()) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let parts = rt.block_on(differential_check(corpus, BOUNDARY_MEMORY_CAP));
            prop_assert!(parts > 1, "boundary corpus must produce more than one part, got {parts}");
        }
    }
}
