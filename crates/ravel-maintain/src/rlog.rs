//! The RLOG side of the codec seam (ADR-0032, issue #231): L0-to-L1 log
//! segment compaction.
//!
//! # What the merge does (docs/log-segment-format.md, ADR-0032)
//!
//! An L1 `.rlog` part is the sorted union of its inputs' records, re-blocked
//! from scratch. Concretely the merge:
//!
//! - takes the sorted `STREAM_DIR`s of all inputs and forms one merged, sorted
//!   stream set (the *global `stream_ref` remap* -- every input's local
//!   `stream_ref` values are renumbered into this one directory);
//! - checks the cross-object stream-identity invariant explicitly: two inputs
//!   may list the same `stream_id` only with byte-identical resource+scope
//!   blobs, because `stream_id` is the canonical hash of exactly those bytes
//!   (a disagreement is an upstream bug or a hash collision, and a merge is the
//!   first place it becomes visible across objects) -- a mismatch is a typed
//!   [`MaintainError::StreamAttrsConflict`], never a silent pick;
//! - re-sorts the merged record set by `(stream_ref, ts)` ascending, rebuilds
//!   `FIELD_DIR` from the merged column set under the same 1000-dynamic-column
//!   cap with overflow folded into `attrs_raw`, and rebuilds `SKIP_IDX`, the
//!   per-block `BLOOM`s, and `POSTINGS` over the merged, re-blocked contents at
//!   the same 8192 record block target.
//!
//! # POSTINGS is rebuilt, never merged (ADR-0049 decision 6, issue #509)
//!
//! A POSTINGS posting list holds *block indices*, and the merge re-blocks every
//! record, so an input's block indices describe nothing in the output. Nothing
//! from an input's POSTINGS section is ever copied, concatenated, or shifted
//! into the output: the output's postings are built by the writer from the
//! output's own blocks, exactly like `FIELD_DIR`, `SKIP_IDX`, and `BLOOM`.
//!
//! Two consequences follow from rebuilding on the merged object:
//!
//! - The per-field distinct-value cap (`RlogConfig::postings_max_distinct`)
//!   applies to the *merged* object. A merged object can exceed it while no
//!   single input does, exactly as the 1000-dynamic-column cap already behaves
//!   on merge. When it fires the field is simply not indexed in the output, and
//!   results do not change: POSTINGS pruning is widen-only (ADR-0013), so an
//!   unindexed field prunes nothing and the field stays queryable through
//!   `BLOOM` plus the exact scan.
//! - The output's *indexed-field list* is recovered from the inputs
//!   ([`RlogCodec::load_input_catalog`]), because per-tenant configuration of
//!   that list is issue #511 and does not exist yet. See
//!   [`input_indexed_fields`] for what is recovered and how.
//!
//! # Reuse, not reimplementation
//!
//! Every one of those encode steps is exactly what [`ravel_logseg::RlogWriter`]
//! already does for a single-object L0 write. So the merge does not re-derive
//! any of them: it decodes each input's records back to [`LogRecord`]s with
//! [`RlogReader`], pushes the merged records into a fresh `RlogWriter`, and
//! calls [`RlogWriter::finish_compacted`] to stamp `level = 1`, the compaction
//! `input_set_hash`, and the `part_index`. The dynamic-column cap, the
//! `attrs_raw` overflow encoding, the bloom sizing rule (per-block, sized by
//! that block's own token cardinality), and the block framing all come from the
//! one writer implementation, so an L0 write and an L1 merge cannot drift. The
//! only ravel-logseg addition this required is `finish_compacted` itself.
//!
//! # Memory (issue #745, ADR-0065 decision 4)
//!
//! The read side ([`RlogCodec::load_input_catalog`]) retains only per-input
//! catalog metadata: a [`RlogRangeReader`] holding the decoded STREAM_DIR,
//! FIELD_DIR, and SKIP_IDX plus the object key, never block or bloom bytes.
//!
//! The merge itself is a **k-way block-streaming merge**, so its peak resident
//! memory is bounded independently of any one stream's size -- the defect
//! issue #745 fixed. Issue #275 already fetched one stream's blocks by range
//! rather than whole objects, but it then fully materialized *all* of that
//! stream's decoded records from every input into one `Vec` before returning,
//! and the part builder held a second fully decoded copy in its accumulator.
//! One hot, high-volume stream (a single busy service, the common case for
//! logs) can carry most of a sealed hour, so that made peak memory scale with
//! stream size.
//!
//! Instead, [`build_parts`] merges each stream through one
//! [`StreamCursor`] per input that decodes exactly one block at a time
//! ([`RlogRangeReader::stream_blocks`] +
//! [`RlogRangeReader::decode_block`]), fetching the next block's bytes by range
//! only once the previous block is drained. A record's `(stream_ref, ts)`
//! stored order makes each input's stream one ts-ascending sequence spread
//! across ascending blocks, so N inputs carrying the same stream are a standard
//! k-way merge ordered by `ts_ns`, ties broken by canonical input order. That
//! is byte-for-byte the ordering the old "gather everything then stable-sort by
//! `ts_ns`" produced, which matters because parts are content-addressed and any
//! reordering would change every downstream hash. Merged records feed straight
//! into the in-progress part's [`RlogWriter`]; there is no intermediate
//! `Vec<LogRecord>` batch.
//!
//! Peak resident memory is then: per-input catalog metadata (KBs per input,
//! unchanged from #275), plus at most one decoded block per input carrying the
//! current stream (`O(input_count * block_size)`, independent of stream size),
//! plus the in-progress part's writer buffer. The writer buffer is bounded by
//! `max_l1_part_bytes` -- a part is flushed on a stream boundary once its
//! record-byte estimate reaches the cap -- and holding a whole part before its
//! content-addressed key exists is unavoidable, not a regression. The
//! [`MergeMemoryTracker`] seam accounts these terms at their real
//! allocation/decode points and records a high-water mark; the acceptance test
//! asserts that mark stays under a fixed bound while a hot stream grows.
//!
//! The first RLOG merge held every input object whole (RLOG then had no ranged
//! section reader); [`ravel_logseg::open_from_suffix`] is now the RLOG analogue
//! of `ravel_segment::open_from_suffix` and closed that raw-bytes gap.

use std::collections::{BTreeMap, BTreeSet};

use ravel_commit::keys;
use ravel_logseg::field_dir::FieldDir;
use ravel_logseg::footer::{self, SuffixOutcome, kind};
use ravel_logseg::postings::PostingsSection;
use ravel_logseg::{
    AttrValue, LogRecord, LogStreamId, RlogConfig, RlogRangeReader, RlogWriter, StreamBlockLoc,
    decode_section, writer::ObjectIdentity,
};
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_proto::commit::v1::CompactionPart;

use crate::bucket::Bucket;
use crate::build::{BuiltPart, put_part};
use crate::codec::SegmentCodec;
use crate::config::{CompactorConfig, MergeMemoryTracker};
use crate::error::{MaintainError, Result};
use crate::read::InputRecord;

/// The RLOG output trailer version every L1 part carries (ADR-0032, trailer
/// version 2). Recorded in each part's `CompactionPart.segment_format_version`,
/// the log analogue of RSEG's [`crate::build::OUTPUT_FORMAT_VERSION`].
///
/// Tied to `ravel_logseg`'s own trailer version at compile time, for the reason
/// [`crate::rspan_codec::OUTPUT_FORMAT_VERSION`] records: as a mirrored literal
/// this goes stale on the next bump, and it goes stale silently, because the
/// writer stamps `footer::VERSION` into the trailer while this constant keeps
/// reporting the old number. The compactor would then write parts that claim a
/// version they are not.
pub const OUTPUT_FORMAT_VERSION: u32 = ravel_logseg::footer::VERSION as u32;

/// Untrusted-input cap on an input's FIELD_DIR entry count for the compactor's
/// own decode of that section (the indexed-field recovery below needs the
/// `column_id` -> name mapping). Same value as `ravel_logseg`'s internal
/// `MAX_FIELDS`, which is not exported; a real object holds at most
/// `RlogConfig::max_dynamic_columns` (1000) entries, so this is a sanity
/// ceiling, not a policy.
const MAX_FIELD_DIR_ENTRIES: u64 = 1 << 20;

/// The term a [`PostingsSection::probe`] uses purely to ask "is this column
/// indexed in this object?". Empty bytes sort at or before every real term, so
/// the probe is answered from the sparse index without decompressing a term
/// block (unless the field genuinely holds an empty-string term, in which case
/// one block decodes). Only the `Option` is read, never the block list.
const INDEXED_PROBE_TERM: &[u8] = &[];

/// One RLOG input's retained catalog metadata: the data-object key, a
/// [`RlogRangeReader`] over its directories (STREAM_DIR, FIELD_DIR, SKIP_IDX),
/// and the input's indexed-field names. This is all the read side retains; the
/// block/bloom bytes are fetched by range one stream at a time during the merge
/// (issue #275, docs/log-segment-format.md, this module's memory note). The
/// untrusted-input caps on the directory sections live inside the ranged reader.
#[derive(Debug, Clone)]
pub struct RlogInputCatalog {
    pub object_key: String,
    pub reader: RlogRangeReader,
    /// The dynamic attribute names this input carries a POSTINGS entry for, as
    /// recovered by [`input_indexed_fields`]. Only the *names* survive into the
    /// merge; no posting list from an input is ever read (ADR-0049 decision 6).
    pub indexed_fields: Vec<String>,
}

/// The logs codec: implements the [`SegmentCodec`] seam for `.rlog` objects.
pub struct RlogCodec;

impl SegmentCodec for RlogCodec {
    type Catalog = RlogInputCatalog;

    async fn load_input_catalog(
        store: &dyn ObjectStoreBackend,
        config: &CompactorConfig,
        input: &InputRecord,
    ) -> Result<Self::Catalog> {
        let object_key = keys::reconstruct_data_key(&input.record)?;
        let cfg = RlogConfig::default();

        // Locate the footer and section directory from a suffix probe: one
        // ranged GET, growing to a second only if the probe missed the footer
        // (the RLOG analogue of the RSEG read path, issue #275).
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
                            "rlog footer not covered by ranged fetch".into(),
                        ));
                    }
                }
            }
        };

        // Fetch and decode the three whole-read directory sections by range.
        // BLOCKS and BLOOM are never fetched here; the merge streams blocks by
        // range one stream at a time. FIELD_DIR is decoded (and validated) even
        // though the merge rebuilds it, so a corrupt input fails loud here.
        let stream_dir_raw =
            fetch_section(store, &object_key, &ftr, kind::STREAM_DIR, &cfg).await?;
        let field_dir_raw = fetch_section(store, &object_key, &ftr, kind::FIELD_DIR, &cfg).await?;
        let skip_idx_raw = fetch_section(store, &object_key, &ftr, kind::SKIP_IDX, &cfg).await?;

        // Which fields this input had indexed. Recovered here, while the
        // object's footer and FIELD_DIR bytes are already at hand, and reduced
        // immediately to a name list: the POSTINGS bytes themselves are dropped
        // before this function returns and never take part in the merge.
        let indexed_fields =
            input_indexed_fields(store, &object_key, &ftr, &field_dir_raw, &cfg).await?;

        let reader =
            RlogRangeReader::from_sections(&ftr, &stream_dir_raw, &field_dir_raw, &skip_idx_raw)?;
        Ok(RlogInputCatalog {
            object_key,
            reader,
            indexed_fields,
        })
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

        // Global stream_ref remap + cross-object stream-identity check. The
        // merged set is the sorted union of every input's STREAM_DIR; the dense
        // merged stream_ref is the ordinal in this set (the writer re-derives it
        // per part, so we need only the ordering here). Two inputs claiming the
        // same stream_id with different blobs is a fatal invariant breach.
        let mut merged: BTreeMap<LogStreamId, Vec<u8>> = BTreeMap::new();
        for catalog in &catalogs {
            for entry in catalog.reader.stream_dir().entries() {
                match merged.entry(entry.stream_id) {
                    std::collections::btree_map::Entry::Vacant(slot) => {
                        slot.insert(entry.blob.clone());
                    }
                    std::collections::btree_map::Entry::Occupied(slot) => {
                        if slot.get() != &entry.blob {
                            return Err(MaintainError::StreamAttrsConflict {
                                stream_id: entry.stream_id.to_hex(),
                                a_len: slot.get().len(),
                                b_len: entry.blob.len(),
                            });
                        }
                    }
                }
            }
        }

        // No whole-object fetch: the per-input ranged readers are already in the
        // catalogs. Each stream's blocks are fetched by range one block at a
        // time in the k-way merge below, so raw resident bytes stay bounded to
        // one block per input, never a whole stream or the whole bucket
        // (issue #745, building on issue #275).
        let identity = compactor_identity(bucket, config);
        // The output's indexed-field list: the union of the inputs' (issue #509,
        // and see `input_indexed_fields`). Every part of this compaction gets
        // the same list, so a field is indexed uniformly across the output.
        let indexed_fields = merged_indexed_fields(&catalogs);
        let tracker = config.merge_memory_tracker.as_ref();
        let mut parts = Vec::new();
        let mut part_index: u32 = 0;
        let mut current: Option<PartBuilder> = None;

        // Merge stream by stream in sorted stream_id order. Each stream is
        // k-way merged from every input carrying it (ts-ascending, canonical
        // input-order tie-break) straight into the current part's writer; a
        // part flushes on a stream boundary once its accumulated record-byte
        // estimate reaches the size cap (so a stream never straddles two parts,
        // the log analogue of RSEG's series-boundary split). No intermediate
        // `Vec<LogRecord>` and no per-stream dedup: distinct submissions of
        // identical content are distinct records (ADR-0032).
        for stream_id in merged.keys() {
            let part = current.get_or_insert_with(|| PartBuilder::new(&identity, &indexed_fields));
            merge_stream_into_part(store, &catalogs, stream_id, part, tracker).await?;
            // Flush on this stream boundary once the part reaches the cap. The
            // `take` is `Some` here (just inserted above); the `if let` avoids
            // an `unwrap`/`expect` on the critical path rather than asserting.
            let over_cap = part.estimate >= config.max_l1_part_bytes && !part.is_empty();
            if over_cap && let Some(builder) = current.take() {
                let built = builder
                    .finish(store, bucket, input_set_hash, part_index, config.dry_run)
                    .await?;
                parts.push(built);
                part_index += 1;
                if let Some(t) = tracker {
                    t.set_writer_bytes(0);
                }
            }
        }
        if let Some(part) = current
            && !part.is_empty()
        {
            let built = part
                .finish(store, bucket, input_set_hash, part_index, config.dry_run)
                .await?;
            parts.push(built);
            if let Some(t) = tracker {
                t.set_writer_bytes(0);
            }
        }

        Ok(parts)
    }
}

/// One in-progress L1 part: the shared [`RlogWriter`] the merged records push
/// into directly, plus the running record-byte estimate that decides where the
/// part splits (a stream boundary once the estimate reaches `max_l1_part_bytes`)
/// and feeds the [`MergeMemoryTracker`]'s writer term. Holding a whole part's
/// records before [`PartBuilder::finish`] stamps its content-addressed key is
/// unavoidable (the key is a hash of the whole object); the k-way merge is what
/// keeps everything *else* bounded (issue #745).
struct PartBuilder {
    writer: RlogWriter,
    /// Sum of [`estimate_record`] over every pushed record: the split trigger
    /// (matching the old accumulator's `batch_bytes`) and the writer-buffer
    /// term the tracker records.
    estimate: u64,
    min_stream: Option<LogStreamId>,
    max_stream: Option<LogStreamId>,
    count: usize,
}

impl PartBuilder {
    fn new(identity: &ObjectIdentity, indexed_fields: &[String]) -> Self {
        let writer = RlogWriter::new(RlogConfig::default(), *identity)
            .with_indexed_fields(indexed_fields.to_vec());
        PartBuilder {
            writer,
            estimate: 0,
            min_stream: None,
            max_stream: None,
            count: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Push one merged record into the part's writer, updating the running
    /// estimate and the part's inclusive stream-id bounds.
    fn push(&mut self, r: LogRecord, tracker: Option<&MergeMemoryTracker>) -> Result<()> {
        self.min_stream = Some(self.min_stream.map_or(r.stream_id, |m| m.min(r.stream_id)));
        self.max_stream = Some(self.max_stream.map_or(r.stream_id, |m| m.max(r.stream_id)));
        self.estimate = self.estimate.saturating_add(estimate_record(&r));
        self.count += 1;
        self.writer.push(r)?;
        if let Some(t) = tracker {
            t.set_writer_bytes(self.estimate);
        }
        Ok(())
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
            self.min_stream,
            self.max_stream,
            input_set_hash,
            part_index,
            dry_run,
        )
        .await
    }
}

/// One input's cursor over a single stream's records, yielding them in stored
/// (ts-ascending) order one block at a time. At most one decoded block is
/// resident: `head` is the next record to merge, `block` holds the rest of the
/// current block, and the next block's bytes are fetched by range only once the
/// current block is drained (issue #745). `input_index` is the cursor's
/// canonical position in `catalogs`, the k-way merge's tie-break on equal
/// `ts_ns`.
struct StreamCursor<'a> {
    input_index: usize,
    object_key: &'a str,
    reader: &'a RlogRangeReader,
    /// Candidate blocks for the stream, ascending (ts-ascending for the
    /// stream), consumed front to back via `next_loc`.
    locs: Vec<StreamBlockLoc>,
    next_loc: usize,
    /// Remaining records of the current decoded block.
    block: std::vec::IntoIter<LogRecord>,
    /// The current block's decoded-byte estimate, held live in the tracker
    /// until the block is released.
    block_bytes: u64,
    /// The next record to merge, or `None` once the cursor is exhausted.
    head: Option<LogRecord>,
}

impl<'a> StreamCursor<'a> {
    /// Open a cursor over `stream_id` in `catalog`, or `None` if the input does
    /// not carry the stream. Does not fetch yet; call [`Self::refill`] once to
    /// load the first record.
    fn open(
        catalog: &'a RlogInputCatalog,
        input_index: usize,
        stream_id: &LogStreamId,
    ) -> Result<Option<Self>> {
        let Some(locs) = catalog.reader.stream_blocks(stream_id)? else {
            return Ok(None);
        };
        Ok(Some(StreamCursor {
            input_index,
            object_key: &catalog.object_key,
            reader: &catalog.reader,
            locs,
            next_loc: 0,
            block: Vec::new().into_iter(),
            block_bytes: 0,
            head: None,
        }))
    }

    /// Take the current head record; the caller must [`Self::refill`] before
    /// the next merge step.
    fn take_head(&mut self) -> Option<LogRecord> {
        self.head.take()
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

    /// Load the next record into `head`: the next record of the current block,
    /// or the first record of the next non-empty block (fetched by range and
    /// decoded here), or `None` when the stream is exhausted in this input. At
    /// most one block's raw bytes and one decoded block are resident at a time.
    async fn refill(
        &mut self,
        store: &dyn ObjectStoreBackend,
        tracker: Option<&MergeMemoryTracker>,
    ) -> Result<()> {
        if let Some(rec) = self.block.next() {
            self.head = Some(rec);
            return Ok(());
        }
        // Current block drained: release it and pull the next non-empty block.
        self.release_block(tracker);
        while self.next_loc < self.locs.len() {
            let loc = self.locs[self.next_loc].clone();
            self.next_loc += 1;
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
            // A candidate block with no row for this stream (a neighbour's
            // boundary block): release and try the next.
            self.release_block(tracker);
        }
        self.head = None;
        Ok(())
    }
}

/// K-way merge one stream from every input carrying it into `part`, in
/// ts-ascending order with ties broken by canonical input order. This is
/// byte-for-byte the ordering the old "concatenate every input's decoded
/// records, then stable-sort by `ts_ns`" produced -- each input's stream is
/// already ts-ascending (the format's `(stream_ref, ts)` order), so a stable
/// sort of the concatenation orders equal-`ts_ns` records by (input, position),
/// exactly what selecting the minimum `(ts_ns, input_index)` head does here.
async fn merge_stream_into_part(
    store: &dyn ObjectStoreBackend,
    catalogs: &[RlogInputCatalog],
    stream_id: &LogStreamId,
    part: &mut PartBuilder,
    tracker: Option<&MergeMemoryTracker>,
) -> Result<()> {
    let mut cursors: Vec<StreamCursor> = Vec::new();
    for (idx, catalog) in catalogs.iter().enumerate() {
        if let Some(mut cursor) = StreamCursor::open(catalog, idx, stream_id)? {
            cursor.refill(store, tracker).await?;
            if cursor.head.is_some() {
                cursors.push(cursor);
            }
        }
    }
    loop {
        // Pick the cursor whose head has the minimum (ts_ns, input_index).
        // input_index is unique per cursor, so the key is a total order and the
        // tie-break is deterministic.
        let mut best: Option<(usize, i64, usize)> = None;
        for (i, cursor) in cursors.iter().enumerate() {
            if let Some(head) = &cursor.head {
                let key = (head.ts_ns, cursor.input_index);
                match best {
                    Some((_, bts, bidx)) if (bts, bidx) <= key => {}
                    _ => best = Some((i, head.ts_ns, cursor.input_index)),
                }
            }
        }
        let Some((bi, _, _)) = best else {
            break;
        };
        if let Some(rec) = cursors[bi].take_head() {
            part.push(rec, tracker)?;
        }
        cursors[bi].refill(store, tracker).await?;
    }
    Ok(())
}

/// Encode one in-progress part's writer into an L1 object and PUT it: run the
/// shared writer pipeline via [`RlogWriter::finish_compacted_with_stats`]
/// (stamping `level = 1`, the `input_set_hash`, and `part_index`), then PUT it
/// `CreateIfAbsent`. The part's summary stats are read back from the produced
/// object's own footer, so they describe exactly what was written.
///
/// The writer was built with the same [`RlogWriter::with_indexed_fields`] the
/// L0 write path uses, so this part's POSTINGS is built by the one writer
/// implementation from this part's own blocks (ADR-0049 decision 6, issue
/// #509). The per-field distinct-value cap therefore applies to the merged
/// part; when it fires the writer drops that field's postings and reports it in
/// `WriteStats`, which is logged here because a silently unindexed field is
/// invisible in the object bytes (they are simply absent, which is always
/// legal).
///
/// `first_stream_id`/`last_stream_id` are the part's inclusive stream-id bounds
/// accumulated as records were pushed (streams are merged in sorted id order,
/// so `first` is the smallest and `last` the largest id in the part).
#[allow(clippy::too_many_arguments)]
async fn finalize_part(
    store: &dyn ObjectStoreBackend,
    bucket: &Bucket,
    writer: RlogWriter,
    first_stream_id: Option<LogStreamId>,
    last_stream_id: Option<LogStreamId>,
    input_set_hash: &[u8; 32],
    part_index: u32,
    dry_run: bool,
) -> Result<BuiltPart> {
    let (object, stats) =
        writer.finish_compacted_with_stats(1, input_set_hash.to_vec(), part_index)?;
    if stats.postings_capped_fields > 0 {
        tracing::warn!(
            part_index,
            capped_fields = stats.postings_capped_fields,
            "rlog compaction dropped POSTINGS for fields over the distinct-value cap in the merged part"
        );
    }
    let object = bytes::Bytes::from(object);

    // Authoritative summary from the object we just wrote.
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

    let part = CompactionPart {
        part_index,
        first_series_id: first_stream_id.map(|s| s.0.to_vec()).unwrap_or_default(),
        last_series_id: last_stream_id.map(|s| s.0.to_vec()).unwrap_or_default(),
        content_hash: content_hash.to_vec(),
        object_size: object.len() as u64,
        sample_count: ftr.record_count,
        series_count: ftr.stream_count,
        // Logs have no run concept (no cross-record dedup runs, ADR-0032); the
        // per-record count already lives in sample_count.
        run_count: 0,
        min_event_ts_ns: ftr.min_ts_ns,
        max_event_ts_ns: ftr.max_ts_ns,
        segment_format_version: OUTPUT_FORMAT_VERSION,
    };
    let built = BuiltPart {
        key,
        bytes: object,
        part,
    };
    if !dry_run {
        put_part(store, &built).await?;
    }
    Ok(built)
}

/// The compactor's object identity for an L1 part. `writer_epoch`/`writer_seq`
/// are zero and `writer_id` is the compactor's uuid: informational only, never
/// part of any identity or dedup order (RLOG has none), matching the RSEG L1
/// writer's identity convention (`build.rs`).
fn compactor_identity(bucket: &Bucket, config: &CompactorConfig) -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: bucket.tenant_hash.0,
        shard: bucket.shard,
        writer_id: config.compactor_writer_id.into_bytes(),
        writer_epoch: 0,
        writer_seq: 0,
    }
}

/// A rough uncompressed byte estimate for one record, for the part size cap.
/// Approximate on purpose (like RSEG's accumulated page-byte estimate): it only
/// decides where parts split, never correctness.
fn estimate_record(r: &LogRecord) -> u64 {
    let mut est: u64 = 48; // fixed-column overhead per row
    est += r.body.len() as u64;
    est += r.severity_text.len() as u64;
    for (k, v) in &r.attrs {
        est += k.len() as u64 + attr_value_estimate(v);
    }
    est
}

fn attr_value_estimate(v: &AttrValue) -> u64 {
    match v {
        AttrValue::Str(s) => s.len() as u64 + 2,
        AttrValue::Bytes(b) => b.len() as u64 + 2,
        AttrValue::List(items) => items.iter().map(attr_value_estimate).sum::<u64>() + 2,
        AttrValue::Map(kvs) => {
            kvs.iter()
                .map(|(k, v)| k.len() as u64 + attr_value_estimate(v))
                .sum::<u64>()
                + 2
        }
        _ => 8,
    }
}

/// The dynamic attribute names one input object carries POSTINGS for.
///
/// # Why the inputs are the source (issue #509 deliverable 3)
///
/// ADR-0049 decision 3 makes the indexed-field list explicit per-tenant
/// configuration, and that configuration is issue #511: it is not on main, so
/// the compactor has no tenant-scoped list to read and must not invent one.
/// What it does have is the inputs, each of which already records the decision
/// its writer was configured with. So the output indexes what its inputs
/// indexed: the same field list, applied to the merged blocks.
///
/// Recovery goes through the public reader surface, never a second parser: the
/// POSTINGS section is fetched by range, crc-verified and parsed by
/// [`PostingsSection::parse`], and each of the input's FIELD_DIR columns is
/// probed with [`INDEXED_PROBE_TERM`]. `Ok(Some(_))` means that column has a
/// POSTINGS entry, so its name was in the writer's list. Only the name is kept;
/// the section bytes and every posting list in them are dropped here.
///
/// Two known edges, both benign and both widen-only:
///
/// - A field the writer indexed but that hit the distinct-value cap in *this*
///   input reads back as `Ok(None)` ([`PostingsSection::probe`] cannot
///   distinguish "capped" from "never indexed"), so its name is not recovered.
///   The merged object holds a superset of that input's values for the field, so
///   it would exceed the same cap anyway and the field would be dropped from the
///   output either way; the only difference is a `Capped` marker entry versus no
///   entry, and both read back as "not indexed".
/// - A field whose name is in the writer's list but which never appeared in this
///   input (or overflowed the 1000-dynamic-column budget) has no column and so
///   no name to recover. Also legal: an unindexed field prunes nothing.
///
/// A corrupt POSTINGS section is a loud error, matching how FIELD_DIR is decoded
/// (and validated) above even though the merge rebuilds it: a query can degrade
/// past corruption because absence is legal, but a compactor rewriting the
/// object should not quietly bake a corrupt input's damage into the output.
///
/// Issue #511 replaces this whole function with a read of the tenant's
/// configured list. That changes behaviour in two ways the inputs cannot express:
/// a newly configured field becomes indexed at the next compaction even though
/// no input indexed it, and a de-configured field stops being indexed even
/// though its inputs still carry postings.
async fn input_indexed_fields(
    store: &dyn ObjectStoreBackend,
    object_key: &str,
    ftr: &footer::LogFooter,
    field_dir_raw: &[u8],
    cfg: &RlogConfig,
) -> Result<Vec<String>> {
    // POSTINGS is optional (ADR-0049 decision 5): no section means the input
    // indexed nothing, so there is nothing to carry forward.
    let Some(desc) = ftr.section(kind::POSTINGS) else {
        return Ok(Vec::new());
    };
    let got = store
        .get(
            object_key,
            GetRange::Range(desc.offset, desc.offset + desc.len),
        )
        .await?;
    let raw = decode_section(got.data.as_ref(), desc, cfg)?;
    let section = PostingsSection::parse(&raw)?;
    let field_dir = FieldDir::decode(field_dir_raw, MAX_FIELD_DIR_ENTRIES)?;
    let mut names: BTreeSet<String> = BTreeSet::new();
    for entry in field_dir.entries() {
        if section
            .probe(entry.column_id, INDEXED_PROBE_TERM)?
            .is_some()
        {
            names.insert(entry.name.clone());
        }
    }
    Ok(names.into_iter().collect())
}

/// The output's indexed-field list: the union of the inputs' recovered lists,
/// sorted and deduplicated. Union, not intersection: a field indexed by one
/// input stays indexed in the output, and indexing a field no other input
/// indexed is never wrong (its postings are built from the merged blocks like
/// any other field's). Deterministic ordering keeps identical inputs producing
/// byte-identical output.
fn merged_indexed_fields(catalogs: &[RlogInputCatalog]) -> Vec<String> {
    let mut names: BTreeSet<&str> = BTreeSet::new();
    for catalog in catalogs {
        for name in &catalog.indexed_fields {
            names.insert(name.as_str());
        }
    }
    names.into_iter().map(str::to_string).collect()
}

/// Fetch one required whole-read section by range and return its decompressed,
/// crc-verified bytes. Fetches exactly `[offset, offset + len)` (the section's
/// stored bytes), never the whole object.
async fn fetch_section(
    store: &dyn ObjectStoreBackend,
    object_key: &str,
    ftr: &footer::LogFooter,
    k: u32,
    cfg: &RlogConfig,
) -> Result<Vec<u8>> {
    let desc = ftr.section(k).ok_or_else(|| {
        MaintainError::Invariant(format!("input .rlog object missing section kind {k}"))
    })?;
    let got = store
        .get(
            object_key,
            GetRange::Range(desc.offset, desc.offset + desc.len),
        )
        .await?;
    Ok(decode_section(got.data.as_ref(), desc, cfg)?)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::collections::BTreeSet;

    use bytes::Bytes;
    use proptest::prelude::*;
    use prost::Message;
    use ravel_commit::record::{self, NewCommitRecord};
    use ravel_logseg::field_dir::FieldDir;
    use ravel_logseg::record::FieldType;
    use ravel_logseg::skip_index::SkipIndex;
    use ravel_logseg::{FieldSel, RlogConfig, RlogReader, RlogWriter, footer, read_section};
    use ravel_logseg::{LogRecord, Predicate, stream_attrs_bytes, writer::ObjectIdentity};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, list_all};
    use ravel_proto::commit::v1::CompactionRecord;
    use ravel_types::logstream::{AttrValue, LogStreamId, canonical_attr_bytes, log_stream_id};
    use ravel_types::{Signal, TenantHash, TenantId};
    use uuid::Uuid;

    use super::*;
    use crate::{
        Bucket, CompactionOutcome, CompactorConfig, FixedClock, MergeMemoryTracker, compact_bucket,
    };

    /// FIELD_DIR entry-count cap for the test-only `field_dir_len` decode (a
    /// real object never approaches it; the reader carries its own copy).
    const MAX_FIELDS: u64 = 1 << 20;

    const TENANT: &str = "acme";
    const SHARD: u32 = 7;
    const HOUR: u32 = 495_000;
    const NS_PER_HOUR: i64 = 3_600_000_000_000;
    const EPOCH: u64 = 10;

    fn tenant_hash() -> TenantHash {
        TenantId::new(TENANT).hash()
    }

    fn bucket() -> Bucket {
        Bucket::new(tenant_hash(), Signal::Logs, SHARD, HOUR)
    }

    /// Past the seal margin for [`HOUR`] under default config.
    fn sealed_now_ns() -> i64 {
        (i64::from(HOUR) + 1) * NS_PER_HOUR + 2 * NS_PER_HOUR
    }

    /// A synthetic stream `n`'s id and canonical resource+scope blob. Distinct
    /// per `n`, and the id is the true hash of the blob, so the object records
    /// real stream identity, never a placeholder.
    fn stream_ident(n: u32) -> (LogStreamId, Vec<u8>) {
        let res = vec![(
            "service.name".to_string(),
            AttrValue::Str(format!("svc{n}")),
        )];
        let id = log_stream_id(&res, "scope", "1", &[]);
        let blob = stream_attrs_bytes(&res, "scope", "1", &[]);
        (id, blob)
    }

    fn record(stream_n: u32, ts: i64, body: &str, attrs: Vec<(String, AttrValue)>) -> LogRecord {
        let (stream_id, stream_attrs) = stream_ident(stream_n);
        LogRecord {
            stream_id,
            stream_attrs,
            ts_ns: ts,
            observed_ts_ns: ts,
            severity_num: 9,
            severity_text: "INFO".into(),
            body: body.into(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs,
        }
    }

    /// Seed one L0 `.rlog` input (data object + commit record), exactly as the
    /// ingest log shard would (`ravel-ingest`), and return the object bytes so a
    /// test can decode the input directly for a differential check.
    async fn seed(
        store: &dyn ObjectStoreBackend,
        writer_id: Uuid,
        seq: u64,
        records: &[LogRecord],
    ) -> Bytes {
        seed_l0(store, writer_id, seq, records, RlogConfig::default(), &[]).await
    }

    /// [`seed`] with the L0 writer's config and POSTINGS field list under test
    /// control: `cfg` so an input can be blocked differently from the 8192-record
    /// output (the compactor always writes with `RlogConfig::default()`), and
    /// `indexed` so an input carries POSTINGS at all.
    async fn seed_l0(
        store: &dyn ObjectStoreBackend,
        writer_id: Uuid,
        seq: u64,
        records: &[LogRecord],
        cfg: RlogConfig,
        indexed: &[&str],
    ) -> Bytes {
        let th = tenant_hash();
        let identity = ObjectIdentity {
            tenant_hash: th.0,
            shard: SHARD,
            writer_id: writer_id.into_bytes(),
            writer_epoch: EPOCH,
            writer_seq: seq,
        };
        let mut w = RlogWriter::new(cfg, identity)
            .with_indexed_fields(indexed.iter().map(|s| (*s).to_string()).collect());
        for r in records {
            w.push(r.clone()).expect("push");
        }
        let bytes = Bytes::from(w.finish().expect("finish L0"));
        let content_hash: [u8; 32] = *blake3::hash(&bytes).as_bytes();
        let data_key = keys::data_key(
            &th,
            Signal::Logs,
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

        let mut ids: BTreeSet<LogStreamId> = BTreeSet::new();
        let mut min_ts = i64::MAX;
        let mut max_ts = i64::MIN;
        for r in records {
            ids.insert(r.stream_id);
            min_ts = min_ts.min(r.ts_ns);
            max_ts = max_ts.max(r.ts_ns);
        }
        let created = i64::from(HOUR) * NS_PER_HOUR + (seq as i64) * 1_000_000;
        let rec = record::build(NewCommitRecord {
            tenant_hash: th,
            signal: Signal::Logs,
            shard: SHARD,
            writer_id,
            writer_epoch: EPOCH,
            writer_seq: seq,
            object_size: bytes.len() as u64,
            content_hash,
            sample_count: records.len() as u64,
            series_count: ids.len() as u64,
            min_event_ts_ns: min_ts,
            max_event_ts_ns: max_ts,
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

    /// Decode every record of an RLOG object (no predicate), in the object's
    /// stored `(stream_ref, ts)` order.
    fn decode_all(bytes: &[u8]) -> Vec<LogRecord> {
        let cfg = RlogConfig::default();
        let reader = RlogReader::new(bytes, &cfg).expect("open");
        let (rows, _) = reader.scan(&Predicate::And(Vec::new())).expect("scan");
        rows
    }

    /// Fetch the single compaction record in the bucket and every L1 part it
    /// references (parts in record order = ascending stream ranges).
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

    /// An order-independent canonical key for a record: the attribute set is
    /// folded through the frozen `canonical_attr_bytes` grammar so that whether
    /// an attribute was stored columnar or in `attrs_raw` (which can differ
    /// between an L0 input and the L1 merge) does not affect equality.
    type Canon = (
        [u8; 16],
        i64,
        i64,
        u8,
        String,
        String,
        Option<[u8; 16]>,
        Option<[u8; 8]>,
        u32,
        Vec<u8>,
        Vec<u8>,
    );
    fn canon(r: &LogRecord) -> Canon {
        (
            r.stream_id.0,
            r.ts_ns,
            r.observed_ts_ns,
            r.severity_num,
            r.severity_text.clone(),
            r.body.clone(),
            r.trace_id,
            r.span_id,
            r.flags,
            canonical_attr_bytes(&r.attrs),
            r.stream_attrs.clone(),
        )
    }

    fn canon_multiset(records: &[LogRecord]) -> Vec<Canon> {
        let mut v: Vec<Canon> = records.iter().map(canon).collect();
        v.sort();
        v
    }

    /// The FIELD_DIR entry count of an RLOG object.
    fn field_dir_len(bytes: &[u8]) -> usize {
        let cfg = RlogConfig::default();
        let ftr = footer::open(bytes).expect("open");
        let raw =
            read_section(bytes, ftr.section(kind::FIELD_DIR).unwrap(), &cfg).expect("section");
        FieldDir::decode(&raw, MAX_FIELDS).expect("decode").len()
    }

    // --- POSTINGS rebuild helpers (ADR-0049 decision 6, issue #509) ----------

    /// The block index of every record of an RLOG object, in stored order,
    /// taken from the object's own SKIP_IDX record counts. This is the ground
    /// truth POSTINGS must agree with, derived without reading POSTINGS.
    fn row_block_indices(bytes: &[u8]) -> Vec<u32> {
        let cfg = RlogConfig::default();
        let ftr = footer::open(bytes).expect("open");
        let raw =
            read_section(bytes, ftr.section(kind::SKIP_IDX).unwrap(), &cfg).expect("skip section");
        let skip = SkipIndex::decode(&raw, 1 << 24).expect("decode skip");
        let mut out = Vec::new();
        for (i, entry) in skip.l0.iter().enumerate() {
            for _ in 0..entry.record_count {
                out.push(i as u32);
            }
        }
        out
    }

    /// The blocks of `bytes` that really hold a row with the string attribute
    /// `name` = `value`, computed from the decoded records plus the object's own
    /// block framing. Never consults POSTINGS, so it is a valid oracle for it.
    fn true_blocks_for(bytes: &[u8], name: &str, value: &str) -> BTreeSet<u32> {
        let rows = decode_all(bytes);
        let blocks = row_block_indices(bytes);
        assert_eq!(
            rows.len(),
            blocks.len(),
            "SKIP_IDX record counts must cover exactly the decoded rows"
        );
        rows.iter()
            .zip(blocks)
            .filter(|(r, _)| {
                r.attrs
                    .iter()
                    .any(|(k, v)| k == name && matches!(v, AttrValue::Str(s) if s == value))
            })
            .map(|(_, b)| b)
            .collect()
    }

    /// The POSTINGS posting list for a string attribute term of an object:
    /// `None` when the field carries no postings there (never indexed, or
    /// dropped for exceeding the distinct-value cap).
    fn postings_probe(bytes: &[u8], name: &str, value: &str) -> Option<BTreeSet<u32>> {
        let cfg = RlogConfig::default();
        let ftr = footer::open(bytes).expect("open");
        let desc = ftr
            .section(kind::POSTINGS)
            .expect("object must carry a POSTINGS section");
        let raw = read_section(bytes, desc, &cfg).expect("postings section");
        let section = PostingsSection::parse(&raw).expect("parse postings");
        let fd_raw =
            read_section(bytes, ftr.section(kind::FIELD_DIR).unwrap(), &cfg).expect("field_dir");
        let fd = FieldDir::decode(&fd_raw, MAX_FIELDS).expect("decode field_dir");
        let cid = fd
            .column(name, FieldType::Str)
            .expect("indexed column present in FIELD_DIR")
            .column_id;
        section
            .probe(cid, value.as_bytes())
            .expect("probe")
            .map(|blocks| blocks.into_iter().collect())
    }

    /// A record on stream 0 with one `svc` string attribute.
    fn svc_record(ts: i64, svc: &str) -> LogRecord {
        record(
            0,
            ts,
            "log line",
            vec![("svc".into(), AttrValue::Str(svc.into()))],
        )
    }

    /// The two-input corpus behind the postings-rebuild tests. The inputs
    /// interleave in `ts` (A even, B odd) and are each written with 1000-record
    /// blocks, while the compactor writes the merged object with the default
    /// 8192-record blocks. So merged row index equals `ts`, the output has two
    /// blocks (0: ts 0..8192, 1: ts 8192..8600), and no input's block index for
    /// any term matches the output's -- a copied, concatenated, or offset-shifted
    /// posting list cannot accidentally be right.
    ///
    /// Terms, and where they land:
    ///
    /// | term        | in A            | in B            | in the merged output |
    /// |-------------|-----------------|-----------------|----------------------|
    /// | `cross`     | ts 6000, blk 3  | ts 201, blk 0   | rows 6000/201, blk 0 |
    /// | `tail_only` | ts 8580, blk 4  | absent          | row 8580, blk 1      |
    /// | `both_tail` | ts 8300, blk 4  | ts 8301, blk 4  | rows 8300/8301, blk 1|
    fn interleaved_corpus() -> (Vec<LogRecord>, Vec<LogRecord>) {
        let a: Vec<LogRecord> = (0..4300i64)
            .map(|i| {
                let ts = i * 2;
                let svc = match ts {
                    6000 => "cross",
                    8580 => "tail_only",
                    8300 => "both_tail",
                    _ => "bulk",
                };
                svc_record(ts, svc)
            })
            .collect();
        let b: Vec<LogRecord> = (0..4300i64)
            .map(|i| {
                let ts = i * 2 + 1;
                let svc = match ts {
                    201 => "cross",
                    8301 => "both_tail",
                    _ => "bulk",
                };
                svc_record(ts, svc)
            })
            .collect();
        (a, b)
    }

    /// L0 writer config for the corpus above: 1000-record blocks, so the inputs
    /// are blocked differently from the output.
    fn l0_blocked_cfg() -> RlogConfig {
        RlogConfig {
            block_target_records: 1000,
            ..RlogConfig::default()
        }
    }

    /// Seeds [`interleaved_corpus`] as two L0 inputs indexing `svc`, compacts,
    /// and returns the input bytes and the single L1 part.
    async fn compact_interleaved_corpus(store: &MemoryStore) -> (Bytes, Bytes, Bytes) {
        let (a, b) = interleaved_corpus();
        let a_bytes = seed_l0(store, Uuid::from_u128(1), 1, &a, l0_blocked_cfg(), &["svc"]).await;
        let b_bytes = seed_l0(store, Uuid::from_u128(2), 2, &b, l0_blocked_cfg(), &["svc"]).await;

        let clock = FixedClock::new(sealed_now_ns());
        compact_bucket(store, &clock, &CompactorConfig::default(), &bucket())
            .await
            .expect("compact");
        let (_rec, mut parts) = read_output(store).await;
        assert_eq!(parts.len(), 1, "one stream never straddles parts");
        (a_bytes, b_bytes, parts.remove(0))
    }

    /// Every record matching `pred` in `objects`, as a canonical multiset.
    fn scan_all(objects: &[&Bytes], pred: &Predicate) -> Vec<Canon> {
        let cfg = RlogConfig::default();
        let mut rows: Vec<LogRecord> = Vec::new();
        for obj in objects {
            let reader = RlogReader::new(obj, &cfg).expect("open");
            let (got, _) = reader.scan(pred).expect("scan");
            rows.extend(got);
        }
        canon_multiset(&rows)
    }

    fn svc_equals(value: &str) -> Predicate {
        Predicate::Equals {
            field: FieldSel::Attr("svc".into()),
            value: AttrValue::Str(value.into()),
        }
    }

    /// The acceptance test for issue #509. POSTINGS in an L1 part is rebuilt
    /// from the merged, re-blocked records: every term's posting list names the
    /// output's own block indices (checked against an oracle derived from the
    /// output's decoded rows and its own SKIP_IDX, never from POSTINGS), and a
    /// query over the L1 part returns exactly the records the same query returns
    /// The rebuild inherits POSTINGS v2's merged-view indexing with no code of
    /// its own (ADR-0049's 2026-08-03 amendment, issue #547).
    ///
    /// `gather_stream` materializes records through the reader, which populates
    /// `stream_attrs` from STREAM_DIR, and `flush_part` hands whole
    /// `LogRecord`s to `RlogWriter::with_indexed_fields`. This module collects
    /// no terms itself, so the L1 output indexes the merged view exactly as the
    /// writer does at L0. That is the property that would break if this module
    /// ever grew its own term collection.
    ///
    /// The object mixes both layers for one key: stream 1's records get
    /// `service.name = "svc1"` from their resource blob, and stream 2's records
    /// carry it as a per-record attribute. A v1 index would name only stream
    /// 2's blocks. The merged index must name both, which is what makes the
    /// prune sound for a merged-view query.
    ///
    /// A key that is resource-level across the WHOLE object still has no
    /// postings, because postings are keyed by a FIELD_DIR column and those come
    /// from the per-record layer. That gap is issue #552, not this ticket.
    #[tokio::test]
    async fn compaction_indexes_the_merged_view_in_the_output() {
        let store = MemoryStore::new();
        let svc = || {
            (
                "service.name".to_string(),
                AttrValue::Str("svc1".to_string()),
            )
        };
        // Input A: stream 1, whose resource blob is `service.name = "svc1"`.
        // No record carries it per-record.
        // 9000 records, so the output's first 8192-record block holds nothing
        // but these: a block whose only match for the key is resource-level.
        let a: Vec<LogRecord> = (0..9_000)
            .map(|i| record(1, 1_000 + i, "resource-only", vec![]))
            .collect();
        // Input B: stream 2 (resource `svc2`), each record carrying
        // `service.name = "svc1"` itself. This is what gives the object a
        // FIELD_DIR column for the key.
        let b: Vec<LogRecord> = (0..1_000)
            .map(|i| record(2, 20_000 + i, "per-record", vec![svc()]))
            .collect();
        seed_l0(
            &store,
            Uuid::from_u128(1),
            1,
            &a,
            l0_blocked_cfg(),
            &["service.name"],
        )
        .await;
        seed_l0(
            &store,
            Uuid::from_u128(2),
            2,
            &b,
            l0_blocked_cfg(),
            &["service.name"],
        )
        .await;

        let clock = FixedClock::new(sealed_now_ns());
        compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket())
            .await
            .expect("compact");
        let (_rec, parts) = read_output(&store).await;

        // Every record in the object matches `service.name = "svc1"` on the
        // merged view, so every block holding a record must be in the list.
        for l1 in &parts {
            let per_record = true_blocks_for(l1, "service.name", "svc1");
            let listed = postings_probe(l1, "service.name", "svc1").expect("the key is indexed");
            assert!(
                per_record.is_subset(&listed),
                "the per-record matches must still be listed"
            );
            let all_blocks: BTreeSet<u32> = row_block_indices(l1).into_iter().collect();
            // Guard against a degenerate corpus: if both layers happened to
            // cover the same blocks, the assertion below would hold under v1
            // indexing too and prove nothing.
            assert_ne!(
                per_record, all_blocks,
                "corpus must have a block whose only match is resource-level"
            );
            assert_eq!(
                listed, all_blocks,
                "a resource-level match must be indexed too, not only the per-record ones"
            );
        }
    }

    /// over its inputs.
    #[tokio::test]
    async fn compaction_rebuilds_postings_from_merged_blocks() {
        let store = MemoryStore::new();
        let (a_bytes, b_bytes, l1) = compact_interleaved_corpus(&store).await;

        // The output is re-blocked: two 8192-record-target blocks, not the
        // inputs' five 1000-record blocks each.
        let out_blocks = row_block_indices(&l1);
        assert_eq!(
            out_blocks.len(),
            8600,
            "every input record is in the output"
        );
        let block_count = footer::open(&l1).expect("open l1").block_count;
        assert_eq!(block_count, 2, "8600 records at 8192 per block");

        // Every term's posting list is exactly the set of output blocks that
        // really hold it. This is the property that fails for a copied,
        // concatenated, or offset-shifted list.
        for term in ["bulk", "cross", "tail_only", "both_tail"] {
            let want = true_blocks_for(&l1, "svc", term);
            assert!(
                !want.is_empty(),
                "term {term} must be present in the output"
            );
            assert_eq!(
                postings_probe(&l1, "svc", term),
                Some(want.clone()),
                "posting list for {term} must name the output's own blocks"
            );
            for &b in &want {
                assert!(
                    u64::from(b) < block_count,
                    "block index {b} for {term} is not a block of this object"
                );
            }
        }

        // Concretely: `tail_only` sits in the output's block 1 but in its input's
        // block 4, so the output's list cannot have come from the input's.
        assert_eq!(
            postings_probe(&l1, "svc", "tail_only"),
            Some(BTreeSet::from([1])),
            "tail_only is in the output's second block"
        );
        assert_eq!(
            postings_probe(&a_bytes, "svc", "tail_only"),
            Some(BTreeSet::from([4])),
            "tail_only was in its input's fifth block"
        );

        // The query differential: for every term, the L1 part answers exactly
        // what its inputs answer, and the postings-pruned path is the one
        // serving it.
        for term in ["bulk", "cross", "tail_only", "both_tail"] {
            let pred = svc_equals(term);
            let from_l1 = scan_all(&[&l1], &pred);
            let from_inputs = scan_all(&[&a_bytes, &b_bytes], &pred);
            assert!(!from_l1.is_empty(), "term {term} must match some record");
            assert_eq!(
                from_l1, from_inputs,
                "query on svc = {term} must return the same records over the L1 part as over its inputs"
            );
        }

        // Pruning really happens through POSTINGS, and soundly: `tail_only`
        // lives only in block 1, so the exact posting list prunes block 0 that
        // the skip index alone keeps.
        let cfg = RlogConfig::default();
        let reader = RlogReader::new(&l1, &cfg).expect("open l1");
        let (rows, stats) = reader.scan(&svc_equals("tail_only")).expect("scan");
        assert_eq!(rows.len(), 1);
        assert!(!stats.postings_degraded, "POSTINGS must parse, not degrade");
        assert_eq!(stats.blocks_total, 2);
        assert_eq!(stats.blocks_after_skip, 2, "no ts predicate to prune with");
        assert_eq!(
            stats.blocks_after_postings, 1,
            "the rebuilt postings prune the block that cannot hold the term"
        );
    }

    /// A term carried by two inputs at *different* block indices resolves to the
    /// output's own blocks, never to the union or concatenation of the inputs'.
    /// This is the test that catches a concatenated posting list.
    #[tokio::test]
    async fn postings_term_in_two_inputs_at_different_blocks_never_concatenated() {
        let store = MemoryStore::new();
        let (a_bytes, b_bytes, l1) = compact_interleaved_corpus(&store).await;

        // `cross` is in input A's block 3 and input B's block 0.
        let in_a = postings_probe(&a_bytes, "svc", "cross").expect("indexed in A");
        let in_b = postings_probe(&b_bytes, "svc", "cross").expect("indexed in B");
        assert_eq!(in_a, BTreeSet::from([3]));
        assert_eq!(in_b, BTreeSet::from([0]));
        assert_ne!(in_a, in_b, "the inputs must disagree for this test to bite");

        // In the merged object both occurrences are in block 0.
        let out = postings_probe(&l1, "svc", "cross").expect("indexed in the output");
        assert_eq!(out, true_blocks_for(&l1, "svc", "cross"));
        assert_eq!(out, BTreeSet::from([0]));

        // Not the union (which would carry the phantom block 3), and not a
        // shift-and-concatenate (which would carry 3 and 5).
        let union: BTreeSet<u32> = in_a.union(&in_b).copied().collect();
        assert_ne!(out, union, "a concatenated list would include block 3");
        assert!(
            !out.contains(&3),
            "block 3 does not exist in the output ({} blocks)",
            footer::open(&l1).expect("open").block_count
        );

        // Same for a term both inputs hold in their *last* block: the output
        // holds it in block 1, and every matching record is still returned.
        let both_tail = postings_probe(&l1, "svc", "both_tail").expect("indexed");
        assert_eq!(both_tail, BTreeSet::from([1]));
        assert_eq!(
            postings_probe(&a_bytes, "svc", "both_tail"),
            Some(BTreeSet::from([4]))
        );
        assert_eq!(
            postings_probe(&b_bytes, "svc", "both_tail"),
            Some(BTreeSet::from([4]))
        );
        let pred = svc_equals("both_tail");
        assert_eq!(
            scan_all(&[&l1], &pred),
            scan_all(&[&a_bytes, &b_bytes], &pred),
            "both occurrences must still be found in the output"
        );
        assert_eq!(scan_all(&[&l1], &pred).len(), 2);
    }

    /// The per-field distinct-value cap applies to the MERGED object: a field
    /// under the cap in every single input can exceed it once merged, exactly as
    /// the 1000-dynamic-column cap already behaves on merge. The field is then
    /// not indexed in the output, and the output stays correct because POSTINGS
    /// pruning is widen-only (ADR-0013): an unindexed field prunes nothing.
    #[tokio::test]
    async fn postings_distinct_value_cap_applies_to_merged_object() {
        let cap = RlogConfig::default().postings_max_distinct; // 10_000
        let per_input = cap / 2 + 1000; // 6000: under the cap alone, over it merged
        let store = MemoryStore::new();

        let mk = |prefix: char, odd: i64| -> Vec<LogRecord> {
            (0..per_input as i64)
                .map(|i| {
                    record(
                        0,
                        i * 2 + odd,
                        "log line",
                        vec![("reqid".into(), AttrValue::Str(format!("{prefix}{i:05}")))],
                    )
                })
                .collect()
        };
        let a = mk('r', 0);
        let b = mk('q', 1);
        let a_bytes = seed_l0(
            &store,
            Uuid::from_u128(1),
            1,
            &a,
            RlogConfig::default(),
            &["reqid"],
        )
        .await;
        let b_bytes = seed_l0(
            &store,
            Uuid::from_u128(2),
            2,
            &b,
            RlogConfig::default(),
            &["reqid"],
        )
        .await;

        // Every single input is under the cap, so each has real postings.
        assert!(per_input <= cap);
        assert!(
            postings_probe(&a_bytes, "reqid", "r00042").is_some(),
            "input A is under the cap, so reqid is indexed there"
        );
        assert!(
            postings_probe(&b_bytes, "reqid", "q00042").is_some(),
            "input B is under the cap, so reqid is indexed there"
        );

        let clock = FixedClock::new(sealed_now_ns());
        compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket())
            .await
            .expect("compact");
        let (_rec, parts) = read_output(&store).await;
        assert_eq!(parts.len(), 1);
        let l1 = &parts[0];

        // The merged object holds 12000 distinct values, over the cap, so the
        // field is dropped from the output's POSTINGS: it reads back as "not
        // indexed", never as a narrowed or empty posting list.
        assert!(2 * per_input > cap, "the merged object must exceed the cap");
        assert_eq!(
            postings_probe(l1, "reqid", "r00042"),
            None,
            "a field over the cap in the merged object must not be indexed"
        );
        assert_eq!(postings_probe(l1, "reqid", "q00042"), None);

        // And the output is still correct: a query on the unindexed field
        // returns every matching record, identical to the same query over the
        // inputs, with no postings pruning applied at all (widen-only).
        let cfg = RlogConfig::default();
        for term in ["r00042", "q00777"] {
            let pred = Predicate::Equals {
                field: FieldSel::Attr("reqid".into()),
                value: AttrValue::Str(term.into()),
            };
            let reader = RlogReader::new(l1, &cfg).expect("open l1");
            let (rows, stats) = reader.scan(&pred).expect("scan");
            assert_eq!(rows.len(), 1, "reqid = {term} must still be found");
            assert!(!stats.postings_degraded, "capping is not a parse failure");
            assert_eq!(
                stats.blocks_after_postings, stats.blocks_after_skip,
                "an unindexed field prunes nothing"
            );
            assert_eq!(
                scan_all(&[l1], &pred),
                scan_all(&[&a_bytes, &b_bytes], &pred),
                "the same query over the inputs returns the same records"
            );
        }

        // No record was lost while the index was dropped.
        assert_eq!(decode_all(l1).len(), 2 * per_input);
    }

    /// The output's indexed-field list is the union of its inputs' (issue #509
    /// deliverable 3, until issue #511 makes it per-tenant configuration): a
    /// field one input indexed is indexed in the output even when another input
    /// indexed nothing, and an object whose inputs indexed nothing gets no
    /// POSTINGS section at all.
    #[tokio::test]
    async fn output_indexed_field_list_is_the_union_of_its_inputs() {
        let store = MemoryStore::new();
        let a: Vec<LogRecord> = (0..4i64).map(|i| svc_record(i * 2, "alpha")).collect();
        let b: Vec<LogRecord> = (0..4i64).map(|i| svc_record(i * 2 + 1, "beta")).collect();
        seed_l0(
            &store,
            Uuid::from_u128(1),
            1,
            &a,
            RlogConfig::default(),
            &["svc"],
        )
        .await;
        // Input B indexes nothing at all: no POSTINGS section.
        let b_bytes = seed_l0(
            &store,
            Uuid::from_u128(2),
            2,
            &b,
            RlogConfig::default(),
            &[],
        )
        .await;
        assert!(
            footer::open(&b_bytes)
                .expect("open")
                .section(kind::POSTINGS)
                .is_none(),
            "input B must carry no POSTINGS"
        );

        let clock = FixedClock::new(sealed_now_ns());
        compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket())
            .await
            .expect("compact");
        let (_rec, parts) = read_output(&store).await;
        assert_eq!(parts.len(), 1);

        // svc is indexed in the output, and covers B's records too: B's "beta"
        // is in the output's postings even though B had no postings of its own.
        assert_eq!(
            postings_probe(&parts[0], "svc", "alpha"),
            Some(true_blocks_for(&parts[0], "svc", "alpha"))
        );
        assert_eq!(
            postings_probe(&parts[0], "svc", "beta"),
            Some(true_blocks_for(&parts[0], "svc", "beta")),
            "the rebuilt postings cover every merged record, not only the indexed input's"
        );
    }

    #[tokio::test]
    async fn no_input_postings_means_no_output_postings() {
        // Neither input indexes a field, so the output has no POSTINGS section:
        // the compactor invents no indexed-field list of its own (absence is
        // always legal, ADR-0049 decision 5).
        let store = MemoryStore::new();
        let a = vec![svc_record(1, "alpha")];
        let b = vec![svc_record(2, "beta")];
        seed(&store, Uuid::from_u128(1), 1, &a).await;
        seed(&store, Uuid::from_u128(2), 2, &b).await;

        let clock = FixedClock::new(sealed_now_ns());
        compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket())
            .await
            .expect("compact");
        let (_rec, parts) = read_output(&store).await;
        assert_eq!(parts.len(), 1);
        assert!(
            footer::open(&parts[0])
                .expect("open l1")
                .section(kind::POSTINGS)
                .is_none(),
            "no input indexed anything, so the output indexes nothing"
        );
    }

    #[tokio::test]
    async fn compacts_two_l0_rlog_objects_into_one_l1_part_verbatim() {
        let store = MemoryStore::new();
        let a = vec![
            record(0, 10, "alpha", vec![("k".into(), AttrValue::I64(1))]),
            record(1, 20, "bravo", Vec::new()),
        ];
        let b = vec![
            record(0, 15, "charlie", vec![("k".into(), AttrValue::I64(2))]),
            record(2, 5, "delta", Vec::new()),
        ];
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
        // The single L1 part decodes as an L1 object with a non-empty hash.
        let ftr = footer::open(&parts[0]).expect("open l1");
        assert_eq!(ftr.level, 1);
        assert!(!ftr.input_set_hash.is_empty());
        // Assert the recorded version against the format's own constant, not
        // against the compactor's `OUTPUT_FORMAT_VERSION`, which would only
        // assert that constant against itself (issue #482). The `open` above
        // rejects any trailer whose version is not `footer::VERSION`, so the
        // part having opened at all is what ties this number to the bytes on
        // the object rather than to another constant in this crate.
        assert_eq!(
            rec.parts[0].segment_format_version,
            u32::from(ravel_logseg::footer::VERSION)
        );

        // The L1 records are the union of both inputs, decoded in (stream_ref,
        // ts) order. The part's own STREAM_DIR resolves stream identity.
        let l1 = decode_all(&parts[0]);
        // Order check: stored order is (stream_ref, ts) ascending.
        let order: Vec<(LogStreamId, i64)> = l1.iter().map(|r| (r.stream_id, r.ts_ns)).collect();
        let mut sorted = order.clone();
        sorted.sort();
        assert_eq!(order, sorted, "L1 records in (stream, ts) order");

        let mut expected = a.clone();
        expected.extend(b.clone());
        assert_eq!(canon_multiset(&l1), canon_multiset(&expected));

        // And decoding the inputs directly then concatenating gives the same set.
        let mut direct = decode_all(&a_bytes);
        direct.extend(decode_all(&b_bytes));
        assert_eq!(canon_multiset(&l1), canon_multiset(&direct));
    }

    #[tokio::test]
    async fn same_stream_different_attrs_across_objects_is_typed_error() {
        // Two inputs claim the same stream_id with different resource+scope
        // blobs: an upstream identity violation the merge must fail loud on
        // (the cross-object analogue of writer.rs's
        // `same_stream_different_attrs_rejected`).
        let store = MemoryStore::new();
        let (id, blob_ok) = stream_ident(0);
        let mut good = record(0, 1, "x", Vec::new());
        good.stream_id = id;
        good.stream_attrs = blob_ok;
        // Same id, a different (truthful-looking but conflicting) blob.
        let mut clash = record(0, 2, "y", Vec::new());
        clash.stream_id = id;
        clash.stream_attrs = stream_attrs_bytes(
            &[("service.name".into(), AttrValue::Str("OTHER".into()))],
            "scope",
            "1",
            &[],
        );

        seed(&store, Uuid::from_u128(1), 1, &[good]).await;
        seed(&store, Uuid::from_u128(2), 2, &[clash]).await;

        let clock = FixedClock::new(sealed_now_ns());
        let err = compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket())
            .await
            .expect_err("must reject conflicting stream attrs");
        match err {
            MaintainError::StreamAttrsConflict { stream_id, .. } => {
                assert_eq!(stream_id, id.to_hex(), "error must name the stream");
            }
            other => panic!("expected StreamAttrsConflict, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn dynamic_column_union_over_cap_folds_overflow_into_attrs_raw() {
        // Each input stays well under the 1000-column cap, but their union
        // exceeds it: the merge must apply the same cap-and-spill rule, folding
        // the overflow keys into attrs_raw. No value is dropped and FIELD_DIR is
        // never left over-cap.
        let store = MemoryStore::new();
        let attrs_a: Vec<(String, AttrValue)> = (0..600)
            .map(|i| (format!("a{i:03}"), AttrValue::I64(i)))
            .collect();
        let attrs_b: Vec<(String, AttrValue)> = (0..600)
            .map(|i| (format!("b{i:03}"), AttrValue::I64(i)))
            .collect();
        let a = vec![record(0, 1, "x", attrs_a.clone())];
        let b = vec![record(0, 2, "y", attrs_b.clone())];
        seed(&store, Uuid::from_u128(1), 1, &a).await;
        seed(&store, Uuid::from_u128(2), 2, &b).await;

        let clock = FixedClock::new(sealed_now_ns());
        compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket())
            .await
            .expect("compact");
        let (_rec, parts) = read_output(&store).await;
        assert_eq!(parts.len(), 1);

        // FIELD_DIR is capped at exactly 1000 dynamic columns (union was 1200).
        assert_eq!(field_dir_len(&parts[0]), 1000, "FIELD_DIR capped at 1000");

        // Every attribute of every input record still round-trips (the 200
        // overflow keys live in attrs_raw, never dropped).
        let l1 = decode_all(&parts[0]);
        let mut expected = a.clone();
        expected.extend(b.clone());
        assert_eq!(canon_multiset(&l1), canon_multiset(&expected));
        for r in &l1 {
            assert_eq!(r.attrs.len(), 600, "all 600 attrs preserved per record");
        }
    }

    #[tokio::test]
    async fn bloom_is_rebuilt_over_merged_block_not_copied_from_one_input() {
        // Two inputs contribute the same stream; input A's records carry the
        // token "alpha" in body, input B's carry "beta". They merge into the
        // same output block. A bloom copied from one input would be missing the
        // other's token and wrongly prune it; a rebuilt bloom (sized by the
        // merged block's own tokens) contains both, so a HasWord scan over the
        // L1 output finds both.
        let store = MemoryStore::new();
        let a: Vec<LogRecord> = (0..8)
            .map(|i| record(0, i * 2, "alpha alpha", Vec::new()))
            .collect();
        let b: Vec<LogRecord> = (0..8)
            .map(|i| record(0, i * 2 + 1, "beta beta", Vec::new()))
            .collect();
        seed(&store, Uuid::from_u128(1), 1, &a).await;
        seed(&store, Uuid::from_u128(2), 2, &b).await;

        let clock = FixedClock::new(sealed_now_ns());
        compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket())
            .await
            .expect("compact");
        let (_rec, parts) = read_output(&store).await;
        assert_eq!(parts.len(), 1);

        let cfg = RlogConfig::default();
        let reader = RlogReader::new(&parts[0], &cfg).expect("open l1");
        for (word, want) in [("alpha", 8usize), ("beta", 8usize)] {
            let (rows, stats) = reader
                .scan(&Predicate::HasWord {
                    field: FieldSel::Body,
                    word: word.into(),
                })
                .expect("scan");
            assert_eq!(rows.len(), want, "HasWord({word}) must find every match");
            assert!(
                stats.blocks_scanned >= 1,
                "the merged block survived bloom pruning for {word}"
            );
        }

        // Negative control: "gamma" is in neither input's body, so the rebuilt
        // bloom must PRUNE the merged block, not merely be non-empty. This
        // distinguishes a real bloom from an all-ones (vacuously-present) bloom
        // or from no bloom pruning at all -- both of which would still pass the
        // positive asserts above. The signal is `blocks_after_bloom`: the block
        // is a skip-index candidate (no ts/stream predicate excludes it, so
        // `blocks_after_skip >= 1`) yet the bloom removes it before the scan
        // (`blocks_after_bloom == 0`). `blocks_scanned == 0` and no rows follow.
        // `bloom_degraded` must be false, or the "pruning" would just be a
        // parse failure, not the bloom proving absence.
        let (rows, stats) = reader
            .scan(&Predicate::HasWord {
                field: FieldSel::Body,
                word: "gamma".into(),
            })
            .expect("scan");
        assert!(rows.is_empty(), "gamma is in neither input");
        assert!(!stats.bloom_degraded, "bloom must parse, not degrade");
        assert!(
            stats.blocks_after_skip >= 1,
            "the merged block is a skip-index candidate for gamma"
        );
        assert_eq!(
            stats.blocks_after_bloom, 0,
            "the rebuilt bloom must prune the block for the absent word gamma"
        );
        assert_eq!(stats.blocks_scanned, 0, "a pruned block is never scanned");
    }

    #[tokio::test]
    async fn part_splitting_keeps_streams_whole_and_disjoint() {
        // Many distinct streams and a tiny part cap force splits on stream
        // boundaries: parts get disjoint, ascending stream-id ranges (a stream
        // never straddles two parts), and the union of records is preserved.
        let store = MemoryStore::new();
        let mk = |seq_body: &str| -> Vec<LogRecord> {
            (0..20u32)
                .map(|s| record(s, i64::from(s), seq_body, Vec::new()))
                .collect()
        };
        let a = mk("aaaa");
        let b = mk("bbbb");
        seed(&store, Uuid::from_u128(1), 1, &a).await;
        seed(&store, Uuid::from_u128(2), 2, &b).await;

        let clock = FixedClock::new(sealed_now_ns());
        // Tiny cap to force splits on stream boundaries.
        let config = CompactorConfig {
            max_l1_part_bytes: 256,
            ..CompactorConfig::default()
        };

        compact_bucket(&store, &clock, &config, &bucket())
            .await
            .expect("compact");
        let (rec, parts) = read_output(&store).await;
        assert!(parts.len() >= 2, "tiny cap must split into parts");

        // Part stream-id ranges are disjoint and ascending.
        let mut prev_last: Option<[u8; 16]> = None;
        for (i, p) in rec.parts.iter().enumerate() {
            let first: [u8; 16] = p.first_series_id.as_slice().try_into().unwrap();
            let last: [u8; 16] = p.last_series_id.as_slice().try_into().unwrap();
            assert!(first <= last);
            if let Some(pl) = prev_last {
                assert!(
                    pl < first,
                    "part stream ranges must be disjoint and ordered"
                );
            }
            prev_last = Some(last);
            // Every part is a valid L1 object.
            assert_eq!(footer::open(&parts[i]).expect("open").level, 1);
        }

        // Content complete across all parts.
        let mut l1: Vec<LogRecord> = Vec::new();
        for p in &parts {
            l1.extend(decode_all(p));
        }
        let mut expected = a.clone();
        expected.extend(b.clone());
        assert_eq!(canon_multiset(&l1), canon_multiset(&expected));
    }

    // --- issue #745: bounded-memory k-way merge ------------------------------

    /// Acceptance test for issue #745 (ADR-0065 decision 4): the RLOG
    /// compaction merge's peak resident decode memory is bounded by block size
    /// times input count, independent of how large one hot stream grows.
    ///
    /// A single hot stream (the common log shape: one busy service carrying
    /// most of a sealed hour) is grown 10x across two runs. The
    /// [`MergeMemoryTracker`]'s recorded *transient* high-water -- the merge's
    /// own decode-side buffers, at most one fetched raw block plus one decoded
    /// block per input -- must stay under a fixed bound and must NOT grow with
    /// the stream. The defect was that the old merge decoded a whole stream's
    /// records from every input into one `Vec`, plus a second copy in the part
    /// accumulator, so peak scaled with stream size; a regression to that shape
    /// would push the `decoded` term to `O(stream)` and break the bound below.
    ///
    /// Deterministic: it asserts on the tracker's accounting, never on process
    /// RSS or allocator hooks, and runs against [`MemoryStore`].
    #[tokio::test]
    async fn merge_peak_memory_is_bounded_independently_of_stream_size() {
        const INPUTS: u32 = 3;
        // l0_blocked_cfg's block target: each input is re-blocked at 1000
        // records, so a grown stream spans many blocks per input.
        const L0_BLOCK: u64 = 1000;
        // A generous per-record upper bound (estimate_record for these tiny
        // records is ~53 bytes); the bound is intentionally loose so it tracks
        // block size and input count, not the exact estimate.
        const PER_RECORD_BOUND: u64 = 256;
        // At most one raw block plus one decoded block resident per input.
        const TRANSIENT_BOUND: u64 = INPUTS as u64 * 2 * L0_BLOCK * PER_RECORD_BOUND;
        // Rough bytes-per-record for the "stream size" the peak is compared
        // against (only used for reporting and the not-scaling assertion).
        const PER_RECORD_APPROX: i64 = 53;

        // records-per-input at the base scale and 10x.
        let scales = [3_000i64, 30_000i64];
        let mut peaks = Vec::new();
        for &records_per_input in &scales {
            let store = MemoryStore::new();
            for j in 0..INPUTS {
                // One hot stream (stream 0); ts interleaves across inputs so
                // the k-way merge genuinely interleaves them.
                let recs: Vec<LogRecord> = (0..records_per_input)
                    .map(|i| record(0, i * i64::from(INPUTS) + i64::from(j), "x", Vec::new()))
                    .collect();
                seed_l0(
                    &store,
                    Uuid::from_u128(u128::from(j) + 1),
                    u64::from(j) + 1,
                    &recs,
                    l0_blocked_cfg(),
                    &[],
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

            // One hot stream => exactly one part (a stream never straddles).
            let (_rec, parts) = read_output(&store).await;
            assert_eq!(parts.len(), 1, "one stream is one part");
            assert_eq!(
                decode_all(&parts[0]).len() as i64,
                records_per_input * i64::from(INPUTS),
                "every record survived the merge"
            );

            let transient = tracker.peak_transient_bytes();
            let total = tracker.peak_total_bytes();
            let stream_bytes =
                (records_per_input * i64::from(INPUTS)) as u64 * PER_RECORD_APPROX as u64;
            println!(
                "[mem:rlog #745] records/input={records_per_input} stream~={stream_bytes}B \
                 peak_transient={transient}B peak_total={total}B transient_bound={TRANSIENT_BOUND}B \
                 part_cap={}B",
                config.max_l1_part_bytes
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
            // The only stream-dependent residency is the writer's in-progress
            // part, bounded by the part cap (content-addressing needs the whole
            // part before its key exists).
            assert!(
                total < TRANSIENT_BOUND + config.max_l1_part_bytes,
                "total peak {total} exceeded transient bound + part cap"
            );
            peaks.push(transient);
        }

        // The stream grew 10x; the decode-side peak did not (the same handful
        // of blocks stay resident regardless of stream size).
        let (base, ten_x) = (peaks[0], peaks[1]);
        assert!(
            ten_x <= base * 2,
            "decode-side peak scaled with the stream: base={base} 10x={ten_x}"
        );
        // And concretely, at 10x the peak is a small fraction of the stream.
        let stream_bytes_10x = (scales[1] * i64::from(INPUTS)) as u64 * PER_RECORD_APPROX as u64;
        assert!(
            ten_x < stream_bytes_10x / 5,
            "10x peak {ten_x} is not far below the stream size {stream_bytes_10x}"
        );
    }

    /// Byte-identical-output test for issue #745: the new k-way streaming merge
    /// produces the exact same part bytes as the old "concatenate every input's
    /// decoded records for the stream in canonical input order, then stable
    /// sort by `ts_ns`" path. Parts are content-addressed, so a reordering bug
    /// would silently change every downstream hash.
    ///
    /// The corpus carries the same `(stream, ts)` in two different inputs with
    /// *different bodies*, so a wrong tie-break would reorder those records and
    /// change the bytes -- the exact failure this guards against.
    #[tokio::test]
    async fn streaming_merge_output_is_byte_identical_to_stable_sort_order() {
        let store = MemoryStore::new();
        // Three inputs over three streams. `a` and `b` share an even ts grid
        // (cross-input ties on every stream); `c` uses an odd grid (it
        // interleaves). Bodies are tagged by input so order is observable.
        let mk = |tag: &str, odd: i64| -> Vec<LogRecord> {
            let mut v = Vec::new();
            for s in 0..3u32 {
                for k in 0..40i64 {
                    v.push(record(
                        s,
                        k * 2 + odd,
                        &format!("{tag}-{s}-{k}"),
                        vec![("svc".into(), AttrValue::Str(format!("v{}", k % 4)))],
                    ));
                }
            }
            v
        };
        let a = mk("a", 0);
        let b = mk("b", 0); // same ts grid as `a` => cross-input ts ties
        let c = mk("c", 1); // odd grid => interleaves
        // writer_ids 1,2,3 with seqs 1,2,3 sort to canonical order [a, b, c],
        // the order the merge breaks ts ties in.
        let a_bytes = seed_l0(
            &store,
            Uuid::from_u128(1),
            1,
            &a,
            l0_blocked_cfg(),
            &["svc"],
        )
        .await;
        let b_bytes = seed_l0(
            &store,
            Uuid::from_u128(2),
            2,
            &b,
            l0_blocked_cfg(),
            &["svc"],
        )
        .await;
        let c_bytes = seed_l0(
            &store,
            Uuid::from_u128(3),
            3,
            &c,
            l0_blocked_cfg(),
            &["svc"],
        )
        .await;

        let clock = FixedClock::new(sealed_now_ns());
        compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket())
            .await
            .expect("compact");
        let (_rec, parts) = read_output(&store).await;
        assert_eq!(parts.len(), 1, "small corpus fits one part");
        let actual = &parts[0];

        // Reference: the OLD logical order. For each stream in sorted id order,
        // concatenate each input's records for that stream in canonical (seed)
        // order, then stable-sort by ts_ns; concatenate across streams.
        let per_input: Vec<Vec<LogRecord>> = [&a_bytes, &b_bytes, &c_bytes]
            .iter()
            .map(|b| decode_all(b))
            .collect();
        let mut all_ids: BTreeSet<LogStreamId> = BTreeSet::new();
        for recs in &per_input {
            for r in recs {
                all_ids.insert(r.stream_id);
            }
        }
        let mut old_order: Vec<LogRecord> = Vec::new();
        for id in &all_ids {
            let mut recs: Vec<LogRecord> = Vec::new();
            for recs_in in &per_input {
                recs.extend(recs_in.iter().filter(|r| &r.stream_id == id).cloned());
            }
            recs.sort_by_key(|r| r.ts_ns); // stable: exactly the old path
            old_order.extend(recs);
        }

        // Build the reference part through the same writer, identity, indexed
        // fields, input_set_hash, and part_index the compaction used.
        let ftr = footer::open(actual).expect("open actual part");
        assert_eq!(ftr.input_set_hash.len(), 32, "input_set_hash is 32 bytes");
        let mut ish = [0u8; 32];
        ish.copy_from_slice(&ftr.input_set_hash);
        let identity = compactor_identity(&bucket(), &CompactorConfig::default());
        let mut writer = RlogWriter::new(RlogConfig::default(), identity)
            .with_indexed_fields(vec!["svc".to_string()]);
        for r in old_order {
            writer.push(r).expect("push");
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
    struct RecSpec {
        stream_n: u32,
        ts: i64,
        body: String,
        attrs: Vec<(String, AttrValue)>,
    }

    fn attr_strategy() -> impl Strategy<Value = (String, AttrValue)> {
        let key = prop::sample::select(vec!["k0", "k1", "k2", "k3"]).prop_map(String::from);
        let val = prop_oneof![
            (0i64..8).prop_map(AttrValue::I64),
            prop::sample::select(vec!["p", "q", "r"]).prop_map(|s| AttrValue::Str(s.into())),
            any::<bool>().prop_map(AttrValue::Bool),
        ];
        (key, val)
    }

    fn rec_strategy() -> impl Strategy<Value = RecSpec> {
        (
            0u32..4,
            0i64..40,
            prop::sample::select(vec!["ok", "warn timeout", "connection refused", "fine"]),
            prop::collection::vec(attr_strategy(), 0..3),
        )
            .prop_map(|(stream_n, ts, body, attrs)| RecSpec {
                stream_n,
                ts,
                body: body.into(),
                attrs,
            })
    }

    fn corpus_strategy() -> impl Strategy<Value = Vec<Vec<RecSpec>>> {
        // 2..=5 inputs, each 1..=15 records.
        prop::collection::vec(prop::collection::vec(rec_strategy(), 1..15), 2..6)
    }

    async fn differential_check(corpus: Vec<Vec<RecSpec>>, max_l1_part_bytes: u64) {
        let store = MemoryStore::new();
        let mut all_input_records: Vec<LogRecord> = Vec::new();
        for (i, input) in corpus.iter().enumerate() {
            let records: Vec<LogRecord> = input
                .iter()
                .map(|s| record(s.stream_n, s.ts, &s.body, s.attrs.clone()))
                .collect();
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
            max_l1_part_bytes,
            ..CompactorConfig::default()
        };
        compact_bucket(&store, &clock, &config, &bucket())
            .await
            .expect("compact");
        let (rec, parts) = read_output(&store).await;
        assert_eq!(rec.level, 1);

        // Decode every L1 part (concatenated in part order) and compare its
        // record set to the inputs decoded directly. Both are compared as an
        // order-independent canonical multiset (the correctness core).
        let mut l1: Vec<LogRecord> = Vec::new();
        for p in &parts {
            l1.extend(decode_all(p));
        }
        assert_eq!(
            canon_multiset(&l1),
            canon_multiset(&all_input_records),
            "L1 decoded set must equal the input union"
        );

        // Within each part, records are in (stream_ref, ts) order.
        for p in &parts {
            let recs = decode_all(p);
            let order: Vec<(LogStreamId, i64)> =
                recs.iter().map(|r| (r.stream_id, r.ts_ns)).collect();
            let mut sorted = order.clone();
            sorted.sort();
            assert_eq!(order, sorted, "part records in (stream, ts) order");
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 24, ..ProptestConfig::default() })]

        /// The correctness core (ADR-0032, issue #231): for a random corpus of
        /// log records split across N L0 objects, the full decoded record set is
        /// identical whether the N L0 inputs are decoded and concatenated or the
        /// single compacted L1 output is decoded. Default part cap: a single L1
        /// part.
        #[test]
        fn differential_l0_union_equals_l1_output(corpus in corpus_strategy()) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(differential_check(corpus, CompactorConfig::default().max_l1_part_bytes));
        }

        /// The same union-equality property under a tiny part cap that forces
        /// the merge to split across multiple parts on stream boundaries, so the
        /// "concatenate all parts" side of the differential actually crosses part
        /// boundaries (the large-cap test above never leaves one part). 512 bytes
        /// is below a handful of records' estimate, so any multi-stream corpus
        /// splits; single-stream corpora stay one part (a stream never straddles)
        /// and still exercise the property.
        #[test]
        fn differential_holds_across_part_boundaries(corpus in corpus_strategy()) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(differential_check(corpus, 512));
        }
    }
}
