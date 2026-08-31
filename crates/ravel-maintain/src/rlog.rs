//! The RLOG side of the codec seam (ADR-0032): L0-to-L1 log
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
//! # POSTINGS is rebuilt, never merged (ADR-0049 decision 6)
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
//!   that list is future per-tenant configuration and does not exist yet. See
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
//! # Memory (ADR-0065 decision 4, ADR-0032 amendment 2026-08-26)
//!
//! The read side ([`RlogCodec::load_input_catalog`]) retains only per-input
//! catalog metadata: a [`RlogRangeReader`] holding the decoded STREAM_DIR,
//! FIELD_DIR, and SKIP_IDX plus the object key, never block or bloom bytes.
//!
//! The merge itself is a **k-way block-streaming merge**, so its peak resident
//! memory is bounded independently of any one stream's size -- the defect
//! the k-way merge fixed. The ranged reader already fetched one stream's blocks by range
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
//! [`RlogRangeReader::decode_block_in_group`]), fetching the next range only
//! once the previous one is drained (one ahead: the fetch for range `n + 1`
//! rides along with range `n`'s, so the cursor advances two ranges per round
//! trip while its raw residency stays at two). The fetched range is one block
//! under version 3 and one row group under version 4, whose blocks' pages are
//! spread across its column chunks (ADR-0699 decision 1); the group's raw
//! bytes are held while its blocks are decoded out of them one at a time, so
//! the decoded term is a block in both versions (issue #748). A record's
//! `(stream_ref, ts)`
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
//! unchanged by the k-way merge), plus at most one decoded block per input
//! carrying the current stream (`O(input_count * block_size)`, independent of
//! stream size) and the raw bytes of the range that block came from
//! (`O(input_count * group_size)` under version 4, stored bytes rather than
//! decoded records), plus the in-progress part's writer buffer. The writer
//! buffer tracks the **memory split target** `l1_part_memory_target_bytes`:
//! [`PartSink`] closes the in-progress part as soon as its [`estimate_record`]
//! (decoded-heap) total reaches that target, wherever in the merged record
//! sequence that falls, so a stream may span consecutive parts. On this path the
//! check runs after every merged record, so the buffer exceeds the target by at
//! most one record; that is as tight as the target gets anywhere, and it is
//! still an estimate of heap rather than a measurement of it
//! ([`CompactorConfig::l1_part_memory_target_bytes`] gives the other paths'
//! overshoot). It did NOT
//! always work this way -- the check used to run only between streams, on the
//! "a stream never straddles two parts" invariant this module and ADR-0032
//! once stated -- and a bucket carrying one OTLP resource/scope (one stream)
//! therefore had no split point at all: 3M wide rows of one ClickBench logs
//! stream held the whole hour in one writer at 45.7 GB resident. Issue #711
//! replaced the invariant with the size target; parts are still written by the
//! frozen [`RlogWriter`] unchanged, so only the partitioning of records into
//! parts changed, never a part's bytes. Holding one whole part before its
//! content-addressed key exists remains unavoidable. The
//! [`MergeMemoryTracker`] seam accounts these terms at their real
//! allocation/decode points and records a high-water mark; the acceptance test
//! asserts that mark stays proportional to the memory split target while a hot
//! stream grows.
//!
//! # Memory split target and stored-size target are two separate knobs (issue #872)
//!
//! `l1_part_memory_target_bytes` above sizes the decoded record heap and is what
//! keeps this merge survivable on a small host. It does NOT govern object
//! geometry: on a wide schema it reaches 256 MiB of heap after only a few MB of
//! stored bytes, so every L1 object was tiny (~3.5 MB on ClickBench, a 74x gap
//! below the 256 MiB the knob's old name implied). Object geometry is a second,
//! independent knob, the **stored-size target** `max_l1_part_bytes`: [`PartSink`]
//! also sums [`estimate_stored_record`] (an encoded-bytes proxy) per record,
//! plus [`estimate_stored_stream`] once per distinct stream in the part (the
//! STREAM_DIR entry the object stores once per stream), and closes the part when
//! that total reaches the target. A part closes on
//! whichever target is reached first. With the shipped defaults (both 256 MiB)
//! the memory split target still fires first on every real schema, so this split is
//! behaviour-neutral until an operator lowers the stored target to grow
//! objects.
//!
//! The first RLOG merge held every input object whole (RLOG then had no ranged
//! section reader); [`ravel_logseg::open_from_suffix`] is now the RLOG analogue
//! of `ravel_segment::open_from_suffix` and closed that raw-bytes gap.
//!
//! # One merge, two callers
//!
//! [`merge_catalogs`] is that merge, and both RLOG writers of L1 parts run
//! through it: compaction ([`RlogCodec::build_parts`]) keeping every record,
//! and the ADR-0064 selective-erasure rewrite
//! (`erasure_rewrite::build_rewrite_logs`) keeping the records no pending
//! request's matcher drops (issue #725). The rewrite used to have its own
//! whole-object read and its own single unbounded writer, which is how it
//! reached the pre-#711 peak on the same tenant shape. Sharing the driver
//! bounds it and keeps the two from drifting on merge order, on where a part
//! splits, or on the cross-object stream-identity invariant.
//!
//! # What a reader must tolerate after the split
//!
//! Two consecutive parts of one compaction may carry the same `stream_id`,
//! with adjacent, non-overlapping `(first_series_id, last_series_id)` bounds
//! (part `k`'s last equals part `k+1`'s first) and adjacent event-time ranges.
//! Nothing in the read path prunes on those bounds -- the catalog turns every
//! part into its own `SegmentRef` and the resolver unions them -- so a split
//! stream reads back as the same row set in the same order. Record
//! conservation (`sum(part.sample_count)`, the compaction and ADR-0064 erasure
//! gates) is likewise unaffected: splitting repartitions records, it never
//! adds or drops one. The one aggregate that does change is
//! `sum(part.series_count)`, which counts a straddling stream once per part it
//! appears in; it is reported, never used as a gate.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;

use futures::stream::{StreamExt, TryStreamExt, iter as stream_iter};
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

/// The RLOG output trailer version every L1 part carries (currently 3:
/// ADR-0032 introduced v2, ADR-0095 moved it to v3). Recorded in each part's
/// `CompactionPart.segment_format_version`,
/// the log analogue of RSEG's [`crate::build::OUTPUT_FORMAT_VERSION`].
///
/// Read from `ravel_logseg`'s single-sourced supported-version window
/// (`SUPPORTED_VERSIONS.newest()`, ADR-0066 decision 1, slice A), the same
/// single source the reader gate and the CLI `audit-versions`/`migrate` paths
/// use. As a mirrored literal this would go stale silently on the next bump --
/// the writer stamps the trailer from the window while a stale literal kept
/// reporting the old number, so the compactor would write parts claiming a
/// version they are not; routing through the window moves both together.
pub const OUTPUT_FORMAT_VERSION: u32 = ravel_logseg::footer::SUPPORTED_VERSIONS.newest() as u32;

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
/// (docs/log-segment-format.md, this module's memory note). The
/// untrusted-input caps on the directory sections live inside the ranged reader.
#[derive(Debug, Clone)]
pub struct RlogInputCatalog {
    pub object_key: String,
    pub reader: RlogRangeReader,
    /// The dynamic attribute names this input carries a POSTINGS entry for, as
    /// recovered by [`input_indexed_fields`]. Only the *names* survive into the
    /// merge; no posting list from an input is ever read (ADR-0049 decision 6).
    /// Empty when the caller asked for no recovery (see
    /// [`load_catalog_from_object`]).
    pub indexed_fields: Vec<String>,
    /// This input object's own footer `record_count`, retained from the footer
    /// this load already read. The erasure rewrite's input-side conservation
    /// gate (`erasure_rewrite::input_footer_cross_check`) needs it, and taking
    /// it from here is what lets that gate keep working without the
    /// whole-object GET it used to read the footer from.
    pub record_count: u64,
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
        load_catalog_from_object(store, config, object_key, true).await
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
        // The output's indexed-field list: the union of the inputs' (see
        // `input_indexed_fields`). Every part of this compaction gets the same
        // list, so a field is indexed uniformly across the output.
        let indexed_fields = merged_indexed_fields(&catalogs);
        // Compaction drops nothing, so every merged record is kept and the
        // counts the driver returns are the input count twice over; the
        // compaction-side conservation gate reads `part.sample_count`, not
        // these.
        let merged = merge_catalogs(
            store,
            config,
            bucket,
            &catalogs,
            input_set_hash,
            indexed_fields,
            config.dry_run,
            &mut |_| Ok(true),
        )
        .await?;
        Ok(merged.parts)
    }
}

/// The object-key-parametrized core of [`RlogCodec::load_input_catalog`],
/// generalized so a caller holding an object key from something other than an
/// L0 [`InputRecord`] can decode a catalog too. The erasure rewrite is that
/// caller: its live input set is a list of whole-object keys (L0 data objects,
/// or the parts of the live compaction/rewrite record), never `InputRecord`s.
///
/// `recover_indexed_fields` chooses whether [`input_indexed_fields`] runs. The
/// compactor wants the list (its output re-indexes what its inputs indexed);
/// the erasure rewrite writes its parts with no indexed fields at all
/// (`erasure_rewrite::build_rewrite_logs`'s doc says why), so recovering a list
/// it would discard would cost one extra ranged GET per input carrying
/// POSTINGS and nothing else.
pub(crate) async fn load_catalog_from_object(
    store: &dyn ObjectStoreBackend,
    config: &CompactorConfig,
    object_key: String,
    recover_indexed_fields: bool,
) -> Result<RlogInputCatalog> {
    let cfg = RlogConfig::default();

    // Locate the footer and section directory from a suffix probe: one
    // ranged GET, growing to a second only if the probe missed the footer
    // (the RLOG analogue of the RSEG read path).
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
    let stream_dir_raw = fetch_section(store, &object_key, &ftr, kind::STREAM_DIR, &cfg).await?;
    let field_dir_raw = fetch_section(store, &object_key, &ftr, kind::FIELD_DIR, &cfg).await?;
    let skip_idx_raw = fetch_section(store, &object_key, &ftr, kind::SKIP_IDX, &cfg).await?;

    // Which fields this input had indexed. Recovered here, while the
    // object's footer and FIELD_DIR bytes are already at hand, and reduced
    // immediately to a name list: the POSTINGS bytes themselves are dropped
    // before this function returns and never take part in the merge.
    let indexed_fields = if recover_indexed_fields {
        input_indexed_fields(store, &object_key, &ftr, &field_dir_raw, &cfg).await?
    } else {
        Vec::new()
    };

    // PAGE_DIR is present exactly on a version-4 input (ADR-0699
    // decision 2), and locating its blocks' pages is impossible without it.
    let page_dir_raw = match ftr.section(kind::PAGE_DIR) {
        Some(_) => Some(fetch_section(store, &object_key, &ftr, kind::PAGE_DIR, &cfg).await?),
        None => None,
    };
    let record_count = ftr.record_count;
    let reader = RlogRangeReader::from_sections_with_page_dir(
        &ftr,
        &stream_dir_raw,
        &field_dir_raw,
        &skip_idx_raw,
        page_dir_raw.as_deref(),
    )?;
    Ok(RlogInputCatalog {
        object_key,
        reader,
        indexed_fields,
        record_count,
    })
}

/// One catalog per object key, `input_read_concurrency` loads in flight.
///
/// `buffered`, not `buffer_unordered`: the returned catalogs stay aligned
/// one-to-one with `object_keys` in canonical order, which is the k-way
/// merge's tie-break on equal `ts_ns` (see [`merge_stream_into_parts`]).
pub(crate) async fn load_catalogs_by_key(
    store: &dyn ObjectStoreBackend,
    config: &CompactorConfig,
    object_keys: &[String],
    recover_indexed_fields: bool,
) -> Result<Vec<RlogInputCatalog>> {
    let mut pending = Vec::with_capacity(object_keys.len());
    for object_key in object_keys {
        pending.push(load_catalog_from_object(
            store,
            config,
            object_key.clone(),
            recover_indexed_fields,
        ));
    }
    stream_iter(pending)
        .buffered(config.input_read_concurrency.max(1))
        .try_collect()
        .await
}

/// What one run of [`merge_catalogs`] produced: the parts, and the record
/// counts the caller's conservation gates need.
pub(crate) struct MergeOutput {
    /// Every part built, in part-index order.
    pub parts: Vec<BuiltPart>,
    /// Records the merge read out of the inputs, kept and dropped alike.
    pub input_record_count: u64,
    /// Records the filter kept, so the ones actually pushed into a part.
    pub output_record_count: u64,
}

/// The shared k-way block-streaming merge: every input's records, in global
/// `(stream_id, ts)` order, filtered through `keep`, partitioned into parts
/// closed at the memory split target `l1_part_memory_target_bytes` or the stored-size
/// target `max_l1_part_bytes`, whichever is reached first.
///
/// Both callers of the RLOG merge run through here. Compaction
/// ([`RlogCodec::build_parts`]) keeps every record; the ADR-0064 erasure
/// rewrite (`erasure_rewrite::build_rewrite_logs`) keeps the records no pending
/// request's matcher drops. Sharing the driver is what gives the rewrite the
/// same memory split target issue #711 gave compaction (issue #725): peak resident
/// memory is one decoded block per input plus the in-progress part, never the
/// bucket. It also means the two cannot drift on merge order, on where a part
/// splits, or on the stream-identity invariant below.
///
/// `indexed_fields` is the POSTINGS field list every part is written with, and
/// `dry_run` decides whether each part is PUT as it closes: compaction PUTs
/// here, while the erasure rewrite defers every PUT to its own publish path,
/// which writes parts only after its conservation gate passes.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn merge_catalogs(
    store: &dyn ObjectStoreBackend,
    config: &CompactorConfig,
    bucket: &Bucket,
    catalogs: &[RlogInputCatalog],
    input_set_hash: &[u8; 32],
    indexed_fields: Vec<String>,
    dry_run: bool,
    keep: &mut (dyn FnMut(&LogRecord) -> Result<bool> + Send),
) -> Result<MergeOutput> {
    // Global stream_ref remap + cross-object stream-identity check. The
    // merged set is the sorted union of every input's STREAM_DIR; the dense
    // merged stream_ref is the ordinal in this set (the writer re-derives it
    // per part, so we need only the ordering here). Two inputs claiming the
    // same stream_id with different blobs is a fatal invariant breach.
    let mut merged: BTreeMap<LogStreamId, Vec<u8>> = BTreeMap::new();
    for catalog in catalogs {
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
    // catalogs. Each stream's blocks are fetched by range in the k-way merge
    // below one decode unit at a time -- a block under version 3, a row group
    // under version 4 (ADR-0699 decision 1, a block's pages are spread across
    // its group's column chunks) -- so raw resident bytes stay bounded to one
    // such unit per input, never a whole stream or the whole bucket.
    let identity = compactor_identity(bucket, config);
    let tracker = config.merge_memory_tracker.as_ref();
    let mut sink = PartSink {
        store,
        bucket,
        config,
        input_set_hash,
        identity,
        indexed_fields,
        tracker,
        dry_run,
        current: None,
        parts: Vec::new(),
        part_index: 0,
    };
    let mut counts = RecordCounts::default();

    // Merge stream by stream in sorted stream_id order. Each stream is
    // k-way merged from every input carrying it (ts-ascending, canonical
    // input-order tie-break) straight into the current part's writer, and
    // the part flushes the moment its accumulated record-heap estimate
    // reaches `l1_part_memory_target_bytes` (the memory split target) or its encoded
    // estimate reaches `max_l1_part_bytes` (the stored target), whichever comes
    // first -- mid-stream if that is where the target falls (issue #711 for the
    // memory split target, issue #872 for the stored target, ADR-0032 amendment
    // 2026-08-26). No intermediate
    // `Vec<LogRecord>` and no per-stream dedup: distinct submissions of
    // identical content are distinct records (ADR-0032).
    for stream_id in merged.keys() {
        merge_stream_into_parts(
            store,
            catalogs,
            stream_id,
            &mut sink,
            tracker,
            keep,
            &mut counts,
        )
        .await?;
    }
    let parts = sink.finish().await?;
    Ok(MergeOutput {
        parts,
        input_record_count: counts.input,
        output_record_count: counts.output,
    })
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

/// The sequence of L1 parts one merge produces, and the one place a part is
/// closed and a new one opened.
///
/// The split rule pushes records in the merge's canonical order and closes the
/// in-progress part as soon as EITHER target is reached (issue #872): its
/// [`PartBuilder::estimate`] (decoded heap) reaches the memory split target
/// `l1_part_memory_target_bytes`, or its [`PartBuilder::stored_estimate`] (encoded
/// bytes) reaches the stored target `max_l1_part_bytes`, wherever in the record
/// sequence that falls. The memory split target is the whole of issue #711: the check
/// used to run only between streams, so a bucket whose records all belong to
/// one stream (one OTLP resource/scope, the common shape for a single busy
/// service) held its entire row set live in one writer.
///
/// Consecutive parts may therefore carry the same `stream_id` at their shared
/// boundary, with adjacent, non-overlapping `(series_id, ts)` ranges. Records
/// still enter each part in global `(stream_id, ts)` order, so every part is
/// individually sorted and is written by the frozen [`RlogWriter`] unchanged:
/// only the partitioning of records into parts differs, never a part's bytes
/// given its record set.
struct PartSink<'a> {
    store: &'a dyn ObjectStoreBackend,
    bucket: &'a Bucket,
    config: &'a CompactorConfig,
    input_set_hash: &'a [u8; 32],
    identity: ObjectIdentity,
    indexed_fields: Vec<String>,
    tracker: Option<&'a MergeMemoryTracker>,
    /// Whether a closed part is encoded but not PUT. Not read from `config`:
    /// the erasure rewrite defers every part PUT to its own publish path (which
    /// writes them only after its conservation gate passes) while running with
    /// a config whose `dry_run` is false.
    dry_run: bool,
    current: Option<PartBuilder>,
    parts: Vec<BuiltPart>,
    part_index: u32,
}

impl PartSink<'_> {
    /// Push one merged record, opening a part if none is in progress and
    /// closing it once the cap is reached.
    async fn push(&mut self, r: LogRecord) -> Result<()> {
        if self.current.is_none() {
            self.current = Some(PartBuilder::new(&self.identity, &self.indexed_fields));
        }
        let mut over_memory = false;
        let mut over_stored = false;
        if let Some(part) = self.current.as_mut() {
            part.push(r, self.tracker)?;
            // Two independent targets; a part closes when EITHER is reached
            // (issue #872). The memory split target sizes compactor peak memory
            // (issue #711); the stored target governs object geometry. Both are
            // checked here, after every record, so a part exceeds whichever
            // fires by at most one record. On a
            // wide schema the heap estimate reaches its target first (small
            // objects); on a narrow, highly compressible schema the encoded
            // estimate reaches its target first. With the shipped defaults
            // (both 256 MiB) the memory split target fires and the stored target
            // does not, so this crate's geometry is unchanged.
            over_memory = part.estimate >= self.config.l1_part_memory_target_bytes;
            over_stored = part.stored_estimate >= self.config.max_l1_part_bytes;
        }
        if over_memory || over_stored {
            if let Some(t) = self.tracker {
                // The memory split target is the one that keeps the host alive;
                // when both fire at once attribute the flush to it.
                if over_memory {
                    t.note_memory_target_flush();
                } else {
                    t.note_stored_target_flush();
                }
            }
            self.flush().await?;
        }
        Ok(())
    }

    /// Close the in-progress part, if any, and PUT it. A part is never empty
    /// here: [`Self::push`] only ever calls this after pushing a record, and
    /// [`Self::finish`] guards on `is_empty`.
    async fn flush(&mut self) -> Result<()> {
        // The `if let` (rather than an `unwrap`/`expect`) keeps the critical
        // path free of a panic path; a `None` here is a no-op, not a lie.
        if let Some(builder) = self.current.take() {
            let built = builder
                .finish(
                    self.store,
                    self.bucket,
                    self.input_set_hash,
                    self.part_index,
                    self.dry_run,
                )
                .await?;
            self.parts.push(built);
            self.part_index += 1;
            if let Some(t) = self.tracker {
                t.set_writer_bytes(0);
            }
        }
        Ok(())
    }

    /// Close the trailing part and yield every part built, in part-index order.
    async fn finish(mut self) -> Result<Vec<BuiltPart>> {
        if self.current.as_ref().is_some_and(|p| !p.is_empty()) {
            self.flush().await?;
        }
        Ok(self.parts)
    }
}

/// One in-progress L1 part: the shared [`RlogWriter`] the merged records push
/// into directly, plus the two running estimates that decide where the part
/// splits (the decoded-heap `estimate` against the memory split target and the encoded
/// `stored_estimate` against the stored target, whichever is reached first, see
/// [`PartSink`]); the heap estimate also feeds the [`MergeMemoryTracker`]'s
/// writer term. Holding a
/// whole part's records before [`PartBuilder::finish`] stamps its
/// content-addressed key is unavoidable (the key is a hash of the whole
/// object); the k-way merge is what keeps everything *else* bounded.
struct PartBuilder {
    writer: RlogWriter,
    /// Sum of [`estimate_record`] over every pushed record: the **memory
    /// bound**'s trigger (compared against `l1_part_memory_target_bytes`) and the
    /// writer-buffer term the tracker records. This is decoded Rust heap, not
    /// stored bytes.
    estimate: u64,
    /// Sum of [`estimate_stored_record`] over every pushed record, plus
    /// [`estimate_stored_stream`] once per distinct stream in this part: the
    /// **stored-size target**'s trigger (compared against `max_l1_part_bytes`).
    /// This estimates encoded/on-object bytes, not heap.
    stored_estimate: u64,
    /// The streams whose STREAM_DIR entry this part has already been charged
    /// for. A set rather than a "did the stream change" check so the charge is
    /// once per distinct stream whatever order records arrive in; it holds one
    /// 16-byte id per stream in the part, which is negligible beside the part's
    /// records.
    charged_streams: BTreeSet<LogStreamId>,
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
            stored_estimate: 0,
            charged_streams: BTreeSet::new(),
            min_stream: None,
            max_stream: None,
            count: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Push one merged record into the part's writer, updating both running
    /// estimates (heap for the memory split target, encoded for the stored target) and
    /// the part's inclusive stream-id bounds.
    ///
    /// A record whose stream this part has not seen before also charges the
    /// stored estimate for that stream's STREAM_DIR entry
    /// ([`estimate_stored_stream`]): the blob is written once per stream in the
    /// object, so it belongs in the encoded estimate once per stream, not once
    /// per record and not never.
    fn push(&mut self, r: LogRecord, tracker: Option<&MergeMemoryTracker>) -> Result<()> {
        self.min_stream = Some(self.min_stream.map_or(r.stream_id, |m| m.min(r.stream_id)));
        self.max_stream = Some(self.max_stream.map_or(r.stream_id, |m| m.max(r.stream_id)));
        self.estimate = self.estimate.saturating_add(estimate_record(&r));
        if self.charged_streams.insert(r.stream_id) {
            self.stored_estimate = self
                .stored_estimate
                .saturating_add(estimate_stored_stream(&r.stream_attrs));
        }
        self.stored_estimate = self
            .stored_estimate
            .saturating_add(estimate_stored_record(&r));
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
/// current block, and the next block is decoded only once the current one is
/// drained. `input_index` is the cursor's
/// canonical position in `catalogs`, the k-way merge's tie-break on equal
/// `ts_ns`.
///
/// The fetch unit and the decode unit differ under RLOG version 4. A version-4
/// block's pages are spread across its row group's column chunks (ADR-0699
/// decision 1), so the smallest contiguous range that holds one block is its
/// whole row group: `locs` is one loc per group and each is fetched with one
/// ranged GET. The *decoded* unit stays one block: `group` keeps the fetched
/// group's raw (still compressed) bytes while its blocks are decoded one at a
/// time out of them, each released before the next is decoded (issue #748).
/// Under version 3 a loc is a single block and the two units coincide, which is
/// why this is one code path and not two.
struct StreamCursor<'a> {
    input_index: usize,
    object_key: &'a str,
    reader: &'a RlogRangeReader,
    /// Candidate blocks for the stream, ascending (ts-ascending for the
    /// stream), one loc per row group under version 4 and one per block under
    /// version 3, consumed front to back via `next_loc`.
    locs: Vec<StreamBlockLoc>,
    next_loc: usize,
    /// The current loc's raw bytes, held while its blocks are decoded one at a
    /// time, and dropped as its last block is decoded. `None` between locs.
    group: Option<(StreamBlockLoc, bytes::Bytes)>,
    /// The raw byte count `group` still has charged to the tracker's fetched
    /// term, released when the group's last block is decoded.
    group_raw_bytes: u64,
    /// How many of `group`'s blocks have been decoded already.
    decoded_in_group: usize,
    /// Remaining records of the current decoded block.
    block: std::vec::IntoIter<LogRecord>,
    /// The current block's decoded-byte estimate, held live in the tracker
    /// until the block is released.
    block_bytes: u64,
    /// The next loc's raw bytes, already fetched (issue #711). At most one:
    /// the prefetch is one loc ahead, so the cursor's raw-byte residency is
    /// two locs, not the stream.
    prefetched: Option<(StreamBlockLoc, bytes::Bytes)>,
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
            group: None,
            group_raw_bytes: 0,
            decoded_in_group: 0,
            block: Vec::new().into_iter(),
            block_bytes: 0,
            prefetched: None,
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
    /// or the first record of the next non-empty block (decoded here out of the
    /// current loc's bytes, fetching the next loc by range when the current one
    /// is spent), or `None` when the stream is exhausted in this input. At most
    /// one decoded block plus at most two locs' raw bytes (the one being decoded
    /// from and the one prefetched behind it) are resident at a time.
    async fn refill(
        &mut self,
        store: &dyn ObjectStoreBackend,
        tracker: Option<&MergeMemoryTracker>,
    ) -> Result<()> {
        if let Some(rec) = self.block.next() {
            self.head = Some(rec);
            return Ok(());
        }
        // Current block drained: release it and decode the next non-empty one.
        self.release_block(tracker);
        loop {
            if let Some(recs) = self.decode_next_block(tracker)? {
                self.block = recs.into_iter();
                if let Some(rec) = self.block.next() {
                    self.head = Some(rec);
                    return Ok(());
                }
                // A candidate block with no row for this stream (a neighbour's
                // boundary block): release and try the next.
                self.release_block(tracker);
                continue;
            }
            // The current loc is fully decoded: fetch the next one.
            match self.next_raw_block(store, tracker).await? {
                Some((loc, data)) => {
                    self.group_raw_bytes = data.len() as u64;
                    self.decoded_in_group = 0;
                    self.group = Some((loc, data));
                }
                None => {
                    self.head = None;
                    return Ok(());
                }
            }
        }
    }

    /// Decode the current loc's next undecoded block, or `None` when the loc is
    /// spent (or absent).
    ///
    /// The loc's raw bytes stay resident across this call so the following block
    /// can be decoded from them, and are dropped as the last block is decoded:
    /// the tracker's raw term is therefore per loc (one row group under version
    /// 4, one block under version 3) while its decoded term is per block.
    fn decode_next_block(
        &mut self,
        tracker: Option<&MergeMemoryTracker>,
    ) -> Result<Option<Vec<LogRecord>>> {
        let Some((loc, data)) = self.group.take() else {
            return Ok(None);
        };
        let Some(&block) = loc.block_indices().get(self.decoded_in_group) else {
            // `stream_blocks` never returns a loc with no blocks; release the
            // raw charge rather than leak it if one ever did.
            if let Some(t) = tracker {
                t.block_decoded(self.group_raw_bytes, 0);
            }
            self.group_raw_bytes = 0;
            self.decoded_in_group = 0;
            return Ok(None);
        };
        let last = self.decoded_in_group + 1 == loc.block_indices().len();
        let recs = self
            .reader
            .decode_block_in_group(&loc, block, data.as_ref())?;
        self.decoded_in_group += 1;
        let decoded_bytes: u64 = recs.iter().map(estimate_record).sum();
        if last {
            // Both the raw loc and this block's records are resident at this
            // instant; the raw buffer is dropped right after.
            let raw_len = self.group_raw_bytes;
            if let Some(t) = tracker {
                t.block_decoded(raw_len, decoded_bytes);
            }
            self.group_raw_bytes = 0;
            self.decoded_in_group = 0;
            drop(data);
        } else {
            // The raw loc is kept for the blocks still to be decoded from it,
            // so nothing is released from the fetched term yet.
            if let Some(t) = tracker {
                t.block_decoded(0, decoded_bytes);
            }
            self.group = Some((loc, data));
        }
        self.block_bytes = decoded_bytes;
        Ok(Some(recs))
    }

    /// The next candidate block's raw bytes, or `None` once the candidate list
    /// is exhausted.
    ///
    /// The GET for the block *after* it is issued in the same concurrent
    /// `try_join` and its bytes are kept in `prefetched` (issue #711), so the
    /// cursor advances two blocks per round trip instead of one. Futures are
    /// lazy, so a stored-but-unpolled future would not be in flight; joining
    /// the two fetches is what actually overlaps them. Residency stays
    /// bounded: exactly one prefetched raw block per cursor, never a window
    /// that grows with the stream.
    async fn next_raw_block(
        &mut self,
        store: &dyn ObjectStoreBackend,
        tracker: Option<&MergeMemoryTracker>,
    ) -> Result<Option<(StreamBlockLoc, bytes::Bytes)>> {
        if let Some((loc, data)) = self.prefetched.take() {
            return Ok(Some((loc, data)));
        }
        let Some(loc) = self.take_next_loc() else {
            return Ok(None);
        };
        let data = match self.take_next_loc() {
            Some(ahead) => {
                let (a, b) = futures::try_join!(
                    fetch_block(store, self.object_key, &loc),
                    fetch_block(store, self.object_key, &ahead)
                )?;
                if let Some(t) = tracker {
                    t.block_fetched(b.len() as u64);
                }
                self.prefetched = Some((ahead, b));
                a
            }
            None => fetch_block(store, self.object_key, &loc).await?,
        };
        if let Some(t) = tracker {
            t.block_fetched(data.len() as u64);
        }
        Ok(Some((loc, data)))
    }

    /// Consume the next candidate block location, if any.
    fn take_next_loc(&mut self) -> Option<StreamBlockLoc> {
        let loc = self.locs.get(self.next_loc)?.clone();
        self.next_loc += 1;
        Some(loc)
    }
}

/// One block's raw bytes by range. Named (not an inline `async` block) so its
/// future is `Send`-general over the borrowed store; see
/// [`StreamCursor::next_raw_block`].
async fn fetch_block(
    store: &dyn ObjectStoreBackend,
    object_key: &str,
    loc: &StreamBlockLoc,
) -> Result<bytes::Bytes> {
    let got = store
        .get(object_key, GetRange::Range(loc.start(), loc.end()))
        .await?;
    Ok(got.data)
}

/// K-way merge one stream from every input carrying it into `part`, in
/// ts-ascending order with ties broken by canonical input order. This is
/// byte-for-byte the ordering the old "concatenate every input's decoded
/// records, then stable-sort by `ts_ns`" produced -- each input's stream is
/// already ts-ascending (the format's `(stream_ref, ts)` order), so a stable
/// sort of the concatenation orders equal-`ts_ns` records by (input, position),
/// exactly what selecting the minimum `(ts_ns, input_index)` head does here.
///
/// Every merged record is tallied in `counts` and offered to `keep`; the ones
/// it keeps go into `sink`, which closes the in-progress part and opens the
/// next one the moment the size cap is reached -- possibly in the middle of
/// this stream (issue #711). The merge order is unaffected: the sink is a pure
/// partitioner over the sequence this function produces, and a dropped record
/// shifts the split points without reordering anything. A record `keep`
/// rejects is released here, so the ADR-0064 erasure rewrite's peak memory is
/// bounded by its survivors' part, not by everything it read (issue #725).
///
/// The cursors are opened concurrently (`input_read_concurrency` at a time):
/// each open costs one ranged GET for the stream's first block, so a stream
/// carried by hundreds of inputs would otherwise serialize hundreds of round
/// trips before the first record merges.
async fn merge_stream_into_parts(
    store: &dyn ObjectStoreBackend,
    catalogs: &[RlogInputCatalog],
    stream_id: &LogStreamId,
    sink: &mut PartSink<'_>,
    tracker: Option<&MergeMemoryTracker>,
    keep: &mut (dyn FnMut(&LogRecord) -> Result<bool> + Send),
    counts: &mut RecordCounts,
) -> Result<()> {
    let concurrency = sink.config.input_read_concurrency.max(1);
    // Box each open-and-first-refill with an explicit `+ Send` bound before
    // `buffered`, the workaround `crate::build::fetch_batch_pages` documents
    // for futures that borrow the `&dyn ObjectStoreBackend`. `buffered` keeps
    // canonical input order, which is the k-way merge's tie-break.
    type CursorFuture<'f, 'a> =
        Pin<Box<dyn Future<Output = Result<Option<StreamCursor<'a>>>> + Send + 'f>>;
    let opens: Vec<CursorFuture<'_, '_>> = catalogs
        .iter()
        .enumerate()
        .map(|(idx, catalog)| {
            Box::pin(open_cursor(store, catalog, idx, stream_id, tracker)) as CursorFuture<'_, '_>
        })
        .collect();
    let opened: Vec<Option<StreamCursor>> = stream_iter(opens)
        .buffered(concurrency)
        .try_collect()
        .await?;
    let mut cursors: Vec<StreamCursor> = opened.into_iter().flatten().collect();
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
            counts.add_input()?;
            if keep(&rec)? {
                counts.add_output()?;
                sink.push(rec).await?;
            }
        }
        cursors[bi].refill(store, tracker).await?;
    }
    Ok(())
}

/// Open one input's cursor over `stream_id` and load its first record. Named
/// (not an inline `async` block) so its future is `Send`-general over the
/// borrowed store; see the call site in [`merge_stream_into_parts`]. Returns
/// `None` when the input does not carry the stream, or carries no record for
/// it.
async fn open_cursor<'a>(
    store: &'a dyn ObjectStoreBackend,
    catalog: &'a RlogInputCatalog,
    input_index: usize,
    stream_id: &LogStreamId,
    tracker: Option<&MergeMemoryTracker>,
) -> Result<Option<StreamCursor<'a>>> {
    let Some(mut cursor) = StreamCursor::open(catalog, input_index, stream_id)? else {
        return Ok(None);
    };
    cursor.refill(store, tracker).await?;
    Ok(cursor.head.is_some().then_some(cursor))
}

/// Encode one in-progress part's writer into an L1 object and PUT it: run the
/// shared writer pipeline via [`RlogWriter::finish_compacted_with_stats`]
/// (stamping `level = 1`, the `input_set_hash`, and `part_index`), then PUT it
/// `CreateIfAbsent`. The part's summary stats are read back from the produced
/// object's own footer, so they describe exactly what was written.
///
/// The writer was built with the same [`RlogWriter::with_indexed_fields`] the
/// L0 write path uses, so this part's POSTINGS is built by the one writer
/// implementation from this part's own blocks (ADR-0049 decision 6). The
/// per-field distinct-value cap therefore applies to the merged part; when it
/// fires the writer drops that field's postings and reports it in
/// `WriteStats`, which is logged here because a silently unindexed field is
/// invisible in the object bytes (they are simply absent, which is always
/// legal).
///
/// `first_stream_id`/`last_stream_id` are the part's inclusive stream-id bounds
/// accumulated as records were pushed (streams are merged in sorted id order,
/// so `first` is the smallest and `last` the largest id in the part). Since
/// issue #711 a part may open or close in the middle of a stream, so one part's
/// `last` may equal the next part's `first`; the bounds are adjacent, not
/// strictly disjoint.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn finalize_part(
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
        declared_column_stats: Vec::new(),
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
pub(crate) fn compactor_identity(bucket: &Bucket, config: &CompactorConfig) -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: bucket.tenant_hash.0,
        shard: bucket.shard,
        writer_id: config.compactor_writer_id.into_bytes(),
        writer_epoch: 0,
        writer_seq: 0,
    }
}

/// Per-heap-allocation bookkeeping charged on top of the bytes asked for: the
/// allocator header plus size-class rounding. A deliberate flat figure, in the
/// range a general-purpose allocator costs for the small allocations a
/// [`LogRecord`] is made of.
const ALLOC_OVERHEAD_BYTES: u64 = 16;

/// The `Vec<LogRecord>` slot one record occupies in the writer, whatever its
/// payload: 10 fixed fields, three `String`/`Vec` headers, and the `attrs`
/// vector header.
const RECORD_SLOT_BYTES: u64 = std::mem::size_of::<LogRecord>() as u64;

/// One `(String, AttrValue)` slot in a record's `attrs` vector: the key's
/// `String` header plus the widest `AttrValue` variant, before any of their
/// heap payloads.
const ATTR_SLOT_BYTES: u64 = std::mem::size_of::<(String, AttrValue)>() as u64;

/// The Rust-side heap one merged [`LogRecord`] occupies while it waits in the
/// in-progress part's writer, which is what the memory split target
/// `l1_part_memory_target_bytes` is measured in (issue #711). This is deliberately NOT the
/// record's encoded size (that is [`estimate_stored_record`], which the
/// stored-size target uses): the writer holds row-major `LogRecord`s, and for a
/// wide log row (the ClickBench shape: ~105 attributes) the Rust representation
/// is an order of magnitude larger than the bytes it compresses to. Sizing the
/// memory split target against encoded payload bytes is what let one 3M-row stream
/// reach 45 GB resident under a nominal 256 MiB cap.
///
/// The formula, one term per real allocation:
///
/// ```text
/// estimate(record) = RECORD_SLOT_BYTES                       // its Vec slot
///                  + alloc(stream_attrs.len())               // per-row resource/scope copy
///                  + alloc(severity_text.len())
///                  + alloc(body.len())
///                  + alloc(attrs.len() * ATTR_SLOT_BYTES)    // the attrs vector itself
///                  + sum over attrs of
///                        alloc(key.len()) + value(v)
///
/// alloc(n)  = 0 if n == 0 else n + ALLOC_OVERHEAD_BYTES
/// value(v)  = alloc(len) for Str/Bytes
///           = alloc(items.len() * slot) + sum of element values for List/Map
///           = 0 for I64/F64/Bool, which live inline in the enum and are
///             already counted by the slot term that holds them
/// ```
///
/// It stays an estimate -- it does not model allocator size classes, `String`
/// capacity above `len`, or shared allocations -- but it is now within a small
/// factor of the true heap rather than a small fraction of it, so a 256 MiB
/// target means roughly 256 MiB of live records in the part it closes. It still only decides where parts
/// split, never correctness.
pub(crate) fn estimate_record(r: &LogRecord) -> u64 {
    let mut est = RECORD_SLOT_BYTES;
    est = est.saturating_add(alloc_estimate(r.stream_attrs.len() as u64));
    est = est.saturating_add(alloc_estimate(r.severity_text.len() as u64));
    est = est.saturating_add(alloc_estimate(r.body.len() as u64));
    est = est.saturating_add(alloc_estimate(r.attrs.len() as u64 * ATTR_SLOT_BYTES));
    for (k, v) in &r.attrs {
        est = est.saturating_add(alloc_estimate(k.len() as u64));
        est = est.saturating_add(attr_value_estimate(v));
    }
    est
}

/// One heap allocation of `n` payload bytes, or nothing at all when `n` is 0
/// (an empty `String`/`Vec` allocates nothing).
fn alloc_estimate(n: u64) -> u64 {
    if n == 0 {
        0
    } else {
        n.saturating_add(ALLOC_OVERHEAD_BYTES)
    }
}

/// The heap one attribute *value* owns beyond its slot in the containing
/// vector. Scalars own none: they live inline in the [`AttrValue`] enum, whose
/// size the slot term already carries.
fn attr_value_estimate(v: &AttrValue) -> u64 {
    match v {
        AttrValue::Str(s) => alloc_estimate(s.len() as u64),
        AttrValue::Bytes(b) => alloc_estimate(b.len() as u64),
        AttrValue::List(items) => {
            let slots = items.len() as u64 * std::mem::size_of::<AttrValue>() as u64;
            items
                .iter()
                .map(attr_value_estimate)
                .fold(alloc_estimate(slots), u64::saturating_add)
        }
        AttrValue::Map(kvs) => {
            let slots = kvs.len() as u64 * ATTR_SLOT_BYTES;
            kvs.iter()
                .map(|(k, v)| alloc_estimate(k.len() as u64).saturating_add(attr_value_estimate(v)))
                .fold(alloc_estimate(slots), u64::saturating_add)
        }
        AttrValue::I64(_) | AttrValue::F64(_) | AttrValue::Bool(_) => 0,
    }
}

/// Fixed per-record encoded cost the stored-size estimate charges regardless of
/// payload: the columnar `ts_ns`, `stream_ref`, and `severity_number` cells a
/// record always occupies (docs/log-segment-format.md). A deliberately flat
/// figure in the range those fixed-width columns cost before compression.
const STORED_RECORD_FIXED_BYTES: u64 = 16;

/// The encoded/on-object bytes one merged [`LogRecord`] is estimated to add to
/// a part, the quantity `max_l1_part_bytes` (the stored-size target) caps.
///
/// This is deliberately NOT [`estimate_record`], which measures the record's
/// Rust *heap* for the memory split target. The two answer different questions and are
/// an order of magnitude apart on a wide schema, which is the whole point of
/// issue #872: the memory split target reached 256 MiB of heap after only ~3.5 MB of
/// stored bytes on the ClickBench tenant, so a single knob measured in heap
/// could not also govern object geometry.
///
/// It is a pre-compression payload proxy: the sum of the value bytes that
/// actually enter this part's columns (timestamps and identifiers as a small
/// fixed cost, then `severity_text`, `body`, and every dynamic attribute's key
/// and value bytes). Two things are excluded on purpose:
///
/// - `stream_attrs` (the resource/scope blob) is stored once per stream in
///   STREAM_DIR, not once per record, so charging it per record would inflate
///   the estimate on exactly the wide-stream shape this is meant to size. It is
///   not free, though: [`estimate_stored_stream`] charges each distinct stream's
///   blob once per part it appears in, so a part carrying many streams with
///   large resource blobs still reaches the target. Charging neither made the
///   estimate blind to the whole STREAM_DIR section, and a many-stream,
///   fat-blob bucket then ran far past the target while the estimate stayed
///   small.
/// - zstd compression. The estimate is the uncompressed column payload, so it
///   is an upper bound on the bytes those columns compress to; a target of `T`
///   payload bytes yields an object of `T / compression_ratio` stored bytes.
///   That makes the target conservative (objects no larger than `T`), which is
///   the safe direction for a memory-adjacent geometry knob, and it is why the
///   size-target default is left at 256 MiB: on the current corpus the memory
///   bound still fires first, so no stored target value can change today's
///   geometry.
///
/// Like [`estimate_record`] it only decides where parts split, never
/// correctness: the frozen [`RlogWriter`] produces identical bytes for a given
/// record set however the records were partitioned.
pub(crate) fn estimate_stored_record(r: &LogRecord) -> u64 {
    let mut est = STORED_RECORD_FIXED_BYTES;
    est = est.saturating_add(r.severity_text.len() as u64);
    est = est.saturating_add(r.body.len() as u64);
    for (k, v) in &r.attrs {
        est = est.saturating_add(k.len() as u64);
        est = est.saturating_add(attr_value_stored_estimate(v));
    }
    est
}

/// Fixed per-stream encoded cost of one STREAM_DIR entry beyond its blob: the
/// 16-byte `stream_id` plus the entry's `blob_len`, `first_blk`, and `last_blk`
/// varints (docs/log-segment-format.md, "STREAM_DIR (uncompressed form)"). A
/// deliberately flat figure at the top of the varints' range, in the same spirit
/// as [`STORED_RECORD_FIXED_BYTES`].
const STORED_STREAM_DIR_ENTRY_BYTES: u64 = 32;

/// The encoded/on-object bytes one distinct stream adds to a part: its
/// STREAM_DIR entry, which is the resource/scope blob plus
/// [`STORED_STREAM_DIR_ENTRY_BYTES`] of entry overhead.
///
/// [`PartBuilder`] charges this once per distinct stream in the part, which is
/// exactly how the writer stores it: the blob is written once per stream in
/// STREAM_DIR, not once per record ([`estimate_stored_record`] therefore leaves
/// it out). A part that opens a stream, closes, and reopens the same stream in
/// the next part is charged in both, because both parts carry the entry.
///
/// Without this charge the stored estimate ignored STREAM_DIR entirely, so a
/// bucket with many streams and large resource or scope blobs could run far past
/// `max_l1_part_bytes` in real object bytes while its estimate stayed near the
/// (tiny) sum of its record payloads. The estimate is only meaningful as a
/// proxy for object size if it counts every section that grows with the data.
fn estimate_stored_stream(stream_attrs: &[u8]) -> u64 {
    STORED_STREAM_DIR_ENTRY_BYTES.saturating_add(stream_attrs.len() as u64)
}

/// The encoded value bytes one attribute *value* contributes to the stored-size
/// estimate. Scalars cost their fixed column width; strings and byte strings
/// cost their length; nested containers cost the sum of their elements plus a
/// small per-element tag. No allocator or Rust-slot overhead is counted (that
/// is [`attr_value_estimate`]'s job for the heap bound), because these bytes
/// are what land in the object, not what the record occupies in memory.
fn attr_value_stored_estimate(v: &AttrValue) -> u64 {
    match v {
        AttrValue::Str(s) => s.len() as u64,
        AttrValue::Bytes(b) => b.len() as u64,
        AttrValue::I64(_) | AttrValue::F64(_) => 8,
        AttrValue::Bool(_) => 1,
        AttrValue::List(items) => items
            .iter()
            .map(attr_value_stored_estimate)
            .fold(0u64, |acc, n| acc.saturating_add(n).saturating_add(1)),
        AttrValue::Map(kvs) => kvs.iter().fold(0u64, |acc, (k, v)| {
            acc.saturating_add(k.len() as u64)
                .saturating_add(attr_value_stored_estimate(v))
                .saturating_add(1)
        }),
    }
}

/// The dynamic attribute names one input object carries POSTINGS for.
///
/// # Why the inputs are the source
///
/// ADR-0049 decision 3 makes the indexed-field list explicit per-tenant
/// configuration, and that configuration is not yet built: it is not on main, so
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
/// Once per-tenant configuration of the indexed-field list exists, a read of
/// that list replaces this whole function. That changes behaviour in two ways
/// the inputs cannot express: a newly configured field becomes indexed at the
/// next compaction even though no input indexed it, and a de-configured field
/// stops being indexed even though its inputs still carry postings.
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
    use std::sync::Arc;

    use bytes::Bytes;
    use proptest::prelude::*;
    use prost::Message;
    use ravel_catalog::{Catalog, CatalogConfig};
    use ravel_commit::record::{self, NewCommitRecord};
    use ravel_logseg::field_dir::FieldDir;
    use ravel_logseg::record::FieldType;
    use ravel_logseg::skip_index::SkipIndex;
    use ravel_logseg::{FieldSel, RlogConfig, RlogReader, RlogWriter, footer, read_section};
    use ravel_logseg::{LogRecord, Predicate, stream_attrs_bytes, writer::ObjectIdentity};
    use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, list_all};
    use ravel_proto::commit::v1::CompactionRecord;
    use ravel_types::logstream::{AttrValue, LogStreamId, canonical_attr_bytes, log_stream_id};
    use ravel_types::{Signal, TenantHash, TenantId, TimeRange};
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

    // --- POSTINGS rebuild helpers (ADR-0049 decision 6) ----------

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
            // A small row group (production default 32). Under RLOG version 4
            // the merge's fetch and decode unit is a row group, not a block
            // (ADR-0699 decision 1: a block's pages are spread across its
            // group's column chunks, so no smaller contiguous range holds all
            // of one block), so a test that grows a stream past the group cap
            // has to use a group small enough for the caps to bind at test
            // scale.
            group_target_blocks: 2,
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

    /// The POSTINGS-rebuild acceptance test. POSTINGS in an L1 part is rebuilt
    /// from the merged, re-blocked records: every term's posting list names the
    /// output's own block indices (checked against an oracle derived from the
    /// output's decoded rows and its own SKIP_IDX, never from POSTINGS), and a
    /// query over the L1 part returns exactly the records the same query returns
    /// The rebuild inherits POSTINGS v2's merged-view indexing with no code of
    /// its own (ADR-0049's amendment).
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
    /// from the per-record layer. That is a separate gap in the per-record layer.
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

    /// The output's indexed-field list is the union of its inputs' (until it becomes per-tenant configuration): a
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
        // assert that constant against itself. The `open` above
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

    /// Many distinct streams and a tiny part cap: parts get ascending,
    /// non-overlapping stream-id ranges and the union of records is preserved.
    ///
    /// Since issue #711 the ranges are *adjacent*, not strictly disjoint: a
    /// part can close mid-stream, so one part's `last_series_id` may equal the
    /// next part's `first_series_id`. This test therefore asserts `<=` between
    /// parts; the strict-disjointness case is covered by the record-level
    /// checks in `single_stream_bucket_splits_into_bounded_parts`.
    #[tokio::test]
    async fn part_splitting_keeps_stream_ranges_ordered_and_non_overlapping() {
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

        // Part stream-id ranges are ascending and non-overlapping (adjacent at
        // a mid-stream split, strictly increasing otherwise).
        let mut prev_last: Option<[u8; 16]> = None;
        for (i, p) in rec.parts.iter().enumerate() {
            let first: [u8; 16] = p.first_series_id.as_slice().try_into().unwrap();
            let last: [u8; 16] = p.last_series_id.as_slice().try_into().unwrap();
            assert!(first <= last);
            if let Some(pl) = prev_last {
                assert!(
                    pl <= first,
                    "part stream ranges must be ordered and non-overlapping"
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

    // --- mid-stream part splitting (issue #711) ------------------------------

    /// Event-time base for the split fixtures: the start of the fixture
    /// bucket's own ingest hour, so every record's event time sits inside the
    /// hour its bucket is keyed by and the catalog's listing window covers it.
    const SPLIT_BASE_NS: i64 = HOUR as i64 * NS_PER_HOUR;

    /// One wide record of stream 0 at ordinal `i`. Every record this builds has
    /// the *same* [`estimate_record`] value -- fixed attribute count, fixed key
    /// lengths, fixed value lengths, and a zero-padded body and ordinal that do
    /// not change width over the range the tests use -- so a part's record
    /// capacity is `ceil(cap / estimate)` exactly and the part count is
    /// arithmetic, not an observation.
    fn wide_record(i: i64) -> LogRecord {
        let attrs: Vec<(String, AttrValue)> = (0..8u32)
            .map(|k| {
                (
                    format!("attr_{k:02}"),
                    AttrValue::Str(format!("value-{:08}-{k:02}", i % 100_000_000)),
                )
            })
            .collect();
        record(0, SPLIT_BASE_NS + i, &format!("row-{i:08}"), attrs)
    }

    /// The number of parts a single-stream corpus of `records` records must
    /// produce under `cap`: a part takes records until its running estimate
    /// reaches the cap, so it holds `ceil(cap / estimate)` of them.
    fn expected_part_count(record_estimate: u64, cap: u64, records: u64) -> u64 {
        let per_part = cap.div_ceil(record_estimate);
        records.div_ceil(per_part)
    }

    /// Read a compacted logs bucket back the way a query does: through the real
    /// [`ravel_catalog::Catalog`] resolve, then decode every segment it serves,
    /// concatenated in the resolver's own segment order.
    async fn resolver_rows(store: &Arc<MemoryStore>, range: TimeRange) -> Vec<LogRecord> {
        let catalog = Catalog::new(
            Arc::clone(store) as Arc<dyn ObjectStoreBackend>,
            CatalogConfig {
                shard_count: SHARD + 1,
                ..CatalogConfig::default()
            },
        )
        .expect("catalog");
        let resolved = catalog
            .resolve(&tenant_hash(), Signal::Logs, range, &[], sealed_now_ns())
            .await
            .expect("resolve");
        let mut rows = Vec::new();
        for segment in &resolved.segments {
            let got = store
                .get(&segment.data_object_key, GetRange::Full)
                .await
                .expect("get segment");
            rows.extend(decode_all(got.data.as_ref()));
        }
        rows
    }

    /// Issue #711: a bucket whose records all belong to ONE stream, and whose
    /// total estimated heap exceeds `max_l1_part_bytes` several times over,
    /// splits into exactly the number of parts the cap implies. The stream
    /// straddles every boundary.
    ///
    /// Demonstrated red by restoring the pre-#711 between-streams-only check:
    /// delete the `if over_memory || over_stored { .. self.flush().await?; }`
    /// block from `PartSink::push` and re-add the flush to `build_parts`'s
    /// `for stream_id in merged.keys()` loop (`if part.estimate >=
    /// config.l1_part_memory_target_bytes { sink.flush().await?; }` after
    /// `merge_stream_into_parts`). The part count collapses to 1 and this test
    /// fails at `assert_eq!(parts.len() as u64, expected_parts)` with
    /// `1 != 10`.
    #[tokio::test]
    async fn single_stream_bucket_splits_into_bounded_parts() {
        const PER_INPUT: i64 = 600;
        const INPUTS: i64 = 2;
        const CAP: u64 = 64 * 1024;
        let total = (PER_INPUT * INPUTS) as u64;

        let store = Arc::new(MemoryStore::new());
        // Interleaved by input but globally unique in ts, so the canonical
        // merge order is a plain ts-ascending sequence with no tie-break.
        let per_input: Vec<Vec<LogRecord>> = (0..INPUTS)
            .map(|j| {
                (0..PER_INPUT)
                    .map(|i| wide_record(i * INPUTS + j))
                    .collect()
            })
            .collect();
        for (j, recs) in per_input.iter().enumerate() {
            seed(
                store.as_ref(),
                Uuid::from_u128(j as u128 + 1),
                j as u64 + 1,
                recs,
            )
            .await;
        }

        let record_estimate = estimate_record(&wide_record(0));
        let expected_parts = expected_part_count(record_estimate, CAP, total);
        assert!(
            expected_parts >= 4,
            "fixture must exceed the cap several times over, got {expected_parts} parts"
        );

        let config = CompactorConfig {
            l1_part_memory_target_bytes: CAP,
            ..CompactorConfig::default()
        };
        let clock = FixedClock::new(sealed_now_ns());
        compact_bucket(store.as_ref(), &clock, &config, &bucket())
            .await
            .expect("compact");

        let (rec, parts) = read_output(store.as_ref()).await;
        assert_eq!(
            parts.len() as u64,
            expected_parts,
            "cap {CAP} over {total} records estimated at {record_estimate} bytes each"
        );

        // Every record exactly once, across the parts, in canonical order.
        let mut merged: Vec<LogRecord> = Vec::new();
        for p in &parts {
            merged.extend(decode_all(p));
        }
        let expected: Vec<LogRecord> = (0..total as i64).map(wide_record).collect();
        assert_eq!(merged.len() as u64, total, "no record lost or duplicated");
        assert_eq!(
            canon_multiset(&merged),
            canon_multiset(&expected),
            "the part union must equal the input union"
        );
        assert_eq!(
            merged.iter().map(|r| r.ts_ns).collect::<Vec<_>>(),
            expected.iter().map(|r| r.ts_ns).collect::<Vec<_>>(),
            "concatenating the parts in part order must reproduce canonical order"
        );

        // The stream really straddles: every part carries the one stream id,
        // and the parts' bounds are adjacent and non-overlapping in event time.
        let (stream0, _) = stream_ident(0);
        let mut prev_max: Option<i64> = None;
        let mut summed: u64 = 0;
        for p in &rec.parts {
            assert_eq!(p.first_series_id, stream0.0.to_vec());
            assert_eq!(p.last_series_id, stream0.0.to_vec());
            assert!(p.min_event_ts_ns <= p.max_event_ts_ns);
            if let Some(prev) = prev_max {
                assert!(
                    prev < p.min_event_ts_ns,
                    "part event-time ranges must be adjacent and non-overlapping"
                );
            }
            prev_max = Some(p.max_event_ts_ns);
            summed += p.sample_count;
        }
        assert_eq!(summed, total, "sum(part.sample_count) conserves the count");

        // And the query path serves exactly those rows back.
        let rows = resolver_rows(
            &store,
            TimeRange {
                start_ns: SPLIT_BASE_NS,
                end_ns: SPLIT_BASE_NS + total as i64 + 1,
            },
        )
        .await;
        assert_eq!(
            canon_multiset(&rows),
            canon_multiset(&expected),
            "the resolver must serve the same rows the unsplit bucket held"
        );
    }

    /// Issue #711: peak resident memory tracks the part cap, not the record
    /// count. The same single-stream shape is compacted at 2x and 8x the
    /// records; the tracker's total high-water must barely move. The fixture is
    /// written at the production default row group size, so this pins the
    /// property at the setting production runs with.
    ///
    /// Demonstrated red twice. Against the unsplit code (the same flip as
    /// `single_stream_bucket_splits_into_bounded_parts`): the whole stream then
    /// lands in one writer, so the writer term is the whole corpus and the
    /// ratio goes to about 4 (the ratio of the two record counts), failing
    /// `ratio < 1.3`. And against the per-row-group decode issue #748 replaced:
    /// flipping `StreamCursor::decode_next_block` back to
    /// `decode_block(&loc, ..)` with `last` forced true makes the decoded term
    /// a whole input's stream at these defaults, giving 2x=1195290 8x=3991290
    /// ratio=3.34.
    #[tokio::test]
    async fn merge_peak_total_scales_with_the_cap_not_the_record_count() {
        const BASE: i64 = 400;
        const CAP: u64 = 256 * 1024;
        const INPUTS: i64 = 2;
        // The inputs are re-blocked at 100 records, at the production default
        // row group size (32 blocks): at this scale an input's whole stream is
        // one row group, which is exactly the shape issue #748 is about. The
        // merge's fetch unit is that group, but its decode unit is one block
        // (ADR-0699 amendment), so the decoded term is the same at both scales
        // and only the group's stored (compressed) bytes follow the corpus.
        const L0_BLOCK: i64 = 100;
        // At most one decoded block plus two locs' stored bytes per input.
        // Loose on purpose: the claim under test is the ratio.
        const TRANSIENT_ALLOWANCE: u64 = 4 * 1024 * 1024;

        let mut peaks = Vec::new();
        for scale in [2i64, 8i64] {
            let store = MemoryStore::new();
            let total = BASE * scale;
            for j in 0..INPUTS {
                let recs: Vec<LogRecord> = (0..total / INPUTS)
                    .map(|i| wide_record(i * INPUTS + j))
                    .collect();
                seed_l0(
                    &store,
                    Uuid::from_u128(j as u128 + 1),
                    j as u64 + 1,
                    &recs,
                    RlogConfig {
                        block_target_records: L0_BLOCK as usize,
                        ..RlogConfig::default()
                    },
                    &[],
                )
                .await;
            }

            let tracker = MergeMemoryTracker::new();
            let config = CompactorConfig {
                l1_part_memory_target_bytes: CAP,
                merge_memory_tracker: Some(tracker.clone()),
                ..CompactorConfig::default()
            };
            let clock = FixedClock::new(sealed_now_ns());
            compact_bucket(&store, &clock, &config, &bucket())
                .await
                .expect("compact");

            let (_rec, parts) = read_output(&store).await;
            let peak = tracker.peak_total_bytes();
            println!(
                "[mem:rlog #711] scale={scale} records={total} parts={} \
                 peak_total={peak}B cap={CAP}B",
                parts.len()
            );
            assert!(peak > 0, "the tracker must record real residency");
            peaks.push((peak, parts.len()));
        }

        // 4x the records, and the peak must not follow. Asserted before the
        // shape checks below so the failure a regression produces is the
        // ratio itself, with both numbers in the message.
        let (two_x, eight_x) = (peaks[0].0 as f64, peaks[1].0 as f64);
        let ratio = eight_x / two_x;
        assert!(
            ratio < 1.3,
            "peak scaled with the record count: 2x={two_x} 8x={eight_x} ratio={ratio}"
        );
        for (scale, (peak, parts)) in [2, 8].into_iter().zip(peaks) {
            assert!(
                peak < 2 * CAP + TRANSIENT_ALLOWANCE,
                "scale {scale}: peak {peak} exceeded 2x the cap plus the per-input transient"
            );
            assert!(parts > 1, "scale {scale}: the fixture must actually split");
        }
    }

    // --- memory split target vs stored-size target (issue #872) ---------------------

    /// One narrow record of stream 0: a tiny body and no attributes, so its
    /// encoded [`estimate_stored_record`] is a handful of bytes while its
    /// decoded [`estimate_record`] still carries the per-record Rust-slot and
    /// resource-blob overhead. This is the "narrow rows, high compression"
    /// direction: stored bytes accumulate far slower than heap, so the
    /// stored-size target can be made to fire first.
    fn narrow_record(i: i64) -> LogRecord {
        record(0, SPLIT_BASE_NS + i, "x", Vec::new())
    }

    /// Issue #872: on a wide schema the MEMORY bound fires first. With both
    /// bounds set to the same value, the decoded heap estimate reaches it long
    /// before the encoded estimate does (wide rows are an order of magnitude
    /// larger in memory than on the object), so every part closes on memory and
    /// none on the stored target. The parts are small on disk, which is exactly
    /// the geometry issue #872 is about.
    #[tokio::test]
    async fn wide_rows_close_the_part_on_the_memory_bound() {
        const PER_INPUT: i64 = 400;
        const INPUTS: i64 = 2;
        // Equal bounds: the only thing that decides which fires is the per-record
        // heap-vs-stored ratio, and for a wide row that ratio is well above 1.
        const BOUND: u64 = 64 * 1024;
        let total = (PER_INPUT * INPUTS) as u64;

        let store = Arc::new(MemoryStore::new());
        for j in 0..INPUTS {
            let recs: Vec<LogRecord> = (0..PER_INPUT)
                .map(|i| wide_record(i * INPUTS + j))
                .collect();
            seed(
                store.as_ref(),
                Uuid::from_u128(j as u128 + 1),
                j as u64 + 1,
                &recs,
            )
            .await;
        }

        let heap = estimate_record(&wide_record(0));
        let stored = estimate_stored_record(&wide_record(0));
        assert!(
            heap > stored * 2,
            "fixture must be wide: heap {heap} should dwarf stored {stored}"
        );
        let expected_parts = expected_part_count(heap, BOUND, total);
        assert!(
            expected_parts >= 4,
            "fixture must split several times, got {expected_parts}"
        );

        let tracker = MergeMemoryTracker::new();
        let config = CompactorConfig {
            l1_part_memory_target_bytes: BOUND,
            max_l1_part_bytes: BOUND,
            merge_memory_tracker: Some(tracker.clone()),
            ..CompactorConfig::default()
        };
        let clock = FixedClock::new(sealed_now_ns());
        compact_bucket(store.as_ref(), &clock, &config, &bucket())
            .await
            .expect("compact");

        let (rec, parts) = read_output(store.as_ref()).await;
        assert_eq!(
            parts.len() as u64,
            expected_parts,
            "part count is the memory arithmetic"
        );
        // The bound that fired: memory only. Every non-final flush is a memory
        // flush; the stored target never triggered.
        assert_eq!(
            tracker.stored_target_flushes(),
            0,
            "no part may close on the stored target for a wide schema"
        );
        assert_eq!(
            tracker.memory_target_flushes(),
            expected_parts - 1,
            "every closed part (all but the trailing one) closed on the memory split target"
        );
        // And the objects really are small on disk relative to the bound, which
        // is the 74x gap issue #872 names.
        for p in &rec.parts {
            assert!(
                p.object_size < BOUND,
                "part stored size {} should sit well under the {BOUND}B bound",
                p.object_size
            );
        }
    }

    /// Issue #872: on a narrow, highly compressible schema the STORED target
    /// fires first. The memory split target is left at its 256 MiB default (far above
    /// anything this corpus reaches in heap) and the stored target is set small,
    /// so every part closes on encoded bytes and none on memory. This is the
    /// follow-up's shape: lowering the stored target is what grows/shrinks
    /// objects, independently of the memory invariant.
    #[tokio::test]
    async fn narrow_rows_close_the_part_on_the_stored_target() {
        const PER_INPUT: i64 = 500;
        const INPUTS: i64 = 2;
        const STORED_TARGET: u64 = 4 * 1024;
        let total = (PER_INPUT * INPUTS) as u64;

        let store = Arc::new(MemoryStore::new());
        for j in 0..INPUTS {
            let recs: Vec<LogRecord> = (0..PER_INPUT)
                .map(|i| narrow_record(i * INPUTS + j))
                .collect();
            seed(
                store.as_ref(),
                Uuid::from_u128(j as u128 + 1),
                j as u64 + 1,
                &recs,
            )
            .await;
        }

        let stored = estimate_stored_record(&narrow_record(0));
        // Every part also pays this fixture's single stream's STREAM_DIR entry
        // once, at its first record, so the records-per-part arithmetic runs
        // against what is left of the target.
        let stream_charge = estimate_stored_stream(&narrow_record(0).stream_attrs);
        assert!(
            stream_charge < STORED_TARGET,
            "fixture is unusable: its stream blob charges {stream_charge}B against a \
             {STORED_TARGET}B target, leaving no room for records"
        );
        let expected_parts = expected_part_count(stored, STORED_TARGET - stream_charge, total);
        assert!(
            expected_parts >= 3,
            "fixture must split several times, got {expected_parts}"
        );

        let tracker = MergeMemoryTracker::new();
        let config = CompactorConfig {
            max_l1_part_bytes: STORED_TARGET,
            merge_memory_tracker: Some(tracker.clone()),
            // `l1_part_memory_target_bytes` is left at its shipped 256 MiB
            // default: nothing this corpus does in heap comes close, so the
            // memory split target can never be what fires.
            ..CompactorConfig::default()
        };
        let clock = FixedClock::new(sealed_now_ns());
        compact_bucket(store.as_ref(), &clock, &config, &bucket())
            .await
            .expect("compact");

        let (_rec, parts) = read_output(store.as_ref()).await;
        assert_eq!(
            parts.len() as u64,
            expected_parts,
            "part count is the stored arithmetic"
        );
        assert_eq!(
            tracker.memory_target_flushes(),
            0,
            "no part may close on the memory split target for this corpus"
        );
        assert_eq!(
            tracker.stored_target_flushes(),
            expected_parts - 1,
            "every closed part (all but the trailing one) closed on the stored target"
        );
    }

    /// Padding bytes in a fat stream's resource blob. The padding is
    /// pseudo-random alphanumerics, not a repeated character: STREAM_DIR is
    /// zstd-compressed as a whole section, so a compressible pad would leave the
    /// object far smaller than the blob bytes the estimate charges and the
    /// object-size assertion below would hold whatever the estimate counted.
    const FAT_BLOB_PAD_BYTES: usize = 4096;

    /// Deterministic pseudo-random alphanumerics, `n` bytes, distinct per
    /// `seed`. An LCG, so the fixture is reproducible and no stream's blob is a
    /// copy of another's.
    fn incompressible_pad(seed: u64, n: usize) -> String {
        const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
        let mut state = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut out = String::with_capacity(n);
        for _ in 0..n {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            out.push(ALPHABET[(state >> 33) as usize % ALPHABET.len()] as char);
        }
        out
    }

    /// Stream `n` with a large resource blob ([`FAT_BLOB_PAD_BYTES`] of padding
    /// in one resource attribute). The id is still the true hash of the blob, as
    /// in [`stream_ident`], so the object carries real stream identity.
    fn fat_stream_ident(n: u32) -> (LogStreamId, Vec<u8>) {
        let res = vec![
            (
                "service.name".to_string(),
                AttrValue::Str(format!("svc{n:03}")),
            ),
            (
                "resource.detail".to_string(),
                AttrValue::Str(incompressible_pad(u64::from(n), FAT_BLOB_PAD_BYTES)),
            ),
        ];
        let id = log_stream_id(&res, "scope", "1", &[]);
        let blob = stream_attrs_bytes(&res, "scope", "1", &[]);
        (id, blob)
    }

    /// One tiny record of the fat stream `n`: the record payload is a handful of
    /// bytes, so everything this fixture's parts weigh comes from STREAM_DIR.
    fn fat_stream_record(n: u32, ts: i64) -> LogRecord {
        let (stream_id, stream_attrs) = fat_stream_ident(n);
        LogRecord {
            stream_id,
            stream_attrs,
            ts_ns: ts,
            observed_ts_ns: ts,
            severity_num: 9,
            severity_text: "INFO".into(),
            body: "x".into(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: Vec::new(),
        }
    }

    /// A part's STREAM_DIR is charged against the stored-size target once per
    /// distinct stream, so a bucket of many streams with large resource blobs
    /// splits on those blobs rather than running past the target on them.
    ///
    /// `estimate_stored_record` excludes `stream_attrs`, correctly: the blob is
    /// stored once per stream, not once per record. Nothing charged it per
    /// stream either, so STREAM_DIR was invisible to the target: this fixture's
    /// records are a few bytes each, so its estimate stayed near 4 KiB while its
    /// single object held ~96 fat blobs.
    ///
    /// Both assertions are magnitudes proportional to the fixture, not floors:
    /// the part count is the stream-charge arithmetic over `STREAMS` streams and
    /// a `STORED_TARGET`-byte target, and each part's real object size stays
    /// within twice the target.
    ///
    /// Demonstrated red by under-charging, with the per-part charge model
    /// below in place: adding `&& self.charged_streams.len() == 1` to the
    /// charge in `PartBuilder::push` (one stream per part instead of every
    /// distinct one) yields a single part of 269_641 bytes, 4.1x the
    /// 65_536-byte target, failing the size assertion and then the count
    /// assertion with `1 != 7`. A flat floor low enough to be safe would have
    /// passed at both.
    #[tokio::test]
    async fn many_streams_charge_stream_dir_against_the_stored_target() {
        const STREAMS: u32 = 96;
        const INPUTS: u64 = 2;
        const STORED_TARGET: u64 = 64 * 1024;

        let store = Arc::new(MemoryStore::new());
        for j in 0..INPUTS {
            // One record per stream per input, so each stream is carried by
            // every input and the merge sees it as one stream, not two.
            let recs: Vec<LogRecord> = (0..STREAMS)
                .map(|s| fat_stream_record(s, SPLIT_BASE_NS + i64::from(s) * 2 + j as i64))
                .collect();
            seed(
                store.as_ref(),
                Uuid::from_u128(u128::from(j) + 1),
                j + 1,
                &recs,
            )
            .await;
        }

        // The split rule, walked: a stream's first record in a part pays the
        // STREAM_DIR entry plus its own payload, later records in the SAME part
        // pay their payload only, and the part closes as soon as the running
        // total reaches the target. Every stream here has the same blob length
        // and the same record shape, so the walk does not depend on which
        // stream id sorts first.
        //
        // The charge is per part, not per merge: `PartBuilder::push` charges a
        // stream once into the builder it is pushed into, and `PartSink::flush`
        // starts the next record on a fresh builder with an empty
        // `charged_streams`. So the walk clears its own charged set at every
        // split. A stream whose records straddle a boundary is charged in both
        // parts, because both objects carry its STREAM_DIR entry. Modelling one
        // charge per stream over the whole walk undercounts the accumulator and
        // can predict fewer parts than the merge produces. At this fixture's
        // blob and record sizes every split lands on a stream's first record,
        // so both models happen to reach the same seven parts; the reset is
        // what keeps the prediction exact when either size moves.
        let charge = estimate_stored_stream(&fat_stream_record(0, 0).stream_attrs);
        let per_record = estimate_stored_record(&fat_stream_record(0, 0));
        assert!(
            charge > per_record * 100,
            "fixture must be STREAM_DIR-dominated: charge {charge} vs record {per_record}"
        );
        let mut expected_parts = 0u64;
        let mut acc = 0u64;
        let mut charged: BTreeSet<u32> = BTreeSet::new();
        for s in 0..STREAMS {
            for _ in 0..INPUTS {
                acc += if charged.insert(s) {
                    charge + per_record
                } else {
                    per_record
                };
                if acc >= STORED_TARGET {
                    expected_parts += 1;
                    acc = 0;
                    charged.clear();
                }
            }
        }
        if acc > 0 {
            expected_parts += 1;
        }
        assert!(
            expected_parts >= 5,
            "fixture must split several times, got {expected_parts}"
        );

        let tracker = MergeMemoryTracker::new();
        let config = CompactorConfig {
            max_l1_part_bytes: STORED_TARGET,
            merge_memory_tracker: Some(tracker.clone()),
            // `l1_part_memory_target_bytes` is left at its shipped 256 MiB
            // default: this corpus holds a few hundred tiny records in heap, so
            // the memory split target can never be what fires and every split
            // below is the stored target's.
            ..CompactorConfig::default()
        };
        let clock = FixedClock::new(sealed_now_ns());
        compact_bucket(store.as_ref(), &clock, &config, &bucket())
            .await
            .expect("compact");

        let (rec, parts) = read_output(store.as_ref()).await;
        assert_eq!(
            tracker.memory_target_flushes(),
            0,
            "no part may close on the memory split target for this corpus"
        );
        // The estimate is only worth having if it tracks the object it names.
        // Each part's real stored size stays within twice the target; uncharged,
        // one part carried every stream's blob.
        for p in &rec.parts {
            assert!(
                p.object_size < 2 * STORED_TARGET,
                "part of {} bytes exceeded twice the {STORED_TARGET}-byte stored target",
                p.object_size
            );
        }
        assert_eq!(
            parts.len() as u64,
            expected_parts,
            "part count is the STREAM_DIR-charge arithmetic"
        );

        // Records are conserved across the split.
        let mut l1: Vec<LogRecord> = Vec::new();
        for p in &parts {
            l1.extend(decode_all(p));
        }
        assert_eq!(
            l1.len() as u64,
            u64::from(STREAMS) * INPUTS,
            "every record survives the split"
        );
    }

    /// Issue #872 deliverable 2: the shipped defaults reproduce today's
    /// geometry. Today's geometry is the memory-target-only split, so a run at
    /// the shipped stored default (256 MiB, hugely above the memory split target on
    /// this fixture) must be byte-for-byte identical -- same part count, same
    /// per-object stored size -- to a run with the stored target disabled
    /// entirely (`u64::MAX`). If the stored target ever perturbed a
    /// memory-target run, this fails. The memory split target is scaled down to 64 KiB
    /// so the fixture splits without a 256 MiB corpus; the ratio to the shipped
    /// stored default is what production ships (memory << stored).
    #[tokio::test]
    async fn shipped_stored_default_does_not_change_memory_bound_geometry() {
        const PER_INPUT: i64 = 400;
        const INPUTS: i64 = 2;
        const MEM_TARGET: u64 = 64 * 1024;

        async fn run(stored_target: u64) -> Vec<u64> {
            let store = Arc::new(MemoryStore::new());
            for j in 0..INPUTS {
                let recs: Vec<LogRecord> = (0..PER_INPUT)
                    .map(|i| wide_record(i * INPUTS + j))
                    .collect();
                seed(
                    store.as_ref(),
                    Uuid::from_u128(j as u128 + 1),
                    j as u64 + 1,
                    &recs,
                )
                .await;
            }
            let config = CompactorConfig {
                l1_part_memory_target_bytes: MEM_TARGET,
                max_l1_part_bytes: stored_target,
                ..CompactorConfig::default()
            };
            let clock = FixedClock::new(sealed_now_ns());
            compact_bucket(store.as_ref(), &clock, &config, &bucket())
                .await
                .expect("compact");
            let (rec, _parts) = read_output(store.as_ref()).await;
            rec.parts.iter().map(|p| p.object_size).collect()
        }

        // Shipped stored default vs the stored target switched off.
        let shipped = run(crate::config::DEFAULT_MAX_L1_PART_BYTES).await;
        let disabled = run(u64::MAX).await;
        assert!(
            shipped.len() > 1,
            "the fixture must split under the memory split target"
        );
        assert_eq!(
            shipped, disabled,
            "the shipped stored default must not change the memory-target geometry: \
             object count and every per-object stored size must be identical"
        );
    }

    /// Issue #872 constraint: raising the stored target far above the memory
    /// bound does not weaken the memory split target. With the stored target at
    /// `u64::MAX` the memory split target is the only thing that can close a part, so
    /// compactor peak memory must stay within the memory split target plus the fixed
    /// decode-side transient even as a hot stream grows.
    #[tokio::test]
    async fn memory_stays_bounded_when_the_stored_target_is_raised_far_above_it() {
        const PER_INPUT: i64 = 800;
        const INPUTS: i64 = 2;
        const MEM_TARGET: u64 = 128 * 1024;
        const TRANSIENT_ALLOWANCE: u64 = 4 * 1024 * 1024;

        let store = Arc::new(MemoryStore::new());
        for j in 0..INPUTS {
            let recs: Vec<LogRecord> = (0..PER_INPUT)
                .map(|i| wide_record(i * INPUTS + j))
                .collect();
            seed_l0(
                store.as_ref(),
                Uuid::from_u128(j as u128 + 1),
                j as u64 + 1,
                &recs,
                RlogConfig {
                    block_target_records: 100,
                    ..RlogConfig::default()
                },
                &[],
            )
            .await;
        }

        let tracker = MergeMemoryTracker::new();
        let config = CompactorConfig {
            l1_part_memory_target_bytes: MEM_TARGET,
            max_l1_part_bytes: u64::MAX, // stored target effectively off
            merge_memory_tracker: Some(tracker.clone()),
            ..CompactorConfig::default()
        };
        let clock = FixedClock::new(sealed_now_ns());
        compact_bucket(store.as_ref(), &clock, &config, &bucket())
            .await
            .expect("compact");

        let (_rec, parts) = read_output(store.as_ref()).await;
        assert!(
            parts.len() > 1,
            "the fixture must split on the memory split target"
        );
        assert!(
            tracker.memory_target_flushes() > 0 && tracker.stored_target_flushes() == 0,
            "the memory split target must be the binding one when the stored target is raised off"
        );
        let peak = tracker.peak_total_bytes();
        assert!(
            peak < MEM_TARGET + TRANSIENT_ALLOWANCE,
            "peak {peak} exceeded the memory split target {MEM_TARGET} plus the transient allowance, \
             so raising the stored target weakened the memory split target"
        );
    }

    // --- concurrent input reads (issue #711) ---------------------------------

    /// Issue #711: `input_read_concurrency` changes only how many reads are in
    /// flight, never a byte of the output. The same bucket is compacted at
    /// concurrency 1 and 8 and every part must be byte-identical.
    ///
    /// The concurrency is proved to be real, not nominal: a FaultStore gate
    /// holds every commit-record GET, and at concurrency 8 exactly 8 are held
    /// simultaneously while at concurrency 1 only ever 1 is. Without the
    /// fan-out the `wait_until_held(8)` below never returns and the test hangs
    /// rather than passing vacuously.
    #[tokio::test]
    async fn input_read_concurrency_changes_timing_not_bytes() {
        const INPUTS: u64 = 16;

        async fn compact_at(concurrency: usize) -> (Vec<[u8; 32]>, usize) {
            let store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
            for j in 0..INPUTS {
                let recs: Vec<LogRecord> = (0..40i64)
                    .map(|i| wide_record(i * INPUTS as i64 + j as i64))
                    .collect();
                seed(
                    store.as_ref(),
                    Uuid::from_u128(u128::from(j) + 1),
                    j + 1,
                    &recs,
                )
                .await;
            }

            // Hold every commit-record GET (".cmt" names commit records and
            // nothing else) so the in-flight count is observable.
            let gate = store.hold(Op::Get, Some(".cmt".to_string()), Occurrence::Always);
            let config = CompactorConfig {
                max_l1_part_bytes: 64 * 1024,
                input_read_concurrency: concurrency,
                ..CompactorConfig::default()
            };
            let task = tokio::spawn({
                let store = Arc::clone(&store);
                async move {
                    let clock = FixedClock::new(sealed_now_ns());
                    compact_bucket(store.as_ref(), &clock, &config, &bucket())
                        .await
                        .expect("compact");
                }
            });

            // The first window fills to exactly `concurrency` and no further:
            // `buffer_unordered` polls no more than that many futures at once.
            gate.wait_until_held(concurrency).await;
            for _ in 0..64 {
                tokio::task::yield_now().await;
            }
            let peak_in_flight = gate.held_count();

            // From here on release everything the gate catches, so the rest of
            // the run (and `read_output`'s own read of the compaction record,
            // which shares the `.cmt` suffix) is never blocked. Parks on the
            // gate's `Notify` rather than spinning.
            let releaser = tokio::spawn({
                let gate = gate.clone();
                async move {
                    loop {
                        gate.wait_until_held(1).await;
                        for id in gate.held() {
                            gate.release(id);
                        }
                    }
                }
            });
            task.await.expect("compaction task");

            let (_rec, parts) = read_output(store.as_ref()).await;
            releaser.abort();
            let hashes = parts
                .iter()
                .map(|p| *blake3::hash(p).as_bytes())
                .collect::<Vec<_>>();
            (hashes, peak_in_flight)
        }

        let (serial_hashes, serial_in_flight) = compact_at(1).await;
        let (concurrent_hashes, concurrent_in_flight) = compact_at(8).await;

        assert_eq!(
            serial_in_flight, 1,
            "concurrency 1 must never have two commit-record GETs in flight"
        );
        assert_eq!(
            concurrent_in_flight, 8,
            "concurrency 8 must hold exactly 8 commit-record GETs at once"
        );
        assert!(
            serial_hashes.len() > 1,
            "the fixture must split into several parts"
        );
        assert_eq!(
            serial_hashes, concurrent_hashes,
            "concurrent input reads must produce byte-identical parts"
        );
    }

    // --- bounded-memory k-way merge ------------------------------

    /// Acceptance test (ADR-0065 decision 4): the RLOG
    /// compaction merge's peak resident decode memory is bounded by block size
    /// times input count, independent of how large one hot stream grows.
    ///
    /// RLOG version 4 split the merge's fetch unit from its decode unit. A
    /// version-4 block's pages are spread across its row group's column chunks
    /// (ADR-0699 decision 1), so the smallest contiguous range holding all of
    /// one block is the group and that is what one ranged GET brings; the
    /// cursor then holds those *stored* bytes and decodes the group's blocks
    /// one at a time out of them, releasing each before the next (issue #748).
    /// So the decoded term is one block per input, as it was under version 3,
    /// and the raw term is one row group of compressed bytes.
    ///
    /// A single hot stream (the common log shape: one busy service carrying
    /// most of a sealed hour) is grown 10x across two runs. The
    /// [`MergeMemoryTracker`]'s recorded *transient* high-water -- the merge's
    /// own decode-side buffers, at most one decoded block plus the raw bytes of
    /// the row groups it is decoding from, per input -- must stay under a fixed
    /// bound and must NOT grow with the stream. The defect was that the old
    /// merge decoded a whole stream's
    /// records from every input into one `Vec`, plus a second copy in the part
    /// accumulator, so peak scaled with stream size; a regression to that shape
    /// would push the `decoded` term to `O(stream)` and break the bound below.
    ///
    /// Demonstrated red against the per-row-group decode issue #748 replaced:
    /// flipping `StreamCursor::decode_next_block` back to
    /// `decode_block(&loc, ..)` with `last` forced true doubles the decoded
    /// term at this fixture's 2-block groups (transient 1452268 B against the
    /// 1098000 B bound), failing `transient < TRANSIENT_BOUND`.
    ///
    /// Deterministic: it asserts on the tracker's accounting, never on process
    /// RSS or allocator hooks, and runs against [`MemoryStore`].
    #[tokio::test]
    async fn merge_peak_memory_is_bounded_independently_of_stream_size() {
        const INPUTS: u32 = 3;
        // l0_blocked_cfg's block target: each input is re-blocked at 1000
        // records, so a grown stream spans many blocks per input.
        const L0_BLOCK: u64 = 1000;
        // l0_blocked_cfg's row group size: the merge's *fetch* unit under
        // version 4 (its decode unit is one block). Both scales below span more
        // than this many blocks per input, so the bound genuinely binds rather
        // than happening to cover the whole stream.
        const L0_GROUP_BLOCKS: u64 = 2;
        // Per-record upper bound on the decoded term. `estimate_record` charges
        // the record's Rust-side heap since issue #711, so these tiny records
        // estimate at ~242 bytes rather than the ~53 payload bytes this bound
        // was first sized against. It is deliberately close to that figure:
        // decoding a whole 2-block group instead of one block has to break it.
        const PER_RECORD_BOUND: u64 = 350;
        // Per-record upper bound on the raw term. Those are stored, compressed
        // bytes: a whole 3000-record input object here is 890 bytes, so this is
        // an order of magnitude of headroom.
        const RAW_PER_RECORD_BOUND: u64 = 4;
        // Per input: one decoded block, plus the raw bytes of at most two row
        // groups (the one being decoded from and the one prefetched behind it).
        const TRANSIENT_BOUND: u64 = INPUTS as u64
            * (L0_BLOCK * PER_RECORD_BOUND + 2 * L0_GROUP_BLOCKS * L0_BLOCK * RAW_PER_RECORD_BOUND);
        // Rough estimated bytes-per-record for the "stream size" the peak is
        // compared against (reporting and the not-scaling assertion only).
        const PER_RECORD_APPROX: i64 = 270;

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

            // 90k tiny records estimate at ~24 MB, far under the 256 MiB
            // default cap, so this corpus still lands in one part. (Since
            // issue #711 that is a consequence of the cap, not of a
            // one-stream-per-part rule.)
            let (_rec, parts) = read_output(&store).await;
            assert_eq!(parts.len(), 1, "corpus is well under the part cap");
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
                 (one decoded block plus the raw row groups, x input count)"
            );
            // The only stream-dependent residency is the writer's in-progress
            // part, bounded by the part cap (content-addressing needs the whole
            // part before its key exists).
            assert!(
                total < TRANSIENT_BOUND + config.l1_part_memory_target_bytes,
                "total peak {total} exceeded transient bound + memory split target"
            );
            peaks.push(transient);
        }

        // The stream grew 10x; the decode-side peak did not (the same one
        // decoded block per input stays resident regardless of stream size).
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

    /// Byte-identical-output test: the new k-way streaming merge
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

        /// The correctness core (ADR-0032): for a random corpus of
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
        /// the merge to split across multiple parts, so the "concatenate all
        /// parts" side of the differential actually crosses part boundaries
        /// (the large-cap test above never leaves one part). 512 bytes is
        /// below a single record's estimate, so every corpus splits -- since
        /// issue #711 that includes a single-stream corpus, which splits
        /// mid-stream rather than staying in one part.
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
