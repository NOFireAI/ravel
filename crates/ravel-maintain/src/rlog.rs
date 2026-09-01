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
//! [`RlogRangeReader::block_rows_in_group`]), fetching the next range only
//! once the previous one is drained (one ahead: the fetch for range `n + 1`
//! rides along with range `n`'s, so the cursor advances two ranges per round
//! trip while its raw residency stays at two). The fetched range is one block
//! under version 3 and one row group under version 4, whose blocks' pages are
//! spread across its column chunks (ADR-0699 decision 1); the group's raw
//! bytes are held while its blocks are decoded out of them one at a time, so
//! the decoded term is a block in both versions (issue #748).
//!
//! That decoded block is held in its COLUMNAR form, as a
//! [`StreamBlockRows`], and its records are materialized one at a time as the
//! merge consumes them (ADR-0979 decision 1). A block's row form re-owns every
//! attribute key and every stream blob per record, where the columnar form
//! stores each once per column, so holding the block as records cost several
//! times what holding it as columns costs -- per cursor, and the number of
//! simultaneously open cursors is the number of inputs carrying the stream.
//! The merge's ordering key is read straight out of the decoded timestamp
//! column ([`StreamBlockRows::peek_ts`]), so choosing the next record
//! materializes nothing; exactly one record is materialized per record
//! emitted, through the same `rebuild_record` path the eager decode used, in
//! the same order. Total decode and rebuild work is unchanged; only when a row
//! is rebuilt moves. A record's `(stream_ref, ts)`
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
//! unchanged by the k-way merge), plus at most one decoded columnar block per
//! input carrying the current stream (`O(input_count * block_size)`,
//! independent of stream size) and the raw bytes of the range that block came
//! from (`O(input_count * group_size)` under version 4, stored bytes rather
//! than decoded records), plus the in-progress part's writer buffer. The writer
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
//! independent knob, the **stored-size target** `max_l1_part_bytes`, and it is
//! measured in the bytes its name promises: the part's ACTUAL encoded object
//! size (issue #872). The RLOG writer holds row-major records and only encodes
//! at [`RlogWriter::finish_compacted`], so there is no incremental encoded size
//! to read; instead [`PartBuilder`] keeps a cheap pre-compression payload proxy
//! ([`estimate_stored_record`] per record plus [`estimate_stored_stream`] once
//! per distinct stream) that SCHEDULES an exact-encode probe once it reaches the
//! target, and the part closes on the probe's real byte count. Closing on the
//! proxy directly would have sized every object at the target divided by the
//! compression ratio (the proxy is an upper bound over the zstd-compressed
//! sections), the same ratio-times-smaller objects issue #872 names. A part
//! closes on whichever target is reached first. With the shipped defaults (both
//! 256 MiB) the memory split target still fires first on every real schema and
//! the payload proxy never reaches 256 MiB, so no probe runs and this split is
//! behaviour-neutral until an operator lowers the stored target below the size
//! the memory split target already yields. Lowering it caps objects lower;
//! growing them means raising `l1_part_memory_target_bytes`.
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
use ravel_logseg::page_dir::PageDir;
use ravel_logseg::postings::PostingsSection;
use ravel_logseg::reader::MAX_BLOCKS;
use ravel_logseg::record::{
    COL_ATTRS_RAW, COL_BODY, COL_SEVERITY_TEXT, COL_SPAN_ID, COL_TRACE_ID, FIRST_DYNAMIC_COL,
};
use ravel_logseg::skip_index::SkipIndex;
use ravel_logseg::{
    AttrValue, FieldType, LogRecord, LogStreamId, RlogConfig, RlogRangeReader, RlogWriter,
    StreamBlockLoc, StreamBlockRows, decode_section,
    writer::{ObjectIdentity, WriteStats},
};
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_proto::commit::v1::CompactionPart;
use ravel_types::declared_stats::{DeclaredColumnStat, DeclaredStatType, DeclaredStatValue};

use crate::bucket::Bucket;
use crate::build::{BuiltPart, put_part};
use crate::codec::SegmentCodec;
use crate::config::{AdmissionMode, CompactorConfig, MergeMemoryTracker};
use crate::error::{MaintainError, MergeCursorBudgetSite, Result};
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
    /// Everything the bounded merge needs to price a cursor's decoded residency
    /// from this input before any BLOCKS byte is fetched (ADR-0979 decision 4).
    /// Never empty on a loaded catalog: an input with no PAGE_DIR is refused at
    /// load with [`MaintainError::MergeCursorInputMissingPageDir`].
    pub pricing: InputCursorPricing,
}

/// The pre-decode admission pricing metadata of one input: the per-block shape
/// and string-payload figures [`block_decode_ceiling_bytes`] evaluates the
/// ADR-0979 decision 1 decoded-block ceiling from, all read from directories the
/// catalog already holds resident (SKIP_IDX, FIELD_DIR, PAGE_DIR).
///
/// The decoded block's dominant term scales with the block's SHAPE (rows times
/// the column-id width), not with its stored or encoded size: `encode_i64` picks
/// the smallest codec per page, so a constant or run-length column -- every
/// one-stream block's `stream_ref`, usually `severity` and `flags` too -- stores
/// a handful of bytes and decodes to `16 B x rows`. Only the string payload
/// scales with page contents, and PAGE_DIR carries it per page. Pricing a
/// cursor from raw page `uncomp_len` alone therefore under-charges by a ratio
/// that can exceed 10,000x, which is the under-charge the budget exists to
/// refuse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputCursorPricing {
    /// Rows in each whole-object block, from the resident SKIP_IDX level-0
    /// entries' `record_count`. A candidate block index from
    /// [`RlogRangeReader::stream_blocks`] indexes straight into it.
    pub block_rows: Vec<u32>,
    /// Per whole-object block index, the sum of PAGE_DIR `uncomp_len` over that
    /// block's STRING pages only: the decompressed payload the block's string
    /// and byte-valued cells own after decode. The numeric pages' `uncomp_len`
    /// is deliberately NOT in here; their decoded cost is the shape term.
    pub block_string_uncomp_lens: Vec<u64>,
    /// The width of this input's column-id space: the largest column id plus
    /// one, over the reserved fixed ids and every dynamic id FIELD_DIR names. A
    /// width rather than a count, because a decoded block's per-column slot
    /// vectors are sized by the largest id it carries.
    pub total_cols: u64,
    /// How many of those columns decode to owned per-cell buffers (the fixed
    /// string and byte columns plus every FIELD_DIR `Str`/`Bytes` entry). Every
    /// one is priced as PLAIN, the conservative arm: dictionary encoding stores
    /// the distinct values once and only shrinks the figure.
    pub string_cols: u64,
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
        // The declared eligible columns to recompute per-part stamps for, learned
        // from the inputs' own stamps (ADR-0873 decision 3). Recomputed over
        // output rows in the merge, never copied from an input.
        let declared = declared_columns_from_inputs(inputs);
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
            declared,
            config.dry_run,
            // Compaction releases each part's bytes at PUT (ADR-0979 decision 3):
            // the retained-parts memory term goes to zero, and an `AlreadyExists`
            // part is HEAD-verified after the record PUT instead of repaired from
            // RAM.
            false,
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

    // PAGE_DIR is present exactly on a version-4 input (ADR-0699 decision 2),
    // and the bounded merge cannot admission-price a cursor over an input
    // without it (its decoded string-page term is unknowable before the fetch,
    // ADR-0979 decision 4), so refuse it by object key and format version
    // rather than read it under a guessed cost. PAGE_DIR is mandatory in v4, so
    // in practice this is a version/corruption gate, not a live path.
    let Some(_) = ftr.section(kind::PAGE_DIR) else {
        return Err(MaintainError::MergeCursorInputMissingPageDir {
            object_key,
            format_version: OUTPUT_FORMAT_VERSION,
        });
    };
    let page_dir_raw = fetch_section(store, &object_key, &ftr, kind::PAGE_DIR, &cfg).await?;
    // The per-block shape and string-payload figures the bounded merge reserves
    // a cursor's decode against (ADR-0979 decision 4). Decoded here, once, from
    // directory bytes already fetched: these are the same decodes the range
    // reader runs internally, and this keeps only the per-block figures (one u32
    // and one u64 per block) rather than a second copy of the directories. The
    // reader's own `from_sections_with_page_dir` below re-decodes and fully
    // validates the same bytes, so a corrupt directory still fails loud there.
    let pricing = input_cursor_pricing(&field_dir_raw, &skip_idx_raw, &page_dir_raw)?;
    // Input read / catalog load phase (issue #977): the reader below retains a
    // decoded form of these directory sections for the whole merge. Charge the
    // decoded section payload lengths (what `fetch_section` returns after
    // decompression, the retained form); the block/bloom bytes are NOT held
    // here (the merge streams them by range), so they are not charged.
    if let Some(t) = config.merge_memory_tracker.as_ref() {
        let dir_bytes = stream_dir_raw.len() as u64
            + field_dir_raw.len() as u64
            + skip_idx_raw.len() as u64
            + page_dir_raw.len() as u64
            // The retained per-block reservation figures are derived from the
            // directories but held past them, so charge them too: one u32 (rows)
            // and one u64 (string payload) per block.
            + (pricing.block_rows.len() as u64)
                .saturating_mul(std::mem::size_of::<u32>() as u64)
            + (pricing.block_string_uncomp_lens.len() as u64)
                .saturating_mul(std::mem::size_of::<u64>() as u64);
        t.add_catalog_directory_bytes(dir_bytes);
    }
    let record_count = ftr.record_count;
    let reader = RlogRangeReader::from_sections_with_page_dir(
        &ftr,
        &stream_dir_raw,
        &field_dir_raw,
        &skip_idx_raw,
        Some(&page_dir_raw),
    )?;
    Ok(RlogInputCatalog {
        object_key,
        reader,
        indexed_fields,
        record_count,
        pricing,
    })
}

/// The fixed column ids whose cells decode to owned per-row byte buffers:
/// `severity_text`, `body`, `trace_id`, `span_id`, and the canonical
/// `attrs_raw` overflow blob (docs/log-segment-format.md FIELD_DIR). The
/// remaining reserved ids (`ts`, `observed_ts`, `stream_ref`, `severity_num`,
/// `flags`) decode to `Option<i64>` slots and are priced by the shape term.
const FIXED_STRING_COLS: [u32; 5] = [
    COL_SEVERITY_TEXT,
    COL_BODY,
    COL_TRACE_ID,
    COL_SPAN_ID,
    COL_ATTRS_RAW,
];

/// Decoded bytes a numeric cell slot costs: `Option<i64>` and `Option<u64>` are
/// 16 bytes, and every column of the decoded block carries one slot per row of
/// the block whether or not that row has a value (`Option<bool>` is smaller,
/// so pricing every column at this width is the conservative arm).
const DECODED_CELL_BYTES: u64 = 16;

/// Decoded bytes a string cell's SLOT costs on top of the shape term, covering
/// the `Option<Vec<u8>>` handle a plain string column keeps per row (24 bytes)
/// with margin for the dictionary form's id plus its distinct-value handle.
/// The cell CONTENTS are priced separately, from the block's string-page
/// `uncomp_len`.
const DECODED_STRING_SLOT_BYTES: u64 = 40;

/// Decoded bytes one column id costs in ONE of the decoder's per-kind slot
/// vectors. Four of the five slots are an `Option<Vec<..>>`, 24 bytes by the
/// null-pointer niche; the string kind's slot is a two-variant enum whose wider
/// arm holds two vectors (a dictionary's distinct values and its per-row ids),
/// so 48 bytes covers every kind.
///
/// ADR-0979's amended ceiling states this term as `2 x SUM(slot sizes) x
/// width` -- 288 B per width unit at the current sizes (24/24/24/48/24). The
/// 48-B-per-kind constant here covers the widest slot uniformly, so 48 x 5 x
/// the growth factor = 480 B per width unit sits above that floor at every
/// width: a valid, deliberately conservative instantiation. A flat 24 B per
/// kind at exact width would not bound the string kind or the growth step:
/// on the eight-one-record-block fixture of
/// `the_slot_spine_term_is_what_bounds_a_small_block` it prices a block at
/// 2,154 B against the 2,232 B it decodes to.
const DECODED_SLOT_SPINE_BYTES: u64 = 48;

/// How many such per-kind slot vectors a decoded block holds: i64, f64, bool,
/// string, and fixed-width (`ravel_logseg::block::DecodedBlock`).
const DECODER_SLOT_KINDS: u64 = 5;

/// A slot vector is grown to reach the id being inserted, so its capacity is a
/// `Vec` growth step at or above the id-space width, never the width exactly.
/// `Vec` reserves `max(2 x capacity, needed)`, so twice the width bounds it
/// (the id space is at least `FIRST_DYNAMIC_COL` wide, above the minimum
/// non-zero capacity a `Vec` of these element sizes starts at).
const DECODED_SLOT_SPINE_CAPACITY_FACTOR: u64 = 2;

/// One input's pre-decode admission pricing metadata (ADR-0979 decision 4),
/// decoded from the FIELD_DIR, SKIP_IDX, and PAGE_DIR section bytes the catalog
/// load already fetched. See [`InputCursorPricing`] for why each term is read
/// from the source it is read from.
fn input_cursor_pricing(
    field_dir_raw: &[u8],
    skip_idx_raw: &[u8],
    page_dir_raw: &[u8],
) -> Result<InputCursorPricing> {
    let field_dir = FieldDir::decode(field_dir_raw, MAX_FIELD_DIR_ENTRIES)?;
    // The reserved fixed ids 0..FIRST_DYNAMIC_COL never appear in FIELD_DIR but
    // are always part of the block's column-id space, so the width starts there.
    let mut total_cols = u64::from(FIRST_DYNAMIC_COL);
    let mut string_ids: BTreeSet<u32> = FIXED_STRING_COLS.iter().copied().collect();
    for entry in field_dir.entries() {
        total_cols = total_cols.max(u64::from(entry.column_id).saturating_add(1));
        if matches!(entry.ty, FieldType::Str | FieldType::Bytes) {
            string_ids.insert(entry.column_id);
        }
    }
    let string_cols = string_ids.len() as u64;

    let skip = SkipIndex::decode(skip_idx_raw, MAX_BLOCKS)?;
    let block_rows: Vec<u32> = skip.l0.iter().map(|e| e.record_count).collect();

    let page_dir = PageDir::decode(page_dir_raw)?;
    let block_count = page_dir.block_count();
    let mut block_string_uncomp_lens = Vec::with_capacity(block_count as usize);
    for b in 0..block_count {
        let index = u32::try_from(b)
            .map_err(|_| MaintainError::Invariant("page_dir block index range".to_string()))?;
        let pages = page_dir.block_pages(index).ok_or_else(|| {
            MaintainError::Invariant("page_dir block absent from its own directory".to_string())
        })?;
        let mut sum = 0u64;
        for p in pages {
            if string_ids.contains(&p.desc.column_id) {
                sum = sum.saturating_add(p.desc.uncomp_len);
            }
        }
        block_string_uncomp_lens.push(sum);
    }
    if block_rows.len() != block_string_uncomp_lens.len() {
        return Err(MaintainError::Invariant(format!(
            "input block framing disagrees: SKIP_IDX frames {} blocks, PAGE_DIR covers {}",
            block_rows.len(),
            block_string_uncomp_lens.len()
        )));
    }
    Ok(InputCursorPricing {
        block_rows,
        block_string_uncomp_lens,
        total_cols,
        string_cols,
    })
}

/// The ADR-0979 decision 1 ceiling on what ONE block of `pricing`'s input
/// decodes to, evaluated from pre-decode metadata alone:
///
/// ```text
/// 16 B x rows x total_cols          every column's per-row cell slot
///   + string-page uncomp_len        the string cells' own buffers
///   + 40 B x rows x string_cols     those cells' per-row handles
///   + 48 B x total_cols x 5 x 2     the decoder's five per-kind slot vectors
/// ```
///
/// Each term is priced from the source that actually bounds it: `rows` and the
/// block framing from SKIP_IDX, the column-id width and the string-column count
/// from FIELD_DIR, the string payload from PAGE_DIR's per-page `uncomp_len`
/// restricted to string pages. Every string column is priced as plain, the
/// conservative arm.
///
/// The last term does not scale with rows: the decoder's slot vectors are
/// indexed by column id, so they cost the id-space width whatever the block
/// holds. On a block of a few rows they exceed the per-row terms, so a ceiling
/// that omits them is not a ceiling there.
fn block_decode_ceiling_bytes(pricing: &InputCursorPricing, block: usize) -> Result<u64> {
    let rows = u64::from(*pricing.block_rows.get(block).ok_or_else(|| {
        MaintainError::Invariant(
            "stream candidate block index past the input's SKIP_IDX block count".to_string(),
        )
    })?);
    let string_bytes = *pricing.block_string_uncomp_lens.get(block).ok_or_else(|| {
        MaintainError::Invariant(
            "stream candidate block index past the input's PAGE_DIR block count".to_string(),
        )
    })?;
    let cells = DECODED_CELL_BYTES
        .saturating_mul(rows)
        .saturating_mul(pricing.total_cols);
    let string_slots = DECODED_STRING_SLOT_BYTES
        .saturating_mul(rows)
        .saturating_mul(pricing.string_cols);
    let slot_spines = DECODED_SLOT_SPINE_BYTES
        .saturating_mul(pricing.total_cols)
        .saturating_mul(DECODER_SLOT_KINDS)
        .saturating_mul(DECODED_SLOT_SPINE_CAPACITY_FACTOR);
    Ok(cells
        .saturating_add(string_bytes)
        .saturating_add(string_slots)
        .saturating_add(slot_spines))
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
/// `declared` is the eligible declared columns each output part recomputes
/// min/max/null-count stamps for (ADR-0873 decision 3): the compactor passes
/// the set recovered from its inputs, while the erasure rewrite passes an empty
/// set so its parts stay unstamped (the wave-3 staleness rule). `dry_run`
/// decides whether each part is PUT as it closes: compaction PUTs
/// here, while the erasure rewrite defers every PUT to its own publish path,
/// which writes parts only after its conservation gate passes.
///
/// `retain_bytes` decides whether each closed part keeps its encoded bytes
/// resident until publish (ADR-0979 decision 3). Compaction passes `false` and
/// releases them at PUT (the retained-parts memory term goes to zero); the
/// erasure rewrite passes `true` because its deferred publish path is what PUTs
/// them. When compaction's part PUT answers `AlreadyExists`, the returned
/// [`BuiltPart::put_already_existed`] flags it for the caller's post-publish
/// HEAD verification, since a dropped-bytes part cannot be repaired from RAM.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn merge_catalogs(
    store: &dyn ObjectStoreBackend,
    config: &CompactorConfig,
    bucket: &Bucket,
    catalogs: &[RlogInputCatalog],
    input_set_hash: &[u8; 32],
    indexed_fields: Vec<String>,
    declared: Vec<(String, DeclaredStatType)>,
    dry_run: bool,
    retain_bytes: bool,
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
        declared,
        tracker,
        dry_run,
        retain_bytes,
        current: None,
        parts: Vec::new(),
        part_index: 0,
    };
    let mut counts = RecordCounts::default();

    // Merge stream by stream in sorted stream_id order. Each stream is
    // k-way merged from every input carrying it (ts-ascending, canonical
    // input-order tie-break) straight into the current part's writer, and
    // the part flushes the moment its accumulated record-heap estimate
    // reaches `l1_part_memory_target_bytes` (the memory split target) or an
    // exact-encode probe shows its object bytes reaching `max_l1_part_bytes`
    // (the stored target), whichever comes first -- mid-stream if that is where
    // the target falls (issue #711 for the memory split target, issue #872 for
    // the stored target, ADR-0032 amendment 2026-08-26). No intermediate
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
/// in-progress part as soon as EITHER target is reached (issue #872), whichever
/// falls first in the record sequence:
///
/// - the **memory split target** `l1_part_memory_target_bytes`, reached when the
///   part's [`PartBuilder::estimate`] (decoded record heap) does. This is
///   checked after every record, so the writer buffer exceeds it by at most one
///   record; it is the whole of issue #711 (the check used to run only between
///   streams, so a bucket whose records all belong to one stream held its entire
///   row set live in one writer).
/// - the **stored-size target** `max_l1_part_bytes`, reached when the part's
///   ACTUAL encoded object bytes do. The RLOG writer holds row-major records and
///   only encodes at [`RlogWriter::finish_compacted`], so there is no incremental
///   encoded size to read; instead [`PartBuilder::stored_estimate`] (a cheap
///   pre-compression payload proxy) schedules an exact-encode probe
///   ([`PartBuilder::encode_clone`]), and the part closes on the probe's real
///   byte count, not on the proxy. The proxy only decides WHEN to probe, never
///   whether to close, so the compression ratio between payload and object no
///   longer sizes the part: on a wide, compressible schema a proxy-driven close
///   produced objects a ratio-times smaller than the target
///   (`estimate_stored_record` is documented as an upper bound over the
///   zstd-compressed sections), which is the bug issue #872 names. The probe is
///   gated on the proxy first reaching the target, so at the shipped defaults
///   (both 256 MiB) it never fires: the memory target closes every part on a
///   real schema long before the payload proxy reaches 256 MiB, so this crate's
///   geometry is unchanged until an operator lowers the stored target.
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
    /// The declared eligible columns each output part recomputes stamps for
    /// (ADR-0873 decision 3). Empty on the erasure-rewrite route, whose parts
    /// stay unstamped (the wave-3 staleness rule), and on metrics/spans buckets.
    declared: Vec<(String, DeclaredStatType)>,
    tracker: Option<&'a MergeMemoryTracker>,
    /// Whether a closed part is encoded but not PUT. Not read from `config`:
    /// the erasure rewrite defers every part PUT to its own publish path (which
    /// writes them only after its conservation gate passes) while running with
    /// a config whose `dry_run` is false.
    dry_run: bool,
    /// Whether each closed part keeps its encoded bytes resident until publish
    /// (ADR-0979 decision 3). Compaction releases them at PUT (`false`), so the
    /// retained-parts memory term is zero; the erasure rewrite keeps them
    /// (`true`) because its own publish path PUTs them after its conservation
    /// gate.
    retain_bytes: bool,
    current: Option<PartBuilder>,
    parts: Vec<BuiltPart>,
    part_index: u32,
}

impl PartSink<'_> {
    /// Push one merged record, opening a part if none is in progress and
    /// closing it once a target is reached.
    async fn push(&mut self, r: LogRecord) -> Result<()> {
        if self.current.is_none() {
            self.current = Some(PartBuilder::new(
                &self.identity,
                &self.indexed_fields,
                &self.declared,
            ));
        }
        let mut over_memory = false;
        // Encoded object bytes from a stored-target probe that showed the part
        // reached the target, carried to `flush` so the closing object is
        // encoded once, not re-encoded (the probe already produced the exact
        // bytes for this `part_index` and `input_set_hash`).
        let mut stored_close: Option<(Vec<u8>, WriteStats)> = None;
        if let Some(part) = self.current.as_mut() {
            part.push(r, self.tracker)?;
            // The memory split target sizes compactor peak memory (issue #711)
            // and is checked cheaply after every record, so it takes priority
            // when both would fire.
            over_memory = part.estimate >= self.config.l1_part_memory_target_bytes;
            if !over_memory
                && part.stored_estimate >= self.config.max_l1_part_bytes
                && part.stored_estimate >= part.next_probe_stored
            {
                // The stored target governs object geometry, so it closes on the
                // object's ACTUAL encoded size, not on the payload proxy (issue
                // #872). The proxy only schedules this probe; encoding is what
                // measures the bytes the knob is named for. `encode_clone`
                // encodes a clone of the buffered records, so the builder can
                // keep accumulating when the part is not yet full.
                if let Some(t) = self.tracker {
                    // The clone is a second live copy of the part's record heap,
                    // so it belongs in the run's peak rather than being treated
                    // as free; the object bytes join it below, once their size
                    // is known.
                    t.set_probe_bytes(part.estimate);
                }
                let probe = part.encode_clone(self.input_set_hash, self.part_index);
                if let Some(t) = self.tracker {
                    if let Ok((object, _)) = probe.as_ref() {
                        t.set_probe_bytes(part.estimate.saturating_add(object.len() as u64));
                    }
                    // Cleared on the error path too, so a failed encode does not
                    // leave the term charged for the rest of the run.
                    t.set_probe_bytes(0);
                    // Counted whether or not it closes the part: this is the
                    // O(part) encode the stored target costs, and "no probe runs
                    // at the shipped defaults" is a claim a test asserts on.
                    t.note_probe_run();
                }
                let (object, stats) = probe?;
                let encoded = object.len() as u64;
                if encoded >= self.config.max_l1_part_bytes {
                    stored_close = Some((object, stats));
                } else {
                    part.schedule_next_probe(encoded, self.config.max_l1_part_bytes);
                }
            }
        }
        if over_memory {
            if let Some(t) = self.tracker {
                t.note_memory_target_flush();
            }
            self.flush(None).await?;
        } else if let Some(enc) = stored_close {
            if let Some(t) = self.tracker {
                t.note_stored_target_flush();
            }
            self.flush(Some(enc)).await?;
        }
        Ok(())
    }

    /// Close the in-progress part, if any, and PUT it. A part is never empty
    /// here: [`Self::push`] only ever calls this after pushing a record, and
    /// [`Self::finish`] guards on `is_empty`.
    ///
    /// `pre_encoded` reuses the bytes a stored-target probe already produced for
    /// this part; `None` (a memory-target close or the trailing part) encodes
    /// the part here, consuming its records so no clone is made on the common
    /// path.
    async fn flush(&mut self, pre_encoded: Option<(Vec<u8>, WriteStats)>) -> Result<()> {
        // The `if let` (rather than an `unwrap`/`expect`) keeps the critical
        // path free of a panic path; a `None` here is a no-op, not a lie.
        if let Some(mut builder) = self.current.take() {
            // Copy the Copy-typed stream bounds before the encode consumes the
            // builder.
            let first_stream_id = builder.min_stream;
            let last_stream_id = builder.max_stream;
            // Take the part's declared-column fold out before the encode consumes
            // the builder; it carries exactly the records this part holds
            // (ADR-0873 decision 3), stamped against the closed part's own row
            // count in `finalize_part`.
            let declared_accum = std::mem::take(&mut builder.declared_accum);
            let (object, stats) = match pre_encoded {
                Some(enc) => enc,
                None => builder.into_encoded(self.input_set_hash, self.part_index)?,
            };
            let built = finalize_part(
                self.store,
                self.bucket,
                object,
                stats,
                first_stream_id,
                last_stream_id,
                self.input_set_hash,
                self.part_index,
                self.dry_run,
                self.retain_bytes,
                &declared_accum,
            )
            .await?;
            // Only bytes actually retained past PUT count toward the
            // retained-parts term. On the compaction path (`retain_bytes =
            // false`) `finalize_part` released them at PUT, so this is 0 and the
            // term stays flat at zero (ADR-0979 decision 3); the erasure rewrite
            // (`retain_bytes = true`) still charges each closed part's encoded
            // size, unchanged.
            let retained = built.bytes.as_ref().map_or(0, |b| b.len() as u64);
            self.parts.push(built);
            self.part_index += 1;
            if let Some(t) = self.tracker {
                // Charge whatever encoded bytes remain resident in `self.parts`
                // until publish, and clear the writer term (its decoded-heap
                // records were handed to the writer and released on encode).
                t.add_retained_part_bytes(retained);
                t.set_writer_bytes(0);
            }
        }
        Ok(())
    }

    /// Close the trailing part and yield every part built, in part-index order.
    async fn finish(mut self) -> Result<Vec<BuiltPart>> {
        if self.current.as_ref().is_some_and(|p| !p.is_empty()) {
            self.flush(None).await?;
        }
        Ok(self.parts)
    }
}

/// The declared eligible columns to recompute stamps for on a compaction's
/// output (ADR-0873 decision 3, L1 half), recovered from the input commit
/// records.
///
/// The compactor recomputes extrema over the rows it writes and never copies an
/// input's stamp -- inputs may predate a declaration, and a copied value could
/// name a row a later input does not carry -- but it learns WHICH columns are
/// declared from the stamps its inputs already carry (wave 5a stamped every L0
/// flush). The result is the union over inputs, deduplicated by name (first
/// eligible occurrence in canonical input order wins), read through the wave-2
/// predicate so only a trustworthy declaration seeds the fold.
///
/// A column declared after every input was written carries no stamp on any
/// input and is therefore not recomputed here; it converges once a stamped
/// flush enters a later compaction (the reachability follow-up on #1022). A
/// metrics or spans bucket carries no declared columns and yields an empty set.
fn declared_columns_from_inputs(inputs: &[InputRecord]) -> Vec<(String, DeclaredStatType)> {
    let mut seen: BTreeMap<String, DeclaredStatType> = BTreeMap::new();
    for input in inputs {
        for stat in ravel_commit::declared_stats::read_commit_record(&input.record).covered() {
            seen.entry(stat.name().to_string())
                .or_insert_with(|| stat.declared_type());
        }
    }
    seen.into_iter().collect()
}

/// Running min/max and non-null row count for one declared column over the
/// records pushed to the part. `min <= max` holds by construction (both start at
/// the first value and only widen), so a stamp built from it never trips the
/// reader's `min <= max` clause.
#[derive(Clone, Copy)]
struct Running<T> {
    min: T,
    max: T,
    non_null: u64,
}

impl<T: Ord + Copy> Running<T> {
    fn start(v: T) -> Self {
        Running {
            min: v,
            max: v,
            non_null: 1,
        }
    }

    fn observe(&mut self, v: T) {
        if v < self.min {
            self.min = v;
        }
        if v > self.max {
            self.max = v;
        }
        self.non_null += 1;
    }
}

/// One declared column's running extrema over the records pushed to a part. Only
/// the [`Running`] matching `ty` is ever populated: a declared I64 column counts
/// an I64 value as non-null and reads a same-named BOOL value (or an absent one)
/// as NULL, and vice versa -- the wave-5a `(name, value kind)` rule.
struct ColumnAccum {
    name: String,
    ty: DeclaredStatType,
    i64_run: Option<Running<i64>>,
    bool_run: Option<Running<bool>>,
    /// First-occurrence-wins within the record currently being folded: set once
    /// this column has taken a value from that record, cleared before the next.
    seen_this_record: bool,
}

/// The per-part fold of declared-column extrema for the compaction output
/// (ADR-0873 decision 3, the L1 half of wave 5). It mirrors the ingest-side
/// wave-5a accumulator (`ravel_ingest`'s `DeclaredStatAccum`): eligible I64/BOOL
/// declared columns only, first-occurrence-wins per record, and a stamp whose
/// `null_count` is derived as `sample_count - non_null` so the part's own reader
/// ([`ravel_commit::declared_stats::read_compaction_part`]) never drops it.
///
/// Unlike the ingest side it is seeded with the declared column set up front
/// (from [`declared_columns_from_inputs`]), so it tracks only the columns it
/// will stamp rather than every eligible attribute the merge sees. It rides
/// [`PartBuilder::push`], the same per-record path `estimate` and the stream
/// bounds ride, so the exact-encode probe ([`PartBuilder::encode_clone`], which
/// clones the buffered records without re-pushing) folds no record twice. Each
/// part gets its own accumulator, opened when the part opens and consumed when
/// it closes, so a record folds into exactly the part it is written into: at a
/// mid-stream split the record that crosses the boundary is the one that opens
/// the next [`PartBuilder`] and is folded only there.
#[derive(Default)]
struct DeclaredStatAccum {
    cols: Vec<ColumnAccum>,
}

impl DeclaredStatAccum {
    /// Build a fold over the declared eligible columns. The set already passed
    /// [`DeclaredStatType::from_tag`] in [`declared_columns_from_inputs`], so
    /// every column here is I64 or BOOL.
    fn new(declared: &[(String, DeclaredStatType)]) -> Self {
        let cols = declared
            .iter()
            .map(|(name, ty)| ColumnAccum {
                name: name.clone(),
                ty: *ty,
                i64_run: None,
                bool_run: None,
                seen_this_record: false,
            })
            .collect();
        DeclaredStatAccum { cols }
    }

    /// Fold one record's attributes. Each declared column takes at most one
    /// non-null row from the record: the first attribute matching its name AND
    /// its declared value kind wins, and a repeat (or a same-named value of the
    /// other kind, or an absence) is a NULL for the declaration, so no column's
    /// non-null count can exceed the part's row count and the derived
    /// `null_count` can never go negative.
    fn observe_record(&mut self, attrs: &[(String, AttrValue)]) {
        if self.cols.is_empty() {
            return;
        }
        for c in &mut self.cols {
            c.seen_this_record = false;
        }
        for (name, value) in attrs {
            for c in &mut self.cols {
                if c.seen_this_record || c.name != *name {
                    continue;
                }
                match (c.ty, value) {
                    (DeclaredStatType::I64, AttrValue::I64(v)) => {
                        match &mut c.i64_run {
                            Some(r) => r.observe(*v),
                            None => c.i64_run = Some(Running::start(*v)),
                        }
                        c.seen_this_record = true;
                    }
                    (DeclaredStatType::Bool, AttrValue::Bool(b)) => {
                        match &mut c.bool_run {
                            Some(r) => r.observe(*b),
                            None => c.bool_run = Some(Running::start(*b)),
                        }
                        c.seen_this_record = true;
                    }
                    _ => {}
                }
            }
        }
    }

    /// Build the part's stamps against its own `sample_count` (the part footer's
    /// `record_count`). A declared column the part never saw a matching-typed
    /// value for stamps absent extrema with `null_count == sample_count`; every
    /// returned stat is valid by construction (`min <= max` from [`Running`],
    /// `null_count = sample_count - non_null`), so `read_compaction_part` drops
    /// none of them.
    fn build_stamps(&self, sample_count: u64) -> Vec<DeclaredColumnStat> {
        let mut out = Vec::new();
        for c in &self.cols {
            let observed = match c.ty {
                DeclaredStatType::I64 => c.i64_run.map(|r| {
                    (
                        DeclaredStatValue::I64(r.min),
                        DeclaredStatValue::I64(r.max),
                        r.non_null,
                    )
                }),
                DeclaredStatType::Bool => c.bool_run.map(|r| {
                    (
                        DeclaredStatValue::Bool(r.min),
                        DeclaredStatValue::Bool(r.max),
                        r.non_null,
                    )
                }),
            };
            if let Some(stat) = build_one(&c.name, c.ty, observed, sample_count) {
                out.push(stat);
            }
        }
        out
    }
}

/// Build one declared-column stamp, or `None` when the accumulated non-null
/// count exceeds `sample_count` (impossible under the per-record dedup above,
/// but a `None` rather than a self-dropping stamp keeps decision 3's invariant
/// true even if that ever changes) or the typed constructor refuses the triple.
fn build_one(
    name: &str,
    ty: DeclaredStatType,
    observed: Option<(DeclaredStatValue, DeclaredStatValue, u64)>,
    sample_count: u64,
) -> Option<DeclaredColumnStat> {
    let (min, max, null_count) = match observed {
        Some((min, max, non_null)) => {
            let null_count = sample_count.checked_sub(non_null)?;
            (Some(min), Some(max), null_count)
        }
        None => (None, None, sample_count),
    };
    DeclaredColumnStat::new(name, ty, min, max, null_count).ok()
}

/// The floor on how far apart two stored-target probes may sit on the payload
/// proxy axis. A probe encodes the whole part (an O(part) cost), so the
/// scheduler in [`PartBuilder::schedule_next_probe`] aims the next one near the
/// target rather than running one per record; this floor keeps a deficit of a
/// few bytes from turning the last stretch before a close into an encode per
/// record.
///
/// It is also the width of the stored target's overshoot band. The scheduler
/// aims the next probe the remaining encoded deficit further along the proxy
/// axis, or this floor if the deficit is smaller, so as long as encoded bytes
/// grow at most 1:1 with the proxy over the closing interval the encoded size
/// can pass the target by at most this floor plus the crossing record's proxy
/// charge -- see [`PartBuilder::schedule_next_probe`] for the derivation and for
/// what the band is conditional on.
const PROBE_MIN_STEP_BYTES: u64 = 4096;

/// One in-progress L1 part: the merged records buffered in canonical order, the
/// decoded-heap estimate that drives the memory split target and the tracker's
/// writer term, and the payload proxy that schedules the stored-target probe
/// (see [`PartSink`]).
///
/// Holding one whole part's records before its content-addressed key exists is
/// unavoidable (the key hashes the whole object); the k-way merge keeps
/// everything *else* bounded. The records are buffered here rather than pushed
/// into one long-lived [`RlogWriter`] so the part can be encoded to measure its
/// real size ([`Self::encode_clone`]) without being consumed.
struct PartBuilder {
    /// The compaction identity every encode stamps into the footer. Combined
    /// with the caller's `input_set_hash` and `part_index` it makes each encode
    /// deterministic, so a probe's bytes are byte-identical to the close's.
    identity: ObjectIdentity,
    /// The POSTINGS field list every part is written with (ADR-0049 decision 6).
    indexed_fields: Vec<String>,
    /// The merged records buffered for this part, in canonical `(stream_id, ts)`
    /// order.
    records: Vec<LogRecord>,
    /// Sum of [`estimate_record`] over every pushed record: the **memory split
    /// target**'s trigger (compared against `l1_part_memory_target_bytes`) and
    /// the writer-buffer term the tracker records. Decoded Rust heap, not stored
    /// bytes.
    estimate: u64,
    /// Sum of [`estimate_stored_record`] over every pushed record, plus
    /// [`estimate_stored_stream`] once per distinct stream in this part: a cheap
    /// pre-compression payload proxy. It does NOT close the part (issue #872):
    /// it only schedules the exact-encode probe that measures the object's real
    /// bytes, the quantity `max_l1_part_bytes` is named for. See
    /// [`PartSink::push`] and [`Self::next_probe_stored`].
    stored_estimate: u64,
    /// The `stored_estimate` value at which the next stored-target probe runs.
    /// Starts at 0, so the first probe runs as soon as `stored_estimate` first
    /// reaches `max_l1_part_bytes`; [`Self::schedule_next_probe`] advances it
    /// past a probe that found the part still short of the target, so probing is
    /// a few encodes per part, not one per record.
    next_probe_stored: u64,
    /// The streams whose STREAM_DIR entry this part has already been charged for
    /// in `stored_estimate`. A set rather than a "did the stream change" check so
    /// the charge is once per distinct stream whatever order records arrive in;
    /// it holds one 16-byte id per stream in the part, negligible beside the
    /// records.
    charged_streams: BTreeSet<LogStreamId>,
    min_stream: Option<LogStreamId>,
    max_stream: Option<LogStreamId>,
    count: usize,
    /// The declared-column extrema fold for this part (ADR-0873 decision 3).
    /// Fresh per part and folded once per pushed record, so a record folds into
    /// exactly the part it is written into.
    declared_accum: DeclaredStatAccum,
}

impl PartBuilder {
    fn new(
        identity: &ObjectIdentity,
        indexed_fields: &[String],
        declared: &[(String, DeclaredStatType)],
    ) -> Self {
        PartBuilder {
            identity: *identity,
            indexed_fields: indexed_fields.to_vec(),
            records: Vec::new(),
            estimate: 0,
            stored_estimate: 0,
            next_probe_stored: 0,
            charged_streams: BTreeSet::new(),
            min_stream: None,
            max_stream: None,
            count: 0,
            declared_accum: DeclaredStatAccum::new(declared),
        }
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Push one merged record, updating the decoded-heap estimate (the memory
    /// split target's trigger and the tracker's writer term), the payload proxy
    /// that schedules the stored-target probe, and the part's inclusive
    /// stream-id bounds.
    ///
    /// A record whose stream this part has not seen before also charges the
    /// proxy for that stream's STREAM_DIR entry ([`estimate_stored_stream`]): the
    /// blob is written once per stream in the object, so it belongs in the proxy
    /// once per stream, not once per record and not never.
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
        // Fold the declared-column extrema on the same per-record path the
        // estimates ride (ADR-0873 decision 3). Done here, once per pushed
        // record, so the exact-encode probe -- which clones `records` without
        // re-pushing -- never double-counts a record.
        self.declared_accum.observe_record(&r.attrs);
        self.records.push(r);
        if let Some(t) = tracker {
            t.set_writer_bytes(self.estimate);
        }
        Ok(())
    }

    /// Build a fresh writer over `records`. `RlogConfig::default()` and the
    /// indexed-field list match the L0 write path, so an L0 write and this L1
    /// merge cannot drift on encoding.
    fn build_writer(
        identity: &ObjectIdentity,
        indexed_fields: &[String],
        records: Vec<LogRecord>,
    ) -> Result<RlogWriter> {
        let mut w = RlogWriter::new(RlogConfig::default(), *identity)
            .with_indexed_fields(indexed_fields.to_vec());
        for r in records {
            w.push(r)?;
        }
        Ok(w)
    }

    /// Encode this part WITHOUT consuming the builder, by cloning its records.
    /// Used by the stored-target probe to measure the object's real encoded
    /// size; when the probe decides to close, the caller reuses the returned
    /// bytes as the closing object (they are byte-identical to what
    /// [`Self::into_encoded`] would produce for the same `part_index`, the encode
    /// being deterministic).
    fn encode_clone(
        &self,
        input_set_hash: &[u8; 32],
        part_index: u32,
    ) -> Result<(Vec<u8>, WriteStats)> {
        let w = Self::build_writer(&self.identity, &self.indexed_fields, self.records.clone())?;
        Ok(w.finish_compacted_with_stats(1, input_set_hash.to_vec(), part_index)?)
    }

    /// Encode this part, consuming the builder (no clone). Used to close a part
    /// on the memory split target, or the trailing part.
    fn into_encoded(
        self,
        input_set_hash: &[u8; 32],
        part_index: u32,
    ) -> Result<(Vec<u8>, WriteStats)> {
        let PartBuilder {
            identity,
            indexed_fields,
            records,
            ..
        } = self;
        let w = Self::build_writer(&identity, &indexed_fields, records)?;
        Ok(w.finish_compacted_with_stats(1, input_set_hash.to_vec(), part_index)?)
    }

    /// Schedule the next stored-target probe after one found the part still
    /// short of the target, and bound how far past the target the part can then
    /// close.
    ///
    /// The step is the proxy distance to the next probe, and it is the remaining
    /// encoded deficit `target - encoded_now` spent along the PROXY axis, floored
    /// at [`PROBE_MIN_STEP_BYTES`] so a part whose deficit is a handful of bytes
    /// does not encode once per record.
    ///
    /// Spending an encoded deficit as a proxy distance is the whole bound, and
    /// the assumption it rests on is 1:1: over the interval that closes the
    /// part, encoded bytes grow by at most as much as the uncompressed payload
    /// proxy does. Under that assumption a probe that found `encoded_now <
    /// target` cannot be followed by a close more than `step` of encoded growth
    /// later, and the next probe fires at the first record on or after
    /// `stored_estimate + step`, so the proxy -- and with it the encoded size --
    /// advances by at most `step` plus that record's own charge. With
    /// `step = deficit` that lands the close at `target` plus one record's
    /// charge; with the floor binding (`deficit < PROBE_MIN_STEP_BYTES`) at
    /// `target + PROBE_MIN_STEP_BYTES` plus one record's charge. So the enforced
    /// band is
    /// `[target, target + PROBE_MIN_STEP_BYTES + one record's proxy charge]`,
    /// and the same arithmetic bounds a trailing part (its records simply run
    /// out before the next probe).
    ///
    /// The 1:1 assumption is the disclosed condition, not a proof: it holds for
    /// the sections the proxy models -- the payload columns and STREAM_DIR, where
    /// the proxy counts uncompressed bytes and the object stores compressed ones,
    /// so encoded growth per proxy byte is at most 1 and usually far below it --
    /// and not for the sections it does not model (POSTINGS, SKIP_IDX,
    /// PAGE_DIR), which can grow faster than the payload they index and carry a
    /// part past the band. It is also not the "one record" tightness the memory
    /// split target has.
    ///
    /// The cost is probes, and it is superlinear in compressibility rather than
    /// the two or three encodes a per-part rate model would spend. A payload
    /// that compresses `r`-fold turns a step of `deficit` proxy bytes into only
    /// `deficit / r` encoded bytes, so each probe closes a `1 / r` fraction of
    /// the deficit and the deficit decays GEOMETRICALLY by `(1 - 1 / r)` per
    /// probe. Starting from the deficit at the first probe,
    /// `d0 = target * (1 - 1 / r)` (the proxy reaches `target` when the object is
    /// near `target / r`), reaching the floor takes
    /// `ln(d0 / PROBE_MIN_STEP_BYTES) / ln(1 / (1 - 1 / r))` probes, which is
    /// about `r * ln(d0 / PROBE_MIN_STEP_BYTES)`, and the tail below the floor
    /// takes about `r` more (each floored step closes
    /// `PROBE_MIN_STEP_BYTES / r`). So probes per part are about
    /// `r * ln(d0 / PROBE_MIN_STEP_BYTES) + r`, and both stored-target geometry
    /// tests pin the exact count their fixture produces against that formula.
    /// The cost is only paid when an operator lowers `max_l1_part_bytes` below
    /// the memory split target; at the shipped defaults no probe runs at all.
    fn schedule_next_probe(&mut self, encoded_now: u64, target: u64) {
        let deficit = target.saturating_sub(encoded_now);
        let step = deficit.max(PROBE_MIN_STEP_BYTES);
        self.next_probe_stored = self.stored_estimate.saturating_add(step);
    }
}

/// One input's cursor over a single stream's records, yielding them in stored
/// (ts-ascending) order one block at a time. At most one decoded block is
/// resident, held in COLUMNAR form: `block` is the current block's
/// [`StreamBlockRows`] view, positioned at the next row to merge, and the next
/// block is decoded only once the current one is drained. `input_index` is the
/// cursor's canonical position in `catalogs`, the k-way merge's tie-break on
/// equal `ts_ns`.
///
/// The cursor holds NO materialized records (ADR-0979 decision 1). The merge
/// orders cursors by [`Self::peek_ts`], which reads the decoded timestamp
/// column directly, and materializes a record only when it is the one being
/// emitted ([`Self::next_record`]). Row form is several times the size of the
/// columnar form it is rebuilt from -- every record re-owns its attribute keys
/// and stream blob, which the columnar block stores once per column -- so a
/// cursor holding its block as records held the dominant term of a merge whose
/// open-cursor count is the number of inputs carrying the stream. Every row is
/// still rebuilt exactly once, through the same `rebuild_record` path and in
/// the same order, so the merged record sequence and every part byte are
/// unchanged.
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
    /// This input's pre-decode pricing metadata, the source of the per-block
    /// decode ceiling the cursor's charge grows to before each later block's
    /// decode ([`StreamCursor::refill`], ADR-0979 decision 4 as amended).
    pricing: &'a InputCursorPricing,
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
    /// The current decoded block, columnar, positioned at the next row of this
    /// stream it will yield. `None` before the first decode and once the cursor
    /// is exhausted; a `Some` that is exhausted is a drained block awaiting
    /// release in the next [`Self::refill`].
    block: Option<StreamBlockRows<'a>>,
    /// The current block's [`StreamBlockRows::heap_estimate`], held live in the
    /// tracker's decoded term until the block is released. Read once at decode
    /// so the release subtracts exactly what the charge added, even though the
    /// view's own estimate does not change as rows are drained out of it.
    block_bytes: u64,
    /// The next loc's raw bytes, already fetched (issue #711). At most one:
    /// the prefetch is one loc ahead, so the cursor's raw-byte residency is
    /// two locs, not the stream.
    prefetched: Option<(StreamBlockLoc, bytes::Bytes)>,
    /// The pre-decode reservation ceiling this cursor was admitted under
    /// (ADR-0979 decision 4). Kept after the reconcile as the ceiling the
    /// cursor's residency is bounded by; the live charge is [`Self::charged`].
    /// Set by the merge after [`Self::open`]; 0 until then.
    reservation: u64,
    /// What this cursor currently charges against
    /// [`CompactorConfig::merge_cursor_budget_bytes`]: the pre-decode
    /// reservation ceiling from admission until its first decode completes, then
    /// its actual residency ([`Self::resident_bytes`]), re-derived after every
    /// block decode (ADR-0979 decision 4 as amended). Released back to the
    /// stream's running charge when the cursor drains, so the charge tracks the
    /// concurrent overlap `D`, not the input count.
    charged: u64,
}

impl<'a> StreamCursor<'a> {
    /// Open a cursor over `stream_id` in `catalog`, or `None` if the input does
    /// not carry the stream. Does not fetch yet; call [`Self::refill`] once to
    /// load the first block.
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
            pricing: &catalog.pricing,
            locs,
            next_loc: 0,
            group: None,
            group_raw_bytes: 0,
            decoded_in_group: 0,
            block: None,
            block_bytes: 0,
            prefetched: None,
            reservation: 0,
            charged: 0,
        }))
    }

    /// The raw (still stored-form) bytes this cursor holds right now: the row
    /// group it is decoding blocks out of plus the one prefetched behind it, at
    /// most two. These are the same bytes the #977 tracker's fetched term counts.
    fn raw_resident_bytes(&self) -> u64 {
        let group = self.group.as_ref().map_or(0, |(_, d)| d.len() as u64);
        let prefetched = self.prefetched.as_ref().map_or(0, |(_, d)| d.len() as u64);
        group.saturating_add(prefetched)
    }

    /// The decoded columnar block this cursor holds right now, as
    /// [`StreamBlockRows::heap_estimate`] reported it at decode. Zero between
    /// blocks and once the cursor is exhausted. This is the same figure the #977
    /// tracker's decoded term carries for this cursor.
    fn decoded_bytes(&self) -> u64 {
        self.block_bytes
    }

    /// What this cursor actually holds resident: its location metadata, its raw
    /// row-group bytes, and its decoded block. This is what the merge reconciles
    /// the pre-decode reservation down to once a decode completes (ADR-0979
    /// decision 4 as amended), and it is bounded above by that reservation --
    /// the raw term by `2 * G` and the decoded term by the per-block ceiling the
    /// reservation took the maximum of.
    fn resident_bytes(&self) -> u64 {
        loc_metadata_bytes(&self.locs)
            .saturating_add(self.raw_resident_bytes())
            .saturating_add(self.decoded_bytes())
    }

    /// The `ts_ns` of the record this cursor would yield next, or `None` once
    /// it is exhausted. This is the k-way merge's ordering key, and it is read
    /// out of the decoded timestamp column: choosing the next record to emit
    /// materializes nothing.
    ///
    /// Only valid immediately after [`Self::refill`], which is what
    /// re-establishes "the current block has a row left" across a block or loc
    /// boundary. A drained-but-unreleased block reads as exhausted, which is
    /// exactly what a cursor that has run out of records also reads as; the
    /// merge distinguishes them by refilling the cursor it just consumed from
    /// before the next comparison.
    fn peek_ts(&self) -> Option<i64> {
        self.block.as_ref().and_then(|b| b.peek_ts())
    }

    /// Materialize the record [`Self::peek_ts`] just reported, rebuilding
    /// exactly one row out of the decoded columnar block. The caller must
    /// [`Self::refill`] before the next merge step.
    fn next_record(&mut self) -> Result<Option<LogRecord>> {
        match self.block.as_mut().and_then(|b| b.next()) {
            Some(rec) => Ok(Some(rec?)),
            None => Ok(None),
        }
    }

    /// Release the current decoded block: drop the columnar buffers and take
    /// their bytes back out of the tracker's decoded term.
    fn release_block(&mut self, tracker: Option<&MergeMemoryTracker>) {
        self.block = None;
        if self.block_bytes > 0 {
            if let Some(t) = tracker {
                t.block_released(self.block_bytes);
            }
            self.block_bytes = 0;
        }
    }

    /// Make the cursor ready to yield: leave `block` on a row of this stream,
    /// decoding the next block out of the current loc's bytes (fetching the next
    /// loc by range when the current one is spent) until one has a row or the
    /// stream is exhausted in this input. At most one decoded block plus at most
    /// two locs' raw bytes (the one being decoded from and the one prefetched
    /// behind it) are resident at a time, and no record is materialized here.
    ///
    /// `budget`, when present, is the merge's live cursor-budget accounting.
    /// Every decode this call performs is preceded by a grow-and-check on it
    /// (ADR-0979 decision 4 as amended): the charge rises to cover the block
    /// about to be decoded BEFORE the decode starts, and a growth that would
    /// cross the budget refuses instead, so a later, larger block cannot be
    /// materialized past a budget the reconcile has already lowered the charge
    /// below. `None` is for the first refill of a freshly admitted cursor,
    /// whose reservation already covers every block it can decode, and for
    /// tests driving a cursor with no budget in play.
    async fn refill(
        &mut self,
        store: &dyn ObjectStoreBackend,
        tracker: Option<&MergeMemoryTracker>,
        mut budget: Option<&mut CursorBudget<'_>>,
    ) -> Result<()> {
        loop {
            if self.block.as_ref().is_some_and(|b| !b.is_exhausted()) {
                return Ok(());
            }
            // The current block is drained (or there is none): release it and
            // decode the next. A candidate block with no row for this stream (a
            // neighbour's boundary block) simply comes back exhausted and is
            // released on the next turn of this loop.
            self.release_block(tracker);
            if let (Some(b), Some(block)) = (budget.as_deref_mut(), self.pending_block_index()) {
                let required = self.decode_charge_requirement(block)?;
                b.grow_to(&mut self.charged, block, required)?;
            }
            if self.decode_next_block(tracker)? {
                continue;
            }
            // The current loc is fully decoded: fetch the next one.
            match self.next_raw_block(store, tracker).await? {
                Some((loc, data)) => {
                    self.group_raw_bytes = data.len() as u64;
                    self.decoded_in_group = 0;
                    self.group = Some((loc, data));
                }
                None => return Ok(()),
            }
        }
    }

    /// The whole-object index of the block [`Self::decode_next_block`] would
    /// decode right now, or `None` when the loc it decodes out of is spent or
    /// absent (a fetch has to come first, and the block that follows it is only
    /// known once that loc is held).
    fn pending_block_index(&self) -> Option<usize> {
        let (loc, _) = self.group.as_ref()?;
        loc.block_indices().get(self.decoded_in_group).copied()
    }

    /// What this cursor must be charged before decoding `block`: everything it
    /// will hold at the instant that decode completes, priced from metadata
    /// alone (ADR-0979 decision 4 as amended). Its location metadata and the raw
    /// row-group bytes it still holds are measured -- both are already resident
    /// -- and the block itself is priced at its pre-decode ceiling, since its
    /// decoded size is exactly what is not knowable yet.
    ///
    /// The current decoded block is NOT in the sum: `refill` releases it before
    /// the decode this figure gates, so charging it would price memory the
    /// cursor no longer holds.
    fn decode_charge_requirement(&self, block: usize) -> Result<u64> {
        let ceiling = block_decode_ceiling_bytes(self.pricing, block)?;
        Ok(loc_metadata_bytes(&self.locs)
            .saturating_add(self.raw_resident_bytes())
            .saturating_add(ceiling))
    }

    /// Decode the current loc's next undecoded block into `block`, returning
    /// whether one was decoded (`false` when the loc is spent, or absent).
    ///
    /// The loc's raw bytes stay resident across this call so the following block
    /// can be decoded from them, and are dropped as the last block is decoded:
    /// the tracker's raw term is therefore per loc (one row group under version
    /// 4, one block under version 3) while its decoded term is per block.
    ///
    /// The decoded term charged is the columnar block's
    /// [`StreamBlockRows::heap_estimate`], the bytes this cursor actually holds
    /// until the block is released (ADR-0979 decision 1). It is NOT the row-form
    /// `estimate_record` sum the eager decode charged: the records that sum
    /// measured are no longer built here, and charging both would count one
    /// cursor twice in the same phase snapshot.
    fn decode_next_block(&mut self, tracker: Option<&MergeMemoryTracker>) -> Result<bool> {
        let Some((loc, data)) = self.group.take() else {
            return Ok(false);
        };
        let Some(&block) = loc.block_indices().get(self.decoded_in_group) else {
            // `stream_blocks` never returns a loc with no blocks; release the
            // raw charge rather than leak it if one ever did.
            if let Some(t) = tracker {
                t.block_decoded(self.group_raw_bytes, 0);
            }
            self.group_raw_bytes = 0;
            self.decoded_in_group = 0;
            return Ok(false);
        };
        let last = self.decoded_in_group + 1 == loc.block_indices().len();
        // Copied out of `self` before the call so the view's lifetime is the
        // catalog's, not this `&mut self` borrow's. The view owns its decoded
        // block and borrows only the reader's directories, so `data` is free to
        // drop below.
        let reader: &'a RlogRangeReader = self.reader;
        let rows = reader.block_rows_in_group(&loc, block, data.as_ref())?;
        self.decoded_in_group += 1;
        let decoded_bytes = rows.heap_estimate();
        if last {
            // Both the raw loc and this block's columnar buffers are resident at
            // this instant; the raw buffer is dropped right after.
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
        self.block = Some(rows);
        self.block_bytes = decoded_bytes;
        Ok(true)
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

/// One input queued for overlap-gated admission (ADR-0979 decision 2): its
/// canonical `input_index`, the SKIP_IDX ts lower bound its cursor's first
/// record cannot precede, and the pre-decode reservation admitting it charges
/// against the merge budget (ADR-0979 decision 4).
struct PendingCursor {
    input_index: usize,
    lower_bound: i64,
    reservation: u64,
}

/// The merge's live cursor-budget accounting, handed to
/// [`StreamCursor::refill`] so a cursor cannot decode a block its charge does
/// not already cover (ADR-0979 decision 4 as amended).
///
/// Admission checks the same budget against the same running `charged` before
/// a cursor opens; this is the same check moved to the other point in a
/// cursor's life where its residency can rise, the decode of a later, larger
/// block after the reconcile has lowered its charge. Without it the reconcile
/// would convert the reservation from a bound into a transient: charges lowered
/// to actuals free budget for further admissions, and every one of those
/// cursors could then grow back toward its ceiling with nothing checking the
/// sum again.
struct CursorBudget<'m> {
    /// [`CompactorConfig::merge_cursor_budget_bytes`].
    budget: u64,
    /// The stream's running charge over its open cursors, the same variable
    /// admission reserves against.
    charged: &'m mut u64,
    /// Hex stream id, for the refusal.
    stream_id: &'m LogStreamId,
    /// Cursors open at the moment of the grow, including the growing one.
    open_cursors: usize,
    /// Inputs carrying this stream, for the refusal.
    inputs_carrying_stream: usize,
}

impl CursorBudget<'_> {
    /// Raise one cursor's charge to `required` before it decodes `block`,
    /// refusing with [`MaintainError::MergeCursorBudgetExceeded`] if the
    /// stream's running charge cannot take the growth.
    ///
    /// A charge already at or above `required` is left alone: the reconcile
    /// after the previous decode may have left it above what the next block
    /// needs, and lowering it here would be a reconcile, which belongs after a
    /// decode, not before one.
    ///
    /// The check and the decode it gates are not separated by an `.await`: this
    /// runs inside [`StreamCursor::refill`] between releasing the previous
    /// block and calling the synchronous [`StreamCursor::decode_next_block`],
    /// and the merge loop drives one cursor's refill at a time. So no other
    /// cursor can be admitted, and no other block decoded, between a growth
    /// passing and the memory it accounts for being taken -- the same
    /// serialization the admission reservation has.
    fn grow_to(&mut self, cursor_charged: &mut u64, block: usize, required: u64) -> Result<()> {
        let Some(grow) = required.checked_sub(*cursor_charged).filter(|g| *g > 0) else {
            return Ok(());
        };
        let charged_bytes = *self.charged;
        let required_bytes = charged_bytes.saturating_add(grow);
        if required_bytes > self.budget {
            return Err(MaintainError::MergeCursorBudgetExceeded {
                stream_id: self.stream_id.to_hex(),
                open_cursors: self.open_cursors,
                charged_bytes,
                budget_bytes: self.budget,
                required_bytes,
                inputs_carrying_stream: self.inputs_carrying_stream,
                site: MergeCursorBudgetSite::BlockGrow {
                    block_index: block,
                    grow_bytes: grow,
                },
            });
        }
        *self.charged = required_bytes;
        *cursor_charged = required;
        Ok(())
    }
}

/// The heap a cursor's own location metadata holds for its whole lifetime: the
/// owned `Vec<StreamBlockLoc>` and each loc's block-index list. Charged in the
/// pre-decode reservation and again in the reconciled residency, because the
/// cursor holds it in both states.
fn loc_metadata_bytes(locs: &[StreamBlockLoc]) -> u64 {
    let loc_slot = std::mem::size_of::<StreamBlockLoc>() as u64;
    let idx_slot = std::mem::size_of::<usize>() as u64;
    let mut total = (locs.len() as u64).saturating_mul(loc_slot);
    for l in locs {
        total = total.saturating_add((l.block_indices().len() as u64).saturating_mul(idx_slot));
    }
    total
}

/// The pre-decode reservation one input's cursor over `stream_id` is charged
/// against the merge budget (ADR-0979 decision 4), or `None` if the input does
/// not carry the stream. Computed from resident metadata alone, before any
/// BLOCKS byte is fetched:
///
/// - `2 * G`: the two row groups' stored bytes a cursor holds at once (the loc
///   being decoded from plus the one prefetched behind it), sized as twice the
///   largest of the stream's locs so a later, larger group cannot exceed it;
/// - the cursor's location metadata ([`loc_metadata_bytes`]);
/// - `B_dec`: the MAXIMUM over the stream's candidate blocks of that block's
///   decoded-block ceiling ([`block_decode_ceiling_bytes`]). The max, not the
///   first block's cost, because [`StreamCursor::refill`] decodes later blocks
///   after releasing earlier ones, so a later, larger block must not exceed the
///   reservation.
///
/// This is a ceiling on the cursor's residency for its whole lifetime, not a
/// measurement of it: [`StreamCursor::resident_bytes`] is what the cursor
/// actually holds, and the merge reconciles the charge down to that figure as
/// soon as the decode completes (ADR-0979 decision 4 as amended; the default
/// budget is sized from reconciled residency, so holding the ceiling for a
/// cursor's lifetime would refuse runs the default exists to admit).
fn cursor_reservation_bytes(
    catalog: &RlogInputCatalog,
    stream_id: &LogStreamId,
) -> Result<Option<u64>> {
    let Some(locs) = catalog.reader.stream_blocks(stream_id)? else {
        return Ok(None);
    };
    if locs.is_empty() {
        return Ok(None);
    }
    // 2*G: twice the largest loc's stored row-group bytes.
    let max_group = locs.iter().map(|l| l.byte_len()).max().unwrap_or(0);
    let two_g = max_group.saturating_mul(2);
    let loc_meta = loc_metadata_bytes(&locs);
    // B_dec: the max decoded-block ceiling over the stream's candidate blocks.
    let mut b_dec = 0u64;
    for l in &locs {
        for &block in l.block_indices() {
            b_dec = b_dec.max(block_decode_ceiling_bytes(&catalog.pricing, block)?);
        }
    }
    Ok(Some(two_g.saturating_add(loc_meta).saturating_add(b_dec)))
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
/// # Overlap-gated cursor admission (ADR-0979 decision 2)
///
/// Cursors are not all opened up front. Each input carrying the stream is
/// queued with its SKIP_IDX ts lower bound (`stream_ts_bounds`, a sound lower
/// bound on the first timestamp that input's cursor can yield). The merge
/// maintains the invariant: before emitting a record with key `(ts,
/// input_index)`, every queued cursor whose lower bound is `<= ts` is opened.
/// Because a queued cursor's true first timestamp is `>= its lower bound`, an
/// unopened cursor left behind (lower bound `> ts`) can hold no record that
/// precedes the candidate, and equality forces admission so exact-`ts` ties
/// still resolve by `input_index` exactly as an all-open merge would. The
/// emitted sequence -- and therefore every part boundary and every part byte --
/// is identical to opening every cursor at once; only WHEN a cursor opens
/// changes. The number of simultaneously open cursors becomes `D`, the max
/// concurrent ts-overlap of the stream's input slices, instead of `n`, the
/// input count. Admitted cursors open `input_read_concurrency` at a time in
/// canonical order (so a stream carried by hundreds of inputs does not
/// serialize hundreds of round trips), and a drained cursor releases its
/// residency immediately. [`AdmissionMode::EagerAll`] opens every cursor up
/// front instead, the pre-decision-2 behaviour, retained so the differential
/// test can assert the two produce byte-identical parts.
///
/// # Fail-closed cursor budget (ADR-0979 decision 4)
///
/// Admitting a cursor first reserves its ceiling cost
/// ([`cursor_reservation_bytes`]) against
/// [`CompactorConfig::merge_cursor_budget_bytes`]. The reservation is charged
/// BEFORE the cursor fetches or decodes anything, so the budget is enforced at
/// reserve time and the merge fails closed rather than after allocating: if
/// admitting the batch that merge order requires would exceed the budget, the
/// run aborts with [`MaintainError::MergeCursorBudgetExceeded`] before
/// publishing. Once a cursor's decode completes, its charge is reconciled down
/// to what it actually holds ([`reconcile_cursor_charge`]), which is what the
/// default budget is sized from. A drained cursor releases its charge, so the
/// charge tracks `D`, not `n`.
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
    let budget = sink.config.merge_cursor_budget_bytes;
    let mode = sink.config.merge_admission;

    // The admission queue: one entry per input carrying the stream, ordered by
    // (lower_bound, input_index) so opens follow the frontier and ties keep
    // canonical order.
    let mut pending: Vec<PendingCursor> = Vec::new();
    for (idx, catalog) in catalogs.iter().enumerate() {
        let Some((lower_bound, _upper)) = catalog.reader.stream_ts_bounds(stream_id) else {
            continue;
        };
        let Some(reservation) = cursor_reservation_bytes(catalog, stream_id)? else {
            continue;
        };
        pending.push(PendingCursor {
            input_index: idx,
            lower_bound,
            reservation,
        });
    }
    pending.sort_by_key(|a| (a.lower_bound, a.input_index));

    // Box each open-and-first-refill with an explicit `+ Send` bound before
    // `buffered`, the workaround `crate::build::fetch_batch_pages` documents for
    // futures that borrow the `&dyn ObjectStoreBackend`. `buffered` keeps
    // canonical input order, which is the k-way merge's tie-break.
    type CursorFuture<'f, 'a> =
        Pin<Box<dyn Future<Output = Result<Option<StreamCursor<'a>>>> + Send + 'f>>;

    let mut open: Vec<StreamCursor> = Vec::new();
    let mut charged: u64 = 0;
    let mut pos = 0usize;

    loop {
        // Admission (ADR-0979 decision 2): open every queued cursor the
        // invariant requires before the next emit. The frontier is the min
        // peek_ts over open cursors; under `Overlap` a queued cursor is admitted
        // when its lower bound is at or below the frontier (with a single
        // bootstrap open when nothing is open yet so a frontier exists), under
        // `EagerAll` every remaining cursor is admitted regardless.
        loop {
            let frontier = open.iter().filter_map(|c| c.peek_ts()).min();
            // The prefix of `pending` (sorted by lower bound) to admit this
            // round.
            let mut batch_len = 0usize;
            while let Some(p) = pending.get(pos + batch_len) {
                let admit = match mode {
                    AdmissionMode::EagerAll => true,
                    AdmissionMode::Overlap => match frontier {
                        // Nothing open yet: bootstrap with the single
                        // lowest-lower-bound cursor, then re-derive the frontier
                        // from it on the next round.
                        None => batch_len == 0,
                        Some(ts) => p.lower_bound <= ts,
                    },
                };
                if !admit {
                    break;
                }
                batch_len += 1;
                if matches!(mode, AdmissionMode::Overlap) && frontier.is_none() {
                    break;
                }
            }
            if batch_len == 0 {
                break;
            }
            // Reserve the whole batch at reserve time (ADR-0979 decision 4):
            // check the budget before any cursor fetches or decodes, so the
            // merge fails closed rather than after allocating. The refusal's
            // figures separate the two: `open_charge` is what the cursors that
            // exist hold, and the required total names the batch position that
            // crossed the budget. Batch members before that position were never
            // opened, so folding their reservations into the "already charged"
            // figure would report memory nothing holds.
            let open_charge = charged;
            let mut batch_charge = 0u64;
            for k in 0..batch_len {
                batch_charge = batch_charge.saturating_add(pending[pos + k].reservation);
                let required = open_charge.saturating_add(batch_charge);
                if required > budget {
                    return Err(MaintainError::MergeCursorBudgetExceeded {
                        stream_id: stream_id.to_hex(),
                        open_cursors: open.len(),
                        charged_bytes: open_charge,
                        budget_bytes: budget,
                        required_bytes: required,
                        inputs_carrying_stream: pending.len(),
                        site: MergeCursorBudgetSite::Admission {
                            batch_position: k,
                            batch_len,
                        },
                    });
                }
            }
            charged = open_charge.saturating_add(batch_charge);
            let opens: Vec<CursorFuture<'_, '_>> = (0..batch_len)
                .map(|k| {
                    let p = &pending[pos + k];
                    Box::pin(open_cursor(
                        store,
                        &catalogs[p.input_index],
                        p.input_index,
                        stream_id,
                        tracker,
                    )) as CursorFuture<'_, '_>
                })
                .collect();
            let opened: Vec<Option<StreamCursor>> = stream_iter(opens)
                .buffered(concurrency)
                .try_collect()
                .await?;
            for (k, cursor_opt) in opened.into_iter().enumerate() {
                let reservation = pending[pos + k].reservation;
                match cursor_opt {
                    Some(mut cursor) => {
                        cursor.reservation = reservation;
                        cursor.charged = reservation;
                        // `open_cursor` fetched and decoded this cursor's first
                        // block, so its actual residency is known now: reconcile
                        // the ceiling down to it before the next admission round
                        // reads the charge.
                        reconcile_cursor_charge(&mut cursor, &mut charged);
                        open.push(cursor);
                    }
                    // Carried the stream in STREAM_DIR but materialized no row
                    // (its candidate blocks were all a neighbour's boundary
                    // blocks): it holds nothing, so release its reservation.
                    None => charged = charged.saturating_sub(reservation),
                }
            }
            pos += batch_len;
            if let Some(t) = tracker {
                t.note_open_cursors(open.len() as u64);
            }
        }

        // Pick the cursor whose next row has the minimum (ts_ns, input_index).
        // input_index is unique per cursor, so the key is a total order and the
        // tie-break is deterministic. peek_ts is only valid immediately after a
        // refill: every open cursor was refilled when it was admitted or after
        // it last emitted, so the column read here is live. Skipping a refill
        // would leave a drained block reading as exhausted and silently drop the
        // records behind it; the record-count conservation gate downstream is
        // what fails closed if that protocol is ever broken. The comparison
        // reads each cursor's decoded ts column and materializes nothing
        // (ADR-0979 decision 1); only the winner rebuilds a record.
        let mut best: Option<(usize, i64, usize)> = None;
        for (i, cursor) in open.iter().enumerate() {
            if let Some(ts) = cursor.peek_ts() {
                let key = (ts, cursor.input_index);
                match best {
                    Some((_, bts, bidx)) if (bts, bidx) <= key => {}
                    _ => best = Some((i, ts, cursor.input_index)),
                }
            }
        }
        // No open cursor has a record: since admission above opens every queued
        // cursor the invariant needs (and bootstraps when nothing is open),
        // reaching here with an empty best means the queue is drained too.
        let Some((bi, _, _)) = best else {
            break;
        };
        if let Some(rec) = open[bi].next_record()? {
            counts.add_input()?;
            if keep(&rec)? {
                counts.add_output()?;
                sink.push(rec).await?;
            }
        }
        // The refill carries the budget: if it crosses into a block whose
        // pre-decode ceiling the cursor's reconciled charge no longer covers, it
        // grows the charge first and refuses before the decode rather than
        // after it (ADR-0979 decision 4 as amended). Nothing else touches
        // `charged` while this borrow is live, which is the serialization the
        // grow needs: one cursor refills at a time, and the admission round for
        // this emit is already complete.
        let mut cursor_budget = CursorBudget {
            budget,
            charged: &mut charged,
            stream_id,
            open_cursors: open.len(),
            inputs_carrying_stream: pending.len(),
        };
        open[bi]
            .refill(store, tracker, Some(&mut cursor_budget))
            .await?;
        // A drained cursor releases its residency (refill already dropped its
        // last block) and its charge, so both the open-cursor high-water and the
        // budget charge track the concurrent overlap `D`, not `n`. A cursor that
        // is still live may have crossed into a new block or a new row group in
        // that refill, so its charge is re-derived from what it now holds.
        if open[bi].peek_ts().is_none() {
            let cursor = open.swap_remove(bi);
            charged = charged.saturating_sub(cursor.charged);
        } else {
            reconcile_cursor_charge(&mut open[bi], &mut charged);
        }
    }
    Ok(())
}

/// Reconcile one cursor's budget charge to what it actually holds resident
/// (ADR-0979 decision 4 as amended), updating the stream's running `charged`
/// total by the difference.
///
/// Mandatory, not an optimization. The pre-decode reservation is a ceiling over
/// every block the cursor may decode, evaluated before any of them is fetched;
/// the default budget is sized from a cursor's ACTUAL residency, so a merge that
/// held the ceiling for each cursor's lifetime would refuse corpora the default
/// was chosen to admit. Called once the fetch and decode that follow an
/// admission complete, and again after each later block decode, so the charge is
/// the cursor's residency for all but the instant between reserving and
/// decoding. The reconciled figure can only be at or below the reservation: the
/// raw term is bounded by the reservation's `2 * G` and the decoded term by the
/// per-block ceiling the reservation took the maximum over.
fn reconcile_cursor_charge(cursor: &mut StreamCursor<'_>, charged: &mut u64) {
    let actual = cursor.resident_bytes();
    *charged = charged
        .saturating_sub(cursor.charged)
        .saturating_add(actual);
    cursor.charged = actual;
}

/// Open one input's cursor over `stream_id` and load its first block. Named
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
    // No budget: the caller reserved this cursor's ceiling before calling, and
    // that reservation is the maximum over every block the cursor can decode,
    // so this first refill is covered whichever blocks it decodes.
    cursor.refill(store, tracker, None).await?;
    Ok(cursor.peek_ts().is_some().then_some(cursor))
}

/// Turn one part's already-encoded L1 object bytes into a [`BuiltPart`] and PUT
/// it `CreateIfAbsent`. The object was produced by
/// [`PartBuilder::into_encoded`]/[`PartBuilder::encode_clone`] via the shared
/// [`RlogWriter::finish_compacted_with_stats`] pipeline (stamping `level = 1`,
/// the `input_set_hash`, and `part_index`); the part's summary stats are read
/// back from the object's own footer, so they describe exactly what was
/// written.
///
/// The object was encoded with the same [`RlogWriter::with_indexed_fields`] the
/// L0 write path uses, so its POSTINGS is built by the one writer implementation
/// from this part's own blocks (ADR-0049 decision 6). The per-field
/// distinct-value cap therefore applies to the merged part; when it fires the
/// writer drops that field's postings and reports it in `stats`, logged here
/// because a silently unindexed field is invisible in the object bytes (they are
/// simply absent, which is always legal).
///
/// `first_stream_id`/`last_stream_id` are the part's inclusive stream-id bounds
/// accumulated as records were pushed (streams are merged in sorted id order,
/// so `first` is the smallest and `last` the largest id in the part). Since
/// issue #711 a part may open or close in the middle of a stream, so one part's
/// `last` may equal the next part's `first`; the bounds are adjacent, not
/// strictly disjoint.
#[allow(clippy::too_many_arguments)]
async fn finalize_part(
    store: &dyn ObjectStoreBackend,
    bucket: &Bucket,
    object: Vec<u8>,
    stats: WriteStats,
    first_stream_id: Option<LogStreamId>,
    last_stream_id: Option<LogStreamId>,
    input_set_hash: &[u8; 32],
    part_index: u32,
    dry_run: bool,
    retain_bytes: bool,
    declared_accum: &DeclaredStatAccum,
) -> Result<BuiltPart> {
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

    let mut part = CompactionPart {
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
    // Stamp the declared-column extrema recomputed over exactly this part's rows
    // (ADR-0873 decision 3), through the validated commit-side path and against
    // the part's own row count read back from the footer above, so
    // `non_null + null_count == sample_count` by construction and the part's own
    // reader never drops a stamp. An empty fold (metrics/spans, an unstamped
    // input set, or the erasure-rewrite route) stamps nothing, which is the
    // permanently legal state. This mutates only the record metadata: the object
    // bytes and `content_hash` above are already fixed, so stamping cannot move a
    // differential hash.
    let stamps = declared_accum.build_stamps(part.sample_count);
    ravel_commit::declared_stats::stamp_compaction_part(&mut part, &stamps);
    let mut built = BuiltPart {
        key,
        bytes: Some(object),
        part,
        put_already_existed: false,
    };
    if !dry_run {
        match put_part(store, &built).await? {
            crate::build::PartPut::Created => {}
            crate::build::PartPut::AlreadyExisted => built.put_already_existed = true,
        }
    }
    // Release the encoded bytes unless the caller retains them for its own
    // deferred publish path (ADR-0979 decision 3). Compaction passes
    // `retain_bytes = false`: a fresh PUT's part is age-zero and unreachable by
    // the unreferenced-part sweep, so its in-RAM copy was belt-and-braces, not a
    // correctness dependency; an `AlreadyExists` part is instead HEAD-verified
    // after the record PUT (its bytes are dropped here too, since retaining
    // every such part on an abandoned-run retry would recreate the whole-output
    // term this decision removes). Under `dry_run` nothing is ever PUT, so the
    // drop happens at close. The erasure rewrite passes `retain_bytes = true`:
    // it defers every PUT to its own post-conservation-gate publish path, so the
    // bytes are the product itself.
    if !retain_bytes {
        built.bytes = None;
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

/// A cheap upper bound on the encoded/on-object bytes one merged [`LogRecord`]
/// adds to a part. It does NOT close the part on `max_l1_part_bytes` (the
/// stored-size target): the part closes on its object's ACTUAL encoded size,
/// measured by an exact-encode probe ([`PartBuilder::encode_clone`]). This
/// figure's only job is to SCHEDULE that probe -- summed per record into
/// [`PartBuilder::stored_estimate`], it says when the object is plausibly large
/// enough to be worth encoding to check (issue #872).
///
/// This is deliberately NOT [`estimate_record`], which measures the record's
/// Rust *heap* for the memory split target. The two answer different questions
/// and are an order of magnitude apart on a wide schema: the memory split target
/// reaches 256 MiB of heap after only ~3.5 MB of stored bytes on the ClickBench
/// tenant, so a single knob measured in heap could not also govern object
/// geometry.
///
/// It is a pre-compression payload proxy: the sum of the value bytes that
/// actually enter this part's columns (timestamps and identifiers as a small
/// fixed cost, then `severity_text`, `body`, and every dynamic attribute's key
/// and value bytes). Two things are excluded on purpose:
///
/// - `stream_attrs` (the resource/scope blob) is stored once per stream in
///   STREAM_DIR, not once per record, so charging it per record would inflate
///   the proxy on exactly the wide-stream shape this is meant to size. It is
///   not free, though: [`estimate_stored_stream`] charges each distinct stream's
///   blob once per part it appears in, so the proxy over a part carrying many
///   streams with large resource blobs still grows toward the probe threshold.
///   Charging neither made the proxy blind to the whole STREAM_DIR section.
/// - zstd compression. The proxy is the uncompressed column payload, so it is an
///   upper bound on the bytes those columns compress to. That is the direction
///   that matters for a scheduling gate: the proxy reaches the target no later
///   than the object's real bytes would, so the probe is never scheduled too
///   late to catch the crossing. (Under the old proxy-as-close design this same
///   upper bound made objects `T / compression_ratio` in size, a ratio-times
///   miss; closing on the probe removes it.)
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
/// Without this charge the proxy would ignore STREAM_DIR entirely, so a bucket
/// with many streams and large resource or scope blobs would grow real object
/// bytes the proxy could not see, scheduling the exact-encode probe too late and
/// overshooting `max_l1_part_bytes`. The proxy is only a sound probe-scheduling
/// coordinate if it counts every section that grows with the data.
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
        AdmissionMode, Bucket, CompactionOutcome, CompactorConfig, FixedClock, MergeMemoryTracker,
        compact_bucket,
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
    /// delete the `if over_memory { .. self.flush(None).await?; }` block from
    /// `PartSink::push` and re-add the flush to `build_parts`'s `for stream_id in
    /// merged.keys()` loop (`if part.estimate >=
    /// config.l1_part_memory_target_bytes { sink.flush(None).await?; }` after
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

    /// One record of stream 0 with a deliberately KNOWN encoded/decoded ratio: a
    /// short per-ordinal-unique prefix (so blocks do not collapse to nothing and
    /// parts fill in a bounded record count) followed by a long repeated filler
    /// (so the pre-compression payload proxy runs several times ahead of the
    /// bytes the columns actually compress to). Every record is the same width,
    /// so each contributes about the same object bytes and the geometry band is
    /// uniform. Fixed single stream, so STREAM_DIR is a one-time per-part charge.
    ///
    /// This is the fixture the stored-target geometry rests on: the gap between
    /// its payload proxy and its compressed object bytes is exactly the ratio the
    /// old proxy-as-close design mis-sized parts by (issue #872).
    fn ratio_record(i: i64) -> LogRecord {
        // A high-entropy prefix (does not compress, so it sets the object bytes
        // and parts fill in a bounded record count) followed by a long
        // compressible filler (inflates the payload proxy well past the object
        // bytes). The gap is a stable ratio near 4.
        let body = format!(
            "{}{}",
            incompressible_pad(i as u64, 48),
            "compressible-".repeat(16)
        );
        record(0, SPLIT_BASE_NS + i, &body, Vec::new())
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

    /// Compact the [`ratio_record`] fixture into a FRESH store with both targets
    /// effectively disabled, so the whole bucket is one part, and return its
    /// decoded record sequence: the pre-split baseline the differential check
    /// compares against.
    async fn ratio_fixture_single_part(per_input: i64, inputs: i64) -> Vec<LogRecord> {
        let store = MemoryStore::new();
        for j in 0..inputs {
            let recs: Vec<LogRecord> = (0..per_input)
                .map(|i| ratio_record(i * inputs + j))
                .collect();
            seed(&store, Uuid::from_u128(j as u128 + 1), j as u64 + 1, &recs).await;
        }
        let config = CompactorConfig {
            max_l1_part_bytes: u64::MAX,
            l1_part_memory_target_bytes: u64::MAX,
            ..CompactorConfig::default()
        };
        let clock = FixedClock::new(sealed_now_ns());
        compact_bucket(&store, &clock, &config, &bucket())
            .await
            .expect("compact");
        let (_rec, parts) = read_output(&store).await;
        assert_eq!(parts.len(), 1, "baseline must be a single part");
        let mut rows = Vec::new();
        for p in &parts {
            rows.extend(decode_all(p));
        }
        rows
    }

    /// Issue #872 acceptance (geometry): the stored-size target closes each part
    /// on its ACTUAL encoded object bytes, so every non-final part reaches the
    /// target rather than coming out a compression-ratio-times-smaller object,
    /// which is what closing on the pre-compression payload proxy produced. The
    /// memory split target is left at its 256 MiB default (far above anything
    /// this corpus reaches in heap), so the stored target is the only thing that
    /// can fire.
    ///
    /// The band is exact from the fixture, never merely `> 0`, and its width is
    /// the scheduler's guarantee rather than this fixture's luck. A part closes
    /// the moment a probe shows its object reaching `STORED_TARGET`. Probes sit
    /// at least [`PROBE_MIN_STEP_BYTES`] of payload proxy apart, and
    /// [`PartBuilder::schedule_next_probe`] aims the next one exactly the
    /// remaining encoded deficit further along the proxy axis, so while encoded
    /// bytes grow at most 1:1 with the proxy the encoded size grows between two
    /// probes by at most that deficit (or the floor, when it is larger) plus the
    /// charge of the record that crosses. Every part therefore sits in
    /// `[STORED_TARGET, STORED_TARGET + PROBE_MIN_STEP_BYTES + one record's proxy
    /// charge]`. The trailing part loses only the lower bound (its records run
    /// out before the next probe rather than a probe closing it); the same upper
    /// bound covers it by the same arithmetic, so it is asserted, not excused.
    ///
    /// The probe count is pinned exactly, and the pin is checked against the
    /// scheduler's geometric cost model rather than merely recorded. Spending an
    /// encoded deficit as a PROXY distance closes only a `1 / r` fraction of it
    /// on a payload that compresses `r`-fold, so the deficit decays by
    /// `(1 - 1 / r)` per probe: from the deficit at the crossing probe,
    /// `d0 = STORED_TARGET * (1 - 1 / r)`, the ladder takes
    /// `ln(d0 / PROBE_MIN_STEP_BYTES) / ln(r / (r - 1))` probes to reach the
    /// floor, about `r` more below it (each floored step closes
    /// `PROBE_MIN_STEP_BYTES / r`), plus the crossing probe itself -- which is
    /// `r * ln(d0 / PROBE_MIN_STEP_BYTES) + r` for large `r`.
    ///
    /// This fixture measures `r` = 7.08 (`ratio` below, printed by the run), so
    /// `d0` = 14_069, the ladder is `ln(14069 / 4096) / ln(7.08 / 6.08)` = 8.1
    /// probes, the floor tail is 7.1, and with the crossing probe that is 16.2
    /// per part. 11 of the 12 parts close on the target (178 probes) and the
    /// trailing part runs its own partial ladder without ever closing (its proxy
    /// passes the target, its records run out), which accounts for the remaining
    /// 13 of the 191 pinned below. The model treats `r` as constant along a part
    /// and ignores that a floored step overshoots, so a few percent is the
    /// expected agreement; a change that made probing linear in the deficit, or
    /// per-record, would miss it by an order of magnitude. Demonstrated red by
    /// the rate-model scheduler named in
    /// [`stored_target_overshoot_is_bounded_when_compressibility_collapses`]
    /// (an uncapped cumulative-rate step in place of
    /// `let step = deficit.max(PROBE_MIN_STEP_BYTES);`), under which this fixture
    /// runs 34 probes, not 191.
    ///
    /// Demonstrated red against the old proxy-as-close design: replace the probe
    /// block in `PartSink::push` with `over_stored = part.stored_estimate >=
    /// self.config.max_l1_part_bytes` and flush on it. The same fixture then
    /// closes each part when its PAYLOAD reaches the target, so the objects come
    /// out `ratio` times smaller -- below `STORED_TARGET / 2` -- and the
    /// lower-bound assertion below fails. The `ratio` sub-assertion pins that
    /// this fixture's gap is well above 2, so the old design provably misses the
    /// band low. This corpus is uniformly compressible, so it cannot show the
    /// upper bound failing: the fixture that does is
    /// [`stored_target_overshoot_is_bounded_when_compressibility_collapses`].
    #[tokio::test]
    async fn stored_target_closes_parts_on_actual_encoded_object_bytes() {
        const PER_INPUT: i64 = 2500;
        const INPUTS: i64 = 2;
        const STORED_TARGET: u64 = 16 * 1024;
        let total = (PER_INPUT * INPUTS) as u64;

        let store = Arc::new(MemoryStore::new());
        for j in 0..INPUTS {
            let recs: Vec<LogRecord> = (0..PER_INPUT)
                .map(|i| ratio_record(i * INPUTS + j))
                .collect();
            seed(
                store.as_ref(),
                Uuid::from_u128(j as u128 + 1),
                j as u64 + 1,
                &recs,
            )
            .await;
        }

        // The fixture's encoded/decoded gap, measured not assumed: encode a
        // sample exactly as the merge does and compare the payload proxy the old
        // design closed on to the object bytes the new design closes on. A ratio
        // above 2 is what puts the old design's parts (~STORED_TARGET / ratio)
        // below the STORED_TARGET/2 band floor.
        let sample: Vec<LogRecord> = (0..200).map(ratio_record).collect();
        let (proxy, object_len) = proxy_and_object_bytes(&sample);
        let ratio = proxy as f64 / object_len as f64;
        assert!(
            ratio > 2.0,
            "fixture must have a real compression gap: payload proxy {proxy} vs object \
             {object_len} is only {ratio:.2}x; the old proxy-close bug needs ratio > 2 to \
             land parts below STORED_TARGET/2"
        );

        let tracker = MergeMemoryTracker::new();
        let config = CompactorConfig {
            max_l1_part_bytes: STORED_TARGET,
            merge_memory_tracker: Some(tracker.clone()),
            ..CompactorConfig::default()
        };
        let clock = FixedClock::new(sealed_now_ns());
        compact_bucket(store.as_ref(), &clock, &config, &bucket())
            .await
            .expect("compact");

        let (rec, parts) = read_output(store.as_ref()).await;
        assert!(
            parts.len() >= 4,
            "fixture must split several times, got {}",
            parts.len()
        );
        assert_eq!(
            tracker.memory_target_flushes(),
            0,
            "no part may close on the memory split target for this corpus"
        );
        assert_eq!(
            tracker.stored_target_flushes() as usize,
            parts.len() - 1,
            "every closed part (all but the trailing one) closed on the stored target"
        );

        // The geometry band: every non-final part reached the target, and no
        // part -- trailing one included -- ran past the scheduler's overshoot
        // bound. The upper edge is the probe-spacing floor plus the proxy charge
        // of one record (the granularity at which a probe can fire), computed
        // from the fixture rather than guessed; the lower edge is the target
        // itself, which the old proxy-close (object ~ STORED_TARGET / ratio)
        // could not clear.
        let max_record_charge = estimate_stored_record(&ratio_record(0))
            + estimate_stored_stream(&ratio_record(0).stream_attrs);
        let band_top = STORED_TARGET + PROBE_MIN_STEP_BYTES + max_record_charge;
        println!(
            "[geom:rlog #872] target={STORED_TARGET}B parts={} probes={} band_top={band_top}B \
             ratio={ratio:.2} sizes={:?}",
            parts.len(),
            tracker.probes_run(),
            rec.parts.iter().map(|p| p.object_size).collect::<Vec<_>>()
        );
        // The probe cost, pinned exactly (the corpus is deterministic) and cross
        // checked against the geometric model in the doc comment above:
        // r = 7.08, d0 = STORED_TARGET * (1 - 1/r) = 14_069, ladder
        // ln(14069/4096) / ln(7.08/6.08) = 8.1, floor tail r = 7.1, crossing
        // probe 1, so 16.2 per part; 11 closing parts = 178, plus the trailing
        // part's partial ladder = 191 pinned.
        assert_eq!(
            tracker.probes_run(),
            191,
            "the exact-encode probe count for this fixture is deterministic; the \
             geometric model predicts about 16.2 probes per part for r={ratio:.2} \
             over {} parts",
            parts.len()
        );
        let last = parts.len() - 1;
        for (i, p) in rec.parts.iter().enumerate() {
            assert!(
                p.object_size > 0 && p.object_size <= band_top,
                "part {i} of {} bytes ran past the overshoot bound {band_top} \
                 (= {STORED_TARGET} + {PROBE_MIN_STEP_BYTES} + {max_record_charge})",
                p.object_size
            );
            if i != last {
                assert!(
                    p.object_size >= STORED_TARGET,
                    "part {i} of {} bytes is below {STORED_TARGET} -- the stored \
                     target must close on real object bytes, not {}x-smaller payload",
                    p.object_size,
                    ratio as u64
                );
            }
        }

        // Conservation and decode-equality (issue #872 deliverable 5): moving
        // part boundaries changes content hashes legitimately but never adds,
        // drops, or reorders a record. The split run's decoded record sequence
        // must equal a single-part run of the same inputs.
        let mut split_rows: Vec<LogRecord> = Vec::new();
        for p in &parts {
            split_rows.extend(decode_all(p));
        }
        assert_eq!(
            split_rows.len() as u64,
            total,
            "every record survives the split exactly once"
        );
        let baseline = ratio_fixture_single_part(PER_INPUT, INPUTS).await;
        assert_eq!(
            split_rows, baseline,
            "the decoded record sequence must be identical whatever the split"
        );
    }

    /// Body bytes every [`collapsing_record`] carries, compressible or not. Equal
    /// widths keep the payload proxy advancing at exactly the same rate on both
    /// sides of the switch, so the fixture's only variable is how many object
    /// bytes those proxy bytes turn into. 13 * 20: the filler's unit width.
    const COLLAPSE_BODY_BYTES: usize = 260;

    /// Record `i` of a corpus whose compressibility COLLAPSES at ordinal
    /// `prefix`: below it a repeated filler zstd takes to almost nothing, from it
    /// pseudo-random alphanumerics that do not compress.
    ///
    /// This is the adversarial shape for the probe scheduler. A rate model fitted
    /// on the prefix concludes the part needs a very large number of further
    /// payload bytes to reach the stored target; if the next probe may be aimed
    /// that far ahead, every record of the incompressible suffix in between lands
    /// in the same part.
    fn collapsing_record(i: i64, prefix: i64) -> LogRecord {
        let body = if i < prefix {
            "compressible-".repeat(COLLAPSE_BODY_BYTES / 13)
        } else {
            incompressible_pad(i as u64, COLLAPSE_BODY_BYTES)
        };
        record(0, SPLIT_BASE_NS + i, &body, Vec::new())
    }

    /// Encode `recs` exactly as the merge encodes a part, and return
    /// `(payload proxy bytes, object bytes)` for them.
    fn proxy_and_object_bytes(recs: &[LogRecord]) -> (u64, u64) {
        let proxy: u64 = recs.iter().map(estimate_stored_record).sum::<u64>()
            + recs
                .first()
                .map_or(0, |r| estimate_stored_stream(&r.stream_attrs));
        let identity = compactor_identity(&bucket(), &CompactorConfig::default());
        let mut w = RlogWriter::new(RlogConfig::default(), identity);
        for r in recs {
            w.push(r.clone()).expect("push");
        }
        let object = w
            .finish_compacted(1, vec![0u8; 32], 0)
            .expect("encode")
            .len() as u64;
        (proxy, object)
    }

    /// Issue #872 finding 2: the stored target's overshoot is bounded by the
    /// scheduler's DESIGN, not by a corpus happening to compress uniformly.
    ///
    /// A rate model on its own only says how far ahead the next probe should sit
    /// to be worth running; it puts no ceiling on the object that probe then
    /// measures. Fitted on a compressible prefix, the step it asks for is tens of
    /// times the remaining deficit, and an incompressible suffix inside that step
    /// closes a part many times over the target. What bounds it is that
    /// [`PartBuilder::schedule_next_probe`] spends the remaining ENCODED deficit
    /// as a PROXY distance and models no rate at all: the step assumes only that
    /// encoded bytes grow at most 1:1 with the proxy over the closing interval,
    /// so the encoded size cannot pass the target by more than
    /// [`PROBE_MIN_STEP_BYTES`] plus the proxy charge of the record that crosses
    /// -- the same band the uniform fixture asserts, now over a corpus on which
    /// any fitted rate is wrong.
    ///
    /// Demonstrated red against the uncapped rate-model scheduler this replaced:
    /// substitute an uncapped cumulative-rate step in `schedule_next_probe` --
    /// `let rate = (encoded_now as f64 / self.stored_estimate.max(1) as f64)
    /// .max(f64::MIN_POSITIVE); let step = (((deficit as f64) / rate).ceil() as
    /// u64).max(PROBE_MIN_STEP_BYTES);` in place of
    /// `let step = deficit.max(PROBE_MIN_STEP_BYTES);` -- and the probe pin below
    /// fails first at 85 against 124, then, with the pin relaxed to 85, this
    /// fixture emits 29 parts whose first object is 150_646 bytes, 9.2x the
    /// 16 KiB target, failing the band assertion at part 0. The remaining parts
    /// land between 16_853 and 18_893 bytes, inside the band, which is why the
    /// assertion covers every part and not just the largest. The uniformly
    /// compressible corpus of
    /// [`stored_target_closes_parts_on_actual_encoded_object_bytes`] passes the
    /// band under both schedulers, which is exactly why this fixture exists.
    #[tokio::test]
    async fn stored_target_overshoot_is_bounded_when_compressibility_collapses() {
        const PER_INPUT: i64 = 2000;
        const INPUTS: i64 = 2;
        /// Ordinal where the corpus stops compressing. Enough compressible
        /// payload ahead of it that the first part's early probes all land inside
        /// it and fit a low rate (its 200 records charge the proxy 56_061 bytes,
        /// 3.4 targets' worth, and encode to 1_220), and thousands of incompressible
        /// records after it for that rate to be wrong about.
        const PREFIX: i64 = 200;
        const STORED_TARGET: u64 = 16 * 1024;
        let total = (PER_INPUT * INPUTS) as u64;

        let store = Arc::new(MemoryStore::new());
        for j in 0..INPUTS {
            let recs: Vec<LogRecord> = (0..PER_INPUT)
                .map(|i| collapsing_record(i * INPUTS + j, PREFIX))
                .collect();
            seed(
                store.as_ref(),
                Uuid::from_u128(j as u128 + 1),
                j as u64 + 1,
                &recs,
            )
            .await;
        }

        // The collapse is real, measured in the quantity the scheduler fits: the
        // encoded bytes each phase produces per payload proxy byte. Both samples
        // are the same width and the same record count, so only compressibility
        // differs.
        const SAMPLE: i64 = 200;
        let pre: Vec<LogRecord> = (0..SAMPLE).map(|i| collapsing_record(i, PREFIX)).collect();
        let post: Vec<LogRecord> = (PREFIX..PREFIX + SAMPLE)
            .map(|i| collapsing_record(i, PREFIX))
            .collect();
        let (pre_proxy, pre_object) = proxy_and_object_bytes(&pre);
        let (post_proxy, post_object) = proxy_and_object_bytes(&post);
        assert_eq!(
            pre_proxy, post_proxy,
            "both phases must charge the same proxy"
        );
        let pre_rate = pre_object as f64 / pre_proxy as f64;
        let post_rate = post_object as f64 / post_proxy as f64;
        println!(
            "[geom:rlog #872] collapse proxy={pre_proxy}B pre_object={pre_object}B \
             post_object={post_object}B pre_rate={pre_rate:.4} post_rate={post_rate:.4}"
        );
        assert!(
            post_rate > 10.0 * pre_rate,
            "fixture must collapse: pre_rate {pre_rate:.4} vs post_rate {post_rate:.4} is \
             only {:.1}x, too flat to break a rate model",
            post_rate / pre_rate
        );

        let tracker = MergeMemoryTracker::new();
        let config = CompactorConfig {
            max_l1_part_bytes: STORED_TARGET,
            merge_memory_tracker: Some(tracker.clone()),
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
            "the memory split target is at its 256 MiB default and must not fire"
        );
        assert_eq!(
            tracker.stored_target_flushes() as usize,
            parts.len() - 1,
            "every closed part must have closed on the stored target"
        );
        // The probe cost, pinned exactly rather than as `> 0`: the corpus is
        // deterministic, so the geometric model is checkable against it. Almost
        // every part of this fixture lies past the collapse, where
        // r = 1 / post_rate = 1.59, so d0 = STORED_TARGET * (1 - 1/r) = 6_072,
        // the ladder to the floor is ln(6072/4096) / ln(1.59/0.59) = 0.4 probes,
        // the floor tail is r = 1.6, and the crossing probe makes 3.0 per part:
        // 40 closing parts = 120, plus the first part's longer ladder across the
        // compressible prefix (r = 1/pre_rate = 46 there) = 124 pinned.
        assert_eq!(
            tracker.probes_run(),
            124,
            "the exact-encode probe count for this fixture is deterministic; the \
             geometric model predicts about 3.0 probes per part for \
             r={:.2} past the collapse, over {} parts",
            1.0 / post_rate,
            parts.len()
        );

        // The band, identical to the uniform fixture's because it is the
        // scheduler's guarantee and not the corpus's: the probe-spacing floor
        // plus one record's proxy charge above the target. Every part is checked,
        // trailing one included.
        let max_record_charge = estimate_stored_record(&collapsing_record(PREFIX, PREFIX))
            + estimate_stored_stream(&collapsing_record(0, PREFIX).stream_attrs);
        let band_top = STORED_TARGET + PROBE_MIN_STEP_BYTES + max_record_charge;
        let sizes: Vec<u64> = rec.parts.iter().map(|p| p.object_size).collect();
        let largest = sizes.iter().copied().max().unwrap_or(0);
        println!(
            "[geom:rlog #872] collapse target={STORED_TARGET}B parts={} probes={} \
             band_top={band_top}B largest={largest}B",
            parts.len(),
            tracker.probes_run()
        );
        assert!(
            parts.len() >= 4,
            "fixture must split several times, got {}",
            parts.len()
        );
        let last = parts.len() - 1;
        for (i, p) in rec.parts.iter().enumerate() {
            assert!(
                p.object_size > 0 && p.object_size <= band_top,
                "part {i} of {} bytes ran past the overshoot bound {band_top} \
                 (= {STORED_TARGET} + {PROBE_MIN_STEP_BYTES} + {max_record_charge}); \
                 sizes {sizes:?}",
                p.object_size
            );
            if i != last {
                assert!(
                    p.object_size >= STORED_TARGET,
                    "non-final part {i} of {} bytes did not reach the target",
                    p.object_size
                );
            }
        }

        // Content is conserved across the collapse boundary too.
        let mut split_rows: Vec<LogRecord> = Vec::new();
        for p in &parts {
            split_rows.extend(decode_all(p));
        }
        assert_eq!(
            split_rows.len() as u64,
            total,
            "every record survives the split exactly once"
        );
    }

    /// ADR-0979 / issue #872: the exact-encode probe's residency is CHARGED to
    /// the tracker, so a stored-target-bound run's peak is the real peak instead
    /// of the writer buffer alone.
    ///
    /// [`PartBuilder::encode_clone`] clones the part's buffered records and
    /// encodes the clone, so at the instant the probe returns the run holds the
    /// writer's records, a second copy of them inside the writer the probe built,
    /// and the encoded object those records produced. That is roughly
    /// `2 * W + object bytes` where `W` is the part's record-heap estimate at the
    /// probe, not the `W` a probe-free run reports, and
    /// [`MergeMemoryTracker::peak_total_bytes`] has to cover it for a host to be
    /// sized from it.
    ///
    /// Asserted proportionally against the tracker's own `peak_writer_bytes`,
    /// which is `W` measured at the same close, so the claim needs no
    /// hand-computed heap figure: the tracked peak must be at least 2x it (the
    /// clone), and is expected under 3x it (the clone plus one object, an object
    /// being a small fraction of the heap on this fixture -- the printed figures
    /// give the exact multiple). The probe term alone is asserted to carry at
    /// least the clone and at most the clone plus one band-width object.
    ///
    /// Demonstrated red by removing the charge: delete the two
    /// `t.set_probe_bytes(part.estimate...)` calls in `PartSink::push` (keeping
    /// the `t.set_probe_bytes(0)` and the counter), and `peak_probe_bytes()`
    /// reads 0 -- failing the clone assertion below -- while `peak_total_bytes()`
    /// falls from 486_654 to 252_714 against a `peak_writer_bytes()` of 217_189,
    /// a 1.16x multiple that also misses the 2x-to-3x band.
    #[tokio::test]
    async fn probe_residency_is_charged_to_the_tracked_peak() {
        const PER_INPUT: i64 = 1200;
        const INPUTS: i64 = 2;
        const STORED_TARGET: u64 = 16 * 1024;

        let store = Arc::new(MemoryStore::new());
        for j in 0..INPUTS {
            let recs: Vec<LogRecord> = (0..PER_INPUT)
                .map(|i| ratio_record(i * INPUTS + j))
                .collect();
            // Small L0 blocks so the decode-side terms `peak_total_bytes` pools
            // in stay a fraction of one part's heap, and the multiple below is
            // about the writer and the probe rather than about cursors. Blocking
            // changes nothing about the merged record sequence, so the split
            // points and the probe count are the same as with default blocking.
            seed_l0(
                store.as_ref(),
                Uuid::from_u128(j as u128 + 1),
                j as u64 + 1,
                &recs,
                RlogConfig {
                    block_target_records: 40,
                    group_target_blocks: 1,
                    ..RlogConfig::default()
                },
                &[],
            )
            .await;
        }

        let tracker = MergeMemoryTracker::new();
        let config = CompactorConfig {
            max_l1_part_bytes: STORED_TARGET,
            merge_memory_tracker: Some(tracker.clone()),
            ..CompactorConfig::default()
        };
        let clock = FixedClock::new(sealed_now_ns());
        compact_bucket(store.as_ref(), &clock, &config, &bucket())
            .await
            .expect("compact");

        let (_rec, parts) = read_output(store.as_ref()).await;
        assert!(
            parts.len() >= 3,
            "the fixture must close several parts on the stored target, got {}",
            parts.len()
        );
        let peak_total = tracker.peak_total_bytes();
        let peak_writer = tracker.peak_writer_bytes();
        let peak_probe = tracker.peak_probe_bytes();
        let multiple = peak_total as f64 / peak_writer.max(1) as f64;
        println!(
            "[memory:rlog #872] parts={} probes={} peak_writer={peak_writer}B \
             peak_probe={peak_probe}B peak_total={peak_total}B multiple={multiple:.2}x",
            parts.len(),
            tracker.probes_run()
        );
        // Deterministic corpus, so the probe count is pinned here too: r = 7.08
        // as in `stored_target_closes_parts_on_actual_encoded_object_bytes`
        // (same `ratio_record` fixture), 16.2 probes per part by the geometric
        // model, 5 closing parts = 81 plus the trailing part's partial ladder =
        // 89. Under the rate-model scheduler that test names it runs 16.
        assert_eq!(
            tracker.probes_run(),
            89,
            "the probe count is deterministic for this corpus: 16.2 per part by \
             the geometric model over the {} closing parts",
            parts.len() - 1
        );
        assert!(
            peak_writer > 0,
            "the run must have buffered records for the charge to mean anything"
        );
        assert!(
            peak_probe >= peak_writer,
            "the probe term must carry at least the cloned record heap: \
             peak_probe={peak_probe} peak_writer={peak_writer}"
        );
        assert!(
            peak_probe <= peak_writer + 2 * STORED_TARGET,
            "the probe term must be one clone plus one object, not more: \
             peak_probe={peak_probe} peak_writer={peak_writer}"
        );
        assert!(
            (2.0..3.0).contains(&multiple),
            "a probing run's tracked peak must cover the probe's second copy of \
             the part heap: peak_total={peak_total} is {multiple:.2}x \
             peak_writer={peak_writer}, expected 2x to 3x"
        );
        assert_eq!(
            tracker.phase_peaks().probe_bytes,
            peak_probe,
            "the operator-facing phase split must carry the probe term the \
             total already includes"
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

    /// A part's STREAM_DIR bytes must count toward the payload proxy that
    /// schedules the stored-target probe, so a bucket of many streams with large
    /// resource blobs splits on those blobs rather than making one giant object.
    ///
    /// `estimate_stored_record` excludes `stream_attrs`, correctly: the blob is
    /// stored once per stream, not once per record. If nothing charged it per
    /// stream either, the proxy would stay near the tiny sum of this fixture's
    /// few-byte records and never reach the target, so the exact-encode probe
    /// would never be scheduled and the merge would run every stream's blob into
    /// one object. [`estimate_stored_stream`] charging each distinct stream once
    /// keeps the proxy tracking the object it schedules a probe for.
    ///
    /// The fat blobs are incompressible (pseudo-random), so the object cannot
    /// compress far below its STREAM_DIR bytes: each non-final part therefore
    /// closes AT the target (the probe measures real object bytes) and stays
    /// under twice it.
    ///
    /// Demonstrated red by under-charging: gate the STREAM_DIR charge in
    /// `PartBuilder::push` to the first stream only (`&& self.charged_streams
    /// .len() == 1`). The proxy then stays a few KiB over the whole bucket, never
    /// reaches the 64 KiB target, no probe is ever scheduled, and the merge emits
    /// a single ~270 KiB object, failing the `object_size < 2 * STORED_TARGET`
    /// assertion.
    ///
    /// The probe count is pinned exactly rather than left printed, and it is the
    /// scheduler's figure, not the charge's: under the rate-model scheduler named
    /// in [`stored_target_overshoot_is_bounded_when_compressibility_collapses`]
    /// this fixture runs 8 probes, not 15.
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

        let charge = estimate_stored_stream(&fat_stream_record(0, 0).stream_attrs);
        let per_record = estimate_stored_record(&fat_stream_record(0, 0));
        assert!(
            charge > per_record * 100,
            "fixture must be STREAM_DIR-dominated: charge {charge} vs record {per_record}"
        );
        // The fixture's proxy-to-object ratio, measured over one part's worth of
        // distinct streams, for the probe-count arithmetic below. The pads are
        // pseudo-random alphanumerics, so `r` sits just above 1.
        let sample: Vec<LogRecord> = (0..20).map(|s| fat_stream_record(s, 0)).collect();
        let sample_proxy: u64 = sample.iter().map(estimate_stored_record).sum::<u64>()
            + sample
                .iter()
                .map(|r| estimate_stored_stream(&r.stream_attrs))
                .sum::<u64>();
        let (_, sample_object) = proxy_and_object_bytes(&sample);
        let ratio = sample_proxy as f64 / sample_object as f64;

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
        assert!(
            parts.len() >= 5,
            "fixture must split several times, got {}",
            parts.len()
        );
        assert_eq!(
            tracker.memory_target_flushes(),
            0,
            "no part may close on the memory split target for this corpus"
        );
        assert_eq!(
            tracker.stored_target_flushes() as usize,
            parts.len() - 1,
            "every closed part (all but the trailing one) closed on the stored target"
        );
        // Probes pinned exactly, not left unasserted: the corpus is
        // deterministic, and the figure agrees with the scheduler's geometric
        // cost model (see `PartBuilder::schedule_next_probe`). r = 1.48 measured
        // above, d0 = STORED_TARGET * (1 - 1/r) = 21_257, ladder
        // ln(21257/4096) / ln(1.48/0.48) = 1.5, floor tail r = 1.5, crossing
        // probe 1, so 3.9 per part; the 4 closing parts predict 15.8 against 15
        // measured, and the trailing part never probes (its 14_400-byte object
        // leaves the proxy short of the 64 KiB target).
        println!(
            "[geom:rlog #872] many-streams target={STORED_TARGET}B parts={} probes={} \
             ratio={ratio:.2} sizes={:?}",
            parts.len(),
            tracker.probes_run(),
            rec.parts.iter().map(|p| p.object_size).collect::<Vec<_>>()
        );
        assert_eq!(
            tracker.probes_run(),
            15,
            "the exact-encode probe count for this fixture is deterministic; the \
             geometric model predicts 15.8 for r={ratio:.2} over the {} closing parts",
            parts.len() - 1
        );
        // The probe measures the object it names: with the STREAM_DIR charge
        // scheduling it, each non-final part reaches the target and none runs
        // past twice it; uncharged, one part would carry every stream's blob.
        let last = parts.len() - 1;
        for (i, p) in rec.parts.iter().enumerate() {
            assert!(
                p.object_size < 2 * STORED_TARGET,
                "part {i} of {} bytes exceeded twice the {STORED_TARGET}-byte stored target",
                p.object_size
            );
            if i != last {
                assert!(
                    p.object_size >= STORED_TARGET,
                    "non-final part {i} of {} bytes did not reach the {STORED_TARGET}-byte target",
                    p.object_size
                );
            }
        }

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

    /// Issue #872: at the shipped defaults NO exact-encode probe runs. The
    /// "geometry is unchanged" argument for shipping equal targets rests on that
    /// claim, and the equal-geometry test above cannot see it: a probe that runs
    /// and finds the part short changes no object byte, it just spends an O(part)
    /// encode. So this asserts the probe counter directly.
    ///
    /// The relationship under test is the shipped one -- both targets equal --
    /// scaled to 64 KiB each so a small corpus reaches it. Why it holds is the
    /// two estimators, not this corpus: `estimate_record` charges strictly more
    /// per record than `estimate_stored_record`, term for term (the same payload
    /// lengths plus `ALLOC_OVERHEAD_BYTES` per allocation, `RECORD_SLOT_BYTES` =
    /// `size_of::<LogRecord>()` against the proxy's flat
    /// `STORED_RECORD_FIXED_BYTES`, and a per-record copy of `stream_attrs` the
    /// proxy charges once per stream instead). With equal targets the heap sum
    /// therefore crosses first on every record shape, the part closes on the
    /// memory split target, the proxy never reaches `max_l1_part_bytes`, and the
    /// probe is never even scheduled. The per-record figures are asserted below
    /// on two fixtures of opposite shape (wide rows, and the compressible
    /// single-stream row the stored-target tests use), so an estimator change
    /// that inverts the inequality fails here rather than silently starting to
    /// probe at the defaults.
    ///
    /// Demonstrated red by probing early: in `PartSink::push`, relax the probe
    /// gate to `part.stored_estimate >= self.config.max_l1_part_bytes / 8`. The
    /// geometry is untouched -- 15 parts and 14 memory-target closes either way,
    /// since a probe that finds the part short changes nothing -- and
    /// `probes_run` goes from 0 to 14, so the counter assertion below is the only
    /// thing in the suite that catches it.
    #[tokio::test]
    async fn no_probe_runs_when_both_targets_are_equal() {
        const PER_INPUT: i64 = 400;
        const INPUTS: i64 = 2;
        /// The shipped defaults are equal (256 MiB each); this is that same
        /// relationship at a size an 800-record corpus reaches.
        const BOTH: u64 = 64 * 1024;
        assert_eq!(
            crate::config::DEFAULT_MAX_L1_PART_BYTES,
            crate::config::DEFAULT_L1_PART_MEMORY_TARGET_BYTES,
            "this test stands in for the shipped defaults, which are equal"
        );

        for (name, mk) in [
            ("wide", wide_record as fn(i64) -> LogRecord),
            ("ratio", ratio_record as fn(i64) -> LogRecord),
        ] {
            let heap = estimate_record(&mk(0));
            let stored =
                estimate_stored_record(&mk(0)) + estimate_stored_stream(&mk(0).stream_attrs);
            assert!(
                heap > stored,
                "{name}: the memory target can only fire first if the heap charge {heap} \
                 exceeds the proxy charge {stored} per record"
            );
        }

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

        let tracker = MergeMemoryTracker::new();
        let config = CompactorConfig {
            l1_part_memory_target_bytes: BOTH,
            max_l1_part_bytes: BOTH,
            merge_memory_tracker: Some(tracker.clone()),
            ..CompactorConfig::default()
        };
        let clock = FixedClock::new(sealed_now_ns());
        compact_bucket(store.as_ref(), &clock, &config, &bucket())
            .await
            .expect("compact");

        let (_rec, parts) = read_output(store.as_ref()).await;
        assert!(parts.len() > 1, "the fixture must split");
        assert_eq!(
            tracker.memory_target_flushes() as usize,
            parts.len() - 1,
            "every closed part must have closed on the memory split target"
        );
        assert_eq!(
            tracker.stored_target_flushes(),
            0,
            "the stored target must not fire when the targets are equal"
        );
        assert_eq!(
            tracker.probes_run(),
            0,
            "no exact-encode probe may run when the memory target fires first: \
             {} parts, {} memory flushes",
            parts.len(),
            tracker.memory_target_flushes()
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
    ///
    /// Run at BOTH split targets (issue #872), because the two close paths write
    /// the object differently: a memory-target close consumes the builder's
    /// records into a fresh writer, while a stored-target close reuses the bytes
    /// an exact-encode probe already produced. Byte-identity across concurrency
    /// has to hold on both, and it is the probe-reuse path -- where the object
    /// comes from an encode taken at a moment the concurrency could in principle
    /// influence -- that most needs saying. Each run asserts which target fired,
    /// so neither config can silently fall back to the other and cover the same
    /// path twice: give the `SplitOn::Memory` arm the stored config
    /// (`max_l1_part_bytes: 8 * 1024`) and it fails with "memory mode closed 0 on
    /// memory and 3 on stored" rather than passing on a duplicated path.
    #[tokio::test]
    async fn input_read_concurrency_changes_timing_not_bytes() {
        const INPUTS: u64 = 16;

        /// Which target the run under test closes its parts on.
        #[derive(Copy, Clone, Debug)]
        enum SplitOn {
            /// The memory split target: closes by consuming the builder.
            Memory,
            /// The stored-size target: closes on the bytes a probe measured.
            Stored,
        }

        async fn compact_at(concurrency: usize, split: SplitOn) -> (Vec<[u8; 32]>, usize) {
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
            // Wide records are far larger in heap than on the object, so a 64 KiB
            // memory target and an 8 KiB stored target each split this corpus
            // several times while leaving the other target far away, which is
            // what the flush-counter assertions below check.
            let tracker = MergeMemoryTracker::new();
            let config = match split {
                SplitOn::Memory => CompactorConfig {
                    l1_part_memory_target_bytes: 64 * 1024,
                    input_read_concurrency: concurrency,
                    merge_memory_tracker: Some(tracker.clone()),
                    ..CompactorConfig::default()
                },
                SplitOn::Stored => CompactorConfig {
                    max_l1_part_bytes: 8 * 1024,
                    input_read_concurrency: concurrency,
                    merge_memory_tracker: Some(tracker.clone()),
                    ..CompactorConfig::default()
                },
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
            // The run really closed its parts on the target this config names,
            // so neither mode can quietly exercise the other's close path.
            let (memory, stored) = (
                tracker.memory_target_flushes(),
                tracker.stored_target_flushes(),
            );
            match split {
                SplitOn::Memory => assert!(
                    memory > 0 && stored == 0,
                    "memory mode closed {memory} on memory and {stored} on stored"
                ),
                SplitOn::Stored => assert!(
                    stored > 0 && memory == 0,
                    "stored mode closed {stored} on stored and {memory} on memory"
                ),
            }
            let hashes = parts
                .iter()
                .map(|p| *blake3::hash(p).as_bytes())
                .collect::<Vec<_>>();
            (hashes, peak_in_flight)
        }

        for split in [SplitOn::Memory, SplitOn::Stored] {
            let (serial_hashes, serial_in_flight) = compact_at(1, split).await;
            let (concurrent_hashes, concurrent_in_flight) = compact_at(8, split).await;

            assert_eq!(
                serial_in_flight, 1,
                "{split:?}: concurrency 1 must never have two commit-record GETs in flight"
            );
            assert_eq!(
                concurrent_in_flight, 8,
                "{split:?}: concurrency 8 must hold exactly 8 commit-record GETs at once"
            );
            assert!(
                serial_hashes.len() > 1,
                "{split:?}: the fixture must split into several parts, got {}",
                serial_hashes.len()
            );
            assert_eq!(
                serial_hashes, concurrent_hashes,
                "{split:?}: concurrent input reads must produce byte-identical parts"
            );
        }
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
    /// and the raw term is one row group of compressed bytes. Since ADR-0979
    /// decision 1 that decoded block is held columnar and its charge is the
    /// view's `heap_estimate()`, so the same bound is stated over a smaller
    /// per-record figure (see `PER_RECORD_BOUND`).
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
    /// Demonstrated red against the row-form cursor ADR-0979 decision 1
    /// replaced: charging `decode_block_in_group(&loc, block, ..)`'s
    /// `estimate_record` sum in `StreamCursor::decode_next_block` instead of
    /// the columnar view's `heap_estimate()` -- the shape where the cursor
    /// holds the block's records -- raises the decoded term to the row form
    /// (transient 726480 B against the 498000 B bound), failing
    /// `transient < TRANSIENT_BOUND`. The earlier red, against the
    /// per-row-group decode issue #748 replaced, was the same assertion at the
    /// old bound: decoding a whole 2-block group doubled the term.
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
        // Per-record upper bound on the decoded term. Since ADR-0979 decision 1
        // a cursor holds its block COLUMNAR and the term is the block's
        // `heap_estimate()`, which for these tiny records works out at ~108
        // bytes per row against the ~242 the row form estimated. The bound is
        // resized to match and stays deliberately close to it: a cursor that
        // went back to holding the block's records has to break it.
        const PER_RECORD_BOUND: u64 = 150;
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

    // --- ADR-0979 D1 differential part-hash fixture ---------------------------

    /// The ts every stream carries in ALL THREE differential inputs, chosen
    /// outside every input's own grid so no input holds two records at it: the
    /// only tie it creates is the cross-input one the merge resolves by
    /// `input_index`.
    const TRIPLE_TIE_TS: i64 = 1001;

    /// The differential fixture's three L0 inputs, in canonical input order.
    ///
    /// Every stream is carried by all three. `a` and `b` share an even ts grid
    /// and their ranges straddle (a covers `[0, 58]`, b covers `[30, 88]`), so
    /// every even ts in `[30, 58]` is a two-way cross-input tie with different
    /// bodies; `c` uses the odd grid `[1, 79]` and interleaves both without
    /// tying; and every stream carries one record at [`TRIPLE_TIE_TS`] in all
    /// three, a three-way tie. A wrong tie-break, or a materialization that
    /// yields a block's rows in any other order, reorders records that differ
    /// in their bodies and changes the part bytes.
    fn differential_hash_inputs() -> [Vec<LogRecord>; 3] {
        let mk = |tag: &str, ks: std::ops::Range<i64>, odd: i64| -> Vec<LogRecord> {
            let mut v = Vec::new();
            for s in 0..4u32 {
                for k in ks.clone() {
                    v.push(record(
                        s,
                        k * 2 + odd,
                        &format!("{tag}-{s}-{k:02}"),
                        vec![
                            ("svc".into(), AttrValue::Str(format!("v{}", k % 3))),
                            ("seq".into(), AttrValue::I64(k)),
                        ],
                    ));
                }
                v.push(record(
                    s,
                    TRIPLE_TIE_TS,
                    &format!("{tag}-tie-{s}"),
                    vec![("svc".into(), AttrValue::Str("tie".into()))],
                ));
            }
            v
        };
        [mk("a", 0..30, 0), mk("b", 15..45, 0), mk("c", 0..40, 1)]
    }

    /// Blocking for the differential fixture's inputs: 8 records per block, 2
    /// blocks per row group. A stream then spans several blocks and several row
    /// groups per input, so the cursor crosses both a block boundary and a loc
    /// boundary mid-stream, and a block on a stream boundary carries the
    /// neighbouring stream's rows too (the `stream_ref` filter).
    fn differential_l0_cfg() -> RlogConfig {
        RlogConfig {
            block_target_records: 8,
            group_target_blocks: 2,
            ..RlogConfig::default()
        }
    }

    /// Seed [`differential_hash_inputs`], compact, and return each L1 part's
    /// `content_hash` as hex plus the total record count over the parts.
    async fn differential_hash_run() -> (Vec<String>, usize) {
        let store = MemoryStore::new();
        for (i, recs) in differential_hash_inputs().iter().enumerate() {
            seed_l0(
                &store,
                Uuid::from_u128(i as u128 + 1),
                i as u64 + 1,
                recs,
                differential_l0_cfg(),
                &["svc"],
            )
            .await;
        }
        let config = CompactorConfig {
            // Small enough that the bucket splits into several parts, so the
            // pin covers part boundaries and `part_index`, not one object. Split
            // on the memory target: it is unaffected by the issue #872 stored-
            // target change, so this pin stays reproducible independently of
            // object-geometry knobs.
            l1_part_memory_target_bytes: 32 * 1024,
            ..CompactorConfig::default()
        };
        let clock = FixedClock::new(sealed_now_ns());
        compact_bucket(&store, &clock, &config, &bucket())
            .await
            .expect("compact");
        let (recrd, parts) = read_output(&store).await;
        // The pin is on the record's own content_hash field, not on a hash
        // recomputed here: that is the value every downstream key and repair
        // check is built from.
        let hashes: Vec<String> = recrd
            .parts
            .iter()
            .map(|p| hex::encode(&p.content_hash))
            .collect();
        for (p, bytes) in recrd.parts.iter().zip(parts.iter()) {
            assert_eq!(
                hex::encode(p.content_hash.as_slice()),
                hex::encode(blake3::hash(bytes).as_bytes()),
                "the record's content_hash must be the part object's hash"
            );
        }
        let rows: usize = parts.iter().map(|p| decode_all(p).len()).sum();
        (hashes, rows)
    }

    /// A pinned byte-stability guard: this exact fixture and config must keep
    /// producing the same part `content_hash` vector, so an accidental change to
    /// the merge, the writer pipeline, or the split logic shows up as a diff
    /// here.
    ///
    /// The pin covers part boundaries and `part_index`, not one object:
    /// [`differential_hash_inputs`] fixes the two-way and three-way equal-ts
    /// tie-break order in the record bodies, [`differential_l0_cfg`] makes each
    /// stream span several blocks and several row groups per input with
    /// stream-boundary blocks, and the run splits into several parts on the
    /// memory split target.
    ///
    /// The vector is a RE-CAPTURE, not a regeneration from the code under
    /// review. The original pin was taken under `max_l1_part_bytes: 4096`, a
    /// stored-target split, and issue #872 moved every stored-target boundary by
    /// closing on actual encoded bytes instead of the payload proxy, so those
    /// constants are genuinely unreproducible. Rather than accept whatever the
    /// new code prints, the fixture was ported back onto commit 76c90a3 -- the
    /// last commit before the ADR-0979 D1 columnar-cursor swap, where
    /// `l1_part_memory_target_bytes` already existed -- in a scratch checkout,
    /// run there with the config below (`l1_part_memory_target_bytes: 32 *
    /// 1024`, `max_l1_part_bytes` at its 256 MiB default, which this corpus's
    /// payload proxy never approaches), and the six hashes it printed there are
    /// the six constants below. So this still compares the current merge against
    /// the pre-D1 path's real bytes, now over the memory-target boundaries: the
    /// memory-target close, the trailing-part close, and every part in between,
    /// which is what the #872 change moved onto records plus a fresh writer at
    /// close. The comparison is meaningful because `ravel-logseg` (the frozen
    /// writer that turns a record set into bytes) has no commits between 76c90a3
    /// and here, so a diff can only come from this crate.
    ///
    /// The stored-target geometry #872 introduced is pinned separately, by
    /// [`stored_target_closes_parts_on_actual_encoded_object_bytes`] (band plus
    /// decode-equality against a single-part baseline) and
    /// [`stored_target_overshoot_is_bounded_when_compressibility_collapses`].
    #[tokio::test]
    async fn differential_part_hashes_match_the_pre_columnar_cursor() {
        /// Total records seeded across the three inputs: 4 streams x
        /// (30 + 1) + 4 x (30 + 1) + 4 x (40 + 1).
        const EXPECTED_ROWS: usize = 4 * 31 + 4 * 31 + 4 * 41;
        /// Part `content_hash` values, in `part_index` order, as printed by this
        /// fixture ported onto commit 76c90a3 under
        /// `l1_part_memory_target_bytes: 32 * 1024` (see the note above).
        const EXPECTED_PART_HASHES: &[&str] = &[
            "853fb59210a344a2e656f6e1a29aa2feeeab0ec486373f3d37fd8975f74cb7ac",
            "dca4d7e8b9729de6c64f132fa1eb52028ab85de629d17e1f8b85d7b916c883a4",
            "6669e7d418d5e42b1bcd46e664c9b00503e3cbdbf6ed35e9ddd49d493e386459",
            "29d933b58b29a40ee80a1587ecf91021bdc747376131f86fcef8dfdca9b1679a",
            "f6c414a7fc62039e3000817c8ab65e36060bd20cee30fab61d1cb2eb4f166dd1",
            "5641cc650be9829ca9bfb88d203ca98872ddf4fd1149815d25ed8bb4ddd44e46",
        ];

        let (hashes, rows) = differential_hash_run().await;
        assert_eq!(
            rows, EXPECTED_ROWS,
            "every seeded record survived the merge"
        );
        assert_eq!(
            hashes, EXPECTED_PART_HASHES,
            "part content_hash vector diverged from the pre-columnar-cursor pin"
        );
    }

    // --- ADR-0979 D1 cursor-phase accounting ---------------------------------

    /// The data object key [`seed_l0`] PUT for `(writer_id, seq)`, recomputed
    /// from the bytes it returned exactly as it computed it.
    fn seeded_data_key(writer_id: Uuid, seq: u64, bytes: &Bytes) -> String {
        let content_hash: [u8; 32] = *blake3::hash(bytes).as_bytes();
        keys::data_key(
            &tenant_hash(),
            Signal::Logs,
            SHARD,
            writer_id,
            EPOCH,
            seq,
            &content_hash,
        )
        .expect("data key")
    }

    /// What one input's single candidate block costs a cursor, computed
    /// independently of the compaction run: the columnar view's
    /// `heap_estimate()` (what the cursor now holds) and the row-form
    /// `estimate_record` sum over the same block (what it used to hold, and
    /// what the tracker used to charge).
    async fn cursor_block_terms(store: &MemoryStore, object_key: String) -> (u64, u64) {
        // A tracker-free config: this reader is the test's own, and charging
        // its catalog load would disturb the figures under assertion.
        let catalog =
            load_catalog_from_object(store, &CompactorConfig::default(), object_key, true)
                .await
                .expect("catalog");
        let (stream_id, _) = stream_ident(0);
        let locs = catalog
            .reader
            .stream_blocks(&stream_id)
            .expect("stream blocks")
            .expect("input carries the stream");
        assert_eq!(locs.len(), 1, "fixture input is one row group");
        assert_eq!(
            locs[0].block_indices(),
            &[0],
            "fixture input is one block in that group"
        );
        let data = store
            .get(
                &catalog.object_key,
                GetRange::Range(locs[0].start(), locs[0].end()),
            )
            .await
            .expect("group bytes")
            .data;
        let heap = catalog
            .reader
            .block_rows_in_group(&locs[0], 0, data.as_ref())
            .expect("columnar view")
            .heap_estimate();
        let row_form: u64 = catalog
            .reader
            .decode_block_in_group(&locs[0], 0, data.as_ref())
            .expect("eager rows")
            .iter()
            .map(estimate_record)
            .sum();
        (heap, row_form)
    }

    /// ADR-0979 decision 1's accounting rebase: at its peak the cursor phase's
    /// decoded term is EXACTLY the sum of the live cursors'
    /// `StreamBlockRows::heap_estimate()`, and the row-form `estimate_record`
    /// term the pre-D1 cursor charged is gone rather than added to.
    ///
    /// The fixture pins the geometry so the expected value is arithmetic, not
    /// an observation: three inputs, one stream, one row group of one block
    /// each, and records identical in every column but `ts_ns`, so all three
    /// blocks cost the same. Their timestamps INTERLEAVE across the three inputs
    /// (input `j` carries `i * 3 + j`), so the three slices fully overlap in ts
    /// and overlap-gated admission (ADR-0979 decision 2) opens all three cursors
    /// at once -- the case where `D = n`, worst for admission and the one that
    /// keeps this a `3 x heap_estimate` peak. A block is released only when it is
    /// drained, so at the peak all three are resident and the peak is therefore
    /// `3 x heap_estimate` and nothing else.
    ///
    /// Demonstrated red by adding the old charge back beside the new one in
    /// `StreamCursor::decode_next_block` -- `let decoded_bytes =
    /// rows.heap_estimate() + reader.decode_block_in_group(&loc, block,
    /// data.as_ref())?.iter().map(estimate_record).sum::<u64>();` -- which
    /// makes the peak `3 x (heap_estimate + row_form)` and fails the equality
    /// below, the silent double-count the ADR names as this task's trap.
    #[tokio::test]
    async fn cursor_phase_charges_heap_estimate_once_per_open_cursor() {
        const INPUTS: u64 = 3;
        const ROWS_PER_INPUT: i64 = 6;

        let store = MemoryStore::new();
        let mut keys_seeded = Vec::new();
        for j in 0..INPUTS {
            // Identical in every column but ts_ns, so the three blocks are the
            // same shape and the same size.
            let recs: Vec<LogRecord> = (0..ROWS_PER_INPUT)
                .map(|i| {
                    record(
                        0,
                        // Interleave ts across the three inputs so their slices
                        // overlap and overlap-gated admission opens all three
                        // cursors at once (ADR-0979 decision 2).
                        i * (INPUTS as i64) + (j as i64),
                        "row",
                        vec![
                            ("svc".into(), AttrValue::Str("v".into())),
                            ("n".into(), AttrValue::I64(7)),
                        ],
                    )
                })
                .collect();
            let writer_id = Uuid::from_u128(u128::from(j) + 1);
            let seq = j + 1;
            let bytes = seed_l0(
                &store,
                writer_id,
                seq,
                &recs,
                RlogConfig::default(),
                &["svc"],
            )
            .await;
            keys_seeded.push(seeded_data_key(writer_id, seq, &bytes));
        }

        let mut terms = Vec::new();
        for key in keys_seeded {
            terms.push(cursor_block_terms(&store, key).await);
        }
        let (per_block, row_form) = terms[0];
        assert!(
            terms.iter().all(|&t| t == (per_block, row_form)),
            "fixture inputs must be identical in geometry, got {terms:?}"
        );
        assert!(
            row_form > 0,
            "the old row-form term must be nonzero, or \
             the double-charge below would be indistinguishable from the single charge"
        );

        let tracker = MergeMemoryTracker::new();
        let config = CompactorConfig {
            merge_memory_tracker: Some(tracker.clone()),
            ..CompactorConfig::default()
        };
        let clock = FixedClock::new(sealed_now_ns());
        compact_bucket(&store, &clock, &config, &bucket())
            .await
            .expect("compact");

        let (_rec, parts) = read_output(&store).await;
        let rows: usize = parts.iter().map(|p| decode_all(p).len()).sum();
        assert_eq!(
            rows as u64,
            INPUTS * ROWS_PER_INPUT as u64,
            "every seeded record survived the merge"
        );

        let peak = tracker.peak_cursor_decoded_bytes();
        let both_charges = INPUTS * (per_block + row_form);
        assert_eq!(
            peak,
            INPUTS * per_block,
            "cursor-phase decoded peak must be exactly {INPUTS} x heap_estimate \
             ({per_block} B each); {both_charges} would mean the deleted row-form \
             charge ({row_form} B per block) is still applied alongside it"
        );
        // Stated separately so a regression that reintroduces the old charge
        // fails on a message that names it, not only on the arithmetic above.
        assert_ne!(
            peak, both_charges,
            "a cursor must never be charged under both the columnar and the \
             row-form term in one phase snapshot"
        );
    }

    // --- ADR-0979 D2/D4 admission and budget ---------------------------------

    /// Compact `inputs` on a fresh store under `config` and return each L1
    /// part's `content_hash`, in `part_index` order.
    async fn compact_part_hashes(
        inputs: &[(Uuid, u64, Vec<LogRecord>)],
        config: &CompactorConfig,
    ) -> Vec<Vec<u8>> {
        let store = MemoryStore::new();
        for (writer_id, seq, recs) in inputs {
            seed(&store, *writer_id, *seq, recs).await;
        }
        let clock = FixedClock::new(sealed_now_ns());
        compact_bucket(&store, &clock, config, &bucket())
            .await
            .expect("compact");
        let (rec, _parts) = read_output(&store).await;
        rec.parts.iter().map(|p| p.content_hash.clone()).collect()
    }

    /// A three-input fixture whose stream bounds are disjoint on one stream and
    /// overlapping on another: stream 0 is carried by inputs A and B with
    /// interleaved (overlapping) timestamps, and stream 1 is carried by A and C
    /// with time-disjoint slices (A's `[1000, 1014]` ahead-of-C's... C's
    /// `[5000, 5007]`). So both admission regimes -- concurrent overlap and
    /// admit-only-after-drain -- are exercised in one merge.
    fn admission_mixed_fixture() -> Vec<(Uuid, u64, Vec<LogRecord>)> {
        let a: Vec<LogRecord> = (0..8i64)
            .map(|i| record(0, i * 2, "a0", vec![("k".into(), AttrValue::I64(i))]))
            .chain((0..8i64).map(|i| record(1, 1000 + i * 2, "a1", Vec::new())))
            .collect();
        let b: Vec<LogRecord> = (0..8i64)
            .map(|i| record(0, i * 2 + 1, "b0", vec![("k".into(), AttrValue::I64(i))]))
            .collect();
        let c: Vec<LogRecord> = (0..8i64)
            .map(|i| record(1, 5000 + i, "c1", Vec::new()))
            .collect();
        vec![
            (Uuid::from_u128(1), 1, a),
            (Uuid::from_u128(2), 2, b),
            (Uuid::from_u128(3), 3, c),
        ]
    }

    /// ADR-0979 decision 2's named test: overlap-gated admission
    /// ([`AdmissionMode::Overlap`]) and eager all-open admission
    /// ([`AdmissionMode::EagerAll`]) produce byte-identical parts. D2 changes
    /// only WHEN a cursor opens, never which record is the `(ts, input_index)`
    /// minimum, so every part boundary and every part `content_hash` must match.
    ///
    /// Demonstrated red by making the overlap predicate drop the second input:
    /// in the admission loop, replacing `AdmissionMode::Overlap => match frontier
    /// { ... Some(ts) => p.lower_bound <= ts }` so `p.input_index != 1` is
    /// additionally required to admit -- input 1 (B) is then never opened, B's
    /// stream-0 records vanish from the overlap run, and the two hash vectors
    /// diverge (or the record-count gate aborts the run).
    #[tokio::test]
    async fn admission_on_and_off_produce_identical_part_hashes() {
        let inputs = admission_mixed_fixture();
        // A small memory split target so the bucket splits into several parts
        // and the equality pin covers part boundaries and `part_index`, not one
        // object; identical for both modes.
        let overlap = CompactorConfig {
            merge_admission: AdmissionMode::Overlap,
            l1_part_memory_target_bytes: 1024,
            ..CompactorConfig::default()
        };
        let eager = CompactorConfig {
            merge_admission: AdmissionMode::EagerAll,
            l1_part_memory_target_bytes: 1024,
            ..CompactorConfig::default()
        };
        let on = compact_part_hashes(&inputs, &overlap).await;
        let off = compact_part_hashes(&inputs, &eager).await;
        assert!(on.len() > 1, "the fixture must split into several parts");
        assert_eq!(
            on, off,
            "overlap-gated admission must produce byte-identical parts to eager all-open admission"
        );
    }

    /// ADR-0979 decision 2, the `<=` admission boundary: a queued cursor whose
    /// SKIP_IDX lower bound EQUALS the merge frontier exactly is admitted before
    /// the next record is emitted, so an equal-`ts` tie between that cursor's
    /// first record and the already-open cursor's next record resolves by
    /// `input_index` -- and here the deferred input holds the LOWER index, so it
    /// must be emitted FIRST.
    ///
    /// Geometry. Inputs sort canonically by `(writer_id, writer_epoch,
    /// writer_seq)`, so uuid 1 is `input_index` 0 and uuid 2 is 1:
    ///
    /// ```text
    /// input_index 0 (uuid 1): stream 0 at ts 30 ("b30"), 40   lower bound 30
    /// input_index 1 (uuid 2): stream 0 at ts 10, 20, 30 ("a30")   lower bound 10
    /// ```
    ///
    /// Input 1 bootstraps (lowest bound). Input 0 stays queued while the frontier
    /// is 10 and 20, and is admitted exactly when the frontier reaches 30 --
    /// `lower_bound <= ts` with equality. Both cursors then head at ts 30, and
    /// the `(ts, input_index)` minimum is input 0's "b30".
    ///
    /// Under a `<` regression at the admission predicate, input 0 is not admitted
    /// at the boundary: input 1 emits "a30" and drains, and only then does input 0
    /// bootstrap. The record sequence reorders to a30-before-b30 and the part's
    /// bytes -- and so its `content_hash` -- change. Demonstrated red by editing
    /// the `AdmissionMode::Overlap` arm of the admission predicate in
    /// `merge_stream_into_parts` from `p.lower_bound <= ts` to `p.lower_bound <
    /// ts`: the decoded-sequence assertion below fails with `["a30", "b30"]` and
    /// the pinned hash no longer matches.
    #[tokio::test]
    async fn admission_boundary_tie_orders_by_input_index() {
        // The premise the fixture rests on: the DEFERRED input is the one with
        // the lower canonical index.
        assert!(
            Uuid::from_u128(1).to_string() < Uuid::from_u128(2).to_string(),
            "uuid 1 must sort before uuid 2, so the deferred input is input_index 0"
        );
        let deferred: Vec<LogRecord> = vec![
            record(0, 30, "b30", Vec::new()),
            record(0, 40, "b40", Vec::new()),
        ];
        let open_first: Vec<LogRecord> = vec![
            record(0, 10, "a10", Vec::new()),
            record(0, 20, "a20", Vec::new()),
            record(0, 30, "a30", Vec::new()),
        ];
        let inputs = vec![
            (Uuid::from_u128(1), 1, deferred),
            (Uuid::from_u128(2), 2, open_first),
        ];

        let store = MemoryStore::new();
        for (writer_id, seq, recs) in &inputs {
            seed(&store, *writer_id, *seq, recs).await;
        }
        let clock = FixedClock::new(sealed_now_ns());
        compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket())
            .await
            .expect("compact");
        let (_rec, parts) = read_output(&store).await;
        assert_eq!(parts.len(), 1, "the fixture is one part");
        let bodies: Vec<String> = decode_all(&parts[0]).into_iter().map(|r| r.body).collect();
        assert_eq!(
            bodies,
            vec!["a10", "a20", "b30", "a30", "b40"],
            "at the ts-30 tie the admitted-at-the-boundary input (input_index 0) sorts first"
        );

        // The exact bytes, not only the order: the part hashes under overlap-gated
        // and eager all-open admission, pinned as literals.
        const EXPECTED_HASHES: [&str; 1] =
            ["bcb041dad935c2e867f943330601f9bf04da1e171faa3ebdf67cadd14eefef39"];
        let overlap = compact_part_hashes(&inputs, &CompactorConfig::default()).await;
        let eager = compact_part_hashes(
            &inputs,
            &CompactorConfig {
                merge_admission: AdmissionMode::EagerAll,
                ..CompactorConfig::default()
            },
        )
        .await;
        let hex: Vec<String> = overlap.iter().map(hex::encode).collect();
        assert_eq!(
            hex,
            EXPECTED_HASHES.map(String::from).to_vec(),
            "the boundary-tie fixture's part content_hash"
        );
        assert_eq!(
            overlap, eager,
            "admitting at the boundary reproduces the all-open order exactly"
        );
    }

    /// Compact `inputs` (all on stream 0) with a tracker installed and return the
    /// recorded `max_open_cursors_per_stream`.
    async fn max_open_cursors_for(inputs: &[(Uuid, u64, Vec<LogRecord>)]) -> u64 {
        let tracker = MergeMemoryTracker::new();
        let config = CompactorConfig {
            merge_memory_tracker: Some(tracker.clone()),
            ..CompactorConfig::default()
        };
        let store = MemoryStore::new();
        for (writer_id, seq, recs) in inputs {
            seed(&store, *writer_id, *seq, recs).await;
        }
        let clock = FixedClock::new(sealed_now_ns());
        compact_bucket(&store, &clock, &config, &bucket())
            .await
            .expect("compact");
        tracker.max_open_cursors_per_stream()
    }

    /// ADR-0979 decision 2's acceptance phrasing: with all three inputs'
    /// stream-0 slices overlapping, `max_open_cursors_per_stream` is exactly 3;
    /// making one input's slice time-disjoint (far ahead of the other two) drops
    /// it by exactly 1, to 2 -- the assertion that admission, not luck, bounds
    /// the count. The disjoint slice's cursor is admitted only after the other
    /// two drain, so it is never open alongside them.
    ///
    /// Demonstrated red by making admission ignore the frontier (admit every
    /// queued cursor immediately, as `AdmissionMode::EagerAll` does): the
    /// disjoint slice then opens up front alongside the other two and both
    /// fixtures report 3, so the `2` assertion fails.
    #[tokio::test]
    async fn admission_bounds_open_cursors_to_the_overlap_degree() {
        // input j carries ts i*3 + j, so all three stream-0 slices overlap.
        let overlap_all: Vec<(Uuid, u64, Vec<LogRecord>)> = (0..3u128)
            .map(|j| {
                let recs = (0..10i64)
                    .map(|i| record(0, i * 3 + j as i64, "r", Vec::new()))
                    .collect();
                (Uuid::from_u128(j + 1), (j + 1) as u64, recs)
            })
            .collect();
        // Two inputs interleave in [0, 19]; the third sits far ahead in
        // [10_000, 10_009], time-disjoint from them.
        let one_disjoint: Vec<(Uuid, u64, Vec<LogRecord>)> = vec![
            (
                Uuid::from_u128(1),
                1,
                (0..10i64)
                    .map(|i| record(0, i * 2, "r", Vec::new()))
                    .collect(),
            ),
            (
                Uuid::from_u128(2),
                2,
                (0..10i64)
                    .map(|i| record(0, i * 2 + 1, "r", Vec::new()))
                    .collect(),
            ),
            (
                Uuid::from_u128(3),
                3,
                (0..10i64)
                    .map(|i| record(0, 10_000 + i, "r", Vec::new()))
                    .collect(),
            ),
        ];
        assert_eq!(
            max_open_cursors_for(&overlap_all).await,
            3,
            "three overlapping slices need all three cursors open at once"
        );
        assert_eq!(
            max_open_cursors_for(&one_disjoint).await,
            2,
            "the disjoint third slice admits only after the first two drain, so at most two are open"
        );
    }

    /// One seeded input's catalog, loaded exactly as the merge loads it.
    async fn stream_catalog(
        store: &MemoryStore,
        writer_id: Uuid,
        seq: u64,
        bytes: &Bytes,
    ) -> RlogInputCatalog {
        let key = seeded_data_key(writer_id, seq, bytes);
        load_catalog_from_object(store, &CompactorConfig::default(), key, true)
            .await
            .expect("catalog")
    }

    /// The pre-decode reservation the merge charges one input's cursor over
    /// stream 0, loaded and computed exactly as the merge does.
    async fn stream0_reservation(
        store: &MemoryStore,
        writer_id: Uuid,
        seq: u64,
        bytes: &Bytes,
    ) -> u64 {
        let catalog = stream_catalog(store, writer_id, seq, bytes).await;
        let (stream_id, _) = stream_ident(0);
        cursor_reservation_bytes(&catalog, &stream_id)
            .expect("reservation")
            .expect("input carries stream 0")
    }

    /// What one input's stream-0 cursor actually holds once its first block is
    /// fetched and decoded: the figure the merge reconciles the pre-decode
    /// reservation down to (ADR-0979 decision 4 as amended).
    async fn stream0_resident_bytes(
        store: &MemoryStore,
        writer_id: Uuid,
        seq: u64,
        bytes: &Bytes,
    ) -> u64 {
        let catalog = stream_catalog(store, writer_id, seq, bytes).await;
        let (stream_id, _) = stream_ident(0);
        let cursor = open_cursor(store, &catalog, 0, &stream_id, None)
            .await
            .expect("open cursor")
            .expect("input carries stream 0");
        cursor.resident_bytes()
    }

    /// Per whole-object block, the sum of PAGE_DIR `uncomp_len` over EVERY page
    /// of the block: the pre-amendment reservation basis, kept in the tests only
    /// so the ceiling test can show what it under-charges by.
    fn block_all_page_uncomp_lens(obj: &[u8]) -> Vec<u64> {
        let cfg = RlogConfig::default();
        let ftr = footer::open(obj).expect("open");
        let raw = read_section(obj, ftr.section(kind::PAGE_DIR).unwrap(), &cfg).expect("page_dir");
        let dir = PageDir::decode(&raw).expect("decode page_dir");
        (0..dir.block_count() as u32)
            .map(|b| {
                dir.block_pages(b)
                    .expect("block pages")
                    .iter()
                    .map(|p| p.desc.uncomp_len)
                    .sum()
            })
            .collect()
    }

    /// Every candidate block of stream 0, decoded one at a time exactly as
    /// [`StreamCursor::refill`] decodes them, as
    /// `(whole-object block index, heap_estimate at decode)`. Driving the
    /// primitives directly rather than `refill` itself is what keeps one entry
    /// per block, named by the block it prices, so a per-block ceiling can be
    /// checked against the block it is a ceiling for.
    async fn stream0_decoded_blocks(
        store: &MemoryStore,
        catalog: &RlogInputCatalog,
        stream_id: &LogStreamId,
    ) -> Vec<(usize, u64)> {
        let mut cursor = StreamCursor::open(catalog, 0, stream_id)
            .expect("open cursor")
            .expect("input carries the stream");
        let mut out: Vec<(usize, u64)> = Vec::new();
        loop {
            let Some(block) = cursor.pending_block_index() else {
                match cursor.next_raw_block(store, None).await.expect("fetch") {
                    Some((loc, data)) => {
                        cursor.group_raw_bytes = data.len() as u64;
                        cursor.decoded_in_group = 0;
                        cursor.group = Some((loc, data));
                        continue;
                    }
                    None => break,
                }
            };
            cursor.release_block(None);
            assert!(
                cursor.decode_next_block(None).expect("decode"),
                "a pending block index must decode"
            );
            out.push((block, cursor.decoded_bytes()));
        }
        out
    }

    /// ADR-0979 decision 4 as amended, the ceiling direction: the PER-BLOCK
    /// ceiling [`block_decode_ceiling_bytes`] -- the `B_dec` term alone, without
    /// the reservation's `2 * G` and location-metadata slack -- bounds what
    /// every one of an input's blocks actually decodes to, on a fixture built
    /// from CONSTANT columns, the shape the pre-amendment basis mispriced.
    ///
    /// The fixture is 500 records of one stream with an identical body,
    /// severity, and no attributes, written in 64-record blocks so it spans
    /// EIGHT of them (the default 8192-record blocking would put the whole
    /// fixture in one, and a one-block fixture cannot distinguish a per-block
    /// ceiling from a whole-input one). `stream_ref`, `severity_num`, `flags`,
    /// and the two string columns all encode to a handful of stored bytes per
    /// block while still decoding to a slot per row per column.
    ///
    /// The second half pins the fixture's teeth: twice the block's total page
    /// `uncomp_len` -- the basis this amendment removed -- is below the decoded
    /// heap, so the old code charged less than the memory the block takes.
    ///
    /// On this fixture the largest block's ceiling is 27,863 B against a
    /// 7,673 B decoded heap, and the removed basis would have charged 256 B.
    ///
    /// Demonstrated red by restoring the old basis in the two lines that define
    /// it: dropping the `string_ids.contains` filter in `input_cursor_pricing`
    /// (so the per-block figure is every page's `uncomp_len` again) and
    /// returning `string_bytes * 2` from `block_decode_ceiling_bytes` in place
    /// of its terms. The `ceiling >= heap` assert then fails 256 against 7,673
    /// on block 0, and the ratio grows with rows and column count.
    #[tokio::test]
    async fn block_ceiling_bounds_every_decoded_block_of_a_constant_column_input() {
        let recs: Vec<LogRecord> = (0..500i64)
            .map(|i| record(0, 1_000 + i, "constant body", Vec::new()))
            .collect();
        let cfg = RlogConfig {
            block_target_records: 64,
            ..RlogConfig::default()
        };
        let store = MemoryStore::new();
        let bytes = seed_l0(&store, Uuid::from_u128(1), 1, &recs, cfg, &[]).await;
        let catalog = stream_catalog(&store, Uuid::from_u128(1), 1, &bytes).await;
        let (stream_id, _) = stream_ident(0);

        assert!(
            catalog.pricing.block_rows.len() > 1,
            "the fixture must span more than one block, or a per-block ceiling is untested"
        );
        let decoded = stream0_decoded_blocks(&store, &catalog, &stream_id).await;
        assert_eq!(
            decoded.len(),
            catalog.pricing.block_rows.len(),
            "the cursor decodes every one of the input's blocks on a single-stream fixture"
        );

        for (block, heap) in &decoded {
            let ceiling =
                block_decode_ceiling_bytes(&catalog.pricing, *block).expect("block ceiling");
            assert!(
                ceiling >= *heap,
                "block {block}: its pre-decode ceiling ({ceiling} B) must bound its decoded \
                 heap_estimate ({heap} B)"
            );
        }

        // What the removed basis would have charged for the same blocks.
        let max_decoded = decoded.iter().map(|(_, h)| *h).max().expect("a block");
        let old_basis = block_all_page_uncomp_lens(&bytes)
            .into_iter()
            .max()
            .expect("a block")
            * 2;
        assert!(
            old_basis < max_decoded,
            "the fixture must exercise the under-charge: 2 x page uncomp_len ({old_basis} B) has \
             to be below the decoded heap ({max_decoded} B) for this pin to mean anything"
        );
    }

    /// ADR-0979 decision 1's ceiling includes the decoder's five per-kind slot
    /// vectors (the ADR's amended `2 x SUM(slot sizes) x width` floor, priced
    /// here at the conservative 480 B per width unit), and on a small block that term is
    /// what makes the ceiling a ceiling: the vectors are indexed by column id,
    /// so they cost the id-space width whatever the block holds, while every
    /// other term scales with rows.
    ///
    /// The fixture is eight one-record blocks over four dynamic attribute
    /// columns (a 14-wide column-id space). On it the ceiling is 7,194 B per
    /// block, of which the spine term is 6,720 B, against a decoded heap of
    /// 2,232 B: the per-row terms alone come to 474 B, a fifth of what the block
    /// holds.
    ///
    /// It also pins the two corrections this crate applies to ADR-0979's
    /// flat 24-B-per-kind exact-width pricing of the term. At that figure the
    /// ceiling here is 2,154 B, below the 2,232 B the block decodes to: the
    /// string kind's slot is an enum wider than a `Vec` handle, and a slot
    /// vector's capacity is a `Vec` growth step above the id-space width rather
    /// than the width itself.
    ///
    /// Demonstrated red by dropping the `slot_spines` term from
    /// `block_decode_ceiling_bytes`'s sum: the ceiling falls to 474 B and the
    /// bound assert fails against the 2,232 B the block decodes to. Demonstrated
    /// red for the corrections by setting `DECODED_SLOT_SPINE_BYTES` back to 24
    /// and `DECODED_SLOT_SPINE_CAPACITY_FACTOR` to 1: the same assert fails
    /// 2,154 against 2,232.
    #[tokio::test]
    async fn the_slot_spine_term_is_what_bounds_a_small_block() {
        let recs: Vec<LogRecord> = (0..8i64)
            .map(|i| {
                record(
                    0,
                    1_000 + i,
                    "b",
                    vec![
                        ("k0".into(), AttrValue::I64(i)),
                        ("k1".into(), AttrValue::F64(i as f64)),
                        ("k2".into(), AttrValue::Bool(i % 2 == 0)),
                        ("k3".into(), AttrValue::Str(format!("v{i}"))),
                    ],
                )
            })
            .collect();
        let cfg = RlogConfig {
            block_target_records: 1,
            ..RlogConfig::default()
        };
        let store = MemoryStore::new();
        let bytes = seed_l0(&store, Uuid::from_u128(1), 1, &recs, cfg, &[]).await;
        let catalog = stream_catalog(&store, Uuid::from_u128(1), 1, &bytes).await;
        let (stream_id, _) = stream_ident(0);

        assert_eq!(
            catalog.pricing.block_rows,
            vec![1; 8],
            "the fixture must be eight one-record blocks"
        );
        let spine_term = DECODED_SLOT_SPINE_BYTES
            * catalog.pricing.total_cols
            * DECODER_SLOT_KINDS
            * DECODED_SLOT_SPINE_CAPACITY_FACTOR;
        let decoded = stream0_decoded_blocks(&store, &catalog, &stream_id).await;
        assert_eq!(decoded.len(), 8);

        for (block, heap) in &decoded {
            let ceiling =
                block_decode_ceiling_bytes(&catalog.pricing, *block).expect("block ceiling");
            assert!(
                ceiling >= *heap,
                "block {block}: its pre-decode ceiling ({ceiling} B) must bound its decoded \
                 heap_estimate ({heap} B)"
            );
            assert!(
                ceiling - spine_term < *heap,
                "block {block}: without the slot-spine term the ceiling ({} B) would be below \
                 the block's decoded heap ({heap} B), which is what makes the term load-bearing \
                 rather than margin",
                ceiling - spine_term
            );
        }
    }

    /// ADR-0979 decision 4 as amended, the reconcile direction: after a cursor's
    /// decode completes, the charge held against the budget is the cursor's ACTUAL
    /// residency, and its decoded term equals `heap_estimate()` exactly -- not the
    /// pre-decode ceiling, which is strictly larger here.
    ///
    /// Demonstrated red by making `reconcile_cursor_charge` a no-op (an early
    /// `return` before it reads `resident_bytes`): the charge stays at the
    /// pre-decode ceiling, and the equality assert fails 76,508 against 26,496.
    #[tokio::test]
    async fn reconcile_charges_actual_residency_with_the_decoded_term_exact() {
        let recs: Vec<LogRecord> = (0..200i64)
            .map(|i| record(0, 1_000 + i, "body", vec![("k".into(), AttrValue::I64(i))]))
            .collect();
        let store = MemoryStore::new();
        let bytes = seed(&store, Uuid::from_u128(1), 1, &recs).await;
        let catalog = stream_catalog(&store, Uuid::from_u128(1), 1, &bytes).await;
        let (stream_id, _) = stream_ident(0);

        let reservation = cursor_reservation_bytes(&catalog, &stream_id)
            .expect("reservation")
            .expect("input carries stream 0");
        let mut cursor = open_cursor(&store, &catalog, 0, &stream_id, None)
            .await
            .expect("open cursor")
            .expect("input carries stream 0");
        cursor.reservation = reservation;
        cursor.charged = reservation;

        // Exactly what the merge does once the admission's fetch and decode
        // complete.
        let mut charged = reservation;
        reconcile_cursor_charge(&mut cursor, &mut charged);

        let heap = cursor
            .block
            .as_ref()
            .expect("a decoded block")
            .heap_estimate();
        let loc_meta = loc_metadata_bytes(&cursor.locs);
        let raw = cursor.raw_resident_bytes();
        assert_eq!(
            charged, cursor.charged,
            "the stream's running charge and the cursor's own figure move together"
        );
        assert_eq!(
            charged,
            loc_meta + raw + heap,
            "the reconciled charge is location metadata + raw bytes held + decoded heap"
        );
        assert_eq!(
            charged - loc_meta - raw,
            heap,
            "the reconciled decoded term equals heap_estimate() exactly"
        );
        assert!(
            charged < reservation,
            "the pre-decode ceiling ({reservation} B) must exceed the reconciled residency \
             ({charged} B) on this fixture, or the reconcile pins nothing"
        );
    }

    /// ADR-0979 decision 4's owed first-small/next-large test: a cursor whose
    /// FIRST candidate block is cheaper than its SECOND must reserve the larger
    /// figure, because `StreamCursor::refill` decodes the second block after
    /// releasing the first.
    ///
    /// The fixture is one stream in one input written with 4-record blocks and
    /// one block per row group: block 0 holds four 4-byte bodies, block 1 four
    /// 2,000-byte bodies, so the two blocks differ only in their string-page
    /// payload and block 1's ceiling is the larger.
    ///
    /// Demonstrated red by changing `cursor_reservation_bytes`'s fold from
    /// `b_dec = b_dec.max(block_decode_ceiling_bytes(..)?)` to taking the first
    /// candidate block's ceiling only (`if b_dec == 0 { b_dec = ... }`): the
    /// reservation comes back as 1,696 B against the 9,701 B the second block
    /// needs, and the equality assert fails.
    #[tokio::test]
    async fn reservation_takes_the_max_over_candidate_blocks_not_the_first() {
        let long_body = "x".repeat(2_000);
        let recs: Vec<LogRecord> = (0..4i64)
            .map(|i| record(0, 100 + i, "tiny", Vec::new()))
            .chain((0..4i64).map(|i| record(0, 200 + i, &format!("{long_body}{i}"), Vec::new())))
            .collect();
        let cfg = RlogConfig {
            block_target_records: 4,
            // One block per row group, so the two blocks are two locs and the
            // cursor really releases the first before decoding the second.
            group_target_blocks: 1,
            ..RlogConfig::default()
        };
        let store = MemoryStore::new();
        let bytes = seed_l0(&store, Uuid::from_u128(1), 1, &recs, cfg, &[]).await;
        let catalog = stream_catalog(&store, Uuid::from_u128(1), 1, &bytes).await;
        let (stream_id, _) = stream_ident(0);

        assert_eq!(
            catalog.pricing.block_rows,
            vec![4, 4],
            "the fixture must be two four-record blocks"
        );
        let first = block_decode_ceiling_bytes(&catalog.pricing, 0).expect("block 0 ceiling");
        let second = block_decode_ceiling_bytes(&catalog.pricing, 1).expect("block 1 ceiling");
        assert!(
            first < second,
            "the fixture's first block ({first} B) must be cheaper than its second ({second} B)"
        );

        let locs = catalog
            .reader
            .stream_blocks(&stream_id)
            .expect("stream blocks")
            .expect("input carries stream 0");
        assert_eq!(
            locs.len(),
            2,
            "one loc per block under group_target_blocks 1"
        );
        let two_g = locs.iter().map(|l| l.byte_len()).max().expect("a loc") * 2;
        let expected = two_g + loc_metadata_bytes(&locs) + second;
        let reservation = cursor_reservation_bytes(&catalog, &stream_id)
            .expect("reservation")
            .expect("input carries stream 0");
        assert_eq!(
            reservation, expected,
            "the reservation is 2*G + location metadata + the LARGER block's ceiling"
        );
    }

    /// ADR-0979 decision 4: a budget one byte below the minimum admissible
    /// cursor set fails with [`MaintainError::MergeCursorBudgetExceeded`] naming
    /// the exact figures, and one byte above succeeds with part hashes identical
    /// to an unbudgeted run.
    ///
    /// The fixture is two inputs carrying stream 0 with interleaved (fully
    /// overlapping) timestamps, so both cursors must be open at the merge's
    /// peak. The minimum admissible figure is `resident_a + r_b`, NOT
    /// `r_a + r_b`: the first cursor is admitted at its pre-decode ceiling and
    /// then reconciled down to what it actually holds before the second cursor's
    /// admission reads the charge (ADR-0979 decision 4 as amended). Both terms
    /// are computed here from the same functions the merge uses, so the
    /// threshold is exact rather than observed, and the run at
    /// `resident_a + r_b` only completes because the reconcile happened.
    ///
    /// Demonstrated red by raising the budget the failing run is given from
    /// `min_admissible - 1` to `min_admissible`: the second cursor then fits and
    /// no error is returned, so `expect_err` panics. Demonstrated red for the
    /// reconcile by deleting BOTH `reconcile_cursor_charge` calls in
    /// `merge_stream_into_parts` (the admission arm and the post-refill arm --
    /// deleting only the admission one leaves this fixture reconciled by the
    /// other, since its second cursor is admitted after the first has emitted a
    /// record): the charge stays at `r_a` and the `charged_bytes` assert fails
    /// 3,731 against 1,957.
    #[tokio::test]
    async fn cursor_budget_fails_closed_one_byte_below_the_minimum_admissible_set() {
        let a: Vec<LogRecord> = (0..10i64)
            .map(|i| record(0, i * 2, "a", Vec::new()))
            .collect();
        let b: Vec<LogRecord> = (0..10i64)
            .map(|i| record(0, i * 2 + 1, "b", Vec::new()))
            .collect();

        // Reservations, from a throwaway store seeded identically.
        let probe = MemoryStore::new();
        let a_bytes = seed(&probe, Uuid::from_u128(1), 1, &a).await;
        let b_bytes = seed(&probe, Uuid::from_u128(2), 2, &b).await;
        let r_a = stream0_reservation(&probe, Uuid::from_u128(1), 1, &a_bytes).await;
        let r_b = stream0_reservation(&probe, Uuid::from_u128(2), 2, &b_bytes).await;
        let resident_a = stream0_resident_bytes(&probe, Uuid::from_u128(1), 1, &a_bytes).await;
        assert!(
            resident_a < r_a,
            "the reconcile must lower the first cursor's charge ({r_a} B ceiling, {resident_a} B \
             resident), or this test's threshold is not the reconciled one"
        );
        let min_admissible = resident_a + r_b;

        let inputs = vec![(Uuid::from_u128(1), 1, a), (Uuid::from_u128(2), 2, b)];
        let (stream_id, _) = stream_ident(0);

        // One byte below the minimum admissible set: the second cursor's
        // admission overruns and the run fails closed, naming the exact figures.
        let under = CompactorConfig {
            merge_cursor_budget_bytes: min_admissible - 1,
            ..CompactorConfig::default()
        };
        let store = MemoryStore::new();
        for (writer_id, seq, recs) in &inputs {
            seed(&store, *writer_id, *seq, recs).await;
        }
        let clock = FixedClock::new(sealed_now_ns());
        let err = compact_bucket(&store, &clock, &under, &bucket())
            .await
            .expect_err("a budget below the minimum admissible set must fail closed");
        match err {
            MaintainError::MergeCursorBudgetExceeded {
                stream_id: got_stream,
                open_cursors,
                charged_bytes,
                budget_bytes,
                required_bytes,
                inputs_carrying_stream,
                site,
            } => {
                assert_eq!(got_stream, stream_id.to_hex());
                assert_eq!(
                    open_cursors, 1,
                    "the first cursor was open when the second was refused"
                );
                assert_eq!(
                    charged_bytes, resident_a,
                    "the open cursor charges its reconciled residency, not its ceiling"
                );
                assert_eq!(budget_bytes, min_admissible - 1);
                assert_eq!(
                    required_bytes, min_admissible,
                    "charged + the refused cursor's reservation"
                );
                assert_eq!(inputs_carrying_stream, 2);
                assert_eq!(
                    site,
                    MergeCursorBudgetSite::Admission {
                        batch_position: 0,
                        batch_len: 1,
                    },
                    "the refusal is an admission, not an open cursor's block growth"
                );
            }
            other => panic!("expected MergeCursorBudgetExceeded, got {other:?}"),
        }

        // One byte above: the whole minimum admissible set fits, the merge
        // completes, and the parts are byte-identical to an unbudgeted run.
        let exact = CompactorConfig {
            merge_cursor_budget_bytes: min_admissible,
            ..CompactorConfig::default()
        };
        let with_budget = compact_part_hashes(&inputs, &exact).await;
        let unbudgeted = compact_part_hashes(&inputs, &CompactorConfig::default()).await;
        assert!(!with_budget.is_empty());
        assert_eq!(
            with_budget, unbudgeted,
            "a budget at exactly the minimum admissible set must not change the output"
        );
    }

    /// ADR-0979 decision 4: a refusal partway through an admission BATCH reports
    /// only what the open cursors hold, and names the position in the batch that
    /// crossed the budget. Batch members before that position were never opened,
    /// so their reservations are in `required_bytes` (the number a retry must
    /// budget for) and NOT in `charged_bytes` (what memory is actually held).
    ///
    /// The fixture is three inputs whose stream-0 slices all start at ts 0, so
    /// their SKIP_IDX lower bounds are equal: the first cursor bootstraps alone,
    /// and the remaining two are admitted together as one batch of 2. The budget
    /// is set one byte below what the whole batch needs, so the refusal lands at
    /// batch position 1 with one cursor open.
    ///
    /// Demonstrated red by restoring the pre-fix accumulation (reporting
    /// `charged_bytes` as the open charge plus the batch members already
    /// reserved, which is what the running `charged` held before this fix):
    /// `charged_bytes` comes back as 3,756 B against the 1,477 B the one open
    /// cursor holds, counting a cursor that was never opened.
    #[tokio::test]
    async fn cursor_budget_refusal_mid_batch_reports_only_open_cursors() {
        // Three identical-shape inputs on stream 0, all starting at ts 0, so
        // every lower bound is 0 and the two non-bootstrap cursors are admitted
        // in one batch.
        let recs = |tag: &str| -> Vec<LogRecord> {
            (0..6i64)
                .map(|i| record(0, i * 10, tag, Vec::new()))
                .collect()
        };
        let inputs = vec![
            (Uuid::from_u128(1), 1, recs("a")),
            (Uuid::from_u128(2), 2, recs("b")),
            (Uuid::from_u128(3), 3, recs("c")),
        ];

        let probe = MemoryStore::new();
        let mut seeded = Vec::new();
        for (writer_id, seq, r) in &inputs {
            seeded.push(seed(&probe, *writer_id, *seq, r).await);
        }
        let resident_a = stream0_resident_bytes(&probe, Uuid::from_u128(1), 1, &seeded[0]).await;
        let r_b = stream0_reservation(&probe, Uuid::from_u128(2), 2, &seeded[1]).await;
        let r_c = stream0_reservation(&probe, Uuid::from_u128(3), 3, &seeded[2]).await;
        let batch_total = resident_a + r_b + r_c;

        let store = MemoryStore::new();
        for (writer_id, seq, r) in &inputs {
            seed(&store, *writer_id, *seq, r).await;
        }
        let config = CompactorConfig {
            merge_cursor_budget_bytes: batch_total - 1,
            ..CompactorConfig::default()
        };
        let clock = FixedClock::new(sealed_now_ns());
        let err = compact_bucket(&store, &clock, &config, &bucket())
            .await
            .expect_err("a budget below the batch's total must fail closed");
        match err {
            MaintainError::MergeCursorBudgetExceeded {
                open_cursors,
                charged_bytes,
                required_bytes,
                budget_bytes,
                inputs_carrying_stream,
                site,
                ..
            } => {
                assert_eq!(
                    site,
                    MergeCursorBudgetSite::Admission {
                        batch_position: 1,
                        batch_len: 2,
                    },
                    "the two late cursors admit together, and the second member of the batch is \
                     the one that crossed the budget"
                );
                assert_eq!(open_cursors, 1, "only the bootstrap cursor was ever opened");
                assert_eq!(
                    charged_bytes, resident_a,
                    "the charge names what the open cursor holds, not the batch members before \
                     the refused one"
                );
                assert_eq!(
                    required_bytes, batch_total,
                    "the retry figure is the open charge plus the batch through this position"
                );
                assert_eq!(budget_bytes, batch_total - 1);
                assert_eq!(inputs_carrying_stream, 3);
            }
            other => panic!("expected MergeCursorBudgetExceeded, got {other:?}"),
        }
    }

    /// One input's stream-0 records for the grow-before-a-larger-decode tests:
    /// four tiny bodies at even timestamps `base..base+6`, then four 2,000-byte
    /// bodies at `base+8..base+14`. Written with [`grow_fixture_cfg`] these are
    /// two four-record blocks of ONE row group, so a cursor crosses from the
    /// small decoded block to the large one with no fetch in between: the
    /// transition ADR-0979 decision 4's grow clause gates.
    fn grow_fixture_records(base: i64) -> Vec<LogRecord> {
        let long_body = "x".repeat(2_000);
        (0..4i64)
            .map(|i| record(0, base + i * 2, "tiny", Vec::new()))
            .chain(
                (0..4i64)
                    .map(|i| record(0, base + 8 + i * 2, &format!("{long_body}{i}"), Vec::new())),
            )
            .collect()
    }

    /// Four-record blocks, two blocks per row group: one loc holding both of
    /// [`grow_fixture_records`]'s blocks.
    fn grow_fixture_cfg() -> RlogConfig {
        RlogConfig {
            block_target_records: 4,
            group_target_blocks: 2,
            ..RlogConfig::default()
        }
    }

    /// Seed one [`grow_fixture_records`] input and load its catalog.
    async fn grow_fixture_catalog(
        store: &MemoryStore,
        writer_id: Uuid,
        seq: u64,
        records: &[LogRecord],
    ) -> RlogInputCatalog {
        let bytes = seed_l0(store, writer_id, seq, records, grow_fixture_cfg(), &[]).await;
        stream_catalog(store, writer_id, seq, &bytes).await
    }

    /// ADR-0979 decision 4 as amended, the grow-before-a-larger-decode clause,
    /// observed ACROSS a refill: before an open cursor decodes a block bigger
    /// than the one it just released, its charge GROWS to
    /// `location metadata + retained raw + that block's pre-decode ceiling`, and
    /// the growth is checked against the budget BEFORE the decode starts.
    ///
    /// This is the invariant "at every point in a cursor's life the charge is at
    /// or above its actual resident footprint" at the one point the reconcile
    /// alone cannot hold it: the reconcile lowers the charge to the SMALL first
    /// block's residency, and the next block is larger. The three arms are the
    /// grown figure (exact, computed from the same functions the merge uses), a
    /// budget one byte below it refusing with the decode never having run, and
    /// the budget at that figure decoding the block the run needs.
    ///
    /// On this fixture the charge before the second decode is 14,413 B against
    /// the 1,338 B the reconcile left after the first block, so the growth the
    /// budget must take is 13,075 B.
    ///
    /// Demonstrated red by deleting the grow-and-check from
    /// `StreamCursor::refill` (the `if let (Some(b), Some(block)) = ...` arm
    /// before `decode_next_block`): the refusal arm returns `Ok(())` and
    /// `expect_err` panics, and the charge arm finds 1,338 B where the grown
    /// 14,413 B belongs. Demonstrated red for the ordering by moving that arm to
    /// AFTER the `decode_next_block` call: that decode drops the row group's raw
    /// bytes, so the requirement computed behind it is smaller than the one this
    /// budget is set below, no refusal fires at all, and `expect_err` panics on
    /// a cursor that has already materialized the block.
    #[tokio::test]
    async fn cursor_charge_grows_before_it_decodes_a_larger_later_block() {
        let recs = grow_fixture_records(0);
        let store = MemoryStore::new();
        let catalog = grow_fixture_catalog(&store, Uuid::from_u128(1), 1, &recs).await;
        let (stream_id, _) = stream_ident(0);

        assert_eq!(
            catalog.pricing.block_rows,
            vec![4, 4],
            "the fixture must be two four-record blocks"
        );
        let locs = catalog
            .reader
            .stream_blocks(&stream_id)
            .expect("stream blocks")
            .expect("input carries stream 0")
            .clone();
        assert_eq!(
            locs.len(),
            1,
            "both blocks must sit in ONE row group, so the second decode needs no fetch"
        );
        assert_eq!(locs[0].block_indices(), &[0, 1]);
        let ceiling_1 = block_decode_ceiling_bytes(&catalog.pricing, 1).expect("block 1 ceiling");

        // Open the cursor and reconcile it exactly as the merge does after an
        // admission: the charge is now the SMALL first block's residency.
        let tracker = MergeMemoryTracker::new();
        let mut cursor = open_cursor(&store, &catalog, 0, &stream_id, Some(&tracker))
            .await
            .expect("open cursor")
            .expect("input carries stream 0");
        let reservation = cursor_reservation_bytes(&catalog, &stream_id)
            .expect("reservation")
            .expect("input carries stream 0");
        cursor.reservation = reservation;
        cursor.charged = reservation;
        let mut charged = reservation;
        reconcile_cursor_charge(&mut cursor, &mut charged);
        let heap_0 = cursor.decoded_bytes();
        let after_first_block = charged;

        // The figure the charge must grow to before block 1 is decoded, from the
        // same terms the implementation prices: what the cursor still holds
        // (metadata plus the row group's raw bytes, the decoded block having
        // been released) plus block 1's pre-decode ceiling.
        let grown = loc_metadata_bytes(&locs) + cursor.raw_resident_bytes() + ceiling_1;
        assert!(
            grown > after_first_block,
            "the fixture must need a real growth: block 1's ceiling ({ceiling_1} B) on top of \
             {} B held has to exceed the {after_first_block} B the reconcile left",
            grown - ceiling_1
        );

        // Drain block 0's four rows so the next refill has to decode block 1.
        for _ in 0..4 {
            cursor.next_record().expect("record").expect("a row");
        }
        assert!(
            cursor.peek_ts().is_none(),
            "block 0 is drained but not yet released"
        );

        // (b) One byte below the grown figure: refused, before the decode.
        let mut refused_charged = charged;
        let mut budget = CursorBudget {
            budget: grown - 1,
            charged: &mut refused_charged,
            stream_id: &stream_id,
            open_cursors: 1,
            inputs_carrying_stream: 1,
        };
        let err = cursor
            .refill(&store, Some(&tracker), Some(&mut budget))
            .await
            .expect_err("a budget below the grown figure must refuse the decode");
        match err {
            MaintainError::MergeCursorBudgetExceeded {
                stream_id: got_stream,
                open_cursors,
                charged_bytes,
                budget_bytes,
                required_bytes,
                inputs_carrying_stream,
                site,
            } => {
                assert_eq!(got_stream, stream_id.to_hex());
                assert_eq!(open_cursors, 1, "the growing cursor is itself open");
                assert_eq!(
                    charged_bytes, after_first_block,
                    "the charge at refusal is what the cursor held after the reconcile"
                );
                assert_eq!(budget_bytes, grown - 1);
                assert_eq!(
                    required_bytes, grown,
                    "the number a retry must budget for is the grown figure"
                );
                assert_eq!(inputs_carrying_stream, 1);
                assert_eq!(
                    site,
                    MergeCursorBudgetSite::BlockGrow {
                        block_index: 1,
                        grow_bytes: grown - after_first_block,
                    },
                    "the refusal names the block whose decode it refused and the growth it \
                     could not take"
                );
            }
            other => panic!("expected MergeCursorBudgetExceeded, got {other:?}"),
        }
        // The decode never ran: no block is held, the loc still has one
        // undecoded block, the charge never rose, and the tracker's decoded
        // high-water is still the FIRST block's heap.
        assert!(
            cursor.block.is_none(),
            "no block is decoded after a refusal"
        );
        assert_eq!(cursor.decoded_bytes(), 0);
        assert_eq!(
            cursor.decoded_in_group, 1,
            "block 1 of the row group is still undecoded"
        );
        assert_eq!(
            refused_charged, after_first_block,
            "a refused grow charges nothing"
        );
        assert_eq!(
            tracker.peak_cursor_decoded_bytes(),
            heap_0,
            "the decoded high-water is still the first block's heap, so the larger block was \
             never materialized"
        );

        // (a) and (c). A fresh cursor at exactly the grown figure: the refill
        // decodes block 1, and the charge at that moment IS the grown figure --
        // the ceiling, held until the caller reconciles it back down.
        let tracker = MergeMemoryTracker::new();
        let mut cursor = open_cursor(&store, &catalog, 0, &stream_id, Some(&tracker))
            .await
            .expect("open cursor")
            .expect("input carries stream 0");
        cursor.reservation = reservation;
        cursor.charged = reservation;
        let mut charged = reservation;
        reconcile_cursor_charge(&mut cursor, &mut charged);
        for _ in 0..4 {
            cursor.next_record().expect("record").expect("a row");
        }
        let mut budget = CursorBudget {
            budget: grown,
            charged: &mut charged,
            stream_id: &stream_id,
            open_cursors: 1,
            inputs_carrying_stream: 1,
        };
        cursor
            .refill(&store, Some(&tracker), Some(&mut budget))
            .await
            .expect("a budget at the grown figure admits the decode");
        assert_eq!(
            cursor.charged, grown,
            "the charge at the moment before the second decode is the grown figure"
        );
        assert_eq!(
            *budget.charged, grown,
            "the stream's running charge moved with the cursor's"
        );
        let heap_1 = cursor.decoded_bytes();
        assert!(
            heap_1 > heap_0,
            "the fixture's second block ({heap_1} B) must decode larger than its first \
             ({heap_0} B), or the growth this test pins is not a growth"
        );
        assert!(
            cursor.charged >= cursor.resident_bytes(),
            "the grown charge must still bound what the cursor now holds"
        );
        // The rows behind the larger block are the ones the merge needs.
        let mut got = Vec::new();
        while cursor.peek_ts().is_some() {
            got.push(cursor.next_record().expect("record").expect("a row").ts_ns);
            cursor
                .refill(&store, Some(&tracker), Some(&mut budget))
                .await
                .expect("refill");
        }
        assert_eq!(
            got,
            vec![8, 10, 12, 14],
            "the second block's rows are yielded in order once the growth is admitted"
        );
        // And the reconcile takes the ceiling back down to the actuals.
        reconcile_cursor_charge(&mut cursor, &mut charged);
        assert_eq!(charged, cursor.resident_bytes());
        assert!(
            charged < grown,
            "the reconcile lowers the grown ceiling again"
        );
    }

    /// ADR-0979 decision 4 as amended, end to end: the reconcile lowers a
    /// cursor's charge, another cursor is admitted into the budget that freed,
    /// and the first cursor then refills into a LARGER block. The grow-and-check
    /// is what keeps the sum of both cursors under the budget at that moment;
    /// without it the reservation stops being a bound the moment it is
    /// reconciled, and a run whose true peak is above the budget proceeds.
    ///
    /// Input A is [`grow_fixture_records`] (four tiny rows, then four 2,000-byte
    /// rows in a second block); input B is four tiny rows interleaved with A's
    /// first block, so B is admitted after A's first emit and is still open when
    /// A crosses into its second block. The threshold is computed, not observed:
    /// `resident_B + (A's metadata + A's raw + A's block-1 ceiling)`.
    ///
    /// On this fixture the threshold is 15,653 B (B's 1,240 B resident plus A's
    /// 14,413 B grown figure, a 13,075 B growth on A's 1,338 B reconciled
    /// charge), above both the admission figures it has to dominate for the grow
    /// to be the binding check (A's reservation 14,503 B, B's admission
    /// 7,698 B).
    ///
    /// Demonstrated red by deleting the grow-and-check from
    /// `StreamCursor::refill` (the `if let (Some(b), Some(block)) = ...` arm):
    /// the run one byte below the threshold then completes and `expect_err`
    /// panics -- the merge decoded a block that took it past its budget, which
    /// is the fail-open this test exists for.
    #[tokio::test]
    async fn merge_refuses_a_later_larger_block_the_budget_cannot_take() {
        let a = grow_fixture_records(0);
        let b: Vec<LogRecord> = (0..4i64)
            .map(|i| record(0, 1 + i * 2, "tiny", Vec::new()))
            .collect();
        let inputs = vec![(Uuid::from_u128(1), 1, a), (Uuid::from_u128(2), 2, b)];

        // Every figure below comes from the same functions the merge uses, on a
        // throwaway store seeded identically.
        let probe = MemoryStore::new();
        let cat_a = grow_fixture_catalog(&probe, Uuid::from_u128(1), 1, &inputs[0].2).await;
        let cat_b = grow_fixture_catalog(&probe, Uuid::from_u128(2), 2, &inputs[1].2).await;
        let (stream_id, _) = stream_ident(0);

        let r_a = cursor_reservation_bytes(&cat_a, &stream_id)
            .expect("reservation")
            .expect("A carries stream 0");
        let r_b = cursor_reservation_bytes(&cat_b, &stream_id)
            .expect("reservation")
            .expect("B carries stream 0");
        let cursor_a = open_cursor(&probe, &cat_a, 0, &stream_id, None)
            .await
            .expect("open A")
            .expect("A carries stream 0");
        let cursor_b = open_cursor(&probe, &cat_b, 1, &stream_id, None)
            .await
            .expect("open B")
            .expect("B carries stream 0");
        let resident_a = cursor_a.resident_bytes();
        let resident_b = cursor_b.resident_bytes();
        let ceiling_a1 = block_decode_ceiling_bytes(&cat_a.pricing, 1).expect("block 1 ceiling");
        // What A must be charged before it decodes its second block, and the
        // stream total at that moment: B is open and reconciled by then.
        let a_grown =
            loc_metadata_bytes(&cursor_a.locs) + cursor_a.raw_resident_bytes() + ceiling_a1;
        let threshold = resident_b + a_grown;

        // The fixture only pins the grow if the grow is the run's binding
        // figure: both admissions have to fit strictly below it, or a budget of
        // `threshold - 1` would refuse at an admission instead.
        assert!(
            threshold > r_a,
            "A's admission ({r_a} B) must fit below the grow threshold ({threshold} B)"
        );
        assert!(
            threshold > resident_a + r_b,
            "B's admission ({} B) must fit below the grow threshold ({threshold} B)",
            resident_a + r_b
        );

        let store = MemoryStore::new();
        for (writer_id, seq, recs) in &inputs {
            seed_l0(&store, *writer_id, *seq, recs, grow_fixture_cfg(), &[]).await;
        }
        let clock = FixedClock::new(sealed_now_ns());
        let under = CompactorConfig {
            merge_cursor_budget_bytes: threshold - 1,
            ..CompactorConfig::default()
        };
        let err = compact_bucket(&store, &clock, &under, &bucket())
            .await
            .expect_err("a budget below the grow threshold must fail closed");
        match err {
            MaintainError::MergeCursorBudgetExceeded {
                stream_id: got_stream,
                open_cursors,
                charged_bytes,
                budget_bytes,
                required_bytes,
                inputs_carrying_stream,
                site,
            } => {
                assert_eq!(got_stream, stream_id.to_hex());
                assert_eq!(
                    open_cursors, 2,
                    "both cursors are open when A crosses into its larger block"
                );
                assert_eq!(
                    charged_bytes,
                    resident_a + resident_b,
                    "the charge at refusal is both cursors' reconciled residency"
                );
                assert_eq!(budget_bytes, threshold - 1);
                assert_eq!(required_bytes, threshold);
                assert_eq!(inputs_carrying_stream, 2);
                assert_eq!(
                    site,
                    MergeCursorBudgetSite::BlockGrow {
                        block_index: 1,
                        grow_bytes: a_grown - resident_a,
                    },
                    "the refusal is the block growth, not an admission"
                );
            }
            other => panic!("expected MergeCursorBudgetExceeded, got {other:?}"),
        }

        // One byte above the refused budget -- the threshold itself -- the run
        // completes, with parts byte-identical to an unbudgeted run.
        let exact = CompactorConfig {
            merge_cursor_budget_bytes: threshold,
            ..CompactorConfig::default()
        };
        let with_budget = compact_grow_fixture_hashes(&inputs, &exact).await;
        let unbudgeted = compact_grow_fixture_hashes(&inputs, &CompactorConfig::default()).await;
        assert!(!with_budget.is_empty());
        assert_eq!(
            with_budget, unbudgeted,
            "a budget at exactly the grow threshold must not change the output"
        );
    }

    /// [`compact_part_hashes`] for inputs written with [`grow_fixture_cfg`].
    async fn compact_grow_fixture_hashes(
        inputs: &[(Uuid, u64, Vec<LogRecord>)],
        config: &CompactorConfig,
    ) -> Vec<Vec<u8>> {
        let store = MemoryStore::new();
        for (writer_id, seq, recs) in inputs {
            seed_l0(&store, *writer_id, *seq, recs, grow_fixture_cfg(), &[]).await;
        }
        let clock = FixedClock::new(sealed_now_ns());
        compact_bucket(&store, &clock, config, &bucket())
            .await
            .expect("compact");
        let (rec, _parts) = read_output(&store).await;
        rec.parts.iter().map(|p| p.content_hash.clone()).collect()
    }

    /// Rewrite `obj` with its PAGE_DIR descriptor dropped from the footer,
    /// leaving an object that still OPENS (PAGE_DIR is not one of the
    /// footer-mandatory section kinds) but that the bounded merge cannot
    /// admission-price. The section's bytes stay in the body; only its footer
    /// descriptor is removed, and the footer crc is recomputed by
    /// [`footer::write_footer_and_trailer`].
    fn strip_page_dir_descriptor(obj: &[u8]) -> Vec<u8> {
        // footer_len(4) + footer_crc(4) + version(2) + signal(1) + reserved(1)
        // + magic(4).
        const TRAILER_LEN: usize = 16;
        let total = obj.len();
        let footer_len = u32::from_le_bytes([
            obj[total - TRAILER_LEN],
            obj[total - TRAILER_LEN + 1],
            obj[total - TRAILER_LEN + 2],
            obj[total - TRAILER_LEN + 3],
        ]) as usize;
        let footer_start = total - TRAILER_LEN - footer_len;
        let mut ftr = footer::open(obj).expect("open seeded object");
        assert!(
            ftr.section(kind::PAGE_DIR).is_some(),
            "the seeded v4 object must carry PAGE_DIR to strip it"
        );
        ftr.sections.retain(|s| s.kind != kind::PAGE_DIR);
        let mut out = obj[..footer_start].to_vec();
        footer::write_footer_and_trailer(&mut out, &ftr);
        out
    }

    /// ADR-0979 decision 4: an input carrying no PAGE_DIR cannot be
    /// admission-priced (its decoded page term is unknowable before the fetch),
    /// so the bounded merge refuses it at catalog load with a typed error naming
    /// the object and its format version, rather than reading it under a guessed
    /// cost. PAGE_DIR is mandatory in v4, so this is a version/corruption gate.
    #[tokio::test]
    async fn page_dir_less_input_is_refused_with_typed_error() {
        let store = MemoryStore::new();
        let recs = vec![record(0, 1, "x", Vec::new())];
        let bytes = seed(&store, Uuid::from_u128(1), 1, &recs).await;
        let stripped = strip_page_dir_descriptor(&bytes);
        // Re-open to confirm the stripped object still parses (the refusal is a
        // deliberate policy, not a decode failure).
        let reopened = footer::open(&stripped).expect("stripped object still opens");
        assert!(
            reopened.section(kind::PAGE_DIR).is_none(),
            "the stripped object must carry no PAGE_DIR"
        );

        let key = "l0/synthetic-no-page-dir".to_string();
        store
            .put(&key, Bytes::from(stripped), PutOptions::default())
            .await
            .expect("put");
        let err = load_catalog_from_object(&store, &CompactorConfig::default(), key.clone(), true)
            .await
            .expect_err("a PAGE_DIR-less input must be refused");
        match err {
            MaintainError::MergeCursorInputMissingPageDir {
                object_key,
                format_version,
            } => {
                assert_eq!(object_key, key);
                assert_eq!(format_version, OUTPUT_FORMAT_VERSION);
            }
            other => panic!("expected MergeCursorInputMissingPageDir, got {other:?}"),
        }
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
