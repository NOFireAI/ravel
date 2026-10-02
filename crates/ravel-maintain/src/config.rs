//! Compactor configuration: the seal margin, the trigger threshold, the two
//! part-split targets (stored bytes and decoded heap; neither is a ceiling on
//! resident memory, see [`CompactorConfig::l1_part_memory_target_bytes`]),
//! and the abandonment deadline, plus the sweep/retention knobs (grace, protection
//! horizon, ADR-0019 per-tenant retention windows). All durations are
//! nanoseconds to match the injected [`crate::clock::Clock`].

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use ravel_types::{TenantHash, TenantId};
use uuid::Uuid;

use crate::request_ledger::RequestLedger;

/// A test-injectable accounting hook for the RLOG and RSPAN compaction
/// merges' peak resident memory (RLOG and RSPAN, ADR-0065 decision 4).
///
/// The RLOG k-way merge ([`crate::rlog`]) and the RSPAN k-way merge
/// ([`crate::rspan_codec`]) drive this at their real allocation/decode points
/// so a test can assert each merge's residency is bounded independently of a
/// stream's or trace's size. It is deliberately *load-bearing*, not
/// decorative: the merge calls [`Self::block_fetched`] when it fetches one
/// input block's raw bytes, [`Self::block_decoded`]/[`Self::block_released`] as
/// each decoded block enters and leaves a cursor, and [`Self::set_writer_bytes`]
/// as records accumulate in the in-progress part's writer. If a merge ever
/// regressed to decoding a whole stream/object at once, the `decoded` term
/// would grow with its size and the recorded high-water would break the
/// test's bound.
///
/// # What the `decoded` term counts (ADR-0979 decision 1)
///
/// Whatever the caller's cursor actually holds resident for the block, and
/// exactly one charge per cursor per block:
///
/// - the RLOG merge holds its block in COLUMNAR form
///   ([`ravel_logseg::StreamBlockRows`]) and materializes one record at a
///   time, so it charges the view's `heap_estimate()`. It used to hold the
///   block's records and charge the `estimate_record` sum over them; that
///   charge is gone, not additional. Charging both would count one cursor
///   twice in the same phase snapshot, at a per-record term whose records no
///   longer exist.
/// - the RSPAN merge still holds row-form samples and charges their row-form
///   estimate, which is the same rule: charge what is resident.
///
/// Both are decoded heap, so the term stays one byte kind and the high-water
/// stays comparable across signals; only the shape the bytes are held in
/// differs. [`Self::peak_cursor_decoded_bytes`] reads this term alone.
///
/// Two combined high-water marks are kept:
///
/// - [`Self::peak_transient_bytes`]: `fetched + decoded`, the merge's *own*
///   decode-side buffers. This is the quantity these merges bound: at most
///   one raw block plus one decoded block per input, so it is
///   `O(input_count * block_size)` and does NOT scale with stream/trace size.
/// - [`Self::peak_total_bytes`]: `fetched + decoded + writer + probe`, adding
///   the in-progress part's writer buffer and the in-flight exact-encode probe.
///   The writer term tracks the memory split
///   target `l1_part_memory_target_bytes` (for RLOG,
///   [`CompactorConfig::rlog_memory_target_bytes`]; a part is flushed once its
///   record-heap estimate reaches that target) and is the unavoidable
///   content-addressing cost the ADR calls out: a part's key does not exist
///   until the whole part is buffered. It is a target, not a ceiling: the RLOG
///   merge checks it after every record, so it overshoots by at most one
///   record, while the RSPAN merge checks it only at a trace boundary and
///   overshoots by up to one whole trace
///   ([`CompactorConfig::l1_part_memory_target_bytes`]). The RLOG merge may
///   also close a part earlier on the stored-size target `max_l1_part_bytes`
///   (issue #872), which only lowers the writer term but adds the probe term:
///   the exact-encode probe encodes a CLONE of the buffered records, so while
///   it runs the resident set holds a second copy of the part's row heap plus
///   the encoded object those records produce. That is charged, not assumed
///   away ([`Self::set_probe_bytes`]), so a probing run's high-water is roughly
///   `2 * writer + one part's object bytes` rather than the `writer` a run
///   without probes reports. The probe only runs once the payload proxy reaches
///   the stored target. At the derived defaults the RLOG stored-size cap follows
///   the derived memory target ([`CompactorConfig::rlog_max_l1_part_bytes`]), and
///   the proxy never exceeds the record-heap estimate that the memory target is
///   measured in, so the memory target closes every part first and this term
///   stays zero; the `derived_defaults_run_zero_probes_and_close_on_the_memory_target`
///   test in `rlog.rs` pins that. An operator who sets the stored-size cap below
///   the memory target, or the memory target above the cap, opts into probes.
///
/// # Phase-attributed peaks (issue #977)
///
/// The two combined marks above pool bytes of different kinds (raw fetched,
/// decoded heap, writer heap) into one figure and omit the two terms that
/// actually dominate a large-bucket compaction: the closed parts retained in
/// [`crate::rlog::PartSink`] and the per-input catalog directories. So the
/// tracker also records a peak PER PHASE, read back as a [`MergePhasePeaks`]
/// via [`Self::phase_peaks`], each field naming which bytes it counts:
///
/// - catalog load ([`Self::add_catalog_directory_bytes`]): decoded
///   directory-section payload bytes retained per input.
/// - merge and cursors ([`Self::peak_transient_bytes`]): the decode-side
///   buffers above.
/// - in-progress part writer ([`Self::peak_writer_bytes`]): the current part's
///   accumulated records, decoded heap.
/// - retained closed parts ([`Self::add_retained_part_bytes`]): the encoded
///   bytes of every closed part still held resident until publish. Since
///   ADR-0979 decision 3 the RLOG and RSEG compaction paths release each part's
///   bytes at PUT, so this term is ZERO there; it is nonzero only for a path
///   that defers its PUTs and keeps the bytes (the erasure rewrite).
/// - finish and publish ([`Self::set_publish_record_bytes`]): the encoded
///   compaction-record payload, the only allocation the publish phase adds on
///   top of the retained parts.
///
/// Two fields of different byte kinds (encoded vs decoded heap) must never be
/// summed (the repo measurement rule). The tracker is one PER CONCURRENT
/// COMPACTION RUN: the retained-parts and catalog terms accumulate and are
/// not released within a run. SERIAL reuse across buckets is repaired
/// mechanically -- `rewrite_and_publish` calls [`Self::reset_for_run`] before
/// any accounting, so each bucket's figures are its own -- but two runs
/// sharing one tracker CONCURRENTLY still combine, and that contract is the
/// installer's to keep.
///
/// Production never installs one (`CompactorConfig::merge_memory_tracker` is
/// `None`), so the hooks compile to a single `Option` check and add nothing.
/// Wiring the service to install one (a one-line
/// `merge_memory_tracker: Some(MergeMemoryTracker::new())` where it builds the
/// [`CompactorConfig`]) is what surfaces [`Self::phase_peaks`] to an operator
/// through the `tracing::info!` event `rewrite_and_publish` emits when a
/// tracker is present.
#[derive(Clone, Debug, Default)]
pub struct MergeMemoryTracker {
    inner: Arc<MergeMemoryInner>,
}

#[derive(Debug, Default)]
struct MergeMemoryInner {
    /// Raw block bytes fetched but not yet decoded-and-dropped.
    fetched: AtomicU64,
    /// Decoded bytes currently resident across all merge cursors: the RLOG
    /// merge's columnar `heap_estimate()` per open cursor since ADR-0979
    /// decision 1, the RSPAN merge's row-form estimate. Decoded heap either
    /// way, one charge per cursor.
    decoded: AtomicU64,
    /// High-water of `decoded` ALONE, unpooled with `fetched`. The cursor
    /// phase's two terms are different byte kinds (stored/compressed against
    /// decoded heap), and only this one is what a cursor's columnar block
    /// costs, so it is the term ADR-0979 decision 1's accounting is stated in
    /// and the one a bound on open cursors is checked against.
    peak_decoded: AtomicU64,
    /// The in-progress part's accumulated record-byte estimate in the writer.
    writer: AtomicU64,
    /// What an in-flight exact-encode probe holds on top of the writer buffer:
    /// the clone of the part's records plus, once the encode returns, the
    /// encoded object bytes. Zero whenever no probe is running.
    probe: AtomicU64,
    /// High-water of `probe` alone.
    peak_probe: AtomicU64,
    /// High-water of `fetched + decoded`.
    peak_transient: AtomicU64,
    /// High-water of `fetched + decoded + writer + probe`.
    peak_total: AtomicU64,
    /// Parts closed because the decoded record-heap estimate reached
    /// `l1_part_memory_target_bytes` (the memory split target fired).
    memory_target_flushes: AtomicU64,
    /// Parts closed because the encoded-bytes estimate reached
    /// `max_l1_part_bytes` (the stored-size target fired).
    stored_target_flushes: AtomicU64,
    /// Exact-encode probes the RLOG merge ran: one per
    /// [`crate::rlog::PartBuilder::encode_clone`] call, whether or not it closed
    /// the part. A probe is an O(part) encode, so this is the cost side of the
    /// stored-size target, and it is zero whenever the payload proxy never
    /// reaches the RLOG stored-size cap ([`CompactorConfig::rlog_stored_target_bytes`]).
    probes_run: AtomicU64,
    /// Live sum of the encoded/on-object bytes of closed parts still retained in
    /// [`crate::rlog::PartSink::parts`] after PUT. Zero on the bounded RLOG
    /// compaction path, which releases each part's bytes at PUT (ADR-0979
    /// decision 3); nonzero and monotonic within a run only for a path that
    /// keeps the bytes until its own deferred publish (the erasure rewrite).
    retained_parts: AtomicU64,
    /// High-water of `retained_parts`.
    peak_retained_parts: AtomicU64,
    /// Live sum of encoded directory-section bytes (STREAM_DIR + FIELD_DIR +
    /// SKIP_IDX + PAGE_DIR) retained per input during catalog load.
    catalog_directory: AtomicU64,
    /// High-water of `catalog_directory`.
    peak_catalog_directory: AtomicU64,
    /// High-water of the in-progress part writer term ALONE (decoded heap),
    /// separate from `peak_total`, which pools it with the cursor terms.
    peak_writer: AtomicU64,
    /// High-water of the published compaction record's encoded protobuf payload.
    peak_publish_record: AtomicU64,
    /// High-water of the number of cursors open at once for one stream
    /// (ADR-0979 decision 2). The RLOG merge admits a stream's per-input cursors
    /// only when their SKIP_IDX ts-envelope overlaps the merge frontier and
    /// releases each as it drains, so this is the max concurrent ts-overlap `D`
    /// of the stream's input slices, not the number `n` of inputs carrying the
    /// stream. `fetch_max` across streams keeps the largest per-stream peak, so
    /// the field answers "the most cursors any single stream needed open at
    /// once", the quantity that bounds the merge's cursor fan-out.
    max_open_cursors: AtomicU64,
}

/// A compaction merge's peak resident memory split by the phase that caused it
/// (issue #977). Each field NAMES which bytes it counts; two fields of
/// different byte kinds (encoded vs decoded heap) must never be summed (the
/// repo measurement rule). Read from a [`MergeMemoryTracker`] via
/// [`MergeMemoryTracker::phase_peaks`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MergePhasePeaks {
    /// Input read / catalog load: high-water of the decoded directory-section
    /// payload bytes (STREAM_DIR + FIELD_DIR + SKIP_IDX + PAGE_DIR, after
    /// section decompression) retained across all inputs' catalogs. Decoded
    /// payload bytes, NOT on-object encoded lengths: the reader retains the
    /// decoded form, and this is a residency figure.
    pub catalog_directory_decoded_bytes: u64,
    /// Merge and cursors: high-water of the k-way merge's own decode-side
    /// buffers, one raw fetched unit plus one decoded block per input
    /// (`fetched + decoded`, the existing [`MergeMemoryTracker::peak_transient_bytes`]).
    /// Mixed raw-encoded plus decoded heap.
    pub cursor_bytes: u64,
    /// In-progress part writer: high-water of the current part builder's
    /// accumulated records. Decoded heap bytes.
    pub writer_heap_bytes: u64,
    /// Retained closed parts: high-water of the encoded bytes of closed parts
    /// still held in [`crate::rlog::PartSink::parts`] until publish.
    /// Encoded/on-object bytes. Zero on the bounded compaction path, which
    /// releases each part's bytes at PUT (ADR-0979 decision 3); nonzero only for
    /// a deferred-PUT path that keeps them (the erasure rewrite).
    pub retained_part_encoded_bytes: u64,
    /// Finish and publish: the published compaction record's encoded protobuf
    /// payload, the only allocation the publish phase adds on top of the
    /// retained parts. Encoded bytes.
    pub publish_record_encoded_bytes: u64,
    /// In-progress part measurement: high-water of the exact-encode probe term
    /// (the cloned record heap plus the encoded probe object, mixed decoded
    /// plus encoded kinds). Zero for a run that never probed.
    pub probe_bytes: u64,
}

impl MergeMemoryTracker {
    /// A fresh tracker with every counter at zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// Account `bytes` of raw block bytes just fetched (before decode).
    pub fn block_fetched(&self, bytes: u64) {
        self.inner.fetched.fetch_add(bytes, Ordering::Relaxed);
        self.note();
    }

    /// Account a block decode: the `raw` bytes are about to be dropped and
    /// `decoded` bytes take their place. Called after the decoded form exists
    /// but before the raw buffer is released, so the high-water captures the
    /// instant both are resident.
    ///
    /// `decoded` is what the cursor holds resident until it releases the block:
    /// the columnar view's `heap_estimate()` on the RLOG path, the row-form
    /// estimate on the RSPAN path. One charge per cursor per block; see the
    /// type docs.
    pub fn block_decoded(&self, raw: u64, decoded: u64) {
        self.inner.decoded.fetch_add(decoded, Ordering::Relaxed);
        self.note();
        self.inner.fetched.fetch_sub(raw, Ordering::Relaxed);
    }

    /// Account a decoded block leaving a cursor (its rows were drained into the
    /// writer and the block's buffer is dropped). `decoded` must be the value
    /// [`Self::block_decoded`] charged for that same block.
    pub fn block_released(&self, decoded: u64) {
        self.inner.decoded.fetch_sub(decoded, Ordering::Relaxed);
    }

    /// Set the in-progress part's writer buffer estimate to `bytes`. Passing 0
    /// on flush records that the part's buffer was handed off and released.
    pub fn set_writer_bytes(&self, bytes: u64) {
        self.inner.writer.store(bytes, Ordering::Relaxed);
        self.note();
    }

    /// Set what the in-flight exact-encode probe holds resident, or 0 once it
    /// has returned and its buffers are dropped.
    ///
    /// The RLOG stored-size target measures a part by encoding a CLONE of its
    /// buffered records ([`crate::rlog::PartBuilder::encode_clone`]), so for the
    /// duration of that encode the run holds a second copy of the part's record
    /// heap and then the encoded object it produces. The merge charges the clone
    /// before the encode and the clone plus the object as it returns, so
    /// [`Self::peak_total_bytes`] covers the probe's residency instead of
    /// under-reporting the peak of every run where a probe fires. Mixed byte
    /// kinds, like [`Self::peak_total_bytes`] itself: decoded heap for the clone,
    /// encoded object bytes for the object.
    pub fn set_probe_bytes(&self, bytes: u64) {
        self.inner.probe.store(bytes, Ordering::Relaxed);
        self.note();
    }

    /// High-water of the exact-encode probe term alone (cloned record heap plus
    /// the encoded object, mixed kinds). Zero for a run that never probed.
    pub fn peak_probe_bytes(&self) -> u64 {
        self.inner.peak_probe.load(Ordering::Relaxed)
    }

    /// Recompute both high-water marks from the live counters.
    fn note(&self) {
        let fetched = self.inner.fetched.load(Ordering::Relaxed);
        let decoded = self.inner.decoded.load(Ordering::Relaxed);
        let writer = self.inner.writer.load(Ordering::Relaxed);
        let probe = self.inner.probe.load(Ordering::Relaxed);
        let transient = fetched.saturating_add(decoded);
        let total = transient.saturating_add(writer).saturating_add(probe);
        self.inner
            .peak_transient
            .fetch_max(transient, Ordering::Relaxed);
        self.inner.peak_total.fetch_max(total, Ordering::Relaxed);
        self.inner.peak_writer.fetch_max(writer, Ordering::Relaxed);
        self.inner.peak_probe.fetch_max(probe, Ordering::Relaxed);
        self.inner
            .peak_decoded
            .fetch_max(decoded, Ordering::Relaxed);
    }

    /// Account `bytes` of encoded part bytes still resident in
    /// [`crate::rlog::PartSink::parts`] after a closed part is PUT. On the
    /// bounded compaction path `bytes` is 0, because the part's bytes were
    /// released at PUT (ADR-0979 decision 3), so this term stays flat at zero;
    /// on a deferred-PUT path (the erasure rewrite) `bytes` is the part's encoded
    /// size and the high-water is the retained-parts plateau. Encoded/on-object
    /// bytes, not heap; it is deliberately a separate term from the writer's
    /// decoded-heap bytes so a report never folds the two together.
    pub fn add_retained_part_bytes(&self, bytes: u64) {
        let updated = self
            .inner
            .retained_parts
            .fetch_add(bytes, Ordering::Relaxed)
            .saturating_add(bytes);
        self.inner
            .peak_retained_parts
            .fetch_max(updated, Ordering::Relaxed);
    }

    /// Account `bytes` of encoded directory-section bytes (STREAM_DIR +
    /// FIELD_DIR + SKIP_IDX + PAGE_DIR) retained for one input's catalog, called
    /// once per input as its catalog is loaded. Accumulates across inputs and is
    /// not released within a run.
    pub fn add_catalog_directory_bytes(&self, bytes: u64) {
        let updated = self
            .inner
            .catalog_directory
            .fetch_add(bytes, Ordering::Relaxed)
            .saturating_add(bytes);
        self.inner
            .peak_catalog_directory
            .fetch_max(updated, Ordering::Relaxed);
    }

    /// Record the encoded protobuf payload size of the published compaction
    /// record: the finish/publish phase's own allocation on top of the retained
    /// parts. Encoded bytes.
    pub fn set_publish_record_bytes(&self, bytes: u64) {
        self.inner
            .peak_publish_record
            .fetch_max(bytes, Ordering::Relaxed);
    }

    /// Note that `open` cursors are simultaneously open for the stream being
    /// merged (ADR-0979 decision 2). Called by the RLOG merge each time a
    /// cursor is admitted, so the recorded high-water is the largest number of
    /// cursors any single stream held open at once. `fetch_max`, so a later
    /// stream that opened fewer never lowers it.
    pub fn note_open_cursors(&self, open: u64) {
        self.inner
            .max_open_cursors
            .fetch_max(open, Ordering::Relaxed);
    }

    /// High-water of the number of cursors open at once for any single stream
    /// (ADR-0979 decision 2): the max concurrent ts-overlap `D` of a stream's
    /// input slices, the quantity overlap-gated admission bounds. Read like
    /// [`Self::peak_cursor_decoded_bytes`], after the merge.
    pub fn max_open_cursors_per_stream(&self) -> u64 {
        self.inner.max_open_cursors.load(Ordering::Relaxed)
    }

    /// High-water of the merge's decode-side buffers (`fetched + decoded`).
    /// Bounded by `O(input_count * block_size)`, independent of stream size.
    pub fn peak_transient_bytes(&self) -> u64 {
        self.inner.peak_transient.load(Ordering::Relaxed)
    }

    /// High-water of the cursor phase's DECODED term alone: the sum, over the
    /// cursors open at once, of what each holds decoded for its current block.
    /// On the RLOG path that is each open cursor's
    /// [`ravel_logseg::StreamBlockRows::heap_estimate`] (ADR-0979 decision 1),
    /// and nothing else -- the raw fetched bytes are stored/compressed bytes of
    /// a different kind and are not summed in here, and no row-form
    /// `estimate_record` term is charged alongside it.
    pub fn peak_cursor_decoded_bytes(&self) -> u64 {
        self.inner.peak_decoded.load(Ordering::Relaxed)
    }

    /// High-water of the merge's total residency
    /// (`fetched + decoded + writer + probe`), the decode-side buffers plus the
    /// in-progress part's writer buffer plus whatever an in-flight exact-encode
    /// probe holds ([`Self::set_probe_bytes`]).
    pub fn peak_total_bytes(&self) -> u64 {
        self.inner.peak_total.load(Ordering::Relaxed)
    }

    /// High-water of the retained closed parts (encoded/on-object bytes held in
    /// [`crate::rlog::PartSink::parts`] until publish).
    pub fn peak_retained_part_bytes(&self) -> u64 {
        self.inner.peak_retained_parts.load(Ordering::Relaxed)
    }

    /// High-water of the per-input catalog directory bytes (encoded STREAM_DIR +
    /// FIELD_DIR + SKIP_IDX + PAGE_DIR, summed over the inputs held at once).
    pub fn peak_catalog_directory_bytes(&self) -> u64 {
        self.inner.peak_catalog_directory.load(Ordering::Relaxed)
    }

    /// High-water of the in-progress part writer term alone (decoded heap),
    /// unmixed with the cursor terms that [`Self::peak_total_bytes`] pools in.
    pub fn peak_writer_bytes(&self) -> u64 {
        self.inner.peak_writer.load(Ordering::Relaxed)
    }

    /// High-water of the published compaction record's encoded payload bytes.
    pub fn peak_publish_record_bytes(&self) -> u64 {
        self.inner.peak_publish_record.load(Ordering::Relaxed)
    }

    /// Clear every counter and high-water term for a NEW run. Called at the
    /// start of each bucket's rewrite, so a tracker left installed in a
    /// long-lived [`CompactorConfig`] reports each bucket's own peaks under
    /// that bucket's identity instead of a cumulative maximum, and every
    /// post-run read (the emission, a test, a future CLI surface) stays
    /// valid. Wrong to call mid-run; CONCURRENT runs sharing one tracker
    /// still produce combined figures, and that contract is the installer's
    /// to keep (one tracker per concurrent run).
    pub fn reset_for_run(&self) {
        for field in [
            &self.inner.fetched,
            &self.inner.decoded,
            &self.inner.writer,
            &self.inner.probe,
            &self.inner.peak_probe,
            &self.inner.peak_transient,
            &self.inner.peak_decoded,
            &self.inner.peak_total,
            &self.inner.memory_target_flushes,
            &self.inner.stored_target_flushes,
            &self.inner.probes_run,
            &self.inner.retained_parts,
            &self.inner.peak_retained_parts,
            &self.inner.catalog_directory,
            &self.inner.peak_catalog_directory,
            &self.inner.peak_writer,
            &self.inner.peak_publish_record,
            &self.inner.max_open_cursors,
        ] {
            field.store(0, Ordering::Relaxed);
        }
    }

    /// The full phase split, each term naming its byte kind. See
    /// [`MergePhasePeaks`]; do not sum fields of different kinds.
    pub fn phase_peaks(&self) -> MergePhasePeaks {
        MergePhasePeaks {
            catalog_directory_decoded_bytes: self.peak_catalog_directory_bytes(),
            cursor_bytes: self.peak_transient_bytes(),
            writer_heap_bytes: self.peak_writer_bytes(),
            retained_part_encoded_bytes: self.peak_retained_part_bytes(),
            publish_record_encoded_bytes: self.peak_publish_record_bytes(),
            probe_bytes: self.peak_probe_bytes(),
        }
    }

    /// Record that a part was closed by the memory split target (its decoded
    /// record-heap estimate reached `l1_part_memory_target_bytes`).
    pub fn note_memory_target_flush(&self) {
        self.inner
            .memory_target_flushes
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record that a part was closed by the stored-size target (an exact-encode
    /// probe showed its object bytes reaching `max_l1_part_bytes`).
    pub fn note_stored_target_flush(&self) {
        self.inner
            .stored_target_flushes
            .fetch_add(1, Ordering::Relaxed);
    }

    /// How many parts closed because the memory split target fired.
    pub fn memory_target_flushes(&self) -> u64 {
        self.inner.memory_target_flushes.load(Ordering::Relaxed)
    }

    /// How many parts closed because the stored-size target fired.
    pub fn stored_target_flushes(&self) -> u64 {
        self.inner.stored_target_flushes.load(Ordering::Relaxed)
    }

    /// Record that the RLOG merge ran one exact-encode probe (an O(part)
    /// encode), whether or not it closed the part.
    pub fn note_probe_run(&self) {
        self.inner.probes_run.fetch_add(1, Ordering::Relaxed);
    }

    /// How many exact-encode probes the run made. Zero when the payload proxy
    /// never reached the RLOG stored-size cap; a test asserts that rather than
    /// assuming it.
    pub fn probes_run(&self) -> u64 {
        self.inner.probes_run.load(Ordering::Relaxed)
    }
}

/// Nanoseconds in one hour; an ingest-hour bucket spans exactly this.
pub const NS_PER_HOUR: i64 = 3_600_000_000_000;

/// Default `max_flush_lifetime`: 1 hour (matches ravel-ingest and
/// ravel-catalog, ADR-0010 §11).
pub const DEFAULT_MAX_FLUSH_LIFETIME_NS: i64 = NS_PER_HOUR;
/// Default `clock_skew_allowance`: 5 minutes (matches ravel-catalog).
pub const DEFAULT_CLOCK_SKEW_ALLOWANCE_NS: i64 = 300_000_000_000;
/// Default `max_compaction_lifetime`: 1 hour. Mirrors the
/// writer interlock so the sweeper's unreferenced-part rule is safe.
pub const DEFAULT_MAX_COMPACTION_LIFETIME_NS: i64 = NS_PER_HOUR;
/// Default `max_l1_part_bytes`: 256 MiB. This is the **stored-size target**:
/// the encoded/on-object byte budget a part is closed at, estimated per section
/// in `build.rs` and measured by an exact-encode probe in the RLOG merge. It is
/// deliberately equal to the memory-target default so this crate's geometry is
/// unchanged from when one knob did both jobs: on a wide schema the memory
/// target reaches 256 MiB of decoded record heap long before a part's stored
/// bytes reach 256 MiB, so the memory target stays binding, the RLOG payload
/// proxy never reaches 256 MiB, and no probe runs.
///
/// Which target binds therefore decides object size: on a wide schema it is
/// the memory split target, and objects come out at that target divided by the
/// schema's decoded-heap-to-stored ratio. Lowering this knob does not grow
/// objects, it caps them lower: once it drops below the size the memory target
/// already yields it becomes the binding target and every part closes at it.
/// The binaries derive the RLOG memory split target from the memory budget
/// ([`derive_l1_part_memory_target_bytes`]) and the RLOG merge's stored-size cap
/// follows that derived target ([`CompactorConfig::rlog_max_l1_part_bytes`]), so
/// on a host with memory to spare RLOG objects grow past this default; RSEG
/// keeps reading this value and RSPAN reads neither cap. See
/// [`CompactorConfig::max_l1_part_bytes`].
pub const DEFAULT_MAX_L1_PART_BYTES: u64 = 256 * 1024 * 1024;
/// `l1_part_memory_target_bytes` in [`CompactorConfig::default`]: 256 MiB. This
/// is the **memory split target**: the decoded record-heap estimate the
/// in-progress part is closed at. It is a split target, not a ceiling on
/// resident bytes; see [`CompactorConfig::l1_part_memory_target_bytes`] for
/// what each path overshoots it by.
///
/// The RSPAN merge runs at this value unless the operator sets the knob. The
/// RLOG merge does not when the knob is unset: `ravel-server` and `ravel-cli
/// maintain` derive its target from the memory budget
/// ([`derive_l1_part_memory_target_bytes`]), and this is the floor of that
/// derivation and the value used when the budget is unknown. The floor binds
/// only while the budget the derivation divides ([`merge_memory_budget_bytes`])
/// satisfies `budget / 8 / concurrent_merges <= 256 MiB`. With the default
/// 20 GiB merge cursor budget and the 2 GiB overhead reserve already deducted
/// from that budget, that is a host of at most `22 GiB + 2 GiB *
/// concurrent_merges` for `ravel-cli maintain` (`compact-bucket` is one merge:
/// 24 GiB or less) and, for `ravel-server` at `--maintain-unit-concurrency` 4,
/// a host of 30 GiB or less.
pub const DEFAULT_L1_PART_MEMORY_TARGET_BYTES: u64 = 256 * 1024 * 1024;
/// Floor of the derived memory split target
/// ([`derive_l1_part_memory_target_bytes`]): the fixed default the target had
/// before it was derived, so no host gets smaller parts than it did then.
pub const MIN_DERIVED_L1_PART_MEMORY_TARGET_BYTES: u64 = DEFAULT_L1_PART_MEMORY_TARGET_BYTES;
/// Ceiling of the derived memory split target
/// ([`derive_l1_part_memory_target_bytes`]): 8 GiB.
pub const MAX_DERIVED_L1_PART_MEMORY_TARGET_BYTES: u64 = 8 * 1024 * 1024 * 1024;
/// Share of the memory budget the derived memory split targets of all
/// concurrent merges add up to: one eighth.
pub const L1_PART_MEMORY_TARGET_BUDGET_DIVISOR: u64 = 8;

/// The overhead reserve both binaries deduct from host memory before deriving
/// anything from it: 2 GiB, for the binary, thread stacks, the runtime and
/// tracing buffers. `ravel-server` derives its memory budget as effective memory
/// less this (`ravel_server::config::MEMORY_OVERHEAD_RESERVE_BYTES` re-exports
/// it) and `ravel-cli maintain` deducts it in [`host_memory_budget_bytes`], so
/// the two derive from one definition of "memory the merges may use".
///
/// A provisional round figure, not a measurement: well above the few hundred MiB
/// an idle process costs before its first query, so a flag combination is not
/// falsely refused for lack of the real number.
pub const MEMORY_OVERHEAD_RESERVE_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Host memory less [`MEMORY_OVERHEAD_RESERVE_BYTES`], floored at zero: the
/// budget `ravel-cli maintain` starts its derivation from before
/// [`merge_memory_budget_bytes`] deducts the merge cursor budget.
pub fn host_memory_budget_bytes(host_memory_total_bytes: u64) -> u64 {
    host_memory_total_bytes.saturating_sub(MEMORY_OVERHEAD_RESERVE_BYTES)
}

/// The memory the RLOG part-split derivation divides among concurrent merges:
/// `process_budget_bytes` less `merge_cursor_budget_bytes`, floored at zero
/// BEFORE [`derive_l1_part_memory_target_bytes`] divides it.
///
/// The merge cursor budget (ADR-0979 decision 4, [`CompactorConfig::
/// merge_cursor_budget_bytes`]) is memory a merge is allowed to hold in its
/// cursors on top of the in-progress part's writer buffer, and by default it is
/// 20 GiB ([`DEFAULT_MERGE_CURSOR_BUDGET_BYTES`]). A derivation that took one
/// eighth of the whole budget would hand the writer buffer memory the cursors
/// already claim. `process_budget_bytes` is host memory already net of
/// [`MEMORY_OVERHEAD_RESERVE_BYTES`] (`ravel-server`'s `memory_budget_bytes`, or
/// [`host_memory_budget_bytes`] for `ravel-cli maintain`).
///
/// Worked figures at the default 20 GiB cursor budget: a 32 GiB host under
/// `compact-bucket` is `32 - 2 - 20 = 10 GiB`, so `10 / 8 = 1.25 GiB`;
/// `ravel-server` on a 30 GiB host is `30 - 2 - 20 = 8 GiB`, so
/// `8 / 8 / 4 = 256 MiB` at `--maintain-unit-concurrency` 4. A process that runs
/// several merges at once passes the cursor memory its merges hold TOGETHER:
/// `ravel-cli maintain compact-tenant` splits the default budget across
/// `--bucket-concurrency` buckets (`per_bucket_config`) and passes the sum, the
/// whole budget. `ravel-server` passes the per-merge budget once, which
/// undercounts its `--maintain-unit-concurrency` concurrent merges.
pub fn merge_memory_budget_bytes(process_budget_bytes: u64, merge_cursor_budget_bytes: u64) -> u64 {
    process_budget_bytes.saturating_sub(merge_cursor_budget_bytes)
}

/// The largest part the claim lease supports without tripping
/// [`claim_lease_below_warn_threshold`]: the part whose encode and PUT at
/// [`CLAIM_LEASE_WARN_CONSERVATIVE_ENCODE_BYTES_PER_SEC`] takes half the lease,
/// `lease * 10 MiB/s / 2` (1,572,864,000 bytes, 1.46 GiB, at the default 300 s
/// lease). `claim_lease_below_warn_threshold(lease, claim_lease_max_part_bytes(
/// lease))` is false for every lease, because the cap is rounded down in
/// milliseconds and the warning threshold is computed from it with the same
/// rounding.
pub fn claim_lease_max_part_bytes(claim_lease_duration: Duration) -> u64 {
    let bytes = claim_lease_duration.as_millis()
        * u128::from(CLAIM_LEASE_WARN_CONSERVATIVE_ENCODE_BYTES_PER_SEC)
        / 2000;
    u64::try_from(bytes).unwrap_or(u64::MAX)
}

/// Which term of [`derive_l1_part_memory_target`] decided the target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum L1PartMemoryTargetBound {
    /// `budget / 8 / concurrent_merges`, inside the other terms.
    MemoryShare,
    /// The claim lease: [`claim_lease_max_part_bytes`] was below the memory
    /// share and the ceiling, so a larger part could outlast its own lease.
    ClaimLease,
    /// The 256 MiB floor ([`MIN_DERIVED_L1_PART_MEMORY_TARGET_BYTES`]) lifted a
    /// smaller share or lease cap. A lease too short for a floor-sized part
    /// then trips [`claim_lease_below_warn_threshold`]; that is the operator's
    /// explicit choice and the warning is the signal.
    Floor,
    /// The 8 GiB ceiling ([`MAX_DERIVED_L1_PART_MEMORY_TARGET_BYTES`]) capped a
    /// larger share and lease cap.
    Ceiling,
}

impl L1PartMemoryTargetBound {
    /// `memory_share`, `claim_lease`, `floor` or `ceiling`, for a structured
    /// log field.
    pub fn name(&self) -> &'static str {
        match self {
            L1PartMemoryTargetBound::MemoryShare => "memory_share",
            L1PartMemoryTargetBound::ClaimLease => "claim_lease",
            L1PartMemoryTargetBound::Floor => "floor",
            L1PartMemoryTargetBound::Ceiling => "ceiling",
        }
    }
}

/// The derived memory split target for one merge and the term that decided it.
///
/// The target is the smallest of three terms, then lifted to the floor:
///
/// - the memory share, `memory_budget_bytes / 8 / concurrent_merges`;
/// - the lease cap, [`claim_lease_max_part_bytes`]`(claim_lease_duration)`;
/// - the ceiling, [`MAX_DERIVED_L1_PART_MEMORY_TARGET_BYTES`] (8 GiB);
///
/// and finally `max(that, `[`MIN_DERIVED_L1_PART_MEMORY_TARGET_BYTES`]`)` (256 MiB).
/// The floor is applied LAST: a lease too short for a 256 MiB part still gets
/// 256 MiB (the geometry before this derivation existed) and the startup
/// lease check warns, instead of a target below the floor.
///
/// The lease term exists because the RLOG stored-size cap follows this target
/// ([`CompactorConfig::rlog_max_l1_part_bytes`]), and ADR-1029 decision 3 warns
/// when the lease is under twice the time to encode and PUT one cap-sized part.
/// A target above the lease cap would make the shipped defaults trip that
/// warning (a 4 GiB cap needs a lease of about 820 s against the 300 s default).
///
/// Dividing by `concurrent_merges` keeps the sum of the targets of every merge
/// a process runs at once at one eighth of the budget while the per-merge value
/// is the share. Below the floor the sum is `256 MiB * concurrent_merges`, which
/// can exceed one eighth. `concurrent_merges` below 1 is treated as 1. Integer
/// division, truncating. When two terms tie the earlier in the list above is
/// named.
pub fn derive_l1_part_memory_target(
    memory_budget_bytes: u64,
    concurrent_merges: usize,
    claim_lease_duration: Duration,
) -> (u64, L1PartMemoryTargetBound) {
    let merges = u64::try_from(concurrent_merges.max(1)).unwrap_or(u64::MAX);
    let share = memory_budget_bytes / L1_PART_MEMORY_TARGET_BUDGET_DIVISOR / merges;
    let lease = claim_lease_max_part_bytes(claim_lease_duration);
    let (smallest, term) = if share <= lease && share <= MAX_DERIVED_L1_PART_MEMORY_TARGET_BYTES {
        (share, L1PartMemoryTargetBound::MemoryShare)
    } else if lease <= MAX_DERIVED_L1_PART_MEMORY_TARGET_BYTES {
        (lease, L1PartMemoryTargetBound::ClaimLease)
    } else {
        (
            MAX_DERIVED_L1_PART_MEMORY_TARGET_BYTES,
            L1PartMemoryTargetBound::Ceiling,
        )
    };
    if smallest < MIN_DERIVED_L1_PART_MEMORY_TARGET_BYTES {
        (
            MIN_DERIVED_L1_PART_MEMORY_TARGET_BYTES,
            L1PartMemoryTargetBound::Floor,
        )
    } else {
        (smallest, term)
    }
}

/// [`derive_l1_part_memory_target`] without the bound.
pub fn derive_l1_part_memory_target_bytes(
    memory_budget_bytes: u64,
    concurrent_merges: usize,
    claim_lease_duration: Duration,
) -> u64 {
    derive_l1_part_memory_target(memory_budget_bytes, concurrent_merges, claim_lease_duration).0
}

/// Where a resolved [`CompactorConfig::l1_part_memory_target_bytes`] came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum L1PartMemoryTargetSource {
    /// The operator set it; used verbatim, never clamped.
    Flag,
    /// Derived by [`derive_l1_part_memory_target`] from this budget (net of the
    /// overhead reserve and the merge cursor budget), merge count and claim
    /// lease.
    Derived {
        memory_budget_bytes: u64,
        concurrent_merges: usize,
        claim_lease_duration: Duration,
        bound: L1PartMemoryTargetBound,
    },
    /// No flag and no known memory budget:
    /// [`DEFAULT_L1_PART_MEMORY_TARGET_BYTES`].
    Fallback,
}

/// A resolved memory split target and its provenance, which both binaries
/// print once per run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedL1PartMemoryTarget {
    pub bytes: u64,
    pub source: L1PartMemoryTargetSource,
}

impl ResolvedL1PartMemoryTarget {
    /// An explicit value wins verbatim (its zero refusal belongs to the caller
    /// that parsed it). Otherwise a known budget derives the target with
    /// [`derive_l1_part_memory_target`] (`memory_budget_bytes` is the budget
    /// already net of the overhead reserve and the merge cursor budget, see
    /// [`merge_memory_budget_bytes`]), and an unknown one falls back to
    /// [`DEFAULT_L1_PART_MEMORY_TARGET_BYTES`].
    pub fn resolve(
        explicit: Option<u64>,
        memory_budget_bytes: Option<u64>,
        concurrent_merges: usize,
        claim_lease_duration: Duration,
    ) -> Self {
        match (explicit, memory_budget_bytes) {
            (Some(bytes), _) => ResolvedL1PartMemoryTarget {
                bytes,
                source: L1PartMemoryTargetSource::Flag,
            },
            (None, Some(budget)) => {
                let (bytes, bound) =
                    derive_l1_part_memory_target(budget, concurrent_merges, claim_lease_duration);
                ResolvedL1PartMemoryTarget {
                    bytes,
                    source: L1PartMemoryTargetSource::Derived {
                        memory_budget_bytes: budget,
                        concurrent_merges: concurrent_merges.max(1),
                        claim_lease_duration,
                        bound,
                    },
                }
            }
            (None, None) => ResolvedL1PartMemoryTarget {
                bytes: DEFAULT_L1_PART_MEMORY_TARGET_BYTES,
                source: L1PartMemoryTargetSource::Fallback,
            },
        }
    }

    /// Write this resolution into `config`. The RLOG merge gets
    /// [`Self::bytes`] whatever the source
    /// ([`CompactorConfig::rlog_l1_part_memory_target_bytes`]).
    ///
    /// A derived or fallback resolution also sets the RLOG stored-size cap to
    /// the same value ([`CompactorConfig::rlog_max_l1_part_bytes`]), so the memory
    /// target stays the binding target and the exact-encode probe stays opt-in.
    /// It leaves the shared [`CompactorConfig::l1_part_memory_target_bytes`] and
    /// [`CompactorConfig::max_l1_part_bytes`] alone, whatever they hold, because
    /// RSPAN and RSEG read those and the derivation is RLOG's.
    ///
    /// A flag resolution is the operator's: it sets the shared memory target
    /// (so it reaches RSPAN too) and leaves the RLOG cap following
    /// [`CompactorConfig::max_l1_part_bytes`]. A memory target above that cap
    /// then runs probes, which is the operator's explicit configuration.
    pub fn apply_to(&self, config: &mut CompactorConfig) {
        config.rlog_l1_part_memory_target_bytes = Some(self.bytes);
        match self.source {
            L1PartMemoryTargetSource::Flag => {
                config.l1_part_memory_target_bytes = self.bytes;
            }
            L1PartMemoryTargetSource::Derived { .. } | L1PartMemoryTargetSource::Fallback => {
                config.rlog_max_l1_part_bytes = Some(self.bytes);
            }
        }
    }

    /// `flag`, `derived` or `fallback`, for a structured log field.
    pub fn source_name(&self) -> &'static str {
        match self.source {
            L1PartMemoryTargetSource::Flag => "flag",
            L1PartMemoryTargetSource::Derived { .. } => "derived",
            L1PartMemoryTargetSource::Fallback => "fallback",
        }
    }

    /// The derivation term that decided a derived target, for a structured log
    /// field; `None` for a flag or a fallback, which no term decided.
    pub fn bound_name(&self) -> Option<&'static str> {
        match self.source {
            L1PartMemoryTargetSource::Derived { bound, .. } => Some(bound.name()),
            L1PartMemoryTargetSource::Flag | L1PartMemoryTargetSource::Fallback => None,
        }
    }
}

impl std::fmt::Display for ResolvedL1PartMemoryTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.source {
            L1PartMemoryTargetSource::Flag => write!(f, "{} (set by flag)", self.bytes),
            L1PartMemoryTargetSource::Derived {
                memory_budget_bytes,
                concurrent_merges,
                claim_lease_duration,
                bound,
            } => {
                write!(
                    f,
                    "{} (resolved from a memory budget of {memory_budget_bytes} \
                     over {concurrent_merges} concurrent {}; bound by ",
                    self.bytes,
                    if concurrent_merges == 1 {
                        "merge"
                    } else {
                        "merges"
                    }
                )?;
                match bound {
                    L1PartMemoryTargetBound::MemoryShare => {
                        write!(f, "the memory share, budget / 8 / merges)")
                    }
                    L1PartMemoryTargetBound::ClaimLease => write!(
                        f,
                        "the claim lease, {} s allows a part of at most {} bytes)",
                        claim_lease_duration.as_secs(),
                        claim_lease_max_part_bytes(claim_lease_duration)
                    ),
                    L1PartMemoryTargetBound::Floor => write!(
                        f,
                        "the {MIN_DERIVED_L1_PART_MEMORY_TARGET_BYTES}-byte floor)"
                    ),
                    L1PartMemoryTargetBound::Ceiling => write!(
                        f,
                        "the {MAX_DERIVED_L1_PART_MEMORY_TARGET_BYTES}-byte ceiling)"
                    ),
                }
            }
            L1PartMemoryTargetSource::Fallback => {
                write!(f, "{} (fallback: the memory budget is unknown)", self.bytes)
            }
        }
    }
}

/// This host's usable memory in bytes: the one detector `ravel-cli maintain`
/// and `ravel-server`'s `HostProfile::detect` both call. On Linux it is
/// `/proc/meminfo`'s `MemTotal` capped by a finite cgroup memory limit (v2
/// `memory.max`, else v1 `memory.limit_in_bytes`), because a container reads the
/// host's `MemTotal` and a share of it alone would size against memory the
/// container may not use; on macOS it is `sysctl -n hw.memsize`. `None` when
/// none of these can be read or parsed, and on every other target.
pub fn detect_host_memory_total_bytes() -> Option<u64> {
    detect_host_memory_total_bytes_impl()
}

#[cfg(target_os = "linux")]
fn detect_host_memory_total_bytes_impl() -> Option<u64> {
    let mem_total = std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|contents| parse_meminfo_total_bytes(&contents));
    let cgroup_limit = std::fs::read_to_string("/sys/fs/cgroup/memory.max")
        .ok()
        .and_then(|contents| parse_cgroup_memory_limit(&contents))
        .or_else(|| {
            std::fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes")
                .ok()
                .and_then(|contents| parse_cgroup_memory_limit(&contents))
        });
    effective_memory_total(mem_total, cgroup_limit)
}

/// The memory total the derived defaults size against: `MemTotal` capped by a
/// finite cgroup limit. Either one alone is used when the other is unknown, so
/// a container whose `/proc/meminfo` is unreadable still derives from its
/// limit, and a bare host with no cgroup limit derives from `MemTotal`.
#[cfg(any(target_os = "linux", test))]
fn effective_memory_total(mem_total: Option<u64>, cgroup_limit: Option<u64>) -> Option<u64> {
    match (mem_total, cgroup_limit) {
        (Some(total), Some(limit)) => Some(total.min(limit)),
        (total, limit) => total.or(limit),
    }
}

#[cfg(target_os = "macos")]
fn detect_host_memory_total_bytes_impl() -> Option<u64> {
    let output = std::process::Command::new("sysctl")
        .args(["-n", "hw.memsize"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    parse_sysctl_memsize(&String::from_utf8_lossy(&output.stdout))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn detect_host_memory_total_bytes_impl() -> Option<u64> {
    None
}

/// `MemTotal:       32137720 kB` in bytes. A missing line, a non-numeric
/// count or an unknown unit is `None`.
#[cfg(any(target_os = "linux", test))]
fn parse_meminfo_total_bytes(meminfo: &str) -> Option<u64> {
    let line = meminfo.lines().find(|line| line.starts_with("MemTotal:"))?;
    let mut fields = line.split_whitespace().skip(1);
    let value: u64 = fields.next()?.parse().ok()?;
    match fields.next() {
        Some("kB") | Some("KB") => value.checked_mul(1024),
        None => Some(value),
        Some(_) => None,
    }
}

/// A cgroup memory limit file as a finite byte limit. `max`, the v1 no-limit
/// sentinel (any value at or above 2^60), `0` and anything malformed are
/// `None`, so an unlimited cgroup caps nothing.
#[cfg(any(target_os = "linux", test))]
fn parse_cgroup_memory_limit(contents: &str) -> Option<u64> {
    let raw = contents.trim();
    if raw == "max" {
        return None;
    }
    let bytes: u64 = raw.parse().ok()?;
    if bytes == 0 || bytes >= 1 << 60 {
        return None;
    }
    Some(bytes)
}

/// `sysctl -n hw.memsize` output (a decimal byte count) in bytes.
#[cfg(any(target_os = "macos", test))]
fn parse_sysctl_memsize(output: &str) -> Option<u64> {
    output.trim().parse().ok().filter(|bytes| *bytes > 0)
}
/// Default minimum L0 records for a bucket to be worth compacting.
pub const DEFAULT_MIN_COMPACTION_INPUTS: usize = 2;
/// Default `claim_min_input_bytes`: 64 MiB of listed input bytes (ADR-1029
/// decision 4). Below it a duplicated merge is cheaper than the PUT-class
/// claim traffic that would prevent it, so the bucket runs unclaimed.
pub const DEFAULT_CLAIM_MIN_INPUT_BYTES: u64 = 64 * 1024 * 1024;
/// Default `claim_lease_duration`: 300 s (ADR-1029 decision 3), the same
/// figure [`ravel_fleet::claim::DEFAULT_LEASE_DURATION`] carries. The lease
/// must exceed the longest stage a run cannot be cancelled inside (one
/// stream's cursor drain, one part encode plus PUT), not the whole merge: the
/// owner renews at the cancellation checkpoints.
pub const DEFAULT_CLAIM_LEASE_DURATION: Duration = ravel_fleet::claim::DEFAULT_LEASE_DURATION;
/// Conservative encode+PUT throughput assumed by [`claim_lease_below_warn_threshold`]
/// (ADR-1029 decision 3): 10 MiB/s, well under real object-store PUT
/// throughput even on the smallest supported host, so the startup warning
/// fires only when the configured lease is genuinely too short for the
/// largest L1 part, not on ordinary variance.
pub const CLAIM_LEASE_WARN_CONSERVATIVE_ENCODE_BYTES_PER_SEC: u64 = 10 * 1024 * 1024;

/// Whether `claim_lease_duration` is below ADR-1029 decision 3's startup
/// warning threshold: 2x the time to encode and PUT one `max_l1_part_bytes`
/// part at [`CLAIM_LEASE_WARN_CONSERVATIVE_ENCODE_BYTES_PER_SEC`]. A lease
/// this short can expire, and be stolen, before a run still encoding its
/// largest part reaches its own next renewal checkpoint. Computed in
/// milliseconds, so a part smaller than one second's worth of the assumed
/// rate still yields a nonzero threshold.
pub fn claim_lease_below_warn_threshold(
    claim_lease_duration: Duration,
    max_l1_part_bytes: u64,
) -> bool {
    let encode_ms = u128::from(max_l1_part_bytes) * 1000
        / u128::from(CLAIM_LEASE_WARN_CONSERVATIVE_ENCODE_BYTES_PER_SEC);
    let threshold_ms = u64::try_from(encode_ms.saturating_mul(2)).unwrap_or(u64::MAX);
    claim_lease_duration < Duration::from_millis(threshold_ms)
}
/// Default footer suffix-probe size. 64 KiB covers the footer + catalog of a
/// typical L0 flush in one GET (docs/segment-format.md reader protocol).
pub const DEFAULT_FOOTER_PROBE_BYTES: u64 = 64 * 1024;
/// Default `input_read_concurrency`: 8 input reads in flight at once.
pub const DEFAULT_INPUT_READ_CONCURRENCY: usize = 8;
/// Default `merge_cursor_budget_bytes`: 20 GiB (ADR-0979 decision 4), re-derived
/// below from the arithmetic `crate::rlog` implements rather than from the ADR's
/// summary figure.
///
/// The charge a stream carries is the sum over its open cursors of what each
/// holds, plus, for the instant between reserving and decoding, the pre-decode
/// ceiling of the batch being admitted or of the one block an open cursor is
/// about to decode. For the ClickBench worst case the ADR
/// sizes against (tenant `cb-20260831140539`, top stream carried by ~617 inputs,
/// ~3.5 MB stored per row group, 105 mapped columns of which ~28 are
/// string-typed, 8192-record blocks):
///
/// ```text
/// per-cursor pre-decode ceiling (rlog::cursor_reservation_bytes)
///   16 B x 8192 rows x 115 column ids            = 14.375 MiB shape term
///   + string-page uncomp_len (~4 MiB measured)   =  4.000 MiB string payload
///   + 40 B x 8192 rows x 33 string columns       = 10.313 MiB string slots
///   + 48 B x 115 column ids x 5 x 2 growth       =  0.053 MiB decoder spines
///   + 2 x 3.5 MB row group                       =  6.676 MiB raw, two locs
///                                                  ----------
///                                                   35.416 MiB (37,136,224 B)
///
/// per-cursor reconciled residency (rlog::StreamCursor::resident_bytes)
///   heap_estimate of the decoded block           = 17-24 MiB  (ADR: 18-25 MB)
///   + the raw locs actually held                 <= 6.7 MiB
///                                                  ---------
///                                                   24-30 MiB
///
/// 617 cursors reconciled       617 x 30 MiB     = 18.1 GiB  < 20 GiB
/// 617 cursors at the ceiling   617 x 35.416 MiB = 21.3 GiB  > 20 GiB
/// ```
///
/// The decoder-spine term is the five per-kind slot vectors a decoded block
/// holds, each indexed by column id and so sized by the id-space width whatever
/// the block carries. It does not scale with rows, so at 8192-record blocks it
/// is 55,200 B, 0.15% of the per-cursor ceiling: adding it moves that ceiling
/// from 37,081,024 B to 37,136,224 B and the 617-cursor line from 21.307 GiB to
/// 21.339 GiB, both still the same rounded figures and the same side of the
/// 20 GiB budget. It is in the ceiling for the other end of the range, a block
/// of a few rows, where it is the dominant term and a ceiling without it is not
/// a ceiling at all. The reconciled line is unchanged either way: it is
/// measured, and `heap_estimate` has always counted the slot vectors.
///
/// So the default admits the worst case because the reconcile is mandatory
/// (ADR-0979 decision 4 as amended): held at their admission ceilings, the same
/// 617 cursors would be refused. The residual corner is a stream whose whole
/// 617-input queue is admitted in ONE batch (every input's SKIP_IDX lower bound
/// at or below the first frontier), where the batch is charged at its ceiling
/// before any of it decodes: that transient is the 21.3 GiB line and fails
/// closed with [`crate::error::MaintainError::MergeCursorBudgetExceeded`] naming
/// the number to raise, which is the designed behaviour at the limit rather than
/// an out-of-memory kill. Under 20 GiB the reference box keeps ~10 GB of its
/// 30 GB for the writer buffer, the catalogs, and process overhead. See
/// [`CompactorConfig::merge_cursor_budget_bytes`] for what the budget bounds.
pub const DEFAULT_MERGE_CURSOR_BUDGET_BYTES: u64 = 20 * 1024 * 1024 * 1024;

/// How the RLOG merge admits a stream's per-input cursors (ADR-0979 decision 2).
/// A knob only because the differential test asserts the two modes produce
/// byte-identical parts; production always uses [`Self::Overlap`], the bounded
/// path, and no code changes output bytes between the two.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AdmissionMode {
    /// Overlap-gated admission: a stream's cursor on an input opens only once
    /// the merge frontier reaches that input's SKIP_IDX ts lower bound, so the
    /// number of simultaneously open cursors is the max concurrent ts-overlap
    /// `D` of the stream's input slices rather than the input count `n`. This is
    /// the bound this ADR exists to establish; the default.
    #[default]
    Overlap,
    /// Eagerly open every input's cursor for the stream up front, the pre-D2
    /// behaviour. Retained so the differential test can assert the emitted
    /// record sequence, every part boundary, and every part `content_hash` are
    /// identical to the overlap-gated path. The [`CompactorConfig::merge_cursor_budget_bytes`]
    /// reservation still applies (every cursor is charged), so this is not an
    /// escape from the budget, only from the deferral.
    EagerAll,
}

/// Default `rlog_zstd_level`: 9 (ADR-2135 decision 4). Compaction and the
/// erasure rewrite write once per bucket and are read many times, so they take
/// a higher level than the ingest path's 3; readers are indifferent to it.
pub const DEFAULT_RLOG_ZSTD_LEVEL: i32 = 9;
/// Lowest accepted `rlog_zstd_level`. zstd's negative "fast" levels are refused.
pub const MIN_RLOG_ZSTD_LEVEL: i32 = 1;
/// Highest accepted `rlog_zstd_level`: zstd's maximum level.
pub const MAX_RLOG_ZSTD_LEVEL: i32 = 22;

/// An `rlog_zstd_level` outside
/// [`MIN_RLOG_ZSTD_LEVEL`]`..=`[`MAX_RLOG_ZSTD_LEVEL`] was configured.
#[derive(Debug, Clone, Copy, thiserror::Error, PartialEq, Eq)]
#[error(
    "rlog zstd level {level} is outside the accepted range {MIN_RLOG_ZSTD_LEVEL}..={MAX_RLOG_ZSTD_LEVEL}"
)]
pub struct RlogZstdLevelError {
    /// The refused level.
    pub level: i32,
}

/// Validate a compaction zstd level, returning it unchanged when accepted.
pub fn validate_rlog_zstd_level(level: i32) -> Result<i32, RlogZstdLevelError> {
    if (MIN_RLOG_ZSTD_LEVEL..=MAX_RLOG_ZSTD_LEVEL).contains(&level) {
        Ok(level)
    } else {
        Err(RlogZstdLevelError { level })
    }
}

/// Default `grace`: 24 hours (docs/consistency-model.md "Deletion and GC").
/// A shared floor for the orphan and unreferenced-part age gates.
pub const DEFAULT_GRACE_NS: i64 = 24 * NS_PER_HOUR;
/// Default `max_query_duration`: 1 hour. The horizon must outlast any pinned
/// in-flight query (`protection_horizon >= max_query_duration + grace +
/// clock_skew_allowance`, docs/consistency-model.md), so this is the
/// query-duration term of the default `protection_horizon`.
pub const DEFAULT_MAX_QUERY_DURATION_NS: i64 = NS_PER_HOUR;
/// Default `protection_horizon`: `max_query_duration + grace +
/// clock_skew_allowance`. The
/// supersession and retention sweeps gate physical deletion on
/// `now >= anchor + protection_horizon`, so a query resolved just before the
/// anchor still has this long to finish reading the inputs it pinned. The
/// `clock_skew_allowance` term covers a sweeper whose clock leads a reader's by
/// up to that allowance: without it, a skewed sweeper reaches
/// `now >= anchor + protection_horizon` in true time before the reader's pinned
/// snapshot (held up to `max_query_duration`) is released. Bootstrapping from
/// this default therefore writes a `sys/gc` that satisfies the skew-covering
/// bound by construction, so no reachable default deployment is skew-uncovered.
pub const DEFAULT_PROTECTION_HORIZON_NS: i64 =
    DEFAULT_MAX_QUERY_DURATION_NS + DEFAULT_GRACE_NS + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS;
/// Default `max_ingest_lag`: 2 hours. Used only in the ADR-0019 §5 retention
/// validation floor. This MUST be kept in sync with ravel-catalog's
/// `DEFAULT_MAX_INGEST_LAG_NS` (crates/ravel-catalog/src/config.rs): a
/// ravel-maintain -> ravel-catalog dependency was deliberately avoided (that
/// crate pulls in zstd and the whole resolve stack for one constant), so the
/// value is duplicated here and this comment is the sync contract.
pub const DEFAULT_MAX_INGEST_LAG_NS: i64 = 2 * NS_PER_HOUR;

/// Default `orphan_breaker_min_count` (ADR-0048 decision 4): the mass-orphan
/// circuit breaker never trips below this many candidates, however small the
/// shard, so a handful of genuine orphans in a tiny shard is never mistaken
/// for mass record loss.
pub const DEFAULT_ORPHAN_BREAKER_MIN_COUNT: usize = 50;
/// Default `orphan_breaker_max_ratio` (ADR-0048 decision 4): the mass-orphan
/// circuit breaker never trips at or below this fraction of the shard's
/// listed L0 objects, so a large but proportionally unremarkable orphan count
/// in a large shard is never mistaken for mass record loss.
pub const DEFAULT_ORPHAN_BREAKER_MAX_RATIO: f64 = 0.10;

/// Default `quarantine_horizon_ns` (ADR-0058 amendment): 7 days. The second
/// horizon orphan GC gives an operator between moving a record-less L0 data
/// object out of the live keyspace (into `quarantine/`, past the orphan age
/// gate) and physically deleting it. The first horizon (`grace +
/// max_flush_lifetime`, ~25 h) is when the object is quarantined; this one is
/// how long the recoverable copy then survives before the reaper deletes it.
/// Sized far above the ~25 h loss-to-quarantine window so an operator who
/// misses the quarantine signal still has most of a week to restore the lost
/// commit records or copy the bytes back before the loss becomes permanent.
/// The cost is storage: a quarantined object occupies the bucket for this long
/// past its quarantine before it is reclaimed.
pub const DEFAULT_QUARANTINE_HORIZON_NS: i64 = 7 * 24 * NS_PER_HOUR;

/// Default `audit_retention_window_ns`: 90 days. The
/// dedicated retention window for query-audit records on
/// [`crate::query_audit::QUERY_AUDIT_SHARD`], independent of the ADR-0019
/// per-tenant data-retention windows ([`RetentionConfig`]): query-audit is a
/// server-written activity log with its own fixed lifetime, not tenant data,
/// and it is not tombstone-gated through the resolver (no snapshot excludes it),
/// so it has its own age-based sweep rather than the bucket-tombstone flow.
pub const DEFAULT_AUDIT_RETENTION_NS: i64 = 90 * 24 * NS_PER_HOUR;

/// Default `alert_retention_window_ns`: 90 days (ADR-1688 decisions 4 and 5).
/// The retention window for the `Signal::Alerts` transition history swept by
/// [`crate::alert_retention::sweep_alert_retention`]. It is deliberately the
/// same value as [`DEFAULT_AUDIT_RETENTION_NS`]: ADR-1688 decision 5 sets the
/// alert window to "the same value as the audit window", the retention default
/// an operator already runs with, and decision 4 makes the window double as the
/// evaluator's cold-start fold horizon (the surviving prefix is every
/// transition inside the window plus one current-state record per identity).
/// `0` disables the sweep and keeps today's grow-forever behaviour (decision 5).
pub const DEFAULT_ALERT_RETENTION_NS: i64 = 90 * 24 * NS_PER_HOUR;

/// Default `idem_dedup_window_hours` (ADR-0051 §5): this crate's own policy
/// default, chosen to match the 24h dedup window ADR-0051 documents.
/// `ravel_ingest::idempotency::read_marker` has no default of its own --
/// `dedup_window_hours` is always caller-supplied -- so there is no shared
/// code-level default to match; what actually keeps the sweep from reaping a
/// marker the read path would still honor is
/// `ravel_ingest::IDEM_MARKER_FORWARD_SKEW_TOLERANCE_HOURS`, subtracted from
/// the sweep's own `min_hour` calculation (`crate::sweep`).
pub const DEFAULT_IDEM_DEDUP_WINDOW_HOURS: u32 = 24;

/// Default `interior_reverify_ns` (ADR-0065 decision 3, config name
/// `maintain_interior_reverify`): the slow safety-net cadence for the
/// interior zone's terminal buckets, replacing the flat
/// [`crate::scan::DEFAULT_MEMO_REVERIFY_INTERVAL_NS`] (1 h) for that zone
/// only. Head and tail keep tick-cadence evaluation regardless of this
/// value. The same knob also gates how often the maintain driver runs a
/// full-keyspace [`crate::sweep::sweep_shard`] pass instead of the per-tick
/// [`crate::sweep::sweep_shard_zoned`] pass, via
/// [`crate::scan::MaintainMemo::full_sweep_due`]: one cadence, one operator
/// knob, for both halves of the zone split. 6 h is far below any retention
/// window or protection horizon, so the promptness this bounds (a
/// tombstoned interior bucket's physical sweep, an operator hold) is a
/// documented latency, never a correctness gap (docs/consistency-model.md
/// "Deletion and GC").
pub const DEFAULT_INTERIOR_REVERIFY_NS: i64 = 6 * NS_PER_HOUR;

/// Default `max_batch` for the group-commit [`crate::audit_pipeline::AuditPipeline`]:
/// the buffered-record count that forces a flush before `max_age` elapses.
pub const DEFAULT_AUDIT_MAX_BATCH: usize = 256;
/// Default `max_age` for the group-commit audit pipeline (ADR-0062 §2b): a
/// batch is flushed once this long has elapsed since its first buffered event,
/// even below `max_batch`.
pub const DEFAULT_AUDIT_MAX_AGE: Duration = Duration::from_millis(25);
/// Default submission-channel capacity for the audit pipeline: the number of
/// in-flight submissions the bounded `mpsc` from submitters to the flush task
/// holds before `submit` awaits backpressure.
pub const DEFAULT_AUDIT_CHANNEL_CAPACITY: usize = 1024;

/// Whether a failed audit flush fails the awaiting queries or releases them
/// anyway (ADR-0062 §2b). [`AuditMode::Required`] (the default) fails closed:
/// every query whose event was in a failed batch gets an error, so no response
/// is released without a durable audit record. [`AuditMode::BestEffort`] is the
/// explicit, named opt-out that logs the failure and releases the queries with
/// `Ok`, trading complete audit coverage for availability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AuditMode {
    /// Fail closed: a flush failure is returned to every awaiting submitter.
    #[default]
    Required,
    /// Fail open: a flush failure is logged and every awaiting submitter gets
    /// `Ok(())`. Opt-in only, for deployments preferring availability over
    /// complete audit coverage.
    BestEffort,
}

/// Configuration for the group-commit [`crate::audit_pipeline::AuditPipeline`]
/// (ADR-0062 §2b). A batch flushes on whichever of `max_batch` or `max_age`
/// comes first; `audit_mode` picks the flush-failure posture.
#[derive(Debug, Clone)]
pub struct AuditPipelineConfig {
    /// Flush once the current batch reaches this many buffered records, before
    /// `max_age` elapses. Default [`DEFAULT_AUDIT_MAX_BATCH`].
    pub max_batch: usize,
    /// Flush once this long has elapsed since the current batch's first
    /// buffered event, before `max_batch` is reached. Default
    /// [`DEFAULT_AUDIT_MAX_AGE`] (25 ms).
    pub max_age: Duration,
    /// The [`ravel_types::Signal::Audit`] shard every batch is written to.
    /// Default [`crate::query_audit::QUERY_AUDIT_SHARD`].
    pub shard: u32,
    /// Whether a flush failure fails or releases the awaiting queries. Default
    /// [`AuditMode::Required`].
    pub audit_mode: AuditMode,
    /// Capacity of the bounded submission channel from `submit` to the flush
    /// task. Default [`DEFAULT_AUDIT_CHANNEL_CAPACITY`].
    pub channel_capacity: usize,
}

impl Default for AuditPipelineConfig {
    fn default() -> Self {
        AuditPipelineConfig {
            max_batch: DEFAULT_AUDIT_MAX_BATCH,
            max_age: DEFAULT_AUDIT_MAX_AGE,
            shard: crate::query_audit::QUERY_AUDIT_SHARD,
            audit_mode: AuditMode::Required,
            channel_capacity: DEFAULT_AUDIT_CHANNEL_CAPACITY,
        }
    }
}

/// Whether this process takes advisory compaction claims at all (ADR-1029
/// decision 5's escape hatch).
///
/// [`Coordination::Off`] is the fleet-wide fallback for a store whose
/// qualification record predates the CAS probes, or an emergency. It is not a
/// separate code path: an unclaimed run is the same pipeline with no claim
/// taken, exactly what a bucket below
/// [`CompactorConfig::claim_min_input_bytes`] does, so the unclaimed path is
/// the one this crate's default tests exercise.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Coordination {
    /// Claim buckets at or above the cost gate. The default.
    #[default]
    On,
    /// Never claim. Racing runs still converge at the compaction record's
    /// `CreateIfAbsent`, which is what makes claims advisory (ADR-1029
    /// decision 2); the loser just pays its merge first.
    Off,
}

/// Who this process claims as, and the clock its claim decisions read.
///
/// Installed by the caller that drives a coordinated compaction
/// ([`crate::compact::compact_bucket_claimed`]); `None` means this caller takes
/// no claims whatever [`CompactorConfig::coordination`] says, which is the
/// state every existing direct caller of [`crate::compact::compact_bucket`] is
/// in. The background supervisor tick installs one, and so does `ravel-cli`'s
/// `compact-bucket` and `compact-tenant` (#1034), except on `--dry-run` and
/// `--no-claim`.
///
/// The clock is held as an `Arc` rather than borrowed because the guard is
/// consulted deep inside the merge, at call sites that take no clock
/// ([`crate::codec::SegmentCodec::build_parts`] and the per-signal merges
/// below it). Library logic never reads `SystemTime::now()`: a test installs a
/// [`crate::clock::FixedClock`] here and drives every renewal and expiry
/// decision deterministically. The pre-acquisition jitter wait goes through
/// the participant's [`ClaimSleeper`] for the same reason: tokio's timer by
/// default, and a recording or no-op sleeper in a test.
///
/// [`ClaimSleeper`]: crate::claim_guard::ClaimSleeper
#[derive(Clone)]
pub struct ClaimParticipant {
    process_id: Uuid,
    clock: Arc<dyn crate::clock::Clock>,
    sleeper: Arc<dyn crate::claim_guard::ClaimSleeper>,
}

impl std::fmt::Debug for ClaimParticipant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClaimParticipant")
            .field("process_id", &self.process_id)
            .finish_non_exhaustive()
    }
}

impl ClaimParticipant {
    /// A participant claiming as `process_id` (the ADR-0057/0065 startup uuid,
    /// the same identity this process's `sys/maintain/workers/<id>` heartbeat
    /// is keyed under), reading `clock` for every claim decision and waiting
    /// out the acquisition jitter on tokio's timer.
    pub fn new(process_id: Uuid, clock: Arc<dyn crate::clock::Clock>) -> Self {
        ClaimParticipant {
            process_id,
            clock,
            sleeper: Arc::new(crate::claim_guard::TokioSleeper),
        }
    }

    /// This participant, waiting out the acquisition jitter through `sleeper`
    /// instead of tokio's timer.
    pub fn with_sleeper(self, sleeper: Arc<dyn crate::claim_guard::ClaimSleeper>) -> Self {
        ClaimParticipant { sleeper, ..self }
    }

    /// The sleeper the acquisition jitter is waited out on.
    pub fn sleeper(&self) -> &Arc<dyn crate::claim_guard::ClaimSleeper> {
        &self.sleeper
    }

    /// The process id claims are written under.
    pub fn process_id(&self) -> Uuid {
        self.process_id
    }

    /// The injected clock every claim decision reads.
    pub fn clock(&self) -> &Arc<dyn crate::clock::Clock> {
        &self.clock
    }
}

/// Everything the compactor needs beyond the store and the clock.
#[derive(Debug, Clone)]
pub struct CompactorConfig {
    /// Longest a flush may stay open; a bucket is sealed only after its end
    /// plus this plus the skew allowance.
    pub max_flush_lifetime_ns: i64,
    /// Extra seal margin for cross-host clock skew.
    pub clock_skew_allowance_ns: i64,
    /// Deadline after which a compaction run must not publish its record
    /// measured from the run's start via the clock.
    pub max_compaction_lifetime_ns: i64,
    /// The **stored-size target**: close the in-progress L1 part once its
    /// encoded/on-object bytes reach this. Measured in on-object bytes on both
    /// paths, so a part closes when the object it is about to write reaches this
    /// size (issue #872).
    ///
    /// - RSEG (`build.rs`): the part's stored-size estimate, which charges every
    ///   section that grows with what the part carries: the TS/VAL/HIST pages
    ///   (ADR-0092 decision 3, actual encoded page bytes), each series'
    ///   SERIES_IDS entry and SERIES_META cells, the per-sample provenance
    ///   columns a run-merged run adds, each distinct LABEL_DICT string, and the
    ///   EXEMPLARS records. The page bytes are exact; the metadata sections are a
    ///   pre-compression proxy over their zstd-compressed form (an upper bound).
    /// - RLOG (`rlog.rs`): the ACTUAL encoded object size. The RLOG writer holds
    ///   row-major records and only encodes at
    ///   [`ravel_logseg::RlogWriter::finish_compacted`], so it exposes no
    ///   incremental encoded size; the merge keeps a cheap pre-compression
    ///   payload proxy ([`crate::rlog::estimate_stored_record`] per record plus
    ///   one STREAM_DIR entry per distinct stream) only to SCHEDULE an
    ///   exact-encode probe, and closes the part on the probe's real byte count.
    ///   Overshoot is bounded by how the probes are spaced: the next probe is
    ///   aimed the remaining encoded deficit further along the proxy axis, or a
    ///   4 KiB floor (`PROBE_MIN_STEP_BYTES` in `rlog.rs`) if that deficit is
    ///   smaller. The step therefore assumes only that over the interval that
    ///   closes the part, encoded bytes grow at most 1:1 with the uncompressed
    ///   payload proxy, and the enforced band is `[target, target + 4 KiB + one
    ///   record's proxy charge]`. That assumption holds for the sections the
    ///   proxy models -- the payload columns and STREAM_DIR, where the proxy
    ///   counts uncompressed bytes and the object stores compressed ones -- and
    ///   is the disclosed condition on the band: a section the proxy does not
    ///   model (POSTINGS, SKIP_IDX, PAGE_DIR) growing faster than the payload it
    ///   does model can carry a part past it.
    ///
    /// Named for the bytes it measures: it governs object geometry (object count
    /// and per-object stored size), which is what the historical
    /// `max_l1_part_bytes` name always implied. It does NOT bound memory; neither
    /// does [`Self::l1_part_memory_target_bytes`], which is a split target in
    /// decoded heap. A part closes on whichever of the two is reached first.
    /// Default [`DEFAULT_MAX_L1_PART_BYTES`] (256 MiB). On a wide schema the
    /// binding target is usually [`Self::l1_part_memory_target_bytes`], not
    /// this one, so objects come out at the memory target divided by the
    /// schema's heap-to-stored ratio. Lowering this knob therefore does not
    /// grow objects, it caps them lower; on the RSEG metrics path it is the
    /// operator's cap on stored object size whatever the memory target resolves
    /// to. The RLOG merge reads it through [`Self::rlog_stored_target_bytes`],
    /// which prefers [`Self::rlog_max_l1_part_bytes`] when set (the binaries
    /// set it to follow the derived memory target). The RSPAN merge does not
    /// read it.
    pub max_l1_part_bytes: u64,
    /// The **memory split target**: close the in-progress L1 part once its
    /// decoded record-heap estimate reaches this
    /// ([`crate::rlog::estimate_record`] / `rspan_codec::estimate_record`, the
    /// Rust heap of a `LogRecord`/`SpanRecord`, an order of magnitude larger
    /// than its compressed bytes on a wide schema). A merge holds one whole
    /// in-progress part's records live before its content-addressed key can be
    /// computed, and this is the knob that decides how big that part gets.
    ///
    /// It is a target, not a bound: no code caps resident bytes at this number,
    /// and how far a part can run past it is path-specific. Size a host from the
    /// overshoot, not from this value alone.
    ///
    /// - RLOG (`rlog.rs`): checked after every merged record, so a part exceeds
    ///   the target by at most one record. This is the case issue #711 fixed,
    ///   and the only path where the target is nearly tight.
    /// - RSPAN (`rspan_codec.rs`): checked only at a `trace_id` transition, so
    ///   that a trace never straddles two parts. A part therefore runs past the
    ///   target by up to a whole trace, and a single trace larger than the
    ///   target is buffered whole however large it is. The number to survive on
    ///   a span shard is this target plus the largest single trace a tenant
    ///   sends, not this target.
    /// - RSEG metrics (`build.rs`): does not read this field at all. Parts close
    ///   on [`Self::max_l1_part_bytes`] (the estimated stored object), and that path's
    ///   peak is one fetch window's raw pages, plus one series' decoded samples
    ///   (a multi-run series is decoded and merged whole, so it is bounded by
    ///   that series' size and by nothing configurable). A finished part's
    ///   encoded bytes are released at its PUT (ADR-0979 decision 3), so they
    ///   are not a term of that peak.
    ///
    /// Named for what it measures and what it does: a split target in decoded
    /// heap. It was `max_l1_part_memory_bytes`, which reads as a resident-bytes
    /// ceiling an operator can size a host from, and none of the three paths
    /// enforces one. A part closes on whichever of this and
    /// [`Self::max_l1_part_bytes`] is reached first.
    ///
    /// [`Self::default`] carries [`DEFAULT_L1_PART_MEMORY_TARGET_BYTES`]
    /// (256 MiB). This field is what the RSPAN merge reads, and what the RLOG
    /// merge reads unless [`Self::rlog_l1_part_memory_target_bytes`] is set.
    /// An operator flag sets this field, so it reaches both codecs. A value the
    /// binaries derive from the memory budget does not: it goes to
    /// [`Self::rlog_l1_part_memory_target_bytes`] only
    /// ([`ResolvedL1PartMemoryTarget::apply_to`]), and RSPAN stays at
    /// 256 MiB, because RSPAN has no stored-size target to cap object size and
    /// the claim-lease startup check (ADR-1029 decision 3) sizes the largest
    /// part as the larger of [`Self::max_l1_part_bytes`] and
    /// [`Self::rlog_max_l1_part_bytes`].
    pub l1_part_memory_target_bytes: u64,
    /// The memory split target the RLOG merge (`rlog::merge_catalogs`, which
    /// compaction and the RLOG erasure rewrite share) reads in place of
    /// [`Self::l1_part_memory_target_bytes`] when set
    /// ([`Self::rlog_memory_target_bytes`]). The RSPAN merge never reads it.
    ///
    /// `ravel-server` and `ravel-cli maintain` set it from
    /// [`ResolvedL1PartMemoryTarget::apply_to`]: unless the operator sets the
    /// knob, they derive it as the smallest of `memory_budget / 8 /
    /// concurrent_merges`, the claim lease's part cap and 8 GiB, lifted to
    /// 256 MiB ([`derive_l1_part_memory_target`]), where `memory_budget` is the
    /// process budget less the merge cursor budget ([`merge_memory_budget_bytes`]).
    /// RLOG part size then follows host memory instead of a fixed number that
    /// splits wide-schema parts long before [`Self::max_l1_part_bytes`] (issue
    /// #2351). A derived or fallback value also sets
    /// [`Self::rlog_max_l1_part_bytes`] to itself. Default `None`.
    pub rlog_l1_part_memory_target_bytes: Option<u64>,
    /// The RLOG merge's stored-size cap, read in place of
    /// [`Self::max_l1_part_bytes`] when set ([`Self::rlog_stored_target_bytes`]).
    /// RSEG reads [`Self::max_l1_part_bytes`] and RSPAN reads neither cap, so
    /// this field changes nothing outside `rlog::merge_catalogs` (compaction and
    /// the RLOG erasure rewrite).
    ///
    /// [`ResolvedL1PartMemoryTarget::apply_to`] sets it to the derived (or
    /// fallback) memory target, which keeps the memory target the binding
    /// target. The cap only costs anything when the payload proxy reaches it,
    /// because that schedules an exact-encode probe that clones the part's
    /// record heap. The proxy ([`crate::rlog::estimate_stored_record`] plus
    /// `estimate_stored_stream` once per stream in the part)
    /// counts uncompressed payload bytes; the memory target counts the heap
    /// ([`crate::rlog::estimate_record`]). Every term of the proxy is at most
    /// the corresponding heap term: a string, byte string or key of `n` bytes is
    /// charged `n` by the proxy and `n + 16` by the heap estimate (zero bytes
    /// charge zero in both); a scalar attribute is charged 8 or 1 by the proxy
    /// and its attribute or element slot by the heap estimate, which is larger
    /// (`const` assertions in `rlog.rs` pin both); a record's fixed 16 bytes and
    /// the 32-byte STREAM_DIR entry of its stream's first record in the part are
    /// charged against `size_of::<LogRecord>()`; and a list or map element's
    /// 1-byte tag is charged against its element slot. So a part's proxy never
    /// exceeds its heap estimate, and with this cap at or above the memory
    /// target the memory target fires first on every record and no probe runs
    /// (the `derived_defaults_run_zero_probes_and_close_on_the_memory_target`
    /// test in `rlog.rs` runs a merge at that setting and asserts zero probes).
    ///
    /// An explicit stored-size cap (`--max-l1-part-bytes`) sets this field and
    /// [`Self::max_l1_part_bytes`] together. An explicit memory target
    /// (`--l1-part-memory-target-bytes`) does not touch it, so one set above the
    /// shared cap runs probes: that is the opt-in path. Default `None`.
    pub rlog_max_l1_part_bytes: Option<u64>,
    /// Buckets with fewer L0 records than this are left uncompacted; set 1 for
    /// v1-retirement campaigns.
    pub min_compaction_inputs: usize,
    /// Suffix-probe size for the first footer GET of each input.
    pub footer_probe_bytes: u64,
    /// How many per-input reads a compaction or rewrite keeps in flight at
    /// once: the commit-record GETs of [`crate::read::load_inputs`] and the
    /// per-input catalog loads that follow it. A bucket with hundreds of
    /// inputs otherwise pays one full store round trip per input in sequence,
    /// which dominates the merge on any real object store.
    ///
    /// This bounds request concurrency only, never resident bytes: a catalog
    /// is directory metadata (KBs), and the merge itself still streams one
    /// block at a time per cursor. Values below 1 are treated as 1. Output
    /// bytes do not depend on it -- inputs are re-sorted into canonical order
    /// after loading and the merge is a deterministic k-way merge over that
    /// order -- so raising it can change timing but never content. Default
    /// [`DEFAULT_INPUT_READ_CONCURRENCY`] (8).
    pub input_read_concurrency: usize,
    /// The RLOG merge's per-stream cursor-memory budget (ADR-0979 decision 4).
    /// Unlike the two part-split targets and `input_read_concurrency`, this one
    /// DOES bound resident memory: it caps the sum, over the cursors open at
    /// once for one stream, of what each cursor holds -- two row groups' stored
    /// bytes, the cursor's location metadata, and its decoded columnar block.
    ///
    /// A cursor is admitted against a pre-decode CEILING on those terms, priced
    /// from resident directory metadata: the block's decoded size is
    /// `16 B x rows x column-id width + its string pages' uncomp_len +
    /// 40 B x rows x string columns + 48 B x column-id width x 5 slot vectors
    /// x 2 for their growth step`,
    /// maximized over the cursor's candidate blocks. That is a shape formula,
    /// not a stored-size one, because the page codecs are size-adaptive: a
    /// constant column stores a few bytes per block and still decodes to
    /// `16 B x rows`.
    ///
    /// The reservation is charged BEFORE a cursor fetches or decodes anything,
    /// so the budget is enforced at reserve time and the merge fails closed
    /// rather than after allocating: if opening a cursor that merge order
    /// requires would push the charge over the budget, the run aborts with a
    /// typed [`crate::error::MaintainError::MergeCursorBudgetExceeded`] naming
    /// the stream, the reservation, and the budget, before publishing anything.
    /// Once the decode completes, the charge is reconciled down to the cursor's
    /// actual residency (its decoded block's `heap_estimate`, the raw bytes it
    /// actually holds, and its location metadata), which is the basis
    /// [`DEFAULT_MERGE_CURSOR_BUDGET_BYTES`] is sized from. The reconcile is a
    /// lowering, never a release of the bound: before an open cursor decodes a
    /// LATER block, its charge grows back to cover that block's ceiling on top
    /// of what it still holds, and a growth the budget cannot take refuses with
    /// the same typed error before the decode starts. Without that, the freed
    /// budget could be spent admitting more cursors and every one of them could
    /// then grow toward its own ceiling with nothing checking the sum again.
    /// Nothing is published, the L0 inputs stay live and queryable, and any
    /// parts already PUT age out under the unreferenced-part sweep exactly like
    /// an abandoned run's. This converts an out-of-memory kill at an arbitrary
    /// point into a refusal naming the stream and the number to raise. Because
    /// overlap-gated admission ([`AdmissionMode::Overlap`]) releases each
    /// cursor's reservation as it drains, the charge tracks the concurrent
    /// overlap `D`, not the input count. Default
    /// [`DEFAULT_MERGE_CURSOR_BUDGET_BYTES`] (20 GiB); a budget so small that
    /// even one stream's minimum admissible cursor set does not fit refuses that
    /// stream's merge.
    pub merge_cursor_budget_bytes: u64,
    /// How the RLOG merge admits a stream's cursors (ADR-0979 decision 2).
    /// Default [`AdmissionMode::Overlap`] (the bounded path). Only the
    /// differential part-hash test sets [`AdmissionMode::EagerAll`]; output
    /// bytes are identical either way.
    pub merge_admission: AdmissionMode,
    /// zstd level the RLOG compaction merge and the erasure rewrite write their
    /// parts at (ADR-2135 decision 4). Checked by [`validate_rlog_zstd_level`]
    /// on a logs or query-audit bucket at the start of
    /// [`crate::compact::compact_bucket`] and its claimed variants,
    /// [`crate::rewrite::migrate_bucket_format`],
    /// [`crate::rewrite::rewrite_and_publish`],
    /// [`crate::erasure_rewrite::erasure_rewrite_bucket`], and
    /// [`crate::erasure_rewrite::build_rewrite_logs`], before the run's first
    /// store request; a level outside
    /// [`MIN_RLOG_ZSTD_LEVEL`]`..=`[`MAX_RLOG_ZSTD_LEVEL`] fails the run with
    /// [`crate::error::MaintainError::InvalidRlogZstdLevel`]. Default
    /// [`DEFAULT_RLOG_ZSTD_LEVEL`] (9).
    pub rlog_zstd_level: i32,
    /// This compactor process's uuid. Informational only: it is recorded in
    /// each part's footer `writer_id` and never enters dedup priority.
    /// Default is the nil uuid; the service sets a real one.
    pub compactor_writer_id: Uuid,
    /// Shared grace period for the orphan and unreferenced-part age gates
    /// (docs/consistency-model.md "Deletion and GC"). An object is
    /// only ever a deletion candidate once its `last_modified` age exceeds
    /// this plus the relevant lifetime bound. Default
    /// [`DEFAULT_GRACE_NS`] (24 h).
    pub grace_ns: i64,
    /// Horizon between a deletion anchor (a compaction record's
    /// `created_unix_ns`, a tombstone's `retired_at_ns`) and physical
    /// deletion. Must satisfy `>= max_query_duration + grace +
    /// clock_skew_allowance` so a query resolved just before the anchor still
    /// has time to read the inputs it pinned even when the sweeper's clock
    /// leads the reader's. Default [`DEFAULT_PROTECTION_HORIZON_NS`]
    /// (25 h 5 min).
    pub protection_horizon_ns: i64,
    /// The longest a single query may run, set from `sys/gc`'s
    /// `max_query_duration_ns` by every sweep driver that reads `sys/gc` (the
    /// server's maintain mode and `ravel-cli maintain sweep`). A term of the
    /// pinned-query window (ADR-1133 decision 3). Default
    /// [`DEFAULT_MAX_QUERY_DURATION_NS`] (1 h).
    pub max_query_duration_ns: i64,
    /// The HEAD cache TTL query processes are held to, set from `sys/gc`'s
    /// `head_cache_ttl_ns` by the same drivers. A term of the pinned-query
    /// window (ADR-1133 decision 3). Default
    /// [`ravel_catalog::DEFAULT_HEAD_CACHE_TTL_NS`] (30 s).
    pub head_cache_ttl_ns: i64,
    /// Mass-orphan circuit breaker minimum candidate count (ADR-0048
    /// decision 4). The breaker trips a pass only when it would delete at
    /// least this many orphan candidates AND more than
    /// [`Self::orphan_breaker_max_ratio`] of the shard's listed L0 objects;
    /// both conditions must hold, so a tiny shard's small orphan count never
    /// trips on ratio alone. Default [`DEFAULT_ORPHAN_BREAKER_MIN_COUNT`]
    /// (50).
    pub orphan_breaker_min_count: usize,
    /// Mass-orphan circuit breaker maximum ratio (ADR-0048 decision 4): the
    /// fraction of a shard's listed L0 objects that orphan candidates may
    /// reach before the breaker trips, paired with
    /// [`Self::orphan_breaker_min_count`]. Default
    /// [`DEFAULT_ORPHAN_BREAKER_MAX_RATIO`] (0.10).
    pub orphan_breaker_max_ratio: f64,
    /// Second horizon for orphan GC (ADR-0058 amendment): how long a
    /// record-less L0 data object, once it clears the orphan age gate, survives
    /// in the `quarantine/` prefix before the reaper physically deletes it.
    /// Orphan GC never deletes a candidate directly; it copies the object under
    /// `quarantine/<original key>/q<quarantined_at_ns>` and deletes the live
    /// key, and [`crate::sweep::sweep_quarantine`] deletes the copy only once
    /// its embedded quarantine timestamp is more than this behind the clock. A
    /// small out-of-band commit-record loss that stays under the mass-orphan
    /// breaker's thresholds is therefore recoverable for this long instead of
    /// being deleted permanently at the first horizon. Default
    /// [`DEFAULT_QUARANTINE_HORIZON_NS`] (7 days); an existing deployment gets
    /// the recoverable behaviour without setting anything.
    pub quarantine_horizon_ns: i64,
    /// One-shot deliberate operator override for a tripped mass-orphan
    /// breaker (ADR-0048 decision 4). The server never sets this; it exists
    /// so a future `ravel-cli maintain sweep --override-orphan-breaker`
    /// invocation can force a single overridden pass. Default `false`: the
    /// breaker's halt is sticky and never auto-resumes (ADR-0048 rejected
    /// alternative 3), because in a mass-orphan state the record-absence
    /// signal orphan GC re-verifies against is exactly what out-of-band
    /// record loss forges, so only a human can tell mass record loss from a
    /// legitimate mass abandonment.
    pub force_orphan_gc: bool,
    /// How far behind the current ingest-hour bucket an idempotency marker
    /// (ADR-0051 §5) must be before [`crate::sweep::sweep_idempotency_markers`]
    /// deletes it. Kept here rather than as a bare parameter to that function,
    /// matching how every other shared sweep/compaction knob in this struct is
    /// threaded, and so existing call sites built via
    /// `..CompactorConfig::default()` stay unaffected. Default
    /// [`DEFAULT_IDEM_DEDUP_WINDOW_HOURS`] (24h): this crate's own policy
    /// default matching the window ADR-0051 documents, not a shared
    /// code-level default (`read_marker` has no default of its own).
    pub idem_dedup_window_hours: u32,
    /// Retention window for query-audit records on
    /// [`crate::query_audit::QUERY_AUDIT_SHARD`]. A
    /// query-audit record whose newest event is older than this is swept by
    /// [`crate::audit_retention::sweep_audit_retention`], horizon-gated on the
    /// record's durable `created_unix_ns` and legal-hold-gated exactly as the
    /// superseded-input sweep is. Independent of [`RetentionConfig`]'s
    /// per-tenant ADR-0019 windows, which govern tenant data, not this
    /// server-written activity log. Threaded through the config like every
    /// other sweep knob so `..CompactorConfig::default()` call sites are
    /// unaffected. Default [`DEFAULT_AUDIT_RETENTION_NS`] (90 days).
    pub audit_retention_window_ns: i64,
    /// Retention window for the `Signal::Alerts` transition history (ADR-1688
    /// decision 5). An alert commit record whose newest event is older than
    /// this is swept by
    /// [`crate::alert_retention::sweep_alert_retention`], unless the keep set
    /// that sweep is given spares it (ADR-1688 decision 2). The keep set holds
    /// the last record of every identity the alert state memo carries, which is
    /// a firing alert's current state and also the final `resolved` record of a
    /// rule that has since been deleted, and its watermark hour: a record whose
    /// ingest hour is not strictly below that watermark is kept too, because the
    /// memo is not complete for it. The sweep is horizon-gated on the record's
    /// durable `created_unix_ns` and gated on the caller-supplied
    /// [`crate::sweep::LeaseCheck`] -- the same hook the audit retention and
    /// superseded-input sweeps consult, which the server populates with the
    /// tenant's legal holds. The window doubles as the evaluator's
    /// cold-start fold horizon (decision 4). Independent of [`RetentionConfig`]'s
    /// per-tenant ADR-0019 windows: alert transitions are a server-written
    /// history, not tenant data, and are not tombstone-gated through the
    /// resolver. Threaded through the config like every other sweep knob so
    /// `..CompactorConfig::default()` call sites are unaffected. Default
    /// [`DEFAULT_ALERT_RETENTION_NS`] (90 days); `0`, or any negative value,
    /// disables the sweep.
    pub alert_retention_window_ns: i64,
    /// Dry-run switch. When `true`, every maintenance path
    /// computes exactly the same eligible set and decision it would in a real
    /// run -- all reads (LIST/GET/HEAD, re-verify listings, k-way merges,
    /// part planning) happen identically -- but each `store.put`/`store.delete`
    /// that would mutate or delete an object is skipped while the surrounding
    /// counters still advance, so a report reflects what a real run *would*
    /// have written or deleted. This is carried in the config (already threaded
    /// through every compaction/sweep/retention function) rather than added as
    /// a separate parameter to each so existing call sites, which all build the
    /// config via `..CompactorConfig::default()`, stay byte-for-byte unchanged
    /// with `dry_run == false`. Default `false`.
    pub dry_run: bool,
    /// Optional test-injectable accounting hook for the RLOG and
    /// RSPAN compaction merges' peak resident memory. `None` in
    /// production (the merges' accounting hooks are skipped); a test installs
    /// one and reads its high-water marks after `compact_bucket` to assert the
    /// k-way merge stayed bounded independently of stream/trace size. Carried
    /// in the config, like every other merge knob, so
    /// `..CompactorConfig::default()` call sites are unaffected. Default
    /// `None`.
    pub merge_memory_tracker: Option<MergeMemoryTracker>,
    /// Optional test-injectable ledger of the run's store requests and wire
    /// bytes, split by the phase that issued them (ADR-0996 task 996-8).
    /// `None` in production (every hook is skipped); a test or an operator
    /// build installs one and reads [`RequestLedger::report`] after the run.
    /// Counters only: no fetch decision, coalescing choice, or budget anywhere
    /// in this crate reads a ledger figure (see [`crate::request_ledger`]).
    /// Carried in the config like every other merge knob, so
    /// `..CompactorConfig::default()` call sites are unaffected. Default
    /// `None`.
    pub request_ledger: Option<RequestLedger>,
    /// Slow safety-net re-verify cadence for the interior zone (ADR-0065
    /// decision 3, config `maintain_interior_reverify`). A terminal interior
    /// bucket is re-evaluated no later than this after its last verification,
    /// or sooner if its computed retention expiry arrives first
    /// ([`crate::scan::classify_zone`], [`crate::scan::MaintainMemo`]). Head
    /// and tail hours ignore this and are evaluated every tick. Default
    /// [`DEFAULT_INTERIOR_REVERIFY_NS`] (6 h); non-positive disables the
    /// safety net (every interior bucket is always due).
    pub interior_reverify_ns: i64,
    /// Whether this process takes advisory compaction claims (ADR-1029
    /// decision 5). Default [`Coordination::On`]; [`Coordination::Off`]
    /// disables claiming fleet-wide. Claims are advisory either way: the
    /// compaction record's `CreateIfAbsent` remains the only serialization
    /// point that decides anything durable.
    pub coordination: Coordination,
    /// The cost gate (ADR-1029 decision 4): a bucket is claimed only when its
    /// listed input bytes (the summed `object_size` of the L0 commit records
    /// the bucket listing found) reach this. Below it, a duplicated merge
    /// costs less than the PUT-class claim traffic that would prevent it, so
    /// the bucket runs unclaimed through the same pipeline. Default
    /// [`DEFAULT_CLAIM_MIN_INPUT_BYTES`] (64 MiB).
    pub claim_min_input_bytes: u64,
    /// How long a claim this process takes stays live without a renewal
    /// (ADR-1029 decision 3). Held as a [`Duration`] rather than this struct's
    /// usual nanoseconds because it is passed straight to
    /// [`ravel_fleet::claim::ClaimConfig`], which the claim primitive reads.
    /// Default [`DEFAULT_CLAIM_LEASE_DURATION`] (300 s).
    pub claim_lease_duration: Duration,
    /// Who this process claims as, and the clock its claim decisions read.
    /// `None` (the default) means this caller takes no claims at all, whatever
    /// [`Self::coordination`] says; the background supervisor installs one per
    /// tick. See [`ClaimParticipant`].
    pub claim_participant: Option<ClaimParticipant>,
    /// The live claim of the run in progress, installed by
    /// [`crate::compact::compact_bucket_claimed`] into its own per-run clone of
    /// this config once the bucket's claim is taken, and read by the
    /// cancellation checkpoints inside the merge
    /// ([`crate::claim_guard::checkpoint`]). It is per RUN, never per process:
    /// two buckets compacted concurrently under one base config each get their
    /// own clone carrying their own guard, so a renewal for one bucket can
    /// never be mistaken for the other's. A caller never sets this; setting it
    /// by hand claims nothing, because the acquisition happens in the driver.
    /// Default `None`, which makes every checkpoint a single `Option` check.
    pub claim_guard: Option<crate::claim_guard::ClaimGuard>,
}

impl Default for CompactorConfig {
    fn default() -> Self {
        CompactorConfig {
            max_flush_lifetime_ns: DEFAULT_MAX_FLUSH_LIFETIME_NS,
            clock_skew_allowance_ns: DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
            max_compaction_lifetime_ns: DEFAULT_MAX_COMPACTION_LIFETIME_NS,
            max_l1_part_bytes: DEFAULT_MAX_L1_PART_BYTES,
            l1_part_memory_target_bytes: DEFAULT_L1_PART_MEMORY_TARGET_BYTES,
            rlog_l1_part_memory_target_bytes: None,
            rlog_max_l1_part_bytes: None,
            min_compaction_inputs: DEFAULT_MIN_COMPACTION_INPUTS,
            footer_probe_bytes: DEFAULT_FOOTER_PROBE_BYTES,
            input_read_concurrency: DEFAULT_INPUT_READ_CONCURRENCY,
            merge_cursor_budget_bytes: DEFAULT_MERGE_CURSOR_BUDGET_BYTES,
            merge_admission: AdmissionMode::Overlap,
            rlog_zstd_level: DEFAULT_RLOG_ZSTD_LEVEL,
            compactor_writer_id: Uuid::nil(),
            grace_ns: DEFAULT_GRACE_NS,
            protection_horizon_ns: DEFAULT_PROTECTION_HORIZON_NS,
            max_query_duration_ns: DEFAULT_MAX_QUERY_DURATION_NS,
            head_cache_ttl_ns: ravel_catalog::DEFAULT_HEAD_CACHE_TTL_NS,
            orphan_breaker_min_count: DEFAULT_ORPHAN_BREAKER_MIN_COUNT,
            orphan_breaker_max_ratio: DEFAULT_ORPHAN_BREAKER_MAX_RATIO,
            quarantine_horizon_ns: DEFAULT_QUARANTINE_HORIZON_NS,
            force_orphan_gc: false,
            idem_dedup_window_hours: DEFAULT_IDEM_DEDUP_WINDOW_HOURS,
            audit_retention_window_ns: DEFAULT_AUDIT_RETENTION_NS,
            alert_retention_window_ns: DEFAULT_ALERT_RETENTION_NS,
            dry_run: false,
            merge_memory_tracker: None,
            request_ledger: None,
            interior_reverify_ns: DEFAULT_INTERIOR_REVERIFY_NS,
            coordination: Coordination::On,
            claim_min_input_bytes: DEFAULT_CLAIM_MIN_INPUT_BYTES,
            claim_lease_duration: DEFAULT_CLAIM_LEASE_DURATION,
            claim_participant: None,
            claim_guard: None,
        }
    }
}

impl CompactorConfig {
    /// The memory split target the RLOG merge closes a part at:
    /// [`Self::rlog_l1_part_memory_target_bytes`] when set, else
    /// [`Self::l1_part_memory_target_bytes`].
    pub fn rlog_memory_target_bytes(&self) -> u64 {
        self.rlog_l1_part_memory_target_bytes
            .unwrap_or(self.l1_part_memory_target_bytes)
    }

    /// The stored-size cap the RLOG merge closes a part at:
    /// [`Self::rlog_max_l1_part_bytes`] when set, else
    /// [`Self::max_l1_part_bytes`].
    pub fn rlog_stored_target_bytes(&self) -> u64 {
        self.rlog_max_l1_part_bytes
            .unwrap_or(self.max_l1_part_bytes)
    }

    /// The largest stored-size cap any codec reads, which is the part size the
    /// claim-lease startup check ([`claim_lease_below_warn_threshold`], ADR-1029
    /// decision 3) must size a lease against: the larger of
    /// [`Self::max_l1_part_bytes`] (RSEG) and [`Self::rlog_stored_target_bytes`]
    /// (RLOG).
    pub fn largest_stored_target_bytes(&self) -> u64 {
        self.max_l1_part_bytes.max(self.rlog_stored_target_bytes())
    }

    /// Whether [`Self::claim_lease_duration`] is under ADR-1029 decision 3's
    /// startup warning threshold for the largest part any codec can write
    /// ([`Self::largest_stored_target_bytes`]). The one call the binaries' startup
    /// checks make.
    pub fn claim_lease_below_warn_threshold(&self) -> bool {
        claim_lease_below_warn_threshold(
            self.claim_lease_duration,
            self.largest_stored_target_bytes(),
        )
    }

    /// The seal margin: a bucket ending at `bucket_end_ns` is sealed once
    /// `now_ns >= bucket_end_ns + this`. No new commit record can
    /// appear in the bucket after that, so a single strongly consistent LIST
    /// is a complete, repeatable input set.
    pub fn seal_margin_ns(&self) -> i64 {
        self.max_flush_lifetime_ns
            .saturating_add(self.clock_skew_allowance_ns)
    }

    /// The orphan-GC age gate: an `l0/` data object with no commit record is a
    /// deletion candidate only once its `last_modified` age exceeds this
    /// (`grace + max_flush_lifetime`). The `max_flush_lifetime` term
    /// is what makes the writer interlock hold: a writer abandons any flush
    /// older than that and never publishes it, so a record-less object older
    /// than this can never gain a commit record later (ADR-0010 §11).
    pub fn orphan_age_gate_ns(&self) -> i64 {
        self.grace_ns.saturating_add(self.max_flush_lifetime_ns)
    }

    /// The unreferenced-part age gate: an `l1/` object referenced by no
    /// compaction record in its bucket is a deletion candidate only once its
    /// `last_modified` age exceeds this (`grace + max_compaction_lifetime`). The `max_compaction_lifetime` term mirrors the abandonment
    /// deadline: a compactor past that deadline never
    /// publishes, so it can never re-reference a part this old.
    pub fn unreferenced_part_age_gate_ns(&self) -> i64 {
        self.grace_ns
            .saturating_add(self.max_compaction_lifetime_ns)
    }

    /// The ADR-0019 §5 retention validation floor
    /// (`max_ingest_lag + max_flush_lifetime + clock_skew_allowance` plus one
    /// bucket span). A retention window `R` below this could tombstone a
    /// bucket before it is guaranteed sealed. `max_ingest_lag_ns` is taken
    /// from the retention config (matching ravel-catalog's
    /// [`DEFAULT_MAX_INGEST_LAG_NS`]); the other two terms are this
    /// compactor config's own.
    pub fn retention_floor_ns(&self, max_ingest_lag_ns: i64) -> i64 {
        max_ingest_lag_ns
            .saturating_add(self.max_flush_lifetime_ns)
            .saturating_add(self.clock_skew_allowance_ns)
            .saturating_add(NS_PER_HOUR)
    }
}

/// A raw per-tenant retention policy as a deployment would express it
/// (ADR-0019 §5): `retention: { default: none, tenants: { <id>: R } }`.
/// Tenant ids are plain strings here; [`RetentionConfig::from_policy`] hashes
/// them at load so the validated config never stores raw ids.
#[derive(Debug, Clone, Default)]
pub struct RetentionPolicy {
    /// Default window in nanoseconds, or `None` for no retention (the
    /// ADR-0019 §5 default).
    pub default: Option<i64>,
    /// Per-tenant overrides: `(tenant_id, window_ns)`.
    pub tenants: Vec<(String, i64)>,
}

/// A retention window below the ADR-0019 §5 floor was configured. Rejected at
/// load so a bucket can never be tombstoned before it is sealed.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RetentionConfigError {
    #[error(
        "retention window {window_ns} ns for {tenant} is below the ADR-0019 floor of {floor_ns} ns (max_ingest_lag + max_flush_lifetime + clock_skew_allowance + one bucket span)"
    )]
    BelowFloor {
        tenant: String,
        window_ns: i64,
        floor_ns: i64,
    },
}

/// The validated per-tenant retention configuration (ADR-0019).
/// Only the sweeper reads it; resolvers never do (ADR-0019 §5 / alternative
/// 1). Tenant ids are hashed at construction, so this struct never holds a
/// raw tenant id.
#[derive(Debug, Clone, Default)]
pub struct RetentionConfig {
    default_window_ns: Option<i64>,
    tenants: HashMap<TenantHash, i64>,
    floor_ns: i64,
}

impl RetentionConfig {
    /// Validate a [`RetentionPolicy`] against the ADR-0019 §5 floor and hash
    /// every tenant id (hashed at load so the config never stores a raw tenant id). Rejects
    /// any window below `config.retention_floor_ns(max_ingest_lag_ns)`.
    pub fn from_policy(
        policy: RetentionPolicy,
        config: &CompactorConfig,
        max_ingest_lag_ns: i64,
    ) -> Result<Self, RetentionConfigError> {
        let floor_ns = config.retention_floor_ns(max_ingest_lag_ns);
        if let Some(r) = policy.default
            && r < floor_ns
        {
            return Err(RetentionConfigError::BelowFloor {
                tenant: "default".to_string(),
                window_ns: r,
                floor_ns,
            });
        }
        let mut tenants = HashMap::with_capacity(policy.tenants.len());
        for (id, r) in policy.tenants {
            if r < floor_ns {
                return Err(RetentionConfigError::BelowFloor {
                    tenant: id,
                    window_ns: r,
                    floor_ns,
                });
            }
            tenants.insert(TenantId::new(id).hash(), r);
        }
        Ok(RetentionConfig {
            default_window_ns: policy.default,
            tenants,
            floor_ns,
        })
    }

    /// The retention window that applies to one tenant: its per-tenant
    /// override if set, else the default, else `None` (no retention).
    pub fn window_for(&self, tenant: &TenantHash) -> Option<i64> {
        self.tenants.get(tenant).copied().or(self.default_window_ns)
    }

    /// The ADR-0019 §5 floor this config was validated against (introspection
    /// and tests).
    pub fn floor_ns(&self) -> i64 {
        self.floor_ns
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    const GIB: u64 = 1024 * 1024 * 1024;
    const MIB: u64 = 1024 * 1024;

    /// A lease long enough that its part cap (420 GiB) never binds, so a test of
    /// the share, floor and ceiling terms is not also a test of the lease term.
    const LONG_LEASE: Duration = Duration::from_secs(86_400);

    /// The issue #2351 acceptance figures, pinned exactly. A derivation without
    /// the clamp gives 16 GiB for the 128 GiB row and 128 MiB for the 1 GiB
    /// row; one that divides by 4 or 16 instead of 8 fails the 30 GiB row.
    #[test]
    fn derived_target_is_one_eighth_of_the_budget_clamped() {
        let derive =
            |budget, merges| derive_l1_part_memory_target_bytes(budget, merges, LONG_LEASE);
        assert_eq!(derive(2 * GIB, 1), 256 * MIB);
        assert_eq!(derive(2 * GIB, 1), 268_435_456);
        assert_eq!(derive(30 * GIB, 1), 4_026_531_840);
        assert_eq!(4_026_531_840, 3 * GIB + 3 * GIB / 4);
        assert_eq!(derive(128 * GIB, 1), 8 * GIB);
        assert_eq!(derive(128 * GIB, 1), 8_589_934_592);
        // Below the floor: 1 GiB / 8 = 128 MiB clamps up to 256 MiB.
        assert_eq!(derive(GIB, 1), 256 * MIB);
        assert_eq!(derive(0, 1), 256 * MIB);
        assert_eq!(derive(u64::MAX, 1), 8 * GIB);
    }

    #[test]
    fn derived_target_divides_by_concurrent_merges() {
        let derive =
            |budget, merges| derive_l1_part_memory_target_bytes(budget, merges, LONG_LEASE);
        assert_eq!(derive(30 * GIB, 2), 2_013_265_920);
        assert_eq!(derive(30 * GIB, 2) * 2, derive(30 * GIB, 1));
        // 28 GiB (a 30 GiB host less the server's 2 GiB reserve) over 4 units.
        assert_eq!(derive(28 * GIB, 4), 939_524_096);
        // The clamp applies after the division: 128 GiB over 2 merges is 8 GiB
        // each, and over 4 merges 4 GiB each (clamping first would give 4 GiB
        // and 2 GiB).
        assert_eq!(derive(128 * GIB, 2), 8 * GIB);
        assert_eq!(derive(128 * GIB, 4), 4 * GIB);
        // The floor holds per merge, so a small budget over many merges stays at
        // 256 MiB each.
        assert_eq!(derive(2 * GIB, 8), 256 * MIB);
        // Zero merges is one merge, not a division by zero.
        assert_eq!(derive(30 * GIB, 0), derive(30 * GIB, 1));
    }

    /// The lease term (issue #2351 fix round 3). At a 30 GiB budget and one
    /// merge the memory share is 3.75 GiB, but the default 300 s lease only
    /// supports a 1,500 MiB part (`300 s * 10 MiB/s / 2`), so the target is
    /// 1,572,864,000 bytes and the Display names the lease. With the lease raised
    /// to 1200 s the cap is 6,000 MiB and the share (3.75 GiB) binds.
    ///
    /// Distinguishing:
    /// - dropping the lease term: the 300 s row reads 4,026,531,840, not
    ///   1,572,864,000, and its bound reads `MemoryShare`.
    /// - applying the lease before the floor (a `min` after the clamp): the
    ///   30 s row reads 157,286,400 (below the floor), not 268,435,456.
    /// - a lease cap computed as `lease * 10 MiB/s` without the halving: the
    ///   300 s row reads 3,145,728,000.
    #[test]
    fn lease_term_bounds_the_target_and_the_display_says_so() {
        let default_lease = DEFAULT_CLAIM_LEASE_DURATION;
        assert_eq!(default_lease, Duration::from_secs(300));

        let (bytes, bound) = derive_l1_part_memory_target(30 * GIB, 1, default_lease);
        assert_eq!(bytes, 1_572_864_000);
        assert_eq!(bytes, 1500 * MIB);
        assert_eq!(bound, L1PartMemoryTargetBound::ClaimLease);
        let resolved = ResolvedL1PartMemoryTarget::resolve(None, Some(30 * GIB), 1, default_lease);
        assert_eq!(resolved.bytes, 1_572_864_000);
        assert_eq!(resolved.bound_name(), Some("claim_lease"));
        assert_eq!(
            resolved.to_string(),
            "1572864000 (resolved from a memory budget of 32212254720 over 1 concurrent \
             merge; bound by the claim lease, 300 s allows a part of at most 1572864000 bytes)"
        );

        // The lease raised to 1200 s: the share binds.
        let long = Duration::from_secs(1200);
        assert_eq!(claim_lease_max_part_bytes(long), 6000 * MIB);
        let (bytes, bound) = derive_l1_part_memory_target(30 * GIB, 1, long);
        assert_eq!(bytes, 4_026_531_840);
        assert_eq!(bytes, 3 * GIB + 3 * GIB / 4);
        assert_eq!(bound, L1PartMemoryTargetBound::MemoryShare);
        let resolved = ResolvedL1PartMemoryTarget::resolve(None, Some(30 * GIB), 1, long);
        assert_eq!(resolved.bound_name(), Some("memory_share"));
        assert_eq!(
            resolved.to_string(),
            "4026531840 (resolved from a memory budget of 32212254720 over 1 concurrent \
             merge; bound by the memory share, budget / 8 / merges)"
        );

        // The lease cap is 1500 MiB at 300 s: a 12 GiB budget (1536 MiB share)
        // is lease-bound just over it, and an exact tie is named as the share.
        let (bytes, bound) = derive_l1_part_memory_target(12 * GIB, 1, default_lease);
        assert_eq!(bytes, 1500 * MIB);
        assert_eq!(bound, L1PartMemoryTargetBound::ClaimLease);
        let (bytes, bound) = derive_l1_part_memory_target(1500 * MIB * 8, 1, default_lease);
        assert_eq!(bytes, 1500 * MIB);
        assert_eq!(bound, L1PartMemoryTargetBound::MemoryShare);
    }

    /// The floor is applied last. A lease too short for a floor-sized part still
    /// gets 256 MiB (the floor), the lease check DOES warn at that setting, and
    /// that warning is the operator's signal, not something the derivation
    /// hides by going below the floor. A 30 s lease supports 150 MiB; the
    /// warning threshold for a 256 MiB part is 51.2 s.
    ///
    /// Distinguishing: applying the lease after the floor gives 157,286,400 for
    /// the 30 s row; dropping the floor gives the same.
    #[test]
    fn a_lease_too_short_for_the_floor_yields_the_floor_and_the_check_warns() {
        let short = Duration::from_secs(30);
        assert_eq!(claim_lease_max_part_bytes(short), 150 * MIB);
        let (bytes, bound) = derive_l1_part_memory_target(30 * GIB, 1, short);
        assert_eq!(bytes, 256 * MIB);
        assert_eq!(bound, L1PartMemoryTargetBound::Floor);
        let resolved = ResolvedL1PartMemoryTarget::resolve(None, Some(30 * GIB), 1, short);
        assert_eq!(
            resolved.to_string(),
            "268435456 (resolved from a memory budget of 32212254720 over 1 concurrent \
             merge; bound by the 268435456-byte floor)"
        );
        let mut config = CompactorConfig {
            claim_lease_duration: short,
            ..CompactorConfig::default()
        };
        resolved.apply_to(&mut config);
        assert!(claim_lease_below_warn_threshold(
            short,
            config.largest_stored_target_bytes()
        ));
        // At a lease that supports exactly the floor the check does not warn.
        let enough = Duration::from_secs(52);
        assert!(!claim_lease_below_warn_threshold(enough, 256 * MIB));
    }

    /// The lease cap leaves the startup check quiet: for each lease listed, a
    /// part of exactly the cap does not trip the warning, and one 8 MiB larger
    /// does, so the cap sits at the threshold rather than somewhere safely
    /// below it. Over the derivation range at the default lease, a target above
    /// the floor never warns.
    ///
    /// Distinguishing: a cap without the halving (3,145,728,000 bytes at 300 s)
    /// warns at every row; a cap that is half of the right one fails the
    /// "8 MiB over warns" half only for leases above 3.2 s, which is every row
    /// but the millisecond ones.
    #[test]
    fn the_derived_default_target_never_trips_the_lease_warning() {
        for lease in [
            Duration::from_millis(1),
            Duration::from_millis(1234),
            Duration::from_secs(7),
            Duration::from_secs(30),
            Duration::from_secs(52),
            Duration::from_secs(100),
            DEFAULT_CLAIM_LEASE_DURATION,
            Duration::from_secs(1200),
            LONG_LEASE,
        ] {
            let cap = claim_lease_max_part_bytes(lease);
            assert!(
                !claim_lease_below_warn_threshold(lease, cap),
                "{lease:?} lease warns at its own {cap}-byte cap"
            );
            assert!(
                claim_lease_below_warn_threshold(lease, cap + 8 * MIB),
                "{lease:?} lease does not warn 8 MiB over its {cap}-byte cap"
            );
        }
        // At the defaults, over the whole derivation range of budgets and
        // merge counts, the derived target (and so the RLOG cap that follows
        // it) never warns once it is above the floor.
        for budget in [0, 2 * GIB, 8 * GIB, 30 * GIB, 64 * GIB, 512 * GIB] {
            for merges in [1, 2, 4, 16] {
                let (bytes, bound) =
                    derive_l1_part_memory_target(budget, merges, DEFAULT_CLAIM_LEASE_DURATION);
                if bound != L1PartMemoryTargetBound::Floor {
                    assert!(
                        !claim_lease_below_warn_threshold(DEFAULT_CLAIM_LEASE_DURATION, bytes),
                        "budget {budget} merges {merges}: {bytes} bytes warns at 300 s"
                    );
                }
            }
        }
        // The default shared cap (256 MiB, the floor) is clear at the default
        // lease too, so the default configuration warns nowhere.
        assert!(!claim_lease_below_warn_threshold(
            DEFAULT_CLAIM_LEASE_DURATION,
            DEFAULT_MAX_L1_PART_BYTES
        ));
    }

    /// The budget the derivation divides deducts the overhead reserve and the
    /// merge cursor budget, floored at zero BEFORE the division. The worked
    /// figures from the maintenance guide: a 32 GiB host under `compact-bucket`
    /// is `32 - 2 - 20 = 10 GiB`, so 1.25 GiB per merge (the 1.46 GiB lease cap
    /// does not bind); `ravel-server` on a 30 GiB host is `28 - 20 = 8 GiB`,
    /// so 256 MiB at 4 unit slots.
    ///
    /// Distinguishing:
    /// - dropping the cursor deduction: the CLI row reads 3.75 GiB capped to
    ///   the 1.46 GiB lease term (1,572,864,000), not 1,342,177,280, and the
    ///   server row reads 800 MiB, not 256 MiB.
    /// - dropping the reserve deduction on the CLI: 12 GiB / 8 is 1.5 GiB, not
    ///   1.25 GiB.
    /// - flooring after the division instead of before: a host below the
    ///   deductions would underflow instead of reaching the floor.
    #[test]
    fn the_budget_deducts_the_reserve_and_the_cursor_budget_before_dividing() {
        assert_eq!(MEMORY_OVERHEAD_RESERVE_BYTES, 2 * GIB);
        assert_eq!(DEFAULT_MERGE_CURSOR_BUDGET_BYTES, 20 * GIB);

        // 32 GiB host, compact-bucket (one merge), default lease.
        let budget = merge_memory_budget_bytes(
            host_memory_budget_bytes(32 * GIB),
            DEFAULT_MERGE_CURSOR_BUDGET_BYTES,
        );
        assert_eq!(budget, 10 * GIB);
        let (bytes, bound) = derive_l1_part_memory_target(budget, 1, DEFAULT_CLAIM_LEASE_DURATION);
        assert_eq!(bytes, 1_342_177_280);
        assert_eq!(bytes, GIB + GIB / 4);
        assert_eq!(bound, L1PartMemoryTargetBound::MemoryShare);

        // ravel-server on a 30 GiB host: its budget already nets the reserve.
        let budget = merge_memory_budget_bytes(
            30 * GIB - MEMORY_OVERHEAD_RESERVE_BYTES,
            DEFAULT_MERGE_CURSOR_BUDGET_BYTES,
        );
        assert_eq!(budget, 8 * GIB);
        let (bytes, _) = derive_l1_part_memory_target(budget, 4, DEFAULT_CLAIM_LEASE_DURATION);
        assert_eq!(bytes, 256 * MIB);

        // A host smaller than the deductions floors at zero, then at 256 MiB.
        assert_eq!(merge_memory_budget_bytes(GIB, 20 * GIB), 0);
        assert_eq!(host_memory_budget_bytes(GIB), 0);
        let (bytes, bound) = derive_l1_part_memory_target(0, 1, DEFAULT_CLAIM_LEASE_DURATION);
        assert_eq!(bytes, 256 * MIB);
        assert_eq!(bound, L1PartMemoryTargetBound::Floor);
    }

    /// The ceiling term is named too: a 128 GiB budget with a lease long enough
    /// to allow more than 8 GiB is ceiling-bound.
    #[test]
    fn the_ceiling_is_named_when_it_binds() {
        let (bytes, bound) = derive_l1_part_memory_target(128 * GIB, 1, LONG_LEASE);
        assert_eq!(bytes, 8 * GIB);
        assert_eq!(bound, L1PartMemoryTargetBound::Ceiling);
        let resolved = ResolvedL1PartMemoryTarget::resolve(None, Some(128 * GIB), 1, LONG_LEASE);
        assert_eq!(resolved.bound_name(), Some("ceiling"));
        assert_eq!(
            resolved.to_string(),
            "8589934592 (resolved from a memory budget of 137438953472 over 1 concurrent \
             merge; bound by the 8589934592-byte ceiling)"
        );
        // With the default lease the lease cap is the lower term.
        let (bytes, bound) =
            derive_l1_part_memory_target(128 * GIB, 1, DEFAULT_CLAIM_LEASE_DURATION);
        assert_eq!(bytes, 1_572_864_000);
        assert_eq!(bound, L1PartMemoryTargetBound::ClaimLease);
    }

    /// A flag wins over the derivation and is not clamped either way: a
    /// resolution that let the budget win would print 4026531840 here, and one
    /// that clamped the flag would turn 64 MiB into 256 MiB and 16 GiB into
    /// 8 GiB.
    #[test]
    fn explicit_value_overrides_the_derivation_verbatim() {
        for explicit in [1, 64 * MIB, 16 * GIB] {
            let resolved = ResolvedL1PartMemoryTarget::resolve(
                Some(explicit),
                Some(30 * GIB),
                1,
                DEFAULT_CLAIM_LEASE_DURATION,
            );
            assert_eq!(resolved.bytes, explicit);
            assert_eq!(resolved.source, L1PartMemoryTargetSource::Flag);
            assert_eq!(resolved.source_name(), "flag");
            assert_eq!(resolved.bound_name(), None);
            assert_eq!(resolved.to_string(), format!("{explicit} (set by flag)"));
        }
        // An explicit value also wins when the budget is unknown.
        let resolved = ResolvedL1PartMemoryTarget::resolve(
            Some(64 * MIB),
            None,
            1,
            DEFAULT_CLAIM_LEASE_DURATION,
        );
        assert_eq!(resolved.bytes, 64 * MIB);
        assert_eq!(resolved.source, L1PartMemoryTargetSource::Flag);
    }

    /// Validation is the caller's and is unchanged: a zero flag reaches the
    /// caller as zero (so `ravel-cli`'s `ZeroL1PartMemoryTarget` refusal and
    /// the server's flag refusal still see it) rather than being replaced by a
    /// derived or clamped value that would hide the operator error.
    #[test]
    fn explicit_zero_is_passed_through_for_the_caller_to_refuse() {
        let resolved = ResolvedL1PartMemoryTarget::resolve(
            Some(0),
            Some(30 * GIB),
            1,
            DEFAULT_CLAIM_LEASE_DURATION,
        );
        assert_eq!(resolved.bytes, 0);
        assert_eq!(resolved.source, L1PartMemoryTargetSource::Flag);
    }

    #[test]
    fn unset_flag_derives_from_a_known_budget_and_says_so() {
        let resolved = ResolvedL1PartMemoryTarget::resolve(None, Some(30 * GIB), 2, LONG_LEASE);
        assert_eq!(resolved.bytes, 2_013_265_920);
        assert_eq!(
            resolved.source,
            L1PartMemoryTargetSource::Derived {
                memory_budget_bytes: 32_212_254_720,
                concurrent_merges: 2,
                claim_lease_duration: LONG_LEASE,
                bound: L1PartMemoryTargetBound::MemoryShare,
            }
        );
        assert_eq!(resolved.source_name(), "derived");
        assert_eq!(
            resolved.to_string(),
            "2013265920 (resolved from a memory budget of 32212254720 over 2 concurrent merges; \
             bound by the memory share, budget / 8 / merges)"
        );
    }

    /// One merge reads "1 concurrent merge", not "1 concurrent merges".
    #[test]
    fn derived_display_uses_the_singular_for_one_merge() {
        let resolved = ResolvedL1PartMemoryTarget::resolve(None, Some(30 * GIB), 1, LONG_LEASE);
        assert_eq!(
            resolved.to_string(),
            "4026531840 (resolved from a memory budget of 32212254720 over 1 concurrent merge; \
             bound by the memory share, budget / 8 / merges)"
        );
    }

    /// The derived target reaches the RLOG merge only; RSPAN, which has no
    /// stored-size target, stays at the fixed 256 MiB. A flag reaches both.
    ///
    /// Distinguishing:
    /// - `apply_to` writing the derived bytes into `l1_part_memory_target_bytes`
    ///   (RSPAN taking the derived target): the RSPAN assertion reads
    ///   4026531840, not 268435456.
    /// - `apply_to` leaving the RLOG field unset for a derived source: the RLOG
    ///   assertion reads 268435456, not 4026531840.
    /// - `apply_to` keeping RSPAN at 256 MiB for a flag too: the flag row's
    ///   RSPAN assertion reads 268435456, not 12345.
    /// - `apply_to` leaving the RLOG stored-size cap at the 256 MiB shared
    ///   default on a derived row (the cap left where it was before the
    ///   derivation): the derived 1 GiB row's RLOG cap reads 268435456, not
    ///   1073741824, and so does the 3.75 GiB row's.
    /// - `apply_to` setting the cap to twice the target, or half of it: the cap
    ///   assertions read 2147483648 or 536870912.
    /// - `apply_to` writing the derived bytes into the shared
    ///   `max_l1_part_bytes` (which RSEG reads) instead of the RLOG field: the
    ///   shared-cap assertion reads the derived value, not 268435456.
    /// - `apply_to` overwriting a pre-set shared memory target with the 256 MiB
    ///   constant on a derived or fallback row: the pre-set 4096 reads 268435456.
    #[test]
    fn derived_target_reaches_rlog_only_and_a_flag_reaches_both() {
        let lease = DEFAULT_CLAIM_LEASE_DURATION;
        // 8 GiB over 1 merge is a 1 GiB share, below the 1500 MiB lease cap.
        let mut config = CompactorConfig::default();
        let resolved = ResolvedL1PartMemoryTarget::resolve(None, Some(8 * GIB), 1, lease);
        resolved.apply_to(&mut config);
        assert_eq!(resolved.bytes, GIB);
        assert_eq!(config.rlog_memory_target_bytes(), GIB);
        assert_eq!(config.rlog_stored_target_bytes(), GIB);
        assert_eq!(config.rlog_max_l1_part_bytes, Some(1_073_741_824));
        assert_eq!(config.l1_part_memory_target_bytes, 268_435_456);
        assert_eq!(config.max_l1_part_bytes, 268_435_456);

        let mut config = CompactorConfig::default();
        let resolved = ResolvedL1PartMemoryTarget::resolve(None, Some(30 * GIB), 1, LONG_LEASE);
        resolved.apply_to(&mut config);
        assert_eq!(config.rlog_memory_target_bytes(), 4_026_531_840);
        assert_eq!(config.rlog_memory_target_bytes(), 3 * GIB + 3 * GIB / 4);
        assert_eq!(config.rlog_stored_target_bytes(), 4_026_531_840);
        assert_eq!(config.l1_part_memory_target_bytes, 268_435_456);
        assert_eq!(config.max_l1_part_bytes, 268_435_456);

        // A flag reaches the shared memory target, so RSPAN too; it leaves the
        // RLOG cap following the shared cap, so a flag above 256 MiB is the
        // opt-in to probes.
        let mut config = CompactorConfig::default();
        ResolvedL1PartMemoryTarget::resolve(Some(12345), Some(30 * GIB), 1, lease)
            .apply_to(&mut config);
        assert_eq!(config.rlog_memory_target_bytes(), 12345);
        assert_eq!(config.l1_part_memory_target_bytes, 12345);
        assert_eq!(config.rlog_max_l1_part_bytes, None);
        assert_eq!(config.rlog_stored_target_bytes(), 268_435_456);

        let mut config = CompactorConfig::default();
        ResolvedL1PartMemoryTarget::resolve(None, None, 1, lease).apply_to(&mut config);
        assert_eq!(config.rlog_memory_target_bytes(), 268_435_456);
        assert_eq!(config.l1_part_memory_target_bytes, 268_435_456);
        assert_eq!(config.rlog_stored_target_bytes(), 268_435_456);
        assert_eq!(config.rlog_max_l1_part_bytes, Some(268_435_456));

        // A library caller that never resolves: RLOG reads the shared fields.
        let config = CompactorConfig {
            l1_part_memory_target_bytes: 4096,
            max_l1_part_bytes: 8192,
            ..CompactorConfig::default()
        };
        assert_eq!(config.rlog_memory_target_bytes(), 4096);
        assert_eq!(config.rlog_stored_target_bytes(), 8192);

        // The startup check sizes against the larger of the two caps.
        let config = CompactorConfig {
            max_l1_part_bytes: 100,
            rlog_max_l1_part_bytes: Some(900),
            ..CompactorConfig::default()
        };
        assert_eq!(config.largest_stored_target_bytes(), 900);
        let config = CompactorConfig {
            max_l1_part_bytes: 1000,
            rlog_max_l1_part_bytes: Some(900),
            ..CompactorConfig::default()
        };
        assert_eq!(config.largest_stored_target_bytes(), 1000);
    }

    /// The startup check takes the larger of the two caps. A 2 GiB RLOG cap
    /// under the default 300 s lease needs a 410 s lease, so the check warns
    /// although the shared cap alone (256 MiB, a 51 s threshold) would not.
    ///
    /// Distinguishing: a check on `max_l1_part_bytes` alone reads `false` for
    /// the first row (RLOG cap 2 GiB, shared cap 256 MiB); a check on
    /// `rlog_stored_target_bytes()` alone reads `false` for the third row
    /// (shared cap 2 GiB, RLOG cap 256 MiB).
    #[test]
    fn the_startup_lease_check_sizes_against_the_larger_cap() {
        let config = CompactorConfig {
            rlog_max_l1_part_bytes: Some(2 * GIB),
            ..CompactorConfig::default()
        };
        assert!(config.claim_lease_below_warn_threshold());
        let config = CompactorConfig {
            max_l1_part_bytes: 2 * GIB,
            ..CompactorConfig::default()
        };
        assert!(config.claim_lease_below_warn_threshold());
        let config = CompactorConfig {
            max_l1_part_bytes: 2 * GIB,
            rlog_max_l1_part_bytes: Some(256 * MIB),
            ..CompactorConfig::default()
        };
        assert!(config.claim_lease_below_warn_threshold());
        assert!(!CompactorConfig::default().claim_lease_below_warn_threshold());
    }

    /// `apply_to` writes a derived or fallback resolution into the RLOG fields
    /// only and leaves a shared field the caller already set alone; only the
    /// flag arm writes the shared memory target.
    ///
    /// Distinguishing: an `apply_to` that stores the 256 MiB constant on the
    /// derived and fallback arms reads 268435456 where 4096 / 2048 are pinned.
    #[test]
    fn derived_and_fallback_arms_leave_the_preset_shared_fields_alone() {
        for (budget, merges) in [(Some(30 * GIB), 1), (None, 1)] {
            let mut config = CompactorConfig {
                l1_part_memory_target_bytes: 4096,
                max_l1_part_bytes: 2048,
                ..CompactorConfig::default()
            };
            ResolvedL1PartMemoryTarget::resolve(None, budget, merges, DEFAULT_CLAIM_LEASE_DURATION)
                .apply_to(&mut config);
            assert_eq!(config.l1_part_memory_target_bytes, 4096, "{budget:?}");
            assert_eq!(config.max_l1_part_bytes, 2048, "{budget:?}");
        }
        let mut config = CompactorConfig {
            l1_part_memory_target_bytes: 4096,
            ..CompactorConfig::default()
        };
        ResolvedL1PartMemoryTarget::resolve(
            Some(777),
            Some(30 * GIB),
            1,
            DEFAULT_CLAIM_LEASE_DURATION,
        )
        .apply_to(&mut config);
        assert_eq!(config.l1_part_memory_target_bytes, 777);
    }

    #[test]
    fn unset_flag_without_a_budget_falls_back_to_256_mib() {
        let resolved =
            ResolvedL1PartMemoryTarget::resolve(None, None, 4, DEFAULT_CLAIM_LEASE_DURATION);
        assert_eq!(resolved.bytes, 268_435_456);
        assert_eq!(resolved.source, L1PartMemoryTargetSource::Fallback);
        assert_eq!(resolved.source_name(), "fallback");
        assert_eq!(
            resolved.to_string(),
            "268435456 (fallback: the memory budget is unknown)"
        );
        // The fallback is the struct default, so a library caller that never
        // resolves keeps the same geometry as before.
        assert_eq!(
            CompactorConfig::default().l1_part_memory_target_bytes,
            resolved.bytes
        );
    }

    #[test]
    fn meminfo_total_parses_kib_and_rejects_garbage() {
        let meminfo = "MemFree:         1000 kB\nMemTotal:       32137720 kB\nBuffers: 1 kB\n";
        assert_eq!(parse_meminfo_total_bytes(meminfo), Some(32_137_720 * 1024));
        assert_eq!(parse_meminfo_total_bytes("MemTotal: 4096\n"), Some(4096));
        assert_eq!(parse_meminfo_total_bytes("MemFree: 1000 kB\n"), None);
        assert_eq!(parse_meminfo_total_bytes("MemTotal: lots kB\n"), None);
        assert_eq!(parse_meminfo_total_bytes("MemTotal: 10 MB\n"), None);
        assert_eq!(parse_meminfo_total_bytes("MemTotal: 12 furlongs\n"), None);
        assert_eq!(parse_meminfo_total_bytes(""), None);
        // The real shape, and a key that merely starts similarly is not it.
        assert_eq!(
            parse_meminfo_total_bytes("MemTotal:       32137720 kB\nMemFree:         1234567 kB\n"),
            Some(32_909_025_280)
        );
        assert_eq!(
            parse_meminfo_total_bytes("MemAvailable:    100 kB\nMemTotal:       1024 kB\n"),
            Some(1024 * 1024)
        );
    }

    #[test]
    fn cgroup_limit_treats_unlimited_as_none() {
        assert_eq!(parse_cgroup_memory_limit("8589934592\n"), Some(8 * GIB));
        assert_eq!(parse_cgroup_memory_limit("8589934592"), Some(8 * GIB));
        assert_eq!(parse_cgroup_memory_limit("max\n"), None);
        assert_eq!(parse_cgroup_memory_limit("9223372036854771712\n"), None);
        assert_eq!(parse_cgroup_memory_limit("0\n"), None);
        assert_eq!(parse_cgroup_memory_limit(""), None);
        assert_eq!(parse_cgroup_memory_limit("eight gigs"), None);
    }

    /// The effective total is `MemTotal` capped by a finite cgroup limit, and
    /// either one alone when the other is unknown.
    ///
    /// Distinguishing: returning `total` instead of `total.min(limit)` reads
    /// 32,212,254,720 where 17,179,869,184 is pinned.
    #[test]
    fn effective_memory_total_is_mem_total_capped_by_the_cgroup_limit() {
        assert_eq!(
            effective_memory_total(Some(32_212_254_720), Some(17_179_869_184)),
            Some(17_179_869_184)
        );
        // A limit above MemTotal does not raise the total.
        assert_eq!(
            effective_memory_total(Some(32_212_254_720), Some(64_424_509_440)),
            Some(32_212_254_720)
        );
        assert_eq!(
            effective_memory_total(Some(32_212_254_720), None),
            Some(32_212_254_720)
        );
        assert_eq!(
            effective_memory_total(None, Some(17_179_869_184)),
            Some(17_179_869_184)
        );
        assert_eq!(effective_memory_total(None, None), None);
    }

    #[test]
    fn sysctl_memsize_parses_a_byte_count() {
        assert_eq!(parse_sysctl_memsize("17179869184\n"), Some(16 * GIB));
        assert_eq!(parse_sysctl_memsize("0\n"), None);
        assert_eq!(parse_sysctl_memsize(""), None);
    }

    /// Every executor and CI host this runs on is Linux with a readable
    /// `/proc/meminfo`, so detection returns a plausible non-zero figure.
    #[cfg(target_os = "linux")]
    #[test]
    fn host_memory_is_detected_on_linux() {
        let bytes = detect_host_memory_total_bytes().expect("MemTotal readable on Linux");
        assert!(bytes >= 256 * MIB, "detected {bytes} bytes");
    }
}
