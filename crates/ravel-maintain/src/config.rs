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
/// Two combined high-water marks are kept:
///
/// - [`Self::peak_transient_bytes`]: `fetched + decoded`, the merge's *own*
///   decode-side buffers. This is the quantity these merges bound: at most
///   one raw block plus one decoded block per input, so it is
///   `O(input_count * block_size)` and does NOT scale with stream/trace size.
/// - [`Self::peak_total_bytes`]: `fetched + decoded + writer`, adding the
///   in-progress part's writer buffer. The writer term tracks the memory split
///   target `l1_part_memory_target_bytes` (a part is flushed once its
///   record-heap estimate reaches that target) and is the unavoidable
///   content-addressing cost the ADR calls out: a part's key does not exist
///   until the whole part is buffered. It is a target, not a ceiling: the RLOG
///   merge checks it after every record, so it overshoots by at most one
///   record, while the RSPAN merge checks it only at a trace boundary and
///   overshoots by up to one whole trace
///   ([`CompactorConfig::l1_part_memory_target_bytes`]). The RLOG merge may
///   also close a part earlier on the stored-size target `max_l1_part_bytes`
///   (issue #872), which only lowers this term.
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
///   bytes of every part already PUT but still held until publish. This is the
///   term the issue existed to expose; it was invisible before.
/// - finish and publish ([`Self::set_publish_record_bytes`]): the encoded
///   compaction-record payload, the only allocation the publish phase adds on
///   top of the retained parts.
///
/// Two fields of different byte kinds (encoded vs decoded heap) must never be
/// summed (the repo measurement rule). The tracker is one PER COMPACTION RUN:
/// the retained-parts and catalog terms accumulate and are not released within
/// a run, so reusing one tracker across buckets would sum their peaks.
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
    /// Decoded-record bytes currently resident across all merge cursors.
    decoded: AtomicU64,
    /// The in-progress part's accumulated record-byte estimate in the writer.
    writer: AtomicU64,
    /// High-water of `fetched + decoded`.
    peak_transient: AtomicU64,
    /// High-water of `fetched + decoded + writer`.
    peak_total: AtomicU64,
    /// Parts closed because the decoded record-heap estimate reached
    /// `l1_part_memory_target_bytes` (the memory split target fired).
    memory_target_flushes: AtomicU64,
    /// Parts closed because the encoded-bytes estimate reached
    /// `max_l1_part_bytes` (the stored-size target fired).
    stored_target_flushes: AtomicU64,
    /// Live sum of the encoded/on-object bytes of closed parts still retained in
    /// [`crate::rlog::PartSink::parts`] (PUT already, not yet dropped).
    /// Monotonic within a run: parts are held until publish, so this never
    /// decrements before the run ends.
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
    /// Retained closed parts: high-water of the encoded bytes of parts already
    /// PUT but still held in [`crate::rlog::PartSink::parts`] until publish.
    /// Encoded/on-object bytes. The term issue #977 made visible.
    pub retained_part_encoded_bytes: u64,
    /// Finish and publish: the published compaction record's encoded protobuf
    /// payload, the only allocation the publish phase adds on top of the
    /// retained parts. Encoded bytes.
    pub publish_record_encoded_bytes: u64,
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
    /// `decoded` bytes of records take their place. Called after the decoded
    /// records exist but before the raw buffer is released, so the high-water
    /// captures the instant both are resident.
    pub fn block_decoded(&self, raw: u64, decoded: u64) {
        self.inner.decoded.fetch_add(decoded, Ordering::Relaxed);
        self.note();
        self.inner.fetched.fetch_sub(raw, Ordering::Relaxed);
    }

    /// Account a decoded block leaving a cursor (its records were drained into
    /// the writer and the block's buffer is dropped).
    pub fn block_released(&self, decoded: u64) {
        self.inner.decoded.fetch_sub(decoded, Ordering::Relaxed);
    }

    /// Set the in-progress part's writer buffer estimate to `bytes`. Passing 0
    /// on flush records that the part's buffer was handed off and released.
    pub fn set_writer_bytes(&self, bytes: u64) {
        self.inner.writer.store(bytes, Ordering::Relaxed);
        self.note();
    }

    /// Recompute both high-water marks from the live counters.
    fn note(&self) {
        let fetched = self.inner.fetched.load(Ordering::Relaxed);
        let decoded = self.inner.decoded.load(Ordering::Relaxed);
        let writer = self.inner.writer.load(Ordering::Relaxed);
        let transient = fetched.saturating_add(decoded);
        let total = transient.saturating_add(writer);
        self.inner
            .peak_transient
            .fetch_max(transient, Ordering::Relaxed);
        self.inner.peak_total.fetch_max(total, Ordering::Relaxed);
        self.inner.peak_writer.fetch_max(writer, Ordering::Relaxed);
    }

    /// Account `bytes` of encoded part bytes retained in
    /// [`crate::rlog::PartSink::parts`] when a closed part is pushed there. The
    /// part was already PUT; it stays resident until publish, so this is never
    /// released within a run and its high-water is the retained-parts plateau a
    /// large-bucket compaction sits at. Encoded/on-object bytes, not heap; it is
    /// deliberately a separate term from the writer's decoded-heap bytes so a
    /// report never folds the two together.
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

    /// High-water of the merge's decode-side buffers (`fetched + decoded`).
    /// Bounded by `O(input_count * block_size)`, independent of stream size.
    pub fn peak_transient_bytes(&self) -> u64 {
        self.inner.peak_transient.load(Ordering::Relaxed)
    }

    /// High-water of the merge's total residency
    /// (`fetched + decoded + writer`), the decode-side buffers plus the
    /// in-progress part's writer buffer.
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

    /// The full phase split, each term naming its byte kind. See
    /// [`MergePhasePeaks`]; do not sum fields of different kinds.
    pub fn phase_peaks(&self) -> MergePhasePeaks {
        MergePhasePeaks {
            catalog_directory_decoded_bytes: self.peak_catalog_directory_bytes(),
            cursor_bytes: self.peak_transient_bytes(),
            writer_heap_bytes: self.peak_writer_bytes(),
            retained_part_encoded_bytes: self.peak_retained_part_bytes(),
            publish_record_encoded_bytes: self.peak_publish_record_bytes(),
        }
    }

    /// Record that a part was closed by the memory split target (its decoded
    /// record-heap estimate reached `l1_part_memory_target_bytes`).
    pub fn note_memory_target_flush(&self) {
        self.inner
            .memory_target_flushes
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record that a part was closed by the stored-size target (its
    /// encoded-bytes estimate reached `max_l1_part_bytes`).
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
/// in `build.rs` and per record plus per stream in the RLOG merge. It is
/// deliberately equal to the memory-target default so this crate's geometry is
/// unchanged from when one knob did both jobs: on a wide schema the memory
/// target reaches 256 MiB of decoded record heap long before a part's stored
/// bytes reach 256 MiB, so the memory target stays binding and the stored
/// target does not fire. Lowering it to `8..=16 MiB` is the follow-up that
/// grows objects (issue #872); see [`CompactorConfig::max_l1_part_bytes`].
pub const DEFAULT_MAX_L1_PART_BYTES: u64 = 256 * 1024 * 1024;
/// Default `l1_part_memory_target_bytes`: 256 MiB. This is the **memory split
/// target**: the decoded record-heap estimate the in-progress part is closed
/// at, which issue #711 added to keep compactor peak memory survivable on an
/// 8 GB host (a bucket carrying one wide stream once held 45.7 GB resident
/// under a nominal 256 MiB cap that was measured in stored, not heap, bytes).
/// It is a split target, not a ceiling on resident bytes; see
/// [`CompactorConfig::l1_part_memory_target_bytes`] for what each path
/// overshoots it by. Equal to [`DEFAULT_MAX_L1_PART_BYTES`] on purpose: the two
/// jobs were one knob before this split and their shared default reproduces the
/// old behaviour exactly.
pub const DEFAULT_L1_PART_MEMORY_TARGET_BYTES: u64 = 256 * 1024 * 1024;
/// Default minimum L0 records for a bucket to be worth compacting.
pub const DEFAULT_MIN_COMPACTION_INPUTS: usize = 2;
/// Default footer suffix-probe size. 64 KiB covers the footer + catalog of a
/// typical L0 flush in one GET (docs/segment-format.md reader protocol).
pub const DEFAULT_FOOTER_PROBE_BYTES: u64 = 64 * 1024;
/// Default `input_read_concurrency`: 8 input reads in flight at once.
pub const DEFAULT_INPUT_READ_CONCURRENCY: usize = 8;

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

/// Default `audit_retention_window_ns`: 90 days. The
/// dedicated retention window for query-audit records on
/// [`crate::query_audit::QUERY_AUDIT_SHARD`], independent of the ADR-0019
/// per-tenant data-retention windows ([`RetentionConfig`]): query-audit is a
/// server-written activity log with its own fixed lifetime, not tenant data,
/// and it is not tombstone-gated through the resolver (no snapshot excludes it),
/// so it has its own age-based sweep rather than the bucket-tombstone flow.
pub const DEFAULT_AUDIT_RETENTION_NS: i64 = 90 * 24 * NS_PER_HOUR;

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
    /// encoded/on-object bytes reach this. On the RSEG path (`build.rs`) this
    /// is the part's stored-size estimate, which charges every section that
    /// grows with what the part carries: the TS/VAL/HIST pages (ADR-0092
    /// decision 3), each series' SERIES_IDS entry and SERIES_META cells, the
    /// per-sample provenance columns a run-merged run adds, each distinct
    /// LABEL_DICT string, and the EXEMPLARS records. On the
    /// RLOG path (`rlog.rs`) it is an encoded-bytes estimate summed per record
    /// ([`crate::rlog::estimate_stored_record`]) plus one STREAM_DIR entry per
    /// distinct stream in the part, because the RLOG writer holds
    /// row-major records and does not expose an incremental encoded size. Both
    /// estimates are pre-compression proxies over zstd-compressed sections, so
    /// they charge at or above what those sections store, which is the
    /// conservative direction. Named
    /// for the bytes it measures: it governs object geometry (object count and
    /// per-object stored size), which is what the historical `max_l1_part_bytes`
    /// name always implied. It does NOT bound memory; neither does
    /// [`Self::l1_part_memory_target_bytes`], which is a split target in decoded
    /// heap. A part closes on whichever of the two
    /// is reached first. Default [`DEFAULT_MAX_L1_PART_BYTES`] (256 MiB), chosen
    /// so today's geometry is unchanged (issue #872); lower it to grow objects.
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
    ///   that series' size and by nothing configurable), plus every finished
    ///   part's encoded bytes, which are retained until publish.
    ///
    /// Named for what it measures and what it does: a split target in decoded
    /// heap. It was `max_l1_part_memory_bytes`, which reads as a resident-bytes
    /// ceiling an operator can size a host from, and none of the three paths
    /// enforces one. A part closes on whichever of this and
    /// [`Self::max_l1_part_bytes`] is reached first. Default
    /// [`DEFAULT_L1_PART_MEMORY_TARGET_BYTES`] (256 MiB).
    pub l1_part_memory_target_bytes: u64,
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
    /// Slow safety-net re-verify cadence for the interior zone (ADR-0065
    /// decision 3, config `maintain_interior_reverify`). A terminal interior
    /// bucket is re-evaluated no later than this after its last verification,
    /// or sooner if its computed retention expiry arrives first
    /// ([`crate::scan::classify_zone`], [`crate::scan::MaintainMemo`]). Head
    /// and tail hours ignore this and are evaluated every tick. Default
    /// [`DEFAULT_INTERIOR_REVERIFY_NS`] (6 h); non-positive disables the
    /// safety net (every interior bucket is always due).
    pub interior_reverify_ns: i64,
}

impl Default for CompactorConfig {
    fn default() -> Self {
        CompactorConfig {
            max_flush_lifetime_ns: DEFAULT_MAX_FLUSH_LIFETIME_NS,
            clock_skew_allowance_ns: DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
            max_compaction_lifetime_ns: DEFAULT_MAX_COMPACTION_LIFETIME_NS,
            max_l1_part_bytes: DEFAULT_MAX_L1_PART_BYTES,
            l1_part_memory_target_bytes: DEFAULT_L1_PART_MEMORY_TARGET_BYTES,
            min_compaction_inputs: DEFAULT_MIN_COMPACTION_INPUTS,
            footer_probe_bytes: DEFAULT_FOOTER_PROBE_BYTES,
            input_read_concurrency: DEFAULT_INPUT_READ_CONCURRENCY,
            compactor_writer_id: Uuid::nil(),
            grace_ns: DEFAULT_GRACE_NS,
            protection_horizon_ns: DEFAULT_PROTECTION_HORIZON_NS,
            orphan_breaker_min_count: DEFAULT_ORPHAN_BREAKER_MIN_COUNT,
            orphan_breaker_max_ratio: DEFAULT_ORPHAN_BREAKER_MAX_RATIO,
            force_orphan_gc: false,
            idem_dedup_window_hours: DEFAULT_IDEM_DEDUP_WINDOW_HOURS,
            audit_retention_window_ns: DEFAULT_AUDIT_RETENTION_NS,
            dry_run: false,
            merge_memory_tracker: None,
            interior_reverify_ns: DEFAULT_INTERIOR_REVERIFY_NS,
        }
    }
}

impl CompactorConfig {
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
