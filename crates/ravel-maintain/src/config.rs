//! Compactor configuration: the seal margin, the trigger threshold, the part
//! size cap, and the abandonment deadline (docs/compaction-retention-plan.md
//! §3.2, §3.3, §3.4), plus the sweep/retention knobs (grace, protection
//! horizon, ADR-0019 per-tenant retention windows). All durations are
//! nanoseconds to match the injected [`crate::clock::Clock`].

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use ravel_types::{TenantHash, TenantId};
use uuid::Uuid;

/// A test-injectable accounting hook for the RLOG compaction merge's peak
/// resident memory (issue #745, ADR-0065 decision 4).
///
/// The RLOG k-way merge ([`crate::rlog`]) drives this at its real
/// allocation/decode points so a test can assert the merge's residency is
/// bounded independently of a stream's size. It is deliberately *load-bearing*,
/// not decorative: the merge calls [`Self::block_fetched`] when it fetches one
/// input block's raw bytes, [`Self::block_decoded`]/[`Self::block_released`] as
/// each decoded block enters and leaves a cursor, and [`Self::set_writer_bytes`]
/// as records accumulate in the in-progress part's writer. If the merge ever
/// regressed to decoding a whole stream at once, the `decoded` term would grow
/// with the stream and the recorded high-water would break the test's bound.
///
/// Two high-water marks are kept:
///
/// - [`Self::peak_transient_bytes`]: `fetched + decoded`, the merge's *own*
///   decode-side buffers. This is the quantity issue #745 bounds: at most one
///   raw block plus one decoded block per input carrying the current stream, so
///   it is `O(input_count * block_size)` and does NOT scale with stream size.
/// - [`Self::peak_total_bytes`]: `fetched + decoded + writer`, adding the
///   in-progress part's writer buffer. The writer term is bounded by
///   `max_l1_part_bytes` (a part is flushed once its record-byte estimate
///   reaches the cap on a stream boundary) and is the unavoidable
///   content-addressing cost the ADR calls out: a part's key does not exist
///   until the whole part is buffered.
///
/// Production never installs one (`CompactorConfig::merge_memory_tracker` is
/// `None`), so the hooks compile to a single `Option` check and add nothing.
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
}

/// Nanoseconds in one hour; an ingest-hour bucket spans exactly this.
pub const NS_PER_HOUR: i64 = 3_600_000_000_000;

/// Default `max_flush_lifetime`: 1 hour (matches ravel-ingest and
/// ravel-catalog, ADR-0010 §11).
pub const DEFAULT_MAX_FLUSH_LIFETIME_NS: i64 = NS_PER_HOUR;
/// Default `clock_skew_allowance`: 5 minutes (matches ravel-catalog).
pub const DEFAULT_CLOCK_SKEW_ALLOWANCE_NS: i64 = 300_000_000_000;
/// Default `max_compaction_lifetime`: 1 hour (plan §3.4 point 4). Mirrors the
/// writer interlock so the sweeper's unreferenced-part rule is safe.
pub const DEFAULT_MAX_COMPACTION_LIFETIME_NS: i64 = NS_PER_HOUR;
/// Default `max_l1_part_bytes`: 256 MiB (plan §3.3 point 4).
pub const DEFAULT_MAX_L1_PART_BYTES: u64 = 256 * 1024 * 1024;
/// Default minimum L0 records for a bucket to be worth compacting (plan §3.2).
pub const DEFAULT_MIN_COMPACTION_INPUTS: usize = 2;
/// Default footer suffix-probe size. 64 KiB covers the footer + catalog of a
/// typical L0 flush in one GET (docs/segment-format.md reader protocol).
pub const DEFAULT_FOOTER_PROBE_BYTES: u64 = 64 * 1024;

/// Default `grace`: 24 hours (docs/consistency-model.md "Deletion and GC",
/// plan §5). A shared floor for the orphan and unreferenced-part age gates.
pub const DEFAULT_GRACE_NS: i64 = 24 * NS_PER_HOUR;
/// Default `max_query_duration`: 1 hour. The horizon must outlast any pinned
/// in-flight query (`protection_horizon >= max_query_duration + grace`, plan
/// §5 / docs/consistency-model.md), so this is the query-duration term of the
/// default `protection_horizon`.
pub const DEFAULT_MAX_QUERY_DURATION_NS: i64 = NS_PER_HOUR;
/// Default `protection_horizon`: `max_query_duration + grace` (plan §5). The
/// supersession and retention sweeps gate physical deletion on
/// `now >= anchor + protection_horizon`, so a query resolved just before the
/// anchor still has this long to finish reading the inputs it pinned.
pub const DEFAULT_PROTECTION_HORIZON_NS: i64 = DEFAULT_MAX_QUERY_DURATION_NS + DEFAULT_GRACE_NS;
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

/// Default `idem_dedup_window_hours` (ADR-0051 §5): this crate's own policy
/// default, chosen to match the 24h dedup window ADR-0051 documents.
/// `ravel_ingest::idempotency::read_marker` has no default of its own --
/// `dedup_window_hours` is always caller-supplied -- so there is no shared
/// code-level default to match; what actually keeps the sweep from reaping a
/// marker the read path would still honor is
/// `ravel_ingest::IDEM_MARKER_FORWARD_SKEW_TOLERANCE_HOURS`, subtracted from
/// the sweep's own `min_hour` calculation (`crate::sweep`).
pub const DEFAULT_IDEM_DEDUP_WINDOW_HOURS: u32 = 24;

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
    /// plus this plus the skew allowance (plan §3.2).
    pub max_flush_lifetime_ns: i64,
    /// Extra seal margin for cross-host clock skew (plan §3.2).
    pub clock_skew_allowance_ns: i64,
    /// Deadline after which a compaction run must not publish its record
    /// (plan §3.4 point 4); measured from the run's start via the clock.
    pub max_compaction_lifetime_ns: i64,
    /// Split parts on series boundaries once accumulated verbatim page bytes
    /// reach this (plan §3.3 point 4).
    pub max_l1_part_bytes: u64,
    /// Buckets with fewer L0 records than this are left uncompacted; set 1 for
    /// v1-retirement campaigns (plan §3.2).
    pub min_compaction_inputs: usize,
    /// Suffix-probe size for the first footer GET of each input.
    pub footer_probe_bytes: u64,
    /// This compactor process's uuid. Informational only: it is recorded in
    /// each part's footer `writer_id` and never enters dedup priority
    /// (plan §4). Default is the nil uuid; the service sets a real one.
    pub compactor_writer_id: Uuid,
    /// Shared grace period for the orphan and unreferenced-part age gates
    /// (plan §5, docs/consistency-model.md "Deletion and GC"). An object is
    /// only ever a deletion candidate once its `last_modified` age exceeds
    /// this plus the relevant lifetime bound. Default
    /// [`DEFAULT_GRACE_NS`] (24 h).
    pub grace_ns: i64,
    /// Horizon between a deletion anchor (a compaction record's
    /// `created_unix_ns`, a tombstone's `retired_at_ns`) and physical
    /// deletion (plan §5). Must satisfy `>= max_query_duration + grace` so a
    /// query resolved just before the anchor still has time to read the
    /// inputs it pinned. Default [`DEFAULT_PROTECTION_HORIZON_NS`] (25 h).
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
    /// Dry-run switch (plan §8, P8). When `true`, every maintenance path
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
    /// Optional test-injectable accounting hook for the RLOG compaction merge's
    /// peak resident memory (issue #745). `None` in production (the merge's
    /// accounting hooks are skipped); a test installs one and reads its
    /// high-water marks after `compact_bucket` to assert the k-way merge stayed
    /// bounded independently of stream size. Carried in the config, like every
    /// other merge knob, so `..CompactorConfig::default()` call sites are
    /// unaffected. Default `None`.
    pub merge_memory_tracker: Option<MergeMemoryTracker>,
}

impl Default for CompactorConfig {
    fn default() -> Self {
        CompactorConfig {
            max_flush_lifetime_ns: DEFAULT_MAX_FLUSH_LIFETIME_NS,
            clock_skew_allowance_ns: DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
            max_compaction_lifetime_ns: DEFAULT_MAX_COMPACTION_LIFETIME_NS,
            max_l1_part_bytes: DEFAULT_MAX_L1_PART_BYTES,
            min_compaction_inputs: DEFAULT_MIN_COMPACTION_INPUTS,
            footer_probe_bytes: DEFAULT_FOOTER_PROBE_BYTES,
            compactor_writer_id: Uuid::nil(),
            grace_ns: DEFAULT_GRACE_NS,
            protection_horizon_ns: DEFAULT_PROTECTION_HORIZON_NS,
            orphan_breaker_min_count: DEFAULT_ORPHAN_BREAKER_MIN_COUNT,
            orphan_breaker_max_ratio: DEFAULT_ORPHAN_BREAKER_MAX_RATIO,
            force_orphan_gc: false,
            idem_dedup_window_hours: DEFAULT_IDEM_DEDUP_WINDOW_HOURS,
            dry_run: false,
            merge_memory_tracker: None,
        }
    }
}

impl CompactorConfig {
    /// The seal margin: a bucket ending at `bucket_end_ns` is sealed once
    /// `now_ns >= bucket_end_ns + this` (plan §3.2). No new commit record can
    /// appear in the bucket after that, so a single strongly consistent LIST
    /// is a complete, repeatable input set.
    pub fn seal_margin_ns(&self) -> i64 {
        self.max_flush_lifetime_ns
            .saturating_add(self.clock_skew_allowance_ns)
    }

    /// The orphan-GC age gate: an `l0/` data object with no commit record is a
    /// deletion candidate only once its `last_modified` age exceeds this
    /// (`grace + max_flush_lifetime`, plan §5). The `max_flush_lifetime` term
    /// is what makes the writer interlock hold: a writer abandons any flush
    /// older than that and never publishes it, so a record-less object older
    /// than this can never gain a commit record later (ADR-0010 §11).
    pub fn orphan_age_gate_ns(&self) -> i64 {
        self.grace_ns.saturating_add(self.max_flush_lifetime_ns)
    }

    /// The unreferenced-part age gate: an `l1/` object referenced by no
    /// compaction record in its bucket is a deletion candidate only once its
    /// `last_modified` age exceeds this (`grace + max_compaction_lifetime`,
    /// plan §5). The `max_compaction_lifetime` term mirrors the abandonment
    /// deadline (plan §3.4 point 4): a compactor past that deadline never
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
/// (plan §6, ADR-0019 §5): `retention: { default: none, tenants: { <id>: R } }`.
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

/// The validated per-tenant retention configuration (plan §6, ADR-0019).
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
    /// every tenant id (plan §6: "Tenant ids in config, hashed at load;
    /// config never stores tenant hashes" -- i.e. never the raw id). Rejects
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
