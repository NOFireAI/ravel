//! Compactor configuration: the seal margin, the trigger threshold, the part
//! size cap, and the abandonment deadline (docs/compaction-retention-plan.md
//! §3.2, §3.3, §3.4), plus the sweep/retention knobs (grace, protection
//! horizon, ADR-0019 per-tenant retention windows). All durations are
//! nanoseconds to match the injected [`crate::clock::Clock`].

use std::collections::HashMap;

use ravel_types::{TenantHash, TenantId};
use uuid::Uuid;

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
