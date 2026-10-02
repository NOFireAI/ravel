//! The durable, deployment-wide GC configuration object `sys/gc` (ADR-0050
//! section 4).
//!
//! `protection_horizon >= max_query_duration + grace + clock_skew_allowance`
//! protects every pinned reader from the GC sweeper, including one whose clock
//! leads the reader's by up to `clock_skew_allowance` (without the skew term a
//! sweeper skewed ahead reaches its deletion threshold
//! `now >= anchor + protection_horizon` in true time before a reader's
//! still-active snapshot, held up to `max_query_duration`, is released).
//! Before this object the bound lived in three unlinked per-process configs
//! (the maintain sweep config, the query deadline, the Flight ticket ceiling)
//! that could be deployed independently, with nothing validating the
//! constraint anywhere. This module makes the deployment-wide values a
//! single durable truth:
//!
//! - **Bootstrap.** On the first touch of a fresh bucket, [`bootstrap_gc_config`]
//!   writes `sys/gc` via `CreateIfAbsent` from the maintain defaults
//!   ([`GcConfigValues::maintain_defaults`], which satisfy the constraint by
//!   construction). It never refuses to start because the object is merely
//!   absent: an absent object on a fresh bucket is bootstrapped, not a fault
//!   (the fail-open-avoidance lesson). A racing loser (`AlreadyExists`)
//!   re-reads and returns the winner's object, the same race-loser pattern
//!   `write_marker`, `resolve_and_pin`, and the provisioning record already
//!   use, so two processes bootstrapping one fresh bucket both start.
//! - **Mutation.** Only [`set_gc_config`] (behind `ravel-cli gc-config set`)
//!   changes it. It enforces the constraint at write time and swaps with
//!   `CasVersion`, so a concurrent mutation is caught, never silently
//!   overwritten.
//! - **Startup validation, per mode.** Every mode reads the (now durable)
//!   object and validates itself against it, refusing to start on a real
//!   violation with a typed [`GcConfigError`]: maintain's horizon and grace
//!   must equal the stored values ([`validate_maintain`]); a query engine's
//!   deadline must be `<=` `max_query_duration_ns` ([`validate_query_deadline`])
//!   and its HEAD cache TTL `<=` `head_cache_ttl_ns`
//!   ([`validate_query_head_cache_ttl`]); a Flight ticket-TTL ceiling must be
//!   `<=` `protection_horizon_ns - grace_ns` ([`validate_flight_ceiling`]).
//! - **Versions.** Format version 1 records no HEAD cache TTL and decodes to
//!   the compiled [`DEFAULT_HEAD_CACHE_TTL_NS`]; version 2 records one
//!   (ADR-1133 decision 4). Bootstrap writes version 1, so a new build touching
//!   a fresh bucket first does not lock older builds out; only
//!   `ravel-cli gc-config set --head-cache-ttl` flips a version 1 object to
//!   version 2 (a `set` over a stored version 2 writes version 2 again), after
//!   which a build that reads only version 1 refuses the object.
//!
//! The constraint is thereby enforced at exactly two choke points: the single
//! mutation path (the CLI, at write time) and each process's startup (against
//! the single durable truth). A process that can read a bootstrapped `sys/gc`
//! and finds a real violation does not start; there is no "assume defaults"
//! path.

use prost::Message;
use ravel_catalog::DEFAULT_HEAD_CACHE_TTL_NS;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutMode, PutOptions, StoreError, Version};
use ravel_proto::sys::v1 as sysproto;

use crate::config::{
    DEFAULT_GRACE_NS, DEFAULT_MAX_COMPACTION_LIFETIME_NS, DEFAULT_MAX_FLUSH_LIFETIME_NS,
    DEFAULT_MAX_QUERY_DURATION_NS, DEFAULT_PROTECTION_HORIZON_NS,
};

/// The bucket-root GC-config key (ADR-0050 section 4). Fixed, deployment-wide,
/// never under a tenant prefix.
pub const GC_CONFIG_KEY: &str = "sys/gc";

/// The ingest pipeline's own compiled-in `max_flush_lifetime` (ravel-ingest's
/// writer interlock, ADR-0010 §11/§1), read fresh from
/// [`ravel_ingest::IngestConfig::default`] rather than duplicated as a
/// constant. A writer abandons any flush older than this and never publishes
/// it afterward; that is what makes [`Bucket::is_sealed`](crate::Bucket::is_sealed)
/// safe to treat as "nothing more will ever be published here" once
/// `now_ns >= end_ns + seal_margin_ns`. A compactor `max_flush_lifetime_ns`
/// configured BELOW this floor can seal, and this crate's erasure completion
/// gate ([`crate::bucket_erasure_completion`]) can then report a bucket's
/// pending erasure request complete, before the real writer's flush window
/// has elapsed: a record from that still-in-flight flush can land afterward
/// and resurface in every later snapshot for a subject already marked erased.
/// This is the single point every validator of that bound calls, so "the same
/// floor" is one function, not two independently copied expressions.
pub fn ingest_max_flush_lifetime_floor_ns() -> i64 {
    i64::try_from(
        ravel_ingest::IngestConfig::default()
            .max_flush_lifetime
            .as_nanos(),
    )
    .unwrap_or(i64::MAX)
}

/// The highest `sys/gc` format version this build reads, and the version a
/// `gc-config set` that records a HEAD cache TTL writes. A higher version is
/// refused rather than misread.
pub const GC_FORMAT_VERSION: u32 = 2;

/// The format version bootstrap writes, and the one that records no HEAD cache
/// TTL (ADR-1133 decision 4).
pub const GC_FORMAT_VERSION_V1: u32 = 1;

/// The deployment-wide GC values recorded in `sys/gc`, decoded into plain
/// integers so callers (server startup, the CLI, tests) never touch the proto
/// type directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GcConfigValues {
    /// The format version these values were read at, or are written at: 1 or
    /// [`GC_FORMAT_VERSION`].
    pub format_version: u32,
    /// Horizon between a deletion anchor and physical deletion. Must satisfy
    /// `>= max_query_duration_ns + grace_ns + clock_skew_allowance_ns` (the
    /// skew term is not stored here; it is supplied by the writer's config at
    /// the single mutation choke point, see [`satisfies_constraint`] and
    /// [`set_gc_config`]).
    ///
    /// [`satisfies_constraint`]: GcConfigValues::satisfies_constraint
    pub protection_horizon_ns: i64,
    /// Shared grace period for the orphan and unreferenced-part age gates.
    pub grace_ns: i64,
    /// The longest a single query may run; the query-duration term of the
    /// protection-horizon constraint.
    pub max_query_duration_ns: i64,
    /// The longest a flush may stay open.
    pub max_flush_lifetime_ns: i64,
    /// The HEAD cache TTL every query-mode server process is held to
    /// ([`validate_query_head_cache_ttl`]). Recorded from format version 2; a
    /// version 1 object decodes it as [`DEFAULT_HEAD_CACHE_TTL_NS`].
    pub head_cache_ttl_ns: i64,
}

impl GcConfigValues {
    /// The maintain defaults, which satisfy the constraint by construction
    /// (`protection_horizon = max_query_duration + grace + clock_skew_allowance`).
    /// This is what the first process to touch a fresh bucket bootstraps
    /// `sys/gc` from (ADR-0050 section 4), and it matches
    /// [`crate::CompactorConfig::default`]'s horizon, grace, and flush lifetime.
    /// It is a version 1 object carrying the compiled HEAD cache TTL.
    pub fn maintain_defaults() -> Self {
        GcConfigValues {
            format_version: GC_FORMAT_VERSION_V1,
            protection_horizon_ns: DEFAULT_PROTECTION_HORIZON_NS,
            grace_ns: DEFAULT_GRACE_NS,
            max_query_duration_ns: DEFAULT_MAX_QUERY_DURATION_NS,
            max_flush_lifetime_ns: DEFAULT_MAX_FLUSH_LIFETIME_NS,
            head_cache_ttl_ns: DEFAULT_HEAD_CACHE_TTL_NS,
        }
    }

    /// Whether these values satisfy the GC safety constraint
    /// `protection_horizon >= max_query_duration + grace + clock_skew_allowance`.
    /// The `clock_skew_allowance_ns` term is supplied by the caller
    /// (the sweeper/writer config, not stored in `sys/gc`) and closes the gap a
    /// sweeper whose clock leads a reader's would otherwise open: it reaches
    /// `now >= anchor + protection_horizon` in true time up to
    /// `clock_skew_allowance` early, so the horizon must budget for it. Saturating
    /// so an absurd (near-`i64::MAX`) input cannot wrap the comparison.
    pub fn satisfies_constraint(&self, clock_skew_allowance_ns: i64) -> bool {
        self.protection_horizon_ns
            >= self
                .max_query_duration_ns
                .saturating_add(self.grace_ns)
                .saturating_add(clock_skew_allowance_ns)
    }

    /// Whether the horizon outlasts every compaction or rewrite run that could
    /// still publish over a record's inputs:
    /// `protection_horizon >= max_compaction_lifetime + 4 * clock_skew_allowance`
    /// (ADR-1133, gated-set stability amendment). A run that overlaps record R
    /// listed the bucket before R was visible, so it started before R's
    /// `created_unix_ns` in true time, and it publishes within
    /// `max_compaction_lifetime` of its own start on its own clock (`2σ` of true
    /// time on top). The sweeper's horizon check compares its clock against R's
    /// `created_unix_ns` (another `2σ`). Saturating like
    /// [`Self::satisfies_constraint`].
    pub fn satisfies_compaction_lifetime(
        &self,
        max_compaction_lifetime_ns: i64,
        clock_skew_allowance_ns: i64,
    ) -> bool {
        self.protection_horizon_ns
            >= max_compaction_lifetime_ns.saturating_add(clock_skew_allowance_ns.saturating_mul(4))
    }

    /// Reject any non-positive field before a write. Every `sys/gc` value is a
    /// duration bound and must be strictly positive: a zero or negative value
    /// is never meaningful, and (the data-loss bug this closes) an all-zero
    /// proposal `0,0,0,0` trivially satisfies the horizon constraint
    /// (`0 >= 0 + 0`), so without this floor it would be accepted and written.
    /// Once written, no valid `sys/gc` can ever satisfy the constraint with a
    /// value below `0` (zero is already the floor), so every mode's startup
    /// validation would fail forever with no recovery path: the deployment is
    /// permanently bricked. Enforced at the single mutation choke point
    /// ([`set_gc_config`]), so a durable object can never hold a non-positive
    /// field.
    ///
    /// Also refuses a `max_flush_lifetime_ns` below the caller-supplied
    /// `ingest_max_flush_lifetime_ns` floor (issue #1744), kept here as
    /// defence in depth even though nothing reads this field back into a live
    /// `CompactorConfig` today; see [`ingest_max_flush_lifetime_floor_ns`] for
    /// why the floor exists.
    pub fn validate(&self, ingest_max_flush_lifetime_ns: i64) -> Result<(), GcConfigError> {
        for (field, got) in [
            ("protection_horizon_ns", self.protection_horizon_ns),
            ("grace_ns", self.grace_ns),
            ("max_query_duration_ns", self.max_query_duration_ns),
            ("max_flush_lifetime_ns", self.max_flush_lifetime_ns),
            ("head_cache_ttl_ns", self.head_cache_ttl_ns),
        ] {
            if got <= 0 {
                return Err(GcConfigError::NonPositiveValue { field, got });
            }
        }
        if self.max_flush_lifetime_ns < ingest_max_flush_lifetime_ns {
            return Err(GcConfigError::MaxFlushLifetimeBelowIngestFloor {
                got: self.max_flush_lifetime_ns,
                floor: ingest_max_flush_lifetime_ns,
            });
        }
        Ok(())
    }

    /// The upper bound a Flight SQL ticket TTL may reach: `protection_horizon -
    /// grace`. A ticket that outlives this could redeem against a snapshot the
    /// GC sweeper has already collected. Saturating at zero.
    pub fn flight_ceiling_ns(&self) -> i64 {
        self.protection_horizon_ns
            .saturating_sub(self.grace_ns)
            .max(0)
    }

    /// The proto these values encode to. Version 1 leaves `head_cache_ttl_ns`
    /// absent, since a version 1 reader takes the compiled default whatever the
    /// field holds.
    fn to_proto(self, now_ns: i64) -> sysproto::GcConfig {
        let head_cache_ttl_ns = if self.format_version == GC_FORMAT_VERSION_V1 {
            0
        } else {
            self.head_cache_ttl_ns
        };
        sysproto::GcConfig {
            format_version: self.format_version,
            protection_horizon_ns: self.protection_horizon_ns,
            grace_ns: self.grace_ns,
            max_query_duration_ns: self.max_query_duration_ns,
            max_flush_lifetime_ns: self.max_flush_lifetime_ns,
            created_unix_ns: now_ns,
            head_cache_ttl_ns,
        }
    }

    fn from_proto(proto: sysproto::GcConfig) -> Result<Self, GcConfigError> {
        let head_cache_ttl_ns = match proto.format_version {
            GC_FORMAT_VERSION_V1 => DEFAULT_HEAD_CACHE_TTL_NS,
            GC_FORMAT_VERSION => {
                if proto.head_cache_ttl_ns <= 0 {
                    return Err(GcConfigError::StoredHeadCacheTtlNotPositive {
                        got: proto.head_cache_ttl_ns,
                    });
                }
                proto.head_cache_ttl_ns
            }
            got => return Err(GcConfigError::UnsupportedVersion { got }),
        };
        Ok(GcConfigValues {
            format_version: proto.format_version,
            protection_horizon_ns: proto.protection_horizon_ns,
            grace_ns: proto.grace_ns,
            max_query_duration_ns: proto.max_query_duration_ns,
            max_flush_lifetime_ns: proto.max_flush_lifetime_ns,
            head_cache_ttl_ns,
        })
    }
}

/// A `gc-config set` proposal: the four durations it always replaces, and the
/// HEAD cache TTL it records only when given (ADR-1133 decision 4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GcConfigProposal {
    pub protection_horizon_ns: i64,
    pub grace_ns: i64,
    pub max_query_duration_ns: i64,
    pub max_flush_lifetime_ns: i64,
    /// `Some` writes format version 2 carrying this TTL. `None` keeps the
    /// stored format version and, on version 2, its recorded TTL; on a bucket
    /// with no object it writes version 1.
    pub head_cache_ttl_ns: Option<i64>,
}

impl GcConfigProposal {
    /// The values a set of this proposal writes over `current`, the object it
    /// read (`None` when there is none).
    fn resolve(self, current: Option<&GcConfigValues>) -> GcConfigValues {
        let (format_version, head_cache_ttl_ns) = match (self.head_cache_ttl_ns, current) {
            (Some(ttl), _) => (GC_FORMAT_VERSION, ttl),
            (None, Some(current)) => (current.format_version, current.head_cache_ttl_ns),
            (None, None) => (GC_FORMAT_VERSION_V1, DEFAULT_HEAD_CACHE_TTL_NS),
        };
        GcConfigValues {
            format_version,
            protection_horizon_ns: self.protection_horizon_ns,
            grace_ns: self.grace_ns,
            max_query_duration_ns: self.max_query_duration_ns,
            max_flush_lifetime_ns: self.max_flush_lifetime_ns,
            head_cache_ttl_ns,
        }
    }
}

impl From<GcConfigValues> for GcConfigProposal {
    /// Re-propose `values`: a version 2 object's TTL is recorded again, and a
    /// version 1 object's is left to the stored version.
    fn from(values: GcConfigValues) -> Self {
        GcConfigProposal {
            protection_horizon_ns: values.protection_horizon_ns,
            grace_ns: values.grace_ns,
            max_query_duration_ns: values.max_query_duration_ns,
            max_flush_lifetime_ns: values.max_flush_lifetime_ns,
            head_cache_ttl_ns: (values.format_version != GC_FORMAT_VERSION_V1)
                .then_some(values.head_cache_ttl_ns),
        }
    }
}

/// A typed `sys/gc` failure. Every startup-validation variant refuses to start;
/// none warn and continue (ADR-0050's single fail-closed rule). Every variant
/// names the exact values so an operator sees what disagreed.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum GcConfigError {
    #[error("object store error accessing sys/gc: {0}")]
    Store(String),
    /// The store refused this process's credential on the `sys/gc` GET
    /// ([`GcAccessOp::Read`], from [`read_gc_config`], including the read
    /// [`set_gc_config`] issues first) or on [`bootstrap_gc_config`]'s create
    /// ([`GcAccessOp::Create`]). Kept apart from [`GcConfigError::Store`]
    /// because it is a credential grant problem, not a broken store. A refused
    /// create from a per-role Gateway or Query credential means start order
    /// (only Maintain and Admin may create `sys/gc`, ADR-0055 section 4); a
    /// refused read, or a refused create under a credential that should hold
    /// the grant, means the credential lacks it. [`set_gc_config`]'s own
    /// refused PUT still surfaces as [`GcConfigError::Store`].
    #[error("access to sys/gc was refused on {op}: {detail}")]
    AccessDenied { op: GcAccessOp, detail: String },
    #[error("sys/gc is corrupt and could not be decoded: {0}")]
    Decode(String),
    #[error(
        "sys/gc declares format_version {got}, but this build only understands versions \
         {GC_FORMAT_VERSION_V1} to {GC_FORMAT_VERSION}: refusing rather than misread an \
         unknown GC-config format"
    )]
    UnsupportedVersion { got: u32 },
    #[error(
        "sys/gc is format_version {GC_FORMAT_VERSION} but records head_cache_ttl_ns={got}: a \
         version {GC_FORMAT_VERSION} object must record a positive HEAD cache TTL; refusing \
         rather than hold query processes to a meaningless bound"
    )]
    StoredHeadCacheTtlNotPositive { got: i64 },
    #[error(
        "sys/gc was absent then present within one bootstrap, but could not be re-read: a \
         concurrent bootstrap left the object unreadable"
    )]
    ObjectVanished,
    #[error(
        "proposed GC config has a non-positive {field}={got} ns: every sys/gc value is a \
         duration bound and must be strictly positive. A zero or negative value is not a \
         meaningful GC bound, and an all-zero config trivially satisfies the horizon constraint \
         (0 >= 0 + 0) yet no valid config could ever go lower, so writing it would permanently \
         brick every mode's startup validation; refusing to write sys/gc"
    )]
    NonPositiveValue { field: &'static str, got: i64 },
    #[error(
        "proposed GC config has max_flush_lifetime_ns={got} ns, below the ingest pipeline's own \
         max_flush_lifetime floor of {floor} ns (ravel-ingest's fixed writer interlock, ADR-0010 \
         §11 -- there is no flag to change it): sys/gc is the durable, operator-facing record of \
         this deployment's intended GC values, and it must not hold one this process would refuse \
         to run with; refusing to write sys/gc"
    )]
    MaxFlushLifetimeBelowIngestFloor { got: i64, floor: i64 },
    #[error(
        "proposed GC config violates protection_horizon >= max_query_duration + grace + \
         clock_skew_allowance: protection_horizon={protection_horizon_ns} ns, \
         max_query_duration={max_query_duration_ns} ns, grace={grace_ns} ns, \
         clock_skew_allowance={clock_skew_allowance_ns} ns (need protection_horizon >= {}); \
         refusing to write sys/gc",
        .max_query_duration_ns.saturating_add(*.grace_ns).saturating_add(*.clock_skew_allowance_ns)
    )]
    ConstraintViolation {
        protection_horizon_ns: i64,
        max_query_duration_ns: i64,
        grace_ns: i64,
        clock_skew_allowance_ns: i64,
    },
    #[error(
        "a concurrent gc-config set changed sys/gc since this one read it (CasVersion \
         precondition failed): re-read and retry rather than overwrite the other change"
    )]
    CasConflict,
    #[error(
        "maintain is configured with protection_horizon={configured_horizon_ns} ns and \
         grace={configured_grace_ns} ns, but sys/gc records protection_horizon={stored_horizon_ns} ns \
         and grace={stored_grace_ns} ns: maintain's horizon and grace must EQUAL the durable values \
         (they are must-match, not independent knobs); refusing to start"
    )]
    MaintainMismatch {
        configured_horizon_ns: i64,
        configured_grace_ns: i64,
        stored_horizon_ns: i64,
        stored_grace_ns: i64,
    },
    #[error(
        "sys/gc records protection_horizon={stored_horizon_ns} ns, but THIS maintain process's \
         running sweeper is configured with clock_skew_allowance={clock_skew_allowance_ns} ns, and \
         the skew-covering GC bound requires protection_horizon >= max_query_duration + grace + \
         clock_skew_allowance = {} ns (stored max_query_duration={stored_max_query_duration_ns} ns, \
         stored grace={stored_grace_ns} ns): the durable horizon does not cover the skew of the \
         sweeper that actually deletes, so this sweeper could physically delete an object a live \
         reader still holds. This is a deployment error to fix -- either lower the running \
         sweeper's --clock-skew-allowance, or raise the durable horizon via `ravel-cli gc-config \
         set` -- refusing to enter the maintain sweep loop rather than delete a pinned snapshot",
        .stored_max_query_duration_ns.saturating_add(*.stored_grace_ns).saturating_add(*.clock_skew_allowance_ns)
    )]
    MaintainSkewUncovered {
        stored_horizon_ns: i64,
        stored_max_query_duration_ns: i64,
        stored_grace_ns: i64,
        clock_skew_allowance_ns: i64,
    },
    #[error(
        "proposed GC config violates protection_horizon >= max_compaction_lifetime + 4 * \
         clock_skew_allowance: protection_horizon={protection_horizon_ns} ns, \
         max_compaction_lifetime={max_compaction_lifetime_ns} ns (this build's compiled value), \
         clock_skew_allowance={clock_skew_allowance_ns} ns (need protection_horizon >= {}): a \
         compaction or rewrite run could still publish over a record's inputs after their horizon \
         passed (ADR-1133); refusing to write sys/gc",
        .max_compaction_lifetime_ns.saturating_add(.clock_skew_allowance_ns.saturating_mul(4))
    )]
    CompactionLifetimeViolation {
        protection_horizon_ns: i64,
        max_compaction_lifetime_ns: i64,
        clock_skew_allowance_ns: i64,
    },
    #[error(
        "sys/gc records protection_horizon={stored_horizon_ns} ns, but THIS maintain process runs \
         with max_compaction_lifetime={max_compaction_lifetime_ns} ns and \
         clock_skew_allowance={clock_skew_allowance_ns} ns, and ADR-1133 requires \
         protection_horizon >= max_compaction_lifetime + 4 * clock_skew_allowance = {} ns: a \
         compaction or rewrite run could still publish over a record's inputs after their horizon \
         passed, changing the set a delete marker gates. Raise the durable horizon via `ravel-cli \
         gc-config set` or lower the skew allowance; refusing to start",
        .max_compaction_lifetime_ns.saturating_add(.clock_skew_allowance_ns.saturating_mul(4))
    )]
    MaintainCompactionLifetimeUncovered {
        stored_horizon_ns: i64,
        max_compaction_lifetime_ns: i64,
        clock_skew_allowance_ns: i64,
    },
    #[error(
        "this query engine's deadline is {deadline_ns} ns, but sys/gc records \
         max_query_duration={max_query_duration_ns} ns: a query may not outlive the GC protection \
         horizon's query-duration term; refusing to start"
    )]
    QueryDeadlineExceedsHorizon {
        deadline_ns: i64,
        max_query_duration_ns: i64,
    },
    #[error(
        "this Flight SQL ticket-TTL ceiling is {ceiling_ns} ns, but sys/gc records \
         protection_horizon={protection_horizon_ns} ns and grace={grace_ns} ns, so the ceiling must \
         be <= {}: a ticket must not outlive the protection its pinned snapshot depends on; \
         refusing to start",
        .protection_horizon_ns.saturating_sub(*.grace_ns).max(0)
    )]
    FlightCeilingExceedsHorizon {
        ceiling_ns: i64,
        protection_horizon_ns: i64,
        grace_ns: i64,
    },
    #[error(
        "this query process's HEAD cache TTL is {effective_ttl_ns} ns, but sys/gc (format_version \
         {format_version}) records head_cache_ttl={recorded_ttl_ns} ns: a query may not be served \
         a cached HEAD for longer than the recorded TTL, the bound ADR-1133's sweeper delete gate \
         is specified against; refusing to start"
    )]
    QueryHeadCacheTtlExceedsRecorded {
        effective_ttl_ns: i64,
        recorded_ttl_ns: i64,
        format_version: u32,
    },
}

/// Which `sys/gc` request a [`GcConfigError::AccessDenied`] was refused on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GcAccessOp {
    Read,
    Create,
}

impl std::fmt::Display for GcAccessOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            GcAccessOp::Read => "read",
            GcAccessOp::Create => "create",
        })
    }
}

/// Read `sys/gc` if it exists, returning the decoded values and the store
/// version needed for a later `CasVersion` swap. `Ok(None)` is a bucket where
/// the object has not been bootstrapped yet (legitimate absence, not a fault).
pub async fn read_gc_config(
    store: &dyn ObjectStoreBackend,
) -> Result<Option<(GcConfigValues, Version)>, GcConfigError> {
    match store.get(GC_CONFIG_KEY, GetRange::Full).await {
        Ok(outcome) => {
            let proto = sysproto::GcConfig::decode(outcome.data.as_ref())
                .map_err(|err| GcConfigError::Decode(err.to_string()))?;
            let values = GcConfigValues::from_proto(proto)?;
            Ok(Some((values, outcome.version)))
        }
        Err(StoreError::NotFound) => Ok(None),
        Err(StoreError::AccessDenied(detail)) => Err(GcConfigError::AccessDenied {
            op: GcAccessOp::Read,
            detail,
        }),
        Err(err) => Err(GcConfigError::Store(err.to_string())),
    }
}

/// Bootstrap `sys/gc` on a fresh bucket, or read the durable object on a
/// bootstrapped one, returning the values every mode then validates against
/// (ADR-0050 section 4).
///
/// The critical fail-open-avoidance property: a never-bootstrapped bucket does
/// not refuse startup. The object is written from `defaults` (the caller's
/// maintain-config-derived values, which satisfy the constraint), and
/// validation then runs against the object this process just wrote, which
/// trivially matches. A concurrent bootstrap that wins the race is handled by
/// re-reading and returning the winner's object, so a loser never errors and
/// never proceeds with its own unwritten values.
///
/// The object is always written at format version 1 with the compiled
/// [`DEFAULT_HEAD_CACHE_TTL_NS`], whatever version and TTL `defaults` carries,
/// so a new build bootstrapping a fresh bucket first does not lock out older
/// builds that read only version 1 (ADR-1133 decision 4).
///
/// `defaults` is validated (issue #1744 fix round) before it is ever written:
/// the production caller passes `GcConfigValues::maintain_defaults()`, a
/// compiled-in constant, not something a flag or `set_gc_config`'s own
/// [`GcConfigValues::validate`] call has already checked, so this is the only
/// choke point standing between a bootstrap and a durable non-positive or
/// below-the-ingest-floor `sys/gc`. Positive-value and constraint violations
/// are already impossible for `maintain_defaults()` by construction; the
/// floor is the one term nothing else ties to the ingest pipeline's own
/// default, so a future divergence between them fails a fresh bootstrap
/// loudly instead of writing an object every mode would then refuse to
/// validate against anyway.
pub async fn bootstrap_gc_config(
    store: &dyn ObjectStoreBackend,
    defaults: GcConfigValues,
    now_ns: i64,
) -> Result<GcConfigValues, GcConfigError> {
    if let Some((values, _version)) = read_gc_config(store).await? {
        return Ok(values);
    }

    let defaults = GcConfigValues {
        format_version: GC_FORMAT_VERSION_V1,
        head_cache_ttl_ns: DEFAULT_HEAD_CACHE_TTL_NS,
        ..defaults
    };
    defaults.validate(ingest_max_flush_lifetime_floor_ns())?;
    let bytes = defaults.to_proto(now_ns).encode_to_vec();
    match store
        .put(GC_CONFIG_KEY, bytes.into(), PutOptions::create_if_absent())
        .await
    {
        Ok(_) => Ok(defaults),
        // A concurrent process bootstrapped first. Re-read and return the
        // winner's object rather than our own defaults, so a later `gc-config
        // set` that raced the bootstrap is honored and every racer converges on
        // one durable truth.
        Err(StoreError::AlreadyExists) => {
            let (values, _version) = read_gc_config(store)
                .await?
                .ok_or(GcConfigError::ObjectVanished)?;
            Ok(values)
        }
        Err(StoreError::AccessDenied(detail)) => Err(GcConfigError::AccessDenied {
            op: GcAccessOp::Create,
            detail,
        }),
        Err(err) => Err(GcConfigError::Store(err.to_string())),
    }
}

/// The result of a [`set_gc_config`] write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOutcome {
    /// `sys/gc` did not exist and was created (a bucket no process has
    /// bootstrapped yet).
    Created,
    /// `sys/gc` existed and was swapped in place via `CasVersion`.
    Updated,
}

/// Write a new `sys/gc` (the `ravel-cli gc-config set` path, ADR-0050 section
/// 4). Enforces the constraint at write time, then swaps the durable object with
/// `CasVersion` so a concurrent mutation is a [`GcConfigError::CasConflict`],
/// never a silent overwrite. On a bucket with no object yet, creates it with
/// `CreateIfAbsent` (a concurrent bootstrap winning that race is also a
/// `CasConflict`, since the caller's read observed no object).
///
/// This is the single mutation choke point (`ravel-cli gc-config set`) where the
/// skew-covering bound `protection_horizon >= max_query_duration + grace +
/// clock_skew_allowance` is enforced fail-closed: a proposal that fails it is
/// refused with [`GcConfigError::ConstraintViolation`] and writes nothing, so no
/// reachable `sys/gc` can leave a skewed sweeper free to delete a pinned reader's
/// snapshot. `clock_skew_allowance_ns` is the writer's configured skew
/// allowance (the sweeper's [`crate::CompactorConfig::clock_skew_allowance_ns`]),
/// supplied here rather than stored in `sys/gc`: it is a per-process input that
/// each maintain process re-validates against the stored values at startup
/// ([`validate_maintain_skew`]).
///
/// It also refuses, with [`GcConfigError::CompactionLifetimeViolation`], a
/// horizon below `max_compaction_lifetime + 4 * clock_skew_allowance` for this
/// build's compiled [`DEFAULT_MAX_COMPACTION_LIFETIME_NS`]
/// ([`GcConfigValues::satisfies_compaction_lifetime`]), which every maintain
/// process re-checks at startup ([`validate_maintain_compaction_lifetime`]).
///
/// The format version and HEAD cache TTL come from the proposal and the object
/// this call read, in the same read its `CasVersion` swap is conditioned on
/// ([`GcConfigProposal::head_cache_ttl_ns`]): a proposal without a TTL never
/// writes version 1 over a stored version 2. Returns the values written.
pub async fn set_gc_config(
    store: &dyn ObjectStoreBackend,
    proposed: GcConfigProposal,
    clock_skew_allowance_ns: i64,
    now_ns: i64,
) -> Result<(SetOutcome, GcConfigValues), GcConfigError> {
    let current = read_gc_config(store).await?;
    let written = proposed.resolve(current.as_ref().map(|(values, _version)| values));
    written.validate(ingest_max_flush_lifetime_floor_ns())?;
    if !written.satisfies_constraint(clock_skew_allowance_ns) {
        return Err(GcConfigError::ConstraintViolation {
            protection_horizon_ns: written.protection_horizon_ns,
            max_query_duration_ns: written.max_query_duration_ns,
            grace_ns: written.grace_ns,
            clock_skew_allowance_ns,
        });
    }
    if !written
        .satisfies_compaction_lifetime(DEFAULT_MAX_COMPACTION_LIFETIME_NS, clock_skew_allowance_ns)
    {
        return Err(GcConfigError::CompactionLifetimeViolation {
            protection_horizon_ns: written.protection_horizon_ns,
            max_compaction_lifetime_ns: DEFAULT_MAX_COMPACTION_LIFETIME_NS,
            clock_skew_allowance_ns,
        });
    }

    let bytes = written.to_proto(now_ns).encode_to_vec();
    let outcome = match current {
        Some((_current, version)) => {
            match store
                .put(
                    GC_CONFIG_KEY,
                    bytes.into(),
                    PutOptions {
                        mode: PutMode::CasVersion(version),
                        checksum: None,
                    },
                )
                .await
            {
                Ok(_) => SetOutcome::Updated,
                Err(StoreError::PreconditionFailed) => return Err(GcConfigError::CasConflict),
                Err(err) => return Err(GcConfigError::Store(err.to_string())),
            }
        }
        None => match store
            .put(GC_CONFIG_KEY, bytes.into(), PutOptions::create_if_absent())
            .await
        {
            Ok(_) => SetOutcome::Created,
            Err(StoreError::AlreadyExists) => return Err(GcConfigError::CasConflict),
            Err(err) => return Err(GcConfigError::Store(err.to_string())),
        },
    };
    Ok((outcome, written))
}

/// Maintain-mode startup check (ADR-0050 section 4): the configured horizon and
/// grace must EQUAL the stored values exactly. Process flags become must-match,
/// not independent knobs, so a maintain process that would sweep on a different
/// horizon than the deployment's durable truth refuses to start.
pub fn validate_maintain(
    stored: &GcConfigValues,
    configured_horizon_ns: i64,
    configured_grace_ns: i64,
) -> Result<(), GcConfigError> {
    if configured_horizon_ns != stored.protection_horizon_ns
        || configured_grace_ns != stored.grace_ns
    {
        return Err(GcConfigError::MaintainMismatch {
            configured_horizon_ns,
            configured_grace_ns,
            stored_horizon_ns: stored.protection_horizon_ns,
            stored_grace_ns: stored.grace_ns,
        });
    }
    Ok(())
}

/// Maintain-mode startup RE-ASSERT of the skew-covering horizon bound against
/// the RUNNING sweeper's own clock-skew allowance. The write fence in
/// [`set_gc_config`] validates a proposed `sys/gc` against the *CLI's* declared
/// `clock_skew_allowance`, but that knob and the running sweeper's
/// [`crate::CompactorConfig::clock_skew_allowance_ns`] are independent: a
/// deployment can write `sys/gc` with a 5 min skew while running sweepers
/// configured with a larger skew, leaving the durable horizon skew-uncovered
/// for the sweeper that actually deletes. [`validate_maintain`] does not catch
/// it -- it only checks that the configured horizon and grace EQUAL the stored
/// ones; the skew term appears in neither. This check re-runs the bound
/// `protection_horizon >= max_query_duration + grace + clock_skew_allowance`
/// with `clock_skew_allowance_ns` taken from the running sweeper's config, so
/// the config fence holds against the process that actually deletes. Reuses
/// [`GcConfigValues::satisfies_constraint`] (same saturating arithmetic).
/// Called fail-closed at maintain startup: a violation refuses to enter the
/// sweep loop rather than delete a pinned reader's snapshot.
pub fn validate_maintain_skew(
    stored: &GcConfigValues,
    clock_skew_allowance_ns: i64,
) -> Result<(), GcConfigError> {
    if !stored.satisfies_constraint(clock_skew_allowance_ns) {
        return Err(GcConfigError::MaintainSkewUncovered {
            stored_horizon_ns: stored.protection_horizon_ns,
            stored_max_query_duration_ns: stored.max_query_duration_ns,
            stored_grace_ns: stored.grace_ns,
            clock_skew_allowance_ns,
        });
    }
    Ok(())
}

/// Maintain-mode startup check (ADR-1133, gated-set stability amendment): the
/// stored horizon must satisfy
/// `protection_horizon >= max_compaction_lifetime + 4 * clock_skew_allowance`
/// ([`GcConfigValues::satisfies_compaction_lifetime`]) for THIS process's
/// compactor lifetime and skew allowance. One marker per record gates a stable
/// set only if no compaction or rewrite run can publish over the record's
/// inputs once their horizon has passed. Called fail-closed beside
/// [`validate_maintain_skew`], before any delete path runs.
pub fn validate_maintain_compaction_lifetime(
    stored: &GcConfigValues,
    max_compaction_lifetime_ns: i64,
    clock_skew_allowance_ns: i64,
) -> Result<(), GcConfigError> {
    if !stored.satisfies_compaction_lifetime(max_compaction_lifetime_ns, clock_skew_allowance_ns) {
        return Err(GcConfigError::MaintainCompactionLifetimeUncovered {
            stored_horizon_ns: stored.protection_horizon_ns,
            max_compaction_lifetime_ns,
            clock_skew_allowance_ns,
        });
    }
    Ok(())
}

/// Query-mode startup check (ADR-0050 section 4): the engine deadline must be
/// `<=` the stored `max_query_duration_ns`, so a query cannot outlive the GC
/// protection horizon's query-duration term.
pub fn validate_query_deadline(
    stored: &GcConfigValues,
    deadline_ns: i64,
) -> Result<(), GcConfigError> {
    if deadline_ns > stored.max_query_duration_ns {
        return Err(GcConfigError::QueryDeadlineExceedsHorizon {
            deadline_ns,
            max_query_duration_ns: stored.max_query_duration_ns,
        });
    }
    Ok(())
}

/// Query-mode startup check (ADR-1133 decision 4): the process's effective HEAD
/// cache TTL must be `<=` the recorded `head_cache_ttl_ns`, the bound
/// ADR-1133's sweeper delete gate is specified against, so no query is served
/// a cached HEAD for longer than it. On a version 1 object the recorded value
/// is the compiled [`DEFAULT_HEAD_CACHE_TTL_NS`], the value ADR-1133 specifies
/// the gate uses on version 1.
pub fn validate_query_head_cache_ttl(
    stored: &GcConfigValues,
    effective_ttl_ns: i64,
) -> Result<(), GcConfigError> {
    if effective_ttl_ns > stored.head_cache_ttl_ns {
        return Err(GcConfigError::QueryHeadCacheTtlExceedsRecorded {
            effective_ttl_ns,
            recorded_ttl_ns: stored.head_cache_ttl_ns,
            format_version: stored.format_version,
        });
    }
    Ok(())
}

/// Flight SQL startup check (ADR-0050 section 4): the ticket-TTL ceiling must be
/// `<=` `protection_horizon_ns - grace_ns`, so a pinned snapshot a ticket
/// redeems against is still guaranteed present.
pub fn validate_flight_ceiling(
    stored: &GcConfigValues,
    ceiling_ns: i64,
) -> Result<(), GcConfigError> {
    if ceiling_ns > stored.flight_ceiling_ns() {
        return Err(GcConfigError::FlightCeilingExceedsHorizon {
            ceiling_ns,
            protection_horizon_ns: stored.protection_horizon_ns,
            grace_ns: stored.grace_ns,
        });
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use futures::future::join_all;
    use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Rule, ScriptedFault};
    use ravel_object_store::memory::MemoryStore;
    use std::sync::Arc;

    use crate::config::{CompactorConfig, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS};

    fn store() -> Arc<dyn ObjectStoreBackend> {
        Arc::new(MemoryStore::new())
    }

    /// The maintain defaults satisfy the constraint and match
    /// `CompactorConfig::default()`'s horizon, grace, and flush lifetime, so
    /// bootstrapping from them and then validating maintain against the result
    /// is trivially clean.
    #[test]
    fn maintain_defaults_satisfy_the_constraint_and_match_compactor_default() {
        let d = GcConfigValues::maintain_defaults();
        let c = CompactorConfig::default();
        assert!(d.satisfies_constraint(c.clock_skew_allowance_ns));
        assert_eq!(d.protection_horizon_ns, c.protection_horizon_ns);
        assert_eq!(d.grace_ns, c.grace_ns);
        assert_eq!(d.max_flush_lifetime_ns, c.max_flush_lifetime_ns);
        // The constraint holds with exactly zero slack at the defaults: the
        // default horizon is max_query_duration + grace + clock_skew_allowance,
        // so the skew-covering bound is met at the bound.
        assert_eq!(
            d.protection_horizon_ns,
            d.max_query_duration_ns + d.grace_ns + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS
        );
    }

    /// `DEFAULT_MAX_FLUSH_LIFETIME_NS` must equal the ingest pipeline's own
    /// `max_flush_lifetime` floor (issue #1744). The two are declared
    /// independently -- one a compiled-in constant here, the other
    /// `ravel_ingest::IngestConfig::default().max_flush_lifetime` -- and
    /// nothing else in the build ties them together:
    /// `maintain_defaults_satisfy_the_constraint_and_match_compactor_default`
    /// above only compares the maintain default against
    /// `CompactorConfig::default()`, which is derived from this SAME
    /// constant, so it passes even if both drift from ravel-ingest together.
    /// A no-flag `ravel-server` startup resolves `max_flush_lifetime_ns` to
    /// this constant (`services/ravel-server/src/config.rs`'s
    /// `resolve_gc_runtime`), and `bootstrap_gc_config` writes it into a
    /// fresh bucket's `sys/gc` unvalidated (see
    /// `fresh_bucket_bootstraps_from_defaults` below): if this constant ever
    /// falls below the real ingest floor, both paths ship the exact defect
    /// issue #1744 closed, with no explicit flag involved. This test is the
    /// tripwire: a move in either default fails it at gate time.
    #[test]
    fn default_max_flush_lifetime_matches_the_ingest_floor() {
        assert_eq!(
            crate::config::DEFAULT_MAX_FLUSH_LIFETIME_NS,
            ingest_max_flush_lifetime_floor_ns()
        );
    }

    /// The critical bootstrap scenario: a completely fresh bucket, a process
    /// with default (constraint-satisfying) config bootstraps `sys/gc` and the
    /// resulting object matches the maintain defaults. This is the fresh-
    /// `ravel-operator`-cluster shape that must never fail startup.
    #[tokio::test]
    async fn fresh_bucket_bootstraps_from_defaults() {
        let store = store();
        let values =
            bootstrap_gc_config(store.as_ref(), GcConfigValues::maintain_defaults(), 1_000)
                .await
                .expect("a fresh bucket must bootstrap, never refuse");
        assert_eq!(values, GcConfigValues::maintain_defaults());
        // The object is durable now, and re-reads to the same values.
        let (reread, _version) = read_gc_config(store.as_ref())
            .await
            .expect("read")
            .expect("sys/gc exists after bootstrap");
        assert_eq!(reread, GcConfigValues::maintain_defaults());
        // And every mode validates cleanly against what was just written.
        let c = CompactorConfig::default();
        validate_maintain(&values, c.protection_horizon_ns, c.grace_ns).expect("maintain matches");
        validate_query_deadline(&values, 30_000_000_000).expect("30s deadline is under 1h");
        validate_flight_ceiling(&values, values.flight_ceiling_ns()).expect("ceiling at the bound");
    }

    /// Two processes with matching default config racing to bootstrap one fresh
    /// bucket: every one succeeds (no refusal), all converge on identical
    /// values, and exactly one object is written. Concurrency drives the
    /// `CreateIfAbsent` losers through the `AlreadyExists` re-read path.
    #[tokio::test]
    async fn concurrent_bootstrap_race_all_start_and_converge() {
        let store = store();
        let defaults = GcConfigValues::maintain_defaults();
        let results =
            join_all((0..8).map(|i| bootstrap_gc_config(store.as_ref(), defaults, 1_000 + i)))
                .await;
        for r in &results {
            let v = r
                .as_ref()
                .expect("no process refuses to start on a fresh bucket race");
            assert_eq!(
                *v, defaults,
                "every racer converges on the same durable truth"
            );
        }
        // Exactly one durable object exists (one winner), and it is the
        // defaults every racer agreed on.
        let (durable, _version) = read_gc_config(store.as_ref())
            .await
            .expect("read")
            .expect("one object exists");
        assert_eq!(durable, defaults);
    }

    /// The race loser adopts the winner's object rather than proceeding with its
    /// own unwritten values: a winner with a (still valid) non-default horizon
    /// is already present, and a loser bootstrapping with the plain defaults
    /// returns the winner's values, not its own.
    #[tokio::test]
    async fn race_loser_adopts_winners_object_not_its_own_defaults() {
        let store = store();
        // Winner: a larger horizon (still constraint-satisfying), written first.
        let winner = GcConfigValues {
            protection_horizon_ns: DEFAULT_MAX_QUERY_DURATION_NS + 2 * DEFAULT_GRACE_NS,
            grace_ns: DEFAULT_GRACE_NS,
            max_query_duration_ns: DEFAULT_MAX_QUERY_DURATION_NS,
            max_flush_lifetime_ns: DEFAULT_MAX_FLUSH_LIFETIME_NS,
            ..GcConfigValues::maintain_defaults()
        };
        assert!(winner.satisfies_constraint(DEFAULT_CLOCK_SKEW_ALLOWANCE_NS));
        bootstrap_gc_config(store.as_ref(), winner, 1)
            .await
            .expect("winner bootstraps");

        // Loser: plain defaults, but the object already exists, so it must
        // return the winner's values.
        let loser = bootstrap_gc_config(store.as_ref(), GcConfigValues::maintain_defaults(), 2)
            .await
            .expect("loser bootstraps against the existing object");
        assert_eq!(loser, winner, "loser adopts the winner's durable object");
        assert_ne!(
            loser,
            GcConfigValues::maintain_defaults(),
            "loser did not silently proceed with its own unwritten defaults"
        );
    }

    /// `gc-config set` refuses a proposed configuration that violates
    /// `protection_horizon >= max_query_duration + grace`, writing nothing.
    #[tokio::test]
    async fn set_refuses_a_constraint_violating_proposal() {
        let store = store();
        // horizon 2h, but max_query_duration 1h + grace 24h = 25h: violates.
        let bad = GcConfigValues {
            protection_horizon_ns: 2 * 3_600_000_000_000,
            grace_ns: DEFAULT_GRACE_NS,
            max_query_duration_ns: DEFAULT_MAX_QUERY_DURATION_NS,
            max_flush_lifetime_ns: DEFAULT_MAX_FLUSH_LIFETIME_NS,
            ..GcConfigValues::maintain_defaults()
        };
        let err = set_gc_config(
            store.as_ref(),
            bad.into(),
            DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
            1_000,
        )
        .await
        .expect_err("a constraint-violating proposal must be refused");
        assert!(
            matches!(err, GcConfigError::ConstraintViolation { .. }),
            "got: {err}"
        );
        // Nothing was written.
        assert!(
            read_gc_config(store.as_ref())
                .await
                .expect("read")
                .is_none(),
            "a refused set writes no object"
        );
    }

    /// FAILURE SUITE (the "disagreeing-config" row): a config whose
    /// `protection_horizon` meets the OLD bound
    /// (`= max_query_duration + grace`) but NOT the skew-covering bound
    /// (`+ clock_skew_allowance`) is REJECTED by `set_gc_config` validation with
    /// `ConstraintViolation`, proving a skew-uncovered sweeper config can never
    /// be written. The mirror: raising the horizon by exactly the skew allowance
    /// makes the same proposal acceptable and written.
    ///
    /// This test bites the exact fix. To watch it fail, revert
    /// `satisfies_constraint` to the old bound by dropping its
    /// `.saturating_add(clock_skew_allowance_ns)` term (leaving
    /// `>= max_query_duration + grace`): `just_meets_old_bound` then satisfies
    /// the constraint, `set_gc_config` accepts it, and the `expect_err` below
    /// panics with "a config that omits the clock-skew allowance must be refused".
    #[tokio::test]
    async fn set_refuses_a_config_that_omits_the_clock_skew_allowance() {
        let store = store();
        let skew = DEFAULT_CLOCK_SKEW_ALLOWANCE_NS;
        // Meets the OLD bound exactly: horizon = max_query_duration + grace, with
        // zero budget for a skewed-ahead sweeper's clock.
        let just_meets_old_bound = GcConfigValues {
            protection_horizon_ns: DEFAULT_MAX_QUERY_DURATION_NS + DEFAULT_GRACE_NS,
            grace_ns: DEFAULT_GRACE_NS,
            max_query_duration_ns: DEFAULT_MAX_QUERY_DURATION_NS,
            max_flush_lifetime_ns: DEFAULT_MAX_FLUSH_LIFETIME_NS,
            ..GcConfigValues::maintain_defaults()
        };
        // It DID satisfy the old bound (skew term zero), but does NOT
        // satisfy the skew-covering bound.
        assert!(
            just_meets_old_bound.satisfies_constraint(0),
            "the old bound (max_query_duration + grace) is met exactly"
        );
        assert!(
            !just_meets_old_bound.satisfies_constraint(skew),
            "the skew-covering bound is NOT met: this is the skew gap"
        );

        let err = set_gc_config(store.as_ref(), just_meets_old_bound.into(), skew, 1_000)
            .await
            .expect_err("a config that omits the clock-skew allowance must be refused");
        assert!(
            matches!(
                err,
                GcConfigError::ConstraintViolation {
                    clock_skew_allowance_ns,
                    ..
                } if clock_skew_allowance_ns == skew
            ),
            "got: {err}"
        );
        // A skew-uncovered set writes no object: the fence is fail-closed.
        assert!(
            read_gc_config(store.as_ref())
                .await
                .expect("read")
                .is_none(),
            "a skew-uncovered set writes no object"
        );

        // Mirror: raising the horizon by exactly the skew allowance meets the new
        // bound, so the same proposal is now accepted and durably written.
        let covers_skew = GcConfigValues {
            protection_horizon_ns: DEFAULT_MAX_QUERY_DURATION_NS + DEFAULT_GRACE_NS + skew,
            ..just_meets_old_bound
        };
        assert!(covers_skew.satisfies_constraint(skew));
        let (outcome, _written) = set_gc_config(store.as_ref(), covers_skew.into(), skew, 2_000)
            .await
            .expect("a skew-covering config is accepted");
        assert_eq!(outcome, SetOutcome::Created);
        let (stored, _v) = read_gc_config(store.as_ref())
            .await
            .expect("read")
            .expect("the accepted config was written");
        assert_eq!(stored, covers_skew);
    }

    /// Bug regression: an all-zero proposal `0,0,0,0` trivially satisfies the
    /// horizon constraint (`0 >= 0 + 0`), so before the positive-value floor it
    /// was accepted and written, after which no valid `sys/gc` could ever match
    /// and every mode refused to start forever. `set_gc_config` must now refuse
    /// it with `NonPositiveValue` and write nothing.
    #[tokio::test]
    async fn set_refuses_an_all_zero_proposal_and_writes_nothing() {
        let store = store();
        let all_zero = GcConfigValues {
            protection_horizon_ns: 0,
            grace_ns: 0,
            max_query_duration_ns: 0,
            max_flush_lifetime_ns: 0,
            ..GcConfigValues::maintain_defaults()
        };
        // The exact shape of the bug: the constraint check alone accepts this
        // (even with a zero skew allowance the horizon inequality holds).
        assert!(
            all_zero.satisfies_constraint(0),
            "0 >= 0 + 0 + 0: the constraint alone does not catch an all-zero config"
        );
        let err = set_gc_config(
            store.as_ref(),
            all_zero.into(),
            DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
            1_000,
        )
        .await
        .expect_err("an all-zero proposal must now be refused by the positive-value floor");
        assert!(
            matches!(
                err,
                GcConfigError::NonPositiveValue {
                    field: "protection_horizon_ns",
                    got: 0
                }
            ),
            "got: {err}"
        );
        // Nothing was written: a refused set never touches the durable object.
        assert!(
            read_gc_config(store.as_ref())
                .await
                .expect("read")
                .is_none(),
            "a refused all-zero set writes no object"
        );
    }

    /// A single non-positive field (here a negative grace) is refused too, not
    /// only the all-zero case: `validate` names the first offending field.
    #[tokio::test]
    async fn set_refuses_a_single_non_positive_field() {
        let store = store();
        let bad = GcConfigValues {
            grace_ns: -1,
            ..GcConfigValues::maintain_defaults()
        };
        let err = set_gc_config(
            store.as_ref(),
            bad.into(),
            DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
            1_000,
        )
        .await
        .expect_err("a negative grace must be refused");
        assert!(
            matches!(
                err,
                GcConfigError::NonPositiveValue {
                    field: "grace_ns",
                    got: -1
                }
            ),
            "got: {err}"
        );
    }

    /// A `CasVersion` conflict: two concurrent `gc-config set` calls read the
    /// same version; the first wins and the second's stale-version write is
    /// rejected, not silently overwritten or merged.
    #[tokio::test]
    async fn concurrent_set_stale_version_is_rejected() {
        let store = store();
        // Bootstrap so an object (and a version) exists.
        bootstrap_gc_config(store.as_ref(), GcConfigValues::maintain_defaults(), 1)
            .await
            .expect("bootstrap");
        // Both readers observe the same current version.
        let (_v1, version_a) = read_gc_config(store.as_ref())
            .await
            .expect("read")
            .expect("present");
        let (_v2, version_b) = read_gc_config(store.as_ref())
            .await
            .expect("read")
            .expect("present");
        assert_eq!(version_a, version_b, "both sets read the same version");

        let proposal = GcConfigValues {
            protection_horizon_ns: DEFAULT_MAX_QUERY_DURATION_NS + 2 * DEFAULT_GRACE_NS,
            grace_ns: DEFAULT_GRACE_NS,
            max_query_duration_ns: DEFAULT_MAX_QUERY_DURATION_NS,
            max_flush_lifetime_ns: DEFAULT_MAX_FLUSH_LIFETIME_NS,
            ..GcConfigValues::maintain_defaults()
        };
        let bytes = proposal.to_proto(2).encode_to_vec();
        // First writer wins with version_a.
        store
            .put(
                GC_CONFIG_KEY,
                bytes.clone().into(),
                PutOptions {
                    mode: PutMode::CasVersion(version_a),
                    checksum: None,
                },
            )
            .await
            .expect("first CAS write wins");
        // Second writer's stale version_b is rejected.
        let err = store
            .put(
                GC_CONFIG_KEY,
                bytes.into(),
                PutOptions {
                    mode: PutMode::CasVersion(version_b),
                    checksum: None,
                },
            )
            .await
            .expect_err("a stale-version CAS write must be rejected");
        assert!(matches!(err, StoreError::PreconditionFailed), "got: {err}");
        // And `set_gc_config` surfaces that as a typed CasConflict when driven
        // end to end after a concurrent change moved the version on.
        let conflict = set_gc_config_with_stale_read(store.as_ref(), proposal).await;
        assert!(
            matches!(conflict, Err(GcConfigError::CasConflict)),
            "got: {conflict:?}"
        );
    }

    /// Drive `set_gc_config`'s CAS branch against a version that a concurrent
    /// change has already superseded: reads the (now newer) object, then a
    /// second concurrent change moves the version again before the CAS lands.
    /// Modeled by mutating the object between this read and its write.
    async fn set_gc_config_with_stale_read(
        store: &dyn ObjectStoreBackend,
        proposed: GcConfigValues,
    ) -> Result<SetOutcome, GcConfigError> {
        let (_current, version) = read_gc_config(store).await?.expect("present");
        // A racing writer changes the object, invalidating `version`.
        let racer = GcConfigValues {
            max_flush_lifetime_ns: proposed.max_flush_lifetime_ns + 1,
            ..proposed
        };
        store
            .put(
                GC_CONFIG_KEY,
                racer.to_proto(9).encode_to_vec().into(),
                PutOptions {
                    mode: PutMode::CasVersion(version.clone()),
                    checksum: None,
                },
            )
            .await
            .expect("racer wins");
        // Our CAS with the now-stale version must fail as PreconditionFailed.
        match store
            .put(
                GC_CONFIG_KEY,
                proposed.to_proto(10).encode_to_vec().into(),
                PutOptions {
                    mode: PutMode::CasVersion(version),
                    checksum: None,
                },
            )
            .await
        {
            Ok(_) => Ok(SetOutcome::Updated),
            Err(StoreError::PreconditionFailed) => Err(GcConfigError::CasConflict),
            Err(err) => Err(GcConfigError::Store(err.to_string())),
        }
    }

    /// Maintain must-match, not just satisfies: a horizon/grace that
    /// individually satisfy the inequality but do not exactly equal the stored
    /// values still refuses (ADR-0050 section 4).
    #[test]
    fn maintain_horizon_that_only_satisfies_but_does_not_equal_refuses() {
        let stored = GcConfigValues::maintain_defaults();
        // horizon 26h > max_query_duration 1h + grace 24h = 25h: satisfies the
        // inequality, but != the stored 25h.
        let bigger_horizon = 26 * 3_600_000_000_000;
        assert!(bigger_horizon > stored.max_query_duration_ns + stored.grace_ns);
        let err = validate_maintain(&stored, bigger_horizon, stored.grace_ns)
            .expect_err("a merely-satisfying horizon must still refuse: it is must-match");
        assert!(
            matches!(err, GcConfigError::MaintainMismatch { .. }),
            "got: {err}"
        );
        // The exact defaults do pass.
        validate_maintain(&stored, stored.protection_horizon_ns, stored.grace_ns)
            .expect("the exact stored values match");
    }

    /// The gap the write-time fence leaves open: a stored `sys/gc` that
    /// satisfies its OWN declared skew (the CLI's `--clock-skew-allowance` at
    /// write time) is still rejected at maintain startup when the RUNNING
    /// sweeper is configured with a LARGER `clock_skew_allowance`, because the
    /// durable horizon no longer covers the skew of the process that actually
    /// deletes.
    /// `validate_maintain` (horizon/grace must-match) passes it -- the skew term
    /// is in neither field -- so `validate_maintain_skew` is what bites.
    ///
    /// Flip line to watch the fail-closed check pass through (a skew-uncovered
    /// sweeper would then be allowed to delete): change the call below to pass
    /// `0` instead of `larger_skew`, i.e. drop the sweeper-skew term. The bound
    /// then reduces to `horizon >= max_query_duration + grace`, which the stored
    /// config meets, `validate_maintain_skew` returns `Ok`, and the `expect_err`
    /// panics.
    #[test]
    fn maintain_skew_reassert_refuses_when_running_sweeper_skew_exceeds_stored_horizon() {
        let write_time_skew = DEFAULT_CLOCK_SKEW_ALLOWANCE_NS; // what the CLI declared
        // A stored config that meets the bound for the write-time skew exactly:
        // horizon = max_query_duration + grace + 5m. Written by a write fence
        // that was told the skew was 5m.
        let stored = GcConfigValues {
            protection_horizon_ns: DEFAULT_MAX_QUERY_DURATION_NS
                + DEFAULT_GRACE_NS
                + write_time_skew,
            grace_ns: DEFAULT_GRACE_NS,
            max_query_duration_ns: DEFAULT_MAX_QUERY_DURATION_NS,
            max_flush_lifetime_ns: DEFAULT_MAX_FLUSH_LIFETIME_NS,
            ..GcConfigValues::maintain_defaults()
        };
        assert!(
            stored.satisfies_constraint(write_time_skew),
            "the stored config covers the skew it was written against"
        );
        // maintain's must-match validation passes: the running sweeper's horizon
        // and grace equal the stored ones (skew is in neither).
        validate_maintain(&stored, stored.protection_horizon_ns, stored.grace_ns)
            .expect("horizon and grace equal the stored values");

        // The running sweeper, however, is configured with a LARGER skew than
        // the stored horizon budgets for. The re-assert must fail closed.
        let larger_skew = write_time_skew + 60_000_000_000; // +1 min over the 5m the horizon covers
        let err = validate_maintain_skew(&stored, larger_skew)
            .expect_err("a running sweeper skew the stored horizon does not cover must refuse");
        assert!(
            matches!(
                err,
                GcConfigError::MaintainSkewUncovered {
                    clock_skew_allowance_ns,
                    stored_horizon_ns,
                    ..
                } if clock_skew_allowance_ns == larger_skew
                    && stored_horizon_ns == stored.protection_horizon_ns
            ),
            "got: {err}"
        );

        // Mirror: a sweeper whose configured skew the stored horizon DOES cover
        // (here the same skew the config was written against) starts clean.
        validate_maintain_skew(&stored, write_time_skew)
            .expect("a horizon that covers the running sweeper's skew starts normally");
        // And a smaller running skew is covered a fortiori.
        validate_maintain_skew(&stored, write_time_skew - 1)
            .expect("a smaller running skew is covered too");
    }

    /// A stored horizon with small query and grace terms, which satisfies the
    /// skew-covering bound for the default skew.
    fn short_horizon(protection_horizon_ns: i64) -> GcConfigValues {
        GcConfigValues {
            protection_horizon_ns,
            grace_ns: 60_000_000_000,
            max_query_duration_ns: 60_000_000_000,
            ..GcConfigValues::maintain_defaults()
        }
    }

    /// `protection_horizon >= max_compaction_lifetime + 4 * clock_skew_allowance`
    /// at maintain startup (ADR-1133): passes at the bound, refuses one
    /// nanosecond of horizon below it, on a config the skew-covering bound
    /// alone accepts.
    ///
    /// Flip to watch it fail: make `validate_maintain_compaction_lifetime`
    /// return `Ok(())` without the check; the `expect_err` panics.
    #[test]
    fn maintain_compaction_lifetime_check_is_inclusive_at_the_bound() {
        let lifetime = DEFAULT_MAX_COMPACTION_LIFETIME_NS;
        let skew = DEFAULT_CLOCK_SKEW_ALLOWANCE_NS;
        let bound = lifetime + 4 * skew;

        let at_bound = short_horizon(bound);
        validate_maintain_compaction_lifetime(&at_bound, lifetime, skew)
            .expect("a horizon exactly at the bound starts");

        let below = short_horizon(bound - 1);
        validate_maintain_skew(&below, skew)
            .expect("the skew-covering bound alone accepts this horizon");
        let err = validate_maintain_compaction_lifetime(&below, lifetime, skew)
            .expect_err("a horizon one nanosecond below the bound must refuse");
        assert!(
            matches!(
                err,
                GcConfigError::MaintainCompactionLifetimeUncovered {
                    stored_horizon_ns,
                    max_compaction_lifetime_ns,
                    clock_skew_allowance_ns,
                } if stored_horizon_ns == bound - 1
                    && max_compaction_lifetime_ns == lifetime
                    && clock_skew_allowance_ns == skew
            ),
            "got: {err}"
        );
        let message = err.to_string();
        assert!(
            message.contains(&format!("max_compaction_lifetime={lifetime} ns"))
                && message.contains(&format!("= {bound} ns")),
            "the error names both values and the bound: {message}"
        );
    }

    /// The bootstrap defaults (25 h 5 min horizon) outlast this build's
    /// compiled compaction lifetime with the default skew.
    #[test]
    fn maintain_defaults_satisfy_the_compaction_lifetime_bound() {
        let defaults = GcConfigValues::maintain_defaults();
        assert!(defaults.satisfies_compaction_lifetime(
            DEFAULT_MAX_COMPACTION_LIFETIME_NS,
            DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
        ));
        let config = CompactorConfig::default();
        validate_maintain_compaction_lifetime(
            &defaults,
            config.max_compaction_lifetime_ns,
            config.clock_skew_allowance_ns,
        )
        .expect("the bootstrap defaults pass the startup check");
    }

    /// `set_gc_config` refuses a horizon below
    /// `DEFAULT_MAX_COMPACTION_LIFETIME_NS + 4 * clock_skew_allowance` and
    /// writes nothing; the bound itself is written.
    ///
    /// Flip to watch it fail: remove the `satisfies_compaction_lifetime` check
    /// in `set_gc_config`; the `expect_err` panics.
    #[tokio::test]
    async fn set_refuses_a_horizon_a_compaction_run_can_outlive() {
        let store = store();
        let skew = DEFAULT_CLOCK_SKEW_ALLOWANCE_NS;
        let bound = DEFAULT_MAX_COMPACTION_LIFETIME_NS + 4 * skew;
        let below = short_horizon(bound - 1);
        assert!(below.satisfies_constraint(skew));
        let err = set_gc_config(store.as_ref(), below.into(), skew, 1_000)
            .await
            .expect_err("a horizon below the compaction-lifetime bound must be refused");
        assert!(
            matches!(
                err,
                GcConfigError::CompactionLifetimeViolation {
                    protection_horizon_ns,
                    max_compaction_lifetime_ns,
                    clock_skew_allowance_ns,
                } if protection_horizon_ns == bound - 1
                    && max_compaction_lifetime_ns == DEFAULT_MAX_COMPACTION_LIFETIME_NS
                    && clock_skew_allowance_ns == skew
            ),
            "got: {err}"
        );
        assert!(
            read_gc_config(store.as_ref())
                .await
                .expect("read")
                .is_none(),
            "a refused set writes no object"
        );

        let at_bound = short_horizon(bound);
        set_gc_config(store.as_ref(), at_bound.into(), skew, 2_000)
            .await
            .expect("a horizon at the bound is accepted");
        let (stored, _v) = read_gc_config(store.as_ref())
            .await
            .expect("read")
            .expect("the accepted config was written");
        assert_eq!(stored.protection_horizon_ns, bound);
    }

    /// Query deadline validation: a deadline over the stored max_query_duration
    /// refuses; one at or under it passes.
    #[test]
    fn query_deadline_over_horizon_refuses() {
        let stored = GcConfigValues::maintain_defaults(); // max_query_duration = 1h
        let two_hours = 2 * 3_600_000_000_000;
        let err = validate_query_deadline(&stored, two_hours)
            .expect_err("a 2h deadline over a 1h max_query_duration must refuse");
        assert!(
            matches!(err, GcConfigError::QueryDeadlineExceedsHorizon { .. }),
            "got: {err}"
        );
        validate_query_deadline(&stored, stored.max_query_duration_ns)
            .expect("exactly at the bound");
        validate_query_deadline(&stored, 30_000_000_000).expect("30s well under");
    }

    /// Flight ceiling validation: a ceiling over `protection_horizon - grace`
    /// refuses; one at or under it passes.
    #[test]
    fn flight_ceiling_over_horizon_refuses() {
        let stored = GcConfigValues::maintain_defaults(); // ceiling = 25h5m - 24h = 1h5m
        let two_hours = 2 * 3_600_000_000_000;
        let err = validate_flight_ceiling(&stored, two_hours)
            .expect_err("a 2h ceiling over a 1h protection_horizon-grace must refuse");
        assert!(
            matches!(err, GcConfigError::FlightCeilingExceedsHorizon { .. }),
            "got: {err}"
        );
        validate_flight_ceiling(&stored, stored.flight_ceiling_ns()).expect("exactly at the bound");
    }

    /// A future-version `sys/gc` is refused with a typed error, not misread as
    /// v1 (matching the marker/record version guards).
    #[tokio::test]
    async fn future_version_object_is_a_typed_error() {
        let store = store();
        let proto = sysproto::GcConfig {
            format_version: 999,
            protection_horizon_ns: DEFAULT_PROTECTION_HORIZON_NS,
            grace_ns: DEFAULT_GRACE_NS,
            max_query_duration_ns: DEFAULT_MAX_QUERY_DURATION_NS,
            max_flush_lifetime_ns: DEFAULT_MAX_FLUSH_LIFETIME_NS,
            created_unix_ns: 1,
            head_cache_ttl_ns: 0,
        };
        store
            .put(
                GC_CONFIG_KEY,
                proto.encode_to_vec().into(),
                PutOptions::default(),
            )
            .await
            .expect("seed future-version object");
        let err = read_gc_config(store.as_ref())
            .await
            .expect_err("a future-version object must be a typed error, not misread");
        assert!(
            matches!(err, GcConfigError::UnsupportedVersion { got: 999 }),
            "got: {err}"
        );
    }

    /// Seed `sys/gc` with a raw proto at `format_version` carrying
    /// `head_cache_ttl_ns`, bypassing every write-side check.
    async fn seed_raw(store: &dyn ObjectStoreBackend, format_version: u32, head_cache_ttl_ns: i64) {
        let proto = sysproto::GcConfig {
            format_version,
            protection_horizon_ns: DEFAULT_PROTECTION_HORIZON_NS,
            grace_ns: DEFAULT_GRACE_NS,
            max_query_duration_ns: DEFAULT_MAX_QUERY_DURATION_NS,
            max_flush_lifetime_ns: DEFAULT_MAX_FLUSH_LIFETIME_NS,
            created_unix_ns: 1,
            head_cache_ttl_ns,
        };
        store
            .put(
                GC_CONFIG_KEY,
                proto.encode_to_vec().into(),
                PutOptions::default(),
            )
            .await
            .expect("seed raw sys/gc");
    }

    /// The stored proto, decoded without `from_proto`'s version handling.
    async fn stored_proto(store: &dyn ObjectStoreBackend) -> sysproto::GcConfig {
        let got = store
            .get(GC_CONFIG_KEY, GetRange::Full)
            .await
            .expect("sys/gc present");
        sysproto::GcConfig::decode(got.data.as_ref()).expect("decodes")
    }

    /// A version 1 object decodes its HEAD cache TTL as the compiled default
    /// even when the field carries another value: version 1 records no TTL, so
    /// a stray field value must not become the bound query processes are held
    /// to.
    #[tokio::test]
    async fn version_1_decodes_head_cache_ttl_as_the_compiled_default() {
        let store = store();
        let stray = 7 * DEFAULT_HEAD_CACHE_TTL_NS;
        seed_raw(store.as_ref(), GC_FORMAT_VERSION_V1, stray).await;
        let (values, _version) = read_gc_config(store.as_ref())
            .await
            .expect("version 1 is read")
            .expect("present");
        assert_eq!(values.format_version, GC_FORMAT_VERSION_V1);
        assert_eq!(
            values.head_cache_ttl_ns, DEFAULT_HEAD_CACHE_TTL_NS,
            "version 1 ignores the stored field and decodes the compiled default"
        );
    }

    /// A version 2 object round-trips its recorded TTL and its version.
    #[tokio::test]
    async fn version_2_round_trips_its_head_cache_ttl() {
        let store = store();
        let ttl = 12_345_000_000;
        let values = GcConfigValues {
            format_version: GC_FORMAT_VERSION,
            head_cache_ttl_ns: ttl,
            ..GcConfigValues::maintain_defaults()
        };
        store
            .put(
                GC_CONFIG_KEY,
                values.to_proto(9).encode_to_vec().into(),
                PutOptions::default(),
            )
            .await
            .expect("write version 2");
        let proto = stored_proto(store.as_ref()).await;
        assert_eq!(proto.format_version, 2);
        assert_eq!(proto.head_cache_ttl_ns, ttl);
        let (read, _version) = read_gc_config(store.as_ref())
            .await
            .expect("version 2 is read")
            .expect("present");
        assert_eq!(read, values);
    }

    /// A version 2 object must record a positive TTL: zero and negative are
    /// typed errors naming the stored value.
    #[tokio::test]
    async fn version_2_with_a_non_positive_head_cache_ttl_is_refused() {
        for bad in [0, -1] {
            let store = store();
            seed_raw(store.as_ref(), GC_FORMAT_VERSION, bad).await;
            let err = read_gc_config(store.as_ref())
                .await
                .expect_err("a non-positive version 2 TTL must be refused");
            assert_eq!(
                err,
                GcConfigError::StoredHeadCacheTtlNotPositive { got: bad }
            );
        }
    }

    /// Version 3 is above what this build reads and is refused, as every
    /// unknown version is.
    #[tokio::test]
    async fn version_3_is_refused() {
        let store = store();
        seed_raw(store.as_ref(), 3, DEFAULT_HEAD_CACHE_TTL_NS).await;
        let err = read_gc_config(store.as_ref())
            .await
            .expect_err("version 3 must be refused");
        assert_eq!(err, GcConfigError::UnsupportedVersion { got: 3 });
    }

    /// Bootstrap on an empty bucket writes format version 1 with no TTL field,
    /// even when the caller's defaults are version 2, so a new build touching
    /// a fresh bucket first cannot lock out an older build that reads only
    /// version 1.
    #[tokio::test]
    async fn bootstrap_on_an_empty_store_writes_version_1() {
        let store = store();
        let values =
            bootstrap_gc_config(store.as_ref(), GcConfigValues::maintain_defaults(), 1_000)
                .await
                .expect("bootstrap");
        assert_eq!(values.format_version, GC_FORMAT_VERSION_V1);
        let proto = stored_proto(store.as_ref()).await;
        assert_eq!(proto.format_version, 1, "bootstrap writes version 1");
        assert_eq!(
            proto.head_cache_ttl_ns, 0,
            "version 1 leaves the TTL absent"
        );

        let store = self::store();
        let v2_defaults = GcConfigValues {
            format_version: GC_FORMAT_VERSION,
            head_cache_ttl_ns: 5_000_000_000,
            ..GcConfigValues::maintain_defaults()
        };
        let values = bootstrap_gc_config(store.as_ref(), v2_defaults, 1_000)
            .await
            .expect("bootstrap");
        assert_eq!(values, GcConfigValues::maintain_defaults());
        assert_eq!(
            stored_proto(store.as_ref()).await.format_version,
            1,
            "bootstrap writes version 1 whatever version the caller's defaults carry"
        );
    }

    /// The query-side TTL check on a version 2 object: the recorded TTL itself
    /// passes, one nanosecond above it refuses, naming both values.
    #[test]
    fn query_head_cache_ttl_check_is_inclusive_at_the_recorded_value() {
        let recorded = 10_000_000_000;
        let stored = GcConfigValues {
            format_version: GC_FORMAT_VERSION,
            head_cache_ttl_ns: recorded,
            ..GcConfigValues::maintain_defaults()
        };
        validate_query_head_cache_ttl(&stored, recorded).expect("the recorded TTL itself passes");
        validate_query_head_cache_ttl(&stored, recorded - 1).expect("a lower TTL passes");
        let err = validate_query_head_cache_ttl(&stored, recorded + 1)
            .expect_err("one nanosecond above the recorded TTL must refuse");
        assert_eq!(
            err,
            GcConfigError::QueryHeadCacheTtlExceedsRecorded {
                effective_ttl_ns: recorded + 1,
                recorded_ttl_ns: recorded,
                format_version: GC_FORMAT_VERSION,
            }
        );
        let message = err.to_string();
        assert!(
            message.contains(&(recorded + 1).to_string())
                && message.contains(&recorded.to_string()),
            "the error names both values: {message}"
        );
    }

    /// On a version 1 object the query-side check compares against the
    /// compiled default: the default passes, one nanosecond above refuses.
    #[tokio::test]
    async fn query_head_cache_ttl_check_on_version_1_uses_the_compiled_default() {
        let store = store();
        seed_raw(store.as_ref(), GC_FORMAT_VERSION_V1, 0).await;
        let (stored, _version) = read_gc_config(store.as_ref())
            .await
            .expect("read")
            .expect("present");
        validate_query_head_cache_ttl(&stored, DEFAULT_HEAD_CACHE_TTL_NS)
            .expect("the compiled default passes on version 1");
        let err = validate_query_head_cache_ttl(&stored, DEFAULT_HEAD_CACHE_TTL_NS + 1)
            .expect_err("one nanosecond above the compiled default must refuse on version 1");
        assert_eq!(
            err,
            GcConfigError::QueryHeadCacheTtlExceedsRecorded {
                effective_ttl_ns: DEFAULT_HEAD_CACHE_TTL_NS + 1,
                recorded_ttl_ns: DEFAULT_HEAD_CACHE_TTL_NS,
                format_version: GC_FORMAT_VERSION_V1,
            }
        );
    }

    /// A corrupt (undecodable) `sys/gc` is a typed `Decode` error, never a panic.
    #[tokio::test]
    async fn corrupt_object_is_a_typed_decode_error() {
        let store = store();
        store
            .put(
                GC_CONFIG_KEY,
                vec![0xFF, 0xFF, 0xFF, 0x07].into(),
                PutOptions::default(),
            )
            .await
            .expect("seed garbage");
        let err = read_gc_config(store.as_ref())
            .await
            .expect_err("garbage must be a typed decode error, not a panic");
        assert!(matches!(err, GcConfigError::Decode(_)), "got: {err}");
    }

    /// Issue #1744: `GcConfigValues::validate` refuses a `max_flush_lifetime_ns`
    /// below the caller-supplied ingest floor, pinned exactly at the boundary
    /// (one nanosecond below refused, the floor itself accepted) -- never
    /// "some low value is refused". Uses an arbitrary floor, not
    /// `DEFAULT_MAX_FLUSH_LIFETIME_NS` or the real ingest default: `validate`
    /// takes the floor as a parameter rather than deriving it internally
    /// (see [`ingest_max_flush_lifetime_floor_ns`]), and a deliberately
    /// unusual floor value is what proves the check actually uses the
    /// argument. Watch this fail: replace the `ingest_max_flush_lifetime_ns`
    /// parameter inside `validate`'s comparison with a hardcoded
    /// `DEFAULT_MAX_FLUSH_LIFETIME_NS` (or any other fixed constant) --
    /// `below_floor` here (an arbitrary 999_000_000_000 ns) would then pass
    /// validation instead of being refused, and the `expect_err` below panics.
    #[test]
    fn validate_refuses_max_flush_lifetime_below_an_arbitrary_caller_supplied_floor() {
        let arbitrary_floor_ns: i64 = 999_000_000_000; // deliberately not 3600s
        let below_floor = GcConfigValues {
            max_flush_lifetime_ns: arbitrary_floor_ns - 1,
            ..GcConfigValues::maintain_defaults()
        };
        let err = below_floor
            .validate(arbitrary_floor_ns)
            .expect_err("one nanosecond below the supplied floor must be refused");
        assert!(
            matches!(
                err,
                GcConfigError::MaxFlushLifetimeBelowIngestFloor {
                    got,
                    floor
                } if got == arbitrary_floor_ns - 1 && floor == arbitrary_floor_ns
            ),
            "got: {err}"
        );

        let at_floor = GcConfigValues {
            max_flush_lifetime_ns: arbitrary_floor_ns,
            ..GcConfigValues::maintain_defaults()
        };
        at_floor
            .validate(arbitrary_floor_ns)
            .expect("the supplied floor itself must be accepted, not refused");
    }

    /// The production choke point ([`set_gc_config`]) derives the floor from
    /// [`ingest_max_flush_lifetime_floor_ns`], the same function
    /// `ravel-server`'s `Cli::validate` calls (issue #1744): a proposal one
    /// nanosecond below the real ingest floor is refused on the durable
    /// `sys/gc` write path even though it never passes through the CLI at
    /// all, and writes nothing.
    #[tokio::test]
    async fn set_gc_config_refuses_max_flush_lifetime_below_the_real_ingest_floor() {
        let store = store();
        let floor_ns = ingest_max_flush_lifetime_floor_ns();
        let below_floor = GcConfigValues {
            max_flush_lifetime_ns: floor_ns - 1,
            ..GcConfigValues::maintain_defaults()
        };
        let err = set_gc_config(
            store.as_ref(),
            below_floor.into(),
            DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
            1_000,
        )
        .await
        .expect_err(
            "a max_flush_lifetime one nanosecond below the real ingest floor must be refused \
             on the durable sys/gc write path",
        );
        assert!(
            matches!(
                err,
                GcConfigError::MaxFlushLifetimeBelowIngestFloor { floor, .. } if floor == floor_ns
            ),
            "got: {err}"
        );
        assert!(
            read_gc_config(store.as_ref())
                .await
                .expect("read")
                .is_none(),
            "a refused set writes no object"
        );

        // The floor itself passes (mirrors the maintain defaults, which equal
        // it today).
        let at_floor = GcConfigValues {
            max_flush_lifetime_ns: floor_ns,
            ..GcConfigValues::maintain_defaults()
        };
        set_gc_config(
            store.as_ref(),
            at_floor.into(),
            DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
            2_000,
        )
        .await
        .expect("max_flush_lifetime exactly at the real ingest floor must be accepted");
    }

    /// A store error on the bootstrap read surfaces as a typed `Store` error
    /// (fail-closed), proven with `FaultStore` and its counter.
    #[tokio::test]
    async fn bootstrap_surfaces_a_store_error() {
        let inner = MemoryStore::new();
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::Get,
                ScriptedFault::Transient("sys/gc unavailable".into()),
            )
            .with_key_contains(GC_CONFIG_KEY),
        );
        let store = FaultStore::new(inner, plan);
        let err = bootstrap_gc_config(&store, GcConfigValues::maintain_defaults(), 1)
            .await
            .expect_err("a store fault on the read must surface, not be swallowed");
        assert!(matches!(err, GcConfigError::Store(_)), "got: {err}");
        assert_eq!(
            store.fault_count(Op::Get, ravel_object_store::fault::FaultKind::Transient),
            1,
            "the injected fault must actually have fired"
        );
    }

    /// FaultStore has no access-denied fault, so the refusal tests script a
    /// `Permanent` fault whose message starts with this prefix and this
    /// adapter reports it as `StoreError::AccessDenied`, the class the S3
    /// backend maps a 403 to. FaultStore's own counters still prove the fault
    /// fired.
    const DENIED_PREFIX: &str = "denied by test policy: ";

    struct AccessDeniedStore {
        inner: FaultStore<MemoryStore>,
    }

    fn as_access_denied(err: StoreError) -> StoreError {
        match err {
            StoreError::Permanent(msg) if msg.starts_with(DENIED_PREFIX) => {
                StoreError::AccessDenied(msg)
            }
            other => other,
        }
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for AccessDeniedStore {
        async fn put(
            &self,
            key: &str,
            data: bytes::Bytes,
            opts: PutOptions,
        ) -> Result<ravel_object_store::PutOutcome, StoreError> {
            self.inner
                .put(key, data, opts)
                .await
                .map_err(as_access_denied)
        }

        async fn get(
            &self,
            key: &str,
            range: GetRange,
        ) -> Result<ravel_object_store::GetOutcome, StoreError> {
            self.inner.get(key, range).await.map_err(as_access_denied)
        }

        async fn get_pinned(
            &self,
            key: &str,
            range: GetRange,
            pin: &ravel_object_store::Pin,
        ) -> Result<ravel_object_store::PinnedRead, StoreError> {
            self.inner.get_pinned(key, range, pin).await
        }

        async fn put_multipart<'a>(
            &'a self,
            key: &str,
        ) -> Result<Box<dyn ravel_object_store::MultipartUpload + 'a>, StoreError> {
            self.inner.put_multipart(key).await
        }

        async fn head(&self, key: &str) -> Result<ravel_object_store::ObjectMeta, StoreError> {
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<ravel_object_store::PageToken>,
        ) -> Result<ravel_object_store::ListPage, StoreError> {
            self.inner.list(prefix, page).await
        }

        async fn list_after(
            &self,
            prefix: &str,
            start_after: Option<&str>,
            page: Option<ravel_object_store::PageToken>,
        ) -> Result<ravel_object_store::ListPage, StoreError> {
            self.inner.list_after(prefix, start_after, page).await
        }

        async fn list_delimited(
            &self,
            prefix: &str,
        ) -> Result<ravel_object_store::DelimitedList, StoreError> {
            self.inner.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> ravel_object_store::Capabilities {
            self.inner.capabilities()
        }
    }

    /// A store that refuses `op` on `sys/gc` the way a per-role credential
    /// without that grant does.
    fn refusing(inner: MemoryStore, op: Op) -> AccessDeniedStore {
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                op,
                ScriptedFault::Permanent(format!("{DENIED_PREFIX}{GC_CONFIG_KEY}")),
            )
            .with_key_contains(GC_CONFIG_KEY),
        );
        AccessDeniedStore {
            inner: FaultStore::new(inner, plan),
        }
    }

    fn denied_count(store: &AccessDeniedStore, op: Op) -> u64 {
        store
            .inner
            .fault_count(op, ravel_object_store::fault::FaultKind::Permanent)
    }

    /// A credential that may read but not create `sys/gc` (Gateway or Query
    /// under per-role IAM) on a fresh bucket: the refused create surfaces as
    /// `AccessDenied` on the create, not as a generic `Store` error, and
    /// nothing is written.
    #[tokio::test]
    async fn bootstrap_reports_a_refused_create_as_access_denied() {
        let store = refusing(MemoryStore::new(), Op::Put);
        let err = bootstrap_gc_config(&store, GcConfigValues::maintain_defaults(), 1)
            .await
            .expect_err("a refused create must fail the bootstrap");
        assert!(
            matches!(
                err,
                GcConfigError::AccessDenied {
                    op: GcAccessOp::Create,
                    ..
                }
            ),
            "got: {err:?}"
        );
        assert_eq!(
            denied_count(&store, Op::Put),
            1,
            "the refused PUT must actually have fired"
        );
        assert!(
            read_gc_config(&store).await.expect("read").is_none(),
            "a refused create writes nothing"
        );
    }

    /// A credential refused the `sys/gc` GET surfaces `AccessDenied` on the
    /// read, and the bootstrap never goes on to attempt a create.
    #[tokio::test]
    async fn bootstrap_reports_a_refused_read_as_access_denied() {
        let store = refusing(MemoryStore::new(), Op::Get);
        let err = bootstrap_gc_config(&store, GcConfigValues::maintain_defaults(), 1)
            .await
            .expect_err("a refused read must fail the bootstrap");
        assert!(
            matches!(
                err,
                GcConfigError::AccessDenied {
                    op: GcAccessOp::Read,
                    ..
                }
            ),
            "got: {err:?}"
        );
        assert_eq!(
            denied_count(&store, Op::Get),
            1,
            "the refused GET must actually have fired"
        );
        let head = store.inner.head(GC_CONFIG_KEY).await;
        assert!(
            matches!(head, Err(StoreError::NotFound)),
            "a refused read must not fall through to a create, got: {head:?}"
        );
    }

    /// Through the same adapter with no refusal scripted, an absent object
    /// with a permitted PUT still bootstraps from the defaults: the shared-
    /// credential deployment keeps the fresh-bucket property.
    #[tokio::test]
    async fn absent_object_with_a_permitted_put_still_bootstraps() {
        let store = AccessDeniedStore {
            inner: FaultStore::new(MemoryStore::new(), FaultPlan::empty()),
        };
        let values = bootstrap_gc_config(&store, GcConfigValues::maintain_defaults(), 1)
            .await
            .expect("a permitted create bootstraps");
        assert_eq!(values, GcConfigValues::maintain_defaults());
        let (reread, _version) = read_gc_config(&store)
            .await
            .expect("read")
            .expect("sys/gc exists after bootstrap");
        assert_eq!(reread, GcConfigValues::maintain_defaults());
    }

    /// A present object is returned as stored, and a credential with no PUT
    /// grant still starts because the bootstrap never issues the PUT.
    #[tokio::test]
    async fn present_object_returns_its_values_without_a_put() {
        let inner = MemoryStore::new();
        let stored = GcConfigValues {
            protection_horizon_ns: DEFAULT_MAX_QUERY_DURATION_NS + 2 * DEFAULT_GRACE_NS,
            grace_ns: DEFAULT_GRACE_NS,
            max_query_duration_ns: DEFAULT_MAX_QUERY_DURATION_NS,
            max_flush_lifetime_ns: DEFAULT_MAX_FLUSH_LIFETIME_NS,
            ..GcConfigValues::maintain_defaults()
        };
        bootstrap_gc_config(&inner, stored, 1)
            .await
            .expect("seed sys/gc");
        let store = refusing(inner, Op::Put);
        let values = bootstrap_gc_config(&store, GcConfigValues::maintain_defaults(), 2)
            .await
            .expect("a present object needs no PUT");
        assert_eq!(values, stored);
        assert_eq!(
            denied_count(&store, Op::Put),
            0,
            "no PUT may be issued against a present sys/gc"
        );
    }
}
