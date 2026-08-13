//! The durable, deployment-wide GC configuration object `sys/gc` (ADR-0050
//! section 4, EC4).
//!
//! `protection_horizon >= max_query_duration + grace + clock_skew_allowance`
//! protects every pinned reader from the GC sweeper, including one whose clock
//! leads the reader's by up to `clock_skew_allowance` (adversarial finding
//! S1-02: without the skew term a sweeper skewed ahead reaches its deletion
//! threshold `now >= anchor + protection_horizon` in true time before a
//! reader's still-active snapshot, held up to `max_query_duration`, is
//! released). Before this object the bound lived in three unlinked
//! per-process configs (the maintain sweep config, the query deadline, the
//! Flight ticket ceiling) that could be deployed independently, with nothing
//! validating the constraint anywhere (adversarial findings S1-03 / S5-22). This
//! module makes the four deployment-wide values a single durable truth:
//!
//! - **Bootstrap.** On the first touch of a fresh bucket, [`bootstrap_gc_config`]
//!   writes `sys/gc` via `CreateIfAbsent` from the maintain defaults
//!   ([`GcConfigValues::maintain_defaults`], which satisfy the constraint by
//!   construction). It never refuses to start because the object is merely
//!   absent: an absent object on a fresh bucket is bootstrapped, not a fault
//!   (the EC3/#566 lesson). A racing loser (`AlreadyExists`) re-reads and
//!   returns the winner's object, exactly the race-loser pattern EB-7 / EC3 /
//!   EC5 established (`write_marker` / `resolve_and_pin` / the provisioning
//!   record), so two processes bootstrapping one fresh bucket both start.
//! - **Mutation.** Only [`set_gc_config`] (behind `ravel-cli gc-config set`)
//!   changes it. It enforces the constraint at write time and swaps with
//!   `CasVersion`, so a concurrent mutation is caught, never silently
//!   overwritten.
//! - **Startup validation, per mode.** Every mode reads the (now durable)
//!   object and validates itself against it, refusing to start on a real
//!   violation with a typed [`GcConfigError`]: maintain's horizon and grace
//!   must equal the stored values ([`validate_maintain`]); a query engine's
//!   deadline must be `<=` `max_query_duration_ns` ([`validate_query_deadline`]);
//!   a Flight ticket-TTL ceiling must be `<=` `protection_horizon_ns - grace_ns`
//!   ([`validate_flight_ceiling`]).
//!
//! The constraint is thereby enforced at exactly two choke points: the single
//! mutation path (the CLI, at write time) and each process's startup (against
//! the single durable truth). A process that can read a bootstrapped `sys/gc`
//! and finds a real violation does not start; there is no "assume defaults"
//! path.

use prost::Message;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutMode, PutOptions, StoreError, Version};
use ravel_proto::sys::v1 as sysproto;

use crate::config::{
    DEFAULT_GRACE_NS, DEFAULT_MAX_FLUSH_LIFETIME_NS, DEFAULT_MAX_QUERY_DURATION_NS,
    DEFAULT_PROTECTION_HORIZON_NS,
};

/// The bucket-root GC-config key (ADR-0050 section 4). Fixed, deployment-wide,
/// never under a tenant prefix.
pub const GC_CONFIG_KEY: &str = "sys/gc";

/// Format version written into every `sys/gc` object this module emits, and the
/// only version it reads. A future version is refused rather than misread under
/// the v1 layout.
pub const GC_FORMAT_VERSION: u32 = 1;

/// The four deployment-wide GC values recorded in `sys/gc`, decoded into plain
/// integers so callers (server startup, the CLI, tests) never touch the proto
/// type directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GcConfigValues {
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
}

impl GcConfigValues {
    /// The maintain defaults, which satisfy the constraint by construction
    /// (`protection_horizon = max_query_duration + grace + clock_skew_allowance`,
    /// plan §5, S1-02). This is
    /// what the first process to touch a fresh bucket bootstraps `sys/gc` from
    /// (ADR-0050 section 4), and it matches [`crate::CompactorConfig::default`]'s
    /// horizon, grace, and flush lifetime.
    pub fn maintain_defaults() -> Self {
        GcConfigValues {
            protection_horizon_ns: DEFAULT_PROTECTION_HORIZON_NS,
            grace_ns: DEFAULT_GRACE_NS,
            max_query_duration_ns: DEFAULT_MAX_QUERY_DURATION_NS,
            max_flush_lifetime_ns: DEFAULT_MAX_FLUSH_LIFETIME_NS,
        }
    }

    /// Whether these values satisfy the GC safety constraint
    /// `protection_horizon >= max_query_duration + grace + clock_skew_allowance`
    /// (S1-02). The `clock_skew_allowance_ns` term is supplied by the caller
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
    pub fn validate(&self) -> Result<(), GcConfigError> {
        for (field, got) in [
            ("protection_horizon_ns", self.protection_horizon_ns),
            ("grace_ns", self.grace_ns),
            ("max_query_duration_ns", self.max_query_duration_ns),
            ("max_flush_lifetime_ns", self.max_flush_lifetime_ns),
        ] {
            if got <= 0 {
                return Err(GcConfigError::NonPositiveValue { field, got });
            }
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

    fn to_proto(self, now_ns: i64) -> sysproto::GcConfig {
        sysproto::GcConfig {
            format_version: GC_FORMAT_VERSION,
            protection_horizon_ns: self.protection_horizon_ns,
            grace_ns: self.grace_ns,
            max_query_duration_ns: self.max_query_duration_ns,
            max_flush_lifetime_ns: self.max_flush_lifetime_ns,
            created_unix_ns: now_ns,
        }
    }

    fn from_proto(proto: sysproto::GcConfig) -> Result<Self, GcConfigError> {
        if proto.format_version != GC_FORMAT_VERSION {
            return Err(GcConfigError::UnsupportedVersion {
                got: proto.format_version,
            });
        }
        Ok(GcConfigValues {
            protection_horizon_ns: proto.protection_horizon_ns,
            grace_ns: proto.grace_ns,
            max_query_duration_ns: proto.max_query_duration_ns,
            max_flush_lifetime_ns: proto.max_flush_lifetime_ns,
        })
    }
}

/// A typed `sys/gc` failure. Every startup-validation variant refuses to start;
/// none warn and continue (ADR-0050's single fail-closed rule). Every variant
/// names the exact values so an operator sees what disagreed.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum GcConfigError {
    #[error("object store error accessing sys/gc: {0}")]
    Store(String),
    #[error("sys/gc is corrupt and could not be decoded: {0}")]
    Decode(String),
    #[error(
        "sys/gc declares format_version {got}, but this build only understands version \
         {GC_FORMAT_VERSION}: refusing rather than misread a future GC-config format as v1"
    )]
    UnsupportedVersion { got: u32 },
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
         reader still holds (S1-02). This is a deployment error to fix -- either lower the running \
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
        Err(err) => Err(GcConfigError::Store(err.to_string())),
    }
}

/// Bootstrap `sys/gc` on a fresh bucket, or read the durable object on a
/// bootstrapped one, returning the values every mode then validates against
/// (ADR-0050 section 4).
///
/// The critical fail-open-avoidance property (the EC3/#566 lesson): a
/// never-bootstrapped bucket does not refuse startup. The object is written from
/// `defaults` (the caller's maintain-config-derived values, which satisfy the
/// constraint), and validation then runs against the object this process just
/// wrote, which trivially matches. A concurrent bootstrap that wins the race is
/// handled by re-reading and returning the winner's object, so a loser never
/// errors and never proceeds with its own unwritten values.
pub async fn bootstrap_gc_config(
    store: &dyn ObjectStoreBackend,
    defaults: GcConfigValues,
    now_ns: i64,
) -> Result<GcConfigValues, GcConfigError> {
    if let Some((values, _version)) = read_gc_config(store).await? {
        return Ok(values);
    }

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
/// snapshot (S1-02). `clock_skew_allowance_ns` is the writer's configured skew
/// allowance (the sweeper's [`crate::CompactorConfig::clock_skew_allowance_ns`]),
/// supplied here rather than stored in `sys/gc` because the persistent format is
/// frozen; the fence is the validated config, not a stored field.
pub async fn set_gc_config(
    store: &dyn ObjectStoreBackend,
    proposed: GcConfigValues,
    clock_skew_allowance_ns: i64,
    now_ns: i64,
) -> Result<SetOutcome, GcConfigError> {
    proposed.validate()?;
    if !proposed.satisfies_constraint(clock_skew_allowance_ns) {
        return Err(GcConfigError::ConstraintViolation {
            protection_horizon_ns: proposed.protection_horizon_ns,
            max_query_duration_ns: proposed.max_query_duration_ns,
            grace_ns: proposed.grace_ns,
            clock_skew_allowance_ns,
        });
    }

    let bytes = proposed.to_proto(now_ns).encode_to_vec();
    match read_gc_config(store).await? {
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
                Ok(_) => Ok(SetOutcome::Updated),
                Err(StoreError::PreconditionFailed) => Err(GcConfigError::CasConflict),
                Err(err) => Err(GcConfigError::Store(err.to_string())),
            }
        }
        None => match store
            .put(GC_CONFIG_KEY, bytes.into(), PutOptions::create_if_absent())
            .await
        {
            Ok(_) => Ok(SetOutcome::Created),
            Err(StoreError::AlreadyExists) => Err(GcConfigError::CasConflict),
            Err(err) => Err(GcConfigError::Store(err.to_string())),
        },
    }
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
/// the RUNNING sweeper's own clock-skew allowance (issue #993, closing the #904
/// gap). The write fence in [`set_gc_config`] validates a proposed `sys/gc`
/// against the *CLI's* declared `clock_skew_allowance`, but that knob and the
/// running sweeper's [`crate::CompactorConfig::clock_skew_allowance_ns`] are
/// independent: a deployment can write `sys/gc` with a 5 min skew while running
/// sweepers configured with a larger skew, leaving the durable horizon
/// skew-uncovered for the sweeper that actually deletes. [`validate_maintain`]
/// does not catch it -- it only checks that the configured horizon and grace
/// EQUAL the stored ones; the skew term appears in neither. This check re-runs
/// the bound `protection_horizon >= max_query_duration + grace +
/// clock_skew_allowance` with `clock_skew_allowance_ns` taken from the running
/// sweeper's config, so the config fence holds against the process that actually
/// deletes. Reuses [`GcConfigValues::satisfies_constraint`] (same saturating
/// arithmetic). Called fail-closed at maintain startup: a violation refuses to
/// enter the sweep loop rather than delete a pinned reader's snapshot.
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
        // so the skew-covering bound is met at the bound (S1-02).
        assert_eq!(
            d.protection_horizon_ns,
            d.max_query_duration_ns + d.grace_ns + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS
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
        };
        let err = set_gc_config(store.as_ref(), bad, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, 1_000)
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

    /// FAILURE SUITE (S1-02, the ticket's required "disagreeing-config" row): a
    /// config whose `protection_horizon` meets the OLD bound
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
        };
        // It DID satisfy the pre-S1-02 bound (skew term zero), but does NOT
        // satisfy the skew-covering bound.
        assert!(
            just_meets_old_bound.satisfies_constraint(0),
            "the old bound (max_query_duration + grace) is met exactly"
        );
        assert!(
            !just_meets_old_bound.satisfies_constraint(skew),
            "the skew-covering bound is NOT met: this is the S1-02 gap"
        );

        let err = set_gc_config(store.as_ref(), just_meets_old_bound, skew, 1_000)
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
        let outcome = set_gc_config(store.as_ref(), covers_skew, skew, 2_000)
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
        };
        // The exact shape of the bug: the constraint check alone accepts this
        // (even with a zero skew allowance the horizon inequality holds).
        assert!(
            all_zero.satisfies_constraint(0),
            "0 >= 0 + 0 + 0: the constraint alone does not catch an all-zero config"
        );
        let err = set_gc_config(
            store.as_ref(),
            all_zero,
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
        let err = set_gc_config(store.as_ref(), bad, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, 1_000)
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

    /// Issue #993, the #904-gap closer: a stored `sys/gc` that satisfies its
    /// OWN declared skew (the CLI's `--clock-skew-allowance` at write time) is
    /// still rejected at maintain startup when the RUNNING sweeper is configured
    /// with a LARGER `clock_skew_allowance`, because the durable horizon no
    /// longer covers the skew of the process that actually deletes.
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
        // horizon = max_query_duration + grace + 5m. Written by a #904 write
        // fence that was told the skew was 5m.
        let stored = GcConfigValues {
            protection_horizon_ns: DEFAULT_MAX_QUERY_DURATION_NS
                + DEFAULT_GRACE_NS
                + write_time_skew,
            grace_ns: DEFAULT_GRACE_NS,
            max_query_duration_ns: DEFAULT_MAX_QUERY_DURATION_NS,
            max_flush_lifetime_ns: DEFAULT_MAX_FLUSH_LIFETIME_NS,
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
}
