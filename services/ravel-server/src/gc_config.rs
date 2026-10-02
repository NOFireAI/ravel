//! Server-side startup wiring for the durable GC-config object `sys/gc`
//! (ADR-0050 section 4, EC4).
//!
//! The object itself, its bootstrap/read/set, and the per-mode validators live
//! in [`ravel_maintain::gc_config`] (that crate owns the GC constraint and the
//! `CompactorConfig` the defaults derive from). This module is only the thin
//! server adapter: it bootstraps from this process's maintain defaults and
//! converts the `Duration`-typed process knobs (the query engine deadline, the
//! Flight ticket ceiling) into the nanoseconds the validators compare.
//!
//! The fresh-bucket property every startup path here depends on:
//! [`bootstrap`] writes `sys/gc` from the maintain defaults on a fresh
//! bucket rather than refusing because the object is absent, and a racing loser
//! re-reads the winner's object. So a fresh, never-bootstrapped bucket never
//! fails startup for any process whose credential may create `sys/gc`; only a
//! *present* object that a mode really violates refuses. Under per-role
//! credentials Gateway and Query may not create it, and their refused bootstrap
//! fails with [`bootstrap_failure_context`]'s start-order fix.

use std::time::Duration;

use ravel_maintain::{CompactorConfig, GcConfigError, GcConfigValues};
use ravel_object_store::ObjectStoreBackend;

/// A `Duration` as saturating `i64` nanoseconds, matching how every ns knob in
/// this workspace is compared. An absurd duration saturates to `i64::MAX`
/// rather than wrapping.
pub fn duration_to_ns(d: Duration) -> i64 {
    i64::try_from(d.as_nanos()).unwrap_or(i64::MAX)
}

/// Bootstrap `sys/gc` on a fresh bucket (from this process's maintain defaults),
/// or read the durable object on a bootstrapped one. Never refuses on a merely
/// absent object; a concurrent bootstrap loser re-reads the winner's object.
pub async fn bootstrap(
    store: &dyn ObjectStoreBackend,
    now_ns: i64,
) -> Result<GcConfigValues, GcConfigError> {
    ravel_maintain::bootstrap_gc_config(store, GcConfigValues::maintain_defaults(), now_ns).await
}

/// The context startup attaches to a failed [`bootstrap`]. A refused access is
/// a start-order problem under per-role credentials (ADR-0055 section 4: only
/// the Maintain and Admin roles may create `sys/gc`), so it names the fix rather
/// than reading like a broken credential; every other failure keeps the generic
/// context.
pub fn bootstrap_failure_context(err: &GcConfigError) -> String {
    match err {
        GcConfigError::AccessDenied { .. } => "sys/gc could not be read or created with this \
             process's object-store credential. Under per-role credentials only the Maintain \
             and Admin roles create sys/gc, so a gateway or query process started on a bucket \
             where it does not exist yet is refused. Start the maintain process first, or run \
             `ravel-cli gc-config set` under the Admin credential, then restart this process"
            .to_string(),
        _ => "failed to bootstrap or read the durable GC config (sys/gc)".to_string(),
    }
}

/// Maintain-mode check: the running compactor's horizon and grace must EQUAL the
/// stored values (ADR-0050 section 4).
pub fn validate_maintain(
    stored: &GcConfigValues,
    compactor: &CompactorConfig,
) -> Result<(), GcConfigError> {
    ravel_maintain::validate_maintain(stored, compactor.protection_horizon_ns, compactor.grace_ns)
}

/// Query-mode check: the engine deadline must be `<=` the stored
/// `max_query_duration_ns` (ADR-0050 section 4).
pub fn validate_query(stored: &GcConfigValues, deadline: Duration) -> Result<(), GcConfigError> {
    ravel_maintain::validate_query_deadline(stored, duration_to_ns(deadline))
}

/// Flight SQL check: the ticket-TTL ceiling must be `<=` the stored
/// `protection_horizon_ns` minus `grace_ns` (ADR-0050 section 4). The server
/// sources the ceiling from `sys/gc` (see [`flight_ceiling`]), so this passes by
/// construction and stands as the fail-closed guard against a hand-set ceiling.
pub fn validate_flight(stored: &GcConfigValues, ceiling: Duration) -> Result<(), GcConfigError> {
    ravel_maintain::validate_flight_ceiling(stored, duration_to_ns(ceiling))
}

/// The Flight ticket-TTL ceiling this deployment must use, sourced from the
/// durable `sys/gc` rather than a hardcoded default: `protection_horizon -
/// grace`. Wiring this into the Flight service is what makes the ticket ceiling
/// track the single durable GC authority (the flight_ticket.rs "the minting
/// caller ... owns the ceiling" note), instead of the conservative 24 h default
/// that predates `sys/gc`.
pub fn flight_ceiling(stored: &GcConfigValues) -> Duration {
    Duration::from_nanos(u64::try_from(stored.flight_ceiling_ns()).unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ravel_maintain::gc_config::GcAccessOp;

    #[test]
    fn refused_bootstrap_names_the_start_order_fix() {
        let err = GcConfigError::AccessDenied {
            op: GcAccessOp::Create,
            detail: "sys/gc: AccessDenied".into(),
        };
        let msg = bootstrap_failure_context(&err);
        assert!(msg.contains("Start the maintain process first"), "{msg}");
        assert!(
            msg.contains("`ravel-cli gc-config set` under the Admin credential"),
            "{msg}"
        );
        assert!(
            msg.contains("only the Maintain and Admin roles create sys/gc"),
            "{msg}"
        );
    }

    #[test]
    fn other_bootstrap_failures_keep_the_generic_context() {
        let msg = bootstrap_failure_context(&GcConfigError::Store("timeout".into()));
        assert_eq!(
            msg,
            "failed to bootstrap or read the durable GC config (sys/gc)"
        );
    }
}
