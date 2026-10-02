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
//! credentials Gateway and Query may not create it, and their refused create
//! fails with [`bootstrap_failure_context`]'s start-order fix. On S3 a GET of
//! an absent key is refused when the credential holds no covering
//! `s3:ListBucket`, so a fresh bucket can surface as a refused read in any
//! mode; that context names both readings. Every other refusal names the
//! missing grant.

use std::time::Duration;

use crate::Mode;
use ravel_maintain::{CompactorConfig, GcAccessOp, GcConfigError, GcConfigValues};
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

/// The context startup attaches to a failed [`bootstrap`] in `mode`. A refused
/// access names the grant or start order that fixes it, chosen by which request
/// was refused and by whether this mode's role may create `sys/gc` (ADR-0055
/// section 4: only the Maintain and Admin roles may); every other failure keeps
/// the generic context.
pub fn bootstrap_failure_context(err: &GcConfigError, mode: Mode) -> String {
    match err {
        GcConfigError::AccessDenied {
            op: GcAccessOp::Read,
            ..
        } => "this process's object-store credential was refused GetObject on sys/gc. On S3, a \
             GET of a key that does not exist is refused rather than reported missing when the \
             credential holds no s3:ListBucket covering that key, and the gateway, query and \
             maintain templates grant none covering sys/gc, so on a fresh bucket this can \
             mean sys/gc has not been created yet: run `ravel-cli gc-config set` under the \
             Admin credential, with a protection horizon and grace equal to the maintain \
             process's --gc-protection-horizon and --gc-grace (its defaults when unset) and \
             a max query duration and max flush lifetime matching the \
             --gc-max-query-duration and --gc-max-flush-lifetime the processes run with, \
             then restart this process. If sys/gc exists, this credential lacks read on it, \
             plus \
             kms:Decrypt on the bucket's default key if the bucket uses SSE-KMS, and \
             restarting will not help until that grant is fixed"
            .to_string(),
        GcConfigError::AccessDenied {
            op: GcAccessOp::Create,
            ..
        } => match mode {
            Mode::Gateway | Mode::Query => "sys/gc does not exist yet and this process's \
                 object-store credential was refused its create. Under per-role credentials \
                 only the Maintain and Admin roles create sys/gc: start the maintain process \
                 first, or run `ravel-cli gc-config set` under the Admin credential with a \
                 protection horizon and grace equal to the maintain process's \
                 --gc-protection-horizon and --gc-grace (its defaults when unset) and a max \
                 query duration and max flush lifetime matching the --gc-max-query-duration \
                 and --gc-max-flush-lifetime the processes run with, then restart this \
                 process. Under a shared credential the same refusal means this \
                 credential lacks PutObject on sys/gc or kms:GenerateDataKey on the bucket's \
                 default key"
                .to_string(),
            Mode::Maintain => "sys/gc does not exist yet and this process's object-store \
                 credential was refused its create. The Maintain role needs PutObject on \
                 sys/gc, which deploy/iam/maintain.json grants, plus kms:GenerateDataKey on the \
                 bucket's default key if the bucket uses SSE-KMS, which the shipped templates \
                 grant only on the tenant key"
                .to_string(),
            Mode::All => "sys/gc does not exist yet and this process's object-store \
                 credential was refused its create. An all-in-one process runs under one \
                 credential for every role, and that credential lacks PutObject on sys/gc, or \
                 kms:GenerateDataKey on the bucket's default key if the bucket uses SSE-KMS"
                .to_string(),
        },
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

/// Query-mode check: the HEAD cache TTL the query process's catalog runs on
/// must be `<=` the stored `head_cache_ttl_ns` (ADR-1133 decision 4). On a
/// version 1 `sys/gc` the stored value is the compiled default.
pub fn validate_query_head_cache_ttl(
    stored: &GcConfigValues,
    catalog_config: &ravel_catalog::CatalogConfig,
) -> Result<(), GcConfigError> {
    ravel_maintain::validate_query_head_cache_ttl(stored, catalog_config.head_cache_ttl_ns)
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

    fn denied(op: GcAccessOp) -> GcConfigError {
        GcConfigError::AccessDenied {
            op,
            detail: "sys/gc: AccessDenied".into(),
        }
    }

    #[test]
    fn refused_create_in_gateway_or_query_names_the_start_order_fix() {
        for mode in [Mode::Gateway, Mode::Query] {
            let msg = bootstrap_failure_context(&denied(GcAccessOp::Create), mode);
            assert!(msg.contains("start the maintain process first"), "{msg}");
            assert!(
                msg.contains("`ravel-cli gc-config set` under the Admin credential"),
                "{msg}"
            );
            assert!(
                msg.contains(
                    "equal to the maintain process's --gc-protection-horizon and --gc-grace"
                ),
                "{msg}"
            );
            assert!(
                msg.contains("only the Maintain and Admin roles create sys/gc"),
                "{msg}"
            );
            assert!(
                msg.contains(
                    "Under a shared credential the same refusal means this credential lacks \
                     PutObject on sys/gc or kms:GenerateDataKey"
                ),
                "{msg}"
            );
        }
    }

    #[test]
    fn refused_create_in_maintain_names_the_maintain_grant() {
        let msg = bootstrap_failure_context(&denied(GcAccessOp::Create), Mode::Maintain);
        assert!(
            msg.contains("The Maintain role needs PutObject on sys/gc"),
            "{msg}"
        );
        assert!(msg.contains("kms:GenerateDataKey"), "{msg}");
        assert!(
            msg.contains("the shipped templates grant only on the tenant key"),
            "{msg}"
        );
        assert!(
            !msg.to_lowercase().contains("start the maintain process"),
            "{msg}"
        );
        assert!(!msg.contains("gc-config set"), "{msg}");
    }

    #[test]
    fn refused_create_in_all_names_the_shared_credential() {
        let msg = bootstrap_failure_context(&denied(GcAccessOp::Create), Mode::All);
        assert!(
            msg.contains("one credential for every role, and that credential lacks PutObject"),
            "{msg}"
        );
        assert!(msg.contains("kms:GenerateDataKey"), "{msg}");
        assert!(!msg.contains("Maintain role"), "{msg}");
        assert!(!msg.contains("maintain.json"), "{msg}");
        assert!(
            !msg.to_lowercase().contains("start the maintain process"),
            "{msg}"
        );
        assert!(!msg.contains("gc-config set"), "{msg}");
    }

    #[test]
    fn refused_read_names_the_absent_key_case_and_the_read_grant_in_every_mode() {
        for mode in [Mode::All, Mode::Gateway, Mode::Query, Mode::Maintain] {
            let msg = bootstrap_failure_context(&denied(GcAccessOp::Read), mode);
            assert!(msg.contains("refused GetObject on sys/gc"), "{msg}");
            assert!(
                msg.contains("a GET of a key that does not exist is refused"),
                "{msg}"
            );
            assert!(
                msg.contains("run `ravel-cli gc-config set` under the Admin credential"),
                "{msg}"
            );
            assert!(
                msg.contains(
                    "equal to the maintain process's --gc-protection-horizon and --gc-grace"
                ),
                "{msg}"
            );
            assert!(msg.contains("kms:Decrypt"), "{msg}");
            assert!(
                msg.contains("restarting will not help until that grant is fixed"),
                "{msg}"
            );
            assert!(
                !msg.to_lowercase().contains("start the maintain process"),
                "{msg}"
            );
        }
    }

    #[test]
    fn other_bootstrap_failures_keep_the_generic_context() {
        let msg = bootstrap_failure_context(&GcConfigError::Store("timeout".into()), Mode::Gateway);
        assert_eq!(
            msg,
            "failed to bootstrap or read the durable GC config (sys/gc)"
        );
    }
}
