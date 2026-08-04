//! Durable GC-config (`sys/gc`) startup behavior (ADR-0050 section 4, EC4).
//!
//! These exercise the exact startup gates `ravel-server`'s `main` runs against
//! the durable `sys/gc` object: bootstrap-or-read, then per-mode validation.
//! The maintain/query gates live in `ravel_server::gc_config` and `main` calls
//! them before any listener binds; a query engine's real deadline is
//! `ravel_query::EngineConfig::default().deadline`, and these tests drive the
//! same gate with injected deadlines to prove the refusal and the pass.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use ravel_maintain::{CompactorConfig, GcConfigError, GcConfigValues, set_gc_config};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_server::{Cli, gc_config};

const NS_PER_HOUR: i64 = 3_600_000_000_000;

fn store() -> Arc<dyn ObjectStoreBackend> {
    Arc::new(MemoryStore::new())
}

fn cli(args: &[&str]) -> Cli {
    let mut argv = vec!["ravel-server"];
    argv.extend_from_slice(args);
    Cli::try_parse_from(argv).expect("flags parse")
}

/// Build the real compactor `main` would build for these flags: the three GC
/// knobs from `resolve_gc_runtime`, every other field at its default. Mirrors
/// `main.rs` exactly so the tests exercise the same resolution the binary does.
fn compactor_for(cli: &Cli) -> CompactorConfig {
    let gc = cli.resolve_gc_runtime().expect("--gc-* flags resolve");
    CompactorConfig {
        protection_horizon_ns: gc.protection_horizon_ns,
        grace_ns: gc.grace_ns,
        max_flush_lifetime_ns: gc.max_flush_lifetime_ns,
        ..CompactorConfig::default()
    }
}

/// The named acceptance test: bootstrap a bucket with the default `sys/gc`,
/// then a query-mode process configured with an engine deadline that exceeds
/// the stored `max_query_duration_ns` refuses to start with a typed error.
#[tokio::test]
async fn startup_rejects_deadline_exceeding_horizon() {
    let store = store();
    // Bootstrap the fresh bucket's sys/gc from the maintain defaults
    // (max_query_duration = 1h).
    let gc = gc_config::bootstrap(store.as_ref(), 1_000)
        .await
        .expect("a fresh bucket bootstraps sys/gc, never refuses");
    assert_eq!(gc.max_query_duration_ns, NS_PER_HOUR);

    // A query-mode process whose engine deadline is 2h exceeds the stored 1h
    // max_query_duration: startup refuses with the typed error.
    let err = gc_config::validate_query(&gc, Duration::from_secs(2 * 3600))
        .expect_err("a deadline over the stored max_query_duration must refuse startup");
    assert!(
        matches!(err, GcConfigError::QueryDeadlineExceedsHorizon { .. }),
        "got: {err}"
    );

    // The real server's query deadline (EngineConfig::default().deadline, 30s)
    // is well under the horizon and starts cleanly.
    gc_config::validate_query(&gc, ravel_query::EngineConfig::default().deadline)
        .expect("the default 30s engine deadline is under the 1h horizon");
}

/// The critical bootstrap scenario: a completely fresh bucket, a process with
/// default (constraint-satisfying) config starts successfully and the resulting
/// `sys/gc` object matches what was expected.
#[tokio::test]
async fn fresh_bucket_bootstraps_gc_config_and_starts_cleanly() {
    let store = store();
    let gc = gc_config::bootstrap(store.as_ref(), 42)
        .await
        .expect("a fresh bucket must bootstrap sys/gc, never fail startup");
    assert_eq!(gc, GcConfigValues::maintain_defaults());

    // The object is durable now.
    let raw = store
        .get(ravel_maintain::GC_CONFIG_KEY, GetRange::Full)
        .await
        .expect("sys/gc exists after bootstrap");
    assert!(!raw.data.is_empty(), "sys/gc holds bytes");

    // Every mode this process could be validates cleanly against what it wrote.
    gc_config::validate_maintain(&gc, &CompactorConfig::default())
        .expect("maintain's default horizon/grace equal the bootstrapped values");
    gc_config::validate_query(&gc, ravel_query::EngineConfig::default().deadline)
        .expect("the default engine deadline is under the horizon");
    gc_config::validate_flight(&gc, gc_config::flight_ceiling(&gc))
        .expect("the sys/gc-sourced Flight ceiling is at the bound, not over it");
}

/// The report's headline claim, as a test: a completely fresh bucket does not
/// fail startup for ANY process. Simulate the fresh-`ravel-operator`-cluster
/// shape -- gateway, query, and maintain pods all starting roughly together
/// against one empty bucket, every one with matching default config -- and
/// assert all bootstrap and validate cleanly, with no refusal, and converge on
/// one durable object.
#[tokio::test]
async fn fresh_bucket_no_process_refuses_startup() {
    let store = store();
    let compactor = CompactorConfig::default();
    let query_deadline = ravel_query::EngineConfig::default().deadline;

    // Gateway pod: no GC mode-validation of its own, but it may be the process
    // that bootstraps the object first.
    let gateway_gc = gc_config::bootstrap(store.as_ref(), 1)
        .await
        .expect("gateway pod bootstraps cleanly");

    // Maintain pod: bootstraps (or reads) and must EQUAL the stored values.
    let maintain_gc = gc_config::bootstrap(store.as_ref(), 2)
        .await
        .expect("maintain pod reads cleanly");
    gc_config::validate_maintain(&maintain_gc, &compactor)
        .expect("maintain pod validates: default horizon/grace equal the stored values");

    // Query pod: bootstraps (or reads) and its engine deadline must be under
    // the stored max_query_duration.
    let query_gc = gc_config::bootstrap(store.as_ref(), 3)
        .await
        .expect("query pod reads cleanly");
    gc_config::validate_query(&query_gc, query_deadline)
        .expect("query pod validates: 30s deadline under the 1h horizon");

    // All three pods agree on the one durable truth, and it is the maintain
    // defaults the fresh bucket was bootstrapped from.
    assert_eq!(gateway_gc, GcConfigValues::maintain_defaults());
    assert_eq!(maintain_gc, gateway_gc);
    assert_eq!(query_gc, gateway_gc);
}

/// The checkpoint's data-loss/availability scenario, fixed: after a
/// `gc-config set` to non-default values, a maintain process started with
/// matching `--gc-*` flags starts cleanly. Before these flags existed there
/// was nothing to set, so `CompactorConfig::default()` (25h/24h) could never
/// equal the operator's durable values and maintain was permanently bricked.
#[tokio::test]
async fn maintain_starts_after_gc_config_set_and_matching_flags() {
    let store = store();
    // Operator sets a non-default but constraint-satisfying config: horizon
    // 50h >= max_query_duration 1h + grace 40h.
    let set = GcConfigValues {
        protection_horizon_ns: 50 * NS_PER_HOUR,
        grace_ns: 40 * NS_PER_HOUR,
        max_query_duration_ns: NS_PER_HOUR,
        max_flush_lifetime_ns: NS_PER_HOUR,
    };
    assert!(set.satisfies_constraint());
    set_gc_config(store.as_ref(), set, 1_000)
        .await
        .expect("a constraint-satisfying set writes sys/gc");

    // A maintain process started with matching flags: its resolved compactor
    // horizon/grace equal the durable values.
    let compactor = compactor_for(&cli(&[
        "--mode",
        "maintain",
        "--gc-protection-horizon",
        "50h",
        "--gc-grace",
        "40h",
    ]));

    // The exact main.rs sequence: bootstrap/read, then validate maintain.
    let gc = gc_config::bootstrap(store.as_ref(), 2_000)
        .await
        .expect("reads the durable object, never refuses on mere presence");
    assert_eq!(gc, set, "reads back the operator's durable values");
    gc_config::validate_maintain(&gc, &compactor)
        .expect("maintain starts: matching --gc-* flags equal the durable sys/gc");
}

/// The must-match invariant is preserved, not weakened: after the same
/// `gc-config set`, a maintain process whose flags do NOT match the durable
/// object still refuses to start. Here the mismatching config is the
/// compiled-in defaults (no `--gc-*` flags at all), the exact pre-fix shape.
#[tokio::test]
async fn maintain_still_refuses_on_genuine_mismatch() {
    let store = store();
    let set = GcConfigValues {
        protection_horizon_ns: 50 * NS_PER_HOUR,
        grace_ns: 40 * NS_PER_HOUR,
        max_query_duration_ns: NS_PER_HOUR,
        max_flush_lifetime_ns: NS_PER_HOUR,
    };
    set_gc_config(store.as_ref(), set, 1_000)
        .await
        .expect("set writes sys/gc");

    // No --gc-* flags: the default 25h/24h compactor, which does not equal the
    // operator's 50h/40h durable values.
    let compactor = compactor_for(&cli(&["--mode", "maintain"]));

    let gc = gc_config::bootstrap(store.as_ref(), 2_000)
        .await
        .expect("reads the durable object");
    let err = gc_config::validate_maintain(&gc, &compactor)
        .expect_err("default flags do not equal the operator's sys/gc: must still refuse");
    assert!(
        matches!(err, GcConfigError::MaintainMismatch { .. }),
        "got: {err}"
    );
}

/// The query side of the same remediation: after a `gc-config set` that tightens
/// `max_query_duration` below the default 30s engine deadline, a query process
/// started with a matching `--gc-max-query-duration` flag starts cleanly, while
/// the default deadline would refuse. Proves the flag drives the SAME deadline
/// value the validation checks (and, via `start`, the real engine enforces).
#[tokio::test]
async fn query_starts_after_gc_config_set_and_matching_deadline_flag() {
    let store = store();
    // horizon 25h >= max_query_duration 10s + grace 24h: constraint holds, but
    // max_query_duration (10s) is now below the default engine deadline (30s).
    let set = GcConfigValues {
        protection_horizon_ns: 25 * NS_PER_HOUR,
        grace_ns: 24 * NS_PER_HOUR,
        max_query_duration_ns: 10 * 1_000_000_000,
        max_flush_lifetime_ns: NS_PER_HOUR,
    };
    assert!(set.satisfies_constraint());
    set_gc_config(store.as_ref(), set, 1_000)
        .await
        .expect("set writes sys/gc");
    let gc = gc_config::bootstrap(store.as_ref(), 2_000)
        .await
        .expect("reads the durable object");

    // Default deadline (no flag): 30s > 10s, query refuses.
    let default_deadline = cli(&["--mode", "query"])
        .resolve_gc_runtime()
        .expect("resolve")
        .query_deadline;
    let err = gc_config::validate_query(&gc, default_deadline)
        .expect_err("the default 30s deadline exceeds the tightened 10s max_query_duration");
    assert!(
        matches!(err, GcConfigError::QueryDeadlineExceedsHorizon { .. }),
        "got: {err}"
    );

    // Matching flag: 10s <= 10s, query starts.
    let matched = cli(&["--mode", "query", "--gc-max-query-duration", "10s"])
        .resolve_gc_runtime()
        .expect("resolve")
        .query_deadline;
    gc_config::validate_query(&gc, matched)
        .expect("query starts: --gc-max-query-duration matches the durable max_query_duration");
}
