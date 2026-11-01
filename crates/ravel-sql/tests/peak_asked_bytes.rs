//! Issue #3607: `SqlOutcome.peak_asked_intermediate_bytes` is the high-water
//! mark of what this query's memory consumers asked for, before
//! `AggregateReserveAhead` charges a migrated hash aggregate stream ahead of
//! that ask (`TenantDelegatingPool::peak_asked_bytes`, wired through at
//! `crates/ravel-sql/src/executor.rs`'s `peak_asked_intermediate_bytes:
//! caps.peak_asked_bytes`). This runs the identical high-cardinality
//! `GROUP BY` through a real `SqlExecutor` twice, once under the default
//! reserve-ahead factor (1.25x) and once with it off, and checks the two
//! peaks the run reports: `outcome.peak_asked_intermediate_bytes` (asked) and
//! `outcome.accounting.peak_intermediate_bytes` (charged).

#![allow(clippy::expect_used, clippy::unwrap_used)]

use crate::util;

use std::sync::Arc;
use std::time::Duration;

use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_sql::{AggregateReserveAhead, SqlConfig, SqlOutcome, SqlRequest};
use util::{Fixture, SegSpec, SeriesSpec, tenant_id};

/// One group per sample: high enough that the hash table, not the scan,
/// dominates the pool's peak (the same shape
/// `aggregate_emit_reservation.rs` uses for its migrated-aggregate tests).
const GROUPS: i64 = 40_000;

const GROUP_BY: &str = "SELECT concat('group-', CAST(ts AS VARCHAR)) AS k, count(value) AS n \
                        FROM samples GROUP BY k";

fn one_sample_per_group() -> Vec<SegSpec> {
    vec![SegSpec::new(
        10,
        1,
        1,
        vec![SeriesSpec::new(
            "m",
            (0..GROUPS).map(|i| (i, i as f64)).collect(),
        )],
    )]
}

fn request() -> SqlRequest {
    SqlRequest {
        sql: GROUP_BY.to_string(),
        window: util::full_window(),
        min_tokens: Vec::new(),
        now_ns: util::NOW_NS,
        deadline: Duration::from_secs(60),
        row_window: false,
        max_rows: None,
        budgets: None,
    }
}

/// Run `GROUP_BY` through a real `SqlExecutor` whose `SqlConfig` charges hash
/// aggregates under `reserve_ahead`, over a fresh fixture so the two runs
/// share nothing but their data and statement.
async fn run(reserve_ahead: AggregateReserveAhead) -> SqlOutcome {
    let tenant = tenant_id("acme");
    let specs = one_sample_per_group();
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let mut config = SqlConfig::default();
    config.aggregate_reserve_ahead = reserve_ahead;
    // Pinned to one SQL partition: the asked gauge is a live, concurrency-
    // sensitive high-water mark (it rises and falls with every grow and
    // shrink, not just with the plan's shape), so a multi-partition plan's
    // scheduling jitter alone shifts it a little between otherwise identical
    // runs. One partition makes both runs' timing the same.
    config.engine.sql_partition_count = Some(1);
    let fixture = Fixture::build(store, &[(&tenant, &specs)], config, 1 << 30).await;
    fixture
        .executor
        .execute(tenant.hash(), &request())
        .await
        .expect("the statement runs")
}

/// A held release can leave the charged peak above the asked peak even with
/// no reserve-ahead factor in play (`holds_last_release` in memory.rs is
/// independent of `AggregateReserveAhead`); this is the slack the OFF-run
/// bound allows for it, beyond plain equality. The DEFAULT-run bound adds the
/// same slack on top of the 1.25x factor itself.
const HOLD_ALLOWANCE: f64 = 1.1;

/// Deliverable 3 (issue #3607): the asked peak is the same live-demand figure
/// regardless of the reserve-ahead setting, and only the charged peak moves
/// with it.
///
/// FLIP 1: at `crates/ravel-sql/src/executor.rs`'s
/// `peak_asked_intermediate_bytes: caps.peak_asked_bytes,` (the construction
/// of the successful-attempt `SqlOutcome`), change the right-hand side to
/// `accounting.peak_intermediate_bytes` (copying the charged peak into the
/// asked key). The DEFAULT and OFF runs then report different "asked" peaks
/// (one with the factor baked in, one without), failing
/// `asked_peak_is_unmoved_by_the_reserve_ahead_setting`'s equality assertion.
///
/// FLIP 2: change the same right-hand side to the literal `0`. Both runs then
/// report an asked peak of zero, failing this test's `asked_peak > 0`
/// assertion.
#[tokio::test]
async fn asked_peak_is_unmoved_by_the_reserve_ahead_setting_while_charged_peak_grows_with_it() {
    let default_run = run(AggregateReserveAhead::DEFAULT).await;
    let off_run = run(AggregateReserveAhead::OFF).await;

    let default_asked = default_run.peak_asked_intermediate_bytes;
    let off_asked = off_run.peak_asked_intermediate_bytes;
    let default_charged = default_run.accounting.peak_intermediate_bytes;
    let off_charged = off_run.accounting.peak_intermediate_bytes;
    eprintln!(
        "DEFAULT: asked={default_asked} charged={default_charged}; \
         OFF: asked={off_asked} charged={off_charged}"
    );

    assert!(
        default_asked > 0,
        "the fixture's hash aggregate must actually reserve memory"
    );
    assert_eq!(
        default_asked, off_asked,
        "the asked peak is the live reservation demand before the \
         reserve-ahead factor, so changing the factor must not move it"
    );

    assert!(
        default_asked <= default_charged,
        "a consumer cannot be charged less than it asked for: asked \
         {default_asked}, charged {default_charged}"
    );
    assert!(
        off_asked <= off_charged,
        "a consumer cannot be charged less than it asked for: asked \
         {off_asked}, charged {off_charged}"
    );

    assert!(
        default_charged > off_charged,
        "the default factor must charge the migrated aggregate ahead of what \
         it asks for: DEFAULT charged {default_charged}, OFF charged {off_charged}"
    );

    let default_ratio = default_charged as f64 / default_asked as f64;
    let max_default_ratio = 1.25 * HOLD_ALLOWANCE;
    assert!(
        (1.0..=max_default_ratio).contains(&default_ratio),
        "DEFAULT charged/asked ratio {default_ratio} outside [1.0, \
         {max_default_ratio}]: charged {default_charged}, asked {default_asked}"
    );

    let off_ratio = off_charged as f64 / off_asked as f64;
    assert!(
        (1.0..=HOLD_ALLOWANCE).contains(&off_ratio),
        "OFF charges one to one, give or take a held release: ratio {off_ratio} \
         outside [1.0, {HOLD_ALLOWANCE}]: charged {off_charged}, asked {off_asked}"
    );
}
