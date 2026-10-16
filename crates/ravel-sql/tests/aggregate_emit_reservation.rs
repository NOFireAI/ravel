//! Issues #2633 and #2720: a grouped aggregate's emitted output stays charged
//! to the query pool while it is handed out.
//!
//! A hash aggregate materializes every group's output at once and returns it
//! in `batch_size` slices. DataFusion 55.2's migrated aggregate streams keep
//! the materialized batch in their own reservation until the last slice is cut
//! from it, so the output a consumer is still pulling is in the pool's
//! `reserved()` without the pool's help. The legacy
//! `GroupedHashAggregateStream`, which DataFusion 55.2 still runs for the
//! shapes `AggregateExec::execute_typed` has not migrated (for example a
//! single-stage aggregate with a limit or over ordered input), shrinks its
//! reservation to the now-empty group state before it returns the first
//! slice. On that path the pool's aggregate hold keeps the shrunk bytes
//! charged.
//!
//! Both tests read the pool after the first `next()` returns and before the
//! second is polled, then drain the stream and assert that further batches
//! arrived, so at the read point the aggregate had not handed out its whole
//! output; and they assert that the pool is back to zero once the stream
//! drops, so a reading taken after the drain would be zero and could not
//! satisfy the bound.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use crate::util;

use std::sync::Arc;

use datafusion::execution::memory_pool::MemoryPool;
use datafusion::physical_plan::{displayable, execute_stream};
use futures::StreamExt;
use ravel_query::{PhaseAccounting, SegmentFetcher};
use ravel_sql::{
    RavelTableProvider, SessionTable, SpillDecision, SqlConfig, TenantDelegatingPool,
    TenantMemoryAccountant, build_session,
};
use ravel_types::accounting::QueryAccounting;
use util::{Fixture, SegSpec, SeriesSpec, tenant_id};

/// One group per sample: five `batch_size` (8192) slices of output.
const GROUPS: i64 = 40_000;

/// Groups keyed by a distinct string per sample, through the production scan.
/// The default plan runs it on DataFusion 55.2's migrated streams.
const GROUP_BY: &str = "SELECT concat('group-', CAST(ts AS VARCHAR)) AS k, count(value) AS n \
                        FROM samples GROUP BY k";

/// The same keys as a `DISTINCT` under a limit above the group count. Run with
/// one SQL partition it plans single-stage, and `execute_typed` has no
/// migrated stream for a single-stage aggregate with a limit, so it runs the
/// legacy `GroupedHashAggregateStream`.
const DISTINCT_LIMIT: &str = "SELECT DISTINCT concat('group-', CAST(ts AS VARCHAR)) AS k \
                              FROM samples LIMIT 100000";

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

/// What the pool read while the first output batch of one statement was held.
struct FirstBatchReading {
    plan: String,
    batch_size: usize,
    first_bytes: usize,
    reserved_while_held: usize,
    held_while_held: usize,
    later_batches: usize,
}

/// Run `sql` through the production pool (`SqlConfig::query_pool`, then
/// `build_session` with spill disabled) under `config`, reading the pool
/// between the first and second `next()`. Asserts one output row per group
/// and that the pool and the tenant read zero once the stream drops.
async fn read_while_first_batch_is_held(sql: &str, config: SqlConfig) -> FirstBatchReading {
    let tenant = tenant_id("acme");
    let specs = one_sample_per_group();
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;

    let accountant = TenantMemoryAccountant::new(1 << 30);
    let (pool, _breach) = config.query_pool(Arc::clone(&accountant), QueryAccounting::new());
    let held = || {
        pool.downcast_ref::<TenantDelegatingPool>()
            .expect("query_pool builds a TenantDelegatingPool")
            .held_bytes()
    };

    let provider = Arc::new(RavelTableProvider::new(
        fixture.snapshot(&tenant).await,
        tenant.hash(),
        SegmentFetcher::new(Arc::clone(&fixture.store)),
        config.clone(),
        PhaseAccounting::new(),
    ));
    let ctx = build_session(
        &config,
        Arc::clone(&pool) as Arc<dyn MemoryPool>,
        SessionTable::Metrics(provider),
        false,
        SpillDecision::Disabled,
    )
    .expect("session");
    let batch_size = ctx.state().config().batch_size();

    let physical = ctx
        .sql(sql)
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let plan = displayable(physical.as_ref()).indent(true).to_string();
    let mut stream = execute_stream(physical, ctx.task_ctx()).expect("execute");

    let first = stream
        .next()
        .await
        .expect("the aggregate emits a batch")
        .expect("batch");
    let first_bytes = first.get_array_memory_size();
    let reserved_while_held = pool.reserved();
    let held_while_held = held();

    let first_rows = first.num_rows();
    let mut rows = first_rows;
    let mut later_batches = 0usize;
    while let Some(next) = stream.next().await {
        rows += next.expect("batch").num_rows();
        later_batches += 1;
    }
    drop(first);
    drop(stream);

    eprintln!(
        "{sql}\nfirst batch: {first_rows} rows, {first_bytes} bytes; pool reserved \
         while held: {reserved_while_held}, of it held by the pool: {held_while_held}; \
         later batches: {later_batches}\n{plan}"
    );
    assert_eq!(rows, GROUPS as usize, "one output row per group");
    assert_eq!(
        pool.reserved(),
        0,
        "the pool returns to zero once the stream drops, so a post-drain read \
         is zero"
    );
    assert_eq!(held(), 0, "the hold is released when the stream drops");
    assert_eq!(accountant.reserved(), 0);
    FirstBatchReading {
        plan,
        batch_size,
        first_bytes,
        reserved_while_held,
        held_while_held,
        later_batches,
    }
}

/// On the default plan the migrated aggregate keeps its emitted batch
/// reserved itself: the pool's reserved total covers the first output batch
/// while it is held, and the pool holds nothing.
///
/// FLIP: add
/// `ctx.state_ref().write().config_mut().options_mut().execution.enable_migration_aggregate = false;`
/// after `build_session` in the helper and the plan runs on the legacy stream:
/// with the hold disabled as in the next test's FLIP, the bound fails at
/// 65,840 bytes reserved against a first batch of 3,866,832.
#[tokio::test]
async fn an_emitted_aggregate_batch_stays_reserved_while_it_is_held() {
    let reading = read_while_first_batch_is_held(GROUP_BY, SqlConfig::default()).await;
    assert!(
        reading.later_batches >= 2,
        "the output must span several {}-row slices, so the read point precedes \
         the drain; got {} after the first",
        reading.batch_size,
        reading.later_batches
    );
    assert!(
        reading.reserved_while_held >= reading.first_bytes,
        "the pool reserved {} bytes while the first output batch ({} bytes) was \
         held and {} more were to come",
        reading.reserved_while_held,
        reading.first_bytes,
        reading.later_batches
    );
    assert_eq!(
        reading.held_while_held, 0,
        "the migrated aggregate's consumer is not the legacy stream's, so the \
         pool holds nothing for it"
    );
}

/// A single-stage `DISTINCT ... LIMIT` (`--sql-partition-count 1`), which
/// DataFusion 55.2 still runs on the legacy `GroupedHashAggregateStream`: the
/// pool's reserved total covers the first output batch while it is held,
/// because the pool holds what the stream shrank at emit.
///
/// FLIP: set `HASH_AGGREGATE_CONSUMER_PREFIX` to a string no consumer name
/// starts with and the bound fails: measured 65,840 bytes reserved against a
/// first batch of 3,014,768. With the hold it read 4,609,312 reserved, of
/// which the pool held 4,543,472.
#[tokio::test]
async fn a_legacy_aggregate_batch_stays_reserved_through_the_hold() {
    let mut config = SqlConfig::default();
    config.engine.sql_partition_count = Some(1);
    let reading = read_while_first_batch_is_held(DISTINCT_LIMIT, config).await;
    assert!(
        reading.plan.contains("AggregateExec: mode=Single,") && reading.plan.contains(", lim=["),
        "the statement must plan a single-stage aggregate with a limit, the \
         shape execute_typed leaves on the legacy stream:\n{}",
        reading.plan
    );
    assert!(
        reading.later_batches >= 2,
        "the output must span several {}-row slices, so the read point precedes \
         the drain; got {} after the first",
        reading.batch_size,
        reading.later_batches
    );
    assert!(
        reading.reserved_while_held >= reading.first_bytes,
        "the pool reserved {} bytes while the first output batch ({} bytes) was \
         held and {} more were to come",
        reading.reserved_while_held,
        reading.first_bytes,
        reading.later_batches
    );
    assert!(
        reading.held_while_held > 0,
        "the pool held nothing for the legacy stream's consumer"
    );
}
