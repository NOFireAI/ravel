//! Issue #2720: a grouped aggregate's emitted output stays charged to the
//! query pool while it is handed out, without any help from the pool.
//!
//! A hash aggregate materializes every group's output at once and returns it
//! in `batch_size` slices. DataFusion 55's migrated aggregate streams keep the
//! materialized batch in their own reservation until the last slice is cut
//! from it, so the output a consumer is still pulling is in the pool's
//! `reserved()`. The legacy `GroupedHashAggregateStream`, which DataFusion 55
//! still plans for the shapes it has not migrated (a single-stage aggregate
//! with a limit or over ordered input), shrinks its reservation to the
//! now-empty group state before it returns the first slice. That is the gap
//! issue #2633 closed with a pool-side hold; this test reads the pool, not
//! the hold.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use crate::util;

use std::sync::Arc;

use datafusion::execution::memory_pool::MemoryPool;
use futures::StreamExt;
use ravel_query::{PhaseAccounting, SegmentFetcher};
use ravel_sql::{
    RavelTableProvider, SessionTable, SpillDecision, SqlConfig, TenantMemoryAccountant,
    build_session,
};
use ravel_types::accounting::QueryAccounting;
use util::{Fixture, SegSpec, SeriesSpec, tenant_id};

/// One group per sample: five `batch_size` (8192) slices of output.
const GROUPS: i64 = 40_000;

/// Groups keyed by a distinct string per sample, through the production scan.
const SQL: &str = "SELECT concat('group-', CAST(ts AS VARCHAR)) AS k, count(value) AS n \
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

/// The pool's reserved total, read while the first output batch is held and
/// more output is still to come, covers that batch.
///
/// The read point is after the first `next()` returns and before the second
/// is polled. The test then drains the stream and asserts that further
/// batches arrived, so at the read point the aggregate had not handed out its
/// whole output; and it asserts that the pool is back to zero once the stream
/// drops, so a reading taken after the drain would be zero and could not
/// satisfy the bound. Only an aggregate whose reservation still covers the
/// output it is slicing passes.
///
/// FLIP: add
/// `ctx.state_ref().write().config_mut().options_mut().execution.enable_migration_aggregate = false;`
/// after `build_session` to plan the legacy `GroupedHashAggregateStream`, and
/// the bound fails: measured 65,840 bytes reserved against a first batch of
/// 3,539,120.
#[tokio::test]
async fn an_emitted_aggregate_batch_stays_reserved_while_it_is_held() {
    let tenant = tenant_id("acme");
    let specs = one_sample_per_group();
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;

    let accountant = TenantMemoryAccountant::new(1 << 30);
    let (pool, _breach) =
        SqlConfig::default().query_pool(Arc::clone(&accountant), QueryAccounting::new());

    let provider = Arc::new(RavelTableProvider::new(
        fixture.snapshot(&tenant).await,
        tenant.hash(),
        SegmentFetcher::new(Arc::clone(&fixture.store)),
        SqlConfig::default(),
        PhaseAccounting::new(),
    ));
    let ctx = build_session(
        &SqlConfig::default(),
        Arc::clone(&pool) as Arc<dyn MemoryPool>,
        SessionTable::Metrics(provider),
        false,
        SpillDecision::Disabled,
    )
    .expect("session");
    let batch_size = ctx.state().config().batch_size();

    let mut stream = ctx
        .sql(SQL)
        .await
        .expect("plan")
        .execute_stream()
        .await
        .expect("execute");

    let first = stream
        .next()
        .await
        .expect("the aggregate emits a batch")
        .expect("batch");
    let first_bytes = first.get_array_memory_size();
    let reserved_while_held = pool.reserved();

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
        "first batch: {first_rows} rows, {first_bytes} bytes; pool reserved \
         while held: {reserved_while_held}; later batches: {later_batches}"
    );
    assert_eq!(rows, GROUPS as usize, "one output row per group");
    assert!(
        later_batches >= 2,
        "the output must span several {batch_size}-row slices, so the read \
         point precedes the drain; got {later_batches} after the first"
    );
    assert!(
        reserved_while_held >= first_bytes,
        "the pool reserved {reserved_while_held} bytes while the first output \
         batch ({first_bytes} bytes) was held and {later_batches} more were to \
         come"
    );
    assert_eq!(
        pool.reserved(),
        0,
        "the pool returns to zero once the stream drops, so a post-drain read \
         is zero"
    );
    assert_eq!(accountant.reserved(), 0);
}
