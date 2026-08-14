//! End-to-end memory-pool byte accounting, exercising the
//! `TenantDelegatingPool` through a real DataFusion query rather than a
//! hand-built `TaskContext`.
//!
//! The pool unit tests (tests/pushdown_memory.rs) prove the pool's own arithmetic and
//! that a dropped mid-scan stream returns the tenant's bytes. What they
//! cannot prove is the property that actually matters: that a query
//! which pushes **many** batches through the pool releases all of them.
//!
//! The distinction matters. `RsegScanExec` grows its reservation once per
//! fetched/decoded segment and again per emitted batch (see
//! crate::scan module doc), and releases the whole thing when the stream
//! drops, so a single-batch test passes for either a correct implementation
//! or one whose release path only ever covers the last batch. Every test
//! here therefore forces more than one batch (over `RsegScanExec`'s
//! 8192-row batch size), checks that the reserved figure actually grew
//! *across* batches, and only then asserts the return to zero. A per-batch
//! `shrink` that leaked everything but the final batch would show a
//! non-zero residual here and nowhere else.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod util;

use std::sync::Arc;

use datafusion::execution::memory_pool::MemoryPool;
use futures::StreamExt;
use ravel_query::SegmentFetcher;
use ravel_sql::{
    RavelTableProvider, SessionTable, SqlConfig, TenantMemoryAccountant, build_session,
};
use ravel_types::TenantId;
use ravel_types::accounting::QueryAccounting;
use util::{Fixture, SegSpec, SeriesSpec, request, tenant_id};

/// Comfortably more than `RsegScanExec`'s 8192-row batch size, so the scan
/// emits several batches from one partition.
const SAMPLES: i64 = 30_000;

fn big_segment() -> Vec<SegSpec> {
    vec![SegSpec::new(
        10,
        1,
        1,
        vec![SeriesSpec::new(
            "m",
            (0..SAMPLES).map(|i| (i, i as f64)).collect(),
        )],
    )]
}

/// Drain one query through a pool the test owns, returning
/// (batch count, reserved after the first batch, peak reserved).
async fn drain(
    fixture: &Fixture,
    tenant: &TenantId,
    pool: Arc<dyn MemoryPool>,
    sql: &str,
) -> (usize, usize, usize) {
    let snapshot = fixture.snapshot(tenant).await;
    let provider = Arc::new(RavelTableProvider::new(
        snapshot,
        tenant.hash(),
        SegmentFetcher::new(Arc::clone(&fixture.store)),
        SqlConfig::default(),
        QueryAccounting::new(),
    ));
    let ctx = build_session(
        &SqlConfig::default(),
        Arc::clone(&pool),
        SessionTable::Metrics(provider),
    )
    .expect("session");

    let mut stream = ctx
        .sql(sql)
        .await
        .expect("plan")
        .execute_stream()
        .await
        .expect("execute");

    let mut batches = 0usize;
    let mut after_first = 0usize;
    let mut peak = 0usize;
    while let Some(next) = stream.next().await {
        let _batch = next.expect("batch");
        batches += 1;
        let reserved = pool.reserved();
        if batches == 1 {
            after_first = reserved;
        }
        peak = peak.max(reserved);
    }
    // Dropping the stream is what releases every `MemoryReservation`; the
    // pool forwards each drop-time shrink to the tenant accountant (see the
    // pool module docs).
    drop(stream);
    (batches, after_first, peak)
}

#[tokio::test]
async fn a_multi_batch_query_returns_every_reserved_byte() {
    let tenant = tenant_id("acme");
    let specs = big_segment();
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;

    let accountant = TenantMemoryAccountant::new(1 << 30);
    let (pool, _breach) =
        SqlConfig::default().query_pool(Arc::clone(&accountant), QueryAccounting::new());
    assert_eq!(pool.reserved(), 0, "a fresh pool starts empty");

    let (batches, after_first, peak) = drain(
        &fixture,
        &tenant,
        Arc::clone(&pool),
        "SELECT ts, value FROM samples",
    )
    .await;

    assert!(
        batches > 1,
        "the test is meaningless with one batch; got {batches}"
    );
    assert!(
        after_first > 0,
        "the scan must reserve bytes for its first batch"
    );
    assert!(
        peak > after_first,
        "reserved bytes must accumulate across batches \
         (first={after_first}, peak={peak}); otherwise this test would pass \
         for an implementation that only ever tracks one batch"
    );

    assert_eq!(
        pool.reserved(),
        0,
        "the query pool must return to zero once the streams drop"
    );
    assert_eq!(
        accountant.reserved(),
        0,
        "the tenant accountant must return to zero once the streams drop"
    );
}

/// An aggregating query puts DataFusion's own accumulator state through the
/// same pool, on top of the scan's batches. It must also return to zero.
#[tokio::test]
async fn a_multi_batch_aggregate_query_returns_every_reserved_byte() {
    let tenant = tenant_id("acme");
    let specs = big_segment();
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;

    let accountant = TenantMemoryAccountant::new(1 << 30);
    let (pool, _breach) =
        SqlConfig::default().query_pool(Arc::clone(&accountant), QueryAccounting::new());

    let (_batches, _first, _peak) = drain(
        &fixture,
        &tenant,
        Arc::clone(&pool),
        "SELECT series_id, count(value), sum(value) FROM samples GROUP BY series_id",
    )
    .await;

    assert_eq!(pool.reserved(), 0);
    assert_eq!(accountant.reserved(), 0);
}

/// Three consecutive queries against one tenant accountant. A leak of even
/// one batch per query would accumulate and show here, which a single-query
/// test cannot see.
#[tokio::test]
async fn repeated_multi_batch_queries_do_not_accumulate_tenant_bytes() {
    let tenant = tenant_id("acme");
    let specs = big_segment();
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;

    let accountant = TenantMemoryAccountant::new(1 << 30);
    for round in 0..3 {
        let (pool, _breach) =
            SqlConfig::default().query_pool(Arc::clone(&accountant), QueryAccounting::new());
        let (batches, _first, _peak) = drain(
            &fixture,
            &tenant,
            pool,
            "SELECT ts, value FROM samples ORDER BY ts",
        )
        .await;
        assert!(batches >= 1);
        assert_eq!(
            accountant.reserved(),
            0,
            "tenant bytes must be back to zero after round {round}"
        );
    }
}

/// The same property through the real endpoint driver, which owns its pool
/// internally: after `SqlExecutor::execute` returns, the tenant accountant
/// it used is back to zero. This is the assertion an operator cares about,
/// since nothing outside the executor can drop those streams by hand.
#[tokio::test]
async fn the_executor_returns_tenant_bytes_after_a_multi_batch_query() {
    let tenant = tenant_id("acme");
    let specs = big_segment();
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;

    let budget = fixture.executor.tenant_budget(tenant.hash());
    let outcome = fixture
        .executor
        .execute(tenant.hash(), &request("SELECT ts, value FROM samples"))
        .await
        .expect("query");

    assert!(
        outcome.output.num_batches() > 1,
        "expected a multi-batch result, got {}",
        outcome.output.num_batches()
    );
    assert_eq!(outcome.output.num_rows(), SAMPLES as usize);
    assert_eq!(
        budget.reserved(),
        0,
        "the executor must leave the tenant accountant at zero"
    );
}

/// A per-query byte ceiling smaller than the working set trips with the
/// pool's typed error, and still leaves the tenant accountant at zero: a
/// failed query must not leak either budget.
///
/// The reservation's first growth moved from the batch phase into
/// the fetch/decode phase (`prepare_partition`): the single segment's
/// decoded run alone (30,000 rows, 64 bytes each = ~1.9 MiB) already exceeds
/// this test's 1.4 MiB ceiling, so the trip now happens before the scan ever
/// builds its first `RecordBatch`. That is the intended, earlier rejection
/// the issue asked for, not a regression: zero batches is now the correct
/// outcome for a byte ceiling this far below the decoded input size.
#[tokio::test]
async fn a_query_that_outgrows_its_pool_still_releases_tenant_bytes() {
    let tenant = tenant_id("acme");
    let specs = big_segment();
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;

    let config = SqlConfig {
        engine: util::engine_config(),
        max_query_bytes: 1_400_000,
    };
    let accountant = TenantMemoryAccountant::new(1 << 30);
    let (pool, _breach) = config.query_pool(Arc::clone(&accountant), QueryAccounting::new());

    let snapshot = fixture.snapshot(&tenant).await;
    let provider = Arc::new(RavelTableProvider::new(
        snapshot,
        tenant.hash(),
        SegmentFetcher::new(Arc::clone(&fixture.store)),
        config,
        QueryAccounting::new(),
    ));
    let ctx = build_session(&config, Arc::clone(&pool), SessionTable::Metrics(provider))
        .expect("session");
    let mut stream = ctx
        .sql("SELECT ts, value FROM samples")
        .await
        .expect("plan")
        .execute_stream()
        .await
        .expect("execute");

    let mut failed = false;
    let mut batches = 0usize;
    while let Some(next) = stream.next().await {
        match next {
            Ok(_) => batches += 1,
            Err(e) => {
                assert!(
                    e.to_string().contains("memory pool exhausted"),
                    "expected the query pool's own error, got: {e}"
                );
                failed = true;
                break;
            }
        }
    }
    drop(stream);

    assert!(failed, "the query pool must trip");
    assert_eq!(
        batches, 0,
        "the fetch/decode-phase charge must reject this query \
         before its one segment's decoded run ever reaches the batch phase"
    );
    assert_eq!(
        accountant.reserved(),
        0,
        "a query that failed on its own budget must not leak tenant bytes"
    );
}
