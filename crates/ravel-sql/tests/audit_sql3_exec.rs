//! Audit SQL-3 reproducers (issue #145, review
//! docs/reviews/2026-07-28-ravel-sql-audit/sql3-exec-memory-deadline.md).
//!
//! Each test is `#[ignore]`d and fails when un-ignored on the audited HEAD
//! (a7c95ef). They stay on the result branch; they are not a fix.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod util;

use std::sync::Arc;

use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool};
use ravel_object_store::memory::MemoryStore;
use ravel_sql::{CeilingBreach, SqlConfig, SqlError, TenantDelegatingPool, TenantMemoryAccountant};
use ravel_types::accounting::QueryAccounting;
use util::{Fixture, SegSpec, SeriesSpec, request, tenant_id};

/// sql3-F01 (S1, MEM). `MemoryPool::grow` is infallible and checks no
/// ceiling, so a reservation can grow past both the per-query and per-tenant
/// limits with no error, while `try_grow` of the same size refuses. The
/// configured budget is therefore not a hard cap on the `grow` path, which
/// (per the implementer's note at memory.rs:196-214) is the path the
/// nested-loop and sort-merge join operators use.
///
/// This drives the pool directly, no query engine needed, so it is
/// deterministic and isolates the mechanism from operator-selection details.
/// It FAILS on the audited HEAD: `reserved()` ends at 4096, four times the
/// 1024-byte ceiling, with no error raised.
#[test]
#[ignore = "sql3-F01: grow bypasses the memory ceiling; fails on the audited HEAD"]
fn sql3_f01_grow_bypasses_the_query_and_tenant_ceiling() {
    let tenant = TenantMemoryAccountant::new(1024);
    // The breach flag is the #163 addition; sql3-F01 ignores it and still
    // asserts the raw counters overshoot on the grow path (accepted in #156).
    let pool: Arc<dyn MemoryPool> = Arc::new(TenantDelegatingPool::new(
        1024,
        Arc::clone(&tenant),
        CeilingBreach::new(),
        QueryAccounting::new(),
    ));

    let res = MemoryConsumer::new("audit-sql3-f01").register(&pool);

    // The checked path enforces the ceiling: 4096 > 1024 is refused and
    // reserves nothing. This half already holds today.
    assert!(
        res.try_grow(4096).is_err(),
        "try_grow beyond the ceiling must fail"
    );
    assert_eq!(pool.reserved(), 0, "a failed try_grow reserves nothing");
    assert_eq!(tenant.reserved(), 0);

    // The infallible path is what joins reach. It must not exceed the ceiling.
    // On the audited HEAD it does: grow adds 4096 unconditionally to both
    // budgets.
    res.grow(4096);
    assert!(
        pool.reserved() <= 1024,
        "grow must not push the query reserved ({}) past the ceiling 1024",
        pool.reserved()
    );
    assert!(
        tenant.reserved() <= 1024,
        "grow must not push the tenant reserved ({}) past the ceiling 1024",
        tenant.reserved()
    );
}

/// sql3-F02 (S2, MEM). A memory-budget exhaustion raised deep in a plan (a
/// sort or an aggregate) is a native `DataFusionError::ResourcesExhausted`,
/// but by the time it reaches `execution_error` it is wrapped in
/// `Context`/`Shared`, which `take_sql_error` (executor.rs:366) re-wraps
/// rather than unwraps for a *native* error (it only recovers a nested
/// `SqlError`, and only strips the `ArrowError` layer). The top-level
/// `ResourcesExhausted` match therefore misses it and it degrades to
/// `SqlError::Execution`, whose `client_message` is the fixed, redacted
/// `MSG_EXECUTION` -- so the client is never told they hit the memory budget
/// and cannot narrow the query.
///
/// A projection-only scan happens to recover correctly (its error crosses
/// `SortPreservingMergeExec`, wrapped in `ArrowError`, which
/// `take_sql_error` *does* strip). The two common non-trivial shapes below
/// do not. Both assertions FAIL on the audited HEAD: the error is
/// `SqlError::Execution`, not `ResourcesExhausted`.
#[tokio::test]
async fn sql3_f02_a_sort_or_aggregate_budget_error_keeps_its_type() {
    let tenant = tenant_id("acme");
    let specs = vec![SegSpec::new(
        10,
        1,
        1,
        vec![SeriesSpec::new(
            "m",
            (0..20_000).map(|i| (i, i as f64)).collect(),
        )],
    )];
    // A ceiling a multi-batch scan overruns, so the sort/aggregate above it
    // raises the pool's ResourcesExhausted while running.
    let config = SqlConfig {
        engine: util::engine_config(),
        max_query_bytes: 1024 * 1024,
    };
    let fixture = Fixture::build(
        Arc::new(MemoryStore::new()),
        &[(&tenant, &specs)],
        config,
        1 << 40,
    )
    .await;

    for sql in [
        "SELECT ts, value FROM samples ORDER BY ts",
        "SELECT series_id, sum(value) FROM samples GROUP BY series_id",
    ] {
        let err = fixture
            .executor
            .execute(tenant.hash(), &request(sql))
            .await
            .expect_err("the tiny budget must trip");

        assert!(
            matches!(err, SqlError::ResourcesExhausted(_)),
            "a memory-budget exhaustion must keep its ResourcesExhausted type \
             so the client learns the budget was the cause; got {err:?} for [{sql}]"
        );
        assert!(
            err.client_message().contains("budget"),
            "the client message must name the budget; got {:?} for [{sql}]",
            err.client_message()
        );
    }
}
