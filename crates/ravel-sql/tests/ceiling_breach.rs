//! End-to-end coverage for aborting a join that overruns the per-query memory
//! ceiling, and for the no-leak guarantee of that abort path.
//!
//! # What this test asserts, and what it does not
//!
//! The ticket's two required end-to-end assertions are that a join query which
//! overruns `max_query_bytes` returns `SqlError::ResourcesExhausted` (not a
//! completed result, not a different error) and that afterward the tenant
//! accountant is back at zero reserved bytes -- proving the abort still drops
//! the stream and every `MemoryReservation` normally (crate::memory's
//! shrink-on-drop), so the abort path adds no leak. Both hold
//! here.
//!
//! What path trips the overrun is a DataFusion-version detail the ticket calls
//! out explicitly. In datafusion 54.1.0 a `NestedLoopJoinExec` reserves its
//! dominant allocation -- the buffered build side -- through the *checked*
//! `try_grow` (datafusion-physical-plan-54.1.0
//! src/joins/nested_loop_join.rs:831), and so does ravel's own scan, per batch
//! (crate::scan, scan.rs:406). The bare, infallible `grow` calls in that same
//! join (nested_loop_join.rs:1592/975/1647) are narrow force-progress and
//! bitmap paths, each guarded so it is never the *gating* allocation before a
//! `try_grow` has already refused. The upshot is that a join memory overrun
//! surfaces here as the pool's `try_grow` `ResourcesExhausted`, not as the
//! `CeilingBreach` that `grow` trips. The breach mechanism proper -- `grow`
//! pushing a budget over its ceiling and tripping the flag the stream then
//! aborts on -- is covered deterministically at the pool level by the unit
//! tests in `crate::memory` (which drive `grow` directly, exactly as
//! `tests/audit_sql3_exec.rs`'s `sql3_f01` does). This test guards the
//! surrounding contract that both the checked path and the breach path share:
//! a join memory overrun aborts the executor with `ResourcesExhausted` and
//! leaks nothing.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod util;

use std::sync::Arc;

use ravel_object_store::memory::MemoryStore;
use ravel_sql::{SqlConfig, SqlError};
use util::{Fixture, SegSpec, SeriesSpec, request, tenant_id};

#[tokio::test]
async fn a_join_that_overruns_the_query_ceiling_aborts_and_frees_the_tenant_budget() {
    let tenant = tenant_id("acme");
    // Enough samples that the deduped left side the join buffers spans more
    // than one scan batch, so the join's build-side reservation overruns the
    // per-query ceiling below during buffering rather than the query merely
    // running slowly.
    let specs = vec![SegSpec::new(
        10,
        1,
        1,
        vec![SeriesSpec::new(
            "m",
            (0..12_000).map(|i| (i, i as f64)).collect(),
        )],
    )];
    // A per-query byte ceiling that admits the scan's first batch but not the
    // whole buffered build side, so the join overruns it while buffering.
    let config = SqlConfig {
        engine: util::engine_config(),
        max_query_bytes: 768 * 1024,
        parallel_final_aggregation: false,
        skip_partial_aggregation: true,
        // ADR-0774's rewrite is a `SqlConfig` field now; keep this
        // fixture's plan shape as it was by not installing the rule.
        late_materialization_extra_columns: None,
    };
    let fixture = Fixture::build(
        Arc::new(MemoryStore::new()),
        &[(&tenant, &specs)],
        config,
        // A generous tenant ceiling so the per-query ceiling is the one that
        // trips, isolating the per-query overrun.
        1 << 40,
    )
    .await;

    // A non-equi self-join forces NestedLoopJoinExec (no equi key for a hash
    // join), the operator whose build side buffers the whole left input.
    let sql = "SELECT a.value FROM samples a JOIN samples b ON a.value > b.value";
    let err = fixture
        .executor
        .execute(tenant.hash(), &request(sql))
        .await
        .expect_err("the join must overrun the per-query ceiling");

    assert!(
        matches!(err, SqlError::ResourcesExhausted(_)),
        "a join memory overrun must surface as ResourcesExhausted, not a \
         completed result or a different error; got {err:?}"
    );

    // The abort dropped the stream, so every MemoryReservation shrank back
    // through the pool into the tenant accountant: no leak on the abort path,
    // which is exactly the drop path the breach abort also relies on.
    let tenant_budget = fixture.executor.tenant_budget(tenant.hash());
    assert_eq!(
        tenant_budget.reserved(),
        0,
        "the aborted query must return the tenant's reserved bytes to zero"
    );
}
