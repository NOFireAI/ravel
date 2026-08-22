//! Memory-ceiling enforcement and budget-error typing for the SQL executor.
//!
//! The grow-path probe is `#[ignore]`d: it documents the desired ceiling
//! behavior and is not part of the gate run.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod util;

use std::fmt;
use std::sync::{Arc, Mutex};

use datafusion::error::Result as DFResult;
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use ravel_object_store::memory::MemoryStore;
use ravel_sql::{
    CeilingBreach, RavelTableProvider, SessionTable, SqlConfig, SqlError, TenantDelegatingPool,
    TenantMemoryAccountant, build_session,
};
use ravel_types::accounting::QueryAccounting;
use util::{Fixture, SegSpec, SeriesSpec, request, tenant_id};

/// A `MemoryPool` decorator that delegates every call to an inner pool and, on
/// each `try_grow` the inner pool *rejects*, records the failing reservation's
/// `MemoryConsumer` name and `can_spill` flag.
///
/// The budget-error message alone cannot say which operator tripped: an
/// `RsegScanExec` scan reservation and the final `GroupedHashAggregateStream`
/// reservation both surface the identical `"query memory pool exhausted: ..."`
/// prefix when their `try_grow` is refused. Recording the failing consumer's
/// identity is the signal that discriminates the two failure sites, which the
/// string prefix cannot.
#[derive(Debug)]
struct RecordingPool {
    inner: Arc<dyn MemoryPool>,
    /// The consumer name and `can_spill` flag of the most recent rejected
    /// `try_grow`. Interior mutability so the immutable `&self` pool methods
    /// can record into it while the query holds the pool as `Arc<dyn ..>`.
    last_failed: Mutex<Option<(String, bool)>>,
}

impl RecordingPool {
    fn new(inner: Arc<dyn MemoryPool>) -> Arc<Self> {
        Arc::new(RecordingPool {
            inner,
            last_failed: Mutex::new(None),
        })
    }

    /// The `(consumer_name, can_spill)` of the last rejected `try_grow`, or
    /// `None` if no `try_grow` was refused.
    fn last_failed(&self) -> Option<(String, bool)> {
        self.last_failed.lock().expect("lock").clone()
    }
}

impl fmt::Display for RecordingPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RecordingPool({})", self.inner.name())
    }
}

impl MemoryPool for RecordingPool {
    fn name(&self) -> &str {
        "RecordingPool"
    }

    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> DFResult<()> {
        let result = self.inner.try_grow(reservation, additional);
        if result.is_err() {
            let consumer = reservation.consumer();
            *self.last_failed.lock().expect("lock") =
                Some((consumer.name().to_string(), consumer.can_spill()));
        }
        result
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

/// `MemoryPool::grow` is infallible and checks no ceiling, so a reservation
/// can grow past both the per-query and per-tenant limits with no error, while
/// `try_grow` of the same size refuses. The configured budget is therefore not
/// a hard cap on the `grow` path (memory.rs:196-214), which is the path the
/// nested-loop and sort-merge join operators use.
///
/// This drives the pool directly, no query engine needed, so it is
/// deterministic and isolates the mechanism from operator-selection details.
/// The grow path must not push `reserved()` past the 1024-byte ceiling.
#[test]
#[ignore = "grow-path memory-ceiling probe; not wired as a gate"]
fn grow_must_not_bypass_the_query_and_tenant_ceiling() {
    let tenant = TenantMemoryAccountant::new(1024);
    // The breach flag is a separate signal; this probe ignores it and asserts
    // the raw counters do not overshoot the ceiling on the grow path.
    let pool: Arc<dyn MemoryPool> = Arc::new(TenantDelegatingPool::new(
        1024,
        Arc::clone(&tenant),
        CeilingBreach::new(),
        QueryAccounting::new(),
    ));

    let res = MemoryConsumer::new("grow-ceiling-probe").register(&pool);

    // The checked path enforces the ceiling: 4096 > 1024 is refused and
    // reserves nothing. This half already holds today.
    assert!(
        res.try_grow(4096).is_err(),
        "try_grow beyond the ceiling must fail"
    );
    assert_eq!(pool.reserved(), 0, "a failed try_grow reserves nothing");
    assert_eq!(tenant.reserved(), 0);

    // The infallible path is what joins reach. It must not exceed the ceiling.
    // A naive `grow` adds 4096 unconditionally to both budgets.
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

/// A memory-budget exhaustion raised deep in a plan (a sort or an aggregate)
/// is a native `DataFusionError::ResourcesExhausted`, but by the time it
/// reaches `execution_error` it is wrapped in `Context`/`Shared`, which
/// `take_sql_error` (executor.rs:366) re-wraps rather than unwraps for a
/// *native* error (it only recovers a nested `SqlError`, and only strips the
/// `ArrowError` layer). The top-level `ResourcesExhausted` match therefore
/// misses it and it degrades to `SqlError::Execution`, whose `client_message`
/// is the fixed, redacted `MSG_EXECUTION` -- so the client is never told they
/// hit the memory budget and cannot narrow the query.
///
/// A projection-only scan happens to recover correctly (its error crosses
/// `SortPreservingMergeExec`, wrapped in `ArrowError`, which `take_sql_error`
/// *does* strip). The two common non-trivial shapes below must keep their
/// `ResourcesExhausted` type.
#[tokio::test]
async fn a_sort_or_aggregate_budget_error_keeps_its_type() {
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
        parallel_final_aggregation: false,
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

/// One segment of `rows` samples in a single series (ts == value == i), so the
/// scan runs on one partition (segment count caps `target_partitions`) and its
/// per-partition live set stays small, while every ts is a distinct value the
/// aggregate/sort above must hold. Sized to overrun a small query budget.
fn single_series_specs(rows: i64) -> Vec<SegSpec> {
    vec![SegSpec::new(
        10,
        1,
        0,
        vec![SeriesSpec::new(
            "m",
            (0..rows).map(|i| (i, i as f64)).collect(),
        )],
    )]
}

/// ADR-0102 decision 3: with the disk manager disabled in `build_session`, a
/// high-cardinality final aggregation whose group state exceeds the query
/// memory budget fails because the aggregate's *own* reservation cannot grow,
/// instead of routing around the budget through DataFusion's spill path.
///
/// With the disk manager disabled, `GroupedHashAggregateStream` is built in
/// `ReportError` mode: its `MemoryConsumer` is registered with
/// `can_spill == false`, and when its reservation's `try_grow` is refused it
/// propagates the error directly rather than spilling to disk.
///
/// Why the `can_spill` flag, not the message string: a message-prefix
/// assertion cannot distinguish the fixed tree from the broken one. The
/// aggregate's reservation and the feeding `RsegScanExec` reservation both
/// draw on the same query pool, and *both* surface the identical `"query
/// memory pool exhausted: ..."` prefix when a `try_grow` is refused -- so
/// `starts_with("query memory pool exhausted")` holds whether spilling was
/// available or not. This test therefore wraps the real query pool in a
/// [`RecordingPool`] and asserts on the identity of the consumer whose
/// `try_grow` was refused last: it must be the aggregate
/// (`GroupedHashAggregateStream`), and -- the load-bearing half -- that
/// consumer must have been registered with `can_spill == false`. Only the
/// disk-manager-disabled build registers the aggregate non-spillable.
///
/// Prove-the-test: with the `with_disk_manager_builder(...Disabled)` line
/// removed from `build_session`, the aggregate is instead built in spill mode,
/// so its `MemoryConsumer` is registered with `can_spill == true` and it spills
/// rather than failing hard. The consumer *name* is unchanged (the aggregate is
/// still the last consumer whose `try_grow` the pool refuses at this budget),
/// so the name check is not the discriminator -- the `can_spill` flag is. The
/// recorded pair becomes `("GroupedHashAggregateStream[0] (count(1))", true)`,
/// so the `!can_spill` assertion below fails. Verified by removing the line and
/// observing exactly:
///
/// ```text
/// thread 'a_high_cardinality_aggregation_over_budget_is_resources_exhausted'
/// panicked at crates/ravel-sql/tests/memory_ceiling_and_budget_errors.rs:
/// the aggregate must be built non-spillable (ReportError mode) so budget
/// exhaustion is a hard error, not a spill; consumer
/// "GroupedHashAggregateStream[0] (count(1))" had can_spill=true
/// ```
#[tokio::test]
async fn a_high_cardinality_aggregation_over_budget_is_resources_exhausted() {
    let tenant = tenant_id("acme");
    let specs = single_series_specs(300_000);

    let config = SqlConfig {
        engine: util::engine_config(),
        max_query_bytes: 16 * 1024 * 1024,
        parallel_final_aggregation: false,
    };
    let fixture = Fixture::build(
        Arc::new(MemoryStore::new()),
        &[(&tenant, &specs)],
        config,
        1 << 40,
    )
    .await;

    // Build the query's session directly so the real per-query pool (the same
    // `TenantDelegatingPool` the executor builds in `plan_pinned_with`) can be
    // wrapped in a `RecordingPool` before it is installed on the session. The
    // executor builds its pool internally with no injection seam, and adding
    // one would be test-only production surface; the public `build_session` /
    // `RavelTableProvider` API reproduces the executor's construction path
    // exactly (same disabled disk manager, same single-partition final
    // aggregate) while letting the test observe the pool. The executor's
    // mapping of this failure to `SqlError::ResourcesExhausted` is covered by
    // `a_sort_or_aggregate_budget_error_keeps_its_type`.
    let tenant_accountant = TenantMemoryAccountant::new(1 << 40);
    let (inner_pool, _breach) = config.query_pool(tenant_accountant, QueryAccounting::new());
    let recording = RecordingPool::new(inner_pool);
    let pool: Arc<dyn MemoryPool> = Arc::clone(&recording) as Arc<dyn MemoryPool>;

    let snapshot = fixture.snapshot(&tenant).await;
    let provider = RavelTableProvider::new(
        snapshot,
        tenant.hash(),
        fixture.fetcher.clone(),
        config,
        QueryAccounting::new(),
    );
    let ctx = build_session(
        &config,
        pool,
        SessionTable::Metrics(Arc::new(provider)),
        false,
    )
    .expect("metrics session builds");

    let frame = ctx
        .sql("SELECT ts, count(*) AS n FROM samples GROUP BY ts")
        .await
        .expect("high-cardinality aggregation plans");
    let result = frame.collect().await;
    assert!(
        result.is_err(),
        "a high-cardinality aggregation over budget must trip the pool"
    );

    // The discriminating assertion: with spilling disabled, the aggregate
    // itself is the consumer whose `try_grow` is refused, and it is registered
    // as non-spillable. A scan tripping, or a spillable aggregate, would record
    // a different consumer or `can_spill == true` and fail here.
    let (consumer, can_spill) = recording
        .last_failed()
        .expect("some try_grow must have been refused");
    assert!(
        consumer.starts_with("GroupedHashAggregateStream"),
        "the aggregate itself, not the scan, must be the consumer that trips; \
         last refused try_grow was for {consumer:?} (can_spill={can_spill})"
    );
    assert!(
        !can_spill,
        "the aggregate must be built non-spillable (ReportError mode) so budget \
         exhaustion is a hard error, not a spill; consumer {consumer:?} had \
         can_spill={can_spill}"
    );
}

/// ADR-0102 decision 3: with the disk manager disabled, a large `ORDER BY` that
/// overruns the budget through DataFusion's external sort also fails as
/// `SqlError::ResourcesExhausted` -- but its message originates in
/// `DiskManager::create_tmp_file`
/// (`"Memory Exhausted while Sorting (DiskManager is disabled)"`), propagated as
/// `DataFusionError::ResourcesExhausted` and mapped to the same
/// `SqlError::ResourcesExhausted` variant the aggregation path uses. Same typed
/// variant, distinct message from the aggregation case (which is the pool's
/// `try_grow` text) -- confirmed here by matching the disk-manager wording.
///
/// `ORDER BY value` forces a real external sort: the pipeline output is already
/// ordered by `(series_id, ts)`, so ordering by `ts` alone could be elided,
/// whereas `value` is unrelated to that order.
///
/// Prove-the-test: with the `with_disk_manager_builder(...Disabled)` line
/// removed, the sorter's spill attempt reaches the default `OsTmpDirectory`
/// disk manager and the overrun instead surfaces as the pool's
/// `"query memory pool exhausted: ..."` error (no disk-manager wording), so the
/// `contains("DiskManager is disabled")` assertion below fails. Verified by
/// removing the line and observing that exact message.
#[tokio::test]
async fn a_large_order_by_over_budget_is_resources_exhausted() {
    let tenant = tenant_id("acme");
    let specs = single_series_specs(300_000);

    let config = SqlConfig {
        engine: util::engine_config(),
        max_query_bytes: 16 * 1024 * 1024,
        parallel_final_aggregation: false,
    };
    let fixture = Fixture::build(
        Arc::new(MemoryStore::new()),
        &[(&tenant, &specs)],
        config,
        1 << 40,
    )
    .await;

    let err = fixture
        .executor
        .execute(
            tenant.hash(),
            &request("SELECT ts, value FROM samples ORDER BY value"),
        )
        .await
        .expect_err("a large ORDER BY over budget must trip the disabled disk manager");

    assert!(
        matches!(err, SqlError::ResourcesExhausted(_)),
        "ORDER BY over budget must fail typed rather than spill; got {err:?}"
    );
    // The sort path's message originates from the disabled disk manager, a
    // different source than the aggregation path's pool `try_grow`; both are the
    // same typed variant, and the distinct message is expected.
    let SqlError::ResourcesExhausted(msg) = &err else {
        unreachable!("asserted above");
    };
    assert!(
        msg.contains("DiskManager is disabled"),
        "the sort path's error must name the disabled disk manager; got {msg:?}"
    );
}
