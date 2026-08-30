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
    /// The `(consumer_name, can_spill)` of every rejected `try_grow`, in the
    /// order they failed. A query executes across multiple concurrent
    /// partitions, so the LAST failure is not deterministic across runs (a
    /// partial aggregate's own refusal can land after the final aggregate's
    /// during teardown) -- recording every failure and checking whether a
    /// specific consumer/spillability pair is PRESENT among them, rather than
    /// asserting on whichever happened to be last, is what makes this
    /// ordering-independent. Interior mutability so the immutable `&self`
    /// pool methods can record into it while the query holds the pool as
    /// `Arc<dyn ..>`.
    failed: Mutex<Vec<(String, bool)>>,
}

impl RecordingPool {
    fn new(inner: Arc<dyn MemoryPool>) -> Arc<Self> {
        Arc::new(RecordingPool {
            inner,
            failed: Mutex::new(Vec::new()),
        })
    }

    /// Every `(consumer_name, can_spill)` pair recorded from a rejected
    /// `try_grow`, in failure order.
    fn failed(&self) -> Vec<(String, bool)> {
        self.failed.lock().expect("lock").clone()
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
            self.failed
                .lock()
                .expect("lock")
                .push((consumer.name().to_string(), consumer.can_spill()));
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
        skip_partial_aggregation: true,
        // ADR-0774's rewrite is a `SqlConfig` field now; keep this
        // fixture's plan shape as it was by not installing the rule.
        late_materialization_extra_columns: None,
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

/// ADR-0102 decision 3: with the disk manager disabled, a high-cardinality
/// final aggregation whose group state exceeds the query memory budget fails
/// as `SqlError::ResourcesExhausted`, the typed error the executor maps every
/// `DataFusionError::ResourcesExhausted` into (`executor.rs:1272/1286/1358`),
/// rather than succeeding via a silent spill to local disk. This is the
/// executor-driven half of the coverage: it drives the real
/// `SqlExecutor::execute` path and checks the error TYPE. The sibling test
/// below, `a_high_cardinality_aggregation_is_refused_by_the_aggregate_not_the_scan`,
/// checks WHICH consumer produced it -- the two are deliberately separate
/// tests, not one test doing both, since replacing this type-level check with
/// the consumer-identity check (an earlier revision of this file did exactly
/// that) silently dropped the type assertion ADR-0102 decision 3 requires.
///
/// This test does NOT assert on the error's message text: both the aggregate
/// and the `RsegScanExec` feeding it draw on the same query pool and both
/// surface the identical `"query memory pool exhausted: ..."` prefix when
/// their `try_grow` is refused, so a message-prefix assertion cannot tell
/// which one actually tripped (see the sibling test's docstring for the
/// mechanism that does).
#[tokio::test]
async fn a_high_cardinality_aggregation_over_budget_is_resources_exhausted() {
    let tenant = tenant_id("acme");
    let specs = single_series_specs(300_000);

    let config = SqlConfig {
        engine: util::engine_config(),
        max_query_bytes: 16 * 1024 * 1024,
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
        1 << 40,
    )
    .await;

    let err = fixture
        .executor
        .execute(
            tenant.hash(),
            &request("SELECT ts, count(*) AS n FROM samples GROUP BY ts"),
        )
        .await
        .expect_err("a high-cardinality aggregation over budget must trip the pool");

    assert!(
        matches!(err, SqlError::ResourcesExhausted(_)),
        "aggregation over budget must fail typed rather than spill; got {err:?}"
    );
}

/// ADR-0102 decision 3, the WHICH-consumer half: with the disk manager
/// disabled, the final aggregate itself -- not the scan feeding it -- is the
/// consumer whose `try_grow` is refused, registered non-spillable
/// (`can_spill == false`).
///
/// Why not the message string: both the aggregate's reservation and the
/// feeding `RsegScanExec` reservation draw on the same query pool, and BOTH
/// surface the identical `"query memory pool exhausted: ..."` prefix when a
/// `try_grow` is refused -- `starts_with("query memory pool exhausted")`
/// holds regardless of which consumer actually tripped, so it cannot
/// discriminate the fixed tree from the broken one. This test wraps the real
/// query pool in a [`RecordingPool`] and asserts a `(GroupedHashAggregateStream,
/// can_spill == false)` entry is PRESENT among every consumer whose
/// `try_grow` was refused (not that it was the LAST one refused -- the query
/// executes across concurrent partitions, so failure order across a partial
/// aggregate's own reservation and the final aggregate's is not guaranteed
/// stable run to run; presence, not position, is what's load-bearing here).
///
/// Prove-the-test: with the `with_disk_manager_builder(...Disabled)` line
/// removed from `build_session` (reverting to the pre-#456 tree), this exact
/// query was observed to refuse `try_grow` for `RsegScanExec[0]`
/// (`can_spill=false`) -- the scan itself, not the aggregate -- because
/// spilling lets every `GroupedHashAggregateStream` reservation in this run
/// succeed (registered `can_spill=true`, confirmed by inspecting every
/// recorded failure), so the non-spillable scan reservation is what exhausts
/// the pool instead. No `(GroupedHashAggregateStream*, can_spill=false)` entry
/// exists anywhere in that failure list, so the presence assertion below
/// panics with the recorded failures dumped, e.g.:
///
/// ```text
/// thread 'a_high_cardinality_aggregation_is_refused_by_the_aggregate_not_the_scan'
/// panicked at crates/ravel-sql/tests/memory_ceiling_and_budget_errors.rs:
/// expected a (GroupedHashAggregateStream*, can_spill=false) entry among the
/// refused try_grow calls; got [("GroupedHashAggregateStream[0] (count(1))", true), ..., ("RsegScanExec[0]", false)]
/// ```
///
/// Verified by removing the line, observing the failure list above (no
/// non-spillable aggregate entry, only spillable aggregate entries plus the
/// non-spillable scan), then restoring the line and confirming green.
#[tokio::test]
async fn a_high_cardinality_aggregation_is_refused_by_the_aggregate_not_the_scan() {
    let tenant = tenant_id("acme");
    let specs = single_series_specs(300_000);

    let config = SqlConfig {
        engine: util::engine_config(),
        max_query_bytes: 16 * 1024 * 1024,
        parallel_final_aggregation: false,
        // Off deliberately, and it is the only test in this file that turns it
        // off (issue #680). This case's subject is the aggregate operator's own
        // non-spillable reservation being what the pool refuses. With the
        // tightened skip-partial-aggregation probe on (the shipped default),
        // `GROUP BY ts` gives every row its own group, the probe fires after
        // 8192 rows, and the partial stage stops growing -- so the aggregate
        // never reaches the ceiling and the refusal comes from
        // `RepartitionExec`/`RsegScanExec` instead. The query still fails typed,
        // which is what ADR-0102 decision 3 requires and what
        // `a_high_cardinality_aggregation_over_budget_is_resources_exhausted`
        // asserts; only this test's operator-identity claim depends on the
        // aggregate being the one that runs out.
        skip_partial_aggregation: false,
        // ADR-0774's rewrite is a `SqlConfig` field now; keep this
        // fixture's plan shape as it was by not installing the rule.
        late_materialization_extra_columns: None,
    };
    let fixture = Fixture::build(
        Arc::new(MemoryStore::new()),
        &[(&tenant, &specs)],
        config,
        1 << 40,
    )
    .await;

    // Build the query's session directly so the real per-query pool (the same
    // `TenantDelegatingPool` the executor builds internally) can be wrapped in
    // a `RecordingPool` before it is installed on the session. The executor
    // builds its pool with no injection seam, and adding one would be
    // test-only production surface; `build_session`/`RavelTableProvider` are
    // the same public constructors the executor calls, configured the same
    // way (same disabled disk manager, `parallel_final_aggregation: false` so
    // the final aggregate stays single-partition) -- close to, not a
    // byte-for-byte reproduction of, the executor's own construction path
    // (notably: two independent `QueryAccounting` handles here instead of one
    // shared clone, and no `CeilingBreach`/abort-seam wiring, neither of which
    // this test's assertions depend on).
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

    // The discriminating assertion: PRESENCE of a non-spillable aggregate
    // failure among every refused try_grow, not the identity of the LAST one
    // (see the docstring for why position isn't stable across runs).
    let failed = recording.failed();
    let has_nonspillable_aggregate = failed
        .iter()
        .any(|(name, can_spill)| name.starts_with("GroupedHashAggregateStream") && !can_spill);
    assert!(
        has_nonspillable_aggregate,
        "expected a (GroupedHashAggregateStream*, can_spill=false) entry among \
         the refused try_grow calls; got {failed:?}"
    );
}

/// ADR-0102 decision 3: with the disk manager disabled, a large `ORDER BY` that
/// overruns the budget through DataFusion's external sort also fails as
/// `SqlError::ResourcesExhausted`. Its raw message originates in
/// `DiskManager::create_tmp_file`
/// (`"Memory Exhausted while Sorting (DiskManager is disabled)"`), propagated as
/// `DataFusionError::ResourcesExhausted` -- the same pass-through shape issue
/// #740 (finding 1) covers for the aggregation/exchange path: it names the
/// operator that asked to spill, not the consumers that filled the pool, and
/// carries no byte figures. `execution_error` re-attributes it the same way,
/// via `SqlError::resources_exhausted_reattributed` keying off
/// `MSG_SPILL_DISABLED_MARKER` ("DiskManager is disabled") in the raw message,
/// so the client-visible text below no longer contains that literal DataFusion
/// wording; it names the pool's own occupancy and limit instead.
///
/// `ORDER BY value` forces a real external sort: the pipeline output is already
/// ordered by `(series_id, ts)`, so ordering by `ts` alone could be elided,
/// whereas `value` is unrelated to that order.
///
/// Prove-the-test: with the `with_disk_manager_builder(...Disabled)` line
/// removed, the sorter's spill attempt reaches the default `OsTmpDirectory`
/// disk manager and the overrun instead surfaces as the pool's own
/// `"query memory pool exhausted: ..."` `try_grow` refusal, which does not
/// contain `MSG_SPILL_DISABLED_MARKER` and so is passed through unchanged --
/// the `contains("spill is disabled")` assertion below fails. Verified by
/// removing the line and observing that exact message.
#[tokio::test]
async fn a_large_order_by_over_budget_is_resources_exhausted() {
    let tenant = tenant_id("acme");
    let specs = single_series_specs(300_000);

    let config = SqlConfig {
        engine: util::engine_config(),
        max_query_bytes: 16 * 1024 * 1024,
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
    // The sort path's raw message originates from the disabled disk manager, a
    // different source than the aggregation path's pool `try_grow` text; both
    // are re-attributed to the same client-visible shape by `execution_error`
    // (issue #740), so both assert the reattributed wording, not DataFusion's
    // own text.
    let SqlError::ResourcesExhausted(msg) = &err else {
        unreachable!("asserted above");
    };
    assert!(
        msg.contains("spill is disabled"),
        "the sort path's error must be re-attributed as a disabled-spill \
         refusal, not passed through with DataFusion's own wording; got {msg:?}"
    );
    assert!(
        !msg.contains("DiskManager is disabled") && !msg.contains("Sorting"),
        "DataFusion's own operator wording must not survive re-attribution; \
         got {msg:?}"
    );
    // Nothing else in this single-partition pipeline holds a reservation on
    // this pool before the sort's own first (and only) grow attempt, which
    // already exceeds the whole per-query budget in one call -- so the
    // pool's occupancy at the moment of refusal is 0, not a partial fill.
    // Pinned exact per this repo's number-pinning rule; a DataFusion version
    // that changes the external sorter's batching would change this figure
    // and this assertion would catch it.
    assert!(
        msg.contains("0 of 16777216 bytes reserved"),
        "the reattributed message must name the pool's exact occupancy and \
         the query's configured limit (16777216 = max_query_bytes above); \
         got {msg:?}"
    );
}
