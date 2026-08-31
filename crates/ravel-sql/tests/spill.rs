//! Bounded ephemeral spill, end to end through `SqlExecutor::execute`
//! (ADR-0954).
//!
//! Every test here drives the real resolve/plan/execute path, not the
//! `SpillScratch`/`SpillCounts` units in `crate::spill` (those are exercised in
//! `src/spill.rs`). What is proven here is the executor's behavior against the
//! ADR's normative requirements:
//!
//! - an eligible integer aggregation over the memory budget completes by
//!   spilling and returns the byte-identical result of the same query run under
//!   a budget large enough not to spill (requirements 1, 4);
//! - a spill-enabled plan carries no `RepartitionExec`: the eligibility
//!   predicate classifies logical nodes, an enabled disk manager is a
//!   session-wide permission, and that exchange is the node between the two
//!   scopes (it spills, and no logical predicate ever sees it);
//! - the per-query scratch quota exceeded is a typed `SpillBudgetExhausted`,
//!   never a partial result (requirements 2, 5), with the budget split between
//!   the scan and the aggregate measured so the aggregate is provably the
//!   consumer that reached its spill threshold;
//! - a missing spill directory is a typed `SpillUnavailable` (requirements 3,
//!   5);
//! - an ineligible plan (order-dependent float aggregation) over budget is
//!   still refused and never creates a spill file (requirement 4, fail closed);
//! - spill off (the unset-config default) reproduces the pre-ADR refusal, its
//!   `DiskManager is disabled` re-attribution included (requirement 9);
//! - scratch is cleaned up on completion, on a typed error, and on a cancelled
//!   stream (requirement 7);
//! - the spilling aggregation's pool-accounted peak stays within the configured
//!   cap (requirement 1, the implementation-constraint measurement).
//!
//! # Where each test drives the spill from
//!
//! The eligibility, scratch-quota, unavailable-directory, cleanup, and spill-off
//! cases run through the real `SqlExecutor::execute` path over a scanned
//! `samples` table -- that is where the config plumbing, eligibility gate,
//! scratch lifecycle, and error mapping live.
//!
//! The byte-identical and peak-memory cases instead run the grouped aggregate
//! over an in-memory source ([`run_grouped_count`]). That is deliberate, and it
//! is the one place the executor path cannot demonstrate requirement 1:
//! Ravel's `RsegScanExec` buffers its whole decoded partition as a single
//! non-spillable reservation whose size, for the 1:1 grouping this ADR targets
//! (q33: distinct groups approximately equal to rows), equals or exceeds the
//! aggregate's own state. So over a freshly scanned table the scan -- not the
//! aggregate -- is the binding non-spillable consumer, the two grow
//! concurrently in one shared per-query pool, and enabling aggregate spill
//! cannot bring such a query within a smaller budget: the scan reservation
//! collides at the pool boundary before (or regardless of) the aggregate's
//! spill. Isolating the aggregate on a source that reserves no pool memory is
//! what lets these two tests show the property requirement 1 is actually about
//! -- that spilling the aggregate yields the exact in-memory result within the
//! cap. This scan-domination limitation is a finding on ADR-0954, reported with
//! this change.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod util;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::StreamExt;
use ravel_object_store::memory::MemoryStore;
use ravel_sql::{
    CeilingBreach, SpillConfig, SqlConfig, SqlError, TenantDelegatingPool, TenantMemoryAccountant,
};
use ravel_types::accounting::QueryAccounting;
use util::{Fixture, SegSpec, SeriesSpec, request, tenant_id};

/// A per-query memory budget the high-cardinality aggregation below overruns,
/// used by the executor-path tests where the plan is refused (spill off, or an
/// ineligible plan) or where the budget is not what the case is about at all.
/// 16 MiB is the same ceiling the ADR-0102 sibling tests in
/// `memory_ceiling_and_budget_errors.rs` use to trip the same query shape, so
/// the "over budget" precondition is shared with the pre-spill coverage rather
/// than re-tuned here.
///
/// It is deliberately NOT the budget of the scratch-quota case: the scan's own
/// share of this fixture exceeds 16 MiB, so under this budget no statement can
/// be made about which consumer reached its limit first. See
/// [`SPLIT_QUERY_BYTES`].
const TIGHT_QUERY_BYTES: usize = 16 * 1024 * 1024;

/// A per-query scratch quota large enough that the eligible aggregation's whole
/// spilled working set fits, so the spill completes rather than tripping the
/// quota. Sized well above the group state of the fixture below (a few MiB of
/// Arrow IPC), and far under any real disk.
const AMPLE_SCRATCH_BYTES: u64 = 512 * 1024 * 1024;

/// A per-query scratch quota far too small to hold the spilled group state, so
/// the disk manager refuses a spill write and the query fails
/// `SpillBudgetExhausted`. 64 KiB is below one spilled batch of the fixture.
const TINY_SCRATCH_BYTES: u64 = 64 * 1024;

/// Documented overhead the spilling query's pool-accounted peak reservation is
/// allowed above the configured `max_query_bytes` cap. Zero: the pool's
/// `try_grow` is a hard cap (ADR-0954 requirement 1), and the grouped-hash
/// spill path charges its transient sort headroom to the same pool after
/// freeing the group state, so no pool-accounted reservation ever exceeds the
/// configured ceiling. A nonzero value here would mean the hard cap leaked,
/// which is the property this bound exists to catch.
const SPILL_POOL_PEAK_OVERHEAD_BYTES: u64 = 0;

/// Rows per RSEG segment in the fixture. Deliberately small: `RsegScanExec`
/// reserves a whole decoded segment as one non-spillable block, so a single
/// large segment would dominate the tight pool and starve the aggregate (the
/// consumer whose spill this suite is about). Many small segments keep the
/// scan's live reservation small while the aggregation cardinality stays high,
/// so the eligible aggregate is the binding consumer and spills. The spill
/// fixtures also scan at `fetch_concurrency = 1` (see [`spill_config`]) so only
/// one segment decodes at a time.
const SEG_ROWS: i64 = 8_000;

/// Two metric series whose event timestamps overlap, spread across many small
/// segments (see [`SEG_ROWS`]). `COUNT(*) GROUP BY ts` then yields groups with
/// two distinct counts (2 where both series have the ts, i.e. `ts < short_rows`,
/// and 1 where only the longer series does) rather than a uniform all-ones
/// column, so the byte-identical comparison exercises the spilled merge and not
/// just a constant. `long_rows` distinct group keys total, the aggregation
/// cardinality that overruns [`TIGHT_QUERY_BYTES`].
fn overlapping_series_specs(long_rows: i64, short_rows: i64) -> Vec<SegSpec> {
    let mut segs = Vec::new();
    let mut start = 0i64;
    let mut idx = 0usize;
    while start < long_rows {
        let end = (start + SEG_ROWS).min(long_rows);
        let mut series = vec![SeriesSpec::new(
            "a",
            (start..end).map(|i| (i, i as f64)).collect(),
        )];
        if start < short_rows {
            let b_end = end.min(short_rows);
            series.push(SeriesSpec::new(
                "b",
                (start..b_end).map(|i| (i, i as f64)).collect(),
            ));
        }
        // Distinct provenance per segment. The rows never collide across
        // segments (each covers a disjoint ts slice), so any distinct stamp
        // gives a well-defined dedup order.
        segs.push(SegSpec::new(10 + idx as i64, 1, idx as u64, series));
        start = end;
        idx += 1;
    }
    segs
}

/// The shape both spilling tests use: `long_rows` distinct `ts` groups, of
/// which the first `short_rows` have count 2 and the rest count 1.
const LONG_ROWS: i64 = 300_000;
const SHORT_ROWS: i64 = 150_000;

/// The eligible statement under test: integer `COUNT(*)` grouped by the
/// non-float `ts` key. No `ORDER BY`: a `Sort` node is deliberately outside the
/// spill-eligible plan shape (ADR-0954 requirement 4), so the results come back
/// unordered and the comparison sorts both sides itself.
const SPILLING_SQL: &str = "SELECT ts, count(*) AS n FROM samples GROUP BY ts";

/// The ineligible statement: `SUM` over the `Float64` `value` column, whose
/// fold is order-dependent (ADR-0022/0024). It must be refused, never spilled.
const INELIGIBLE_SQL: &str = "SELECT ts, sum(value) AS s FROM samples GROUP BY ts";

fn spill_config(spill_dir: PathBuf, scratch_bytes: u64, query_bytes: usize) -> SqlConfig {
    // One scan partition, so at most one segment decodes at a time and the
    // non-spillable scan reservation stays small relative to the aggregate
    // (see SEG_ROWS). This is a memory-shape choice, not a correctness one: the
    // aggregate computes the same groups over the same rows at any partitioning.
    let mut engine = util::engine_config();
    engine.fetch_concurrency = 1;
    SqlConfig {
        engine,
        max_query_bytes: query_bytes,
        parallel_final_aggregation: false,
        skip_partial_aggregation: true,
        late_materialization_extra_columns: None,
        spill: Some(SpillConfig {
            dir: spill_dir,
            max_bytes: scratch_bytes,
        }),
    }
}

/// What one run of the in-memory grouped aggregation observed: its `(key,
/// count)` rows (sorted), how many spill files it wrote, and its pool-accounted
/// peak reservation.
struct AggRun {
    rows: Vec<(String, i64)>,
    spill_files: u64,
    peak_pool_bytes: u64,
}

/// Run `SELECT k, count(*) GROUP BY k` over an in-memory table of `keys`,
/// against a `TenantDelegatingPool` capped at `query_bytes` with the disk
/// manager pointed at `scratch_dir` (quota `scratch_bytes`).
///
/// This drives the eligible-aggregate spill mechanism (ADR-0954 requirement 1)
/// in isolation: the source is a `MemTable`, whose scan reserves no pool memory,
/// so the grouped aggregate is the sole pool consumer and is the operator that
/// spills. That isolation is deliberate. Ravel's own `RsegScanExec` buffers its
/// whole decoded partition as one non-spillable reservation whose size, for the
/// 1:1 grouping this ADR targets, equals or exceeds the aggregate's, so over a
/// freshly scanned table the scan -- not the aggregate -- is the binding
/// consumer and enabling aggregate spill cannot shrink the query's footprint.
/// See the suite's module-level note. What this helper proves is the property
/// the ADR's byte-identical requirement is about: that when the aggregate IS the
/// binding consumer, spilling it yields the same result as running it in memory
/// and holds its pool-accounted peak under the cap.
///
/// The group key is a `Utf8` string, not an integer, on purpose: DataFusion 54's
/// `GroupValues` for a primitive key under-reports its real hashbrown allocation
/// (issue #740, documented in `crate::config`), so an integer key's reservation
/// stays far below its true footprint and never trips a realistic budget. A
/// string key's `GroupValues` accounts the stored bytes, so the reservation
/// tracks reality and the aggregate spills when it should.
async fn run_grouped_count(
    scratch_dir: &Path,
    keys: &[String],
    query_bytes: usize,
    scratch_bytes: u64,
) -> AggRun {
    let tenant = TenantMemoryAccountant::new(1 << 40);
    let breach = CeilingBreach::new();
    let accounting = QueryAccounting::new();
    let pool: Arc<dyn MemoryPool> = Arc::new(TenantDelegatingPool::new(
        query_bytes,
        tenant,
        breach,
        accounting.clone(),
    ));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool)
        .with_disk_manager_builder(
            DiskManagerBuilder::default()
                .with_mode(DiskManagerMode::Directories(vec![
                    scratch_dir.to_path_buf(),
                ]))
                .with_max_temp_directory_size(scratch_bytes),
        )
        .build_arc()
        .expect("runtime builds");
    // One partition, matching the executor path this helper stands in for
    // (`build_session` sets `target_partitions` from `fetch_concurrency`, which
    // the spill fixtures pin to 1). A default `SessionConfig` would split the
    // aggregate into `target_partitions` concurrent partial aggregates sharing
    // this one pool, so a spilling partition's `clear_shrink(0)` could not free
    // the pool room its sort headroom needs (the other partitions still hold
    // their group state) and the sort reservation fails under the cap. A single
    // partition makes the aggregate the sole pool consumer this helper's
    // contract (and ADR-0954 requirement 1's headroom reasoning) assumes.
    let config = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::new_with_config_rt(config, runtime);

    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Utf8, false)]));
    // Many small batches, not one giant one. DataFusion's grouped-hash aggregate
    // checks its spill trigger only at input-batch boundaries
    // (`emit_early_if_necessary`/`spill_previous_if_necessary`): fed a single
    // 4.5M-row batch it builds the whole hash table in one call and never gets
    // the chance to spill. Chunking into `batch_size`-row batches gives the
    // aggregate the checkpoints at which it reserves, overruns the cap, and
    // spills -- the behavior under test.
    let batches: Vec<RecordBatch> = keys
        .chunks(8192)
        .map(|chunk| {
            let column = datafusion::arrow::array::StringArray::from_iter_values(chunk.iter());
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(column)]).expect("batch builds")
        })
        .collect();
    let table = datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![batches])
        .expect("in-memory table builds");
    ctx.register_table("t", Arc::new(table))
        .expect("register in-memory table");

    let df = ctx
        .sql("SELECT k, count(*) AS n FROM t GROUP BY k")
        .await
        .expect("aggregation plans");
    let plan = df.create_physical_plan().await.expect("physical plan");
    let batches = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect("aggregation runs to completion");

    let mut rows = Vec::new();
    for batch in &batches {
        let k = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .expect("key column is Utf8");
        let n = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count column is Int64");
        for i in 0..batch.num_rows() {
            rows.push((k.value(i).to_string(), n.value(i)));
        }
    }
    rows.sort_unstable();

    AggRun {
        rows,
        spill_files: spill_file_count(&plan),
        peak_pool_bytes: accounting.snapshot().peak_intermediate_bytes,
    }
}

/// Sum the DataFusion spill-file count over `plan` and its descendants.
///
/// Spill is a typed `MetricValue::SpillCount`, not a named `Count`, so it must
/// be read through the `MetricsSet::spill_count()` accessor: `sum_by_name`
/// deliberately returns `false` for every spill variant and would report zero
/// even for a query that spilled (the same defect fixed in
/// `crate::spill::accumulate_spill_counts`).
fn spill_file_count(plan: &Arc<dyn ExecutionPlan>) -> u64 {
    let mut total = plan.metrics().and_then(|m| m.spill_count()).unwrap_or(0) as u64;
    for child in plan.children() {
        total += spill_file_count(child);
    }
    total
}

/// Keys for the in-memory aggregation: `groups` distinct string values, the
/// first `doubled` of them appearing twice, so `count(*) GROUP BY k` yields two
/// distinct counts (2 then 1) rather than a uniform column -- the byte-identical
/// comparison then exercises the spilled merge, not just a constant.
fn count_varying_keys(groups: i64, doubled: i64) -> Vec<String> {
    let mut keys: Vec<String> = (0..groups).map(|i| format!("key-{i:09}")).collect();
    keys.extend((0..doubled).map(|i| format!("key-{i:09}")));
    keys
}

/// The `ravel-spill-*` subdirectories a query created under `root`. Empty means
/// every scratch subdirectory was cleaned up (or none was ever created).
fn spill_subdirs(root: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(root)
        .expect("scratch root is readable")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("ravel-spill-"))
        })
        .collect()
}

/// Per-query memory budget for the in-memory spilling aggregation: small enough
/// that the integer groups overrun it and the aggregate spills. Unlike the
/// executor path there is no scan competing for this budget (the `MemTable`
/// source reserves nothing), so a tight cap cleanly forces the aggregate -- and
/// only the aggregate -- to spill. See [`run_grouped_count`].
const IN_MEMORY_SPILL_BYTES: usize = 8 * 1024 * 1024;
/// Distinct string group keys in the in-memory aggregation, and how many of
/// them appear twice (yielding count 2 vs 1). Sized so the group state of the
/// single sole-consumer aggregate (see [`run_grouped_count`]) far exceeds
/// [`IN_MEMORY_SPILL_BYTES`] and the aggregate must spill: measured against the
/// locked DataFusion 54.1.0, one `key-NNNNNNNNN` group costs well over 100
/// bytes of pool-accounted `GroupValues` plus sort headroom, so 1,000,000
/// groups reserve on the order of 150 MiB against the 8 MiB cap and spill
/// incrementally.
const IN_MEMORY_GROUPS: i64 = 1_000_000;
const IN_MEMORY_DOUBLED: i64 = 500_000;

/// ADR-0954 requirements 1 and 4: an eligible integer aggregation that overruns
/// the memory budget completes by spilling and returns the byte-identical result
/// of the same query run under a budget large enough not to spill. The equality
/// is over the values, not the row count.
///
/// Driven over an in-memory source so the aggregate is the binding pool consumer
/// and the operator that spills (see [`run_grouped_count`] for why the executor
/// path cannot show this for a scan-fed aggregation: the scan itself is the
/// binding non-spillable consumer there).
#[tokio::test]
async fn an_eligible_aggregation_over_budget_spills_and_matches_the_in_memory_result() {
    let keys = count_varying_keys(IN_MEMORY_GROUPS, IN_MEMORY_DOUBLED);
    let spill_scratch = tempfile::tempdir().expect("spill scratch root");
    let ref_scratch = tempfile::tempdir().expect("reference scratch root");

    let spilled = run_grouped_count(
        spill_scratch.path(),
        &keys,
        IN_MEMORY_SPILL_BYTES,
        AMPLE_SCRATCH_BYTES,
    )
    .await;
    let reference =
        run_grouped_count(ref_scratch.path(), &keys, 1 << 30, AMPLE_SCRATCH_BYTES).await;

    assert!(
        spilled.spill_files > 0,
        "the tight-budget aggregation must actually spill; wrote {} files",
        spilled.spill_files
    );
    assert_eq!(
        reference.spill_files, 0,
        "the large-budget reference must not spill"
    );

    assert_eq!(
        reference.rows.len(),
        IN_MEMORY_GROUPS as usize,
        "the reference must produce one group per distinct key"
    );
    assert_eq!(
        spilled.rows, reference.rows,
        "the spilled result must be identical to the in-memory result, value for value"
    );
    // The values, not just the row count: exactly IN_MEMORY_DOUBLED keys were
    // inserted twice (count 2) and the rest once (count 1). Assert the multiset
    // of counts, which pins the spilled merge produced the right per-group total.
    let twos = spilled.rows.iter().filter(|(_, c)| *c == 2).count();
    let ones = spilled.rows.iter().filter(|(_, c)| *c == 1).count();
    assert_eq!(
        twos, IN_MEMORY_DOUBLED as usize,
        "doubled keys must count 2"
    );
    assert_eq!(
        ones,
        (IN_MEMORY_GROUPS - IN_MEMORY_DOUBLED) as usize,
        "the rest must count 1"
    );
    assert_eq!(
        twos + ones,
        IN_MEMORY_GROUPS as usize,
        "every group must have count 1 or 2"
    );

    // Requirement 7: the runtime dropped inside `run_grouped_count`, so the disk
    // manager's scratch is gone; nothing survives a completed query.
    assert!(
        std::fs::read_dir(spill_scratch.path())
            .expect("scratch readable")
            .next()
            .is_none(),
        "a completed spilling query must leave no scratch behind"
    );
}

/// The scan-only statement: the same fixture, the same decode and dedup work,
/// but an ungrouped `count(*)` whose aggregate state is a single `i64`. Its
/// pool-accounted peak is therefore the scan's share of the budget, which is
/// what the split below is measured against.
const SCAN_ONLY_SQL: &str = "SELECT count(*) AS n FROM samples";

/// Per-query memory budget for the scratch-quota case, sized from the measured
/// split rather than shared with [`TIGHT_QUERY_BYTES`].
///
/// `RsegScanExec` retains every decoded segment of this fixture as one
/// non-spillable reservation, and that share measures 36,640,200 bytes
/// ([`SCAN_ONLY_SQL`] under [`AMPLE_QUERY_BYTES`], locked DataFusion 54.1.0).
/// At 16 MiB the scan alone cannot fit, so nothing about the AGGREGATE's spill
/// threshold could be established from that budget: whichever consumer happened
/// to ask at the pool boundary decided which error came back. 48 MiB is above
/// the scan's whole share and still far below the grouped run's 68,162,568-byte
/// peak, so the scan fits, the aggregate gets a bounded headroom to grow into,
/// and the aggregate is the consumer that must spill. Each of those figures is
/// asserted below.
const SPLIT_QUERY_BYTES: usize = 48 * 1024 * 1024;

/// A budget no run over this fixture approaches, for the reference run that
/// measures the grouped query's whole demand without spilling.
const AMPLE_QUERY_BYTES: usize = 1 << 30;

/// Ceiling on the scan's share of [`SPLIT_QUERY_BYTES`] (measured 36,640,200
/// bytes). Above this the scan would be eating the headroom the aggregate needs,
/// and the over-quota case would stop proving anything about the aggregate's
/// spill.
const SCAN_PEAK_CEILING_BYTES: u64 = 40 * 1024 * 1024;

/// Floor on what the scan leaves for the aggregate under [`SPLIT_QUERY_BYTES`]
/// (measured 13,671,416 bytes). The aggregate's spill threshold IS its pool
/// refusal, so it can only reach it if this much of the budget is actually
/// available to it.
const AGGREGATE_HEADROOM_FLOOR_BYTES: u64 = 8 * 1024 * 1024;

/// The `written`/`quota` figures [`SqlError::spill_budget_exhausted`] formats
/// into its message: `"{written} of {quota} bytes of scratch already written"`.
fn scratch_figures(err: &SqlError) -> (u64, u64) {
    let SqlError::SpillBudgetExhausted(message) = err else {
        panic!("expected SpillBudgetExhausted; got {err:?}");
    };
    let mut numbers = message
        .split_whitespace()
        .filter_map(|word| word.parse::<u64>().ok());
    let written = numbers.next().expect("the message states bytes written");
    let quota = numbers.next().expect("the message states the quota");
    (written, quota)
}

/// Drain `sql` through the real executor path with `accounting` attached, so
/// the run's pool-accounted peak is readable whether it succeeded or failed.
async fn drain_with_accounting(
    fixture: &Fixture,
    tenant: &ravel_types::TenantId,
    sql: &str,
    accounting: &QueryAccounting,
) -> Result<u64, SqlError> {
    let snapshot = fixture.snapshot(tenant).await;
    let mut stream = fixture
        .executor
        .plan_pinned(tenant.hash(), snapshot, sql, accounting, &[])
        .await?
        .execute()
        .await?;
    let mut rows = 0u64;
    while let Some(next) = stream.next().await {
        rows += next?.num_rows() as u64;
    }
    Ok(rows)
}

/// ADR-0954 requirements 2 and 5: an eligible aggregation whose spilled state
/// exceeds the per-query scratch quota fails with the typed
/// `SpillBudgetExhausted`, and returns no partial result.
///
/// The variant alone is not enough to know the query took the path this test is
/// about. The pool is shared: `RsegScanExec` holds a whole decoded segment as a
/// non-spillable reservation, so a budget the scan nearly fills would refuse
/// the aggregate's first reservation and end the query in
/// `ResourcesExhausted` from the scan side before the aggregate ever reached
/// its spill threshold. So the split is measured and asserted with figures:
///
/// 1. a scan-only control ([`SCAN_ONLY_SQL`]) over the same fixture and the
///    same budget COMPLETES, and its pool-accounted peak is the scan's share;
/// 2. that share is under [`SCAN_PEAK_CEILING_BYTES`] and leaves at least
///    [`AGGREGATE_HEADROOM_FLOOR_BYTES`] of the budget for the aggregate;
/// 3. the grouped query's own demand, measured by a reference run under
///    [`AMPLE_QUERY_BYTES`] that does not spill, exceeds that headroom by more
///    than the headroom itself, so the aggregate cannot fit in what the scan
///    leaves and must reach its spill threshold;
/// 4. the failing run's own figures show scratch bytes ALREADY WRITTEN when the
///    quota refused the next write, which is only reachable by an aggregate that
///    reached that threshold and spilled;
/// 5. the quota in those figures is [`TINY_SCRATCH_BYTES`], so the refusal came
///    from this query's configured scratch ceiling and not from some other
///    limit.
#[tokio::test]
async fn an_eligible_aggregation_over_the_scratch_quota_is_spill_budget_exhausted() {
    let tenant = tenant_id("acme");
    let specs = overlapping_series_specs(LONG_ROWS, SHORT_ROWS);
    let scratch = tempfile::tempdir().expect("scratch root");

    let fixture = Fixture::build(
        Arc::new(MemoryStore::new()),
        &[(&tenant, &specs)],
        spill_config(
            scratch.path().to_path_buf(),
            TINY_SCRATCH_BYTES,
            SPLIT_QUERY_BYTES,
        ),
        1 << 40,
    )
    .await;

    // 1 and 2: what the scan takes, and what it leaves.
    let control_accounting = QueryAccounting::new();
    let control_rows = drain_with_accounting(&fixture, &tenant, SCAN_ONLY_SQL, &control_accounting)
        .await
        .expect("the scan-only control must complete within the same budget");
    assert_eq!(
        control_rows, 1,
        "the ungrouped control returns one row, so its peak is the scan's, not \
         an aggregate's"
    );
    let scan_peak = control_accounting.snapshot().peak_intermediate_bytes;
    let budget = SPLIT_QUERY_BYTES as u64;
    let headroom = budget.saturating_sub(scan_peak);
    assert!(
        scan_peak > 0,
        "the control must have reserved from the pool, else it measured nothing"
    );
    assert!(
        scan_peak <= SCAN_PEAK_CEILING_BYTES,
        "the scan's share {scan_peak} must stay under {SCAN_PEAK_CEILING_BYTES} \
         bytes; above that the scan, not the aggregate, is the consumer that \
         approaches the budget and this case proves nothing about the spill"
    );
    assert!(
        headroom >= AGGREGATE_HEADROOM_FLOOR_BYTES,
        "the scan must leave the aggregate at least \
         {AGGREGATE_HEADROOM_FLOOR_BYTES} bytes of the {budget}-byte budget to \
         grow into and spill from; it left {headroom}"
    );

    // 3: how much the grouped query actually wants, measured where nothing is
    // refused and nothing spills. The two consumers overlap in time, so
    // `reference_peak - scan_peak` is a LOWER bound on the aggregate's own
    // demand, which is the direction this assertion needs.
    let reference = Fixture::build(
        Arc::new(MemoryStore::new()),
        &[(&tenant, &specs)],
        spill_config(
            scratch.path().to_path_buf(),
            AMPLE_SCRATCH_BYTES,
            AMPLE_QUERY_BYTES,
        ),
        1 << 40,
    )
    .await;
    let reference_outcome = reference
        .executor
        .execute(tenant.hash(), &request(SPILLING_SQL))
        .await
        .expect("the reference run must complete under an ample budget");
    assert_eq!(
        reference_outcome.output.num_rows(),
        LONG_ROWS as usize,
        "the reference must produce one row per distinct ts group"
    );
    assert_eq!(
        reference_outcome.stats.spill,
        Default::default(),
        "the reference must not spill, or its peak is not the query's whole \
         demand; got {:?}",
        reference_outcome.stats.spill
    );
    let reference_peak = reference_outcome.accounting.peak_intermediate_bytes;
    let aggregate_demand = reference_peak.saturating_sub(scan_peak);
    eprintln!(
        "budget split: scan_peak={scan_peak} of budget={budget}, \
         headroom={headroom}, reference_peak={reference_peak}, \
         aggregate_demand_lower_bound={aggregate_demand}"
    );
    assert!(
        aggregate_demand > headroom,
        "the aggregate wants at least {aggregate_demand} bytes but the scan \
         leaves it {headroom}: it must not fit, or it never reaches its spill \
         threshold and this case would be measuring the scan's refusal"
    );

    let err = fixture
        .executor
        .execute(tenant.hash(), &request(SPILLING_SQL))
        .await
        .expect_err("the tiny scratch quota must trip");

    // 3 and 4: the variant, and the figures behind it.
    assert!(
        matches!(err, SqlError::SpillBudgetExhausted(_)),
        "an over-quota spill must fail SpillBudgetExhausted, distinct from a \
         memory-pool exhaustion from the scan side; got {err:?}"
    );
    let (written, quota) = scratch_figures(&err);
    eprintln!("scratch figures at the refusal: written={written} quota={quota}");
    assert_eq!(
        quota, TINY_SCRATCH_BYTES,
        "the refusal must name this query's configured scratch quota"
    );
    assert!(
        written > 0,
        "the aggregate must have reached its spill threshold and written \
         scratch before the quota refused it; {written} bytes written means the \
         query failed without spilling at all"
    );
    // The gauge the figure comes from (`DiskManager::used_disk_space`) already
    // includes the write that pushed the query past its ceiling, so the reported
    // total is at or above the quota, never below it. Measured: 133,312 bytes of
    // a 65,536-byte quota. Below the quota would mean the refusal came from
    // something other than this ceiling.
    assert!(
        written >= quota,
        "the refusal must report scratch at or above the quota it tripped; \
         got {written} of {quota}"
    );

    // Requirement 7: a typed error cleans up its scratch too.
    assert!(
        spill_subdirs(scratch.path()).is_empty(),
        "a query that failed SpillBudgetExhausted must leave no scratch behind"
    );
}

/// ADR-0954 requirements 3 and 5: an eligible query with spill armed at a
/// directory that does not exist fails the typed `SpillUnavailable`, raised
/// before any operator runs, and creates nothing.
#[tokio::test]
async fn an_eligible_query_with_a_missing_spill_directory_is_spill_unavailable() {
    let tenant = tenant_id("acme");
    // Small: the failure is at scratch creation, before planning, so the data
    // size is irrelevant and a large fixture would only slow the test.
    let specs = overlapping_series_specs(100, 50);
    let root = tempfile::tempdir().expect("scratch root");
    let missing = root.path().join("does-not-exist");

    let fixture = Fixture::build(
        Arc::new(MemoryStore::new()),
        &[(&tenant, &specs)],
        spill_config(missing.clone(), AMPLE_SCRATCH_BYTES, TIGHT_QUERY_BYTES),
        1 << 40,
    )
    .await;

    let err = fixture
        .executor
        .execute(tenant.hash(), &request(SPILLING_SQL))
        .await
        .expect_err("a missing spill directory must be refused");

    assert!(
        matches!(err, SqlError::SpillUnavailable(_)),
        "a missing spill directory must fail SpillUnavailable; got {err:?}"
    );
    assert!(
        !missing.exists(),
        "the executor must never create the configured spill directory"
    );
}

/// ADR-0954 requirement 4, fail closed: an ineligible plan (order-dependent
/// float aggregation) over budget is still refused with a memory-pool
/// exhaustion, NOT a spill, even though spill is configured. No scratch
/// subdirectory is ever created for it.
#[tokio::test]
async fn an_ineligible_float_aggregation_over_budget_is_refused_and_never_spills() {
    let tenant = tenant_id("acme");
    let specs = overlapping_series_specs(LONG_ROWS, SHORT_ROWS);
    let scratch = tempfile::tempdir().expect("scratch root");

    let fixture = Fixture::build(
        Arc::new(MemoryStore::new()),
        &[(&tenant, &specs)],
        spill_config(
            scratch.path().to_path_buf(),
            AMPLE_SCRATCH_BYTES,
            TIGHT_QUERY_BYTES,
        ),
        1 << 40,
    )
    .await;

    let err = fixture
        .executor
        .execute(tenant.hash(), &request(INELIGIBLE_SQL))
        .await
        .expect_err("an ineligible float aggregation over budget must be refused");

    assert!(
        matches!(err, SqlError::ResourcesExhausted(_)),
        "an ineligible plan must be refused with the memory-pool exhaustion, \
         not any spill-specific error; got {err:?}"
    );
    assert!(
        !matches!(
            err,
            SqlError::SpillBudgetExhausted(_) | SqlError::SpillUnavailable(_)
        ),
        "an ineligible plan must not reach any spill path; got {err:?}"
    );
    assert!(
        spill_subdirs(scratch.path()).is_empty(),
        "an ineligible plan must never create a scratch subdirectory"
    );
}

/// A memory budget no plan-shape test can exhaust: the two cases below only
/// plan, they never drain a stream, so the budget must not be part of what they
/// measure.
const PLAN_SHAPE_QUERY_BYTES: usize = 1 << 30;

/// The config for the two plan-shape cases: aggregate repartitioning armed
/// (`parallel_final_aggregation`, ADR-0094) and the default multi-partition
/// scan, so `EnforceDistribution` is free to insert a hash `RepartitionExec`.
/// `spill` is the one variable between the control and the subject.
fn parallel_config(spill: Option<SpillConfig>) -> SqlConfig {
    SqlConfig {
        // The default `fetch_concurrency` (8) becomes `target_partitions`, so
        // there is more than one partition to repartition across. Pinning it to
        // 1 the way `spill_config` does would make the control vacuous.
        engine: util::engine_config(),
        max_query_bytes: PLAN_SHAPE_QUERY_BYTES,
        parallel_final_aggregation: true,
        skip_partial_aggregation: true,
        late_materialization_extra_columns: None,
        spill,
    }
}

/// The indented physical plan text for `sql`, planned through the real
/// `plan_pinned` path so the session it is planned in is the one the executor
/// built (spill decision, repartition knob, and all).
async fn physical_plan_text(
    fixture: &Fixture,
    tenant: &ravel_types::TenantId,
    sql: &str,
) -> String {
    let accounting = QueryAccounting::new();
    let snapshot = fixture.snapshot(tenant).await;
    let plan = fixture
        .executor
        .plan_pinned(tenant.hash(), snapshot, sql, &accounting, &[])
        .await
        .expect("the query plans")
        .create_physical_plan()
        .await
        .expect("the physical plan builds");
    format!(
        "{}",
        datafusion::physical_plan::displayable(plan.as_ref()).indent(false)
    )
}

/// ADR-0954's fail-closed argument, closed over the physical plan: a query
/// granted spill runs its final aggregation single-partitioned, so the
/// `RepartitionExec` that `parallel_final_aggregation` would otherwise
/// introduce is never in a plan whose disk manager is enabled.
///
/// This matters because the two permissions have different scopes. Eligibility
/// is decided by a predicate over the LOGICAL plan
/// (`plan_is_spill_eligible`, an allowlist of logical nodes), but the disk
/// manager it arms belongs to the session: every operator in the physical plan
/// may then spill. `RepartitionExec` spills in DataFusion 54 -- its output
/// channels fall back to a `SpillPool` when the pool refuses their reservation,
/// which is where the `SpillPool (DiskManager is disabled)` message in
/// `tests/memory_pool_attribution.rs` comes from -- and no predicate in this
/// crate has classified an exchange spill's exactness.
///
/// The control half of the test is what makes it non-vacuous: the same
/// statement, the same fixture, the same `parallel_final_aggregation`, spill
/// off, DOES get the hash repartition and a `FinalPartitioned` aggregate.
#[tokio::test]
async fn a_spill_enabled_query_gets_no_unclassified_repartition_exec() {
    let tenant = tenant_id("acme");
    // Two segments is enough: this case reads plan shape, not data.
    let specs = overlapping_series_specs(SEG_ROWS * 2, SEG_ROWS);
    let scratch = tempfile::tempdir().expect("scratch root");

    // Control: spill off. The statement is exact-typed (integer `count`, a
    // non-float `ts` key), so the aggregate repartition fires.
    let control = Fixture::build(
        Arc::new(MemoryStore::new()),
        &[(&tenant, &specs)],
        parallel_config(None),
        1 << 40,
    )
    .await;
    let control_plan = physical_plan_text(&control, &tenant, SPILLING_SQL).await;
    assert!(
        control_plan.contains("RepartitionExec: partitioning=Hash"),
        "the control must repartition, else this test proves nothing about the \
         spill grant; got:\n{control_plan}"
    );
    assert!(
        control_plan.contains("AggregateExec: mode=FinalPartitioned"),
        "the control's final aggregate must be the fanned-out one; got:\n{control_plan}"
    );

    // Subject: the identical statement with spill configured. Eligible, so the
    // disk manager is enabled for the whole session.
    let fixture = Fixture::build(
        Arc::new(MemoryStore::new()),
        &[(&tenant, &specs)],
        parallel_config(Some(SpillConfig {
            dir: scratch.path().to_path_buf(),
            max_bytes: AMPLE_SCRATCH_BYTES,
        })),
        1 << 40,
    )
    .await;

    let accounting = QueryAccounting::new();
    let snapshot = fixture.snapshot(&tenant).await;
    let pinned = fixture
        .executor
        .plan_pinned(tenant.hash(), snapshot, SPILLING_SQL, &accounting, &[])
        .await
        .expect("the eligible query plans");
    // Spill really was granted: this is the case the finding is about, not a
    // query that was refused spill and trivially has no repartition.
    assert_eq!(
        spill_subdirs(scratch.path()).len(),
        1,
        "the subject must have been granted spill (one scratch subdirectory)"
    );

    let plan = pinned
        .create_physical_plan()
        .await
        .expect("the physical plan builds");
    let text = format!(
        "{}",
        datafusion::physical_plan::displayable(plan.as_ref()).indent(false)
    );
    assert!(
        !text.contains("RepartitionExec"),
        "a spill-enabled plan must contain no RepartitionExec: the eligibility \
         predicate classified logical nodes, and an enabled disk manager would \
         grant spill to this unclassified exchange too; got:\n{text}"
    );
    assert!(
        !text.contains("FinalPartitioned"),
        "a spill-enabled plan must keep the single-partition final aggregate; \
         got:\n{text}"
    );
    // The aggregate that spill exists for is still there and still able to
    // spill. `mode=Single` is a NON-partial aggregate mode, and DataFusion 54
    // gives every non-partial mode with no group ordering
    // `OutOfMemoryMode::Spill` once the disk manager is enabled
    // (`aggregates/row_hash.rs`); it is `mode=Partial` that emits early instead
    // of spilling. So dropping the exchange costs parallelism, not the spill.
    assert!(
        text.contains("AggregateExec: mode=Single"),
        "the spill-enabled plan must still carry the grouped aggregate as a \
         single-partition (non-partial) stage, which is the mode that spills; \
         got:\n{text}"
    );
}

/// ADR-0954 requirement 9: with spill unset (the shipped default), an eligible
/// high-cardinality aggregation over budget reproduces the pre-ADR behavior
/// exactly -- a typed `ResourcesExhausted` re-attributed from the disabled disk
/// manager, its `DiskManager is disabled` marker rewritten to the pool's own
/// occupancy. This is the same assertion the pre-spill coverage makes, restated
/// here so a regression in the default path fails in the spill suite too.
#[tokio::test]
async fn spill_off_reproduces_todays_refusal() {
    let tenant = tenant_id("acme");
    let specs = overlapping_series_specs(LONG_ROWS, SHORT_ROWS);

    let config = SqlConfig {
        engine: util::engine_config(),
        max_query_bytes: TIGHT_QUERY_BYTES,
        parallel_final_aggregation: false,
        skip_partial_aggregation: true,
        late_materialization_extra_columns: None,
        spill: None,
    };
    assert_eq!(config.spill, None, "this case is the spill-off default");

    let fixture = Fixture::build(
        Arc::new(MemoryStore::new()),
        &[(&tenant, &specs)],
        config,
        1 << 40,
    )
    .await;

    let err = fixture
        .executor
        .execute(tenant.hash(), &request(SPILLING_SQL))
        .await
        .expect_err("with spill off the aggregation over budget must be refused");

    assert!(
        matches!(err, SqlError::ResourcesExhausted(_)),
        "spill off must reproduce the typed refusal; got {err:?}"
    );
    // The re-attribution the disabled disk manager path produces: DataFusion's
    // own `DiskManager is disabled` wording is replaced by the pool's occupancy.
    let SqlError::ResourcesExhausted(msg) = &err else {
        unreachable!("asserted above");
    };
    assert!(
        !msg.contains("DiskManager is disabled"),
        "the disabled-disk-manager marker must be re-attributed away; got {msg:?}"
    );
}

/// ADR-0954 requirement 7, the cancellation path: a spilling query whose stream
/// is dropped mid-flight still removes its scratch. Driven through the lower
/// level `plan_pinned`/`execute` so the stream can be dropped after a single
/// poll rather than drained.
#[tokio::test]
async fn a_cancelled_spilling_stream_cleans_up_its_scratch() {
    let tenant = tenant_id("acme");
    let specs = overlapping_series_specs(LONG_ROWS, SHORT_ROWS);
    let scratch = tempfile::tempdir().expect("scratch root");

    let fixture = Fixture::build(
        Arc::new(MemoryStore::new()),
        &[(&tenant, &specs)],
        spill_config(
            scratch.path().to_path_buf(),
            AMPLE_SCRATCH_BYTES,
            TIGHT_QUERY_BYTES,
        ),
        1 << 40,
    )
    .await;

    let accounting = QueryAccounting::new();
    let snapshot = fixture.snapshot(&tenant).await;
    let pinned = fixture
        .executor
        .plan_pinned(tenant.hash(), snapshot, SPILLING_SQL, &accounting, &[])
        .await
        .expect("the eligible query plans and creates its scratch");

    // The scratch subdirectory exists once the eligible query is planned.
    assert_eq!(
        spill_subdirs(scratch.path()).len(),
        1,
        "an eligible planned query must have created exactly one scratch subdirectory"
    );

    let mut stream = pinned.execute().await.expect("the stream starts");
    // Poll once, then abandon the stream mid-flight (the cancellation path).
    let _ = stream.next().await;
    drop(stream);

    assert!(
        spill_subdirs(scratch.path()).is_empty(),
        "a cancelled spilling stream must remove its scratch on drop"
    );
}

/// ADR-0954 requirement 1 and its implementation-constraint measurement: a
/// spilling aggregation's pool-accounted peak reservation stays within the
/// configured cap (plus [`SPILL_POOL_PEAK_OVERHEAD_BYTES`], which is zero
/// because `try_grow` is a hard cap), and the measured peak is reported against
/// that cap.
///
/// The asserted quantity is the pool-accounted peak, which is what requirement 1
/// governs and the only bound established by reasoning: `try_grow` refuses any
/// reservation past the cap, and DataFusion 54's grouped-hash spill frees the
/// group state (`clear_shrink(0)`) before it reserves the sort headroom out of
/// the same pool, so no pool-accounted byte exceeds the ceiling. Process RSS is
/// deliberately NOT asserted here: the ADR notes the pool-to-RSS gap (allocator
/// overhead, Arrow rounding, the emitted-batch-plus-sort peak) is not bounded by
/// reasoning, and on a shared multi-test process the resident set is dominated
/// by the harness and any concurrent build, so a tight RSS band would measure
/// the host, not the query. The pool-accounted peak is the deterministic,
/// gate-stable bound; the report records the real figure.
#[tokio::test]
async fn the_spilling_aggregation_holds_its_pool_accounted_peak_under_the_cap() {
    let keys = count_varying_keys(IN_MEMORY_GROUPS, IN_MEMORY_DOUBLED);
    let scratch = tempfile::tempdir().expect("scratch root");

    let run = run_grouped_count(
        scratch.path(),
        &keys,
        IN_MEMORY_SPILL_BYTES,
        AMPLE_SCRATCH_BYTES,
    )
    .await;

    let cap = IN_MEMORY_SPILL_BYTES as u64;
    eprintln!(
        "spill peak measurement: pool_peak={} bytes, cap={cap} bytes, spill_files={}",
        run.peak_pool_bytes, run.spill_files,
    );

    assert!(
        run.spill_files > 0,
        "this test's subject must actually spill; wrote {} files",
        run.spill_files
    );
    assert!(
        run.peak_pool_bytes <= cap + SPILL_POOL_PEAK_OVERHEAD_BYTES,
        "the pool-accounted peak {} must stay within the configured cap {cap} \
         (+{SPILL_POOL_PEAK_OVERHEAD_BYTES} overhead); a value above the cap \
         means try_grow's hard limit leaked (ADR-0954 requirement 1)",
        run.peak_pool_bytes,
    );
    assert!(
        run.peak_pool_bytes > 0,
        "the pool-accounted peak must be observed, not left at zero"
    );
}
