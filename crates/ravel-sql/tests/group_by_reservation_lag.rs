//! Issue #2488 (follow-up of #2367): how far live allocation leads the SQL
//! pool's reservation during a high-cardinality `GROUP BY`.
//!
//! #2367's heap profile at the allocation peak showed 17.41 GB of live GROUP BY
//! state against 15.39 GB reserved, and 22.52 GB allocated against 5.96 GB of
//! SQL reservation five seconds earlier. The server's memory ledger sees only
//! the pool's `reserved()`, so a gap between the two is memory the admission
//! controller cannot account for. This test measures that gap and attributes it
//! by varying one thing at a time.
//!
//! Production path modelled: the SQL endpoint's `GROUP BY` over the `logs`
//! table (q33's shape: a high-cardinality string column plus `Int64` columns,
//! `COUNT`/`SUM`/exact `AVG`). The session is `ravel_sql::build_session`, so
//! the optimizer rules, the allowlists, Ravel's exact `AVG` UDAF and the
//! partitioning knobs are the ones the endpoint runs; the pool is
//! `TenantDelegatingPool`, whose `reserved()` is the figure the ledger reads.
//! Only the input differs: an in-memory table registered next to the (empty)
//! `logs` provider, so the measurement does not depend on segment decoding.
//!
//! Method. The binary installs `stats_alloc`'s instrumented allocator (the
//! workspace denies `unsafe`, so a hand-rolled one cannot compile). A sampler
//! thread reads, back to back (yielding between reads), live bytes (allocated
//! minus deallocated, less the bytes live before the query started, so the
//! input table is excluded) and the pool's `reserved()`. The gap is live minus
//! reserved; the peak gap is its maximum over the run. Result batches are
//! dropped as they arrive.
//!
//! The pool's aggregate hold (issue #2633) closes the emit-phase part of the
//! gap. With it, every Ravel-path row's peak gap equals its build gap, the
//! largest gap sampled before the first result batch, in three `--release`
//! runs. One run with the hold disabled showed the emit-phase gap again, for
//! example 142.7 MiB of peak gap against 104.5 MiB of build gap at 8
//! partitions with a string key and `COUNT(*)`. The plain DataFusion rows still
//! show it: their runtime has DataFusion's default disk manager, so their
//! aggregate consumers can spill and the pool does not hold them.
//!
//! The heap-profiling matrix below (`group_by_allocation_lead_over_reservation`)
//! is `#[ignore]`d: it is the only test in this file that samples the global
//! allocator, and a default `cargo test` run never shares a process with an
//! ignored one, so a second test in this file allocating cannot pollute its
//! live-bytes figure. Its own doc comment gives the command to run it and the
//! basis for the bands it asserts.
//!
//! `exact_integer_avg_takes_part_in_the_partial_aggregation_skip` is the
//! always-on counterpart: it asserts the mechanism (DataFusion's
//! `skipped_aggregation_rows` metric, and identical result rows with the skip
//! disabled) rather than a memory figure, so it carries no profile or host
//! dependence and runs in every `cargo test`.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::alloc::System;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering};
use std::thread;
use std::time::Instant;

use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, StringViewArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::{ExecutionPlan, execute_stream};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use ravel_catalog::Snapshot;
use ravel_object_store::memory::MemoryStore;
use ravel_query::{LogSegmentFetcher, PhaseAccounting};
use ravel_sql::{
    LogsTableProvider, SessionTable, SpillDecision, SqlConfig, TenantMemoryAccountant,
    build_session, session_config,
};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use stats_alloc::{INSTRUMENTED_SYSTEM, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

/// Input rows, and distinct values of each key column. Every key appears
/// twice, in different input partitions, so the final aggregation merges
/// partial groups rather than only concatenating them.
const ROWS: usize = 3_000_000;
const DISTINCT: usize = 1_500_000;
const BATCH_ROWS: usize = 8_192;
/// Input partitions of the in-memory table, held constant so the varied knob
/// is only `target_partitions`.
const INPUT_PARTITIONS: usize = 8;

/// The `target_partitions` / key / aggregate that every one-variable series
/// holds fixed while another varies.
const BASE_PARTITIONS: usize = 8;

#[derive(Clone, Copy, Debug)]
enum Key {
    Str,
    TwoInt,
    StrInt,
}

#[derive(Clone, Copy, Debug)]
enum Agg {
    Count,
    Sum,
    Avg,
}

/// Which session the query runs on.
#[derive(Clone, Copy, Debug)]
enum Variant {
    /// `build_session` as the SQL endpoint builds it.
    Ravel,
    /// DataFusion's own session: same knobs and pool, no Ravel rules or UDAFs.
    Plain,
}

#[derive(Debug)]
struct Run {
    label: String,
    partitions: usize,
    groups: usize,
    peak_live: usize,
    peak_reserved: usize,
    peak_gap: usize,
    /// Peak gap before the first result batch reached the consumer: the
    /// accumulation phase, where the group tables only grow.
    peak_gap_build: usize,
    samples: usize,
}

fn live_bytes() -> isize {
    let s = INSTRUMENTED_SYSTEM.stats();
    // `realloc` growth is already folded into `bytes_allocated` (and shrink into
    // `bytes_deallocated`); `bytes_reallocated` is a separate net figure and
    // adding it would count every `Vec` growth twice.
    s.bytes_allocated as isize - s.bytes_deallocated as isize
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("url", DataType::Utf8View, false),
        Field::new("watch_id", DataType::Int64, false),
        Field::new("client_ip", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

/// q33-shaped input: `url` and `watch_id` each take `DISTINCT` values,
/// `client_ip` a small one, `v` is the aggregated column.
fn input_partitions() -> Vec<Vec<RecordBatch>> {
    let schema = schema();
    let mut parts: Vec<Vec<RecordBatch>> = (0..INPUT_PARTITIONS).map(|_| Vec::new()).collect();
    let mut start = 0usize;
    let mut batch_no = 0usize;
    while start < ROWS {
        let end = (start + BATCH_ROWS).min(ROWS);
        let ids: Vec<usize> = (start..end).map(|i| i % DISTINCT).collect();
        let url: StringViewArray = ids
            .iter()
            .map(|i| {
                Some(format!(
                    "https://example.test/catalog/{i:09}/item?utm_source=bench&session={}",
                    i.wrapping_mul(2_654_435_761) % 1_000_003
                ))
            })
            .collect();
        let watch: Int64Array = ids.iter().map(|i| *i as i64).collect();
        let ip: Int64Array = (start..end).map(|i| (i % 7_919) as i64).collect();
        let v: Int64Array = (start..end).map(|i| (i % 1_000) as i64).collect();
        let columns: Vec<ArrayRef> =
            vec![Arc::new(url), Arc::new(watch), Arc::new(ip), Arc::new(v)];
        parts[batch_no % INPUT_PARTITIONS]
            .push(RecordBatch::try_new(Arc::clone(&schema), columns).expect("batch builds"));
        batch_no += 1;
        start = end;
    }
    parts
}

fn sql_for(key: Key, agg: Agg) -> String {
    let keys = match key {
        Key::Str => "url",
        Key::TwoInt => "watch_id, client_ip",
        Key::StrInt => "url, client_ip",
    };
    let agg = match agg {
        Agg::Count => "count(*)",
        Agg::Sum => "sum(v)",
        Agg::Avg => "avg(v)",
    };
    format!("SELECT {keys}, {agg} AS a FROM t GROUP BY {keys}")
}

fn run_once(
    runtime: &tokio::runtime::Runtime,
    parts: &[Vec<RecordBatch>],
    partitions: usize,
    key: Key,
    agg: Agg,
    variant: Variant,
) -> Run {
    let mut config = SqlConfig::default();
    config.engine.sql_partition_count = Some(partitions);
    config.max_query_bytes = 64 << 30;
    let plain = matches!(variant, Variant::Plain);
    let tenant = TenantMemoryAccountant::new(64 << 30);
    let (pool, _breach) = config.query_pool(tenant, QueryAccounting::new());

    let store = Arc::new(MemoryStore::new());
    let logs = SessionTable::Logs(Arc::new(LogsTableProvider::new(
        Snapshot {
            segments: Vec::new(),
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        },
        TenantHash([0u8; 16]),
        LogSegmentFetcher::new(store),
        PhaseAccounting::new(),
    )));
    // `exact_typed_aggregates = true`: the endpoint's classification for a
    // COUNT/SUM/exact-AVG query over exact-typed keys, which is what lets the
    // final aggregation fan out across `target_partitions`.
    let ctx = if plain {
        // The same session knobs and pool, but none of Ravel's rules or
        // UDAF replacements: DataFusion's own `avg` and no group-key rewrite.
        let runtime_env = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&pool) as Arc<dyn MemoryPool>)
            .build_arc()
            .expect("runtime builds");
        let state = SessionStateBuilder::new()
            .with_config(session_config(&config, true, SpillDecision::Disabled))
            .with_runtime_env(runtime_env)
            .with_default_features()
            .build();
        SessionContext::new_with_state(state)
    } else {
        // `exact_typed_aggregates = true`: the endpoint's classification for a
        // COUNT/SUM/exact-AVG query over exact-typed keys, which is what lets
        // the final aggregation fan out across `target_partitions`.
        build_session(
            &config,
            Arc::clone(&pool) as Arc<dyn MemoryPool>,
            logs,
            true,
            SpillDecision::Disabled,
        )
        .expect("session builds")
    };
    let table = MemTable::try_new(schema(), parts.to_vec()).expect("mem table builds");
    ctx.register_table("t", Arc::new(table))
        .expect("table registers");

    let baseline = live_bytes();
    let stop = Arc::new(AtomicBool::new(false));
    let peak_live = Arc::new(AtomicIsize::new(0));
    let peak_reserved = Arc::new(AtomicUsize::new(0));
    let peak_gap = Arc::new(AtomicIsize::new(isize::MIN));
    let peak_gap_build = Arc::new(AtomicIsize::new(isize::MIN));
    let emitting = Arc::new(AtomicBool::new(false));
    let samples = Arc::new(AtomicUsize::new(0));
    let sampler = {
        let (stop, peak_live, peak_reserved, peak_gap, peak_gap_build, emitting, samples) = (
            Arc::clone(&stop),
            Arc::clone(&peak_live),
            Arc::clone(&peak_reserved),
            Arc::clone(&peak_gap),
            Arc::clone(&peak_gap_build),
            Arc::clone(&emitting),
            Arc::clone(&samples),
        );
        let pool = Arc::clone(&pool);
        thread::spawn(move || {
            let t0 = Instant::now();
            let mut next_trace = 0u128;
            let mut trace: Vec<(u128, isize, usize, bool)> = Vec::new();
            while !stop.load(Ordering::Acquire) {
                let live = live_bytes() - baseline;
                let reserved = pool.reserved();
                peak_live.fetch_max(live, Ordering::Relaxed);
                peak_reserved.fetch_max(reserved, Ordering::Relaxed);
                let gap = live - reserved as isize;
                peak_gap.fetch_max(gap, Ordering::Relaxed);
                if !emitting.load(Ordering::Acquire) {
                    peak_gap_build.fetch_max(gap, Ordering::Relaxed);
                }
                samples.fetch_add(1, Ordering::Relaxed);
                let ms = t0.elapsed().as_millis();
                if ms >= next_trace {
                    trace.push((ms, live, reserved, emitting.load(Ordering::Acquire)));
                    next_trace = ms + 20;
                }
                thread::yield_now();
            }
            trace
        })
    };

    let sql = sql_for(key, agg);
    let started = Instant::now();
    let emitting_flag = Arc::clone(&emitting);
    let groups = runtime.block_on(async {
        let mut stream = ctx
            .sql(&sql)
            .await
            .expect("query plans")
            .execute_stream()
            .await
            .expect("stream starts");
        let mut groups = 0usize;
        while let Some(batch) = stream.next().await {
            emitting_flag.store(true, Ordering::Release);
            groups += batch.expect("batch").num_rows();
        }
        groups
    });
    let elapsed = started.elapsed();
    stop.store(true, Ordering::Release);
    let trace = sampler.join().expect("sampler joins");
    if std::env::var("RAVEL_SQL_GAP_TRACE").as_deref() == Ok("1") {
        for (ms, live, reserved, emit) in &trace {
            eprintln!(
                "TRACE P={partitions} {key:?} {agg:?} t={ms}ms live={} reserved={} gap={} emit={emit}",
                mib(*live as usize),
                mib(*reserved),
                mib((*live - *reserved as isize).max(0) as usize)
            );
        }
    }
    drop(ctx);

    let run = Run {
        label: format!(
            "P={partitions:<2} key={key:?} agg={agg:?}{}",
            match variant {
                Variant::Ravel => "",
                Variant::Plain => " [plain DF]",
            }
        ),
        partitions,
        groups,
        peak_live: peak_live.load(Ordering::Relaxed).max(0) as usize,
        peak_reserved: peak_reserved.load(Ordering::Relaxed),
        peak_gap: peak_gap.load(Ordering::Relaxed).max(0) as usize,
        peak_gap_build: peak_gap_build.load(Ordering::Relaxed).max(0) as usize,
        samples: samples.load(Ordering::Relaxed),
    };
    eprintln!("  ({} took {elapsed:?})", run.label);
    run
}

fn mib(bytes: usize) -> String {
    format!("{:.1}", bytes as f64 / (1 << 20) as f64)
}

/// Sum DataFusion's `name` metric over every node of `plan`, recursively.
/// Nodes that do not publish `name` contribute 0; only a grouping `Partial`
/// `AggregateExec` publishes `skipped_aggregation_rows`, so summing over the
/// whole tree is equivalent to reading it off that one node here.
fn sum_metric(plan: &Arc<dyn ExecutionPlan>, name: &str) -> usize {
    let mut total = plan
        .metrics()
        .and_then(|metrics| metrics.sum_by_name(name))
        .map(|value| value.as_usize())
        .unwrap_or(0);
    for child in plan.children() {
        total += sum_metric(child, name);
    }
    total
}

/// `(key, avg, count)` rows from a GROUP BY's output batches, with the `avg`
/// column compared by bit pattern (testing-patterns convention: float
/// equality by `==` is not reliable in general, even though every group here
/// has exactly one row and so an exact, not approximate, average).
fn extract_rows(batches: &[RecordBatch]) -> Vec<(i64, u64, i64)> {
    let mut rows = Vec::new();
    for batch in batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("key is Int64");
        let avgs = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("avg(int) returns Float64 (ADR-0825)");
        let counts = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count is Int64");
        for i in 0..batch.num_rows() {
            rows.push((keys.value(i), avgs.value(i).to_bits(), counts.value(i)));
        }
    }
    rows.sort_unstable();
    rows
}

/// Build a session over an all-distinct `(key, v)` table split evenly across
/// `partitions` input partitions of `rows_per_partition` rows each. A single
/// input partition collapses the physical plan to `AggregateMode::Single` (no
/// partial/final split, so no probe and no `skipped_aggregation_rows` at all);
/// `partitions` must be at least 2 for the partial stage this test targets to
/// exist. Keys are globally distinct, so they are also distinct within any one
/// partition, which is what the probe's ratio reads.
fn skip_probe_session(
    skip_partial_aggregation: bool,
    partitions: usize,
    rows_per_partition: usize,
) -> SessionContext {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let mut config = SqlConfig {
        skip_partial_aggregation,
        max_query_bytes: 64 << 20,
        ..SqlConfig::default()
    };
    config.engine.sql_partition_count = Some(partitions);
    let tenant = TenantMemoryAccountant::new(64 << 20);
    let (pool, _breach) = config.query_pool(tenant, QueryAccounting::new());
    let store = Arc::new(MemoryStore::new());
    let logs = SessionTable::Logs(Arc::new(LogsTableProvider::new(
        Snapshot {
            segments: Vec::new(),
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        },
        TenantHash([0u8; 16]),
        LogSegmentFetcher::new(store),
        PhaseAccounting::new(),
    )));
    let ctx = build_session(
        &config,
        Arc::clone(&pool) as Arc<dyn MemoryPool>,
        logs,
        true,
        SpillDecision::Disabled,
    )
    .expect("session builds");

    let parts: Vec<Vec<RecordBatch>> = (0..partitions)
        .map(|p| {
            let start = (p * rows_per_partition) as i64;
            let end = start + rows_per_partition as i64;
            let key: Int64Array = (start..end).collect();
            let v: Int64Array = (start..end).map(|i| i * 7).collect();
            vec![
                RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(key), Arc::new(v)])
                    .expect("batch builds"),
            ]
        })
        .collect();
    let table = MemTable::try_new(schema, parts).expect("mem table builds");
    ctx.register_table("t", Arc::new(table))
        .expect("table registers");
    ctx
}

/// Issue #2488's own mechanism, isolated from issue #680's (covered by
/// `tests/skip_partial_aggregation.rs`): not whether `skip_partial_aggregation`
/// fires at all, but whether Ravel's exact integer `AVG` accumulator can take
/// part in the skip once it does.
/// `ExactIntegerAvgGroupsAccumulator::convert_to_state` (crates/ravel-sql/src/
/// avg.rs) is what makes that possible; before it existed,
/// `supports_convert_to_state` returned `false` and the partial stage built a
/// full group hash table for every row regardless of what the probe's ratio
/// showed.
///
/// Deterministic and cheap: the only session-dependent quantity is the
/// probe's row threshold, read off the session this test actually runs
/// queries on
/// (`options().execution.skip_partial_aggregation_probe_rows_threshold`)
/// rather than hardcoding Ravel's override, so a change to
/// `SKIP_PARTIAL_AGGREGATION_PROBE_ROWS` cannot silently stop this test from
/// exercising the skip path. An all-distinct key makes the probe's ratio side
/// of the decision 1.0, which clears whatever ratio threshold is configured
/// as long as it is below 1.0 (a precondition `skip_partial_aggregation`
/// requires to do anything at all).
///
/// Prove-the-test: temporarily change
/// `ExactIntegerAvgGroupsAccumulator::supports_convert_to_state` in
/// crates/ravel-sql/src/avg.rs to return `false`. `skipped_aggregation_rows`
/// falls to 0 and the first assertion below fails.
#[test]
fn exact_integer_avg_takes_part_in_the_partial_aggregation_skip() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime builds");

    const PARTITIONS: usize = 2;

    let probe_rows = skip_probe_session(true, PARTITIONS, 1)
        .state()
        .config()
        .options()
        .execution
        .skip_partial_aggregation_probe_rows_threshold;
    // Past the probe prefix, so each partition's decision is already made.
    let rows_per_partition = probe_rows + 4_096;

    const SQL: &str = "SELECT key, avg(v) AS a, count(*) AS c FROM t GROUP BY key";

    let run = |skip_partial_aggregation: bool| {
        let ctx = skip_probe_session(skip_partial_aggregation, PARTITIONS, rows_per_partition);
        runtime.block_on(async {
            let plan = ctx
                .sql(SQL)
                .await
                .expect("plan")
                .create_physical_plan()
                .await
                .expect("physical plan");
            let mut stream =
                execute_stream(Arc::clone(&plan), ctx.task_ctx()).expect("stream starts");
            let mut batches = Vec::new();
            while let Some(batch) = stream.next().await {
                batches.push(batch.expect("batch"));
            }
            (plan, batches)
        })
    };

    let (skip_plan, skip_batches) = run(true);
    let (_, stock_batches) = run(false);

    let skipped = sum_metric(&skip_plan, "skipped_aggregation_rows");
    assert!(
        skipped > 0,
        "exact integer AVG's partial aggregation skipped 0 rows over \
         {PARTITIONS} partitions of {rows_per_partition} all-distinct keys \
         each (probe threshold {probe_rows}); \
         ExactIntegerAvgGroupsAccumulator::supports_convert_to_state must have \
         stopped returning true, so the partial stage built a hash table \
         instead of forwarding rows.\n{}",
        datafusion::physical_plan::displayable(skip_plan.as_ref()).indent(true)
    );

    assert_eq!(
        extract_rows(&skip_batches),
        extract_rows(&stock_batches),
        "skip-enabled and skip-disabled runs of the same query produced \
         different (key, avg, count) rows"
    );
}

/// Heap-profiling matrix: 17 configurations over [`ROWS`] rows and
/// [`DISTINCT`] groups. CI's `cargo test` runs the dev profile, which is
/// unoptimized, and this test's dev-profile runtime there has never been
/// measured; only two of the 17 printed configurations are asserted on below,
/// the rest are diagnostic context for a reader chasing a regression in the
/// other 15. Run it explicitly, in `--release` (the profile the bands below
/// were measured in):
///
/// ```sh
/// cargo test -p ravel-sql --release --test group_by_reservation_lag -- --ignored --nocapture
/// ```
#[test]
#[ignore = "heap-profiling matrix: run with --release --ignored, see doc comment"]
fn group_by_allocation_lead_over_reservation() {
    /// Peak (live - reserved) bytes for the 32-partition, string-key,
    /// `COUNT(*)` run, as `(low, high)`.
    ///
    /// Basis: five full `--release` runs of this test on this amd64 CI-class
    /// host (16 vCPU, 30 GB RAM) measured 84.6-94.7 MiB for this configuration
    /// (1.5M groups). The band is that range widened for scheduling variation
    /// (the peak depends on how much hash-table and repartition state is in
    /// flight when the sampler fires), kept wide on the low side since a
    /// tighter upstream or Ravel fix should not fail this test. A reading
    /// above `high` means allocation now leads reservation by more than it
    /// did, which is the regression this detects; below `low` means the gap
    /// closed, and the band should be tightened to the new figure.
    ///
    /// Re-measured with the aggregate hold (issue #2633): three runs read
    /// 83.9, 91.5 and 94.5 MiB, and one with the hold disabled 93.0 MiB. This
    /// configuration's peak gap falls before the first result batch, which
    /// the hold does not touch, so the band stands.
    const GAP_BAND_32_PARTITION_STRING_KEY: (usize, usize) = (60 << 20, 110 << 20);

    /// Bound on peak live bytes of the exact `AVG` run over those of the
    /// `COUNT(*)` run at the same partition count and key.
    ///
    /// Before `convert_to_state` existed on the exact integer `AVG`
    /// accumulator its partial aggregation could never skip, so every partial
    /// group table was built and its emitted state sat unreserved in the
    /// exchange. Five `--release` runs on this amd64 host with
    /// `supports_convert_to_state` forced back to `false` (the mutation
    /// `exact_integer_avg_takes_part_in_the_partial_aggregation_skip` below
    /// also catches) measured a 2.10x-2.57x ratio at 8 partitions and
    /// 1.66x-1.96x at 32. With it restored, five runs measured 1.09x-1.21x at
    /// 8 partitions and 1.07x-1.14x at 32. The bound sits between the two,
    /// closer to the fixed side.
    const MAX_AVG_LIVE_OVER_COUNT_LIVE: f64 = 1.5;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .expect("runtime builds");
    let parts = input_partitions();

    let mut runs: Vec<(&str, Run)> = Vec::new();
    // Series 1: target_partitions, string key, each aggregate.
    for agg in [Agg::Count, Agg::Sum, Agg::Avg] {
        for p in [1, 8, 32] {
            runs.push((
                "partitions",
                run_once(&runtime, &parts, p, Key::Str, agg, Variant::Ravel),
            ));
        }
    }
    // Series 2: key shape at the base partition count, COUNT(*).
    for key in [Key::TwoInt, Key::StrInt] {
        runs.push((
            "key",
            run_once(
                &runtime,
                &parts,
                BASE_PARTITIONS,
                key,
                Agg::Count,
                Variant::Ravel,
            ),
        ));
    }

    // Series 3: Ravel's session against a plain DataFusion session, same pool
    // and partition knobs.
    for agg in [Agg::Count, Agg::Avg] {
        runs.push((
            "engine",
            run_once(
                &runtime,
                &parts,
                BASE_PARTITIONS,
                Key::Str,
                agg,
                Variant::Plain,
            ),
        ));
    }

    // Series 4: the same comparison without a string key.
    for variant in [Variant::Ravel, Variant::Plain] {
        for agg in [Agg::Count, Agg::Avg] {
            runs.push((
                "int-avg",
                run_once(&runtime, &parts, BASE_PARTITIONS, Key::TwoInt, agg, variant),
            ));
        }
    }

    eprintln!(
        "\n{:<44} {:>9} {:>11} {:>13} {:>11} {:>7} {:>11}",
        "configuration", "groups", "peak live", "peak reserved", "peak gap", "ratio", "build gap"
    );
    for (_, r) in &runs {
        eprintln!(
            "{:<44} {:>9} {:>9} M {:>11} M {:>9} M {:>7.2} {:>9} M  ({} samples)",
            r.label,
            r.groups,
            mib(r.peak_live),
            mib(r.peak_reserved),
            mib(r.peak_gap),
            r.peak_gap as f64 / r.peak_reserved.max(1) as f64,
            mib(r.peak_gap_build),
            r.samples
        );
    }

    for (_, r) in &runs {
        assert!(
            r.groups >= DISTINCT,
            "{}: {} groups, expected at least {DISTINCT}",
            r.label,
            r.groups
        );
        assert!(
            r.samples >= 50,
            "{}: sampler took {} samples, too few to trust a peak",
            r.label,
            r.samples
        );
        assert!(r.peak_reserved > 0, "{}: nothing was reserved", r.label);
    }

    let (_, gated) = runs
        .iter()
        .find(|(_, r)| {
            r.partitions == 32 && r.label.contains("key=Str ") && r.label.contains("agg=Count")
        })
        .expect("the 32-partition string-key COUNT run exists");
    for p in [BASE_PARTITIONS, 32] {
        let live_of = |agg: &str| {
            runs.iter()
                .find(|(series, r)| {
                    *series == "partitions"
                        && r.partitions == p
                        && r.label.contains("key=Str ")
                        && r.label.contains(agg)
                })
                .map(|(_, r)| r.peak_live as f64)
                .expect("the run exists")
        };
        let ratio = live_of("agg=Avg") / live_of("agg=Count");
        assert!(
            ratio <= MAX_AVG_LIVE_OVER_COUNT_LIVE,
            "exact AVG peak live is {ratio:.2}x COUNT(*)'s at {p} partitions, over the \
             {MAX_AVG_LIVE_OVER_COUNT_LIVE}x bound"
        );
    }

    let (low, high) = GAP_BAND_32_PARTITION_STRING_KEY;
    assert!(
        (low..=high).contains(&gated.peak_gap),
        "32-partition string-key peak gap {} bytes is outside the band {low}..={high}",
        gated.peak_gap
    );
}
