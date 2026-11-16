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
//! thread reads live bytes (allocated minus deallocated, less the bytes live
//! before the query started, so the input table is excluded), the pool's
//! `reserved()` and `sql_memory_aggregate_envelope_bytes()` once a
//! millisecond on every row. Each statement is planned first and the plan's
//! aggregate profile set on the pool, as the executor does, so the envelope
//! is the one the default sizes from that layout. The gap is live minus
//! reserved minus envelope; the peak gap is
//! its maximum over the run. `reserved()` includes the undercount factor
//! (issue #3181), and the envelope is charged on the process budget only, so
//! the gap is what neither the query's ledger nor the process ledger holds.
//! Result batches are dropped as they arrive.
//!
//! This file's statements carry no limit, no grouping set and no ordered
//! input, so on DataFusion 55.2 they run on the migrated aggregate streams.
//! Each row splits its peak gap by phase: `start` (before the first result
//! batch and before any migrated aggregate stream grew), `grown` (before the
//! first result batch, after one had grown) and `emit` (from the first result
//! batch on). On DataFusion 55.2 with the default factor and layout envelope,
//! in four runs sampled every millisecond, the build phase stayed within
//! 26 MiB on every row (q33's among them) but the 8-partition string-key
//! ones, which read 13.5-80.0 MiB, up to 17.2 MiB of it before any aggregate stream
//! had grown; at the Ravel `COUNT(*)` row's largest live step, 27.7-39.6 MiB
//! of the rise was unreserved against 4.6-32.9 MiB of envelope. The emit
//! phase carries the largest gaps (q33's read 271, 461, 916 and 347 MiB): a
//! stream materializes every group while its state is still live, and slices
//! of that output stay referenced after the stream has resized its
//! reservation to zero. The build phase is held to [`MAX_BUILD_PEAK_GAP`];
//! the emit phase is reported and not bounded. The
//! unreserved part of q33's largest build-phase live step stayed under the
//! envelope held at that sample in all four runs.
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
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, StringViewArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result as DFResult;
use datafusion::datasource::MemTable;
use datafusion::execution::memory_pool::{MemoryLimit, MemoryPool, MemoryReservation};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::{ExecutionPlan, execute_stream};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use ravel_catalog::Snapshot;
use ravel_object_store::memory::MemoryStore;
use ravel_query::{LogSegmentFetcher, PhaseAccounting};
use ravel_sql::{
    AggregateReserveAhead, LogsTableProvider, SessionTable, SpillDecision, SqlConfig,
    TenantDelegatingPool, TenantMemoryAccountant, build_session, plan_aggregate_profile,
    session_config, sql_memory_aggregate_envelope_bytes,
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

/// The q33-shaped row: `GROUP BY watch_id, client_ip` with `COUNT(*)`,
/// `SUM` and exact `AVG` over all-distinct key pairs, at 32 partitions, so
/// each final aggregate stream builds about [`Q33_GROUPS_PER_PARTITION`]
/// groups, the size of T1's measured q33 series on issue #3181.
const Q33_PARTITIONS: usize = 32;
const Q33_GROUPS_PER_PARTITION: usize = 3_100_000;
const Q33_ROWS: usize = Q33_PARTITIONS * Q33_GROUPS_PER_PARTITION;

/// Bound on every Ravel row's build-phase peak of `live - pool.reserved() -
/// sql_memory_aggregate_envelope_bytes()` (`peak_gap_build`). The factor and
/// the envelope cover a stream's next step while it builds; neither covers a
/// stream's allocations before its first reservation, which is most of the
/// 8-partition string-key rows' build-phase gap (13.5-80.0 MiB over the four
/// runs the module doc cites), so the bound sits above that. The emit phase
/// is printed and not bounded: a stream's materialized output stays
/// referenced after its reservation reaches zero, and once the stream drops
/// the pool can no longer hold it.
const MAX_BUILD_PEAK_GAP: usize = 96 << 20;

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
    /// `COUNT(*)`, `SUM(v)` and exact `AVG(v)` in one statement.
    CountSumAvg,
}

/// How often the sampler reads live and reserved bytes.
const SAMPLE_INTERVAL: Duration = Duration::from_millis(1);

/// Fewest samples a row may take. The fastest rows, the 3M-row two-integer
/// keys, ran in 34-68 ms in `--release` and took 32-64 samples in four runs.
const MIN_SAMPLES: usize = 25;

/// Forwards to the query's pool and records, beside it, what DataFusion asked
/// for: the net bytes every reservation declared, and the largest single
/// grow a migrated aggregate stream made (`J_max` on issue #3181). The pool
/// it wraps is the `TenantDelegatingPool` from `SqlConfig::query_pool`, whose
/// `reserved()` is in charged units.
#[derive(Debug)]
struct AskedProbe {
    inner: Arc<dyn MemoryPool>,
    asked: AtomicIsize,
    largest_step: AtomicUsize,
}

impl AskedProbe {
    fn new(inner: Arc<dyn MemoryPool>) -> Self {
        Self {
            inner,
            asked: AtomicIsize::new(0),
            largest_step: AtomicUsize::new(0),
        }
    }

    fn grown(&self, reservation: &MemoryReservation, additional: usize) {
        self.asked.fetch_add(additional as isize, Ordering::Relaxed);
        let name = reservation.consumer().name();
        if [
            "PartialHashAggregateStream[",
            "FinalHashAggregateStream[",
            "SingleHashAggregateStream[",
        ]
        .iter()
        .any(|prefix| name.starts_with(prefix))
        {
            self.largest_step.fetch_max(additional, Ordering::Relaxed);
        }
    }
}

impl fmt::Display for AskedProbe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AskedProbe({})", self.inner)
    }
}

impl MemoryPool for AskedProbe {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn register(&self, consumer: &datafusion::execution::memory_pool::MemoryConsumer) {
        self.inner.register(consumer);
    }

    fn unregister(&self, consumer: &datafusion::execution::memory_pool::MemoryConsumer) {
        self.inner.unregister(consumer);
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.grown(reservation, additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
        self.asked.fetch_sub(shrink as isize, Ordering::Relaxed);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> DFResult<()> {
        self.inner.try_grow(reservation, additional)?;
        self.grown(reservation, additional);
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
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
    variant: Variant,
    key: Key,
    partitions: usize,
    groups: usize,
    peak_live: usize,
    peak_reserved: usize,
    peak_envelope: usize,
    /// Peak of `live - reserved - envelope`.
    peak_gap: usize,
    /// Peak gap before the first result batch reached the consumer: the
    /// accumulation phase, where the group tables only grow.
    peak_gap_build: usize,
    /// The part of `peak_gap_build` sampled before any migrated aggregate
    /// stream had grown, when no stream has an envelope yet.
    peak_gap_start: usize,
    /// The part of `peak_gap_build` sampled after the first aggregate grow:
    /// the window the factor and the envelope are sized for.
    peak_gap_grown: usize,
    /// Peak gap from the first result batch on.
    peak_gap_emit: usize,
    /// Peak of `live - asked` before the first result batch: how far
    /// allocation led what DataFusion had declared while the tables grew,
    /// before the factor and the envelope.
    peak_lead: usize,
    /// Largest single grow of a migrated aggregate stream.
    largest_step: usize,
    /// The build-phase sample at which live bytes rose most since the
    /// previous sample.
    largest_live_step: LiveStep,
    samples: usize,
}

/// One sample-to-sample rise in live bytes, the part of it the pool's
/// `reserved()` did not rise by, and the envelope held at that sample.
#[derive(Clone, Copy, Debug, Default)]
struct LiveStep {
    live: usize,
    unaccounted: usize,
    envelope: usize,
}

impl Run {
    /// The largest multiple of `J_max` the lead reached, of the two the
    /// envelope is sized against: a Vec realloc that moves (4 x J) and the
    /// plain copy of the next block (2 x J).
    fn lead_reached(&self) -> &'static str {
        let j = self.largest_step.max(1);
        if self.peak_lead >= 4 * j {
            "4J"
        } else if self.peak_lead >= 2 * j {
            "2J"
        } else {
            "<2J"
        }
    }
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

/// q33's input: `(watch_id, client_ip)` pairs that are all distinct, so the
/// group count equals the row count, and `v` the aggregated column. Rows are
/// dealt round-robin by batch over [`Q33_PARTITIONS`] input partitions.
fn q33_partitions() -> Vec<Vec<RecordBatch>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("watch_id", DataType::Int64, false),
        Field::new("client_ip", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let mut parts: Vec<Vec<RecordBatch>> = (0..Q33_PARTITIONS).map(|_| Vec::new()).collect();
    let mut start = 0usize;
    let mut batch_no = 0usize;
    while start < Q33_ROWS {
        let end = (start + BATCH_ROWS).min(Q33_ROWS);
        let watch: Int64Array = (start..end)
            .map(|i| (i as i64).wrapping_mul(0x9E37_79B9_7F4A_7C15_u64 as i64))
            .collect();
        let ip: Int64Array = (start..end).map(|i| (i % 7_919) as i64).collect();
        let v: Int64Array = (start..end).map(|i| (i % 1_000) as i64).collect();
        let columns: Vec<ArrayRef> = vec![Arc::new(watch), Arc::new(ip), Arc::new(v)];
        parts[batch_no % Q33_PARTITIONS]
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
        Agg::Count => "count(*) AS a",
        Agg::Sum => "sum(v) AS a",
        Agg::Avg => "avg(v) AS a",
        Agg::CountSumAvg => "count(*) AS c, sum(v) AS s, avg(v) AS a",
    };
    format!("SELECT {keys}, {agg} FROM t GROUP BY {keys}")
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
    assert_eq!(
        config.aggregate_reserve_ahead,
        AggregateReserveAhead::DEFAULT,
        "the matrix measures the default reserve-ahead"
    );
    let plain = matches!(variant, Variant::Plain);
    let tenant = TenantMemoryAccountant::new(64 << 30);
    let (query_pool, _breach) = config.query_pool(tenant, QueryAccounting::new());
    let pool = Arc::new(AskedProbe::new(Arc::clone(&query_pool)));

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
            &ravel_sql::SessionPrincipal::without_capabilities(ravel_types::TenantHash([0u8; 16])),
        )
        .expect("session builds")
    };
    let schema = parts[0][0].schema();
    let table = MemTable::try_new(schema, parts.to_vec()).expect("mem table builds");
    ctx.register_table("t", Arc::new(table))
        .expect("table registers");

    let baseline = live_bytes();
    let stop = Arc::new(AtomicBool::new(false));
    let peak_live = Arc::new(AtomicIsize::new(0));
    let peak_reserved = Arc::new(AtomicUsize::new(0));
    let peak_envelope = Arc::new(AtomicUsize::new(0));
    let peak_gap = Arc::new(AtomicIsize::new(isize::MIN));
    let peak_gap_build = Arc::new(AtomicIsize::new(isize::MIN));
    let peak_gap_start = Arc::new(AtomicIsize::new(isize::MIN));
    let peak_gap_grown = Arc::new(AtomicIsize::new(isize::MIN));
    let peak_gap_emit = Arc::new(AtomicIsize::new(isize::MIN));
    let peak_lead = Arc::new(AtomicIsize::new(isize::MIN));
    let emitting = Arc::new(AtomicBool::new(false));
    let samples = Arc::new(AtomicUsize::new(0));
    let sampler = {
        let (stop, peak_live, peak_reserved, peak_envelope) = (
            Arc::clone(&stop),
            Arc::clone(&peak_live),
            Arc::clone(&peak_reserved),
            Arc::clone(&peak_envelope),
        );
        let (peak_gap_start, peak_gap_grown, peak_gap_emit) = (
            Arc::clone(&peak_gap_start),
            Arc::clone(&peak_gap_grown),
            Arc::clone(&peak_gap_emit),
        );
        let (peak_gap, peak_gap_build, peak_lead, emitting, samples) = (
            Arc::clone(&peak_gap),
            Arc::clone(&peak_gap_build),
            Arc::clone(&peak_lead),
            Arc::clone(&emitting),
            Arc::clone(&samples),
        );
        let pool = Arc::clone(&pool);
        thread::spawn(move || {
            let t0 = Instant::now();
            let mut next_trace = 0u128;
            let mut trace: Vec<(u128, isize, usize, usize, bool)> = Vec::new();
            let mut previous: Option<(isize, usize)> = None;
            let mut largest_live_step = LiveStep::default();
            while !stop.load(Ordering::Acquire) {
                let live = live_bytes() - baseline;
                let reserved = pool.reserved();
                let envelope = sql_memory_aggregate_envelope_bytes() as usize;
                let asked = pool.asked.load(Ordering::Relaxed);
                if let Some((previous_live, previous_reserved)) = previous.replace((live, reserved))
                    && !emitting.load(Ordering::Acquire)
                {
                    let rise = (live - previous_live).max(0) as usize;
                    if rise > largest_live_step.live {
                        largest_live_step = LiveStep {
                            live: rise,
                            unaccounted: rise
                                .saturating_sub(reserved.saturating_sub(previous_reserved)),
                            envelope,
                        };
                    }
                }
                peak_live.fetch_max(live, Ordering::Relaxed);
                peak_reserved.fetch_max(reserved, Ordering::Relaxed);
                peak_envelope.fetch_max(envelope, Ordering::Relaxed);
                let gap = live - reserved as isize - envelope as isize;
                let emit = emitting.load(Ordering::Acquire);
                peak_gap.fetch_max(gap, Ordering::Relaxed);
                if !emit {
                    peak_gap_build.fetch_max(gap, Ordering::Relaxed);
                    peak_lead.fetch_max(live - asked, Ordering::Relaxed);
                }
                let phase = if emit {
                    &peak_gap_emit
                } else if pool.largest_step.load(Ordering::Relaxed) == 0 {
                    &peak_gap_start
                } else {
                    &peak_gap_grown
                };
                phase.fetch_max(gap, Ordering::Relaxed);
                samples.fetch_add(1, Ordering::Relaxed);
                let ms = t0.elapsed().as_millis();
                if ms >= next_trace {
                    trace.push((ms, live, reserved, envelope, emit));
                    next_trace = ms + 20;
                }
                thread::sleep(SAMPLE_INTERVAL);
            }
            (trace, largest_live_step)
        })
    };

    let sql = sql_for(key, agg);
    let started = Instant::now();
    let emitting_flag = Arc::clone(&emitting);
    let groups = runtime.block_on(async {
        let plan = ctx
            .sql(&sql)
            .await
            .expect("query plans")
            .create_physical_plan()
            .await
            .expect("physical plan builds");
        // The executor sets the plan's aggregate profile on the pool before
        // `execute_stream`; this test drives DataFusion directly, so it does
        // the same.
        if let Some(profile) = plan_aggregate_profile(&plan) {
            query_pool
                .downcast_ref::<TenantDelegatingPool>()
                .expect("the query pool is a TenantDelegatingPool")
                .set_aggregate_profile(profile);
        }
        let mut stream = execute_stream(plan, ctx.task_ctx()).expect("stream starts");
        let mut groups = 0usize;
        while let Some(batch) = stream.next().await {
            emitting_flag.store(true, Ordering::Release);
            groups += batch.expect("batch").num_rows();
        }
        groups
    });
    let elapsed = started.elapsed();
    stop.store(true, Ordering::Release);
    let (trace, largest_live_step) = sampler.join().expect("sampler joins");
    if std::env::var("RAVEL_SQL_GAP_TRACE").as_deref() == Ok("1") {
        for (ms, live, reserved, envelope, emit) in &trace {
            eprintln!(
                "TRACE P={partitions} {key:?} {agg:?} t={ms}ms live={} reserved={} \
                 envelope={} gap={} emit={emit}",
                mib(*live as usize),
                mib(*reserved),
                mib(*envelope),
                mib((*live - *reserved as isize - *envelope as isize).max(0) as usize)
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
        variant,
        key,
        partitions,
        groups,
        peak_live: peak_live.load(Ordering::Relaxed).max(0) as usize,
        peak_reserved: peak_reserved.load(Ordering::Relaxed),
        peak_envelope: peak_envelope.load(Ordering::Relaxed),
        peak_gap: peak_gap.load(Ordering::Relaxed).max(0) as usize,
        peak_gap_build: peak_gap_build.load(Ordering::Relaxed).max(0) as usize,
        peak_gap_start: peak_gap_start.load(Ordering::Relaxed).max(0) as usize,
        peak_gap_grown: peak_gap_grown.load(Ordering::Relaxed).max(0) as usize,
        peak_gap_emit: peak_gap_emit.load(Ordering::Relaxed).max(0) as usize,
        peak_lead: peak_lead.load(Ordering::Relaxed).max(0) as usize,
        largest_step: pool.largest_step.load(Ordering::Relaxed),
        largest_live_step,
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
        &ravel_sql::SessionPrincipal::without_capabilities(ravel_types::TenantHash([0u8; 16])),
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
/// avg.rs) is what makes that possible. DataFusion 55.2 has no per-accumulator
/// gate on the skip: `convert_to_state` is a required `GroupsAccumulator`
/// method, and every partial stream with one grouping set builds the probe
/// once the ratio threshold is below 1.0. So the first assertion below guards
/// Ravel's session turning the probe on, and the second guards the states
/// `convert_to_state` forwards, which must merge to the rows a stock partial
/// aggregation gives.
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
/// Prove-the-test: in crates/ravel-sql/src/avg.rs, temporarily change
/// `counts.push(1i64)` in `ExactIntegerAvgGroupsAccumulator::convert_to_state`
/// to `counts.push(2i64)`. Every forwarded row then counts twice, each
/// skipped group's average halves, and the second assertion below fails.
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
         each (probe threshold {probe_rows}); the session no longer sets a \
         skip ratio threshold below 1.0 (session.rs \
         SKIP_PARTIAL_AGGREGATION_PROBE_RATIO), or DataFusion stopped \
         building the probe for this plan, so the partial stage built a hash \
         table instead of forwarding rows.\n{}",
        datafusion::physical_plan::displayable(skip_plan.as_ref()).indent(true)
    );

    assert_eq!(
        extract_rows(&skip_batches),
        extract_rows(&stock_batches),
        "skip-enabled and skip-disabled runs of the same query produced \
         different (key, avg, count) rows; the states \
         ExactIntegerAvgGroupsAccumulator::convert_to_state forwards for \
         skipped rows no longer merge to the folded result"
    );
}

/// Heap-profiling matrix: 17 configurations over [`ROWS`] rows and
/// [`DISTINCT`] groups, plus the q33-shaped row over [`Q33_ROWS`] rows. CI's
/// `cargo test` runs the dev profile, which is unoptimized, and this test's
/// dev-profile runtime there has never been measured. Every Ravel row's build
/// phase is held to [`MAX_BUILD_PEAK_GAP`]; two of the 3M-row configurations also carry the bands
/// below. The `lead` column says whether the build-phase lead of live bytes
/// over what DataFusion asked for reached 4 or 2 times the largest single
/// aggregate grow (`J_max`). Run it explicitly, in `--release` (the profile
/// the bands below were measured in):
///
/// ```sh
/// cargo test -p ravel-sql --release --test group_by_reservation_lag -- --ignored --nocapture
/// ```
#[test]
#[ignore = "heap-profiling matrix: run with --release --ignored, see doc comment"]
fn group_by_allocation_lead_over_reservation() {
    /// Peak (live - reserved - envelope) bytes for the 32-partition,
    /// string-key, `COUNT(*)` run, as `(low, high)`.
    ///
    /// Basis: four `--release` runs of this test on DataFusion 55.2 with the
    /// default factor and the envelope sized from the plan's layout (issue
    /// #3181), sampled every millisecond, on an amd64 CI-class host (16 vCPU,
    /// 30 GB RAM), measured 22.4, 17.9, 4.8 and 20.0 MiB for this configuration
    /// (1.5M groups), each peak in the emit phase; before the factor and the
    /// envelope five runs measured 84.6-94.7 MiB. The band is the measured
    /// range widened for scheduling variation (the peak depends on how much
    /// output is still referenced when the sampler fires). A reading above
    /// `high` means allocation leads the ledgers by more than it did, which
    /// is the regression this detects; below `low` means the gap closed
    /// further, and the band should be tightened to the new figure.
    const GAP_BAND_32_PARTITION_STRING_KEY: (usize, usize) = (2 << 20, 40 << 20);

    /// Bound on peak live bytes of the exact `AVG` run over those of the
    /// `COUNT(*)` run at the same partition count and key.
    ///
    /// Before `convert_to_state` existed on the exact integer `AVG`
    /// accumulator its partial aggregation could never skip, so every partial
    /// group table was built and its emitted state sat unreserved in the
    /// exchange. Five `--release` runs on this amd64 host on DataFusion 54.1,
    /// with the accumulator's `supports_convert_to_state` forced back to
    /// `false`, measured a 2.10x-2.57x ratio at 8 partitions and 1.66x-1.96x
    /// at 32. With it restored, five runs measured 1.09x-1.21x at 8 partitions
    /// and 1.07x-1.14x at 32. The bound sits between the two, closer to the
    /// fixed side. DataFusion 55.2 removed that gate, so the unskipped
    /// configuration can no longer be produced by flipping it; three runs on
    /// DataFusion 55.2 measured 1.11x-1.17x at 8 partitions and 1.06x-1.22x
    /// at 32.
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
    drop(parts);

    // Series 5: q33's shape at its own scale.
    let q33_parts = q33_partitions();
    runs.push((
        "q33",
        run_once(
            &runtime,
            &q33_parts,
            Q33_PARTITIONS,
            Key::TwoInt,
            Agg::CountSumAvg,
            Variant::Ravel,
        ),
    ));
    drop(q33_parts);

    eprintln!(
        "\n{:<40} {:>8} {:>7} {:>7} {:>7} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>4} {:>6} {:>6} {:>6}",
        "configuration",
        "groups",
        "live",
        "reserv",
        "envel",
        "gap",
        "build",
        "start",
        "grown",
        "emit",
        "lead",
        "J_max",
        "lead",
        "step",
        "unacc",
        "env@st"
    );
    for (_, r) in &runs {
        eprintln!(
            "{:<40} {:>8} {:>7} {:>7} {:>7} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>4} {:>6} {:>6} {:>6}  ({} samples)",
            r.label,
            r.groups,
            mib(r.peak_live),
            mib(r.peak_reserved),
            mib(r.peak_envelope),
            mib(r.peak_gap),
            mib(r.peak_gap_build),
            mib(r.peak_gap_start),
            mib(r.peak_gap_grown),
            mib(r.peak_gap_emit),
            mib(r.peak_lead),
            mib(r.largest_step),
            r.lead_reached(),
            mib(r.largest_live_step.live),
            mib(r.largest_live_step.unaccounted),
            mib(r.largest_live_step.envelope),
            r.samples
        );
    }
    for (_, r) in runs.iter().filter(|(_, r)| {
        matches!((r.variant, r.key), (Variant::Ravel, Key::Str)) && r.partitions == BASE_PARTITIONS
    }) {
        eprintln!(
            "{}: build-phase margin under the {MAX_BUILD_PEAK_GAP}-byte bound {} bytes",
            r.label,
            MAX_BUILD_PEAK_GAP as isize - r.peak_gap_build as isize
        );
    }

    for (series, r) in &runs {
        let expected_groups = if *series == "q33" { Q33_ROWS } else { DISTINCT };
        assert!(
            r.groups >= expected_groups,
            "{}: {} groups, expected at least {expected_groups}",
            r.label,
            r.groups
        );
        assert!(
            r.samples >= MIN_SAMPLES,
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

    let (_, q33) = runs
        .iter()
        .find(|(series, _)| *series == "q33")
        .expect("the q33 run exists");
    let step = q33.largest_live_step;
    assert!(
        step.envelope >= step.unaccounted,
        "q33: the largest build-phase live step rose {} bytes, {} of them not \
         reserved, while the envelope held {} bytes",
        step.live,
        step.unaccounted,
        step.envelope
    );

    let (low, high) = GAP_BAND_32_PARTITION_STRING_KEY;
    assert!(
        (low..=high).contains(&gated.peak_gap),
        "32-partition string-key peak gap {} bytes is outside the band {low}..={high}",
        gated.peak_gap
    );

    let ravel: Vec<&Run> = runs
        .iter()
        .map(|(_, r)| r)
        .filter(|r| matches!(r.variant, Variant::Ravel))
        .collect();
    let over: Vec<String> = ravel
        .iter()
        .filter(|r| r.peak_gap_build > MAX_BUILD_PEAK_GAP)
        .map(|r| {
            format!(
                "{}: build-phase peak gap {} bytes (start {}, grown {}; emit {})",
                r.label, r.peak_gap_build, r.peak_gap_start, r.peak_gap_grown, r.peak_gap_emit
            )
        })
        .collect();
    assert!(
        over.is_empty(),
        "{} of {} Ravel rows have a build-phase peak gap (live - reserved - envelope) \
         over the {MAX_BUILD_PEAK_GAP}-byte bound:\n{}",
        over.len(),
        ravel.len(),
        over.join("\n")
    );
}
