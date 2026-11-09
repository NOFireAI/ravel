//! Issue #4014: what a building hash aggregate allocates above what it is
//! charged, against the reserve-ahead envelope the pool holds for it.
//!
//! ClickBench statement 9, `SELECT RegionID, COUNT(DISTINCT UserID) ...
//! GROUP BY RegionID`, runs on two aggregate shapes:
//!
//! - class (a): DataFusion's `SingleDistinctToGroupBy` rewrites it to a
//!   group-only aggregate over `(RegionID, UserID)` under a `COUNT` by
//!   `RegionID`. Measured with `Int64` keys and with a `Utf8View` `RegionID`.
//! - class (b): with a `COUNT(*)` beside it the rewrite does not apply, and
//!   one aggregate by `RegionID` keeps a hash set of `(group, UserID)` pairs
//!   in its `COUNT(DISTINCT)` accumulator.
//!
//! Method. The binary installs `stats_alloc`'s instrumented allocator (the
//! workspace denies `unsafe`). The input is one partition of pre-built
//! batches, and the source records the allocator's counters, the pool's
//! `reserved()` and `sql_memory_aggregate_envelope_bytes()` each time the
//! plan pulls a batch, on a current-thread runtime, so the span between two
//! pulls is exactly one input batch through the aggregate. Over that span the
//! peak of live bytes is at most live bytes at its start plus every byte
//! allocated in it, plus the net growth of every `realloc` in it: `stats_alloc`
//! counts a growing `realloc` as its growth only, and the block it moves from
//! is live until the move ends. That is an upper bound while every growing
//! `realloc` at least doubles its block (`Vec`'s amortized growth and arrow's
//! `MutableBuffer` both do) and none shrinks in the span. The span's excess is
//! that peak less the bytes the pool had charged at the span's start, the
//! figure the envelope held at that start must cover.
//!
//! The file has one test so nothing else in the process allocates while it
//! measures.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::alloc::System;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::{ArrayRef, Int64Array, StringViewArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::common::Result as DFResult;
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::execute_stream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use futures::StreamExt;
use ravel_catalog::Snapshot;
use ravel_object_store::memory::MemoryStore;
use ravel_query::{LogSegmentFetcher, PhaseAccounting};
use ravel_sql::{
    AggregateLayout, AggregateProfile, AggregateReserveAhead, LogsTableProvider, SessionTable,
    SpillDecision, SqlConfig, TenantDelegatingPool, TenantMemoryAccountant, build_session,
    plan_aggregate_profile, sql_memory_aggregate_envelope_bytes,
};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use stats_alloc::{INSTRUMENTED_SYSTEM, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

/// Distinct `(RegionID, UserID)` pairs, one per row. Past `7/16 x 2^23` pairs
/// the group table and the distinct set each grow to `2^23` buckets, which is
/// statement 9's step of 58,720,256 bytes for a 16-byte-entry table.
const ROWS: usize = 4_200_000;
/// The distinct set only grows when it doubles, so class (b) takes four times
/// the pairs for two more doublings past 8 MiB asked.
const DISTINCT_SET_ROWS: usize = 4 * ROWS;
const REGIONS: usize = 1_000;
const BATCH_ROWS: usize = 8_192;

/// Fewest aggregate grows a class must show, and the least its largest grow
/// may be, so the comparison covers the large steps the envelope is for.
const MIN_GROWS: usize = 6;
const MIN_LARGEST_STEP: usize = 32 << 20;

const MIGRATED: [&str; 3] = [
    "PartialHashAggregateStream[",
    "FinalHashAggregateStream[",
    "SingleHashAggregateStream[",
];

#[derive(Clone, Copy, Debug)]
enum Class {
    /// Class (a), `Int64` keys.
    GroupOnlyInt,
    /// Class (a), a `Utf8View` `RegionID`.
    GroupOnlyView,
    /// Class (b), `Int64` keys.
    DistinctSet,
}

impl Class {
    fn statement(self) -> &'static str {
        match self {
            Class::GroupOnlyInt | Class::GroupOnlyView => {
                "SELECT \"RegionID\", COUNT(DISTINCT \"UserID\") AS u FROM hits \
                 GROUP BY \"RegionID\" ORDER BY u DESC LIMIT 10"
            }
            Class::DistinctSet => {
                "SELECT \"RegionID\", COUNT(DISTINCT \"UserID\") AS u, COUNT(*) AS c FROM hits \
                 GROUP BY \"RegionID\" ORDER BY u DESC LIMIT 10"
            }
        }
    }

    fn rows(self) -> usize {
        match self {
            Class::GroupOnlyInt | Class::GroupOnlyView => ROWS,
            Class::DistinctSet => DISTINCT_SET_ROWS,
        }
    }

    /// The profile the executor derives for the class's plan.
    fn profile(self) -> AggregateProfile {
        let int_table = AggregateLayout {
            entry_size: 16,
            vec_width_total: 16,
            vec_width_max: 8,
            capacity_accounted: true,
        };
        match self {
            Class::GroupOnlyInt => AggregateProfile::Layout(int_table),
            Class::GroupOnlyView => AggregateProfile::Layout(AggregateLayout {
                entry_size: 32,
                vec_width_total: 24,
                vec_width_max: 16,
                capacity_accounted: false,
            }),
            Class::DistinctSet => AggregateProfile::DistinctSets {
                layout: AggregateLayout {
                    vec_width_total: 24,
                    ..int_table
                },
                sets: 1,
            },
        }
    }
}

/// Forwards to the query's pool and counts what the migrated aggregate
/// streams ask for.
#[derive(Debug)]
struct Probe {
    inner: Arc<dyn MemoryPool>,
    asked: AtomicUsize,
    grows: AtomicUsize,
    largest_step: AtomicUsize,
    shrunk: AtomicBool,
}

impl Probe {
    fn new(inner: Arc<dyn MemoryPool>) -> Self {
        Self {
            inner,
            asked: AtomicUsize::new(0),
            grows: AtomicUsize::new(0),
            largest_step: AtomicUsize::new(0),
            shrunk: AtomicBool::new(false),
        }
    }

    fn migrated(reservation: &MemoryReservation) -> bool {
        let name = reservation.consumer().name();
        MIGRATED.iter().any(|prefix| name.starts_with(prefix))
    }

    fn grown(&self, reservation: &MemoryReservation, additional: usize) {
        if Self::migrated(reservation) && additional > 0 {
            self.asked.fetch_add(additional, Ordering::Relaxed);
            self.grows.fetch_add(1, Ordering::Relaxed);
            self.largest_step.fetch_max(additional, Ordering::Relaxed);
        }
    }
}

impl fmt::Display for Probe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Probe({})", self.inner)
    }
}

impl MemoryPool for Probe {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.grown(reservation, additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
        if Self::migrated(reservation) && shrink > 0 {
            self.asked.fetch_sub(shrink, Ordering::Relaxed);
            self.shrunk.store(true, Ordering::Relaxed);
        }
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

/// The counters at one batch pull.
#[derive(Clone, Copy, Debug)]
struct Sample {
    live: usize,
    allocated: usize,
    reallocated: isize,
    reserved: usize,
    envelope: usize,
    asked: usize,
    grows: usize,
    largest_step: usize,
    shrunk: bool,
}

/// One input partition of pre-built batches that samples the counters each
/// time the plan pulls the next one, the end of the input included.
#[derive(Debug)]
struct SampledSource {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    probe: Arc<Probe>,
    samples: Arc<Mutex<Vec<Sample>>>,
}

impl PartitionStream for SampledSource {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let probe = Arc::clone(&self.probe);
        let samples = Arc::clone(&self.samples);
        let items = self
            .batches
            .clone()
            .into_iter()
            .map(Some)
            .chain(std::iter::once(None));
        let stream = futures::stream::iter(items).filter_map(move |batch| {
            let stats = INSTRUMENTED_SYSTEM.stats();
            let sample = Sample {
                live: live_bytes(),
                allocated: stats.bytes_allocated,
                reallocated: stats.bytes_reallocated,
                reserved: probe.reserved(),
                envelope: sql_memory_aggregate_envelope_bytes() as usize,
                asked: probe.asked.load(Ordering::Relaxed),
                grows: probe.grows.load(Ordering::Relaxed),
                largest_step: probe.largest_step.load(Ordering::Relaxed),
                shrunk: probe.shrunk.load(Ordering::Relaxed),
            };
            samples.lock().unwrap().push(sample);
            std::future::ready(batch.map(Ok))
        });
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        ))
    }
}

fn live_bytes() -> usize {
    let stats = INSTRUMENTED_SYSTEM.stats();
    stats.bytes_allocated - stats.bytes_deallocated
}

fn input(class: Class) -> (SchemaRef, Vec<RecordBatch>) {
    let region_type = match class {
        Class::GroupOnlyView => DataType::Utf8View,
        Class::GroupOnlyInt | Class::DistinctSet => DataType::Int64,
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("RegionID", region_type, false),
        Field::new("UserID", DataType::Int64, false),
    ]));
    let mut batches = Vec::new();
    let mut start = 0usize;
    let rows = class.rows();
    while start < rows {
        let end = (start + BATCH_ROWS).min(rows);
        let region: ArrayRef = match class {
            // Under 12 bytes, so every view is inline and no data buffer
            // grows beside the views.
            Class::GroupOnlyView => Arc::new(
                (start..end)
                    .map(|i| Some(format!("r{:04}", i % REGIONS)))
                    .collect::<StringViewArray>(),
            ),
            Class::GroupOnlyInt | Class::DistinctSet => Arc::new(
                (start..end)
                    .map(|i| (i % REGIONS) as i64)
                    .collect::<Int64Array>(),
            ),
        };
        let user: ArrayRef = Arc::new(
            (start..end)
                .map(|i| (i as i64).wrapping_mul(0x9E37_79B9_7F4A_7C15_u64 as i64))
                .collect::<Int64Array>(),
        );
        batches.push(RecordBatch::try_new(Arc::clone(&schema), vec![region, user]).expect("batch"));
        start = end;
    }
    (schema, batches)
}

/// One batch's span: the most live bytes can have reached in it, against
/// what was charged and enveloped at its start.
#[derive(Clone, Copy, Debug)]
struct Span {
    start: Sample,
    end: Sample,
    peak: usize,
}

impl Span {
    fn excess(&self) -> isize {
        self.peak as isize - self.start.reserved as isize
    }
}

struct Measured {
    class: Class,
    profile: Option<AggregateProfile>,
    spans: Vec<Span>,
}

fn measure(runtime: &tokio::runtime::Runtime, class: Class) -> Measured {
    let mut config = SqlConfig::default();
    config.engine.sql_partition_count = Some(1);
    config.max_query_bytes = 64 << 30;
    assert_eq!(
        config.aggregate_reserve_ahead,
        AggregateReserveAhead::DEFAULT,
        "the test measures the default reserve-ahead"
    );
    let tenant = TenantMemoryAccountant::new(64 << 30);
    let (query_pool, _breach) = config.query_pool(tenant, QueryAccounting::new());
    let probe = Arc::new(Probe::new(Arc::clone(&query_pool)));
    let logs = SessionTable::Logs(Arc::new(LogsTableProvider::new(
        Snapshot {
            segments: Vec::new(),
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        },
        TenantHash([0u8; 16]),
        LogSegmentFetcher::new(Arc::new(MemoryStore::new())),
        PhaseAccounting::new(),
    )));
    let ctx = build_session(
        &config,
        Arc::clone(&probe) as Arc<dyn MemoryPool>,
        logs,
        true,
        SpillDecision::Disabled,
        &ravel_sql::SessionPrincipal::without_capabilities(TenantHash([0u8; 16])),
    )
    .expect("session builds");
    let (schema, batches) = input(class);
    let samples = Arc::new(Mutex::new(Vec::new()));
    let source = SampledSource {
        schema: Arc::clone(&schema),
        batches,
        probe: Arc::clone(&probe),
        samples: Arc::clone(&samples),
    };
    let table = StreamingTable::try_new(schema, vec![Arc::new(source)]).expect("table builds");
    ctx.register_table("hits", Arc::new(table))
        .expect("table registers");

    let (profile, baseline) = runtime.block_on(async {
        let plan = ctx
            .sql(class.statement())
            .await
            .expect("plans")
            .create_physical_plan()
            .await
            .expect("physical plan builds");
        // The probe hides the query pool from the session's profile rule, so
        // set the profile on it as the executor would.
        let profile = plan_aggregate_profile(&plan);
        if let Some(profile) = profile {
            query_pool
                .downcast_ref::<TenantDelegatingPool>()
                .expect("the query pool is a TenantDelegatingPool")
                .set_aggregate_profile(profile);
        }
        let baseline = live_bytes();
        let mut stream = execute_stream(plan, ctx.task_ctx()).expect("stream starts");
        let mut rows = 0usize;
        while let Some(batch) = stream.next().await {
            rows += batch.expect("batch").num_rows();
        }
        assert_eq!(rows, 10, "the statement returns its ten largest regions");
        (profile, baseline)
    });
    drop(ctx);

    let mut samples = samples.lock().unwrap().clone();
    for sample in &mut samples {
        sample.live -= baseline;
    }
    assert_eq!(
        samples.len(),
        class.rows().div_ceil(BATCH_ROWS) + 1,
        "one sample per pull, the end of the input included"
    );
    let spans = samples
        .windows(2)
        .map(|pair| {
            let (start, end) = (pair[0], pair[1]);
            let allocated = end.allocated - start.allocated;
            let moved = (end.reallocated - start.reallocated).max(0) as usize;
            Span {
                start,
                end,
                peak: start.live + allocated + moved,
            }
        })
        .collect();
    Measured {
        class,
        profile,
        spans,
    }
}

fn mib(bytes: f64) -> String {
    format!("{:.2}", bytes / f64::from(1 << 20))
}

/// From 8 MiB asked, every batch's excess is at most the envelope held at its
/// start, for each class, over at least [`MIN_GROWS`] grows and a largest grow
/// of at least [`MIN_LARGEST_STEP`].
/// FLIP: halving the derived table term fails class (a) with `Int64` keys, and
/// halving the set term fails class (b). Not recording the largest grow fails
/// the largest-step assertion.
#[test]
fn a_building_aggregate_allocates_no_more_above_its_charge_than_its_envelope() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime builds");
    for class in [
        Class::GroupOnlyInt,
        Class::GroupOnlyView,
        Class::DistinctSet,
    ] {
        let measured = measure(&runtime, class);
        eprintln!("class {:?}: profile {:?}", measured.class, measured.profile);
        eprintln!(
            "  {:>4} {:>12} {:>11} {:>10} {:>10} {:>10} {:>10} {:>10} {:>7}",
            "grow",
            "asked",
            "step",
            "live MiB",
            "peak MiB",
            "excess MiB",
            "env MiB",
            "largest",
            "ratio"
        );
        // Every span in which a grow landed closes a grow record; its peak and
        // excess are the largest since the previous record.
        let mut grows = 0usize;
        let mut worst_ratio = f64::MIN;
        let mut worst_ratio_from_8_mib = f64::MIN;
        let mut since: Option<Span> = None;
        let mut largest = 0usize;
        let mut covered = 0usize;
        let mut worst_cover = f64::MIN;
        for span in &measured.spans {
            if span.start.shrunk {
                break;
            }
            if span.start.asked >= 8 << 20 {
                assert!(
                    span.excess() <= span.start.envelope as isize,
                    "{class:?}: {} bytes above the charge in one batch at asked {}, \
                     over the envelope of {} held at its start",
                    span.excess(),
                    span.start.asked,
                    span.start.envelope
                );
                covered += usize::from(span.end.grows > span.start.grows);
                worst_cover =
                    worst_cover.max(span.excess() as f64 / span.start.envelope.max(1) as f64);
            }
            since = Some(match since {
                Some(held) if held.excess() >= span.excess() => held,
                _ => *span,
            });
            if span.end.grows == span.start.grows {
                continue;
            }
            grows += 1;
            let worst = since.take().expect("a span is held");
            let step = span.end.asked - span.start.asked;
            largest = largest.max(span.end.largest_step);
            let ratio = worst.excess() as f64 / worst.start.largest_step.max(1) as f64;
            if worst.start.largest_step > 0 {
                worst_ratio = worst_ratio.max(ratio);
                if worst.start.asked >= 8 << 20 {
                    worst_ratio_from_8_mib = worst_ratio_from_8_mib.max(ratio);
                }
            }
            eprintln!(
                "  {grows:>4} {:>12} {step:>11} {:>10} {:>10} {:>10} {:>10} {:>10} {ratio:>7.3}",
                span.end.asked,
                mib(span.end.live as f64),
                mib(worst.peak as f64),
                mib(worst.excess() as f64),
                mib(worst.start.envelope as f64),
                worst.start.largest_step,
            );
        }
        eprintln!(
            "  class {:?}: {grows} grows, largest step {largest}, worst excess/largest_step \
             {worst_ratio:.3}, from 8 MiB asked {worst_ratio_from_8_mib:.3}, worst excess/envelope \
             from 8 MiB asked {worst_cover:.4} over {covered} grows",
            measured.class
        );
        assert_eq!(measured.profile, Some(class.profile()), "{class:?}");
        assert!(
            grows >= MIN_GROWS,
            "{class:?}: {grows} grows, at least {MIN_GROWS} expected"
        );
        assert!(
            covered >= MIN_GROWS,
            "{class:?}: {covered} grows from 8 MiB asked checked against the envelope, \
             at least {MIN_GROWS} expected"
        );
        assert!(
            largest >= MIN_LARGEST_STEP,
            "{class:?}: largest step {largest}, at least {MIN_LARGEST_STEP} expected"
        );
    }
}
