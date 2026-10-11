//! Round-trip cost of one ADR-1702 CPU gate job (issues #2795, #2818), and the
//! cost of freeing a job's result on the caller's thread.
//!
//! `CpuGate::run` is a semaphore permit plus `tokio::task::spawn_blocking`
//! per job. `ravel_cpu_gate_wait_seconds` covers only the permit wait; this
//! bench measures the whole round trip, and the two handoffs inside it: from
//! just before `gate.run` to the job's first instruction on a blocking thread
//! (handoff in), and from the job's last instruction to the await returning
//! (handoff out). It also times, alone, the drop of whatever value a job
//! builds, since a free after `gate.run` returns is seen by no gate counter.
//! Latency, handoff and page read percentiles come from a log-bucketed
//! histogram (relative error under 1.6 percent); drop percentiles from exact
//! samples, at most 16384 per caller.
//!
//! A configuration is one group of variants at one caller count. Every
//! variant of a configuration runs once per run, in an order shuffled per run
//! with a fixed printed seed, so the variants are paired run by run.
//!
//! - `empty` (returns a u64) and `spin` (about 50 us of calibrated busy work):
//!   variants `inline` (the job called directly on the calling task), `gated`
//!   (`gate.run(..)` on a read gate with `inline_floor_bytes = 0`, so every
//!   job takes a permit and goes to the blocking pool) and `bare`
//!   (`tokio::task::spawn_blocking(job).await` with no semaphore, which splits
//!   the gate's own bookkeeping from the blocking pool handoff).
//! - `buffer`: a 768 KiB `Vec<u8>` filled by the job. `buffer.<dispatch>`
//!   returns it, and the caller reads one byte per 4 KiB page (timed and
//!   printed as `page_read`) and then drops it (timed alone);
//!   `buffer_self_drop.<dispatch>` drops it inside the job (timed alone).
//! - `records/rRxaA`: R `ravel_logseg::LogRecord`s, each with A string
//!   attributes, every String and Vec built fresh per record as a decoder
//!   does. `series/s2828xl10`: a remote-write-like request of 2828 series,
//!   each with 10 `(String, String)` labels and one sample. Variants:
//!   `caller_drop` (gated; the caller drops the returned value),
//!   `job_drop` (gated; the job drops it before returning) and `inline` (the
//!   caller builds and drops it on the task). Allocations per value are
//!   counted by construction, checked against the formula, and printed.
//!
//! Under `cargo bench` the run is 5 runs of 1500 ms per configuration. Run any
//! other way (`cargo test --bench cpu_gate_round_trip`, or the binary
//! directly) it is a quick mode: one run of 50 ms, whose verdicts are labelled
//! as not a measurement. A configuration's run length is raised to twice
//! one job's duration per caller queued on a lane, so every caller completes
//! jobs. `RAVEL_BENCH_RUN_MS` overrides the run length in both modes and
//! `RAVEL_BENCH_GATE_PERMITS` the gate's permits (default: the read gate
//! default for this host). Gate counters, drop sample counts, allocation
//! counts, every caller running at least one job, and the presence of every
//! figure and verdict line exactly once are asserted; any failure exits
//! non-zero after the table.
//!
//! The binary runs under jemalloc with the server's background thread
//! configuration, since where a gated job allocates and frees is part of its
//! cost.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::fmt::Write as _;
use std::hint::black_box;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use ravel_cpu_gate::{
    CpuGateConfig, CpuGateSnapshot, DEFAULT_EVAL_FLOOR_SAMPLES, GateSite as _, InstantClock,
    JobSize, ReadGate, ReadSite, default_read_permits,
};
use ravel_logseg::{AttrValue, LogRecord, LogStreamId};

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const SITE: ReadSite = ReadSite::SegmentSection;
const BUFFER_BYTES: usize = 768 * 1024;
const PAGE_BYTES: usize = 4 * 1024;
const SPIN_TARGET: Duration = Duration::from_micros(50);
const CALLERS: [usize; 2] = [1, 16];
const FULL_RUNS: usize = 5;
const FULL_RUN_MS: u64 = 1500;
const QUICK_RUN_MS: u64 = 50;
/// Seed of the per-run variant order shuffle.
const ORDER_SEED: u64 = 0x2795_2818;
/// A configuration runs at least this many job durations per queued caller.
const RUN_FLOOR_JOBS: u32 = 2;

/// Shape (e): R x A, A = 105 being the ClickBench hits column count.
const RECORD_COUNTS: [usize; 2] = [1024, 8192];
const ATTR_COUNTS: [usize; 2] = [16, 105];
const KEY_BYTES: usize = 12;
const VALUE_BYTES: usize = 16;
const BODY_BYTES: usize = 64;
const STREAM_ATTRS_BYTES: usize = 48;
/// Shape (f): the series count of a 64 KiB snappy remote-write request.
const SERIES: usize = 2828;
const LABELS: usize = 10;
const LABEL_NAME_BYTES: usize = 12;
const LABEL_VALUE_BYTES: usize = 20;

/// Pre-registered dispatch band for `empty` at 1 caller, gated minus inline p50.
const DISPATCH_NOT_CAUSE_US: f64 = 30.0;
const DISPATCH_SHARED_CAUSE_US: f64 = 150.0;
/// `buffer.gated` minus `buffer_self_drop.gated` drop p50 above this: the
/// free matters.
const CROSS_THREAD_FREE_US: f64 = 50.0;
/// Pre-registered structured-free band, caller_drop minus job_drop drop p50.
const STRUCTURED_RULED_OUT_US: f64 = 30.0;
const STRUCTURED_CANDIDATE_US: f64 = 150.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Payload {
    Empty,
    Spin,
    Buffer,
    Records { records: usize, attrs: usize },
    Series,
}

impl Payload {
    /// Whether the job builds a value whose drop is timed.
    fn droppable(self) -> bool {
        !matches!(self, Payload::Empty | Payload::Spin)
    }

    /// Strings and Vecs one value holds, by formula, independent of the
    /// builders that count them as they construct.
    fn expected_allocations(self) -> u64 {
        match self {
            Payload::Empty | Payload::Spin => 0,
            Payload::Buffer => 1,
            // The outer Vec; per record stream_attrs, severity_text, body and
            // the attrs Vec; per attribute its key and its value String.
            Payload::Records { records, attrs } => 1 + (records * (4 + 2 * attrs)) as u64,
            // The outer Vec; per series the labels Vec, the samples Vec and
            // two Strings per label.
            Payload::Series => 1 + (SERIES * (2 + 2 * LABELS)) as u64,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Dispatch {
    Inline,
    Gated,
    Bare,
}

/// Where a droppable value is freed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DropSite {
    Caller,
    Job,
}

#[derive(Debug, Clone, Copy)]
struct Variant {
    name: &'static str,
    payload: Payload,
    dispatch: Dispatch,
    drop_site: DropSite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Group {
    /// `empty` or `spin`: inline, gated and bare.
    Dispatch(Payload),
    /// `buffer` and `buffer_self_drop`, each inline, gated and bare.
    Buffer,
    /// `records` or `series`: caller_drop, job_drop and inline.
    Structured(Payload),
}

impl Group {
    fn label(self) -> String {
        match self {
            Group::Dispatch(Payload::Empty) => "empty".to_string(),
            Group::Dispatch(_) => "spin".to_string(),
            Group::Buffer => "buffer".to_string(),
            Group::Structured(Payload::Records { records, attrs }) => {
                format!("records/r{records}xa{attrs}")
            }
            Group::Structured(_) => format!("series/s{SERIES}xl{LABELS}"),
        }
    }

    fn variants(self) -> Vec<Variant> {
        let v = |name, payload, dispatch, drop_site| Variant {
            name,
            payload,
            dispatch,
            drop_site,
        };
        match self {
            Group::Dispatch(p) => vec![
                v("inline", p, Dispatch::Inline, DropSite::Caller),
                v("gated", p, Dispatch::Gated, DropSite::Caller),
                v("bare", p, Dispatch::Bare, DropSite::Caller),
            ],
            Group::Buffer => {
                let b = Payload::Buffer;
                vec![
                    v("buffer.inline", b, Dispatch::Inline, DropSite::Caller),
                    v("buffer.gated", b, Dispatch::Gated, DropSite::Caller),
                    v("buffer.bare", b, Dispatch::Bare, DropSite::Caller),
                    v(
                        "buffer_self_drop.inline",
                        b,
                        Dispatch::Inline,
                        DropSite::Job,
                    ),
                    v("buffer_self_drop.gated", b, Dispatch::Gated, DropSite::Job),
                    v("buffer_self_drop.bare", b, Dispatch::Bare, DropSite::Job),
                ]
            }
            Group::Structured(p) => vec![
                v("caller_drop", p, Dispatch::Gated, DropSite::Caller),
                v("job_drop", p, Dispatch::Gated, DropSite::Job),
                v("inline", p, Dispatch::Inline, DropSite::Caller),
            ],
        }
    }

    fn all() -> Vec<Group> {
        let mut groups = vec![
            Group::Dispatch(Payload::Empty),
            Group::Dispatch(Payload::Spin),
            Group::Buffer,
        ];
        for records in RECORD_COUNTS {
            for attrs in ATTR_COUNTS {
                groups.push(Group::Structured(Payload::Records { records, attrs }));
            }
        }
        groups.push(Group::Structured(Payload::Series));
        groups
    }
}

/// Fixed-size latency histogram in nanoseconds: exact below 64 ns, then 32
/// log-spaced buckets per power of two (relative error under 1.6 percent).
struct Histogram {
    counts: Vec<u64>,
    total: u64,
}

const SUB_BITS: u32 = 5;
const SUB: u64 = 1 << SUB_BITS;
const HIST_BUCKETS: usize = (2 * SUB + (63 - SUB_BITS as u64) * SUB) as usize;

impl Histogram {
    fn new() -> Self {
        Histogram {
            counts: vec![0; HIST_BUCKETS],
            total: 0,
        }
    }

    fn index(ns: u64) -> usize {
        if ns < 2 * SUB {
            return ns as usize;
        }
        let exp = 63 - ns.leading_zeros();
        let shift = exp - SUB_BITS;
        let mantissa = (ns >> shift) - SUB;
        (2 * SUB + u64::from(shift - 1) * SUB + mantissa) as usize
    }

    /// The midpoint of a bucket.
    fn value(index: usize) -> u64 {
        let i = index as u64;
        if i < 2 * SUB {
            return i;
        }
        let k = i - 2 * SUB;
        let shift = k / SUB + 1;
        let low = (SUB + k % SUB) << shift;
        low + (1u64 << shift) / 2
    }

    fn record(&mut self, d: Duration) {
        let ns = u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
        self.counts[Self::index(ns)] += 1;
        self.total += 1;
    }

    fn merge(&mut self, other: &Histogram) {
        for (a, b) in self.counts.iter_mut().zip(&other.counts) {
            *a += b;
        }
        self.total += other.total;
    }

    fn percentile(&self, q: f64) -> Duration {
        if self.total == 0 {
            return Duration::ZERO;
        }
        let rank = ((q * self.total as f64).ceil() as u64).clamp(1, self.total);
        let mut seen = 0;
        for (i, c) in self.counts.iter().enumerate() {
            seen += c;
            if seen >= rank {
                return Duration::from_nanos(Self::value(i));
            }
        }
        Duration::ZERO
    }
}

/// Drop times kept exactly: a structured value's drop runs to tens of
/// milliseconds, where a [`Histogram`] bucket is wider than the verdict band.
/// Memory is bounded by keeping at most [`RESERVOIR_CAP`] samples per caller
/// (reservoir sampling, Algorithm R, seeded per caller).
struct Reservoir {
    kept: Vec<Duration>,
    total: u64,
    rng: OrderRng,
}

const RESERVOIR_CAP: usize = 16 * 1024;

impl Reservoir {
    fn new(seed: u64) -> Self {
        Reservoir {
            kept: Vec::new(),
            total: 0,
            rng: OrderRng(seed),
        }
    }

    fn record(&mut self, d: Duration) {
        self.total += 1;
        if self.kept.len() < RESERVOIR_CAP {
            self.kept.push(d);
        } else {
            let j = (self.rng.next() % self.total) as usize;
            if j < RESERVOIR_CAP {
                self.kept[j] = d;
            }
        }
    }

    fn merge(&mut self, other: &Reservoir) {
        self.kept.extend_from_slice(&other.kept);
        self.total += other.total;
    }

    fn percentile(&self, q: f64) -> Duration {
        let mut sorted = self.kept.clone();
        sorted.sort_unstable();
        if sorted.is_empty() {
            return Duration::ZERO;
        }
        let rank = ((q * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len());
        sorted[rank - 1]
    }
}

/// The payload values a job can return.
enum Value {
    Word(u64),
    Buffer(Vec<u8>),
    Records(Vec<LogRecord>),
    Series(Vec<SeriesPiece>),
}

/// One series of a decoded remote-write request.
#[expect(dead_code, reason = "built only to be dropped")]
struct SeriesPiece {
    labels: Vec<(String, String)>,
    samples: Vec<(i64, f64)>,
}

fn spin(iterations: u64) -> u64 {
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    for i in 0..iterations {
        x = x
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(black_box(i));
    }
    x
}

fn fill() -> Vec<u8> {
    // Non-zero, so the allocation is written rather than served as zero pages.
    black_box(vec![0xa5u8; BUFFER_BYTES])
}

/// A String of exactly `capacity` bytes allocated once.
fn text(capacity: usize, args: std::fmt::Arguments<'_>) -> String {
    let mut s = String::with_capacity(capacity);
    let _ = s.write_fmt(args);
    while s.len() < capacity {
        s.push('x');
    }
    s
}

/// `records` log records of `attrs` string attributes, and the number of
/// Strings and Vecs built for them.
fn build_records(records: usize, attrs: usize) -> (Vec<LogRecord>, u64) {
    let mut built = 1;
    let mut out = Vec::with_capacity(records);
    for r in 0..records {
        let mut stream_attrs = Vec::with_capacity(STREAM_ATTRS_BYTES);
        stream_attrs.extend((0..STREAM_ATTRS_BYTES).map(|i| (r + i) as u8));
        let severity_text = String::from("INFO");
        let body = text(BODY_BYTES, format_args!("record {r:010} "));
        let mut record_attrs = Vec::with_capacity(attrs);
        built += 4;
        for a in 0..attrs {
            let key = text(KEY_BYTES, format_args!("attr_key_{a:03}"));
            let value = text(VALUE_BYTES, format_args!("v{r:06}_{a:08}"));
            record_attrs.push((key, AttrValue::Str(value)));
            built += 2;
        }
        out.push(LogRecord {
            stream_id: LogStreamId([(r % 7) as u8; 16]),
            stream_attrs,
            ts_ns: r as i64,
            observed_ts_ns: r as i64,
            severity_num: 9,
            severity_text,
            body,
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: record_attrs,
        });
    }
    (out, built)
}

/// A remote-write-like request of [`SERIES`] series, and the number of
/// Strings and Vecs built for it.
fn build_series() -> (Vec<SeriesPiece>, u64) {
    let mut built = 1;
    let mut out = Vec::with_capacity(SERIES);
    for s in 0..SERIES {
        let mut labels = Vec::with_capacity(LABELS);
        for l in 0..LABELS {
            labels.push((
                text(LABEL_NAME_BYTES, format_args!("label_nm_{l:03}")),
                text(LABEL_VALUE_BYTES, format_args!("value_{s:06}_{l:07}")),
            ));
        }
        let samples = vec![(s as i64, s as f64 * 0.5)];
        built += 2 + 2 * LABELS as u64;
        out.push(SeriesPiece { labels, samples });
    }
    (out, built)
}

fn build(payload: Payload, spin_iterations: u64) -> (Value, u64) {
    match payload {
        Payload::Empty => (Value::Word(black_box(7)), 0),
        Payload::Spin => (Value::Word(spin(spin_iterations)), 0),
        Payload::Buffer => (Value::Buffer(fill()), 1),
        Payload::Records { records, attrs } => {
            let (v, n) = build_records(records, attrs);
            (Value::Records(v), n)
        }
        Payload::Series => {
            let (v, n) = build_series();
            (Value::Series(v), n)
        }
    }
}

fn timed_drop<T>(value: T) -> Duration {
    let t = Instant::now();
    drop(black_box(value));
    t.elapsed()
}

/// Drops a value alone under the clock; `None` for a plain word.
fn drop_value(value: Value) -> Option<Duration> {
    match value {
        Value::Word(w) => {
            black_box(w);
            None
        }
        Value::Buffer(v) => Some(timed_drop(v)),
        Value::Records(v) => Some(timed_drop(v)),
        Value::Series(v) => Some(timed_drop(v)),
    }
}

/// What a job hands back: its value (or its length once dropped), the
/// allocations it built, its own drop time, and the instants of its first and
/// last instruction.
struct JobOut {
    value: Value,
    built: u64,
    job_drop: Option<Duration>,
    first: Instant,
    last: Instant,
}

fn job(variant: Variant, spin_iterations: u64) -> impl FnOnce() -> JobOut + Send + 'static {
    move || {
        let first = Instant::now();
        let (mut value, built) = build(variant.payload, spin_iterations);
        let mut job_drop = None;
        if variant.drop_site == DropSite::Job && variant.payload.droppable() {
            job_drop = drop_value(std::mem::replace(&mut value, Value::Word(built)));
        }
        JobOut {
            value,
            built,
            job_drop,
            first,
            last: Instant::now(),
        }
    }
}

struct CallerStats {
    jobs: u64,
    latency: Histogram,
    handoff_in: Histogram,
    handoff_out: Histogram,
    drop: Reservoir,
    page_read: Histogram,
    errors: Vec<String>,
}

impl CallerStats {
    fn new(seed: u64) -> Self {
        CallerStats {
            jobs: 0,
            latency: Histogram::new(),
            handoff_in: Histogram::new(),
            handoff_out: Histogram::new(),
            drop: Reservoir::new(seed),
            page_read: Histogram::new(),
            errors: Vec::new(),
        }
    }

    fn error(&mut self, e: String) {
        if self.errors.len() < 4 {
            self.errors.push(e);
        }
    }
}

/// The caller's side of a returned value: for a buffer, one byte read per
/// page (timed on its own), then the drop timed alone.
fn consume(value: Value, stats: &mut CallerStats) {
    if let Value::Buffer(buf) = &value {
        let t = Instant::now();
        let mut sum = 0u64;
        for page in buf.chunks(PAGE_BYTES) {
            sum = sum.wrapping_add(u64::from(page[0]));
        }
        black_box(sum);
        stats.page_read.record(t.elapsed());
    }
    if let Some(d) = drop_value(value) {
        stats.drop.record(d);
    }
}

async fn caller(
    index: usize,
    variant: Variant,
    gate: Arc<ReadGate>,
    spin_iterations: u64,
    start: Arc<tokio::sync::Barrier>,
    deadline: Arc<OnceLock<Instant>>,
    run_for: Duration,
) -> CallerStats {
    let mut stats = CallerStats::new(ORDER_SEED ^ index as u64);
    let size = JobSize::Bytes(BUFFER_BYTES as u64);
    // Every caller is running before the shared deadline is set.
    start.wait().await;
    let deadline = *deadline.get_or_init(|| Instant::now() + run_for);
    let expected_built = variant.payload.expected_allocations();
    while Instant::now() < deadline {
        let job = job(variant, spin_iterations);
        let before = Instant::now();
        let result = match variant.dispatch {
            Dispatch::Inline => Ok(job()),
            Dispatch::Gated => gate.run(SITE, size, job).await.map_err(|e| e.to_string()),
            Dispatch::Bare => tokio::task::spawn_blocking(job)
                .await
                .map_err(|e| e.to_string()),
        };
        let returned = Instant::now();
        match result {
            Ok(out) => {
                if out.built != expected_built {
                    stats.error(format!(
                        "{}: job built {} Strings and Vecs, expected {expected_built}",
                        variant.name, out.built
                    ));
                }
                if let Some(d) = out.job_drop {
                    stats.drop.record(d);
                }
                consume(out.value, &mut stats);
                stats.latency.record(before.elapsed());
                stats.jobs += 1;
                if variant.dispatch != Dispatch::Inline {
                    stats
                        .handoff_in
                        .record(out.first.saturating_duration_since(before));
                    stats
                        .handoff_out
                        .record(returned.saturating_duration_since(out.last));
                }
            }
            Err(e) => stats.error(format!("{} job failed: {e}", variant.name)),
        }
        if variant.dispatch == Dispatch::Inline {
            tokio::task::yield_now().await;
        }
    }
    stats
}

/// One variant over one run, all callers merged.
struct VariantRun {
    jobs: u64,
    caller_jobs: Vec<u64>,
    latency: Histogram,
    handoff_in: Histogram,
    handoff_out: Histogram,
    drop: Reservoir,
    page_read: Histogram,
    gate: GateDelta,
    errors: Vec<String>,
}

/// What the gate's counters moved by over one variant.
#[derive(Debug, Clone, Copy)]
struct GateDelta {
    jobs: u64,
    inline: u64,
    run_count: u64,
    abandoned: u64,
}

fn site_counts(snap: &CpuGateSnapshot<ReadSite>) -> (u64, u64) {
    snap.sites
        .iter()
        .find(|s| s.site == SITE)
        .map_or((0, 0), |s| (s.jobs, s.inline))
}

fn one_variant(
    rt: &tokio::runtime::Runtime,
    gate: &Arc<ReadGate>,
    variant: Variant,
    callers: usize,
    run_for: Duration,
    spin_iterations: u64,
) -> VariantRun {
    let before = gate.snapshot();
    let start = Arc::new(tokio::sync::Barrier::new(callers));
    let deadline = Arc::new(OnceLock::new());
    let stats: Vec<CallerStats> = rt.block_on(async {
        let handles: Vec<_> = (0..callers)
            .map(|index| {
                tokio::spawn(caller(
                    index,
                    variant,
                    Arc::clone(gate),
                    spin_iterations,
                    Arc::clone(&start),
                    Arc::clone(&deadline),
                    run_for,
                ))
            })
            .collect();
        let mut out = Vec::with_capacity(callers);
        for h in handles {
            out.push(h.await.expect("caller task"));
        }
        out
    });
    let after = gate.snapshot();
    let mut run = VariantRun {
        jobs: 0,
        caller_jobs: Vec::with_capacity(callers),
        latency: Histogram::new(),
        handoff_in: Histogram::new(),
        handoff_out: Histogram::new(),
        drop: Reservoir::new(0),
        page_read: Histogram::new(),
        gate: GateDelta {
            jobs: 0,
            inline: 0,
            run_count: after.run_count - before.run_count,
            abandoned: after.abandoned - before.abandoned,
        },
        errors: Vec::new(),
    };
    for s in stats {
        run.jobs += s.jobs;
        run.caller_jobs.push(s.jobs);
        run.latency.merge(&s.latency);
        run.handoff_in.merge(&s.handoff_in);
        run.handoff_out.merge(&s.handoff_out);
        run.drop.merge(&s.drop);
        run.page_read.merge(&s.page_read);
        run.errors.extend(s.errors);
    }
    let (jobs_before, inline_before) = site_counts(&before);
    let (jobs_after, inline_after) = site_counts(&after);
    run.gate.jobs = jobs_after - jobs_before;
    run.gate.inline = inline_after - inline_before;
    run
}

/// The gate counters a variant must leave: a gated variant dispatched exactly
/// the jobs the bench counted and ran none inline; inline and bare variants
/// never touched the gate.
fn check_counts(variant: Variant, counted: u64, gate: GateDelta) -> Result<(), String> {
    let expected_gated = if variant.dispatch == Dispatch::Gated {
        counted
    } else {
        0
    };
    if gate.jobs != expected_gated
        || gate.run_count != expected_gated
        || gate.inline != 0
        || gate.abandoned != 0
    {
        return Err(format!(
            "{}: bench counted {counted} jobs, expected gate jobs {expected_gated} inline 0 \
             abandoned 0, gate shows jobs {} run_count {} inline {} abandoned {}",
            variant.name, gate.jobs, gate.run_count, gate.inline, gate.abandoned
        ));
    }
    Ok(())
}

/// The drop samples a variant must leave: one timed drop per job when the
/// job builds a value, none otherwise, and one page read per job for a
/// buffer the caller frees.
fn check_drops(variant: Variant, jobs: u64, drops: u64, page_reads: u64) -> Result<(), String> {
    let expected_drops = if variant.payload.droppable() { jobs } else { 0 };
    let expected_page_reads =
        if variant.payload == Payload::Buffer && variant.drop_site == DropSite::Caller {
            jobs
        } else {
            0
        };
    if drops != expected_drops || page_reads != expected_page_reads {
        return Err(format!(
            "{}: {jobs} jobs, expected {expected_drops} drop samples and \
             {expected_page_reads} page reads, recorded {drops} and {page_reads}",
            variant.name
        ));
    }
    Ok(())
}

/// Every caller of a variant completed at least one job.
fn check_callers(variant: Variant, caller_jobs: &[u64]) -> Result<(), String> {
    let idle: Vec<String> = caller_jobs
        .iter()
        .enumerate()
        .filter(|(_, n)| **n == 0)
        .map(|(i, _)| i.to_string())
        .collect();
    if idle.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "{}: caller(s) {} of {} ran no jobs",
            variant.name,
            idle.join(","),
            caller_jobs.len()
        ))
    }
}

fn us(d: Duration) -> f64 {
    d.as_secs_f64() * 1e6
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(|a, b| a.total_cmp(b));
    let n = values.len();
    if n == 0 {
        0.0
    } else if n % 2 == 1 {
        values[n / 2]
    } else {
        (values[n / 2 - 1] + values[n / 2]) / 2.0
    }
}

/// `median [min-max]`, and the median for verdicts.
fn spread(mut values: Vec<f64>) -> (String, f64) {
    let mid = median(&mut values);
    let text = format!(
        "{mid:.1} [{:.1}-{:.1}]",
        values.first().copied().unwrap_or(0.0),
        values.last().copied().unwrap_or(0.0)
    );
    (text, mid)
}

fn loadavg() -> String {
    std::fs::read_to_string("/proc/loadavg")
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|_| "unavailable".to_string())
}

fn cores() -> usize {
    std::thread::available_parallelism().map_or(1, |n| n.get())
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|n| *n > 0)
}

/// splitmix64, so the variant order is reproducible from the printed seed.
struct OrderRng(u64);

impl OrderRng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn shuffle(&mut self, order: &mut [usize]) {
        for i in (1..order.len()).rev() {
            let j = (self.next() % (i as u64 + 1)) as usize;
            order.swap(i, j);
        }
    }
}

/// Iterations of [`spin`] taking about [`SPIN_TARGET`] on this host, and the
/// median measured time of that many.
fn calibrate() -> (u64, Duration) {
    let mut probe = 1_000u64;
    loop {
        let t = Instant::now();
        black_box(spin(black_box(probe)));
        if t.elapsed() >= Duration::from_millis(5) {
            break;
        }
        probe *= 2;
    }
    let mut per_iter_ns: Vec<f64> = (0..7)
        .map(|_| {
            let t = Instant::now();
            black_box(spin(black_box(probe)));
            t.elapsed().as_nanos() as f64 / probe as f64
        })
        .collect();
    let ns = median(&mut per_iter_ns);
    let iterations = ((SPIN_TARGET.as_nanos() as f64 / ns).round() as u64).max(1);
    let mut check: Vec<f64> = (0..101)
        .map(|_| {
            let t = Instant::now();
            black_box(spin(black_box(iterations)));
            t.elapsed().as_nanos() as f64
        })
        .collect();
    (
        iterations,
        Duration::from_nanos(median(&mut check).round() as u64),
    )
}

/// One configuration's report lines, each `<label> <figure> <value>`.
struct Report {
    label: String,
    text: String,
    figures: Vec<String>,
}

impl Report {
    fn new(label: String) -> Self {
        Report {
            label,
            text: String::new(),
            figures: Vec::new(),
        }
    }

    fn figure(&mut self, name: impl Into<String>, value: impl std::fmt::Display) {
        let name = name.into();
        let _ = writeln!(self.text, "{} {name} {value}", self.label);
        self.figures.push(name);
    }

    /// Every expected figure, and every figure recorded, appears on exactly
    /// one line of the rendered text.
    fn check_present_once(&self, expected: &[String]) -> Result<(), String> {
        for name in expected.iter().chain(&self.figures) {
            let key = format!("{} {name} ", self.label);
            let n = self.text.lines().filter(|l| l.starts_with(&key)).count();
            if n != 1 {
                return Err(format!("{}: figure {name} printed {n} times", self.label));
            }
        }
        if self.figures.len() != expected.len() {
            return Err(format!(
                "{}: {} figures recorded, {} expected",
                self.label,
                self.figures.len(),
                expected.len()
            ));
        }
        Ok(())
    }
}

/// The figures a group must print, listed independently of the code that
/// prints them.
fn expected_figures(group: Group) -> Vec<String> {
    let mut names = Vec::new();
    let base = |names: &mut Vec<String>, v: &str, dispatched: bool| {
        for figure in ["jobs", "p50_us", "p99_us"] {
            names.push(format!("{v}.{figure}"));
        }
        if dispatched {
            for handoff in ["handoff_in", "handoff_out"] {
                for q in ["p50_us", "p99_us"] {
                    names.push(format!("{v}.{handoff}.{q}"));
                }
            }
        }
    };
    match group {
        Group::Dispatch(_) => {
            for (v, dispatched) in [("inline", false), ("gated", true), ("bare", true)] {
                base(&mut names, v, dispatched);
            }
            names.push("gated_minus_inline.p50_us".to_string());
            names.push("bare_minus_inline.p50_us".to_string());
        }
        Group::Buffer => {
            for shape in ["buffer", "buffer_self_drop"] {
                for (d, dispatched) in [("inline", false), ("gated", true), ("bare", true)] {
                    let v = format!("{shape}.{d}");
                    base(&mut names, &v, dispatched);
                    names.push(format!("{v}.drop.p50_us"));
                    names.push(format!("{v}.drop.p99_us"));
                    if shape == "buffer" {
                        names.push(format!("{v}.page_read.p50_us"));
                        names.push(format!("{v}.page_read.p99_us"));
                    }
                }
            }
            for d in ["inline", "gated", "bare"] {
                names.push(format!("buffer_minus_self_drop.{d}.drop_p50_us"));
            }
            names.push("allocations_per_value".to_string());
        }
        Group::Structured(_) => {
            for (v, dispatched) in [("caller_drop", true), ("job_drop", true), ("inline", false)] {
                base(&mut names, v, dispatched);
                names.push(format!("{v}.drop.p50_us"));
                names.push(format!("{v}.drop.p99_us"));
            }
            names.push("caller_drop_minus_job_drop.drop_p50_us".to_string());
            names.push("allocations_per_value".to_string());
        }
    }
    names
}

/// The verdict lines a whole pass must print, by key.
fn expected_verdicts() -> Vec<String> {
    let mut keys = vec!["empty/c1".to_string(), "buffer/c1".to_string()];
    for records in RECORD_COUNTS {
        for attrs in ATTR_COUNTS {
            keys.push(format!("records/r{records}xa{attrs}/c1"));
        }
    }
    keys.push(format!("series/s{SERIES}xl{LABELS}/c1"));
    keys
}

struct Verdicts {
    quick: bool,
    text: String,
}

impl Verdicts {
    fn line(&mut self, key: &str, verdict: &str, detail: String) {
        let mode = if self.quick {
            "(quick mode, not a measurement) "
        } else {
            ""
        };
        let line = format!("{key} verdict {mode}{verdict} ({detail})");
        println!("{line}");
        self.text.push_str(&line);
        self.text.push('\n');
    }

    fn check_present_once(&self, expected: &[String]) -> Result<(), String> {
        for key in expected {
            let prefix = format!("{key} verdict ");
            let n = self.text.lines().filter(|l| l.starts_with(&prefix)).count();
            if n != 1 {
                return Err(format!("verdict {key} printed {n} times"));
            }
        }
        let total = self.text.lines().count();
        if total != expected.len() {
            return Err(format!(
                "{total} verdict lines printed, {} expected",
                expected.len()
            ));
        }
        Ok(())
    }
}

struct Settings {
    runs: usize,
    run_for: Duration,
    spin_iterations: u64,
    /// Lanes a queued caller waits on: min(gate permits, tokio workers).
    lanes: usize,
}

/// The run length for a configuration: the configured one, raised to
/// [`RUN_FLOOR_JOBS`] times the slowest variant's single inline job per
/// caller queued on a lane, so every caller completes jobs within the run.
fn run_length(group: Group, callers: usize, settings: &Settings) -> Duration {
    let slowest = group
        .variants()
        .into_iter()
        .map(|v| {
            let t = Instant::now();
            let out = job(v, settings.spin_iterations)();
            drop(black_box(out));
            t.elapsed()
        })
        .max()
        .unwrap_or(Duration::ZERO);
    let queued = callers.div_ceil(settings.lanes) as u32;
    settings.run_for.max(slowest * RUN_FLOOR_JOBS * queued)
}

/// All runs of one group at one caller count. Returns the median of the
/// group's verdict figure: gated minus inline p50 (dispatch), buffer minus
/// self-drop gated drop p50 (buffer), caller_drop minus job_drop drop p50
/// (structured).
fn configuration(
    rt: &tokio::runtime::Runtime,
    gate: &Arc<ReadGate>,
    group: Group,
    callers: usize,
    settings: &Settings,
    rng: &mut OrderRng,
    failures: &mut Vec<String>,
) -> f64 {
    let label = format!("{}/c{callers}", group.label());
    let variants = group.variants();
    let run_for = run_length(group, callers, settings);
    println!(
        "loadavg before {label}: {}; run length {} ms",
        loadavg(),
        run_for.as_millis()
    );
    let mut runs: Vec<Vec<Option<VariantRun>>> = Vec::with_capacity(settings.runs);
    for run in 0..settings.runs {
        let mut order: Vec<usize> = (0..variants.len()).collect();
        rng.shuffle(&mut order);
        let mut slot: Vec<Option<VariantRun>> = (0..variants.len()).map(|_| None).collect();
        for idx in order {
            let v = variants[idx];
            let r = one_variant(rt, gate, v, callers, run_for, settings.spin_iterations);
            let checks = [
                check_counts(v, r.jobs, r.gate),
                check_drops(v, r.jobs, r.drop.total, r.page_read.total),
                check_callers(v, &r.caller_jobs),
            ];
            for e in checks.into_iter().filter_map(Result::err) {
                failures.push(format!("{label} run {run}: {e}"));
            }
            for e in &r.errors {
                failures.push(format!("{label} run {run}: {e}"));
            }
            slot[idx] = Some(r);
        }
        runs.push(slot);
    }
    let of = |name: &str| -> Vec<&VariantRun> {
        let idx = variants.iter().position(|v| v.name == name).unwrap();
        runs.iter().filter_map(|s| s[idx].as_ref()).collect()
    };
    let per_run = |name: &str, f: &dyn Fn(&VariantRun) -> f64| -> (String, f64) {
        spread(of(name).into_iter().map(f).collect())
    };
    // Paired run by run, then the median of the per-run differences.
    let paired = |a: &str, b: &str, f: &dyn Fn(&VariantRun) -> f64| -> (String, f64) {
        spread(
            of(a)
                .into_iter()
                .zip(of(b))
                .map(|(x, y)| f(x) - f(y))
                .collect(),
        )
    };
    let p50 = |r: &VariantRun| us(r.latency.percentile(0.50));
    let drop_p50 = |r: &VariantRun| us(r.drop.percentile(0.50));

    let mut report = Report::new(label.clone());
    for v in &variants {
        let jobs: u64 = of(v.name).iter().map(|r| r.jobs).sum();
        report.figure(format!("{}.jobs", v.name), jobs);
        report.figure(format!("{}.p50_us", v.name), per_run(v.name, &p50).0);
        report.figure(
            format!("{}.p99_us", v.name),
            per_run(v.name, &|r| us(r.latency.percentile(0.99))).0,
        );
        if v.dispatch != Dispatch::Inline {
            for (which, pick) in [
                (
                    "handoff_in",
                    (|r: &VariantRun| &r.handoff_in) as fn(&VariantRun) -> &Histogram,
                ),
                ("handoff_out", |r: &VariantRun| &r.handoff_out),
            ] {
                for (q, qv) in [("p50_us", 0.50), ("p99_us", 0.99)] {
                    report.figure(
                        format!("{}.{which}.{q}", v.name),
                        per_run(v.name, &|r| us(pick(r).percentile(qv))).0,
                    );
                }
            }
        }
        if v.payload.droppable() {
            report.figure(
                format!("{}.drop.p50_us", v.name),
                per_run(v.name, &drop_p50).0,
            );
            report.figure(
                format!("{}.drop.p99_us", v.name),
                per_run(v.name, &|r| us(r.drop.percentile(0.99))).0,
            );
        }
        if v.payload == Payload::Buffer && v.drop_site == DropSite::Caller {
            for (q, qv) in [("p50_us", 0.50), ("p99_us", 0.99)] {
                report.figure(
                    format!("{}.page_read.{q}", v.name),
                    per_run(v.name, &|r| us(r.page_read.percentile(qv))).0,
                );
            }
        }
    }
    let verdict_figure = match group {
        Group::Dispatch(_) => {
            let (text, mid) = paired("gated", "inline", &p50);
            report.figure("gated_minus_inline.p50_us", text);
            report.figure("bare_minus_inline.p50_us", paired("bare", "inline", &p50).0);
            mid
        }
        Group::Buffer => {
            let mut mid = 0.0;
            for d in ["inline", "gated", "bare"] {
                let (text, m) = paired(
                    &format!("buffer.{d}"),
                    &format!("buffer_self_drop.{d}"),
                    &drop_p50,
                );
                report.figure(format!("buffer_minus_self_drop.{d}.drop_p50_us"), text);
                if d == "gated" {
                    mid = m;
                }
            }
            report.figure(
                "allocations_per_value",
                Payload::Buffer.expected_allocations(),
            );
            mid
        }
        Group::Structured(payload) => {
            let (text, mid) = paired("caller_drop", "job_drop", &drop_p50);
            report.figure("caller_drop_minus_job_drop.drop_p50_us", text);
            report.figure("allocations_per_value", payload.expected_allocations());
            mid
        }
    };
    if let Err(e) = report.check_present_once(&expected_figures(group)) {
        failures.push(e);
    }
    print!("{}", report.text);
    verdict_figure
}

/// The assertions must reject wrong input. Runs every variant of every group
/// once at 1 caller for `run_for` and checks it, then checks it again with
/// the bench's job count one too high (gated variants, and the drop and page
/// read counts) or one gated job added to the gate's counters (inline and
/// bare): each wrong case must fail. Also checks the histogram's and the
/// reservoir's p50 against an exact one.
fn self_check(
    rt: &tokio::runtime::Runtime,
    gate: &Arc<ReadGate>,
    run_for: Duration,
    spin_iterations: u64,
) -> Result<(), String> {
    let mut h = Histogram::new();
    for ns in 1..=100_000u64 {
        h.record(Duration::from_nanos(ns));
    }
    let p50 = h.percentile(0.50).as_nanos() as f64;
    if (p50 - 50_000.0).abs() > 50_000.0 * 0.016 {
        return Err(format!(
            "self-check histogram: p50 {p50} ns of 1..=100000 ns"
        ));
    }
    let mut r = Reservoir::new(ORDER_SEED);
    for ns in 1..=100_000u64 {
        r.record(Duration::from_nanos(ns));
    }
    let p50 = r.percentile(0.50).as_nanos() as f64;
    if r.total != 100_000 || r.kept.len() != RESERVOIR_CAP || (p50 - 50_000.0).abs() > 2_500.0 {
        return Err(format!(
            "self-check reservoir: total {} kept {} p50 {p50} ns of 1..=100000 ns",
            r.total,
            r.kept.len()
        ));
    }
    for group in Group::all() {
        for v in group.variants() {
            let r = one_variant(rt, gate, v, 1, run_for, spin_iterations);
            check_counts(v, r.jobs, r.gate).map_err(|e| format!("self-check: {e}"))?;
            check_drops(v, r.jobs, r.drop.total, r.page_read.total)
                .map_err(|e| format!("self-check: {e}"))?;
            check_callers(v, &r.caller_jobs).map_err(|e| format!("self-check: {e}"))?;
            if let Some(e) = r.errors.first() {
                return Err(format!("self-check: {e}"));
            }
            let wrong_count = if v.dispatch == Dispatch::Gated {
                check_counts(v, r.jobs + 1, r.gate)
            } else {
                let mut gate = r.gate;
                gate.jobs += 1;
                check_counts(v, r.jobs, gate)
            };
            if wrong_count.is_ok() {
                return Err(format!(
                    "self-check {}/{}: a wrong gate count passed the assertion",
                    group.label(),
                    v.name
                ));
            }
            let wrong_drops = if v.payload.droppable() {
                check_drops(v, r.jobs + 1, r.drop.total, r.page_read.total)
            } else {
                check_drops(v, r.jobs, r.drop.total + 1, r.page_read.total)
            };
            if wrong_drops.is_ok() {
                return Err(format!(
                    "self-check {}/{}: a wrong drop count passed the assertion",
                    group.label(),
                    v.name
                ));
            }
            if check_callers(v, &[r.jobs, 0]).is_ok() {
                return Err(format!(
                    "self-check {}/{}: an idle caller passed the assertion",
                    group.label(),
                    v.name
                ));
            }
        }
    }
    Ok(())
}

fn structured_verdict(diff: f64) -> &'static str {
    if diff >= STRUCTURED_CANDIDATE_US {
        "structured free: shared-mechanism candidate"
    } else if diff <= STRUCTURED_RULED_OUT_US {
        "structured free: ruled out"
    } else {
        "structured free: partial"
    }
}

fn main() {
    let malloc_conf = std::env::var(ravel_server::mem_stats::MALLOC_CONF_ENV).ok();
    let alloc = ravel_server::mem_stats::configure_background_thread(malloc_conf.as_deref());
    let full = std::env::args().any(|a| a == "--bench");
    let run_ms =
        env_u64("RAVEL_BENCH_RUN_MS").unwrap_or(if full { FULL_RUN_MS } else { QUICK_RUN_MS });
    let runs = if full { FULL_RUNS } else { 1 };
    let permits = env_u64("RAVEL_BENCH_GATE_PERMITS")
        .map_or_else(|| default_read_permits(cores()), |n| n as usize);
    let gate: Arc<ReadGate> = Arc::new(ReadGate::new(
        CpuGateConfig {
            permits,
            inline_floor_bytes: 0,
            eval_floor_samples: DEFAULT_EVAL_FLOOR_SAMPLES,
        },
        Arc::new(InstantClock::new()),
    ));
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cores())
        .enable_all()
        .build()
        .expect("runtime");

    println!();
    println!("== cpu_gate_round_trip (issues #2795, #2818) ==");
    println!(
        "host: {} cores (available_parallelism), {} tokio workers; read gate permits {}, \
         inline floor 0 B, site {}; mode {}, {runs} run(s) of at least {run_ms} ms per \
         configuration, variant order shuffled per run with seed {ORDER_SEED:#x}",
        cores(),
        cores(),
        gate.permits(),
        SITE.name(),
        if full { "full" } else { "quick" },
    );
    println!(
        "allocator background thread enabled={} (source {})",
        alloc.enabled, alloc.source
    );
    let (spin_iterations, spin_p50) = calibrate();
    println!(
        "calibration: spin {spin_iterations} iterations, measured p50 {:.1} us over 101 runs \
         (target {} us)",
        us(spin_p50),
        SPIN_TARGET.as_micros()
    );

    let mut failures = Vec::new();
    match self_check(&rt, &gate, Duration::from_millis(5), spin_iterations) {
        Ok(()) => println!(
            "self-check: gate count, drop count and idle-caller assertions reject wrong input: ok"
        ),
        Err(e) => failures.push(e),
    }
    println!(
        "figures: us, median [min-max] over runs of each run's percentile; jobs summed over \
         runs; X_minus_Y paired run by run, median of the per-run differences; drop times the \
         free alone, percentiles from exact samples (at most {RESERVOIR_CAP} per caller); \
         allocations_per_value counts the Strings and Vecs one value holds"
    );
    println!();

    let settings = Settings {
        runs,
        run_for: Duration::from_millis(run_ms),
        spin_iterations,
        lanes: gate.permits().min(cores()).max(1),
    };
    let mut rng = OrderRng(ORDER_SEED);
    let mut verdicts = Verdicts {
        quick: !full,
        text: String::new(),
    };
    for callers in CALLERS {
        for group in Group::all() {
            let figure = configuration(
                &rt,
                &gate,
                group,
                callers,
                &settings,
                &mut rng,
                &mut failures,
            );
            let key = format!("{}/c{callers}", group.label());
            if callers == 1 {
                match group {
                    Group::Dispatch(Payload::Empty) => {
                        let verdict = if figure <= DISPATCH_NOT_CAUSE_US {
                            "dispatch: not the cause"
                        } else if figure >= DISPATCH_SHARED_CAUSE_US {
                            "dispatch: shared cause"
                        } else {
                            "dispatch: partial"
                        };
                        verdicts.line(
                            &key,
                            verdict,
                            format!(
                                "gated minus inline p50 {figure:.1} us; band <= \
                                 {DISPATCH_NOT_CAUSE_US} not, >= {DISPATCH_SHARED_CAUSE_US} shared"
                            ),
                        );
                    }
                    Group::Dispatch(_) => {}
                    Group::Buffer => {
                        let verdict = if figure > CROSS_THREAD_FREE_US {
                            "cross-thread free: matters"
                        } else {
                            "cross-thread free: does not matter"
                        };
                        verdicts.line(
                            &key,
                            verdict,
                            format!(
                                "gated drop p50 buffer minus buffer_self_drop {figure:.1} us; \
                                 band > {CROSS_THREAD_FREE_US}"
                            ),
                        );
                    }
                    Group::Structured(_) => verdicts.line(
                        &key,
                        structured_verdict(figure),
                        format!(
                            "caller_drop minus job_drop drop p50 {figure:.1} us; band >= \
                             {STRUCTURED_CANDIDATE_US} candidate, <= {STRUCTURED_RULED_OUT_US} \
                             ruled out"
                        ),
                    ),
                }
            }
            println!();
        }
    }
    if let Err(e) = verdicts.check_present_once(&expected_verdicts()) {
        failures.push(e);
    }
    println!("loadavg after: {}", loadavg());
    drop(rt);

    if !failures.is_empty() {
        eprintln!(
            "cpu_gate_round_trip: {} assertion(s) failed:",
            failures.len()
        );
        for f in &failures {
            eprintln!("  {f}");
        }
        std::process::exit(1);
    }
    println!("cpu_gate_round_trip: all assertions passed");
}
