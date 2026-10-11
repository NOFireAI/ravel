//! Round-trip cost of one ADR-1702 CPU gate job, and where the result is freed
//! (issues #2795, #2818).
//!
//! `CpuGate::run` is a semaphore permit plus `tokio::task::spawn_blocking`
//! per job. `ravel_cpu_gate_wait_seconds` covers only the permit wait; this
//! bench measures the whole round trip, and the two handoffs inside it: from
//! just before `gate.run` to the job's first instruction on a blocking thread
//! (handoff in), and from the job's last instruction to the await returning
//! (handoff out).
//!
//! Dispatch shapes, `empty` (returns a u64) and `spin` (about 50 us of
//! calibrated busy work), run in three arms:
//!
//! - `inline`: the job called directly on the calling task, which then
//!   yields so every caller task gets a worker;
//! - `gated`: `gate.run(..)` on a read gate with `inline_floor_bytes = 0`, so
//!   every job takes a permit and goes to the blocking pool;
//! - `bare`: `tokio::task::spawn_blocking(job).await` with no semaphore, which
//!   splits the gate's own bookkeeping from the blocking pool handoff.
//!
//! Drop shapes build a value and free it, in three variants: `caller_drop`
//! (built in a gated job, dropped by the caller after `gate.run` returns),
//! `job_drop` (built and dropped inside the gated job) and `inline` (built and
//! dropped on the calling task, which then yields). Only the drop itself is
//! inside the drop timer. The shapes:
//!
//! - `buffer`: one 768 KiB `Vec<u8>`, one allocation. Before the drop, one byte
//!   per 4 KiB page is read on the dropping thread, timed separately as
//!   `page_read`;
//! - `records/rR_aA`: R `ravel_logseg::LogRecord`s with A string attributes
//!   each, every string built fresh per record as a decoder builds it;
//! - `series/s2828_l10`: a remote-write-like request, 2828 series of 10
//!   `(String, String)` labels and one sample each.
//!
//! In every configuration the arms (or variants) run interleaved: each run
//! executes all three in an order shuffled by a fixed, printed seed, so
//! differences are paired run by run. Samples go to fixed-size log-bucketed
//! histograms (32 buckets per power of two, about 3% resolution).
//!
//! Under `cargo bench` the run is 5 paired runs of 1500 ms per configuration.
//! Run any other way (`cargo test --bench cpu_gate_round_trip`, or the binary
//! directly) it is a quick mode: one paired run of 50 ms, and every verdict
//! line says it is not a measurement. `RAVEL_BENCH_RUN_MS` overrides the run
//! length in both modes and `RAVEL_BENCH_GATE_PERMITS` the gate's permits
//! (default: the read gate default for this host). Gate counters, sample
//! counts, per-caller job counts, allocation counts and the presence of every
//! figure and verdict line are asserted; any failure exits non-zero after the
//! table.
//!
//! The binary runs under jemalloc with the server's background thread
//! configuration, since where a gated job allocates and frees is part of its
//! cost.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::fmt::Write as _;
use std::hint::black_box;
use std::sync::Arc;
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
const ORDER_SEED: u64 = 0x2795_2818_c0de_0001;
/// Pre-registered dispatch band for `empty` at 1 caller, gated minus inline p50.
const DISPATCH_NOT_CAUSE_US: f64 = 30.0;
const DISPATCH_SHARED_CAUSE_US: f64 = 150.0;
/// `buffer` caller_drop minus job_drop drop p50 above this: the free matters.
const CROSS_THREAD_FREE_US: f64 = 50.0;
/// Pre-registered band for the structured shapes at 1 caller, caller_drop
/// minus job_drop drop p50.
const STRUCTURED_RULED_OUT_US: f64 = 30.0;
const STRUCTURED_CANDIDATE_US: f64 = 150.0;
/// Record and attribute counts; 105 is the ClickBench hits column count.
const RECORD_COUNTS: [usize; 2] = [1024, 8192];
const ATTR_COUNTS: [usize; 2] = [16, 105];
/// Series in a 64 KiB snappy remote-write request, and labels per series.
const SERIES: usize = 2828;
const LABELS: usize = 10;

/// Fixed-size latency histogram: exact below 32 ns, then 32 log-linear
/// buckets per power of two.
#[derive(Clone)]
struct Histogram {
    counts: Vec<u64>,
    total: u64,
}

const SUB_BITS: u32 = 5;
const SUB: u64 = 1 << SUB_BITS;
const BUCKETS: usize = (SUB as usize) * (64 - SUB_BITS as usize);

impl Histogram {
    fn new() -> Self {
        Histogram {
            counts: vec![0; BUCKETS],
            total: 0,
        }
    }

    fn bucket(ns: u64) -> usize {
        if ns < SUB {
            return ns as usize;
        }
        let e = 63 - ns.leading_zeros();
        let sub = (ns >> (e - SUB_BITS)) - SUB;
        (SUB + u64::from(e - SUB_BITS) * SUB + sub) as usize
    }

    /// The middle of bucket `idx`, in nanoseconds.
    fn value(idx: usize) -> u64 {
        let idx = idx as u64;
        if idx < SUB {
            return idx;
        }
        let k = idx - SUB;
        let shift = k / SUB;
        let lower = (SUB + k % SUB) << shift;
        lower + ((1u64 << shift) >> 1)
    }

    fn record(&mut self, d: Duration) {
        let ns = u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
        self.counts[Self::bucket(ns)] += 1;
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
        let mut seen = 0u64;
        for (idx, n) in self.counts.iter().enumerate() {
            seen += n;
            if seen >= rank {
                return Duration::from_nanos(Self::value(idx));
            }
        }
        Duration::ZERO
    }
}

/// splitmix64, for a reproducible arm order.
struct OrderRng(u64);

impl OrderRng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn shuffle<T>(&mut self, xs: &mut [T]) {
        for i in (1..xs.len()).rev() {
            let j = (self.next() % (i as u64 + 1)) as usize;
            xs.swap(i, j);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Shape {
    Empty,
    Spin,
}

impl Shape {
    const ALL: [Shape; 2] = [Shape::Empty, Shape::Spin];

    fn name(self) -> &'static str {
        match self {
            Shape::Empty => "empty",
            Shape::Spin => "spin",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Arm {
    Inline,
    Gated,
    Bare,
}

impl Arm {
    const ALL: [Arm; 3] = [Arm::Inline, Arm::Gated, Arm::Bare];

    fn name(self) -> &'static str {
        match self {
            Arm::Inline => "inline",
            Arm::Gated => "gated",
            Arm::Bare => "bare",
        }
    }
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

fn run_shape(shape: Shape, spin_iterations: u64) -> u64 {
    match shape {
        Shape::Empty => black_box(7),
        Shape::Spin => spin(spin_iterations),
    }
}

/// A job's output plus the instants of its first and last instruction.
type Stamped = (u64, Instant, Instant);

fn stamped(shape: Shape, spin_iterations: u64) -> impl FnOnce() -> Stamped + Send + 'static {
    move || {
        let first = Instant::now();
        let out = run_shape(shape, spin_iterations);
        (out, first, Instant::now())
    }
}

struct CallerStats {
    latency: Histogram,
    /// Handoff in and out; `None` for the inline arm, which has no handoff.
    handoff: Option<(Histogram, Histogram)>,
    errors: Vec<String>,
}

async fn caller(
    shape: Shape,
    arm: Arm,
    gate: Arc<ReadGate>,
    spin_iterations: u64,
    deadline: Instant,
) -> CallerStats {
    let mut stats = CallerStats {
        latency: Histogram::new(),
        handoff: (arm != Arm::Inline).then(|| (Histogram::new(), Histogram::new())),
        errors: Vec::new(),
    };
    let size = JobSize::Bytes(BUFFER_BYTES as u64);
    while Instant::now() < deadline {
        let job = stamped(shape, spin_iterations);
        let before = Instant::now();
        let result = match arm {
            Arm::Inline => Ok(job()),
            Arm::Gated => gate.run(SITE, size, job).await.map_err(|e| e.to_string()),
            Arm::Bare => tokio::task::spawn_blocking(job)
                .await
                .map_err(|e| e.to_string()),
        };
        let returned = Instant::now();
        match result {
            Ok((output, first, last)) => {
                black_box(output);
                stats.latency.record(before.elapsed());
                if let Some((handoff_in, handoff_out)) = stats.handoff.as_mut() {
                    handoff_in.record(first.saturating_duration_since(before));
                    handoff_out.record(returned.saturating_duration_since(last));
                }
            }
            Err(e) => stats.errors.push(format!("{} job failed: {e}", arm.name())),
        }
        if arm == Arm::Inline {
            tokio::task::yield_now().await;
        }
    }
    stats
}

struct ArmRun {
    jobs: u64,
    per_caller_jobs: Vec<u64>,
    latency: Histogram,
    handoff: Option<(Histogram, Histogram)>,
    gate: GateDelta,
    errors: Vec<String>,
}

/// What the gate's counters moved by over one arm.
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

fn gate_delta(before: &CpuGateSnapshot<ReadSite>, after: &CpuGateSnapshot<ReadSite>) -> GateDelta {
    let (jobs_before, inline_before) = site_counts(before);
    let (jobs_after, inline_after) = site_counts(after);
    GateDelta {
        jobs: jobs_after - jobs_before,
        inline: inline_after - inline_before,
        run_count: after.run_count - before.run_count,
        abandoned: after.abandoned - before.abandoned,
    }
}

fn percentile(sorted: &[Duration], q: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let rank = ((q * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len());
    sorted[rank - 1]
}

fn one_arm(
    rt: &tokio::runtime::Runtime,
    gate: &Arc<ReadGate>,
    shape: Shape,
    arm: Arm,
    callers: usize,
    run_for: Duration,
    spin_iterations: u64,
) -> ArmRun {
    let before = gate.snapshot();
    let deadline = Instant::now() + run_for;
    let stats: Vec<CallerStats> = rt.block_on(async {
        let handles: Vec<_> = (0..callers)
            .map(|_| {
                tokio::spawn(caller(
                    shape,
                    arm,
                    Arc::clone(gate),
                    spin_iterations,
                    deadline,
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
    let mut latency = Histogram::new();
    let mut handoff = (arm != Arm::Inline).then(|| (Histogram::new(), Histogram::new()));
    let mut per_caller_jobs = Vec::with_capacity(callers);
    let mut errors = Vec::new();
    for s in stats {
        per_caller_jobs.push(s.latency.total);
        latency.merge(&s.latency);
        if let (Some((all_in, all_out)), Some((caller_in, caller_out))) =
            (handoff.as_mut(), s.handoff.as_ref())
        {
            all_in.merge(caller_in);
            all_out.merge(caller_out);
        }
        errors.extend(s.errors);
    }
    ArmRun {
        jobs: latency.total,
        per_caller_jobs,
        latency,
        handoff,
        gate: gate_delta(&before, &after),
        errors,
    }
}

/// The gate counters an arm must leave: a gated arm dispatched exactly the
/// jobs the bench counted and ran none inline; the inline and bare arms
/// never touched the gate.
fn check_counts(arm: Arm, counted: u64, gate: GateDelta) -> Result<(), String> {
    let expected_gated = if arm == Arm::Gated { counted } else { 0 };
    if gate.jobs != expected_gated
        || gate.run_count != expected_gated
        || gate.inline != 0
        || gate.abandoned != 0
    {
        return Err(format!(
            "{} arm: bench counted {counted} jobs, expected gate jobs {expected_gated} inline 0 \
             abandoned 0, gate shows jobs {} run_count {} inline {} abandoned {}",
            arm.name(),
            gate.jobs,
            gate.run_count,
            gate.inline,
            gate.abandoned
        ));
    }
    Ok(())
}

/// Every caller task must have completed at least one job.
fn check_callers(what: &str, per_caller_jobs: &[u64]) -> Result<(), String> {
    let idle: Vec<usize> = per_caller_jobs
        .iter()
        .enumerate()
        .filter(|(_, n)| **n == 0)
        .map(|(i, _)| i)
        .collect();
    if idle.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "{what}: caller(s) {idle:?} of {} recorded 0 jobs",
            per_caller_jobs.len()
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
    let mut check: Vec<Duration> = (0..101)
        .map(|_| {
            let t = Instant::now();
            black_box(spin(black_box(iterations)));
            t.elapsed()
        })
        .collect();
    check.sort_unstable();
    (iterations, percentile(&check, 0.50))
}

/// One configuration's report lines, each `<label> <figure> <value>`.
struct Report {
    label: String,
    expected: Vec<String>,
    text: String,
    figures: Vec<String>,
}

impl Report {
    fn new(label: String, expected: Vec<String>) -> Self {
        Report {
            label,
            expected,
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
    fn check_present_once(&self) -> Result<(), String> {
        for name in self.expected.iter().chain(&self.figures) {
            let key = format!("{} {name} ", self.label);
            let n = self.text.lines().filter(|l| l.starts_with(&key)).count();
            if n != 1 {
                return Err(format!("{}: figure {name} printed {n} times", self.label));
            }
        }
        if self.figures.len() != self.expected.len() {
            return Err(format!(
                "{}: {} figures recorded, {} expected",
                self.label,
                self.figures.len(),
                self.expected.len()
            ));
        }
        Ok(())
    }
}

/// The figures every dispatch configuration must print, listed independently
/// of the code that prints them.
fn expected_figures() -> Vec<String> {
    let mut names = Vec::new();
    for arm in ["inline", "gated", "bare"] {
        for figure in ["jobs", "p50_us", "p99_us"] {
            names.push(format!("{arm}.{figure}"));
        }
    }
    names.push("gated_minus_inline.p50_us".to_string());
    names.push("bare_minus_inline.p50_us".to_string());
    for arm in ["gated", "bare"] {
        for handoff in ["handoff_in", "handoff_out"] {
            for q in ["p50_us", "p99_us"] {
                names.push(format!("{arm}.{handoff}.{q}"));
            }
        }
    }
    names
}

/// The figures every drop configuration must print.
fn expected_drop_figures(reads_pages: bool) -> Vec<String> {
    let mut names = vec!["allocs_per_value".to_string()];
    for variant in ["caller_drop", "job_drop", "inline"] {
        let mut figures = vec!["jobs", "drop.p50_us", "drop.p99_us", "round_trip.p50_us"];
        if reads_pages {
            figures.push("page_read.p50_us");
        }
        for figure in figures {
            names.push(format!("{variant}.{figure}"));
        }
    }
    names.push("caller_drop_minus_job_drop.drop_p50_us".to_string());
    names
}

struct Settings {
    runs: usize,
    run_for: Duration,
    spin_iterations: u64,
}

fn order_text(orders: &[Vec<&'static str>]) -> String {
    orders
        .iter()
        .enumerate()
        .map(|(i, o)| format!("run{i} {}", o.join(",")))
        .collect::<Vec<_>>()
        .join("; ")
}

/// Paired runs of all three arms for one dispatch shape and caller count.
/// Returns the median gated-minus-inline p50.
fn configuration(
    rt: &tokio::runtime::Runtime,
    gate: &Arc<ReadGate>,
    shape: Shape,
    callers: usize,
    settings: &Settings,
    rng: &mut OrderRng,
    failures: &mut Vec<String>,
) -> f64 {
    let label = format!("{}/c{callers}", shape.name());
    println!("loadavg before {label}: {}", loadavg());
    let mut runs: Vec<[Option<ArmRun>; 3]> = Vec::with_capacity(settings.runs);
    let mut orders = Vec::with_capacity(settings.runs);
    for run in 0..settings.runs {
        let mut slot: [Option<ArmRun>; 3] = [None, None, None];
        let mut order = Arm::ALL;
        rng.shuffle(&mut order);
        orders.push(order.iter().map(|a| a.name()).collect());
        for arm in order {
            let idx = Arm::ALL.iter().position(|a| *a == arm).unwrap();
            let r = one_arm(
                rt,
                gate,
                shape,
                arm,
                callers,
                settings.run_for,
                settings.spin_iterations,
            );
            if let Err(e) = check_counts(arm, r.jobs, r.gate) {
                failures.push(format!("{label} run {run}: {e}"));
            }
            for e in &r.errors {
                failures.push(format!("{label} run {run}: {e}"));
            }
            if let Err(e) = check_callers(arm.name(), &r.per_caller_jobs) {
                failures.push(format!("{label} run {run}: {e}"));
            }
            slot[idx] = Some(r);
        }
        runs.push(slot);
    }
    println!("order {label}: {}", order_text(&orders));
    let arm_runs = |arm: Arm| -> Vec<&ArmRun> {
        let idx = Arm::ALL.iter().position(|a| *a == arm).unwrap();
        runs.iter().filter_map(|s| s[idx].as_ref()).collect()
    };

    let mut report = Report::new(label.clone(), expected_figures());
    for arm in Arm::ALL {
        let rs = arm_runs(arm);
        let jobs: u64 = rs.iter().map(|r| r.jobs).sum();
        report.figure(format!("{}.jobs", arm.name()), jobs);
        for (q, name) in [(0.50, "p50_us"), (0.99, "p99_us")] {
            report.figure(
                format!("{}.{name}", arm.name()),
                spread(rs.iter().map(|r| us(r.latency.percentile(q))).collect()).0,
            );
        }
    }
    let paired = |arm: Arm| -> (String, f64) {
        spread(
            arm_runs(arm)
                .iter()
                .zip(arm_runs(Arm::Inline))
                .map(|(a, i)| us(a.latency.percentile(0.50)) - us(i.latency.percentile(0.50)))
                .collect(),
        )
    };
    let (gated_minus_inline, gated_minus_inline_mid) = paired(Arm::Gated);
    report.figure("gated_minus_inline.p50_us", gated_minus_inline);
    report.figure("bare_minus_inline.p50_us", paired(Arm::Bare).0);
    for arm in [Arm::Gated, Arm::Bare] {
        let rs = arm_runs(arm);
        for (which, outbound) in [("handoff_in", false), ("handoff_out", true)] {
            for (q, name) in [(0.50, "p50_us"), (0.99, "p99_us")] {
                let per_run = rs
                    .iter()
                    .map(|r| {
                        r.handoff
                            .as_ref()
                            .map_or(0.0, |(i, o)| us(if outbound { o } else { i }.percentile(q)))
                    })
                    .collect();
                report.figure(format!("{}.{which}.{name}", arm.name()), spread(per_run).0);
            }
        }
    }
    if let Err(e) = report.check_present_once() {
        failures.push(e);
    }
    print!("{}", report.text);
    gated_minus_inline_mid
}

/// A remote-write series as a decoder hands it on: owned labels and samples.
struct WireSeries {
    labels: Vec<(String, String)>,
    samples: Vec<(i64, f64)>,
}

/// A value whose free is measured.
enum Built {
    Buffer(Vec<u8>),
    Records(Vec<LogRecord>),
    Series(Vec<WireSeries>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DropShape {
    Buffer,
    Records { records: usize, attrs: usize },
    Series,
}

impl DropShape {
    fn all() -> Vec<DropShape> {
        let mut shapes = vec![DropShape::Buffer];
        for records in RECORD_COUNTS {
            for attrs in ATTR_COUNTS {
                shapes.push(DropShape::Records { records, attrs });
            }
        }
        shapes.push(DropShape::Series);
        shapes
    }

    fn name(self) -> String {
        match self {
            DropShape::Buffer => "buffer".to_string(),
            DropShape::Records { records, attrs } => format!("records/r{records}_a{attrs}"),
            DropShape::Series => format!("series/s{SERIES}_l{LABELS}"),
        }
    }

    fn reads_pages(self) -> bool {
        self == DropShape::Buffer
    }

    /// Allocations one value owns, from the shape's definition: the outer
    /// Vec, then per record `stream_attrs`, `severity_text`, `body` and the
    /// `attrs` Vec plus a key and a value String per attribute; per series the
    /// labels and samples Vecs plus a name and a value String per label.
    fn expected_allocs(self) -> u64 {
        match self {
            DropShape::Buffer => 1,
            DropShape::Records { records, attrs } => 1 + records as u64 * (4 + 2 * attrs as u64),
            DropShape::Series => 1 + SERIES as u64 * (2 + 2 * LABELS as u64),
        }
    }

    fn formula(self) -> String {
        match self {
            DropShape::Buffer => "1 Vec<u8>".to_string(),
            DropShape::Records { records, attrs } => {
                format!("1 + {records} x (4 + 2 x {attrs})")
            }
            DropShape::Series => format!("1 + {SERIES} x (2 + 2 x {LABELS})"),
        }
    }

    fn build(self) -> Built {
        match self {
            DropShape::Buffer => Built::Buffer(black_box(vec![0xa5u8; BUFFER_BYTES])),
            DropShape::Records { records, attrs } => {
                Built::Records(black_box((0..records).map(|r| record(r, attrs)).collect()))
            }
            DropShape::Series => Built::Series(black_box((0..SERIES).map(series).collect())),
        }
    }
}

/// One record, every buffer freshly allocated: keys of 12 bytes, values of
/// 16, a 64-byte body and 48 bytes of stream attributes.
fn record(r: usize, attrs: usize) -> LogRecord {
    let mut body = String::with_capacity(64);
    let _ = write!(body, "record {r:08} body ");
    while body.len() < 64 {
        body.push('x');
    }
    LogRecord {
        stream_id: LogStreamId([r as u8; 16]),
        stream_attrs: (0..48u8).map(|i| i ^ (r as u8)).collect(),
        ts_ns: r as i64,
        observed_ts_ns: r as i64,
        severity_num: 9,
        severity_text: String::from("INFO"),
        body,
        trace_id: Some([1; 16]),
        span_id: Some([2; 8]),
        flags: 0,
        attrs: (0..attrs)
            .map(|a| {
                (
                    format!("attr.key.{a:03}"),
                    AttrValue::Str(format!("value-{:05}-{a:04}", r % 100_000)),
                )
            })
            .collect(),
    }
}

/// One series: names of 12 bytes, values of 20, one sample.
fn series(s: usize) -> WireSeries {
    WireSeries {
        labels: (0..LABELS)
            .map(|l| {
                (
                    format!("label_{l:06}"),
                    format!("value-{:07}-{l:06}", s % 10_000_000),
                )
            })
            .collect(),
        samples: vec![(s as i64, s as f64)],
    }
}

/// Heap allocations `value` owns, counted from the value itself: every
/// String and Vec with a non-zero capacity.
fn count_allocs(value: &Built) -> u64 {
    let owned = |cap: usize| u64::from(cap > 0);
    match value {
        Built::Buffer(buf) => owned(buf.capacity()),
        Built::Records(records) => {
            owned(records.capacity())
                + records
                    .iter()
                    .map(|r| {
                        owned(r.stream_attrs.capacity())
                            + owned(r.severity_text.capacity())
                            + owned(r.body.capacity())
                            + owned(r.attrs.capacity())
                            + r.attrs
                                .iter()
                                .map(|(k, v)| {
                                    owned(k.capacity())
                                        + match v {
                                            AttrValue::Str(s) => owned(s.capacity()),
                                            _ => 0,
                                        }
                                })
                                .sum::<u64>()
                    })
                    .sum::<u64>()
        }
        Built::Series(series) => {
            owned(series.capacity())
                + series
                    .iter()
                    .map(|s| {
                        owned(s.labels.capacity())
                            + owned(s.samples.capacity())
                            + s.labels
                                .iter()
                                .map(|(n, v)| owned(n.capacity()) + owned(v.capacity()))
                                .sum::<u64>()
                    })
                    .sum::<u64>()
        }
    }
}

/// For a buffer, reads one byte per page and times it; then times the drop
/// alone.
fn read_then_drop(value: Built) -> (Option<Duration>, Duration) {
    let page_read = match &value {
        Built::Buffer(buf) => {
            let t = Instant::now();
            let mut sum = 0u64;
            for page in buf.chunks(PAGE_BYTES) {
                sum = sum.wrapping_add(u64::from(page[0]));
            }
            black_box(sum);
            Some(t.elapsed())
        }
        Built::Records(_) | Built::Series(_) => None,
    };
    let value = black_box(value);
    let t = Instant::now();
    drop(value);
    (page_read, t.elapsed())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Variant {
    CallerDrop,
    JobDrop,
    Inline,
}

impl Variant {
    const ALL: [Variant; 3] = [Variant::CallerDrop, Variant::JobDrop, Variant::Inline];

    fn name(self) -> &'static str {
        match self {
            Variant::CallerDrop => "caller_drop",
            Variant::JobDrop => "job_drop",
            Variant::Inline => "inline",
        }
    }

    /// The gate arm whose counters this variant must leave.
    fn arm(self) -> Arm {
        match self {
            Variant::CallerDrop | Variant::JobDrop => Arm::Gated,
            Variant::Inline => Arm::Inline,
        }
    }
}

struct DropStats {
    jobs: u64,
    drop: Histogram,
    page_read: Histogram,
    round_trip: Histogram,
    errors: Vec<String>,
}

impl DropStats {
    fn new() -> Self {
        DropStats {
            jobs: 0,
            drop: Histogram::new(),
            page_read: Histogram::new(),
            round_trip: Histogram::new(),
            errors: Vec::new(),
        }
    }

    fn record(&mut self, page_read: Option<Duration>, drop: Duration, round_trip: Duration) {
        self.jobs += 1;
        if let Some(p) = page_read {
            self.page_read.record(p);
        }
        self.drop.record(drop);
        self.round_trip.record(round_trip);
    }
}

async fn drop_caller(
    shape: DropShape,
    variant: Variant,
    gate: Arc<ReadGate>,
    deadline: Instant,
) -> DropStats {
    let mut stats = DropStats::new();
    let size = JobSize::Bytes(BUFFER_BYTES as u64);
    while Instant::now() < deadline {
        let before = Instant::now();
        match variant {
            Variant::CallerDrop => match gate.run(SITE, size, move || shape.build()).await {
                Ok(value) => {
                    let (page_read, dropped) = read_then_drop(value);
                    stats.record(page_read, dropped, before.elapsed());
                }
                Err(e) => stats.errors.push(format!("caller_drop job failed: {e}")),
            },
            Variant::JobDrop => {
                match gate
                    .run(SITE, size, move || read_then_drop(shape.build()))
                    .await
                {
                    Ok((page_read, dropped)) => stats.record(page_read, dropped, before.elapsed()),
                    Err(e) => stats.errors.push(format!("job_drop job failed: {e}")),
                }
            }
            Variant::Inline => {
                let (page_read, dropped) = read_then_drop(shape.build());
                stats.record(page_read, dropped, before.elapsed());
                tokio::task::yield_now().await;
            }
        }
    }
    stats
}

struct DropRun {
    stats: DropStats,
    per_caller_jobs: Vec<u64>,
    gate: GateDelta,
}

fn drop_one(
    rt: &tokio::runtime::Runtime,
    gate: &Arc<ReadGate>,
    shape: DropShape,
    variant: Variant,
    callers: usize,
    run_for: Duration,
) -> DropRun {
    let before = gate.snapshot();
    let deadline = Instant::now() + run_for;
    let per_caller: Vec<DropStats> = rt.block_on(async {
        let handles: Vec<_> = (0..callers)
            .map(|_| tokio::spawn(drop_caller(shape, variant, Arc::clone(gate), deadline)))
            .collect();
        let mut out = Vec::with_capacity(callers);
        for h in handles {
            out.push(h.await.expect("drop caller task"));
        }
        out
    });
    let after = gate.snapshot();
    let mut stats = DropStats::new();
    let mut per_caller_jobs = Vec::with_capacity(callers);
    for s in per_caller {
        per_caller_jobs.push(s.jobs);
        stats.jobs += s.jobs;
        stats.drop.merge(&s.drop);
        stats.page_read.merge(&s.page_read);
        stats.round_trip.merge(&s.round_trip);
        stats.errors.extend(s.errors);
    }
    DropRun {
        stats,
        per_caller_jobs,
        gate: gate_delta(&before, &after),
    }
}

/// Every counted job left exactly one drop time and one round trip, and a page
/// read time exactly when the shape reads pages.
fn check_drop_samples(
    shape: DropShape,
    variant: Variant,
    jobs: u64,
    run: &DropRun,
) -> Result<(), String> {
    let expected_page_reads = if shape.reads_pages() { jobs } else { 0 };
    let s = &run.stats;
    if s.drop.total != jobs
        || s.round_trip.total != jobs
        || s.page_read.total != expected_page_reads
    {
        return Err(format!(
            "{} {}: expected {jobs} drop and round-trip samples and {expected_page_reads} page \
             reads, recorded {} drop, {} round-trip, {} page reads",
            shape.name(),
            variant.name(),
            s.drop.total,
            s.round_trip.total,
            s.page_read.total
        ));
    }
    Ok(())
}

/// Paired runs of all three variants for one drop shape and caller count.
/// Returns the median of the per-run caller_drop minus job_drop drop p50s.
fn drop_configuration(
    rt: &tokio::runtime::Runtime,
    gate: &Arc<ReadGate>,
    shape: DropShape,
    callers: usize,
    settings: &Settings,
    rng: &mut OrderRng,
    failures: &mut Vec<String>,
) -> f64 {
    let label = format!("{}/c{callers}", shape.name());
    println!("loadavg before {label}: {}", loadavg());
    let mut report = Report::new(label.clone(), expected_drop_figures(shape.reads_pages()));
    let allocs = count_allocs(&shape.build());
    if allocs != shape.expected_allocs() {
        failures.push(format!(
            "{label}: value owns {allocs} allocations, the shape defines {}",
            shape.expected_allocs()
        ));
    }
    report.figure(
        "allocs_per_value",
        format!("{allocs} (by construction: {})", shape.formula()),
    );

    let mut runs: Vec<[Option<DropRun>; 3]> = Vec::with_capacity(settings.runs);
    let mut orders = Vec::with_capacity(settings.runs);
    for run in 0..settings.runs {
        let mut slot: [Option<DropRun>; 3] = [None, None, None];
        let mut order = Variant::ALL;
        rng.shuffle(&mut order);
        orders.push(order.iter().map(|v| v.name()).collect());
        for variant in order {
            let idx = Variant::ALL.iter().position(|v| *v == variant).unwrap();
            let r = drop_one(rt, gate, shape, variant, callers, settings.run_for);
            let checks = [
                check_counts(variant.arm(), r.stats.jobs, r.gate),
                check_drop_samples(shape, variant, r.stats.jobs, &r),
                check_callers(variant.name(), &r.per_caller_jobs),
            ];
            for e in checks.into_iter().filter_map(Result::err) {
                failures.push(format!("{label} run {run} {}: {e}", variant.name()));
            }
            for e in &r.stats.errors {
                failures.push(format!("{label} run {run}: {e}"));
            }
            slot[idx] = Some(r);
        }
        runs.push(slot);
    }
    println!("order {label}: {}", order_text(&orders));
    let variant_runs = |variant: Variant| -> Vec<&DropRun> {
        let idx = Variant::ALL.iter().position(|v| *v == variant).unwrap();
        runs.iter().filter_map(|s| s[idx].as_ref()).collect()
    };
    let per_run = |variant: Variant, pick: fn(&DropStats) -> &Histogram, q: f64| -> Vec<f64> {
        variant_runs(variant)
            .iter()
            .map(|r| us(pick(&r.stats).percentile(q)))
            .collect()
    };

    for variant in Variant::ALL {
        let name = variant.name();
        let jobs: u64 = variant_runs(variant).iter().map(|r| r.stats.jobs).sum();
        report.figure(format!("{name}.jobs"), jobs);
        report.figure(
            format!("{name}.drop.p50_us"),
            spread(per_run(variant, |s| &s.drop, 0.50)).0,
        );
        report.figure(
            format!("{name}.drop.p99_us"),
            spread(per_run(variant, |s| &s.drop, 0.99)).0,
        );
        report.figure(
            format!("{name}.round_trip.p50_us"),
            spread(per_run(variant, |s| &s.round_trip, 0.50)).0,
        );
        if shape.reads_pages() {
            report.figure(
                format!("{name}.page_read.p50_us"),
                spread(per_run(variant, |s| &s.page_read, 0.50)).0,
            );
        }
    }
    let (diff_text, diff_mid) = spread(
        per_run(Variant::CallerDrop, |s| &s.drop, 0.50)
            .into_iter()
            .zip(per_run(Variant::JobDrop, |s| &s.drop, 0.50))
            .map(|(c, j)| c - j)
            .collect(),
    );
    report.figure("caller_drop_minus_job_drop.drop_p50_us", diff_text);
    if let Err(e) = report.check_present_once() {
        failures.push(e);
    }
    print!("{}", report.text);
    diff_mid
}

/// The verdict lines a run must print, listed independently of the code that
/// prints them.
fn expected_verdicts() -> Vec<String> {
    let mut keys = vec!["empty/c1".to_string(), "buffer/c1".to_string()];
    for records in RECORD_COUNTS {
        for attrs in ATTR_COUNTS {
            keys.push(format!("records/r{records}_a{attrs}/c1"));
        }
    }
    keys.push(format!("series/s{SERIES}_l{LABELS}/c1"));
    keys
}

struct Verdicts {
    tag: &'static str,
    keys: Vec<String>,
}

impl Verdicts {
    fn print(&mut self, key: String, text: String) {
        println!("{key} verdict{} {text}", self.tag);
        self.keys.push(key);
    }

    fn check(&self) -> Result<(), String> {
        let expected = expected_verdicts();
        for key in expected.iter().chain(&self.keys) {
            let n = self.keys.iter().filter(|k| *k == key).count();
            if n != 1 || !expected.contains(key) {
                return Err(format!(
                    "verdict {key} printed {n} times (expected {})",
                    u8::from(expected.contains(key))
                ));
            }
        }
        Ok(())
    }
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

/// The assertions must reject a wrong count. Runs every dispatch arm and every
/// drop variant once at 1 caller for `run_for`, checks it, then checks it
/// again with a wrong expected count: the gate count check with the bench's
/// count one too high (gated) or one gated job added to the gate's counters
/// (inline, bare), and the drop sample check with one job too many. Both
/// must fail. Also checks the histogram's p50 against a known distribution.
fn self_check(
    rt: &tokio::runtime::Runtime,
    gate: &Arc<ReadGate>,
    run_for: Duration,
    spin_iterations: u64,
) -> Result<(), String> {
    let mut h = Histogram::new();
    for i in 1..=1000u64 {
        h.record(Duration::from_micros(i));
    }
    let p50 = us(h.percentile(0.50));
    if !(500.0 * 0.97..=500.0 * 1.03).contains(&p50) || h.total != 1000 {
        return Err(format!(
            "self-check histogram: p50 of 1..=1000 us read {p50:.1} us"
        ));
    }
    for shape in Shape::ALL {
        for arm in Arm::ALL {
            let r = one_arm(rt, gate, shape, arm, 1, run_for, spin_iterations);
            check_counts(arm, r.jobs, r.gate)
                .map_err(|e| format!("self-check {}: {e}", shape.name()))?;
            let wrong = if arm == Arm::Gated {
                check_counts(arm, r.jobs + 1, r.gate)
            } else {
                let mut gate = r.gate;
                gate.jobs += 1;
                check_counts(arm, r.jobs, gate)
            };
            if wrong.is_ok() {
                return Err(format!(
                    "self-check {} {}: a wrong count passed the assertion",
                    shape.name(),
                    arm.name()
                ));
            }
        }
    }
    for shape in DropShape::all() {
        for variant in Variant::ALL {
            let r = drop_one(rt, gate, shape, variant, 1, run_for);
            check_counts(variant.arm(), r.stats.jobs, r.gate)
                .and_then(|()| check_drop_samples(shape, variant, r.stats.jobs, &r))
                .map_err(|e| format!("self-check {} {}: {e}", shape.name(), variant.name()))?;
            if check_drop_samples(shape, variant, r.stats.jobs + 1, &r).is_ok() {
                return Err(format!(
                    "self-check {} {}: a wrong drop sample count passed the assertion",
                    shape.name(),
                    variant.name()
                ));
            }
        }
    }
    Ok(())
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
         inline floor 0 B, site {}; mode {}, {runs} paired run(s) of {run_ms} ms per \
         configuration, arm order shuffled each run with seed {ORDER_SEED:#x}",
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
            "self-check: gate count and drop sample assertions reject a wrong expected count: ok"
        ),
        Err(e) => failures.push(e),
    }
    println!(
        "figures: times in us, median [min-max] over runs of each run's percentile; jobs \
         summed over runs; X_minus_Y is paired run by run, then the median of the per-run \
         differences; drop times cover the drop alone"
    );
    println!();

    let settings = Settings {
        runs,
        run_for: Duration::from_millis(run_ms),
        spin_iterations,
    };
    let mut rng = OrderRng(ORDER_SEED);
    let mut verdicts = Verdicts {
        tag: if full {
            ""
        } else {
            " [quick mode, not a measurement]"
        },
        keys: Vec::new(),
    };
    for callers in CALLERS {
        for shape in Shape::ALL {
            let gmi = configuration(
                &rt,
                &gate,
                shape,
                callers,
                &settings,
                &mut rng,
                &mut failures,
            );
            if callers == 1 && shape == Shape::Empty {
                let verdict = if gmi <= DISPATCH_NOT_CAUSE_US {
                    "dispatch: not the cause"
                } else if gmi >= DISPATCH_SHARED_CAUSE_US {
                    "dispatch: shared cause"
                } else {
                    "dispatch: partial"
                };
                verdicts.print(
                    "empty/c1".to_string(),
                    format!(
                        "{verdict} (gated minus inline p50 {gmi:.1} us; band <= \
                         {DISPATCH_NOT_CAUSE_US} not, >= {DISPATCH_SHARED_CAUSE_US} shared)"
                    ),
                );
            }
            println!();
        }
        for shape in DropShape::all() {
            let diff = drop_configuration(
                &rt,
                &gate,
                shape,
                callers,
                &settings,
                &mut rng,
                &mut failures,
            );
            if callers == 1 {
                let key = format!("{}/c1", shape.name());
                let text = match shape {
                    DropShape::Buffer => {
                        let verdict = if diff > CROSS_THREAD_FREE_US {
                            "cross-thread free: matters"
                        } else {
                            "cross-thread free: does not matter"
                        };
                        format!(
                            "{verdict} (caller_drop minus job_drop drop p50 {diff:.1} us; band \
                             > {CROSS_THREAD_FREE_US})"
                        )
                    }
                    DropShape::Records { .. } | DropShape::Series => format!(
                        "{} (caller_drop minus job_drop drop p50 {diff:.1} us; band <= \
                         {STRUCTURED_RULED_OUT_US} ruled out, >= {STRUCTURED_CANDIDATE_US} \
                         candidate)",
                        structured_verdict(diff)
                    ),
                };
                verdicts.print(key, text);
            }
            println!();
        }
    }
    if let Err(e) = verdicts.check() {
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
