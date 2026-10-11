//! Round-trip cost of one ADR-1702 CPU gate job (issues #2795, #2818).
//!
//! `CpuGate::run` is a semaphore permit plus `tokio::task::spawn_blocking`
//! per job. `ravel_cpu_gate_wait_seconds` covers only the permit wait; this
//! bench measures the whole round trip, and the two handoffs inside it: from
//! just before `gate.run` to the job's first instruction on a blocking thread
//! (handoff in), and from the job's last instruction to the await returning
//! (handoff out).
//!
//! Every job shape runs in three arms, interleaved run by run:
//!
//! - `inline`: the job called directly on the calling task;
//! - `gated`: `gate.run(..)` on a read gate with `inline_floor_bytes = 0`, so
//!   every job takes a permit and goes to the blocking pool;
//! - `bare`: `tokio::task::spawn_blocking(job).await` with no semaphore, which
//!   splits the gate's own bookkeeping from the blocking pool handoff.
//!
//! Job shapes: `empty` (returns a u64), `spin` (about 50 us of calibrated busy
//! work), `buffer` (allocates and fills a 768 KiB `Vec<u8>` and returns it;
//! the caller reads one byte per 4 KiB page and drops it, the cross-thread
//! free and cold-cache read of the ingest path) and `buffer_self_drop` (the
//! same fill, dropped inside the job, returning its length).
//!
//! Under `cargo bench` the run is 5 paired runs of 1500 ms per configuration.
//! Run any other way (`cargo test --bench cpu_gate_round_trip`, or the binary
//! directly) it is a quick mode: one paired run of 50 ms. `RAVEL_BENCH_RUN_MS`
//! overrides the run length in both modes and `RAVEL_BENCH_GATE_PERMITS` the
//! gate's permits (default: the read gate default for this host). Gate
//! counters and the presence of every figure are asserted; any failure exits
//! non-zero after the table.
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
/// Pre-registered dispatch band for `empty` at 1 caller, gated minus inline p50.
const DISPATCH_NOT_CAUSE_US: f64 = 30.0;
const DISPATCH_SHARED_CAUSE_US: f64 = 150.0;
/// `buffer` minus `buffer_self_drop` gated p50 above this: the free matters.
const CROSS_THREAD_FREE_US: f64 = 50.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Shape {
    Empty,
    Spin,
    Buffer,
    BufferSelfDrop,
}

impl Shape {
    const ALL: [Shape; 4] = [
        Shape::Empty,
        Shape::Spin,
        Shape::Buffer,
        Shape::BufferSelfDrop,
    ];

    fn name(self) -> &'static str {
        match self {
            Shape::Empty => "empty",
            Shape::Spin => "spin",
            Shape::Buffer => "buffer",
            Shape::BufferSelfDrop => "buffer_self_drop",
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

enum Output {
    Word(u64),
    Buffer(Vec<u8>),
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

fn run_shape(shape: Shape, spin_iterations: u64) -> Output {
    match shape {
        Shape::Empty => Output::Word(black_box(7)),
        Shape::Spin => Output::Word(spin(spin_iterations)),
        Shape::Buffer => Output::Buffer(fill()),
        Shape::BufferSelfDrop => Output::Word(fill().len() as u64),
    }
}

/// The caller's side: one byte read per page of a returned buffer, then the
/// drop on this thread.
fn consume(output: Output) -> u64 {
    match output {
        Output::Word(w) => w,
        Output::Buffer(buf) => {
            let mut sum = 0u64;
            for page in buf.chunks(PAGE_BYTES) {
                sum = sum.wrapping_add(u64::from(page[0]));
            }
            drop(black_box(buf));
            sum
        }
    }
}

/// A job's output plus the instants of its first and last instruction.
type Stamped = (Output, Instant, Instant);

fn stamped(shape: Shape, spin_iterations: u64) -> impl FnOnce() -> Stamped + Send + 'static {
    move || {
        let first = Instant::now();
        let out = run_shape(shape, spin_iterations);
        (out, first, Instant::now())
    }
}

#[derive(Default)]
struct CallerStats {
    latencies: Vec<Duration>,
    handoff_in: Vec<Duration>,
    handoff_out: Vec<Duration>,
    errors: Vec<String>,
}

async fn caller(
    shape: Shape,
    arm: Arm,
    gate: Arc<ReadGate>,
    spin_iterations: u64,
    deadline: Instant,
) -> CallerStats {
    let mut stats = CallerStats::default();
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
                black_box(consume(output));
                stats.latencies.push(before.elapsed());
                stats
                    .handoff_in
                    .push(first.saturating_duration_since(before));
                stats
                    .handoff_out
                    .push(returned.saturating_duration_since(last));
            }
            Err(e) => stats.errors.push(format!("{} job failed: {e}", arm.name())),
        }
    }
    stats
}

struct ArmRun {
    jobs: u64,
    p50: Duration,
    p99: Duration,
    handoff_in: Vec<Duration>,
    handoff_out: Vec<Duration>,
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
    let mut latencies = Vec::new();
    let mut handoff_in = Vec::new();
    let mut handoff_out = Vec::new();
    let mut errors = Vec::new();
    for s in stats {
        latencies.extend(s.latencies);
        handoff_in.extend(s.handoff_in);
        handoff_out.extend(s.handoff_out);
        errors.extend(s.errors);
    }
    latencies.sort_unstable();
    let (jobs_before, inline_before) = site_counts(&before);
    let (jobs_after, inline_after) = site_counts(&after);
    ArmRun {
        jobs: latencies.len() as u64,
        p50: percentile(&latencies, 0.50),
        p99: percentile(&latencies, 0.99),
        handoff_in,
        handoff_out,
        gate: GateDelta {
            jobs: jobs_after - jobs_before,
            inline: inline_after - inline_before,
            run_count: after.run_count - before.run_count,
            abandoned: after.abandoned - before.abandoned,
        },
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

    /// Every figure appears on exactly one line of the rendered text.
    fn check_present_once(&self) -> Result<(), String> {
        for name in &self.figures {
            let key = format!("{} {name} ", self.label);
            let n = self.text.lines().filter(|l| l.starts_with(&key)).count();
            if n != 1 {
                return Err(format!("{}: figure {name} printed {n} times", self.label));
            }
        }
        Ok(())
    }
}

struct Settings {
    runs: usize,
    run_for: Duration,
    spin_iterations: u64,
}

/// Paired runs of all three arms for one shape and caller count. Returns the
/// median gated-minus-inline p50 and the gated p50 median (for the
/// cross-thread comparison).
fn configuration(
    rt: &tokio::runtime::Runtime,
    gate: &Arc<ReadGate>,
    shape: Shape,
    callers: usize,
    settings: &Settings,
    failures: &mut Vec<String>,
) -> (f64, f64) {
    let label = format!("{}/c{callers}", shape.name());
    println!("loadavg before {label}: {}", loadavg());
    let mut runs: Vec<[Option<ArmRun>; 3]> = Vec::with_capacity(settings.runs);
    for run in 0..settings.runs {
        let mut slot: [Option<ArmRun>; 3] = [None, None, None];
        for i in 0..Arm::ALL.len() {
            // Rotate the arm order each run so no arm always goes first.
            let idx = (i + run) % Arm::ALL.len();
            let arm = Arm::ALL[idx];
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
            if r.jobs == 0 {
                failures.push(format!("{label} run {run}: {} arm ran no jobs", arm.name()));
            }
            slot[idx] = Some(r);
        }
        runs.push(slot);
    }
    let arm_runs = |arm: Arm| -> Vec<&ArmRun> {
        let idx = Arm::ALL.iter().position(|a| *a == arm).unwrap();
        runs.iter().filter_map(|s| s[idx].as_ref()).collect()
    };

    let mut report = Report::new(label.clone());
    for arm in Arm::ALL {
        let rs = arm_runs(arm);
        let jobs: u64 = rs.iter().map(|r| r.jobs).sum();
        report.figure(format!("{}.jobs", arm.name()), jobs);
        report.figure(
            format!("{}.p50_us", arm.name()),
            spread(rs.iter().map(|r| us(r.p50)).collect()).0,
        );
        report.figure(
            format!("{}.p99_us", arm.name()),
            spread(rs.iter().map(|r| us(r.p99)).collect()).0,
        );
    }
    let paired = |arm: Arm| -> (String, f64) {
        spread(
            arm_runs(arm)
                .iter()
                .zip(arm_runs(Arm::Inline))
                .map(|(a, i)| us(a.p50) - us(i.p50))
                .collect(),
        )
    };
    let (gated_minus_inline, gated_minus_inline_mid) = paired(Arm::Gated);
    report.figure("gated_minus_inline.p50_us", gated_minus_inline);
    report.figure("bare_minus_inline.p50_us", paired(Arm::Bare).0);
    for arm in [Arm::Gated, Arm::Bare] {
        let rs = arm_runs(arm);
        for (which, pick) in [
            (
                "handoff_in",
                (|r: &ArmRun| r.handoff_in.clone()) as fn(&ArmRun) -> Vec<Duration>,
            ),
            ("handoff_out", |r: &ArmRun| r.handoff_out.clone()),
        ] {
            let mut all: Vec<Duration> = rs.iter().flat_map(|r| pick(r)).collect();
            all.sort_unstable();
            report.figure(
                format!("{}.{which}.p50_us", arm.name()),
                format!("{:.1}", us(percentile(&all, 0.50))),
            );
            report.figure(
                format!("{}.{which}.p99_us", arm.name()),
                format!("{:.1}", us(percentile(&all, 0.99))),
            );
        }
    }
    if let Err(e) = report.check_present_once() {
        failures.push(e);
    }
    print!("{}", report.text);
    let gated_mid = spread(arm_runs(Arm::Gated).iter().map(|r| us(r.p50)).collect()).1;
    (gated_minus_inline_mid, gated_mid)
}

/// The count assertion must reject a wrong count. Runs every arm of every
/// shape once at 1 caller for `run_for`, checks it, then checks it again
/// with the bench's count one too high (the gated arm) or with one gated job
/// added to the gate's counters (the inline and bare arms): both must fail.
fn self_check(
    rt: &tokio::runtime::Runtime,
    gate: &Arc<ReadGate>,
    run_for: Duration,
    spin_iterations: u64,
) -> Result<(), String> {
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
         configuration, arm order rotated each run",
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
        Ok(()) => println!("self-check: count assertion rejects a wrong expected count: ok"),
        Err(e) => failures.push(e),
    }
    println!(
        "figures: latency in us, median [min-max] over runs; jobs summed over runs; \
         X_minus_inline is paired run by run; handoff p50/p99 pooled over runs"
    );
    println!();

    let settings = Settings {
        runs,
        run_for: Duration::from_millis(run_ms),
        spin_iterations,
    };
    let mut buffer_c1 = None;
    for callers in CALLERS {
        for shape in Shape::ALL {
            let (gmi, gated_p50) =
                configuration(&rt, &gate, shape, callers, &settings, &mut failures);
            if callers == 1 {
                match shape {
                    Shape::Empty => {
                        let verdict = if gmi <= DISPATCH_NOT_CAUSE_US {
                            "dispatch: not the cause"
                        } else if gmi >= DISPATCH_SHARED_CAUSE_US {
                            "dispatch: shared cause"
                        } else {
                            "dispatch: partial"
                        };
                        println!(
                            "empty/c1 verdict {verdict} (gated minus inline p50 {gmi:.1} us; \
                             band <= {DISPATCH_NOT_CAUSE_US} not, >= {DISPATCH_SHARED_CAUSE_US} \
                             shared)"
                        );
                    }
                    Shape::Buffer => buffer_c1 = Some(gated_p50),
                    Shape::BufferSelfDrop => {
                        let c = buffer_c1.unwrap_or(0.0);
                        let diff = c - gated_p50;
                        let verdict = if diff > CROSS_THREAD_FREE_US {
                            "cross-thread free: matters"
                        } else {
                            "cross-thread free: does not matter"
                        };
                        println!(
                            "buffer_vs_self_drop/c1 verdict {verdict} (gated p50 buffer minus \
                             buffer_self_drop {diff:.1} us; band > {CROSS_THREAD_FREE_US})"
                        );
                    }
                    Shape::Spin => {}
                }
            }
            println!();
        }
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
