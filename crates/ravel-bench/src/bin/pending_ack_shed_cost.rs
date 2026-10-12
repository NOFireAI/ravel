//! The server CPU one post-decode pending-ack refusal costs over one head
//! refusal (ADR-2793 decision 4). Advisory: CI builds it and runs the lane's
//! count test, and no CI lane runs the measurement.
//!
//! Run it in the release profile, naming K:
//!
//! ```sh
//! cargo run --release -p ravel-bench --features saturation \
//!   --bin pending_ack_shed_cost -- --requests 2000 --git-commit "$(git rev-parse HEAD)"
//! ```
//!
//! A body is `SERIESxSAMPLES` (`--body`, repeatable; default 1x1, 1000x1,
//! 1000x10 and 10000x1): that many distinct series with that many samples
//! each. The bin encodes each body once and decodes it once, refusing one
//! that does not decode to the requested series, then runs arm P and arm H
//! (see `ravel_bench::pending_ack_shed`) `--repeats` times each, P first on
//! even repeats and H first on odd ones, every run on a fresh in-process
//! server, under the same jemalloc global allocator and background thread
//! setting `ravel-server`'s `main` uses. Each run sends the same K requests
//! with 8 in flight, each on a fresh connection, and reads the process CPU
//! clock and `/metrics` immediately before and after them.
//!
//! The figure is `(cpu_P - cpu_H) / K` per repeat, and its median, as "extra
//! CPU per post-decode refusal". A body with a repeat whose `|cpu_P - cpu_H|`
//! is under 100 times the CPU clock's measured resolution has its figure
//! withheld.
//!
//! Bands, fixed before the run. Counts, per arm, body and repeat: the client
//! sees exactly K 429s and no transport error; arm P moves
//! `ravel_ingest_pending_ack_shed_total` by K and
//! `ravel_ingest_concurrency_shed_total` by 0, arm H the reverse; no
//! `ravel_store_calls_total{op="put"}` completes inside the region; in arm P
//! `ravel_ingest_pending_strict_acks` is still 1 and the gate holds exactly
//! one call after the region; the parked request is still unanswered after
//! the K requests, answers 204 once released, and the PUT counter has then
//! risen above its value before the region. CPU, the median extra per
//! refusal: 1x1 3 to 20 us (miss below -50 us or above 200 us), 1000x1 0.5 to
//! 2.5 ms (miss above 10 ms), 1000x10 0.7 to 3.5 ms (miss above 15 ms),
//! 10000x1 5 to 25 ms (miss below 2 ms or above 100 ms).
//!
//! It prints a readable table, then one JSON report with its stamp. Exit 0
//! when every band holds; 1 on a count band miss, ahead of any other
//! verdict; 2 when a body's figure was withheld for clock resolution; 3,
//! after the full report, when every count band held and a CPU figure fell
//! outside its prediction; 4 when the run could not finish, after printing
//! the rows it measured.

use std::process::ExitCode;

use anyhow::Context;
use clap::Parser;
use ravel_bench::pending_ack_shed::{
    self as lane, ArmRun, Body, BodyReport, BodySpec, CLIENT_CONCURRENCY, CONNECTION_MODE,
    CpuVerdict, DEFAULT_BODIES,
};
use ravel_bench::saturation_run as run;
use serde::Serialize;

// The allocator the shipped ravel-server binary installs, so the measured
// server allocates as it does.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Reads of the CPU clock's value changes taken to find its resolution.
const RESOLUTION_READS: usize = 1_000;

/// Read when `--git-commit` is not passed.
const GIT_COMMIT_ENV: &str = "RAVEL_BENCH_GIT_COMMIT";

#[derive(Parser, Debug)]
#[command(
    about = "Measure the server CPU of a post-decode pending-ack refusal over a head refusal"
)]
struct Args {
    /// K, the refused requests per arm run. No default: it selects how much
    /// the run measures.
    #[arg(long)]
    requests: usize,
    /// A body as SERIESxSAMPLES, distinct series and samples per series;
    /// repeatable. Defaults to 1x1, 1000x1, 1000x10 and 10000x1.
    #[arg(long = "body")]
    bodies: Vec<BodySpec>,
    /// Runs of each (arm, body).
    #[arg(long, default_value_t = 3)]
    repeats: usize,
    /// The commit the binary was built from, for the stamp. Falls back to
    /// `RAVEL_BENCH_GIT_COMMIT`; never derived by the bin.
    #[arg(long)]
    git_commit: Option<String>,
}

#[derive(Serialize)]
struct BodyStamp {
    series: usize,
    samples_per_series: usize,
    body_bytes: usize,
    decoded_series: usize,
    decoded_samples: usize,
}

#[derive(Serialize)]
struct Stamp {
    git_commit: Option<String>,
    uname_m: String,
    nproc: usize,
    cpu_model: Option<String>,
    requests: usize,
    repeats: usize,
    client_concurrency: usize,
    connection: &'static str,
    cpu_clock_resolution_ns: u64,
    bodies: Vec<BodyStamp>,
}

#[derive(Serialize)]
struct Report {
    stamp: Stamp,
    bodies: Vec<BodyReport>,
    exit_code: u8,
}

fn main() -> ExitCode {
    let mut measured = Vec::new();
    match measure(&mut measured) {
        Ok(code) => ExitCode::from(code),
        Err(err) => {
            println!();
            println!("rows measured before the error:");
            print_header();
            for run in &measured {
                print_row(run);
            }
            println!("RESULT: could not measure: {err:#}");
            ExitCode::from(4)
        }
    }
}

fn uname_m() -> anyhow::Result<String> {
    let out = std::process::Command::new("uname")
        .arg("-m")
        .output()
        .context("running uname -m")?;
    Ok(String::from_utf8(out.stdout)?.trim().to_string())
}

/// The first `model name` in /proc/cpuinfo, where the file exists.
fn cpu_model() -> Option<String> {
    let cpuinfo = std::fs::read_to_string("/proc/cpuinfo").ok()?;
    cpuinfo.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        (key.trim() == "model name").then(|| value.trim().to_string())
    })
}

/// Runs every body and arm, pushing each finished run onto `measured` so an
/// error part way still leaves the rows it measured.
fn measure(measured: &mut Vec<ArmRun>) -> anyhow::Result<u8> {
    let args = Args::parse();
    if args.requests == 0 || args.repeats == 0 {
        anyhow::bail!("--requests and --repeats must be at least 1");
    }
    let specs = if args.bodies.is_empty() {
        DEFAULT_BODIES.to_vec()
    } else {
        args.bodies.clone()
    };
    let git_commit = args
        .git_commit
        .clone()
        .or_else(|| std::env::var(GIT_COMMIT_ENV).ok());
    run::configure_allocator();
    let resolution_ns = lane::cpu_clock_resolution_ns(RESOLUTION_READS)?;

    let now_ms = run::wall_now_ns() / 1_000_000;
    let mut bodies = Vec::with_capacity(specs.len());
    for &spec in &specs {
        let body = Body::build(spec, now_ms)?;
        println!(
            "body: {spec} series={} samples_per_series={} bytes={} decoded_series={} \
             decoded_samples={}",
            spec.series,
            spec.samples_per_series,
            body.bytes.len(),
            body.decoded_series,
            body.decoded_samples
        );
        bodies.push(body);
    }

    let server_rt = run::server_runtime()?;
    let client_rt = run::client_runtime()?;
    let client = lane::lane_client()?;
    let mut tenant_index = 0;
    let mut reports = Vec::with_capacity(bodies.len());
    for body in &bodies {
        let mut pairs = Vec::with_capacity(args.repeats);
        for repeat in 0..args.repeats {
            let mut p = None;
            let mut h = None;
            for arm in lane::arms_in_order(repeat) {
                let run = client_rt.block_on(lane::measure_arm(
                    server_rt.handle(),
                    &client,
                    arm,
                    body,
                    args.requests,
                    repeat,
                    tenant_index,
                ))?;
                tenant_index += 1;
                print_run(&run);
                measured.push(run.clone());
                match arm {
                    lane::Arm::PostDecode => p = Some(run),
                    lane::Arm::Head => h = Some(run),
                }
            }
            pairs.push((p.context("arm P run")?, h.context("arm H run")?));
        }
        reports.push(BodyReport::build(body, pairs, resolution_ns));
    }

    let exit_code = if reports.iter().any(|r| !r.count_misses.is_empty()) {
        1
    } else if reports.iter().any(|r| r.resolution_refusal.is_some()) {
        2
    } else if reports
        .iter()
        .any(|r| r.verdict.is_some_and(CpuVerdict::fails))
    {
        3
    } else {
        0
    };
    let report = Report {
        stamp: Stamp {
            git_commit,
            uname_m: uname_m()?,
            nproc: std::thread::available_parallelism().map_or(1, |n| n.get()),
            cpu_model: cpu_model(),
            requests: args.requests,
            repeats: args.repeats,
            client_concurrency: CLIENT_CONCURRENCY,
            connection: CONNECTION_MODE,
            cpu_clock_resolution_ns: resolution_ns,
            bodies: bodies
                .iter()
                .map(|b| BodyStamp {
                    series: b.spec.series,
                    samples_per_series: b.spec.samples_per_series,
                    body_bytes: b.bytes.len(),
                    decoded_series: b.decoded_series,
                    decoded_samples: b.decoded_samples,
                })
                .collect(),
        },
        bodies: reports,
        exit_code,
    };
    print_table(&report);
    println!("{}", serde_json::to_string_pretty(&report)?);
    println!("RESULT: exit {exit_code}");
    Ok(exit_code)
}

fn print_run(run: &ArmRun) {
    println!(
        "run: body={}x{} order={} arm={} repeat={} K={} 429={} other_status={} \
         transport_errors={} cpu_s={:.6} wall_s={:.3}",
        run.series,
        run.samples_per_series,
        run.order,
        run.arm.label(),
        run.repeat,
        run.requests,
        run.client_429,
        run.client_other_status,
        run.transport_errors,
        run.cpu_secs(),
        run.wall_secs
    );
}

fn print_header() {
    println!(
        "{:>6} {:>4} {:>8} {:>3} {:>3} {:>3} {:>5} {:>5} {:>5} {:>5} {:>6} {:>6} {:>5} {:>4} \
         {:>9} {:>9} {:>6} {:>9} {:>7}",
        "series",
        "sps",
        "body_B",
        "ord",
        "arm",
        "rep",
        "K",
        "429",
        "other",
        "xport",
        "pend_d",
        "conc_d",
        "put_d",
        "held",
        "put_live",
        "cpu_s",
        "wall_s",
        "cores",
        "req_s"
    );
}

fn print_row(run: &ArmRun) {
    println!(
        "{:>6} {:>4} {:>8} {:>3} {:>3} {:>3} {:>5} {:>5} {:>5} {:>5} {:>6} {:>6} {:>5} {:>4} \
         {:>9} {:>9.6} {:>6.3} {:>9.3} {:>7.1}",
        run.series,
        run.samples_per_series,
        run.body_bytes,
        run.order,
        run.arm.label(),
        run.repeat,
        run.requests,
        run.client_429,
        run.client_other_status,
        run.transport_errors,
        run.pending_ack_shed_delta,
        run.concurrency_shed_delta,
        run.store_put_delta,
        run.held_after_region,
        format!(
            "{}>{}",
            run.store_puts_before_region, run.store_puts_after_release
        ),
        run.cpu_secs(),
        run.wall_secs,
        run.cores_busy(),
        run.requests_per_sec()
    );
    if let Some(status) = run.first_other_status {
        println!("        first non-429 status: {status}");
    }
    if let Some(chain) = &run.first_transport_error {
        println!("        first transport error: {chain}");
    }
}

fn print_table(report: &Report) {
    let stamp = &report.stamp;
    println!();
    println!(
        "stamp: git_commit={} uname_m={} nproc={} cpu_model={} K={} repeats={} \
         client_concurrency={} connection={} cpu_clock_resolution_ns={}",
        stamp.git_commit.as_deref().unwrap_or("not supplied"),
        stamp.uname_m,
        stamp.nproc,
        stamp.cpu_model.as_deref().unwrap_or("unknown"),
        stamp.requests,
        stamp.repeats,
        stamp.client_concurrency,
        stamp.connection,
        stamp.cpu_clock_resolution_ns
    );
    for body in &stamp.bodies {
        println!(
            "stamp body: series={} samples_per_series={} body_bytes={} decoded_series={} \
             decoded_samples={}",
            body.series,
            body.samples_per_series,
            body.body_bytes,
            body.decoded_series,
            body.decoded_samples
        );
    }
    print_header();
    for body in &report.bodies {
        for run in &body.runs {
            print_row(run);
        }
    }
    println!();
    for body in &report.bodies {
        let spec = body.spec();
        let band = body.band.map_or("no band".to_string(), |b| {
            format!(
                "band {} to {} us, miss {}above {} us",
                b.lo_us,
                b.hi_us,
                b.miss_below_us
                    .map_or(String::new(), |lo| format!("below {lo} us or ")),
                b.miss_above_us
            )
        });
        match (
            &body.resolution_refusal,
            body.extra_cpu_per_refusal_us_median,
        ) {
            (Some(why), _) => {
                println!("{spec}: extra CPU per post-decode refusal WITHHELD: {why}")
            }
            (None, Some(median)) => println!(
                "{spec}: extra CPU per post-decode refusal (us) per repeat {:?}, \
                 median {median:.3}; {band}: {:?}",
                body.extra_cpu_per_refusal_us, body.verdict
            ),
            (None, None) => println!("{spec}: no extra CPU figure produced"),
        }
        for miss in &body.count_misses {
            println!("  COUNT BAND MISS: {miss}");
        }
    }
}
