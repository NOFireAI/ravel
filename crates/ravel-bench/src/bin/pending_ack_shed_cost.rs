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
//! For each body size (`--samples-per-request`, default 1, 1,000 and 10,000)
//! the bin encodes one Remote Write 1.0 body once, then runs arm P and arm H
//! (see `ravel_bench::pending_ack_shed`) `--repeats` times each, every run on
//! a fresh in-process server, under the same jemalloc global allocator and
//! background thread setting `ravel-server`'s `main` uses. Each run sends the
//! same K requests with 8 in flight and reads the process CPU clock and
//! `/metrics` immediately before and after them.
//!
//! The figure is `(cpu_P - cpu_H) / K` per repeat, and its median, as "extra
//! CPU per post-decode refusal". A size whose arm P runs did not each move the
//! CPU clock by at least 100 times its measured resolution has its figure
//! withheld.
//!
//! Bands, fixed before the first run. Counts, per arm, size and repeat: the
//! client sees exactly K 429s; arm P moves `ravel_ingest_pending_ack_shed_total`
//! by K and `ravel_ingest_concurrency_shed_total` by 0, arm H the reverse;
//! `ravel_store_calls_total{op="put"}` moves by 0; the parked request is still
//! unanswered after the K requests (in arm P with
//! `ravel_ingest_pending_strict_acks` still 1) and answers 204 once released.
//! CPU, the median extra per refusal: 1 sample 5 to 100 us (miss below -50 us
//! or above 1 ms), 1,000 samples 0.1 to 1 ms (miss above 10 ms), 10,000
//! samples 1 to 10 ms (miss above 100 ms).
//!
//! It prints a readable table, then one JSON report with its stamp. Exit 0
//! when every band holds; 1 on a count band miss, ahead of any other
//! verdict; 2 when the run could not
//! measure, or a size's figure was withheld for clock resolution; 3, after the
//! full report, when every count band held and a CPU figure fell outside its
//! prediction.

use std::process::ExitCode;

use anyhow::Context;
use clap::Parser;
use ravel_bench::pending_ack_shed::{
    self as lane, Arm, ArmRun, CLIENT_CONCURRENCY, CpuVerdict, DEFAULT_SAMPLES_PER_REQUEST,
    SizeReport,
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
    /// Samples per Remote Write body; repeatable. Defaults to 1, 1000 and
    /// 10000.
    #[arg(long = "samples-per-request")]
    samples_per_request: Vec<usize>,
    /// Runs of each (arm, size).
    #[arg(long, default_value_t = 3)]
    repeats: usize,
    /// The commit the binary was built from, for the stamp. Falls back to
    /// `RAVEL_BENCH_GIT_COMMIT`; never derived by the bin.
    #[arg(long)]
    git_commit: Option<String>,
}

#[derive(Serialize)]
struct Stamp {
    git_commit: Option<String>,
    uname_m: String,
    nproc: usize,
    requests: usize,
    repeats: usize,
    client_concurrency: usize,
    cpu_clock_resolution_ns: u64,
    body_bytes: Vec<(usize, usize)>,
}

#[derive(Serialize)]
struct Report {
    stamp: Stamp,
    sizes: Vec<SizeReport>,
    exit_code: u8,
}

fn main() -> ExitCode {
    match measure() {
        Ok(code) => ExitCode::from(code),
        Err(err) => {
            println!("RESULT: could not measure: {err:#}");
            ExitCode::from(2)
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

fn measure() -> anyhow::Result<u8> {
    let args = Args::parse();
    if args.requests == 0 || args.repeats == 0 {
        anyhow::bail!("--requests and --repeats must be at least 1");
    }
    let sizes = if args.samples_per_request.is_empty() {
        DEFAULT_SAMPLES_PER_REQUEST.to_vec()
    } else {
        args.samples_per_request.clone()
    };
    let git_commit = args
        .git_commit
        .clone()
        .or_else(|| std::env::var(GIT_COMMIT_ENV).ok());
    run::configure_allocator();
    let resolution_ns = lane::cpu_clock_resolution_ns(RESOLUTION_READS);

    let now_ms = run::wall_now_ns() / 1_000_000;
    let mut bodies = Vec::with_capacity(sizes.len());
    for &samples in &sizes {
        bodies.push((samples, lane::encode_body(samples, now_ms)?));
    }

    let server_rt = run::server_runtime()?;
    let client_rt = run::client_runtime()?;
    let client = lane::lane_client()?;
    let mut tenant_index = 0;
    let mut reports = Vec::with_capacity(bodies.len());
    for (samples, body) in &bodies {
        let mut pairs = Vec::with_capacity(args.repeats);
        for repeat in 0..args.repeats {
            let mut pair: Vec<ArmRun> = Vec::with_capacity(2);
            for arm in [Arm::PostDecode, Arm::Head] {
                let run = client_rt.block_on(lane::measure_arm(
                    server_rt.handle(),
                    &client,
                    arm,
                    body,
                    *samples,
                    args.requests,
                    repeat,
                    tenant_index,
                ))?;
                tenant_index += 1;
                print_run(&run);
                pair.push(run);
            }
            let h = pair.pop().context("arm H run")?;
            let p = pair.pop().context("arm P run")?;
            pairs.push((p, h));
        }
        reports.push(SizeReport::build(
            *samples,
            body.len(),
            pairs,
            resolution_ns,
        ));
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
            requests: args.requests,
            repeats: args.repeats,
            client_concurrency: CLIENT_CONCURRENCY,
            cpu_clock_resolution_ns: resolution_ns,
            body_bytes: bodies.iter().map(|(s, b)| (*s, b.len())).collect(),
        },
        sizes: reports,
        exit_code,
    };
    print_table(&report);
    println!("{}", serde_json::to_string_pretty(&report)?);
    println!("RESULT: exit {exit_code}");
    Ok(exit_code)
}

fn print_run(run: &ArmRun) {
    println!(
        "run: samples={} arm={} repeat={} K={} 429={} other={} cpu_s={:.6} wall_s={:.3}",
        run.samples_per_request,
        run.arm.label(),
        run.repeat,
        run.requests,
        run.client_429,
        run.client_other,
        run.cpu_secs(),
        run.wall_secs
    );
}

fn print_table(report: &Report) {
    let stamp = &report.stamp;
    println!();
    println!(
        "stamp: git_commit={} uname_m={} nproc={} K={} repeats={} client_concurrency={} \
         cpu_clock_resolution_ns={}",
        stamp.git_commit.as_deref().unwrap_or("not supplied"),
        stamp.uname_m,
        stamp.nproc,
        stamp.requests,
        stamp.repeats,
        stamp.client_concurrency,
        stamp.cpu_clock_resolution_ns
    );
    println!(
        "{:>7} {:>9} {:>3} {:>3} {:>6} {:>6} {:>6} {:>12} {:>12} {:>9} {:>10} {:>8}",
        "samples",
        "body_B",
        "arm",
        "rep",
        "K",
        "429",
        "other",
        "pend_shed_d",
        "conc_shed_d",
        "put_d",
        "cpu_s",
        "wall_s"
    );
    for size in &report.sizes {
        for run in &size.runs {
            println!(
                "{:>7} {:>9} {:>3} {:>3} {:>6} {:>6} {:>6} {:>12} {:>12} {:>9} {:>10.6} {:>8.3}",
                size.samples_per_request,
                size.body_bytes,
                run.arm.label(),
                run.repeat,
                run.requests,
                run.client_429,
                run.client_other,
                run.pending_ack_shed_delta,
                run.concurrency_shed_delta,
                run.store_put_delta,
                run.cpu_secs(),
                run.wall_secs
            );
            if let Some(first) = &run.first_other {
                println!("        first non-429 answer: {first}");
            }
        }
    }
    println!();
    for size in &report.sizes {
        let band = size.band.map_or("no band".to_string(), |b| {
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
            &size.resolution_refusal,
            size.extra_cpu_per_refusal_us_median,
        ) {
            (Some(why), _) => println!(
                "{} samples: extra CPU per post-decode refusal WITHHELD: {why}",
                size.samples_per_request
            ),
            (None, Some(median)) => println!(
                "{} samples: extra CPU per post-decode refusal (us) per repeat {:?}, \
                 median {median:.3}; {band}: {:?}",
                size.samples_per_request, size.extra_cpu_per_refusal_us, size.verdict
            ),
            (None, None) => println!(
                "{} samples: no extra CPU figure produced",
                size.samples_per_request
            ),
        }
        for miss in &size.count_misses {
            println!("  COUNT BAND MISS: {miss}");
        }
    }
}
