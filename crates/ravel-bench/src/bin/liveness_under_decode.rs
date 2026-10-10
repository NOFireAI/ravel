//! ADR-1702 task 11, scenario 1: decode saturation versus liveness
//! (issue #2670). Advisory: no CI lane runs it.
//!
//! Run it in the release profile:
//!
//! ```sh
//! cargo run --release -p ravel-bench --features saturation --bin liveness_under_decode
//! ```
//!
//! It starts an in-process query server through
//! `ravel_server::start_with_heartbeat`, as `ravel-server`'s `main` does, with
//! the health listener bound on a loopback port. The fixture is one metrics
//! segment whose catalog decode is one decode unit: 256 MiB divided by the
//! scale divisor (see `--scale-divisor`). The bin issues one PromQL instant
//! query per core at once; each query decodes the whole catalog of that
//! segment through the server's query path, the decode ADR-1702 task 7 places
//! on the read gate at site `segment_sparse_catalog`. The `decode_jobs` band
//! reports whether it ran there. While the queries run, a prober on its own
//! thread sends `GET /healthz` to the health listener every 100 ms.
//!
//! Before any load it scrapes `/metrics` once and exits 2 when one of
//! `ravel_health_heartbeat_age_seconds`, `ravel_cpu_gate_jobs_total` or
//! `ravel_cpu_gate_inline_total` is missing, naming it.
//!
//! The bands, fixed before the first run:
//!
//! - `probes_issued`: exactly `floor(window_ms / 100)` probes in the decode
//!   window. A probe that overruns its 100 ms slot delays the rest, so a
//!   slow listener reads as a short count.
//! - `probes_answered`: every one of those probes answered 200.
//! - `probe_latency_max`: the slowest probe took under 250 ms.
//! - `inline_jobs`: `ravel_cpu_gate_inline_total` moved by exactly 0 over the
//!   window, summed over every gate and site.
//!
//! Three more bands check that the load happened as stated:
//! `decode_jobs` (one `segment_sparse_catalog` read gate job per query),
//! `decode_unit_size` (the decode unit is at least its target) and
//! `queries_failed` (0).
//!
//! The server takes the gate's default inline floors (256 KiB, 100,000
//! samples): `ServerConfig` has no setting for them. The fixture sizes both
//! gated units the queries produce above those floors: the catalog decode at
//! the decode unit size (checked by `decode_unit_size`), and the selected
//! series at 120,000 samples for the PromQL evaluation. Neither runs inline
//! because of the floor, so `inline_jobs` reads as it would at a floor of 0.
//!
//! Every band prints exactly once with its value. The exit code is 0 when
//! every band holds, 1 naming the first band missed, and 2 when the run could
//! not measure at all.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::Context;
use clap::Parser;
use ravel_bench::saturation::{
    self, DECODE_UNIT_BYTES, FigureSet, HostStamp, PROBE_INTERVAL_MS, fig,
};
use ravel_bench::saturation_run::{
    self as run, NS_PER_HOUR, catalog_decode_bytes, encode_param, labels, series,
};
use ravel_object_store::memory::MemoryStore;
use ravel_types::{Sample, TenantId};
use uuid::Uuid;

/// Bytes of process memory one decode is assumed to hold per byte of decode
/// unit: the fetched object, the charged decode, and the decoded entries'
/// structs and strings. Only sizes the scale divisor; the run prints the
/// divisor it used.
const MEMORY_PER_UNIT_BYTE: u64 = 4;
/// The one series the queries select: enough samples that its evaluation is
/// above the read gate's 100,000-sample evaluation floor.
const TARGET_SAMPLES: usize = 120_000;

#[derive(Parser, Debug)]
#[command(about = "ADR-1702 task 11 scenario 1: health probes during one catalog decode per core")]
struct Args {
    /// Divide the 256 MiB decode unit by this. Default: the smallest power of
    /// two at which one decode per core fits in half of MemAvailable, at 4
    /// bytes of memory per unit byte.
    #[arg(long)]
    scale_divisor: Option<u64>,
    /// Concurrent decodes. Default: one per core.
    #[arg(long)]
    decodes: Option<usize>,
}

fn main() -> ExitCode {
    match run_scenario() {
        Ok(code) => code,
        Err(err) => {
            println!("RESULT: could not measure: {err:#}");
            ExitCode::from(2)
        }
    }
}

/// The filler series' label value padding, unique per series so the label
/// dictionary grows with the series count.
fn pad(i: usize) -> String {
    format!(
        "{:016x}{:016x}",
        (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15),
        i
    )
}

/// Writes one segment whose catalog decode is at least `target` bytes:
/// `sat_target` with [`TARGET_SAMPLES`] samples, plus single-sample
/// `sat_filler` series. Sizes a first attempt from an estimate per series and
/// rebuilds once from the measured figure when it falls short.
fn build_fixture(
    tenant: &TenantId,
    hour: i64,
    target: u64,
) -> anyhow::Result<(ravel_segment::WrittenSegment, u64, usize, Uuid)> {
    let hour_start = hour * NS_PER_HOUR;
    let mut fillers = usize::try_from(target / 120)?.max(1);
    for _ in 0..3 {
        let mut input = Vec::with_capacity(fillers + 1);
        let step = NS_PER_HOUR / TARGET_SAMPLES as i64;
        input.push(series(
            tenant,
            "sat_target",
            labels(&[("__name__", "sat_target"), ("instance", "target")])?,
            (0..TARGET_SAMPLES)
                .map(|i| Sample {
                    ts_ns: hour_start + 1 + i as i64 * step,
                    value: i as f64,
                })
                .collect(),
        )?);
        for i in 0..fillers {
            let instance = format!("filler-{i:09}");
            let pad = pad(i);
            input.push(series(
                tenant,
                "sat_filler",
                labels(&[
                    ("__name__", "sat_filler"),
                    ("instance", &instance),
                    ("pad", &pad),
                ])?,
                vec![Sample {
                    ts_ns: hour_start + 1_000_000_000,
                    value: 1.0,
                }],
            )?);
        }
        let writer_id = Uuid::new_v4();
        let written = run::write_segment(&tenant.hash(), writer_id, hour, input)?;
        let decode = catalog_decode_bytes(&written.bytes)?;
        if decode >= target {
            return Ok((written, decode, fillers, writer_id));
        }
        let scaled = (fillers as f64 * target as f64 / decode as f64 * 1.02).ceil();
        fillers = scaled as usize;
    }
    anyhow::bail!("could not size the fixture to a {target} byte catalog decode in three attempts")
}

fn run_scenario() -> anyhow::Result<ExitCode> {
    let args = Args::parse();
    let host = HostStamp::detect();
    println!("{}", host.line());
    let decodes = args.decodes.unwrap_or(host.cores).max(1);
    let available = host
        .mem_available_kib
        .map(|k| k * 1024)
        .context("MemAvailable unreadable; pass --scale-divisor")?;
    let divisor = match args.scale_divisor {
        Some(d) => d.max(1),
        None => saturation::decode_scale_divisor(decodes as u64, MEMORY_PER_UNIT_BYTE, available),
    };
    let target = DECODE_UNIT_BYTES / divisor;
    println!(
        "scale: decode unit {DECODE_UNIT_BYTES} / divisor {divisor} = {target} bytes, {decodes} concurrent decodes"
    );
    println!(
        "inline floors in effect: {} bytes, {} samples (server defaults; not settable through ServerConfig)",
        ravel_cpu_gate::DEFAULT_INLINE_FLOOR_BYTES,
        ravel_cpu_gate::DEFAULT_EVAL_FLOOR_SAMPLES
    );

    let tenant = run::tenant(0);
    let hour = run::fixture_hour(run::wall_now_ns());
    let built_at = Instant::now();
    let (written, decode_bytes, fillers, writer_id) = build_fixture(&tenant, hour, target)?;
    println!(
        "fixture: 1 segment, {} series, {} bytes on store, catalog decode unit {} bytes, built in {:.1} s",
        fillers + 1,
        written.bytes.len(),
        decode_bytes,
        built_at.elapsed().as_secs_f64()
    );

    let store = Arc::new(MemoryStore::new());
    let server_rt = run::server_runtime()?;
    let client_rt = run::client_runtime()?;
    client_rt.block_on(run::publish_segment(
        &store,
        &tenant.hash(),
        writer_id,
        hour,
        &written,
    ))?;
    drop(written);

    let server = server_rt.block_on(run::start_server(store, host.cores, 1))?;
    let http = server.http_addr();
    let health = server.health_addr();
    let client = reqwest::Client::builder().build()?;

    let before = client_rt.block_on(run::scrape(&client, http))?;
    if let Err(err) = run::require_families(&before) {
        println!("RESULT: FAIL {err}");
        return Ok(ExitCode::from(2));
    }

    let query = encode_param("count_over_time(sat_target[1h])");
    let time_s = (hour + 1) * 3_600;
    let url = format!("http://{http}/api/v1/query?query={query}&time={time_s}");

    let stop = Arc::new(AtomicBool::new(false));
    let start = Instant::now();
    let prober = run::spawn_prober(
        format!("http://{health}/healthz"),
        start,
        Duration::from_millis(PROBE_INTERVAL_MS),
        Arc::clone(&stop),
    )?;
    let failures = client_rt.block_on(async {
        let mut tasks = Vec::with_capacity(decodes);
        for _ in 0..decodes {
            let client = client.clone();
            let url = url.clone();
            tasks.push(tokio::spawn(async move {
                match client
                    .get(&url)
                    .header("authorization", format!("Bearer {}", run::token(0)))
                    .send()
                    .await
                {
                    Ok(response) => run::query_succeeded(response).await,
                    Err(e) => Err(e.to_string()),
                }
            }));
        }
        let mut failures = Vec::new();
        for task in tasks {
            match task.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => failures.push(e),
                Err(e) => failures.push(e.to_string()),
            }
        }
        failures
    });
    let window = start.elapsed();
    stop.store(true, Ordering::Release);
    let probes = run::join_prober(prober)?;
    let after = client_rt.block_on(run::scrape(&client, http))?;
    for failure in failures.iter().take(3) {
        println!("query failure: {failure}");
    }

    let in_window: Vec<_> = probes.iter().filter(|p| p.issued_at <= window).collect();
    let inline = |body: &str| saturation::family_sum(body, "ravel_cpu_gate_inline_total", &[]);
    let decode_jobs = |body: &str| {
        saturation::family_sum(
            body,
            "ravel_cpu_gate_jobs_total",
            &["gate=\"read\"", "site=\"segment_sparse_catalog\""],
        )
    };
    let mut figs = FigureSet::new();
    figs.record(fig::WINDOW_MS, window.as_millis() as f64);
    figs.record(fig::PROBES_ISSUED, in_window.len() as f64);
    figs.record(
        fig::PROBES_ANSWERED_200,
        in_window.iter().filter(|p| p.status == Some(200)).count() as f64,
    );
    figs.record_series(
        fig::PROBE_LATENCY_MS,
        in_window
            .iter()
            .map(|p| p.latency.as_secs_f64() * 1000.0)
            .collect(),
    );
    figs.record(fig::INLINE_JOBS, inline(&after) - inline(&before));
    figs.record(fig::DECODES_ISSUED, decodes as f64);
    figs.record(fig::DECODE_JOBS, decode_jobs(&after) - decode_jobs(&before));
    figs.record(fig::DECODE_UNIT_BYTES, decode_bytes as f64);
    figs.record(fig::DECODE_UNIT_TARGET_BYTES, target as f64);
    figs.record(fig::QUERIES_FAILED, failures.len() as f64);
    println!(
        "window: {} ms, probes after the window (not counted): {}",
        window.as_millis(),
        probes.len() - in_window.len()
    );

    let outcomes = saturation::evaluate_decode_liveness(&figs);
    for outcome in &outcomes {
        println!("{}", outcome.line());
    }
    server_rt.block_on(server.shutdown())?;
    match saturation::first_miss(&outcomes) {
        None => {
            println!("RESULT: PASS");
            Ok(ExitCode::SUCCESS)
        }
        Some(miss) => {
            println!("RESULT: FAIL first band missed: {}", miss.band);
            Ok(ExitCode::from(1))
        }
    }
}
