//! Issue #2818 trial: query latency under a fixed Remote Write load, across
//! the write gate arms in `ravel_bench::ingest_query_trial::ARMS`. Advisory:
//! no band, CI builds it and runs the figure-presence tests, and no CI lane
//! runs the trial.
//!
//! ```sh
//! cargo run --release -p ravel-bench --features saturation --bin ingest_query_latency_trial
//! ```
//!
//! For every arm and every client count (1 and 16) it starts a fresh
//! all-mode server through `ravel_server::start_with_heartbeat`, under the
//! same jemalloc allocator and background thread setting the shipped binary
//! uses, with `--strict-flush-policy arrival` so every Remote Write request
//! pays a flush, `--shards` shard actors, and admission limits off so the
//! load is never refused. The arm sets `--cpu-gate-write-inline-floor-bytes`
//! (`u64::MAX` for the ungated arm, which runs every write job inline);
//! every other gate setting is the default for this host.
//!
//! The query tenants hold a fixture as `heartbeat_under_query_load` builds
//! it (`--segments` segments of `--series-per-segment` series, a sample a
//! second for an hour). For `--seconds` (default 20), `clients` tasks post
//! snappy Remote Write 1.0 bodies of about 64 KiB to `/api/v1/write` for a
//! third tenant, back to back, while one task runs a PromQL range query and
//! one task a SQL aggregate, each back to back against its own fixture
//! tenant.
//!
//! Per arm and client count it records PromQL and SQL p50/p99 latency (ms,
//! successful queries only), ingest requests a second and compressed MiB a
//! second, and the counts and failures they rest on. Every figure prints
//! exactly once, read from the figure set. The exit code is 0 when every
//! figure is present exactly once, every configuration completed at least
//! one PromQL query, one SQL query and one ingest request, and nothing
//! failed; 1 naming the first figure or check that did not hold; 2 when the
//! run could not measure at all.
#![allow(clippy::expect_used, clippy::unwrap_used)]

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use clap::Parser;
use ravel_bench::ingest_query_trial::{
    ARMS, BODY_TARGET_BYTES, CLIENTS, CODEC_INLINE_FLOOR_BYTES, RwBodies, TrialArm, TrialFigures,
    WRITE_INLINE_FLOOR_BYTES, expected_figures, figure_name, rw_snappy_bodies,
};
use ravel_bench::saturation::HostStamp;
use ravel_bench::saturation_run::{self as run, NS_PER_HOUR, ServerOptions, labels, series};
use ravel_ingest::{AdmissionLimits, CountLimit, RateLimit};
use ravel_object_store::memory::MemoryStore;
use ravel_server::config::{CpuGatePermits, StrictFlushPolicy};
use ravel_server::{LimitsConfig, Mode};
use ravel_types::Sample;
use uuid::Uuid;

const PROMQL_TENANT: usize = 0;
const SQL_TENANT: usize = 1;
const INGEST_TENANT: usize = 2;
const BODY_VARIANTS: usize = 4;

#[derive(Parser, Debug)]
#[command(about = "Issue #2818: query latency under Remote Write load, per write gate arm")]
struct Args {
    /// Load window per arm and client count, in seconds.
    #[arg(long, default_value_t = 20)]
    seconds: u64,
    #[arg(long, default_value_t = 4)]
    shards: u32,
    #[arg(long, default_value_t = 4)]
    segments: usize,
    #[arg(long, default_value_t = 125)]
    series_per_segment: usize,
}

fn main() -> ExitCode {
    match run_trial() {
        Ok(code) => code,
        Err(err) => {
            println!("RESULT: could not measure: {err:#}");
            ExitCode::from(2)
        }
    }
}

#[derive(Default)]
struct Measured {
    promql: Vec<Duration>,
    sql: Vec<Duration>,
    queries_failed: u64,
    ingest_requests: u64,
    ingest_bytes: u64,
    ingest_failed: u64,
    window: Duration,
    failures: Vec<String>,
}

fn run_trial() -> anyhow::Result<ExitCode> {
    let args = Args::parse();
    if CODEC_INLINE_FLOOR_BYTES != ravel_cpu_gate::DEFAULT_INLINE_FLOOR_BYTES
        || WRITE_INLINE_FLOOR_BYTES != ravel_cpu_gate::DEFAULT_WRITE_INLINE_FLOOR_BYTES
    {
        anyhow::bail!("the trial's floors no longer match the shipped ones");
    }
    run::configure_allocator();
    let host = HostStamp::detect();
    println!("{}", host.line());
    let server_rt = run::server_runtime()?;
    let client_rt = run::client_runtime()?;
    let mut figs = TrialFigures::new();
    for arm in ARMS {
        for clients in CLIENTS {
            let m = measure(&args, &host, arm, clients, &server_rt, &client_rt)?;
            for failure in m.failures.iter().take(3) {
                println!("{} c{clients} failure: {failure}", arm.name);
            }
            println!(
                "{} c{clients}: window {:.3} ms",
                arm.name,
                m.window.as_secs_f64() * 1000.0
            );
            let name = |metric| figure_name(arm.name, clients, metric);
            let secs = m.window.as_secs_f64();
            figs.record_percentile(name("promql_p50_ms"), &m.promql, 0.50);
            figs.record_percentile(name("promql_p99_ms"), &m.promql, 0.99);
            figs.record_percentile(name("sql_p50_ms"), &m.sql, 0.50);
            figs.record_percentile(name("sql_p99_ms"), &m.sql, 0.99);
            if m.ingest_requests > 0 {
                figs.record(name("ingest_req_per_s"), m.ingest_requests as f64 / secs);
                figs.record(
                    name("ingest_mib_per_s"),
                    m.ingest_bytes as f64 / secs / (1024.0 * 1024.0),
                );
            }
            figs.record(name("promql_queries"), m.promql.len() as f64);
            figs.record(name("sql_queries"), m.sql.len() as f64);
            figs.record(name("ingest_requests"), m.ingest_requests as f64);
            figs.record(name("queries_failed"), m.queries_failed as f64);
            figs.record(name("ingest_failed"), m.ingest_failed as f64);
        }
    }

    let (lines, errors) = figs.report(&expected_figures(&ARMS));
    for line in &lines {
        println!("{line}");
    }
    if let Some(first) = errors.first() {
        println!("RESULT: FAIL {first}");
        return Ok(ExitCode::from(1));
    }
    for arm in ARMS {
        for clients in CLIENTS {
            let get = |metric| figs.get(&figure_name(arm.name, clients, metric));
            for (metric, holds) in [
                ("promql_queries", get("promql_queries")? >= 1.0),
                ("sql_queries", get("sql_queries")? >= 1.0),
                ("ingest_requests", get("ingest_requests")? >= 1.0),
                ("queries_failed", get("queries_failed")? == 0.0),
                ("ingest_failed", get("ingest_failed")? == 0.0),
            ] {
                if !holds {
                    println!(
                        "RESULT: FAIL {} does not hold",
                        figure_name(arm.name, clients, metric)
                    );
                    return Ok(ExitCode::from(1));
                }
            }
        }
    }
    println!("RESULT: ADVISORY every figure present once, every load ran");
    Ok(ExitCode::SUCCESS)
}

fn build_fixture(
    args: &Args,
    store: &Arc<MemoryStore>,
    hour: i64,
    client_rt: &tokio::runtime::Runtime,
) -> anyhow::Result<()> {
    let hour_start = hour * NS_PER_HOUR;
    for worker in [PROMQL_TENANT, SQL_TENANT] {
        let tenant = run::tenant(worker);
        for segment in 0..args.segments {
            let mut input = Vec::with_capacity(args.series_per_segment);
            for i in 0..args.series_per_segment {
                let instance = format!("load-{segment}-{i:05}");
                let job = format!("job-{}", i % 8);
                input.push(series(
                    &tenant,
                    "sat_load",
                    labels(&[
                        ("__name__", "sat_load"),
                        ("instance", &instance),
                        ("job", &job),
                    ])?,
                    (0..3_600i64)
                        .map(|s| Sample {
                            ts_ns: hour_start + s * 1_000_000_000 + 1,
                            value: (s * (i as i64 + 1)) as f64,
                        })
                        .collect(),
                )?);
            }
            let writer_id = Uuid::new_v4();
            let written = run::write_segment(&tenant.hash(), writer_id, hour, input)?;
            client_rt.block_on(run::publish_segment(
                store,
                &tenant.hash(),
                writer_id,
                hour,
                &written,
            ))?;
        }
    }
    Ok(())
}

fn options(host: &HostStamp, arm: TrialArm, shards: u32) -> ServerOptions {
    let mut cpu_gate_permits = CpuGatePermits::derive(host.cores);
    cpu_gate_permits.write_inline_floor_bytes = arm.write_inline_floor_bytes;
    ServerOptions {
        mode: Mode::All,
        strict_flush_policy: StrictFlushPolicy::Arrival,
        shard_count: shards,
        cpu_gate_permits,
        limits: LimitsConfig {
            defaults: AdmissionLimits {
                max_active_series: CountLimit::Unlimited,
                max_active_streams: CountLimit::Unlimited,
                ingest_byte_rate: RateLimit::Unlimited,
                series_creation_rate: RateLimit::Unlimited,
            },
            ..LimitsConfig::default()
        },
    }
}

async fn post_write(client: &reqwest::Client, url: &str, body: Vec<u8>) -> Result<(), String> {
    let response = client
        .post(url)
        .header("content-type", "application/x-protobuf")
        .header("content-encoding", "snappy")
        .header("x-prometheus-remote-write-version", "0.1.0")
        .header(
            "authorization",
            format!("Bearer {}", run::token(INGEST_TENANT)),
        )
        .body(body)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let code = response.status();
    if code.is_success() {
        return Ok(());
    }
    let text = response.text().await.unwrap_or_default();
    Err(format!(
        "HTTP {code}: {}",
        text.chars().take(300).collect::<String>()
    ))
}

fn measure(
    args: &Args,
    host: &HostStamp,
    arm: TrialArm,
    clients: usize,
    server_rt: &tokio::runtime::Runtime,
    client_rt: &tokio::runtime::Runtime,
) -> anyhow::Result<Measured> {
    let hour = run::fixture_hour(run::wall_now_ns());
    let store = Arc::new(MemoryStore::new());
    build_fixture(args, &store, hour, client_rt)?;
    let bodies: Arc<RwBodies> = Arc::new(
        rw_snappy_bodies(
            BODY_TARGET_BYTES,
            BODY_VARIANTS,
            run::wall_now_ns() / 1_000_000,
        )
        .context("snappy bodies")?,
    );
    let server = server_rt.block_on(run::start_server_with(
        store,
        INGEST_TENANT + 1,
        options(host, arm, args.shards),
    ))?;
    let http = server.http_addr();
    let client = reqwest::Client::builder().build()?;
    println!(
        "{} c{clients}: floor {} B, {} shards; body {} series, {} B snappy",
        arm.name,
        arm.write_inline_floor_bytes,
        args.shards,
        bodies.series_per_body,
        bodies.compressed_bytes
    );

    let start_s = hour * 3_600;
    let end_s = start_s + 3_599;
    let promql = format!(
        "http://{http}/api/v1/query_range?query={}&start={start_s}&end={end_s}&step=15",
        run::encode_param("sum by (job) (rate(sat_load[5m]))")
    );
    let sql_body = serde_json::json!({
        "query": "SELECT label(labels, 'job') AS job, count(*) AS n, avg(value) AS mean \
                  FROM samples GROUP BY 1 ORDER BY 1",
        "start": start_s as f64,
        "end": (end_s + 1) as f64,
    });
    let sql_url = format!("http://{http}/api/v1/sql");
    let write_url = format!("http://{http}/api/v1/write");

    // One of each before the window, so a server that cannot answer is a
    // could-not-measure rather than a window of failures.
    client_rt.block_on(async {
        let promql_ok = client
            .get(&promql)
            .header(
                "authorization",
                format!("Bearer {}", run::token(PROMQL_TENANT)),
            )
            .send()
            .await
            .map_err(|e| e.to_string());
        match promql_ok {
            Ok(response) => run::query_succeeded(response).await,
            Err(e) => Err(e),
        }
        .map_err(|e| anyhow::anyhow!("warm-up PromQL query: {e}"))?;
        let sql_ok = client
            .post(&sql_url)
            .json(&sql_body)
            .header(
                "authorization",
                format!("Bearer {}", run::token(SQL_TENANT)),
            )
            .send()
            .await
            .map_err(|e| e.to_string());
        match sql_ok {
            Ok(response) => run::query_succeeded(response).await,
            Err(e) => Err(e),
        }
        .map_err(|e| anyhow::anyhow!("warm-up SQL query: {e}"))?;
        post_write(&client, &write_url, bodies.bodies[0].clone())
            .await
            .map_err(|e| anyhow::anyhow!("warm-up Remote Write: {e}"))
    })?;

    let window = Duration::from_secs(args.seconds);
    let measured = client_rt.block_on(async {
        let start = Instant::now();
        let mut writers = Vec::with_capacity(clients);
        for c in 0..clients {
            let client = client.clone();
            let write_url = write_url.clone();
            let bodies = Arc::clone(&bodies);
            writers.push(tokio::spawn(async move {
                let (mut ok, mut bytes, mut failed, mut failures) = (0u64, 0u64, 0u64, Vec::new());
                let mut i = c;
                while start.elapsed() < window {
                    let body = bodies.bodies[i % bodies.bodies.len()].clone();
                    let len = body.len() as u64;
                    match post_write(&client, &write_url, body).await {
                        Ok(()) => {
                            ok += 1;
                            bytes += len;
                        }
                        Err(e) => {
                            failed += 1;
                            failures.push(e);
                        }
                    }
                    i += clients;
                }
                (ok, bytes, failed, failures)
            }));
        }
        let mut queriers = Vec::with_capacity(2);
        for is_sql in [false, true] {
            let client = client.clone();
            let promql = promql.clone();
            let sql_url = sql_url.clone();
            let sql_body = sql_body.clone();
            queriers.push(tokio::spawn(async move {
                let (mut latencies, mut failures) = (Vec::new(), Vec::new());
                while start.elapsed() < window {
                    let request = if is_sql {
                        client.post(&sql_url).json(&sql_body).header(
                            "authorization",
                            format!("Bearer {}", run::token(SQL_TENANT)),
                        )
                    } else {
                        client.get(&promql).header(
                            "authorization",
                            format!("Bearer {}", run::token(PROMQL_TENANT)),
                        )
                    };
                    let sent = Instant::now();
                    let result = match request.send().await {
                        Ok(response) => run::query_succeeded(response).await,
                        Err(e) => Err(e.to_string()),
                    };
                    match result {
                        Ok(()) => latencies.push(sent.elapsed()),
                        Err(e) => failures.push(e),
                    }
                }
                (is_sql, latencies, failures)
            }));
        }
        let mut m = Measured::default();
        for writer in writers {
            match writer.await {
                Ok((ok, bytes, failed, failures)) => {
                    m.ingest_requests += ok;
                    m.ingest_bytes += bytes;
                    m.ingest_failed += failed;
                    m.failures.extend(failures);
                }
                Err(e) => {
                    m.ingest_failed += 1;
                    m.failures.push(e.to_string());
                }
            }
        }
        for querier in queriers {
            match querier.await {
                Ok((is_sql, latencies, failures)) => {
                    if is_sql {
                        m.sql = latencies;
                    } else {
                        m.promql = latencies;
                    }
                    m.queries_failed += failures.len() as u64;
                    m.failures.extend(failures);
                }
                Err(e) => {
                    m.queries_failed += 1;
                    m.failures.push(e.to_string());
                }
            }
        }
        m.window = start.elapsed();
        m
    });
    server_rt.block_on(server.shutdown())?;
    Ok(measured)
}
