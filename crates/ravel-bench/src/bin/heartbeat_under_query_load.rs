//! ADR-1702 task 11, scenario 2: query saturation versus the runtime
//! heartbeat (issue #2670). Advisory: CI builds it and runs the band tests,
//! and no CI lane runs the scenario.
//!
//! Run it in the release profile:
//!
//! ```sh
//! cargo run --release -p ravel-bench --features saturation --bin heartbeat_under_query_load
//! ```
//!
//! It starts an in-process query server through
//! `ravel_server::start_with_heartbeat`, in the call order `ravel-server`'s
//! `main` uses and under the same jemalloc global allocator and background
//! thread setting, with the health listener bound on a loopback port. One load task per core (or
//! `--queries`) runs queries back to back for at least `--seconds` (default
//! 60): even tasks a PromQL range query, odd tasks a SQL aggregate over
//! `samples` on `POST /api/v1/sql`. Each task queries its own fixture tenant
//! (`--segments` segments of `--series-per-segment` series, one sample a
//! second for an hour), so the per-tenant SQL memory ceiling applies to one
//! query at a time. Meanwhile a task on the client runtime scrapes
//! `ravel_health_heartbeat_age_seconds` from `/metrics` every 250 ms, with a
//! 15 s deadline per scrape and each scrape's latency recorded beside the age
//! it returned, and a
//! dedicated thread sends `GET /readyz` to the health listener every 100 ms.
//!
//! The SQL queries read `samples`, whose scan holds no read gate, so their
//! decode runs on the server's runtime workers.
//!
//! Before any load it scrapes `/metrics` once and exits 2 when one of
//! `ravel_health_heartbeat_age_seconds`, `ravel_cpu_gate_jobs_total` or
//! `ravel_cpu_gate_inline_total` is missing, naming it, or when the heartbeat
//! family carries other than exactly one sample, naming the count. It also
//! exits 2 with fewer than 2 load tasks, since one task issues no SQL.
//!
//! The bands, fixed before the first run:
//!
//! - `heartbeat_age_adr_bound`: the largest scraped heartbeat age is under
//!   10 s, ADR-1702 decision 9's bound (a third of the 30 s readiness
//!   threshold). At or above 10 s is a hard miss. `/metrics` is served by
//!   the runtime being measured, so a scrape that returned no age counts its
//!   own latency as an age here. It is read before `heartbeat_scrapes`, so a
//!   scrape that timed out past 10 s is named as this hard miss.
//! - `heartbeat_age_expected`: the same maximum is under 2 s, the expected
//!   band. Between 2 s and 10 s the run is outside the expected band and
//!   inside the ADR's, and is reported as such.
//! - `readyz_all_200`: every `/readyz` probe on the health listener answered
//!   200.
//!
//! The rest check that the load happened as stated: `load_window` (at least
//! 60 s), `promql_queries` and `sql_queries` (at least one of each
//! completed), `queries_failed` (0), `heartbeat_scrapes` (no scrape failed)
//! and `readyz_probes` (at least one probe).
//!
//! Every band prints exactly once with its value. The exit code is 0 when
//! every band holds, 1 naming the first band missed, and 2 when the run could
//! not measure at all.
#![allow(clippy::expect_used, clippy::unwrap_used)]

// The allocator the shipped ravel-server binary installs, so the measured
// server allocates as it does.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use ravel_bench::saturation::{self, FigureSet, HostStamp, PROBE_INTERVAL_MS, Scrape, fig};
use ravel_bench::saturation_run::{self as run, NS_PER_HOUR, encode_param, labels, series};
use ravel_object_store::memory::MemoryStore;
use ravel_types::Sample;
use uuid::Uuid;

const SCRAPE_PERIOD: Duration = Duration::from_millis(250);

#[derive(Parser, Debug)]
#[command(about = "ADR-1702 task 11 scenario 2: heartbeat age under one query per core")]
struct Args {
    /// Minimum load duration in seconds.
    #[arg(long, default_value_t = 60)]
    seconds: u64,
    /// Concurrent query tasks, at least 2. Default: one per core.
    #[arg(long)]
    queries: Option<usize>,
    #[arg(long, default_value_t = 4)]
    segments: usize,
    /// Series per segment. The default keeps one SQL query over a tenant
    /// inside the default 256 MiB per-query SQL memory limit.
    #[arg(long, default_value_t = 125)]
    series_per_segment: usize,
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

#[derive(Default)]
struct LoadTally {
    promql_ok: u64,
    sql_ok: u64,
    failures: Vec<String>,
}

fn run_scenario() -> anyhow::Result<ExitCode> {
    let args = Args::parse();
    run::configure_allocator();
    let host = HostStamp::detect();
    println!("{}", host.line());
    let workers = args.queries.unwrap_or(host.cores);
    if workers < 2 {
        anyhow::bail!(
            "{workers} load task(s): the PromQL and SQL loads need at least 2 (pass --queries 2 or more)"
        );
    }

    let hour = run::fixture_hour(run::wall_now_ns());
    let hour_start = hour * NS_PER_HOUR;
    let store = Arc::new(MemoryStore::new());
    let server_rt = run::server_runtime()?;
    let client_rt = run::client_runtime()?;

    // One tenant per load task, each with the same data, so the per-tenant
    // SQL memory ceiling bounds one query rather than all of them at once.
    let built_at = Instant::now();
    let mut on_store = 0usize;
    let mut samples = 0u64;
    for worker in 0..workers {
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
            on_store += written.bytes.len();
            samples += written.summary.sample_count;
            client_rt.block_on(run::publish_segment(
                &store,
                &tenant.hash(),
                writer_id,
                hour,
                &written,
            ))?;
        }
    }
    println!(
        "fixture: {workers} tenants x {} segments x {} series, {samples} samples, {on_store} bytes on store, built in {:.1} s",
        args.segments,
        args.series_per_segment,
        built_at.elapsed().as_secs_f64()
    );

    let server = server_rt.block_on(run::start_server(store, host.cores, workers))?;
    let http = server.http_addr();
    let health = server.health_addr();
    let client = reqwest::Client::builder().build()?;

    let first = client_rt.block_on(run::scrape(&client, http, run::PROBE_TIMEOUT))?;
    if let Err(err) = run::require_families(&first) {
        println!("RESULT: FAIL {err}");
        server_rt.block_on(server.shutdown())?;
        return Ok(ExitCode::from(2));
    }

    let start_s = hour * 3_600;
    let end_s = start_s + 3_599;
    let promql = format!(
        "http://{http}/api/v1/query_range?query={}&start={start_s}&end={end_s}&step=15",
        encode_param("sum by (job) (rate(sat_load[5m]))")
    );
    let sql_body = serde_json::json!({
        "query": "SELECT label(labels, 'job') AS job, count(*) AS n, avg(value) AS mean \
                  FROM samples GROUP BY 1 ORDER BY 1",
        "start": start_s as f64,
        "end": (end_s + 1) as f64,
    });
    let sql_url = format!("http://{http}/api/v1/sql");

    let stop = run::WindowStop::new();
    let prober = run::spawn_prober(
        format!("http://{health}/readyz"),
        Duration::from_millis(PROBE_INTERVAL_MS),
        Arc::clone(&stop),
    )?;
    let start = prober.start;

    let scraper = {
        let stop = Arc::clone(&stop);
        let client = client.clone();
        client_rt.spawn(async move {
            let mut scrapes = Vec::new();
            let mut tick = tokio::time::interval(SCRAPE_PERIOD);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            while !stop.is_closed() {
                tick.tick().await;
                let sent = Instant::now();
                let age_s = run::scrape(&client, http, run::HEARTBEAT_SCRAPE_TIMEOUT)
                    .await
                    .ok()
                    .and_then(|body| saturation::single_value(&body, saturation::HEARTBEAT_FAMILY));
                scrapes.push(Scrape {
                    latency: sent.elapsed(),
                    age_s,
                });
            }
            scrapes
        })
    };

    let min_window = Duration::from_secs(args.seconds);
    let tally = client_rt.block_on(async {
        let mut tasks = Vec::with_capacity(workers);
        for worker in 0..workers {
            let client = client.clone();
            let promql = promql.clone();
            let sql_url = sql_url.clone();
            let sql_body = sql_body.clone();
            tasks.push(tokio::spawn(async move {
                let mut tally = LoadTally::default();
                let is_sql = worker % 2 == 1;
                while start.elapsed() < min_window {
                    let request = if is_sql {
                        client.post(&sql_url).json(&sql_body)
                    } else {
                        client.get(&promql)
                    };
                    let result = match request
                        .header("authorization", format!("Bearer {}", run::token(worker)))
                        .send()
                        .await
                    {
                        Ok(response) => run::query_succeeded(response).await,
                        Err(e) => Err(e.to_string()),
                    };
                    match (result, is_sql) {
                        (Ok(()), true) => tally.sql_ok += 1,
                        (Ok(()), false) => tally.promql_ok += 1,
                        (Err(e), _) => tally.failures.push(e),
                    }
                }
                tally
            }));
        }
        let mut total = LoadTally::default();
        for task in tasks {
            match task.await {
                Ok(t) => {
                    total.promql_ok += t.promql_ok;
                    total.sql_ok += t.sql_ok;
                    total.failures.extend(t.failures);
                }
                Err(e) => total.failures.push(e.to_string()),
            }
        }
        total
    });
    let window = start.elapsed();
    stop.close(window);
    let probes = run::join_prober(prober)?;
    let scrapes = client_rt.block_on(scraper)?;
    for failure in tally.failures.iter().take(3) {
        println!("query failure: {failure}");
    }

    println!(
        "window: {:.3} ms, heartbeat scrapes: {}, readyz probes due in the window: {}",
        saturation::window_ms(window),
        scrapes.len(),
        saturation::probes_in_window(&probes, window).len()
    );
    println!("{}", saturation::scrape_summary(&scrapes));
    let mut figs = FigureSet::new();
    figs.record(fig::WINDOW_MS, saturation::window_ms(window));
    figs.record(fig::PROMQL_QUERIES_OK, tally.promql_ok as f64);
    figs.record(fig::SQL_QUERIES_OK, tally.sql_ok as f64);
    figs.record(fig::QUERIES_FAILED, tally.failures.len() as f64);
    saturation::record_heartbeat_scrapes(&mut figs, &scrapes);
    saturation::record_readyz_probes(&mut figs, window, &probes);
    println!("{}", saturation::wake_lateness_line(&figs));

    let outcomes = saturation::evaluate_heartbeat_under_load(&figs);
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
