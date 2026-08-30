//! Per-query SQL latency benchmark over the ADR-0100 corpus (decision 4).
//!
//! Thin wrapper around `ravel_bench::sql_latency`: it parses the dataset source
//! (`--generate` or `--tenant <id>`) and the store flag, classifies the backend
//! for the report's provenance, drives the measurement core, and prints the
//! report as JSON plus a human table.
//!
//! Gated behind the `sql-latency` feature so the default build never compiles
//! ravel-sql/datafusion/arrow or this bin.
//!
//! Generated lane:
//! `cargo run -p ravel-bench --features sql-latency --bin sql_latency_bench -- --generate`
//!
//! Loaded-tenant lane (needs a tenant already loaded by `ravel-cli load`):
//! `... --bin sql_latency_bench -- --tenant <id> --store s3`
//!
//! Flight SQL lane (the same tenant, executed through a running server; needs
//! the `flight-lane` feature on top of `sql-latency`):
//! `... -- --tenant <id> --store s3 --flight 127.0.0.1:4317`
#![allow(clippy::expect_used)]

use std::process::ExitCode;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::Parser;
use ravel_bench::harness::{StoreKind, store_from_env};
use ravel_bench::sql_corpus::{checked_default_corpus, load_external_corpus};
use ravel_bench::sql_latency::{
    Compaction, DatasetInfo, FlightTarget, GenerateConfig, Provenance, RunAccounting,
    SqlLatencyReport, TenantConfigInput, run_generated, run_tenant,
};
use ravel_sql::DEFAULT_MAX_QUERY_BYTES;
use ravel_types::TimeRange;

#[derive(Parser, Debug)]
#[command(about = "Per-query SQL latency over the ADR-0100 corpus (cold/warm, min/median/max)")]
struct Args {
    /// Build a wide-schema dataset in process and measure against it.
    #[arg(long, conflicts_with = "tenant")]
    generate: bool,
    /// Measure against a tenant already loaded in the configured object store.
    #[arg(long)]
    tenant: Option<String>,
    /// Object-store backend. The generated lane defaults to `memory`; the
    /// tenant lane usually needs `s3` (a loaded tenant lives in durable
    /// storage).
    #[arg(long, value_enum, default_value_t = StoreKind::Memory)]
    store: StoreKind,
    /// Executions per statement; the first is the cold run.
    #[arg(long, default_value_t = 3)]
    runs: usize,
    /// An external corpus file (same format as the checked-in set), run instead
    /// of the checked-in corpus.
    #[arg(long)]
    corpus: Option<String>,
    /// Per-query ceiling, in bytes, on the SQL DataFusion memory pool, mirroring
    /// `ravel-server`'s `--sql-max-query-bytes` (ADR-0088). Raise it to measure
    /// a heavy query that otherwise aborts with `query memory budget exhausted`.
    /// Defaults to ravel-sql's compiled-in 256 MiB, so an unset flag leaves the
    /// measured budget byte-for-byte unchanged.
    ///
    /// Applies to the in-process lanes only. It is not a Flight header, so under
    /// `--flight` the server's own ceiling governs and this flag changes
    /// nothing; the report records `sql_max_query_bytes_effective` as null
    /// there rather than echoing what was asked for.
    #[arg(long = "sql-max-query-bytes", value_name = "BYTES", default_value_t = DEFAULT_MAX_QUERY_BYTES)]
    sql_max_query_bytes: usize,
    /// The engine's `max_segments` ceiling, the same knob as `ravel-server
    /// --max-segments`: the number of sealed, below-watermark segments a
    /// statement may fan out over before it is refused with `query fans out
    /// over too many segments` (ADR-0073 decision 2). Only sealed,
    /// below-watermark segments count, so a freshly loaded tenant can sit far
    /// above this and only trip it once a fold seals its hours; raise it to
    /// measure such a folded tenant. Defaults to ravel-query's compiled-in
    /// 1024, so an unset flag leaves the ceiling unchanged; recorded in the
    /// report's provenance.
    #[arg(long = "sql-max-segments", value_name = "N", default_value_t = ravel_query::DEFAULT_MAX_SEGMENTS)]
    sql_max_segments: usize,
    /// Before measuring each statement, write its physical plan to
    /// `<explain-dir>/<id>.txt` (one file per statement), so the DataFusion
    /// optimizer rules that fired (AggregateStatistics,
    /// single_distinct_to_groupby, pushdown) are readable per statement without
    /// a debugger. The plans are a side artifact: they are not timed and never
    /// part of the report's numbers. Requires `--explain-dir`.
    #[arg(long, requires = "explain_dir")]
    explain: bool,
    /// Directory the `--explain` physical plans are written to, one
    /// `<id>.txt` per statement. Required when `--explain` is set.
    #[arg(long = "explain-dir", value_name = "DIR", requires = "explain")]
    explain_dir: Option<std::path::PathBuf>,
    /// Reuse one `SqlExecutor` (and its in-process catalog caches) across every
    /// statement instead of building a fresh cold executor per statement. A
    /// server holds one process-level catalog and `RecordCache` for a tenant's
    /// whole query stream, so its resolve phase is warm for every statement
    /// after the first; without this flag the bench builds a cold executor per
    /// statement and re-pays the resolve GETs each time, overstating
    /// resolve-phase cost relative to that server (issue #857). Under it, only
    /// the first statement's cold run is a genuine cold resolve. Applies to the
    /// in-process lanes only; the Flight lane executes against a running server
    /// that is already process-warm, so this flag does not reach it.
    #[arg(long)]
    warm_catalog: bool,
    /// Pin the logs suffix-probe window to this many bytes, overriding the
    /// per-object derivation (`ravel_query::derive_suffix_len`, issue #883).
    /// Unset leaves that derivation byte-for-byte unchanged; set, every log
    /// read probes exactly this many trailing bytes. This is the seam a probe
    /// sweep sets the window through: the probe floor can only be tightened
    /// against measured `BlockRangeStats::probe_misses`, and a sweep needs to
    /// set the window from outside the fetcher. No `ravel-server` flag
    /// corresponds to it (it is a measurement knob, not a server setting), and
    /// under `--flight` it does not reach the server, so it changes nothing
    /// there. Applies to the in-process lanes only; recorded in the report's
    /// provenance as `logs_suffix_len` (null when unset).
    #[arg(long = "logs-suffix-len", value_name = "BYTES")]
    logs_suffix_len: Option<u64>,

    // --- generated lane knobs ---------------------------------------------
    /// Distinct log records to generate.
    #[arg(long, default_value_t = 2_000)]
    records: usize,
    /// Records per RLOG object; the lever that sets the dataset's object count.
    #[arg(long, default_value_t = 500)]
    records_per_object: usize,
    /// Filler attribute keys per record, widening the schema past the one
    /// declared `duration_ms` column.
    #[arg(long, default_value_t = 16)]
    extra_attrs: usize,

    // --- tenant lane knobs ------------------------------------------------
    /// The operator's belief about which layout the tenant is in, checked
    /// against the resolved snapshot rather than trusted (issue #834): the
    /// report's `dataset.layout` is always derived from the tenant's actual
    /// segment levels, never from this flag. Unset skips the check; set, a
    /// disagreement refuses the run instead of printing a label that
    /// contradicts what the snapshot holds.
    #[arg(long, value_enum)]
    compaction: Option<CompactionArg>,
    /// Upper bound (unix seconds) of the resolve window and the injected query
    /// clock. Defaults to the wall clock at startup.
    #[arg(long)]
    now_secs: Option<u64>,
    /// How many hours before `--now-secs` the resolve window opens. Bounds the
    /// catalog LIST fan-out; widen it for a tenant whose data is older.
    #[arg(long, default_value_t = 24)]
    window_hours: u64,
    /// Shard count for the tenant's catalog. Defaults to the tenant's durable
    /// provisioning record; pass it for a tenant loaded before those records
    /// existed. Disagreeing with the record's ceiling is an error.
    #[arg(long)]
    shards: Option<u32>,
    /// Read-cache byte budget (ADR-0046) attached to the query fetcher. `0`
    /// (the default) attaches no cache, so measurement is byte-for-byte today's
    /// fetcher; `> 0` builds a RAM tier of this size, so a statement's second
    /// and later runs can serve from cache and the report's `cache_*` counters
    /// become meaningful.
    #[arg(long, default_value_t = 0)]
    cache_bytes: u64,
    /// Per-statement wall deadline, in seconds. A statement that exceeds it
    /// fails with `query exceeded its N ms wall deadline`; the default is the
    /// budget every run used before the flag existed.
    #[arg(long, default_value_t = 30)]
    deadline_secs: u64,
    /// Record a statement that fails to execute (deadline expiry, memory
    /// budget, planning error) in the report's `failed` list and move on to
    /// the next one, instead of aborting the run at the first failure. The
    /// process still exits non-zero when any statement failed, after writing
    /// the report, so a partial table is never mistaken for a complete one.
    #[arg(long, default_value_t = false)]
    continue_on_error: bool,
    /// The executor's `fetch_concurrency` (ADR-0088): logs scan partition count
    /// and the bound on in-flight segment fetches per query, the same knob as
    /// `ravel-server --fetch-concurrency`. A full-scan statement's cold time is
    /// latency-bound at the object store, so it moves nearly linearly with
    /// this. Defaults to ravel-query's compiled-in value (8), the number every
    /// earlier run used; recorded in the report's provenance.
    #[arg(long, default_value_t = ravel_query::DEFAULT_FETCH_CONCURRENCY)]
    fetch_concurrency: usize,
    /// The logs per-request byte budget (ADR-0904), the same knob as
    /// `ravel-server --logs-request-cost-bytes`: the byte cost the logs planner
    /// charges each object against when deciding whether a scan routes through
    /// the ranged probe-then-fetch path instead of a whole-object read. Raising
    /// it makes the planner willing to spend more requests on a scan. Defaults
    /// to ravel-query's compiled-in value, the one every earlier run used;
    /// recorded in the report header and provenance.
    #[arg(long, value_name = "BYTES", default_value_t = ravel_query::DEFAULT_LOG_REQUEST_COST_BYTES)]
    logs_request_cost_bytes: u64,
    /// Append one JSON line per finished statement to this file as the run
    /// goes (`{"outcome":"measured"|"skipped"|"failed", ...}`), flushed per
    /// line. The full report still goes to stdout at the end; this is what
    /// survives a run killed hours in.
    #[arg(long, value_name = "PATH")]
    progress_jsonl: Option<std::path::PathBuf>,
    /// Per-tenant SQL memory ceiling, the same knob as `ravel-server
    /// --sql-tenant-max-bytes`. SEPARATE from `--sql-max-query-bytes`: the
    /// per-query pool bounds one statement, this bounds a tenant across its
    /// concurrent queries, and a statement refused here reports `tenant memory
    /// budget exhausted` rather than `query memory pool exhausted`. Raise both
    /// to measure a heavy aggregate. Defaults to the 1 GiB every earlier run
    /// used.
    #[arg(long, value_name = "BYTES", default_value_t = ravel_bench::sql_latency::DEFAULT_TENANT_MAX_BYTES)]
    sql_tenant_max_bytes: usize,
    /// Let an exact-typed query repartition its final aggregation (ADR-0094,
    /// amended by issue #741), the same knob as `ravel-server
    /// --sql-parallel-final-aggregation`. On by default; pass
    /// `--sql-parallel-final-aggregation=false` (or the bare flag, which stays
    /// accepted and still means on) to measure the pre-amendment
    /// single-partition final. This local value is recorded in the report's
    /// provenance as `parallel_final_aggregation_requested`. For an in-process
    /// lane it reaches the executor and so is also the effective value; under
    /// `--flight` it does NOT reach the server (the setting is not on the Flight
    /// wire), so the report's `parallel_final_aggregation_effective` is null and
    /// the server's own default (on) governs unless the server was started
    /// otherwise.
    #[arg(
        long,
        num_args = 0..=1,
        default_value_t = true,
        default_missing_value = "true",
        action = clap::ArgAction::Set,
    )]
    sql_parallel_final_aggregation: bool,
    /// Execute each statement through a running `ravel-server`'s Flight SQL
    /// endpoint (`host:port`, the server's `--listen-grpc`) instead of the
    /// in-process executor, so the numbers are the ones a client of that server
    /// would see. `--store`/`--tenant` are still required and still used: the
    /// dataset stanza and the tenant's declared columns are resolved from the
    /// object store directly, because a Flight client cannot read the catalog.
    /// Needs the `flight-lane` build feature.
    #[arg(long, value_name = "HOST:PORT", requires = "tenant")]
    flight: Option<String>,
    /// Bearer credential for `--flight`, sent as `authorization: Bearer
    /// <TOKEN>`; this is the token side of the server's `--tenant-token
    /// <TOKEN>=<TENANT>` pair. Falls back to the `RAVEL_FLIGHT_TOKEN`
    /// environment variable, which is the better place for it: a token on the
    /// command line lands in the shell history and in `ps`.
    #[arg(long = "flight-token", value_name = "TOKEN", requires = "flight")]
    flight_token: Option<String>,
}

#[derive(Copy, Clone, Debug, clap::ValueEnum)]
enum CompactionArg {
    Pre,
    Post,
}

impl From<CompactionArg> for Compaction {
    fn from(a: CompactionArg) -> Self {
        match a {
            CompactionArg::Pre => Compaction::Pre,
            CompactionArg::Post => Compaction::Post,
        }
    }
}

/// Classify the store for the report: backend name, region, and endpoint. A
/// custom S3 endpoint is MinIO; a bare S3 config is S3; `MemoryStore` has
/// neither region nor endpoint, so both carry the `"n/a"` sentinel.
fn provenance_strings(store: StoreKind) -> (String, String, String) {
    match store {
        StoreKind::Memory => ("memory".to_string(), "n/a".to_string(), "n/a".to_string()),
        StoreKind::S3 => {
            let region = std::env::var("RAVEL_S3_REGION")
                .ok()
                .filter(|r| !r.is_empty())
                .unwrap_or_else(|| "n/a".to_string());
            match std::env::var("RAVEL_S3_ENDPOINT")
                .ok()
                .filter(|e| !e.is_empty())
            {
                Some(endpoint) => ("minio".to_string(), region, endpoint),
                None => ("s3".to_string(), region, "n/a".to_string()),
            }
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    match run(&args).await {
        Ok(report) => {
            match serde_json::to_string_pretty(&report) {
                Ok(json) => println!("{json}"),
                Err(err) => {
                    eprintln!("sql_latency_bench: failed to serialize report: {err}");
                    return ExitCode::FAILURE;
                }
            }
            print_human_table(&report);
            if report.failed.is_empty() {
                ExitCode::SUCCESS
            } else {
                eprintln!(
                    "sql_latency_bench: {} statement(s) failed to execute; the report above is \
                     partial",
                    report.failed.len()
                );
                ExitCode::FAILURE
            }
        }
        Err(err) => {
            eprintln!("sql_latency_bench: {err}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: &Args) -> Result<SqlLatencyReport, ravel_bench::sql_latency::Error> {
    // Refuse a profiled multi-run pass up front (issue #616): the pprof signal
    // sampler segfaults probabilistically the longer it stays armed, so more
    // runs crash rather than measure. Checked here, before the corpus load and
    // before any store resolve, because it depends only on argv and the
    // environment: refusing after a multi-minute LIST fan-out over a
    // 8,424-object tenant wastes the run and real S3 requests. It is not in
    // `measure_corpus` because library code deciding this from process-global
    // env makes an exported RAVEL_BENCH_PROFILE_SVG fail unrelated tests.
    // The Flight lane executes in `measure_over_flight`, which never constructs
    // a `ProfileSession`, so no sampler exists there and the crash this guards
    // cannot happen; refusing a Flight `--runs 3` (the default) would reject a
    // safe run. Gate on the lanes that actually arm a sampler.
    if args.flight.is_none() {
        ravel_bench::profiling::runs_supported_with_profiling(
            ravel_bench::profiling::profile_requested(),
            args.runs,
        )
        .map_err(ravel_bench::sql_latency::Error::from)?;
    }
    let entries = match &args.corpus {
        Some(path) => load_external_corpus(path)?,
        None => checked_default_corpus()?,
    };
    let (store_backend, region, endpoint) = provenance_strings(args.store);
    let store = store_from_env(args.store);

    // `--explain` requires `--explain-dir` (clap enforces it), so a set
    // `--explain` always carries a directory; an unset flag writes no plans.
    let explain_dir = if args.explain {
        args.explain_dir.clone()
    } else {
        None
    };

    match &args.tenant {
        Some(tenant) => {
            let now_secs = args.now_secs.unwrap_or_else(|| {
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0)
            });
            let now_ns = (now_secs as i64).saturating_mul(1_000_000_000);
            let start_ns =
                now_ns.saturating_sub((args.window_hours as i64).saturating_mul(3_600_000_000_000));
            let cfg = TenantConfigInput {
                store,
                store_backend,
                region,
                endpoint,
                tenant: tenant.clone(),
                entries,
                runs: args.runs,
                window: TimeRange {
                    start_ns,
                    end_ns: now_ns,
                },
                now_ns,
                compaction: args.compaction.map(Into::into),
                max_query_bytes: args.sql_max_query_bytes,
                shards: args.shards,
                cache_bytes: args.cache_bytes,
                deadline: Duration::from_secs(args.deadline_secs),
                continue_on_error: args.continue_on_error,
                fetch_concurrency: args.fetch_concurrency,
                logs_request_cost_bytes: args.logs_request_cost_bytes,
                progress_jsonl: args.progress_jsonl.clone(),
                tenant_max_bytes: args.sql_tenant_max_bytes,
                parallel_final_aggregation: args.sql_parallel_final_aggregation,
                max_segments: args.sql_max_segments,
                explain_dir: explain_dir.clone(),
                warm_catalog: args.warm_catalog,
                logs_suffix_len: args.logs_suffix_len,
                flight: args.flight.as_ref().map(|endpoint| FlightTarget {
                    endpoint: endpoint.clone(),
                    token: args.flight_token.clone().or_else(|| {
                        std::env::var("RAVEL_FLIGHT_TOKEN")
                            .ok()
                            .filter(|t| !t.is_empty())
                    }),
                }),
            };
            run_tenant(&cfg).await
        }
        None => {
            if !args.generate {
                return Err("choose a dataset source: --generate or --tenant <id>".into());
            }
            let cfg = GenerateConfig {
                store,
                store_backend,
                region,
                endpoint,
                entries,
                runs: args.runs,
                records: args.records,
                records_per_object: args.records_per_object,
                extra_attrs: args.extra_attrs,
                max_query_bytes: args.sql_max_query_bytes,
                cache_bytes: args.cache_bytes,
                deadline: Duration::from_secs(args.deadline_secs),
                continue_on_error: args.continue_on_error,
                fetch_concurrency: args.fetch_concurrency,
                logs_request_cost_bytes: args.logs_request_cost_bytes,
                progress_jsonl: args.progress_jsonl.clone(),
                tenant_max_bytes: args.sql_tenant_max_bytes,
                parallel_final_aggregation: args.sql_parallel_final_aggregation,
                max_segments: args.sql_max_segments,
                explain_dir: explain_dir.clone(),
                warm_catalog: args.warm_catalog,
                logs_suffix_len: args.logs_suffix_len,
            };
            run_generated(&cfg).await
        }
    }
}

/// Human-readable label for `parallel_final_aggregation_effective`. `Some(v)`
/// is what governed an in-process run. `None` is the Flight lane: the setting
/// is not on the Flight wire, so this process cannot know what governed the
/// server. That is "server-controlled", NOT "server default": when the server
/// was started with an explicit `--sql-parallel-final-aggregation`, `None` does
/// not mean the compiled-in default, so naming the default is a false claim
/// (issue #763).
fn parallel_final_aggregation_effective_label(effective: Option<bool>) -> String {
    match effective {
        Some(v) => v.to_string(),
        None => "unknown (server-controlled)".to_string(),
    }
}

/// The `dataset` report line, shared by the human table and (structurally,
/// via the same `DatasetInfo` fields) the JSON output, so the two cannot
/// disagree (issue #834). `layout` is always the observed value on `d`, never
/// an echo of `--compaction`. `load=` is appended only when `d.load_wall_ms`
/// is `Some`: a run that performed no load (the `--tenant` lane) omits the
/// figure entirely rather than rendering a measured-looking `0.0ms`.
fn dataset_line(d: &DatasetInfo) -> String {
    let mut line = format!(
        "{} objects, {} bytes, {} rows, layout={}",
        d.object_count, d.stored_bytes, d.rows, d.layout
    );
    if let Some(load_ms) = d.load_wall_ms {
        line.push_str(&format!(", load={load_ms:.1}ms"));
    }
    line
}

/// The report's provenance header, as the block of `  key : value` lines the
/// human table prints above the per-statement rows. Built as a string rather
/// than printed inline so a test can assert a stamped figure is present and
/// present exactly once: a knob whose value the header drops, or stamps twice,
/// leaves a run that cannot say which setting produced it.
fn provenance_header(p: &Provenance, d: &DatasetInfo) -> String {
    let effective =
        parallel_final_aggregation_effective_label(p.parallel_final_aggregation_effective);
    let mut out = String::new();
    out.push_str("\nsql_latency_bench report\n");
    out.push_str(&format!(
        "  backend    : {} (region={}, endpoint={})\n",
        p.store_backend, p.region, p.endpoint
    ));
    out.push_str(&format!(
        "  host       : {} logical cores\n",
        p.host_logical_cores
    ));
    out.push_str(&format!(
        "  source     : {}  dataset={}\n",
        p.source, p.dataset_id
    ));
    if let Some(flight) = &p.flight_endpoint {
        out.push_str(&format!(
            "  flight sql : {flight} (scan diagnostics are not on the wire)\n"
        ));
    }
    out.push_str(&format!("  dataset    : {}\n", dataset_line(d)));
    out.push_str(&format!("  runs/query : {}\n", p.runs));
    out.push_str(&format!(
        "  deadline   : {} s per statement\n",
        p.deadline_secs
    ));
    out.push_str(&format!("  fetch conc : {}\n", p.fetch_concurrency));
    out.push_str(&format!(
        "  req cost   : {} bytes\n",
        p.logs_request_cost_bytes
    ));
    out.push_str(&format!(
        "  query max  : requested={} bytes  effective={}\n",
        p.sql_max_query_bytes_requested,
        match p.sql_max_query_bytes_effective {
            Some(v) => format!("{v} bytes"),
            // A Flight run does not send the ceiling to the server; the
            // effective value is the server's own.
            None => "unknown (server config)".to_string(),
        }
    ));
    out.push_str(&format!(
        "  tenant max : {} bytes  parallel final agg: requested={} effective={}\n",
        p.tenant_max_bytes, p.parallel_final_aggregation_requested, effective
    ));
    out.push_str(&format!(
        "  max segs   : {}  explain: {}\n",
        p.sql_max_segments, p.explain
    ));
    out.push_str(&format!(
        "  warm cat   : {}\n",
        match p.warm_catalog {
            Some(true) =>
                "true (resolve phase warm after the first statement, as a server's would be)",
            Some(false) =>
                "false (resolve phase cold per statement; overstates server resolve cost)",
            // The Flight lane builds no in-process executor, so the flag
            // governed nothing on the wire (issue #857 review).
            None => "n/a (Flight lane; the server governs resolve caching)",
        }
    ));
    if p.cache_bytes > 0 {
        out.push_str(&format!("  read cache : {} bytes\n", p.cache_bytes));
    } else {
        out.push_str("  read cache : off\n");
    }
    match p.logs_suffix_len {
        Some(n) => out.push_str(&format!(
            "  suffix len : {n} bytes (pinned; overrides per-object derivation)\n"
        )),
        None => out.push_str("  suffix len : per-object derivation\n"),
    }
    out
}

fn print_human_table(report: &SqlLatencyReport) {
    let p = &report.provenance;
    let d = &report.dataset;
    print!("{}", provenance_header(p, d));
    println!();
    // `get` is the cold run's object-store GETs; the `w_` columns are the warm
    // run's (run 1) GETs, store bytes, and fetch-cache hits, so a reader can see
    // whether the second execution dropped to plan reads only or still fetched
    // objects (issue #767). The warm columns are `-` for a single-run report or
    // the Flight lane (no per-run accounting on the wire).
    //
    // `pmiss` is the cold run's uncovered tail SECTIONS, plan phase plus scan
    // phase (issue #883) -- not a GET count. A short version-4 probe can miss
    // SKIP_IDX and PAGE_DIR both and count twice while the fetcher coalesces
    // their adjacent ranges into one GET, so it bounds the extra requests from
    // above. It sits next to `get` to be read against it: a `get` column that
    // rises alongside `pmiss` is a probe too short; one that rises with `pmiss`
    // flat is not. The per-phase split is in the report JSON's
    // `per_run_accounting`.
    println!(
        "  {:<32} | {:>9} | {:>9} | {:>9} | {:>9} | {:>7} | {:>7} | {:>7} | {:>7} | {:>7} | {:>7} | {:>9} | {:>7}",
        "id",
        "min ms",
        "med ms",
        "max ms",
        "cold ms",
        "rows",
        "blk_tot",
        "blk_scn",
        "get",
        "pmiss",
        "w_get",
        "w_bytes",
        "w_hit"
    );
    println!(
        "  {:-<32}-+-{:-<9}-+-{:-<9}-+-{:-<9}-+-{:-<9}-+-{:-<7}-+-{:-<7}-+-{:-<7}-+-{:-<7}-+-{:-<7}-+-{:-<7}-+-{:-<9}-+-{:-<7}",
        "", "", "", "", "", "", "", "", "", "", "", "", ""
    );
    for e in &report.entries {
        // The Flight lane has no scan diagnostics to print (they are executor
        // counters, and nothing carries them over the wire). `-` says absent;
        // a `0` would read as "scanned nothing".
        let (blocks_total, blocks_scanned, gets) = match &e.scan {
            Some(scan) => (
                scan.blocks_total.to_string(),
                scan.blocks_scanned.to_string(),
                scan.object_store_get_requests.to_string(),
            ),
            None => ("-".to_string(), "-".to_string(), "-".to_string()),
        };
        // The warm run is run index 1. Absent when the run had fewer than two
        // executions, or on the Flight lane which carries no per-run accounting.
        let (warm_gets, warm_bytes, warm_hits) = match e.per_run_accounting.as_deref() {
            Some(runs) if runs.len() >= 2 => (
                runs[1].object_store_get_requests.to_string(),
                runs[1].object_store_bytes.to_string(),
                runs[1].cache_hits.to_string(),
            ),
            _ => ("-".to_string(), "-".to_string(), "-".to_string()),
        };
        // The cold run is run index 0, so this needs no minimum run count, only
        // the per-run array the Flight lane does not carry.
        let probe_misses = match e.per_run_accounting.as_deref() {
            Some([cold, ..]) => (cold.probe_misses_plan + cold.probe_misses_scan).to_string(),
            _ => "-".to_string(),
        };
        println!(
            "  {:<32} | {:>9.3} | {:>9.3} | {:>9.3} | {:>9.3} | {:>7} | {:>7} | {:>7} | {:>7} | {:>7} | {:>7} | {:>9} | {:>7}",
            e.id,
            e.min_ms,
            e.median_ms,
            e.max_ms,
            e.cold_ms,
            e.rows_returned,
            blocks_total,
            blocks_scanned,
            gets,
            probe_misses,
            warm_gets,
            warm_bytes,
            warm_hits,
        );
    }
    print_open_shapes(report);
    print_fetch_amplification(report);
    if !report.skipped.is_empty() {
        println!("\n  skipped (unsatisfied declared column):");
        for s in &report.skipped {
            println!("    {:<32} missing `{}`: {}", s.id, s.missing_key, s.reason);
        }
    }
    if !report.failed.is_empty() {
        println!("\n  failed (executed, no number):");
        for f in &report.failed {
            println!("    {:<32} run {}: {}", f.id, f.run, f.error);
        }
    }
}

/// The cold run's logs-scan fast-path opens, split by the read shape the router
/// chose (issue #904), printed next to the request accounting the main table
/// already shows so a reader can pair the two: `backend_bills_requests` (in the
/// report's request accounting) says whether the backend charges for requests,
/// and this split says which read shape produced them.
///
/// The unit is a SEGMENT OPEN, not a request and not a statement: one statement
/// spanning several segments contributes one open per segment and can take both
/// routes in a single query, so the two columns each count segments. An open is
/// NOT one GET -- a whole-object open is a single GET, a ranged open issues
/// several -- so these columns must not be read as, or summed with, the `get`
/// column in the table above.
fn print_open_shapes(report: &SqlLatencyReport) {
    let rows: Vec<(&str, &RunAccounting)> = report
        .entries
        .iter()
        .filter_map(|e| match e.per_run_accounting.as_deref() {
            Some([cold, ..]) => Some((e.id.as_str(), cold)),
            _ => None,
        })
        .collect();
    if rows.is_empty() {
        return;
    }
    println!("\n  logs-scan fast-path opens, cold run: SEGMENT opens by read shape, not requests.");
    println!(
        "  One statement can take both routes; a ranged open issues several GETs, so these are"
    );
    println!("  not the `get` column above and cannot be summed with it.");
    println!(
        "  {:<32} | {:>18} | {:>12}",
        "id", "whole_object_opens", "ranged_opens",
    );
    println!("  {:-<32}-+-{:-<18}-+-{:-<12}", "", "", "");
    for (id, acc) in rows {
        println!(
            "  {:<32} | {:>18} | {:>12}",
            id, acc.logs_whole_object_opens, acc.logs_ranged_opens,
        );
    }
}

/// The cold run's fetch amplification and the per-phase wire bytes behind it
/// (issue #913).
///
/// Every label spells out which of the two byte kinds it is, because the block
/// carries both and they cannot be summed or compared: `wire_bytes_*` is what
/// the object store transferred (coalescing holes and retries included), and
/// `page_stored_bytes_decoded` is the post-compression length of the pages the
/// decode kept, as they sit in the object.
fn print_fetch_amplification(report: &SqlLatencyReport) {
    let rows: Vec<(&str, &RunAccounting)> = report
        .entries
        .iter()
        .filter_map(|e| match e.per_run_accounting.as_deref() {
            Some([cold, ..]) => Some((e.id.as_str(), cold)),
            _ => None,
        })
        .collect();
    if rows.is_empty() {
        return;
    }
    println!(
        "\n  fetch amplification, cold run: scan-phase WIRE bytes per STORED page byte decoded."
    );
    println!(
        "  Probe, PAGE_DIR, SKIP_IDX and directory bytes are the plan/probe phases, not the numerator."
    );
    println!(
        "  Not the same quantity as page_bytes_fetched / page_bytes_decoded, which is stored over stored."
    );
    println!(
        "  {:<32} | {:>16} | {:>16} | {:>16} | {:>16} | {:>16} | {:>25} | {:>13}",
        "id",
        "wire_bytes_scan",
        "wire_bytes_probe",
        "wire_bytes_plan",
        "wire_bytes_resolve",
        "wire_bytes_unattr",
        "page_stored_bytes_decoded",
        "amplification",
    );
    println!(
        "  {:-<32}-+-{:-<16}-+-{:-<16}-+-{:-<16}-+-{:-<16}-+-{:-<16}-+-{:-<25}-+-{:-<13}",
        "", "", "", "", "", "", "", ""
    );
    let phase = |acc: &RunAccounting, name: &str| {
        acc.wire_bytes_by_phase
            .iter()
            .find(|p| p.phase == name)
            .map_or("-".to_string(), |p| p.wire_bytes.to_string())
    };
    for (id, acc) in rows {
        println!(
            "  {:<32} | {:>16} | {:>16} | {:>16} | {:>16} | {:>16} | {:>25} | {:>13.3}",
            id,
            phase(acc, "scan"),
            phase(acc, "probe"),
            phase(acc, "plan"),
            phase(acc, "resolve"),
            acc.wire_bytes_unattributed,
            acc.page_stored_bytes_decoded,
            acc.fetch_amplification,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The Flight lane's effective-aggregation label is "server-controlled",
    /// not "server default" (issue #763): a `None` effective value means only
    /// that this process cannot know the server's setting, and an explicit
    /// server-side `--sql-parallel-final-aggregation` makes "default" false.
    /// Reverting the string to "unknown (server default)" flips this red.
    #[test]
    fn flight_effective_label_is_server_controlled() {
        assert_eq!(
            parallel_final_aggregation_effective_label(None),
            "unknown (server-controlled)"
        );
        assert_eq!(
            parallel_final_aggregation_effective_label(Some(true)),
            "true"
        );
        assert_eq!(
            parallel_final_aggregation_effective_label(Some(false)),
            "false"
        );
    }

    /// A provenance with `logs_request_cost_bytes` set to `cost` and every
    /// other field at a plausible fixed value, for exercising the header stamp
    /// in isolation.
    fn provenance_with_cost(cost: u64) -> Provenance {
        Provenance {
            store_backend: "memory".to_string(),
            region: "n/a".to_string(),
            endpoint: "n/a".to_string(),
            host_logical_cores: 4,
            source: "generate".to_string(),
            dataset_id: "t".to_string(),
            runs: 3,
            cache_bytes: 0,
            deadline_secs: 30,
            fetch_concurrency: ravel_query::DEFAULT_FETCH_CONCURRENCY,
            logs_request_cost_bytes: cost,
            sql_max_query_bytes_requested: DEFAULT_MAX_QUERY_BYTES,
            sql_max_query_bytes_effective: Some(DEFAULT_MAX_QUERY_BYTES),
            tenant_max_bytes: 1 << 30,
            sql_max_segments: ravel_query::DEFAULT_MAX_SEGMENTS,
            parallel_final_aggregation_requested: true,
            parallel_final_aggregation_effective: Some(true),
            explain: false,
            warm_catalog: Some(false),
            logs_suffix_len: None,
            flight_endpoint: None,
        }
    }

    /// The header stamps the configured request-cost value, in bytes, exactly
    /// once. A distinctive value is used so the exactly-once count cannot be
    /// satisfied by a coincidental match against another field, and the line is
    /// asserted verbatim so a dropped `bytes` unit or a changed value fails.
    #[test]
    fn header_stamps_configured_request_cost_once() {
        let cost = 4_242_424;
        let d = dataset("pre-compaction", None);
        let header = provenance_header(&provenance_with_cost(cost), &d);
        let line = format!("  req cost   : {cost} bytes");
        assert!(
            header.contains(&line),
            "header must stamp the configured request cost verbatim; got:\n{header}"
        );
        assert_eq!(
            header.matches(&format!("{cost}")).count(),
            1,
            "the request-cost value must be stamped exactly once; got:\n{header}"
        );
    }

    /// With the flag absent, the value reaching the header is
    /// `ravel_query::DEFAULT_LOG_REQUEST_COST_BYTES`, so the stamp shows the
    /// constant, not a hardcoded literal that could drift from the engine.
    #[test]
    fn header_stamps_default_request_cost_when_absent() {
        let d = dataset("pre-compaction", None);
        let header = provenance_header(
            &provenance_with_cost(ravel_query::DEFAULT_LOG_REQUEST_COST_BYTES),
            &d,
        );
        let line = format!(
            "  req cost   : {} bytes",
            ravel_query::DEFAULT_LOG_REQUEST_COST_BYTES
        );
        assert!(
            header.contains(&line),
            "an unspecified run stamps the engine default; got:\n{header}"
        );
    }

    fn dataset(layout: &str, load_wall_ms: Option<f64>) -> DatasetInfo {
        DatasetInfo {
            load_wall_ms,
            stored_bytes: 4096,
            object_count: 3,
            rows: 60,
            layout: layout.to_string(),
        }
    }

    /// Both layout directions must show up verbatim in the printed line
    /// (issue #834): a fix that hardcodes one direction passes only one of
    /// these two assertions.
    #[test]
    fn dataset_line_reports_both_layout_directions() {
        assert!(dataset_line(&dataset("pre-compaction", None)).contains("layout=pre-compaction"));
        assert!(dataset_line(&dataset("post-compaction", None)).contains("layout=post-compaction"));
    }

    /// A run with no load omits the figure entirely rather than rendering a
    /// measured-looking `0.0ms` (issue #834).
    #[test]
    fn dataset_line_omits_load_when_absent_and_shows_it_when_present() {
        assert!(!dataset_line(&dataset("pre-compaction", None)).contains("load="));
        assert!(dataset_line(&dataset("pre-compaction", Some(12.5))).contains("load=12.5ms"));
    }

    /// The human table's `layout`/`load` substrings must agree with what the
    /// same `DatasetInfo` serializes to in JSON: both read off one struct, so
    /// there is exactly one code path per field, not two that can drift
    /// apart.
    #[test]
    fn dataset_line_agrees_with_json_serialization() {
        let with_load = dataset("post-compaction", Some(7.0));
        let json = serde_json::to_value(&with_load).expect("DatasetInfo serializes");
        assert_eq!(json["layout"], "post-compaction");
        assert_eq!(json["load_wall_ms"], 7.0);
        assert!(dataset_line(&with_load).contains("layout=post-compaction"));
        assert!(dataset_line(&with_load).contains("load=7.0ms"));

        let no_load = dataset("pre-compaction", None);
        let json = serde_json::to_value(&no_load).expect("DatasetInfo serializes");
        assert!(
            json.get("load_wall_ms").is_none(),
            "load_wall_ms must be omitted from JSON when absent, not null: {json}"
        );
        assert_eq!(json["layout"], "pre-compaction");
        assert!(dataset_line(&no_load).contains("layout=pre-compaction"));
        assert!(!dataset_line(&no_load).contains("load="));
    }
}
