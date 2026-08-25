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
#![allow(clippy::expect_used)]

use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::Parser;
use ravel_bench::harness::{StoreKind, store_from_env};
use ravel_bench::sql_corpus::{checked_default_corpus, load_external_corpus};
use ravel_bench::sql_latency::{
    Compaction, GenerateConfig, SqlLatencyReport, TenantConfigInput, run_generated, run_tenant,
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
    #[arg(long = "sql-max-query-bytes", value_name = "BYTES", default_value_t = DEFAULT_MAX_QUERY_BYTES)]
    sql_max_query_bytes: usize,

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
    /// Which layout the tenant is in. ADR-0100 decision 4 requires the report
    /// to state this rather than guess.
    #[arg(long, value_enum, default_value_t = CompactionArg::Pre)]
    compaction: CompactionArg,
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
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("sql_latency_bench: {err}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: &Args) -> Result<SqlLatencyReport, ravel_bench::sql_latency::Error> {
    let entries = match &args.corpus {
        Some(path) => load_external_corpus(path)?,
        None => checked_default_corpus()?,
    };
    let (store_backend, region, endpoint) = provenance_strings(args.store);
    let store = store_from_env(args.store);

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
                compaction: args.compaction.into(),
                max_query_bytes: args.sql_max_query_bytes,
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
            };
            run_generated(&cfg).await
        }
    }
}

fn print_human_table(report: &SqlLatencyReport) {
    let p = &report.provenance;
    let d = &report.dataset;
    println!("\nsql_latency_bench report");
    println!(
        "  backend    : {} (region={}, endpoint={})",
        p.store_backend, p.region, p.endpoint
    );
    println!("  host       : {} logical cores", p.host_logical_cores);
    println!("  source     : {}  dataset={}", p.source, p.dataset_id);
    println!(
        "  dataset    : {} objects, {} bytes, {} rows, layout={}, load={:.1}ms",
        d.object_count, d.stored_bytes, d.rows, d.layout, d.load_wall_ms
    );
    println!("  runs/query : {}", p.runs);
    println!();
    println!(
        "  {:<32} | {:>9} | {:>9} | {:>9} | {:>9} | {:>7} | {:>7} | {:>7} | {:>7}",
        "id", "min ms", "med ms", "max ms", "cold ms", "rows", "blk_tot", "blk_scn", "get"
    );
    println!(
        "  {:-<32}-+-{:-<9}-+-{:-<9}-+-{:-<9}-+-{:-<9}-+-{:-<7}-+-{:-<7}-+-{:-<7}-+-{:-<7}",
        "", "", "", "", "", "", "", "", ""
    );
    for e in &report.entries {
        println!(
            "  {:<32} | {:>9.3} | {:>9.3} | {:>9.3} | {:>9.3} | {:>7} | {:>7} | {:>7} | {:>7}",
            e.id,
            e.min_ms,
            e.median_ms,
            e.max_ms,
            e.cold_ms,
            e.rows_returned,
            e.scan.blocks_total,
            e.scan.blocks_scanned,
            e.scan.object_store_get_requests,
        );
    }
    if !report.skipped.is_empty() {
        println!("\n  skipped (unsatisfied declared column):");
        for s in &report.skipped {
            println!("    {:<32} missing `{}`: {}", s.id, s.missing_key, s.reason);
        }
    }
}
