//! The read CPU gate's per-job cost on the SQL logs scan's columnar path
//! (issue #2795).
//!
//! [`PARTS`] RLOG parts of [`BLOCKS_PER_PART`] blocks each are written under
//! distinct writer identities and published, and a ClickBench-shaped
//! aggregate (`GROUP BY` a URL-like string, `ORDER BY count DESC LIMIT 10`,
//! the shape of ClickBench's `GROUP BY URL` statement) is executed over them
//! through `SqlExecutor` under these configurations:
//!
//! - `inline`: no gate on the fetcher, every block decoded on the worker;
//! - `run1`: the read gate with one job per block (#2715);
//! - `runN`: the read gate with one job per run of up to N blocks, for each
//!   N in [`RUNS`] (or `RAVEL_BENCH_GATE_RUNS`, comma separated).
//!
//! Each runs at one partition and at [`PARTITIONS`] concurrent partitions,
//! against a gate with `default_read_permits(16)` permits and the default
//! inline floor, the reference box's 16 cores. With [`PARTS`] parts, no
//! predicate and a window over all of them, the statement meets the
//! whole-segment fast path's conditions at both partition counts, under
//! which partitions own whole parts.
//!
//! Blocks hold [`ROWS_PER_BLOCK`] rows, `RlogConfig`'s default
//! `block_target_records` (the bench asserts it), so they are cut by record
//! count as a writer with default settings cuts rows this size: 8192 of them
//! stay far under the default 8 MiB `block_max_bytes`. The body, the one
//! column the statement projects, is a URL-like string of about 65 bytes, so
//! a block's body page decodes to about 0.5 MB and every block job is above
//! the default 256 KiB inline floor; the bench asserts the gate ran one
//! `log_block` job per run.
//!
//! After criterion's own report the bench prints a table: per configuration,
//! the median and the min..max spread of wall time per block (statement time
//! divided by the blocks scanned, over every timed statement, criterion's
//! warm-up included), the difference from `inline`, and, where `run1` is
//! slower than `inline`, how much of that each `runN` recovers,
//! `(run1 - runN) / (run1 - inline)`.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use criterion::Criterion;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_cpu_gate::{CpuGateConfig, InstantClock, ReadGate, ReadSite, default_read_permits};
use ravel_logseg::{
    AttrValue, LogRecord, ObjectIdentity, RlogConfig, RlogWriter, stream_attrs_bytes,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{
    BlockRun, ByteLimit, EngineConfig, LogSegmentFetcher, RequestLimit, SegmentFetcher,
};
use ravel_sql::{SpanSegmentFetcher, SqlConfig, SqlExecutor, SqlRequest};
use ravel_types::logstream::log_stream_id;
use ravel_types::{Signal, TenantHash, TenantId, TimeRange};
use uuid::Uuid;

const PARTS: usize = 16;
const BLOCKS_PER_PART: usize = 8;
const ROWS_PER_BLOCK: usize = 8192;
const PARTITIONS: [usize; 2] = [1, 16];
const RUNS: [usize; 2] = [4, 8];
/// Distinct URLs, an eighth of the rows, so the aggregate builds a large hash
/// table.
const DISTINCT_URLS: u64 = (PARTS * BLOCKS_PER_PART * ROWS_PER_BLOCK / 8) as u64;
const NS_PER_HOUR: i64 = 3_600_000_000_000;
const NOW_NS: i64 = 4 * NS_PER_HOUR;
const QUERY: &str = "SELECT body, count(*) AS c FROM logs GROUP BY body ORDER BY c DESC LIMIT 10";
const TOTAL_BLOCKS: usize = PARTS * BLOCKS_PER_PART;

/// A URL-like body for `row`: a host, a path and a query string, about 65
/// bytes.
fn url(row: u64) -> String {
    let id = row.wrapping_mul(0x9E37_79B9_7F4A_7C15) % DISTINCT_URLS;
    format!(
        "https://www.example-{}.com/catalog/item/{}?ref=search&page={}",
        id % 97,
        id,
        id % 13
    )
}

async fn publish_parts(store: &dyn ObjectStoreBackend, tenant: &TenantId) {
    let tenant_hash = tenant.hash();
    let cfg = RlogConfig::default();
    assert_eq!(cfg.block_target_records, ROWS_PER_BLOCK);
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("bench".to_string()),
    )];
    let stream_id = log_stream_id(&resource, "bench", "1.0", &[]);
    let stream_attrs = stream_attrs_bytes(&resource, "bench", "1.0", &[]);
    let mut row = 0u64;
    for part in 0..PARTS {
        let writer_id = Uuid::from_u128(0x2795_0000 + part as u128);
        let writer_seq = part as u64 + 1;
        let mut writer = RlogWriter::new(
            cfg,
            ObjectIdentity {
                tenant_hash: tenant_hash.0,
                shard: 0,
                writer_id: *writer_id.as_bytes(),
                writer_epoch: 1,
                writer_seq,
            },
        );
        let first_ts = row as i64 * 1_000 + 1_000;
        for _ in 0..BLOCKS_PER_PART * ROWS_PER_BLOCK {
            let ts = row as i64 * 1_000 + 1_000;
            writer
                .push(LogRecord {
                    stream_id,
                    stream_attrs: stream_attrs.clone(),
                    ts_ns: ts,
                    observed_ts_ns: ts,
                    severity_num: 9,
                    severity_text: "INFO".to_string(),
                    body: url(row),
                    trace_id: None,
                    span_id: None,
                    flags: 0,
                    attrs: Vec::new(),
                })
                .expect("push record");
            row += 1;
        }
        let last_ts = row as i64 * 1_000;
        let bytes = writer.finish().expect("finish part");
        let rec = record::build(NewCommitRecord {
            tenant_hash,
            signal: Signal::Logs,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq,
            object_size: bytes.len() as u64,
            content_hash: *blake3::hash(&bytes).as_bytes(),
            sample_count: (BLOCKS_PER_PART * ROWS_PER_BLOCK) as u64,
            series_count: 1,
            min_event_ts_ns: first_ts,
            max_event_ts_ns: last_ts,
            min_ingest_ts_ns: first_ts,
            max_ingest_ts_ns: last_ts,
            segment_format_version: u32::from(ravel_logseg::footer::VERSION),
            created_unix_ns: 10,
            ingest_hour_bucket: 0,
        })
        .expect("commit record");
        let key = keys::reconstruct_data_key(&rec).expect("data key");
        store
            .put(&key, bytes::Bytes::from(bytes), PutOptions::default())
            .await
            .expect("put part");
        publish::publish(store, &rec, &RetryPolicy::default())
            .await
            .expect("publish");
    }
}

/// One configuration's executor at `partitions` partitions.
fn executor(
    store: &Arc<dyn ObjectStoreBackend>,
    partitions: usize,
    gate: Option<(Arc<ReadGate>, BlockRun)>,
) -> SqlExecutor {
    let catalog = Arc::new(Catalog::new(Arc::clone(store), CatalogConfig::default()).expect("cat"));
    let mut logs = LogSegmentFetcher::new(Arc::clone(store));
    if let Some((gate, run)) = gate {
        logs = logs.with_read_gate(gate).with_block_run(run);
    }
    let engine = EngineConfig {
        max_series: usize::MAX,
        max_samples: usize::MAX,
        max_bytes_scanned: ByteLimit::Unlimited,
        max_s3_requests: RequestLimit::Unlimited,
        fetch_concurrency: partitions,
        ..EngineConfig::default()
    };
    SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(store)),
        logs,
        SpanSegmentFetcher::new(Arc::clone(store)),
        SqlConfig {
            engine,
            ..SqlConfig::default()
        },
        1 << 34,
    )
}

fn request() -> SqlRequest {
    SqlRequest {
        sql: QUERY.to_string(),
        window: TimeRange {
            start_ns: 0,
            end_ns: NOW_NS,
        },
        min_tokens: Vec::new(),
        now_ns: NOW_NS,
        deadline: Duration::from_secs(600),
        row_window: false,
        max_rows: None,
        budgets: None,
    }
}

/// Every timed statement's wall time per block, by benchmark id.
type Samples = Mutex<BTreeMap<String, Vec<f64>>>;

fn configurations() -> Vec<(String, Option<BlockRun>)> {
    let runs: Vec<usize> = std::env::var("RAVEL_BENCH_GATE_RUNS")
        .ok()
        .map(|v| v.split(',').filter_map(|n| n.trim().parse().ok()).collect())
        .unwrap_or_else(|| RUNS.to_vec());
    let mut configs = vec![
        ("inline".to_string(), None),
        ("run1".to_string(), Some(BlockRun::SINGLE)),
    ];
    for n in runs {
        configs.push((
            format!("run{n}"),
            Some(BlockRun {
                max_blocks: n,
                max_decoded_bytes: u64::MAX,
            }),
        ));
    }
    configs
}

fn bench(c: &mut Criterion, samples: &Samples) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(16)
        .enable_all()
        .build()
        .expect("runtime");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("gate-runs-bench");
    let tenant_hash: TenantHash = tenant.hash();
    rt.block_on(publish_parts(store.as_ref(), &tenant));

    let mut group = c.benchmark_group("logs_scan_gate_runs");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(8));
    for partitions in PARTITIONS {
        for (name, run) in configurations() {
            let gate = run.map(|run| {
                let gate = Arc::new(ReadGate::new(
                    CpuGateConfig::with_permits(default_read_permits(16)),
                    Arc::new(InstantClock::new()),
                ));
                (gate, run)
            });
            let exec = executor(&store, partitions, gate.clone());
            // One untimed statement checks the shape: every block scanned,
            // the expected rows out, and the gate taking the expected jobs.
            let outcome = rt
                .block_on(exec.execute(tenant_hash, &request()))
                .expect("statement");
            assert_eq!(outcome.output.num_rows(), 10, "{name}");
            assert_eq!(
                outcome.stats.blocks_scanned as usize, TOTAL_BLOCKS,
                "{name}: every block scanned"
            );
            if let Some((gate, run)) = &gate {
                let jobs: u64 = gate
                    .snapshot()
                    .sites
                    .iter()
                    .filter(|s| s.site == ReadSite::LogBlock)
                    .map(|s| s.jobs)
                    .sum();
                let per_part = BLOCKS_PER_PART.div_ceil(run.blocks());
                assert_eq!(jobs as usize, PARTS * per_part, "{name}: one job per run");
            }
            let id = format!("{name}/p{partitions}");
            group.bench_function(&id, |b| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let started = Instant::now();
                        let outcome = rt
                            .block_on(exec.execute(tenant_hash, &request()))
                            .expect("statement");
                        let took = started.elapsed();
                        std::hint::black_box(outcome);
                        total += took;
                        samples
                            .lock()
                            .unwrap()
                            .entry(id.clone())
                            .or_default()
                            .push(took.as_nanos() as f64 / TOTAL_BLOCKS as f64);
                    }
                    total
                });
            });
        }
    }
    group.finish();
}

fn median(v: &mut [f64]) -> f64 {
    v.sort_by(f64::total_cmp);
    let n = v.len();
    if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    }
}

fn report(samples: &Samples) {
    let mut samples = samples.lock().unwrap();
    println!();
    println!(
        "per-block wall time, {PARTS} parts x {BLOCKS_PER_PART} blocks x {ROWS_PER_BLOCK} rows, \
         read gate permits {}",
        default_read_permits(16)
    );
    println!(
        "{:<8} {:>5} {:>8} {:>12} {:>12} {:>12} {:>14} {:>10}",
        "config", "parts", "samples", "median_us", "min_us", "max_us", "minus_inline", "recovers"
    );
    for partitions in PARTITIONS {
        let get = |samples: &mut BTreeMap<String, Vec<f64>>, name: &str| {
            samples
                .get_mut(&format!("{name}/p{partitions}"))
                .map(|v| median(v))
        };
        let inline = get(&mut samples, "inline");
        let run1 = get(&mut samples, "run1");
        for (name, _) in configurations() {
            let Some(v) = samples.get_mut(&format!("{name}/p{partitions}")) else {
                continue;
            };
            let med = median(v);
            let (min, max) = (v[0], v[v.len() - 1]);
            let over = inline.map_or(f64::NAN, |i| med - i);
            let recovers = match (inline, run1) {
                (Some(i), Some(r)) if name != "inline" && name != "run1" && r > i => {
                    format!("{:.0}%", (r - med) / (r - i) * 100.0)
                }
                _ => "-".to_string(),
            };
            println!(
                "{:<8} {:>5} {:>8} {:>12.1} {:>12.1} {:>12.1} {:>14.1} {:>10}",
                name,
                partitions,
                v.len(),
                med / 1e3,
                min / 1e3,
                max / 1e3,
                over / 1e3,
                recovers
            );
        }
    }
}

fn main() {
    let samples: Samples = Mutex::new(BTreeMap::new());
    let mut c = Criterion::default().configure_from_args();
    bench(&mut c, &samples);
    c.final_summary();
    report(&samples);
}
