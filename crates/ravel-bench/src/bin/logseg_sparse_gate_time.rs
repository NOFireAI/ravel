//! Stage 2 sparse-input wall-time measurement for issue #2585 (epic #2467).
//! Sibling of `logseg_width_gate_time.rs` (stage 1, issue #2563), over
//! `sparse_corpus` instead of `wide_corpus`: few attributes per record drawn
//! from many distinct keys `K`, which is the shape the stage-1 review flagged
//! as untested -- `ColumnarLogBatch::from_records` allocates one dense
//! `Vec<Option<AttrValue>>` per distinct key ever seen, independent of how
//! many attributes any one record carries.
//!
//! Per shape: arm R (`RlogWriter::push` per record, `finish`) versus arm C
//! (`ColumnarLogBatch::from_records`, drop the source records, `push_columnar`,
//! `finish`). `from_records` is timed as a sub-interval of arm C. No profiler.
//! Report-only; never wired into `cargo bench`.
//!
//! Run directly, one shape per process:
//!   cargo run -p ravel-bench --release --bin logseg_sparse_gate_time -- \
//!     sparse_20000_100 [--skip-col]
//! `--skip-col` times arm R only and skips arm C entirely (and the arm
//! R/arm C correctness comparison), for a shape whose arm-C encode the
//! memory-safety check ahead of the run ruled out.
#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "../../../ravel-logseg/benches/common/mod.rs"]
mod common;
#[path = "sparse_corpus.rs"]
mod sparse_corpus;

use std::time::{Duration, Instant};

use common::{bench_config, bench_identity};
use ravel_logseg::columnar_batch::ColumnarLogBatch;
use ravel_logseg::writer::WriteStats;
use ravel_logseg::{LogRecord, Predicate, RlogReader, RlogWriter};
use sparse_corpus::{build_sparse_corpus, distinct_keys_used, ATTRS_PER_RECORD};

/// (name, n, k). Every shape is 1 stream.
const SHAPES: [(&str, usize, usize); 4] = [
    ("sparse_20000_100", 20_000, 100),
    ("sparse_20000_1000", 20_000, 1_000),
    ("sparse_20000_10000", 20_000, 10_000),
    ("sparse_200000_1000", 200_000, 1_000),
];

/// Shapes the task calls "slow and large": reduced schedule (1 warmup, 3
/// runs of 1 iteration instead of 2 warmups, 3 runs of 3) and the five-minute
/// single-encode bail-out on arm C.
fn is_reduced_schedule(shape: &str) -> bool {
    matches!(shape, "sparse_20000_10000" | "sparse_200000_1000")
}

const TIME_WARMUP_RUNS_FULL: usize = 2;
const TIME_RUNS: usize = 3;
const TIME_ITERS_PER_RUN_FULL: usize = 3;
const TIME_WARMUP_RUNS_REDUCED: usize = 1;
const TIME_ITERS_PER_RUN_REDUCED: usize = 1;

const ARM_C_SLOW_BAILOUT_K: &str = "sparse_20000_10000";
const ARM_C_SLOW_BAILOUT: Duration = Duration::from_secs(300);

fn fail(msg: String) -> ! {
    eprintln!("ASSERTION FAILED: {msg}");
    std::process::exit(1);
}

fn uname_a() -> String {
    std::str::from_utf8(
        &std::process::Command::new("uname")
            .arg("-a")
            .output()
            .expect("uname")
            .stdout,
    )
    .expect("utf8")
    .trim()
    .to_string()
}

fn uptime() -> String {
    std::str::from_utf8(
        &std::process::Command::new("uptime")
            .output()
            .expect("uptime")
            .stdout,
    )
    .expect("utf8")
    .trim()
    .to_string()
}

fn median_f64(values: &[f64]) -> f64 {
    let mut v = values.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).expect("non-NaN value"));
    v[v.len() / 2]
}

fn min_f64(values: &[f64]) -> f64 {
    values.iter().cloned().fold(f64::MAX, f64::min)
}

fn max_f64(values: &[f64]) -> f64 {
    values.iter().cloned().fold(f64::MIN, f64::max)
}

fn row_count(bytes: &[u8]) -> u64 {
    let cfg = bench_config();
    let reader = RlogReader::new(bytes, &cfg).expect("RlogReader::new");
    let (rows, _stats) = reader.scan(&Predicate::And(Vec::new())).expect("scan");
    rows.len() as u64
}

fn run_arm_r_full(corpus: &[LogRecord]) -> (Vec<u8>, WriteStats) {
    let mut w = RlogWriter::new(bench_config(), bench_identity());
    for r in corpus.to_vec() {
        w.push(r).expect("push");
    }
    w.finish_with_stats().expect("finish")
}

fn run_arm_c_full(corpus: &[LogRecord]) -> (Vec<u8>, WriteStats) {
    let source = corpus.to_vec();
    let batch = ColumnarLogBatch::from_records(&source);
    drop(source);
    let mut w = RlogWriter::new(bench_config(), bench_identity());
    w.push_columnar(batch).expect("push_columnar");
    w.finish_with_stats().expect("finish")
}

fn time_arm_r_once(corpus: &[LogRecord]) -> Duration {
    let input = corpus.to_vec();
    let start = Instant::now();
    let mut w = RlogWriter::new(bench_config(), bench_identity());
    for r in input {
        w.push(r).expect("push");
    }
    let (bytes, _stats) = w.finish_with_stats().expect("finish");
    let elapsed = start.elapsed();
    std::hint::black_box(&bytes);
    elapsed
}

struct ArmCTiming {
    total: Duration,
    from_records: Duration,
}

fn time_arm_c_once(corpus: &[LogRecord]) -> ArmCTiming {
    let source = corpus.to_vec();
    let start = Instant::now();
    let fr_start = Instant::now();
    let batch = ColumnarLogBatch::from_records(&source);
    let from_records = fr_start.elapsed();
    drop(source);
    let mut w = RlogWriter::new(bench_config(), bench_identity());
    w.push_columnar(batch).expect("push_columnar");
    let (bytes, _stats) = w.finish_with_stats().expect("finish");
    let total = start.elapsed();
    std::hint::black_box(&bytes);
    ArmCTiming { total, from_records }
}

fn main() {
    let host = uname_a();
    println!("HOST {host}");
    let cfg = bench_config();
    println!(
        "RLOG_CONFIG max_dynamic_columns={} block_target_records={} block_max_bytes={}",
        cfg.max_dynamic_columns, cfg.block_target_records, cfg.block_max_bytes
    );

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!(
            "usage: logseg_sparse_gate_time <shape: {:?}> [--skip-col]",
            SHAPES.map(|s| s.0)
        );
        std::process::exit(2);
    }
    let shape_name = args[1].as_str();
    let Some(&(name, n, k)) = SHAPES.iter().find(|s| s.0 == shape_name) else {
        eprintln!(
            "unknown shape {shape_name:?}; expected one of {:?}",
            SHAPES.map(|s| s.0)
        );
        std::process::exit(2);
    };
    let skip_col = args.get(2).is_some_and(|a| a == "--skip-col");

    let corpus = build_sparse_corpus(n, k);
    if corpus.len() != n {
        fail(format!("{name}: corpus size {} != {n}", corpus.len()));
    }
    for rec in &corpus {
        if rec.attrs.len() != ATTRS_PER_RECORD {
            fail(format!(
                "{name}: record carries {} attrs, expected {ATTRS_PER_RECORD}",
                rec.attrs.len()
            ));
        }
    }
    let distinct_keys = distinct_keys_used(n, k);
    let total_attrs = n * ATTRS_PER_RECORD;
    println!("DISTINCT_KEYS_USED shape={name} k={k} distinct={distinct_keys}");
    println!("TOTAL_ATTR_COUNT shape={name} total={total_attrs}");

    if skip_col {
        println!("ARM_C_SKIPPED shape={name} reason=memory-safety-check-pre-run");
        let (bytes_r, stats_r) = run_arm_r_full(&corpus);
        let rows_r = row_count(&bytes_r);
        if rows_r != n as u64 {
            fail(format!("{name}: arm R row_count {rows_r} != {n}"));
        }
        println!(
            "CORRECTNESS_ARM_R_ONLY shape={name} object_len={} rows_r={rows_r} \
             dyn_used_r={} dyn_overflow_r={}",
            bytes_r.len(),
            stats_r.dynamic_columns_used,
            stats_r.dynamic_columns_overflowed
        );
    } else {
        let (bytes_r, stats_r) = run_arm_r_full(&corpus);
        let (bytes_c, stats_c) = run_arm_c_full(&corpus);
        let hash_r = blake3::hash(&bytes_r).to_hex().to_string();
        let hash_c = blake3::hash(&bytes_c).to_hex().to_string();
        if bytes_r != bytes_c {
            let first_diff = bytes_r
                .iter()
                .zip(bytes_c.iter())
                .position(|(a, b)| a != b)
                .unwrap_or_else(|| bytes_r.len().min(bytes_c.len()));
            fail(format!(
                "{name}: arm R and arm C objects differ (ADR-0109 violation), \
                 first at offset {first_diff}, len R={} len C={}",
                bytes_r.len(),
                bytes_c.len()
            ));
        }
        let rows_r = row_count(&bytes_r);
        let rows_c = row_count(&bytes_c);
        if rows_r != n as u64 {
            fail(format!("{name}: arm R row_count {rows_r} != {n}"));
        }
        if rows_c != n as u64 {
            fail(format!("{name}: arm C row_count {rows_c} != {n}"));
        }
        println!(
            "CORRECTNESS shape={name} object_len={} hash_r={hash_r} hash_c={hash_c} \
             hash_match={} rows_r={rows_r} rows_c={rows_c} dyn_used_r={} dyn_used_c={} \
             dyn_overflow_r={} dyn_overflow_c={}",
            bytes_r.len(),
            hash_r == hash_c,
            stats_r.dynamic_columns_used,
            stats_c.dynamic_columns_used,
            stats_r.dynamic_columns_overflowed,
            stats_c.dynamic_columns_overflowed
        );
    }

    let reduced = is_reduced_schedule(name);
    let warmup_runs = if reduced { TIME_WARMUP_RUNS_REDUCED } else { TIME_WARMUP_RUNS_FULL };
    let iters_per_run = if reduced { TIME_ITERS_PER_RUN_REDUCED } else { TIME_ITERS_PER_RUN_FULL };

    let uptime_before = uptime();
    println!("UPTIME_BEFORE shape={name} {uptime_before}");

    // Five-minute single-encode bail-out on arm C at K=10,000, ahead of the
    // warmup/measured loops: if the first arm-C encode alone exceeds the
    // budget, record it and stop this shape rather than multiplying the cost
    // by (warmups + runs) * iters.
    let mut bailed_out = false;
    if !skip_col && name == ARM_C_SLOW_BAILOUT_K {
        let probe = time_arm_c_once(&corpus);
        if probe.total > ARM_C_SLOW_BAILOUT {
            println!(
                "ARM_C_SLOW_SINGLE_MEASUREMENT shape={name} ns={} secs={:.1} \
                 bailout_threshold_secs=300 stopping_shape=true",
                probe.total.as_nanos(),
                probe.total.as_secs_f64()
            );
            bailed_out = true;
        }
    }

    if !bailed_out {
        for _ in 0..warmup_runs {
            for _ in 0..iters_per_run {
                std::hint::black_box(time_arm_r_once(&corpus));
                if !skip_col {
                    let c = time_arm_c_once(&corpus);
                    std::hint::black_box(c.total);
                }
            }
        }

        let mut r_run_means_ns = Vec::with_capacity(TIME_RUNS);
        let mut c_run_means_ns = Vec::with_capacity(TIME_RUNS);
        let mut c_fr_run_means_ns = Vec::with_capacity(TIME_RUNS);
        for _ in 0..TIME_RUNS {
            let mut r_sum = Duration::ZERO;
            let mut c_sum = Duration::ZERO;
            let mut fr_sum = Duration::ZERO;
            for _ in 0..iters_per_run {
                r_sum += time_arm_r_once(&corpus);
                if !skip_col {
                    let c = time_arm_c_once(&corpus);
                    c_sum += c.total;
                    fr_sum += c.from_records;
                }
            }
            r_run_means_ns.push(r_sum.as_nanos() as f64 / iters_per_run as f64);
            if !skip_col {
                c_run_means_ns.push(c_sum.as_nanos() as f64 / iters_per_run as f64);
                c_fr_run_means_ns.push(fr_sum.as_nanos() as f64 / iters_per_run as f64);
            }
        }

        let uptime_after = uptime();
        println!("UPTIME_AFTER shape={name} {uptime_after}");

        let r_median = median_f64(&r_run_means_ns);
        let r_min = min_f64(&r_run_means_ns);
        let r_max = max_f64(&r_run_means_ns);
        println!(
            "SHAPE {name} R_median_ns={r_median:.0} R_min_ns={r_min:.0} R_max_ns={r_max:.0}"
        );

        if !skip_col {
            let c_median = median_f64(&c_run_means_ns);
            let c_min = min_f64(&c_run_means_ns);
            let c_max = max_f64(&c_run_means_ns);
            let fr_median = median_f64(&c_fr_run_means_ns);
            let fr_min = min_f64(&c_fr_run_means_ns);
            let fr_max = max_f64(&c_fr_run_means_ns);

            let ratios: Vec<f64> = r_run_means_ns
                .iter()
                .zip(c_run_means_ns.iter())
                .map(|(r, c)| c / r)
                .collect();
            let ratio_median = median_f64(&ratios);
            let ratio_min = min_f64(&ratios);
            let ratio_max = max_f64(&ratios);

            let fr_share: Vec<f64> = c_fr_run_means_ns
                .iter()
                .zip(c_run_means_ns.iter())
                .map(|(fr, c)| fr / c)
                .collect();
            let fr_share_median = median_f64(&fr_share);
            let fr_share_min = min_f64(&fr_share);
            let fr_share_max = max_f64(&fr_share);

            println!(
                "SHAPE {name} C_median_ns={c_median:.0} C_min_ns={c_min:.0} C_max_ns={c_max:.0} \
                 FR_median_ns={fr_median:.0} FR_min_ns={fr_min:.0} FR_max_ns={fr_max:.0} \
                 ratio_median={ratio_median:.4} ratio_min={ratio_min:.4} ratio_max={ratio_max:.4} \
                 fr_share_median={fr_share_median:.4} fr_share_min={fr_share_min:.4} fr_share_max={fr_share_max:.4}"
            );
            println!(
                "SHAPE {name} ratios_per_run={}",
                ratios.iter().map(|r| format!("{r:.4}")).collect::<Vec<_>>().join(",")
            );
        }
    } else {
        let uptime_after = uptime();
        println!("UPTIME_AFTER shape={name} {uptime_after}");
    }

    println!("DONE shape={name}");
}
