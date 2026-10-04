//! Stage 1 width gate (issue #2563, epic #2467, ADR-2467 decision 4): wall
//! time and correctness, row versus columnar, on a WIDE (105-dynamic-attribute)
//! corpus, which the stage 0c/0f measurements (issues #2475, #2485) never
//! exercised (their corpus carries 4 dynamic attributes). The accepted
//! decision routes row-shaped input through the columnar builder
//! (`ColumnarLogBatch::from_records` then `push_columnar`); this bin is the
//! gate on whether that route is still cheap once `from_records`'s pivot has
//! 105 columns to build instead of 4.
//!
//! Per shape: arm R (`RlogWriter::push` per record, `finish`) versus arm C
//! (`ColumnarLogBatch::from_records`, drop the source records, `push_columnar`,
//! `finish` -- the accepted decision's route, i.e. stage 0f's `col_dropped`).
//! `from_records` is timed as a sub-interval of arm C so its share of the
//! total is reported. No profiler, no `stage0::HOOK`. Report-only; never
//! wired into `cargo bench`.
//!
//! Run directly:
//!   cargo run -p ravel-bench --release --bin logseg_width_gate_time [-- <shape>]
//! With no argument every shape runs in one process; with a shape name only
//! that shape runs.
#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "../../../ravel-logseg/benches/common/mod.rs"]
mod common;
#[path = "wide_corpus.rs"]
mod wide_corpus;

use std::time::{Duration, Instant};

use common::{bench_config, bench_identity};
use ravel_logseg::columnar_batch::ColumnarLogBatch;
use ravel_logseg::field_dir::FieldDir;
use ravel_logseg::footer;
use ravel_logseg::writer::WriteStats;
use ravel_logseg::{LogRecord, Predicate, RlogReader, RlogWriter};
use wide_corpus::{build_wide_corpus, WIDE_ATTR_COUNT};

const RECORDS_PER_OBJECT: usize = 20_000;

/// (name, streams, records_per_stream, is_wide). Control shapes reuse
/// `common::build_corpus` (the existing 4-dynamic-attribute corpus, same
/// session, same binary dependency as stage 0f); wide shapes use
/// `wide_corpus::build_wide_corpus` (105 dynamic attributes).
const SHAPES: [(&str, usize, usize, bool); 4] = [
    ("control_1_stream", 1, 20_000, false),
    ("control_1000_streams", 1_000, 20, false),
    ("wide_1_stream", 1, 20_000, true),
    ("wide_1000_streams", 1_000, 20, true),
];

const TIME_WARMUP_RUNS: usize = 3;
const TIME_RUNS: usize = 5;
const TIME_ITERS_PER_RUN: usize = 10;

const TIME_RATIO_GATE: f64 = 1.15;

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

fn build_corpus_for(streams: usize, records_per_stream: usize, is_wide: bool) -> Vec<LogRecord> {
    if is_wide {
        build_wide_corpus(streams, records_per_stream)
    } else {
        common::build_corpus(streams, records_per_stream)
    }
}

fn row_count(bytes: &[u8]) -> u64 {
    let cfg = bench_config();
    let reader = RlogReader::new(bytes, &cfg).expect("RlogReader::new");
    let (rows, _stats) = reader.scan(&Predicate::And(Vec::new())).expect("scan");
    rows.len() as u64
}

/// Decodes FIELD_DIR from a finished object and returns its entry count (the
/// crate's own footer + field_dir API, per deliverable 5's preferred path).
fn field_dir_len(bytes: &[u8]) -> usize {
    let footer = footer::open(bytes).expect("open footer");
    let fd_desc = footer
        .section(footer::kind::FIELD_DIR)
        .expect("FIELD_DIR section present");
    let start = fd_desc.offset as usize;
    let end = start + fd_desc.len as usize;
    let raw = zstd::bulk::decompress(&bytes[start..end], fd_desc.uncomp_len as usize)
        .expect("decompress FIELD_DIR");
    let fd = FieldDir::decode(&raw, 10_000).expect("decode FIELD_DIR");
    fd.len()
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

struct Correctness {
    shape: &'static str,
    object_len: usize,
    hash_r: String,
    hash_c: String,
    rows_r: u64,
    rows_c: u64,
    fd_len_r: usize,
    fd_len_c: usize,
    dyn_used_r: u32,
    dyn_used_c: u32,
    dyn_overflow_r: u32,
    dyn_overflow_c: u32,
}

fn check_correctness(shape: &'static str, corpus: &[LogRecord]) -> Correctness {
    if corpus.len() != RECORDS_PER_OBJECT {
        fail(format!("{shape}: corpus size {} != {RECORDS_PER_OBJECT}", corpus.len()));
    }
    let (bytes_r, stats_r) = run_arm_r_full(corpus);
    let (bytes_c, stats_c) = run_arm_c_full(corpus);

    let hash_r = blake3::hash(&bytes_r).to_hex().to_string();
    let hash_c = blake3::hash(&bytes_c).to_hex().to_string();
    if bytes_r != bytes_c {
        let first_diff = bytes_r
            .iter()
            .zip(bytes_c.iter())
            .position(|(a, b)| a != b)
            .unwrap_or_else(|| bytes_r.len().min(bytes_c.len()));
        fail(format!(
            "{shape}: arm R and arm C objects differ (ADR-0109 violation), \
             first at offset {first_diff}, len R={} len C={}",
            bytes_r.len(),
            bytes_c.len()
        ));
    }

    let rows_r = row_count(&bytes_r);
    let rows_c = row_count(&bytes_c);
    if rows_r != RECORDS_PER_OBJECT as u64 {
        fail(format!("{shape}: arm R row_count {rows_r} != {RECORDS_PER_OBJECT}"));
    }
    if rows_c != RECORDS_PER_OBJECT as u64 {
        fail(format!("{shape}: arm C row_count {rows_c} != {RECORDS_PER_OBJECT}"));
    }

    let fd_len_r = field_dir_len(&bytes_r);
    let fd_len_c = field_dir_len(&bytes_c);
    if fd_len_r != fd_len_c {
        fail(format!(
            "{shape}: FIELD_DIR length differs between byte-identical objects: R={fd_len_r} C={fd_len_c}"
        ));
    }

    Correctness {
        shape,
        object_len: bytes_r.len(),
        hash_r,
        hash_c,
        rows_r,
        rows_c,
        fd_len_r,
        fd_len_c,
        dyn_used_r: stats_r.dynamic_columns_used,
        dyn_used_c: stats_c.dynamic_columns_used,
        dyn_overflow_r: stats_r.dynamic_columns_overflowed,
        dyn_overflow_c: stats_c.dynamic_columns_overflowed,
    }
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

struct ShapeTiming {
    name: &'static str,
    uptime_before: String,
    uptime_after: String,
    r_run_means_ns: Vec<f64>,
    c_run_means_ns: Vec<f64>,
    c_fr_run_means_ns: Vec<f64>,
}

fn measure_shape_time(name: &'static str, corpus: &[LogRecord]) -> ShapeTiming {
    let uptime_before = uptime();

    for _ in 0..TIME_WARMUP_RUNS {
        for _ in 0..TIME_ITERS_PER_RUN {
            std::hint::black_box(time_arm_r_once(corpus));
            let c = time_arm_c_once(corpus);
            std::hint::black_box(c.total);
        }
    }

    let mut r_run_means_ns = Vec::with_capacity(TIME_RUNS);
    let mut c_run_means_ns = Vec::with_capacity(TIME_RUNS);
    let mut c_fr_run_means_ns = Vec::with_capacity(TIME_RUNS);
    for _ in 0..TIME_RUNS {
        let mut r_sum = Duration::ZERO;
        let mut c_sum = Duration::ZERO;
        let mut fr_sum = Duration::ZERO;
        for _ in 0..TIME_ITERS_PER_RUN {
            r_sum += time_arm_r_once(corpus);
            let c = time_arm_c_once(corpus);
            c_sum += c.total;
            fr_sum += c.from_records;
        }
        r_run_means_ns.push(r_sum.as_nanos() as f64 / TIME_ITERS_PER_RUN as f64);
        c_run_means_ns.push(c_sum.as_nanos() as f64 / TIME_ITERS_PER_RUN as f64);
        c_fr_run_means_ns.push(fr_sum.as_nanos() as f64 / TIME_ITERS_PER_RUN as f64);
    }

    let uptime_after = uptime();
    ShapeTiming {
        name,
        uptime_before,
        uptime_after,
        r_run_means_ns,
        c_run_means_ns,
        c_fr_run_means_ns,
    }
}

fn band_status(v: f64, lo: f64, hi: f64) -> &'static str {
    if v < lo {
        "below"
    } else if v > hi {
        "above"
    } else {
        "inside"
    }
}

fn main() {
    let host = uname_a();
    println!("HOST {host}");
    let cfg = bench_config();
    println!(
        "RLOG_CONFIG max_dynamic_columns={} block_target_records={} block_max_bytes={}",
        cfg.max_dynamic_columns, cfg.block_target_records, cfg.block_max_bytes
    );
    if (cfg.max_dynamic_columns as usize) < WIDE_ATTR_COUNT {
        println!(
            "WIDE_CORPUS_OVERFLOW_EXPECTED cap={} wide_attrs={WIDE_ATTR_COUNT}",
            cfg.max_dynamic_columns
        );
    } else {
        println!(
            "WIDE_CORPUS_NO_OVERFLOW_EXPECTED cap={} wide_attrs={WIDE_ATTR_COUNT}",
            cfg.max_dynamic_columns
        );
    }

    let selected: Option<String> = std::env::args().nth(1);
    if let Some(sel) = &selected
        && !SHAPES.iter().any(|s| s.0 == sel)
    {
        eprintln!("unknown shape {sel:?}; expected one of {:?}", SHAPES.map(|s| s.0));
        std::process::exit(2);
    }

    let mut corrects = Vec::new();
    let mut corpora: Vec<(&'static str, Vec<LogRecord>)> = Vec::new();
    for &(name, streams, records_per_stream, is_wide) in &SHAPES {
        if selected.as_deref().is_some_and(|sel| sel != name) {
            continue;
        }
        let corpus = build_corpus_for(streams, records_per_stream, is_wide);
        let c = check_correctness(name, &corpus);
        println!(
            "CORRECTNESS shape={} object_len={} hash_r={} hash_c={} hash_match={} rows_r={} rows_c={} \
             field_dir_len_r={} field_dir_len_c={} dyn_used_r={} dyn_used_c={} dyn_overflow_r={} dyn_overflow_c={}",
            c.shape,
            c.object_len,
            c.hash_r,
            c.hash_c,
            c.hash_r == c.hash_c,
            c.rows_r,
            c.rows_c,
            c.fd_len_r,
            c.fd_len_c,
            c.dyn_used_r,
            c.dyn_used_c,
            c.dyn_overflow_r,
            c.dyn_overflow_c
        );
        corrects.push(c);
        corpora.push((name, corpus));
    }

    let mut timings = Vec::new();
    for (name, corpus) in &corpora {
        let t = measure_shape_time(name, corpus);
        println!("UPTIME_BEFORE shape={} {}", t.name, t.uptime_before);
        println!("UPTIME_AFTER shape={} {}", t.name, t.uptime_after);
        timings.push(t);
    }

    println!("\n=== TIME SUMMARY ===");
    for t in &timings {
        let r_median = median_f64(&t.r_run_means_ns);
        let r_min = min_f64(&t.r_run_means_ns);
        let r_max = max_f64(&t.r_run_means_ns);
        let c_median = median_f64(&t.c_run_means_ns);
        let c_min = min_f64(&t.c_run_means_ns);
        let c_max = max_f64(&t.c_run_means_ns);
        let fr_median = median_f64(&t.c_fr_run_means_ns);
        let fr_min = min_f64(&t.c_fr_run_means_ns);
        let fr_max = max_f64(&t.c_fr_run_means_ns);

        let ratios: Vec<f64> = t
            .r_run_means_ns
            .iter()
            .zip(t.c_run_means_ns.iter())
            .map(|(r, c)| c / r)
            .collect();
        let ratio_median = median_f64(&ratios);
        let ratio_min = min_f64(&ratios);
        let ratio_max = max_f64(&ratios);

        let fr_share: Vec<f64> = t
            .c_fr_run_means_ns
            .iter()
            .zip(t.c_run_means_ns.iter())
            .map(|(fr, c)| fr / c)
            .collect();
        let fr_share_median = median_f64(&fr_share);
        let fr_share_min = min_f64(&fr_share);
        let fr_share_max = max_f64(&fr_share);

        println!(
            "SHAPE {} R_median_ns={r_median:.0} R_min_ns={r_min:.0} R_max_ns={r_max:.0} \
             C_median_ns={c_median:.0} C_min_ns={c_min:.0} C_max_ns={c_max:.0} \
             FR_median_ns={fr_median:.0} FR_min_ns={fr_min:.0} FR_max_ns={fr_max:.0} \
             ratio_median={ratio_median:.4} ratio_min={ratio_min:.4} ratio_max={ratio_max:.4} \
             fr_share_median={fr_share_median:.4} fr_share_min={fr_share_min:.4} fr_share_max={fr_share_max:.4}",
            t.name
        );
        println!(
            "SHAPE {} ratios_per_run={}",
            t.name,
            ratios.iter().map(|r| format!("{r:.4}")).collect::<Vec<_>>().join(",")
        );

        if t.name.starts_with("wide_") {
            println!(
                "GATE shape={} ratio_median={ratio_median:.4} threshold={TIME_RATIO_GATE} verdict={}",
                t.name,
                band_status(ratio_median, 0.0, TIME_RATIO_GATE)
            );
        }
    }

    for c in &corrects {
        if c.hash_r != c.hash_c {
            fail(format!("{}: hash mismatch survived to report (should be unreachable)", c.shape));
        }
    }

    println!("\nDONE");
}
