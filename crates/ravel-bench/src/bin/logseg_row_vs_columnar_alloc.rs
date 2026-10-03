//! Stage 0c memory and wall-time measurement for issue #2475 (epic #2467):
//! for the same records, how do peak live memory and encode wall time
//! compare between the row path (`RlogWriter::push` per record, then
//! `finish`: `build_object`) and the columnar path
//! (`ColumnarLogBatch::from_records` over the records, then
//! `push_columnar` of that one batch, then `finish`: `build_object_columnar`,
//! ADR-0109). Report-only; never wired into `cargo bench`. Uses
//! `stats_alloc`'s instrumented global allocator exactly as
//! `logseg_resolved_row_alloc.rs` does; `stats_alloc`'s `unsafe impl
//! GlobalAlloc` lives entirely inside that crate, no `unsafe` appears in this
//! file.
//!
//! Run directly:
//!   cargo run -p ravel-bench --release --bin logseg_row_vs_columnar_alloc
#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "../../../ravel-logseg/benches/common/mod.rs"]
mod common;

use std::alloc::System;
use std::cell::RefCell;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::time::{Duration, Instant};

use common::{bench_config, bench_identity, build_corpus};
use ravel_logseg::columnar_batch::ColumnarLogBatch;
use ravel_logseg::writer::stage0;
use ravel_logseg::{LogRecord, Predicate, RlogReader, RlogWriter};
use stats_alloc::{INSTRUMENTED_SYSTEM, Stats, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

const RECORDS_PER_OBJECT: usize = 20_000;

/// (name, stream_count, records_per_stream). Each shape has
/// `stream_count * records_per_stream == RECORDS_PER_OBJECT`, matching
/// `logseg_resolved_row_alloc.rs`'s shapes so the two reports are comparable.
const SHAPES: [(&str, usize, usize); 3] = [
    ("1_stream", 1, 20_000),
    ("1000_streams", 1_000, 20),
    ("20000_streams", 20_000, 1),
];

const MEM_WARMUP: usize = 1;
const MEM_RUNS: usize = 5;

const TIME_WARMUP_RUNS: usize = 3;
const TIME_RUNS: usize = 5;
const TIME_ITERS_PER_RUN: usize = 10;

/// The 7 labels `build_object`/`build_object_columnar` both fire through the
/// shared `stage0::HOOK`, in the order each path's structure reaches them.
const HOOK_LABELS: [&str; 7] = [
    "before_index",
    "after_index",
    "after_columns",
    "after_resolve_rows",
    "after_blocks",
    "after_sections",
    "before_return",
];

thread_local! {
    static SAMPLES: RefCell<Vec<(&'static str, Stats)>> = RefCell::new(Vec::with_capacity(8));
}

/// Gates whether the installed hook actually records anything. Off during the
/// wall-time loop so that phase pays no sampling cost beyond the
/// `OnceLock::get` + one relaxed load `stage0::fire` already costs when no
/// hook is installed at all; `stage0::HOOK` is a `OnceLock` and so, once set,
/// cannot be literally uninstalled for a later phase in the same process. This
/// flag is the mechanism that makes "hook unset" true in effect: with it
/// false, the hook closure returns immediately and records no sample, which is
/// the only thing the memory phase's hook does beyond that.
static RECORD_SAMPLES: AtomicBool = AtomicBool::new(false);

fn stage0_hook(label: &'static str) {
    if !RECORD_SAMPLES.load(Relaxed) {
        return;
    }
    let stats = GLOBAL.stats();
    SAMPLES.with(|s| s.borrow_mut().push((label, stats)));
}

/// Net live bytes implied by the allocator's running totals, same definition
/// `logseg_resolved_row_alloc.rs` uses.
fn live_bytes(s: &Stats) -> i64 {
    s.bytes_allocated as i64 - s.bytes_deallocated as i64 + s.bytes_reallocated as i64
}

fn fail(msg: String) -> ! {
    eprintln!("ASSERTION FAILED: {msg}");
    std::process::exit(1);
}

fn median_f64(values: &[f64]) -> f64 {
    let mut v = values.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).expect("non-NaN duration ratio"));
    v[v.len() / 2]
}

/// One labelled sample's live bytes, relative to the arm's baseline.
#[derive(Clone, Copy)]
struct Sample {
    label: &'static str,
    bytes: i64,
}

struct ArmResult {
    /// Every sample this run recorded, in encode order: manual points first
    /// (pushed / batch_built / records_dropped), then the 7 hook labels.
    samples: Vec<Sample>,
    total_allocated: i64,
}

fn read_hook_samples(baseline: i64) -> Vec<Sample> {
    SAMPLES.with(|s| {
        let recorded = s.borrow();
        let mut seen: HashSet<&'static str> = HashSet::new();
        for (label, _) in recorded.iter() {
            if !seen.insert(label) {
                fail(format!("label {label} fired more than once in one encode"));
            }
        }
        for label in HOOK_LABELS {
            if !seen.contains(label) {
                fail(format!("label {label} never fired"));
            }
        }
        recorded
            .iter()
            .map(|(label, stats)| Sample {
                label,
                bytes: live_bytes(stats) - baseline,
            })
            .collect()
    })
}

fn run_arm_r(corpus: &[LogRecord]) -> ArmResult {
    SAMPLES.with(|s| s.borrow_mut().clear());
    RECORD_SAMPLES.store(true, Relaxed);
    let baseline_stats = GLOBAL.stats();
    let baseline = live_bytes(&baseline_stats);
    let baseline_alloc = baseline_stats.bytes_allocated;

    let input = corpus.to_vec();
    let mut w = RlogWriter::new(bench_config(), bench_identity());
    for r in input {
        w.push(r).expect("push");
    }
    let pushed = live_bytes(&GLOBAL.stats()) - baseline;
    let (bytes, _stats) = w.finish_with_stats().expect("finish");
    RECORD_SAMPLES.store(false, Relaxed);
    let final_stats = GLOBAL.stats();

    if object_record_count(&bytes) != RECORDS_PER_OBJECT as u64 {
        fail(format!("arm R: object record_count != {RECORDS_PER_OBJECT}"));
    }

    let mut samples = vec![Sample {
        label: "pushed",
        bytes: pushed,
    }];
    samples.extend(read_hook_samples(baseline));

    ArmResult {
        samples,
        total_allocated: (final_stats.bytes_allocated - baseline_alloc) as i64,
    }
}

/// Decodes the object and counts its rows with an always-true predicate, the
/// cheapest way to assert row count via `ravel-logseg`'s own public read path
/// rather than hand-parsing the footer.
fn object_record_count(bytes: &[u8]) -> u64 {
    let cfg = bench_config();
    let reader = RlogReader::new(bytes, &cfg).expect("RlogReader::new");
    let (rows, _stats) = reader.scan(&Predicate::And(Vec::new())).expect("scan");
    rows.len() as u64
}

fn run_arm_c(corpus: &[LogRecord], keep_records: bool) -> ArmResult {
    SAMPLES.with(|s| s.borrow_mut().clear());
    RECORD_SAMPLES.store(true, Relaxed);
    let baseline_stats = GLOBAL.stats();
    let baseline = live_bytes(&baseline_stats);
    let baseline_alloc = baseline_stats.bytes_allocated;

    let source = corpus.to_vec();
    let batch = ColumnarLogBatch::from_records(&source);
    let batch_built = live_bytes(&GLOBAL.stats()) - baseline;
    let mut manual_samples = vec![Sample {
        label: "batch_built",
        bytes: batch_built,
    }];

    let source = if keep_records {
        Some(source)
    } else {
        drop(source);
        let records_dropped = live_bytes(&GLOBAL.stats()) - baseline;
        manual_samples.push(Sample {
            label: "records_dropped",
            bytes: records_dropped,
        });
        None
    };

    let mut w = RlogWriter::new(bench_config(), bench_identity());
    w.push_columnar(batch).expect("push_columnar");
    let (bytes, _stats) = w.finish_with_stats().expect("finish");
    RECORD_SAMPLES.store(false, Relaxed);
    let final_stats = GLOBAL.stats();

    if object_record_count(&bytes) != RECORDS_PER_OBJECT as u64 {
        fail(format!(
            "arm C (keep_records={keep_records}): object record_count != {RECORDS_PER_OBJECT}"
        ));
    }

    drop(source);
    manual_samples.extend(read_hook_samples(baseline));

    ArmResult {
        samples: manual_samples,
        total_allocated: (final_stats.bytes_allocated - baseline_alloc) as i64,
    }
}

struct ShapeMemReport {
    name: &'static str,
    arm_r: Vec<ArmResult>,
    arm_c_kept: Vec<ArmResult>,
    arm_c_dropped: Vec<ArmResult>,
    /// Encoded bytes from one arm-R run, length reported once byte-identity
    /// with arm C has been asserted below.
    bytes_r: Vec<u8>,
}

fn measure_shape_memory(name: &'static str, streams: usize, records_per_stream: usize) -> ShapeMemReport {
    let corpus = build_corpus(streams, records_per_stream);
    assert_eq!(corpus.len(), RECORDS_PER_OBJECT, "{name}: corpus size");

    let mut arm_r = Vec::with_capacity(MEM_WARMUP + MEM_RUNS);
    let mut bytes_r = Vec::new();
    for i in 0..MEM_WARMUP + MEM_RUNS {
        let input = corpus.to_vec();
        let mut w = RlogWriter::new(bench_config(), bench_identity());
        for r in input {
            w.push(r).expect("push");
        }
        let (bytes, _) = w.finish_with_stats().expect("finish");
        if i == MEM_WARMUP + MEM_RUNS - 1 {
            bytes_r = bytes;
        }
    }
    for _ in 0..MEM_WARMUP + MEM_RUNS {
        arm_r.push(run_arm_r(&corpus));
    }
    arm_r.drain(0..MEM_WARMUP);

    let mut arm_c_kept = Vec::with_capacity(MEM_WARMUP + MEM_RUNS);
    for _ in 0..MEM_WARMUP + MEM_RUNS {
        arm_c_kept.push(run_arm_c(&corpus, true));
    }
    arm_c_kept.drain(0..MEM_WARMUP);
    let bytes_c = {
        let source = corpus.to_vec();
        let batch = ColumnarLogBatch::from_records(&source);
        let mut w = RlogWriter::new(bench_config(), bench_identity());
        w.push_columnar(batch).expect("push_columnar");
        let (bytes, _) = w.finish_with_stats().expect("finish");
        drop(source);
        bytes
    };

    let mut arm_c_dropped = Vec::with_capacity(MEM_WARMUP + MEM_RUNS);
    for _ in 0..MEM_WARMUP + MEM_RUNS {
        arm_c_dropped.push(run_arm_c(&corpus, false));
    }
    arm_c_dropped.drain(0..MEM_WARMUP);

    if bytes_r != bytes_c {
        let first_diff = bytes_r
            .iter()
            .zip(bytes_c.iter())
            .position(|(a, b)| a != b)
            .unwrap_or_else(|| bytes_r.len().min(bytes_c.len()));
        let mut md = String::new();
        md.push_str("# Stage 0c: row versus columnar byte-identity FAILURE (issue #2475)\n\n");
        md.push_str(&format!(
            "ADR-0109 requires the row and columnar paths to produce byte-identical \
             objects for the same records. They did not for shape `{name}`.\n\n"
        ));
        md.push_str(&format!("Shape: `{name}` ({streams} streams, {records_per_stream} records/stream)\n\n"));
        md.push_str(&format!("Arm R (row path) object length: {} bytes\n\n", bytes_r.len()));
        md.push_str(&format!("Arm C (columnar path) object length: {} bytes\n\n", bytes_c.len()));
        md.push_str(&format!("First differing offset: {first_diff}\n\n"));
        std::fs::write(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../stage0c-row-vs-columnar.md"
            ),
            &md,
        )
        .expect("write failure report");
        eprintln!("{md}");
        fail(format!(
            "{name}: row and columnar objects differ (ADR-0109 violation), first at offset {first_diff}"
        ));
    }

    ShapeMemReport {
        name,
        arm_r,
        arm_c_kept,
        arm_c_dropped,
        bytes_r,
    }
}

/// Per-label min/max live bytes across a set of runs, plus the overall peak
/// (the largest single sample across every label in every run) and its label,
/// and min/max TOTAL_ALLOCATED.
struct ArmSummary {
    label_minmax: Vec<(&'static str, i64, i64)>,
    peak: i64,
    peak_label: &'static str,
    total_allocated_min: i64,
    total_allocated_max: i64,
}

fn summarize_arm(runs: &[ArmResult]) -> ArmSummary {
    let mut labels: Vec<&'static str> = Vec::new();
    for r in runs {
        for s in &r.samples {
            if !labels.contains(&s.label) {
                labels.push(s.label);
            }
        }
    }
    let mut label_minmax = Vec::new();
    let mut peak = i64::MIN;
    let mut peak_label = "";
    for label in &labels {
        let mut vals: Vec<i64> = runs
            .iter()
            .flat_map(|r| r.samples.iter())
            .filter(|s| s.label == *label)
            .map(|s| s.bytes)
            .collect();
        vals.sort_unstable();
        let lo = *vals.first().expect("at least one sample");
        let hi = *vals.last().expect("at least one sample");
        if hi > peak {
            peak = hi;
            peak_label = label;
        }
        label_minmax.push((*label, lo, hi));
    }
    let total_allocated_min = runs.iter().map(|r| r.total_allocated).min().expect("runs");
    let total_allocated_max = runs.iter().map(|r| r.total_allocated).max().expect("runs");
    ArmSummary {
        label_minmax,
        peak,
        peak_label,
        total_allocated_min,
        total_allocated_max,
    }
}

struct ShapeTimeReport {
    name: &'static str,
    /// 5 run-means, nanoseconds, in run order.
    r_run_means_ns: Vec<f64>,
    c_run_means_ns: Vec<f64>,
}

fn time_arm_r_once(corpus: &[LogRecord]) -> Duration {
    let input = corpus.to_vec();
    let start = Instant::now();
    let mut w = RlogWriter::new(bench_config(), bench_identity());
    for r in input {
        w.push(r).expect("push");
    }
    let (bytes, _) = w.finish_with_stats().expect("finish");
    let elapsed = start.elapsed();
    std::hint::black_box(&bytes);
    elapsed
}

fn time_arm_c_once(corpus: &[LogRecord]) -> Duration {
    let source = corpus.to_vec();
    let start = Instant::now();
    let batch = ColumnarLogBatch::from_records(&source);
    let mut w = RlogWriter::new(bench_config(), bench_identity());
    w.push_columnar(batch).expect("push_columnar");
    let (bytes, _) = w.finish_with_stats().expect("finish");
    let elapsed = start.elapsed();
    std::hint::black_box(&bytes);
    std::hint::black_box(&source);
    elapsed
}

fn measure_all_shapes_time(corpora: &[(&'static str, Vec<LogRecord>)]) -> Vec<ShapeTimeReport> {
    RECORD_SAMPLES.store(false, Relaxed);

    // Warm-up runs: same interleaved shape, discarded.
    for _ in 0..TIME_WARMUP_RUNS {
        for _ in 0..TIME_ITERS_PER_RUN {
            for (_, corpus) in corpora {
                std::hint::black_box(time_arm_r_once(corpus));
                std::hint::black_box(time_arm_c_once(corpus));
            }
        }
    }

    let mut r_totals: Vec<Vec<Duration>> = vec![Vec::with_capacity(TIME_RUNS); corpora.len()];
    let mut c_totals: Vec<Vec<Duration>> = vec![Vec::with_capacity(TIME_RUNS); corpora.len()];

    for _ in 0..TIME_RUNS {
        let mut r_sum: Vec<Duration> = vec![Duration::ZERO; corpora.len()];
        let mut c_sum: Vec<Duration> = vec![Duration::ZERO; corpora.len()];
        for _ in 0..TIME_ITERS_PER_RUN {
            for (idx, (_, corpus)) in corpora.iter().enumerate() {
                r_sum[idx] += time_arm_r_once(corpus);
                c_sum[idx] += time_arm_c_once(corpus);
            }
        }
        for idx in 0..corpora.len() {
            r_totals[idx].push(r_sum[idx]);
            c_totals[idx].push(c_sum[idx]);
        }
    }

    corpora
        .iter()
        .enumerate()
        .map(|(idx, (name, _))| ShapeTimeReport {
            name,
            r_run_means_ns: r_totals[idx]
                .iter()
                .map(|d| d.as_nanos() as f64 / TIME_ITERS_PER_RUN as f64)
                .collect(),
            c_run_means_ns: c_totals[idx]
                .iter()
                .map(|d| d.as_nanos() as f64 / TIME_ITERS_PER_RUN as f64)
                .collect(),
        })
        .collect()
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

fn band_status(v: f64, lo: f64, hi: f64) -> &'static str {
    if v < lo {
        "below band"
    } else if v > hi {
        "above band"
    } else {
        "inside"
    }
}

fn main() {
    stage0::HOOK
        .set(stage0_hook)
        .unwrap_or_else(|_| fail("stage0 hook already set".to_string()));

    let host = uname_a();
    let uptime_before = uptime();

    let mut mem_reports = Vec::new();
    for &(name, streams, records_per_stream) in &SHAPES {
        mem_reports.push(measure_shape_memory(name, streams, records_per_stream));
    }

    let corpora: Vec<(&'static str, Vec<LogRecord>)> = SHAPES
        .iter()
        .map(|&(name, streams, records_per_stream)| (name, build_corpus(streams, records_per_stream)))
        .collect();
    let time_reports = measure_all_shapes_time(&corpora);

    let uptime_after = uptime();

    // Pre-registered bands (epic #2467), not tuned to.
    struct PeakBand {
        shape: &'static str,
        dropped_lo_pct: f64,
        dropped_hi_pct: f64,
    }
    let peak_bands: [PeakBand; 3] = [
        PeakBand {
            shape: "1_stream",
            dropped_lo_pct: 25.0,
            dropped_hi_pct: 45.0,
        },
        PeakBand {
            shape: "1000_streams",
            dropped_lo_pct: 25.0,
            dropped_hi_pct: 45.0,
        },
        PeakBand {
            shape: "20000_streams",
            dropped_lo_pct: 10.0,
            dropped_hi_pct: 30.0,
        },
    ];
    const KEPT_BAND_PCT: f64 = 10.0;
    const TIME_RATIO_LO: f64 = 0.85;
    const TIME_RATIO_HI: f64 = 1.15;

    let mut md = String::new();
    md.push_str("# Stage 0c: row versus columnar memory and wall-time measurement (issue #2475)\n\n");
    md.push_str(&format!("Host: `{host}`\n\n"));
    md.push_str(&format!("Uptime before: `{uptime_before}`\n\n"));
    md.push_str(&format!("Uptime after: `{uptime_after}`\n\n"));
    md.push_str(
        "Command: `cargo run --release -p ravel-bench --bin logseg_row_vs_columnar_alloc`\n\n",
    );
    md.push_str(
        "Method: per shape, three memory arms (arm R: `RlogWriter::push` per record then \
         `finish`; arm C-kept: `ColumnarLogBatch::from_records` then `push_columnar` then \
         `finish`, with the cloned source records kept alive until `finish` returns; arm \
         C-dropped: the same, but the cloned source records are dropped immediately after \
         `from_records` returns and before `push_columnar`), each sampled with `stats_alloc` \
         (1 warm-up, 5 iterations, min/max reported), baseline taken immediately before the \
         arm's first allocation (the corpus clone). `build_object` and `build_object_columnar` \
         both fire the shared `stage0::HOOK` at 7 matching labels; arm R's samples come from the \
         row path's existing labels, arm C's from the new labels added to \
         `build_object_columnar` by this task. A separate wall-time loop (hook recording turned \
         off via an internal flag, since `stage0::HOOK`'s `OnceLock` cannot be literally \
         uninstalled once set for the memory phase above) runs 3 warm-up runs then 5 measured \
         runs of 10 iterations per arm per shape, arms and shapes interleaved inside each run, \
         cloning the corpus outside the timed region.\n\n",
    );
    md.push_str(
        "Note: `stats_alloc`'s counters are running totals (`bytes_allocated`, \
         `bytes_deallocated`, `bytes_reallocated`); `StatsAlloc` itself reports no high-water \
         mark. Each PEAK figure below is therefore the largest of a fixed, finite set of sampled \
         points (manual samples plus the 7 hook labels), not the arm's true peak live-byte \
         figure -- an allocation spike between two sampled points is invisible to this method. \
         Every PEAK reported here is a LOWER bound on that arm's real peak. This applies to \
         every arm identically, so a ratio of two lower bounds (`C-dropped PEAK / R PEAK`, \
         `C-kept PEAK / R PEAK`) is an estimate with no guaranteed direction: if the true peaks \
         differ from the sampled lower bounds by different amounts, the ratio of the lower \
         bounds can over- or understate the ratio of the true peaks.\n\n",
    );

    for mem in &mem_reports {
        let r_summary = summarize_arm(&mem.arm_r);
        let c_kept_summary = summarize_arm(&mem.arm_c_kept);
        let c_dropped_summary = summarize_arm(&mem.arm_c_dropped);

        md.push_str(&format!("## {}\n\n", mem.name));
        md.push_str(&format!(
            "Encoded object length: {} bytes (row and columnar byte-identical, asserted)\n\n",
            mem.bytes_r.len()
        ));

        for (arm_name, summary) in [
            ("Arm R (row path)", &r_summary),
            ("Arm C-kept (columnar, records kept)", &c_kept_summary),
            ("Arm C-dropped (columnar, records dropped)", &c_dropped_summary),
        ] {
            md.push_str(&format!("### {arm_name}\n\n"));
            md.push_str("| label | min live bytes | max live bytes |\n|---|---|---|\n");
            for (label, lo, hi) in &summary.label_minmax {
                md.push_str(&format!("| {label} | {lo} | {hi} |\n"));
            }
            md.push_str(&format!(
                "\nPEAK: {} bytes at label `{}`\n\n",
                summary.peak, summary.peak_label
            ));
            md.push_str(&format!(
                "TOTAL_ALLOCATED: {} to {} bytes (min to max over {} sampled runs)\n\n",
                summary.total_allocated_min, summary.total_allocated_max, MEM_RUNS
            ));
        }

        let dropped_ratio = c_dropped_summary.peak as f64 / r_summary.peak as f64;
        let kept_ratio = c_kept_summary.peak as f64 / r_summary.peak as f64;
        md.push_str(&format!(
            "PEAK ratio C-dropped/R: {dropped_ratio:.4} ({:.2}% of R)\n\n",
            dropped_ratio * 100.0
        ));
        md.push_str(&format!(
            "PEAK ratio C-kept/R: {kept_ratio:.4} ({:.2}% of R)\n\n",
            kept_ratio * 100.0
        ));

        if let Some(band) = peak_bands.iter().find(|b| b.shape == mem.name) {
            let reduction_pct = (1.0 - dropped_ratio) * 100.0;
            let status = band_status(reduction_pct, band.dropped_lo_pct, band.dropped_hi_pct);
            md.push_str(&format!(
                "C-dropped PEAK reduction vs R: {reduction_pct:.2}% (pre-registered band \
                 [{:.0}%, {:.0}%]): {status}\n\n",
                band.dropped_lo_pct, band.dropped_hi_pct
            ));
        }
        let kept_delta_pct = (kept_ratio - 1.0) * 100.0;
        let kept_status = band_status(kept_delta_pct.abs(), 0.0, KEPT_BAND_PCT);
        md.push_str(&format!(
            "C-kept PEAK delta vs R: {kept_delta_pct:+.2}% (pre-registered band [-{KEPT_BAND_PCT:.0}%, \
             +{KEPT_BAND_PCT:.0}%]): {kept_status}\n\n"
        ));
    }

    md.push_str("## Encode wall time\n\n");
    for t in &time_reports {
        let r_median = median_f64(&t.r_run_means_ns);
        let r_min = t.r_run_means_ns.iter().cloned().fold(f64::MAX, f64::min);
        let r_max = t.r_run_means_ns.iter().cloned().fold(f64::MIN, f64::max);
        let c_median = median_f64(&t.c_run_means_ns);
        let c_min = t.c_run_means_ns.iter().cloned().fold(f64::MAX, f64::min);
        let c_max = t.c_run_means_ns.iter().cloned().fold(f64::MIN, f64::max);
        let ratios: Vec<f64> = t
            .r_run_means_ns
            .iter()
            .zip(t.c_run_means_ns.iter())
            .map(|(r, c)| c / r)
            .collect();
        let ratio_median = median_f64(&ratios);
        let ratio_min = ratios.iter().cloned().fold(f64::MAX, f64::min);
        let ratio_max = ratios.iter().cloned().fold(f64::MIN, f64::max);

        md.push_str(&format!("### {}\n\n", t.name));
        md.push_str("| arm | median run-mean (ns) | min | max |\n|---|---|---|---|\n");
        md.push_str(&format!("| R | {r_median:.0} | {r_min:.0} | {r_max:.0} |\n"));
        md.push_str(&format!("| C | {c_median:.0} | {c_min:.0} | {c_max:.0} |\n"));
        md.push_str(&format!(
            "\nRatio C/R per run: {}\n\n",
            ratios
                .iter()
                .map(|r| format!("{r:.4}"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
        md.push_str(&format!(
            "Ratio C/R median {ratio_median:.4}, min {ratio_min:.4}, max {ratio_max:.4} \
             (pre-registered band [{TIME_RATIO_LO}, {TIME_RATIO_HI}]): {}\n\n",
            band_status(ratio_median, TIME_RATIO_LO, TIME_RATIO_HI)
        ));
    }

    println!("{md}");
    std::fs::write(
        concat!(env!("CARGO_MANIFEST_DIR"), "/../../stage0c-row-vs-columnar.md"),
        &md,
    )
    .expect("write report");
}
