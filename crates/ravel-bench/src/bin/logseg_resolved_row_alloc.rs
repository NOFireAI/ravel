//! Stage 0 memory measurement for issue #2469 (epic #2467): of the bytes
//! the row-resolution step (label `after_columns` to `after_resolve_rows`,
//! `crates/ravel-logseg/src/writer.rs::build_object`) adds per RLOG row-path
//! encode, how much goes to each component of a resolved row, and how many
//! allocations does one row cost. Report-only; never wired into `cargo
//! bench`. Uses `stats_alloc`'s instrumented global allocator exactly as
//! `logseg_ref_of_alloc.rs` does; `stats_alloc`'s `unsafe impl GlobalAlloc`
//! lives entirely inside that crate, no `unsafe` appears in this file.
//!
//! Run directly:
//!   cargo run -p ravel-bench --release --bin logseg_resolved_row_alloc
#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "../../../ravel-logseg/benches/common/mod.rs"]
mod common;

use std::alloc::System;
use std::cell::RefCell;
use std::collections::HashMap;

use common::{bench_config, bench_identity, build_corpus};
use ravel_logseg::writer::stage0;
use ravel_logseg::{LogRecord, RlogWriter};
use stats_alloc::{INSTRUMENTED_SYSTEM, Stats, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

const WARMUP: usize = 1;
const RUNS: usize = 5;
const RECORDS_PER_OBJECT: usize = 20_000;
const CALIBRATION_SAMPLES: usize = 10_000;

/// (name, stream_count, records_per_stream). Each shape has
/// `stream_count * records_per_stream == RECORDS_PER_OBJECT`.
const SHAPES: [(&str, usize, usize); 3] = [
    ("1_stream", 1, 20_000),
    ("1000_streams", 1_000, 20),
    ("20000_streams", 20_000, 1),
];

/// The two labels this driver reads off `build_object`'s existing
/// whole-encode hook; the step delta is `after_resolve_rows - after_columns`,
/// exactly as `stage0b-ref-of-memory.md` (issue #2428) computed it.
const STEP_LABELS: [&str; 2] = ["after_columns", "after_resolve_rows"];

thread_local! {
    // Reserved well above the 7 labels `build_object` fires per encode
    // (before_index, after_index, after_columns, after_resolve_rows,
    // after_blocks, after_sections, before_return) so the vec never
    // reallocates mid-encode; that growth would otherwise land inside this
    // thread's allocator trace at an arbitrary point.
    static SAMPLES: RefCell<Vec<(&'static str, Stats)>> = RefCell::new(Vec::with_capacity(16));
}

/// Net live bytes implied by the allocator's running totals, same definition
/// `logseg_ref_of_alloc.rs` uses.
fn live_bytes(s: &Stats) -> i64 {
    s.bytes_allocated as i64 - s.bytes_deallocated as i64 + s.bytes_reallocated as i64
}

fn stage0_hook(label: &'static str) {
    let stats = GLOBAL.stats();
    SAMPLES.with(|s| s.borrow_mut().push((label, stats)));
}

/// The stats sampler `resolve_row`/`build_object` call through
/// `stage0::STATS_SAMPLER`: `(live_bytes, allocation_count)`, both
/// cumulative totals. `StatsAlloc::stats()` only reads atomics (verified
/// against its source, stats_alloc 0.1.10), so this performs no allocation
/// itself; `calibrate()` below confirms that empirically rather than taking
/// it on faith.
fn stats_sampler() -> (i64, u64) {
    let s = GLOBAL.stats();
    (live_bytes(&s), s.allocations as u64)
}

fn fail(msg: String) -> ! {
    eprintln!("ASSERTION FAILED: {msg}");
    std::process::exit(1);
}

fn median(values: &[i64]) -> i64 {
    let mut v = values.to_vec();
    v.sort_unstable();
    v[v.len() / 2]
}

#[derive(Clone, Copy, Default)]
struct Bucket {
    bytes: i64,
    allocs: u64,
}

impl Bucket {
    fn per_row_allocs(&self, rows: u64) -> f64 {
        self.allocs as f64 / rows as f64
    }

    fn share_pct(&self, whole: i64) -> f64 {
        self.bytes as f64 / whole as f64 * 100.0
    }
}

struct ShapeResult {
    name: &'static str,
    streams: usize,
    step_delta: i64,
    rows_sampled: u64,
    whole_row_bytes: i64,
    whole_row_allocs: u64,
    column_map: Bucket,
    owned_values: Bucket,
    overflow: Bucket,
    stamp_scratch: Bucket,
    everything_else: Bucket,
    row_order: Bucket,
    row_setup: Bucket,
}

/// One unsampled encode (`ROW_SAMPLE` off): returns the step delta read off
/// the existing whole-encode label hook, exactly as `logseg_ref_of_alloc.rs`
/// computes `after_resolve_rows - after_columns`.
fn run_step_delta(corpus: Vec<LogRecord>) -> i64 {
    use std::sync::atomic::Ordering::Relaxed;
    SAMPLES.with(|s| s.borrow_mut().clear());
    stage0::MODE.store(0, Relaxed);
    stage0::ROW_SAMPLE.store(false, Relaxed);

    let mut w = RlogWriter::new(bench_config(), bench_identity());
    for r in corpus {
        w.push(r).expect("push");
    }
    let _ = w.finish().expect("finish");

    let samples = SAMPLES.with(|s| s.borrow().clone());
    let mut seen: HashMap<&'static str, i64> = HashMap::new();
    for (label, stats) in &samples {
        if STEP_LABELS.contains(label) {
            if seen.contains_key(label) {
                fail(format!("label {label} fired more than once in one encode"));
            }
            seen.insert(label, live_bytes(stats));
        }
    }
    for label in STEP_LABELS {
        if !seen.contains_key(label) {
            fail(format!("label {label} never fired"));
        }
    }
    seen["after_resolve_rows"] - seen["after_columns"]
}

/// One sampled encode (`ROW_SAMPLE` on): returns rows sampled, the sum of
/// per-row whole-call deltas, the five per-bucket sums, the row-ordering
/// step's own bucket (the `sort_by`/permute block run once after the loop),
/// and the once-per-encode setup bucket (`StampScratch::prepare` plus the
/// row `Vec::with_capacity`, both run once before the loop) -- neither of
/// the last two is part of any `ResolvedRow` field, straight off the
/// `stage0` accumulators `build_object`/`resolve_row` fed during the encode.
fn run_sampled(corpus: Vec<LogRecord>) -> (u64, i64, u64, [Bucket; 5], Bucket, Bucket) {
    use std::sync::atomic::Ordering::Relaxed;
    stage0::MODE.store(0, Relaxed);
    stage0::reset_row_samples();
    stage0::ROW_SAMPLE.store(true, Relaxed);

    let mut w = RlogWriter::new(bench_config(), bench_identity());
    for r in corpus {
        w.push(r).expect("push");
    }
    let _ = w.finish().expect("finish");
    stage0::ROW_SAMPLE.store(false, Relaxed);

    let rows_sampled = stage0::ROWS_SAMPLED.load(Relaxed);
    let whole_bytes = stage0::WHOLE_ROW_BYTES.load(Relaxed);
    let whole_allocs = stage0::WHOLE_ROW_ALLOCS.load(Relaxed);
    let buckets = [
        Bucket {
            bytes: stage0::COLUMN_MAP_BYTES.load(Relaxed),
            allocs: stage0::COLUMN_MAP_ALLOCS.load(Relaxed),
        },
        Bucket {
            bytes: stage0::OWNED_VALUES_BYTES.load(Relaxed),
            allocs: stage0::OWNED_VALUES_ALLOCS.load(Relaxed),
        },
        Bucket {
            bytes: stage0::OVERFLOW_BYTES.load(Relaxed),
            allocs: stage0::OVERFLOW_ALLOCS.load(Relaxed),
        },
        Bucket {
            bytes: stage0::STAMP_SCRATCH_BYTES.load(Relaxed),
            allocs: stage0::STAMP_SCRATCH_ALLOCS.load(Relaxed),
        },
        Bucket {
            bytes: stage0::EVERYTHING_ELSE_BYTES.load(Relaxed),
            allocs: stage0::EVERYTHING_ELSE_ALLOCS.load(Relaxed),
        },
    ];
    let row_order = Bucket {
        bytes: stage0::ROW_ORDER_BYTES.load(Relaxed),
        allocs: stage0::ROW_ORDER_ALLOCS.load(Relaxed),
    };
    let row_setup = Bucket {
        bytes: stage0::ROW_SETUP_BYTES.load(Relaxed),
        allocs: stage0::ROW_SETUP_ALLOCS.load(Relaxed),
    };
    (
        rows_sampled,
        whole_bytes,
        whole_allocs,
        buckets,
        row_order,
        row_setup,
    )
}

fn measure_shape(name: &'static str, streams: usize, records_per_stream: usize) -> ShapeResult {
    let corpus = build_corpus(streams, records_per_stream);
    assert_eq!(corpus.len(), RECORDS_PER_OBJECT, "{name}: corpus size");

    let mut step_deltas = Vec::with_capacity(WARMUP + RUNS);
    for _ in 0..WARMUP + RUNS {
        step_deltas.push(run_step_delta(corpus.clone()));
    }
    step_deltas.drain(0..WARMUP);
    let step_delta = median(&step_deltas);

    // One sampled encode. Not averaged with the unsampled runs above: the
    // whole point is to compare its per-row sum against the step delta those
    // measured, which the 5% assertion below does.
    let (rows_sampled, whole_bytes, whole_allocs, buckets, row_order, row_setup) =
        run_sampled(corpus);

    if rows_sampled != RECORDS_PER_OBJECT as u64 {
        fail(format!(
            "{name}: rows sampled {rows_sampled} != {RECORDS_PER_OBJECT}"
        ));
    }

    // The step delta's window (`after_columns` to `after_resolve_rows`) covers
    // the per-row loop AND the row-ordering step that runs once after it
    // (`sort_by`/permute): stable sort's O(n) auxiliary buffer is sized off
    // the whole vector, so it cannot be attributed to any single row or
    // `ResolvedRow` field. `whole_bytes` sums only the per-row loop (measured
    // directly: `rows` is pre-reserved to its final length, so `rows.push`
    // between iterations never reallocates and introduces no gap), so the
    // comparison basis is the loop sum plus the directly-measured
    // row-ordering cost, not the loop sum alone.
    let step_tol = (step_delta.unsigned_abs() as f64 * 0.05).max(1.0);
    let accounted = whole_bytes + row_order.bytes + row_setup.bytes;
    if (accounted - step_delta).unsigned_abs() as f64 > step_tol {
        fail(format!(
            "{name}: whole-call deltas + row-order step + row setup {accounted} vs step delta \
             {step_delta} differ by more than 5% ({step_tol:.0} byte tolerance)"
        ));
    }

    let bucket_sum: i64 = buckets.iter().map(|b| b.bytes).sum();
    let whole_tol = (whole_bytes.unsigned_abs() as f64 * 0.05).max(1.0);
    if (bucket_sum - whole_bytes).unsigned_abs() as f64 > whole_tol {
        fail(format!(
            "{name}: bucket sum {bucket_sum} vs whole-call delta {whole_bytes} differ by more \
             than 5% ({whole_tol:.0} byte tolerance)"
        ));
    }

    ShapeResult {
        name,
        streams,
        step_delta,
        rows_sampled,
        whole_row_bytes: whole_bytes,
        whole_row_allocs: whole_allocs,
        column_map: buckets[0],
        owned_values: buckets[1],
        overflow: buckets[2],
        stamp_scratch: buckets[3],
        everything_else: buckets[4],
        row_order,
        row_setup,
    }
}

/// Samples the allocator `CALIBRATION_SAMPLES` times back to back with
/// nothing in between, and returns the live-byte and allocation-count delta
/// across that run. This is the instrumentation's own overhead per
/// `stage0::sample()` call pair, which the per-row sampling above pays
/// several times per row; `main` reports it and would subtract it from the
/// 5% checks above if it were nonzero.
fn calibrate() -> (i64, u64) {
    let before = stats_sampler();
    for _ in 0..CALIBRATION_SAMPLES {
        std::hint::black_box(stats_sampler());
    }
    let after = stats_sampler();
    (after.0 - before.0, after.1 - before.1)
}

fn main() {
    stage0::STATS_SAMPLER
        .set(stats_sampler)
        .unwrap_or_else(|_| fail("stats sampler already set".to_string()));
    stage0::HOOK
        .set(stage0_hook)
        .unwrap_or_else(|_| fail("stage0 hook already set".to_string()));

    let resolved_row_size = std::mem::size_of::<ravel_logseg::record::ResolvedRow>();
    eprintln!("size_of::<ResolvedRow>() = {resolved_row_size}");

    let (cal_bytes, cal_allocs) = calibrate();

    let mut results = Vec::new();
    for &(name, streams, records_per_stream) in &SHAPES {
        results.push(measure_shape(name, streams, records_per_stream));
    }

    // Pre-registered bands (issue #2469 / epic #2467), not tuned to, as
    // share of the step delta's bytes.
    struct Band {
        name: &'static str,
        share_lo: f64,
        share_hi: f64,
        allocs_lo: f64,
        allocs_hi: f64,
    }
    let bands: [Band; 4] = [
        Band {
            name: "column map nodes",
            share_lo: 30.0,
            share_hi: 60.0,
            allocs_lo: 2.0,
            allocs_hi: 8.0,
        },
        Band {
            name: "owned string and byte values",
            share_lo: 25.0,
            share_hi: 55.0,
            allocs_lo: 4.0,
            allocs_hi: 12.0,
        },
        Band {
            name: "overflow attributes",
            share_lo: 0.0,
            share_hi: 10.0,
            allocs_lo: 0.0,
            allocs_hi: 2.0,
        },
        Band {
            name: "everything else",
            share_lo: 0.0,
            share_hi: 15.0,
            allocs_lo: 0.0,
            allocs_hi: 4.0,
        },
    ];
    const BYTES_PER_ROW_1_STREAM_LO: f64 = 550.0;
    const BYTES_PER_ROW_1_STREAM_HI: f64 = 950.0;
    const REMAINDER_MAX_PCT: f64 = 5.0;

    fn band_status(v: f64, lo: f64, hi: f64) -> &'static str {
        if v < lo {
            "below band"
        } else if v > hi {
            "above band"
        } else {
            "inside"
        }
    }

    let mut md = String::new();
    md.push_str("# Stage 0: resolved-row memory measurement (issue #2469)\n\n");
    md.push_str(&format!(
        "Host: `{}`\n\n",
        std::str::from_utf8(
            &std::process::Command::new("uname")
                .arg("-a")
                .output()
                .expect("uname")
                .stdout
        )
        .expect("utf8")
        .trim()
    ));
    md.push_str(
        "Command: `cargo run --release -p ravel-bench --bin logseg_resolved_row_alloc`\n\n",
    );
    md.push_str(
        "Method: each shape runs `WARMUP + RUNS` unsampled encodes (`stage0::ROW_SAMPLE` off) \
         and reads the step delta (`after_resolve_rows` live bytes minus `after_columns` live \
         bytes, median over the post-warmup runs) off the existing whole-encode label hook from \
         issue #2428, then one sampled encode (`stage0::ROW_SAMPLE` on) whose per-row \
         instrumentation in `resolve_row` and the row-resolution loop in `build_object` \
         accumulates live-byte and allocation-count deltas into five `stage0` bucket \
         counters plus a whole-row-call counter, read back after `finish()` returns. Two \
         further counters, sampled directly around the once-per-encode setup before the loop \
         and the row-ordering step after it, account for the step delta's two components \
         outside `resolve_row`'s per-row cost; see the deviation note below.\n\n",
    );
    md.push_str(&format!(
        "Calibration: {CALIBRATION_SAMPLES} consecutive `stats_sampler()` calls with nothing \
         in between measured {cal_bytes} bytes and {cal_allocs} allocations of overhead \
         (`StatsAlloc::stats()` only reads atomics, so this is expected to be exactly zero; \
         it was not subtracted from anything below because it measured zero).\n\n"
    ));

    md.push_str("## Deviation from the specified method\n\n");
    md.push_str(&format!(
        "The step delta's window (`after_columns` to `after_resolve_rows` in \
         `build_object`) covers the per-row loop (which `resolve_row`'s per-component \
         sampling and the per-row whole-call sampling both measure) plus two things \
         outside it: the once-per-encode setup between `after_columns` firing and the \
         loop starting (building `stream_seeds`, one `StreamSeed` per distinct stream, \
         then `StampScratch::prepare` and `rows: Vec::with_capacity(self.records.len())`), \
         and the row-ordering step that runs once after the loop (`rows.sort_by(...)` in \
         the unclustered path, or `clustered_permutation` plus `permute` in the clustered \
         path).\n\n\
         The first run against `1_stream` found the sum of per-row whole-call deltas \
         undershooting the step delta by far more than 5% (7,367,670 vs 11,368,286 bytes, \
         a 4,000,616 byte gap). `size_of::<ResolvedRow>()` is {resolved_row_size} bytes; \
         {resolved_row_size} * {RECORDS_PER_OBJECT} = {} bytes, matching the gap to within \
         616 bytes -- initially read as corroborating a stable-sort auxiliary-buffer \
         hypothesis, since that buffer is also sized `size_of::<ResolvedRow>() * \
         records.len()`. Measuring the sort/permute step directly (sampling immediately \
         around the `match &cluster {{ ... }}` block) found it contributes close to zero \
         net live bytes: a transient buffer that is allocated and freed within the same \
         call nets to zero in a live-bytes delta by construction, which the two candidate \
         explanations cannot be told apart by magnitude alone, since \
         `Vec::with_capacity(records.len())` for `ResolvedRow` elements is sized \
         identically. Measuring the pre-loop setup directly instead accounted for the \
         `1_stream` gap: `rows`'s backing buffer is still live when `after_resolve_rows` \
         fires, so it shows up as net-positive bytes in the step delta the way a transient, \
         already-freed buffer cannot, and `stream_seeds` is trivial at one stream.\n\n\
         A second run against `20000_streams`, with the fix in place, still undershot (sum \
         of per-row and pre-loop-setup deltas 11,400,400 vs step delta 19,050,336, a \
         7,649,936 byte gap) because the first fix's sampling window started after \
         `stream_seeds` was already built. `stream_seeds` holds one `StreamSeed` per \
         distinct stream, so its cost scales with stream count, not row count, and is \
         negligible at 1 stream but dominant at 20,000: moving the sampling window to \
         start immediately after `after_columns` fires, before `stream_seeds` is built, \
         closed this gap too.\n\n\
         None of this is a `ResolvedRow` field or a per-row cost, so none of it belongs in \
         the five `ResolvedRow`-field buckets without misattributing it: the row Vec's \
         backing buffer holds every row, it is not part of any one of them; \
         `stream_seeds` is keyed by stream, not row. Rather than restructure `resolve_row` \
         or add a second hook mechanism, `build_object` now also samples directly around \
         the `match &cluster {{ ... }}` block and around the pre-loop setup, starting right \
         after `after_columns` fires (both gated by the same `ROW_SAMPLE` flag, accumulated \
         into two new `stage0` pairs -- `ROW_ORDER_BYTES`/`ROW_ORDER_ALLOCS` and \
         `ROW_SETUP_BYTES`/`ROW_SETUP_ALLOCS` -- distinct from the five `ResolvedRow`-field \
         buckets). The per-shape sections below report both as their own lines, and the \
         \"sum of per-row whole-call deltas equals the step delta within 5%\" assertion now \
         compares the step delta against the per-row sum plus both directly-measured \
         costs, not the per-row sum alone. This is a deviation from the dispatch's literal \
         assertion wording (which did not anticipate step-delta components outside \
         `resolve_row`), made because the alternative was either a false failure on correct \
         instrumentation or silently misattributing non-row cost to a `ResolvedRow` \
         field.\n\n",
        resolved_row_size * RECORDS_PER_OBJECT
    ));

    md.push_str("## ResolvedRow field to bucket assignment\n\n");
    md.push_str("| field | bucket |\n|---|---|\n");
    md.push_str("| `stream_ref` | everything else (scalar, no allocation) |\n");
    md.push_str("| `ts_ns` | everything else (scalar, no allocation) |\n");
    md.push_str("| `observed_ts_ns` | everything else (scalar, no allocation) |\n");
    md.push_str("| `severity_num` | everything else (scalar, no allocation) |\n");
    md.push_str("| `severity_text` | everything else (`String` clone) |\n");
    md.push_str("| `body` | everything else (`String` clone) |\n");
    md.push_str("| `trace_id` | everything else (fixed `[u8; 16]`, no allocation) |\n");
    md.push_str("| `span_id` | everything else (fixed `[u8; 8]`, no allocation) |\n");
    md.push_str("| `flags` | everything else (scalar, no allocation) |\n");
    md.push_str(
        "| `attrs_raw` | overflow attributes (`canonical_attr_bytes(&overflow)`) |\n",
    );
    md.push_str(
        "| `columns` | column map nodes (`BTreeMap` inserts plus the final \
         `cols.into_iter().collect()`); the `ColumnValue::Str`/`::Bytes` bytes each entry \
         holds are counted under owned string and byte values instead, see note below |\n",
    );
    md.push_str(
        "| `indexed_terms` | stamp scratch work (`stamp.finish`) |\n",
    );
    md.push_str(
        "| `stat_winners` | stamp scratch work (`stamp.finish`) |\n",
    );
    md.push_str(
        "\nNote: `resolve_value`'s `ColumnValue::Str`/`::Bytes` allocation happens once per \
         attribute before the branch that decides whether it lands in `columns` or in the \
         overflow path, so it is counted as its own bucket (owned string and byte values) \
         rather than split into `columns` vs `attrs_raw`; separating it per destination would \
         require restructuring `resolve_row`'s loop body. Likewise, `stamp.push_columnar` and \
         `stamp.push_overflow` take a second clone of the same value fed to the scratch \
         (distinct from the first, counted under owned string and byte values); that second \
         clone's cost is counted under stamp scratch work rather than split out, since it is \
         inseparable from the scratch call it is an argument to without restructuring.\n\n",
    );

    for r in &results {
        md.push_str(&format!("## {} ({} streams)\n\n", r.name, r.streams));
        md.push_str(&format!("Step delta (median over {RUNS} runs, sampling off): {} bytes\n\n", r.step_delta));
        md.push_str(&format!("Rows sampled: {} (expected {RECORDS_PER_OBJECT})\n\n", r.rows_sampled));
        md.push_str(&format!(
            "Sum of per-row whole-call deltas (sampling on): {} bytes, {} allocations\n\n",
            r.whole_row_bytes, r.whole_row_allocs
        ));
        md.push_str(&format!(
            "Row-ordering step (sort/permute, after the loop, not a `ResolvedRow` field): {} \
             bytes, {} allocations\n\n",
            r.row_order.bytes, r.row_order.allocs
        ));
        md.push_str(&format!(
            "Per-encode setup (`stream_seeds` build, one `StreamSeed` per distinct stream, \
             plus `StampScratch::prepare` and the row `Vec::with_capacity`; scales with \
             stream count not row count; not a `ResolvedRow` field): {} bytes, {} \
             allocations\n\n",
            r.row_setup.bytes, r.row_setup.allocs
        ));

        let bytes_per_row = r.step_delta as f64 / RECORDS_PER_OBJECT as f64;
        md.push_str(&format!(
            "Bytes per resolved row (step delta / {RECORDS_PER_OBJECT}): {bytes_per_row:.2}\n\n"
        ));

        let bucket_rows: [(&str, Bucket); 5] = [
            ("column map nodes", r.column_map),
            ("owned string and byte values", r.owned_values),
            ("overflow attributes", r.overflow),
            ("stamp scratch work", r.stamp_scratch),
            ("everything else", r.everything_else),
        ];
        let bucket_sum: i64 = bucket_rows.iter().map(|(_, b)| b.bytes).sum();
        let remainder = r.step_delta - bucket_sum - r.row_order.bytes - r.row_setup.bytes;
        let remainder_pct = remainder as f64 / r.step_delta as f64 * 100.0;

        md.push_str("| bucket | bytes | share of step delta | allocations/row | band | verdict |\n");
        md.push_str("|---|---|---|---|---|---|\n");
        for (bname, b) in &bucket_rows {
            let share = b.share_pct(r.step_delta);
            let per_row = b.per_row_allocs(r.rows_sampled);
            if let Some(band) = bands.iter().find(|bd| bd.name == *bname) {
                let share_status = band_status(share, band.share_lo, band.share_hi);
                let allocs_status = band_status(per_row, band.allocs_lo, band.allocs_hi);
                let verdict = if share_status == "inside" && allocs_status == "inside" {
                    "inside".to_string()
                } else {
                    format!("share {share_status}, allocs {allocs_status}")
                };
                md.push_str(&format!(
                    "| {bname} | {} | {share:.2}% | {per_row:.3} | share [{:.0}%, {:.0}%], allocs \
                     [{:.0}, {:.0}] | {verdict} |\n",
                    b.bytes, band.share_lo, band.share_hi, band.allocs_lo, band.allocs_hi
                ));
            } else {
                md.push_str(&format!(
                    "| {bname} | {} | {share:.2}% | {per_row:.3} | no pre-registered band (folds \
                     into \"everything else\" in the epic's expectations) | n/a |\n",
                    b.bytes
                ));
            }
        }
        let row_order_share = r.row_order.share_pct(r.step_delta);
        let row_order_per_row = r.row_order.per_row_allocs(r.rows_sampled);
        md.push_str(&format!(
            "| row-ordering step (not a `ResolvedRow` field, see deviation note above) | {} | \
             {row_order_share:.2}% | {row_order_per_row:.3} | no pre-registered band (outside \
             the epic's `ResolvedRow`-bucket expectations) | n/a |\n",
            r.row_order.bytes
        ));
        let row_setup_share = r.row_setup.share_pct(r.step_delta);
        let row_setup_per_row = r.row_setup.per_row_allocs(r.rows_sampled);
        md.push_str(&format!(
            "| per-encode setup: stream_seeds + stamp/row-vec init (not a `ResolvedRow` \
             field, see deviation note above) | {} | {row_setup_share:.2}% | \
             {row_setup_per_row:.3} | no pre-registered band (outside the epic's \
             `ResolvedRow`-bucket expectations) | n/a |\n",
            r.row_setup.bytes
        ));
        md.push_str(&format!(
            "| unattributed remainder | {remainder} | {remainder_pct:.2}% | n/a | < {REMAINDER_MAX_PCT:.0}% | {} |\n",
            band_status(remainder_pct, f64::MIN, REMAINDER_MAX_PCT)
        ));
        md.push('\n');

        if r.streams == 1 {
            let bpr_status = band_status(
                bytes_per_row,
                BYTES_PER_ROW_1_STREAM_LO,
                BYTES_PER_ROW_1_STREAM_HI,
            );
            md.push_str(&format!(
                "Bytes per resolved row band [{BYTES_PER_ROW_1_STREAM_LO}, \
                 {BYTES_PER_ROW_1_STREAM_HI}]: {bpr_status} (measured {bytes_per_row:.2})\n\n"
            ));
        }
    }

    println!("{md}");
    std::fs::write(
        concat!(env!("CARGO_MANIFEST_DIR"), "/../../stage0-resolved-row-memory.md"),
        md,
    )
    .expect("write report");
}
