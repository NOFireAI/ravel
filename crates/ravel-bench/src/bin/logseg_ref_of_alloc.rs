//! Stage 0b memory measurement for issue #2428 (epic #2425): how many bytes
//! the `ref_of` map (`crates/ravel-logseg/src/writer.rs`) holds, and what
//! share that is of the bytes an RLOG row-path encode has live at its peak.
//! Report-only; never wired into `cargo bench`. Uses `stats_alloc`'s
//! instrumented global allocator exactly as `segment_alloc_profile.rs` does;
//! `stats_alloc`'s `unsafe impl GlobalAlloc` lives entirely inside that
//! crate, no `unsafe` appears in this file.
//!
//! Run directly:
//!   cargo run -p ravel-bench --release --bin logseg_ref_of_alloc
#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "../../../ravel-logseg/benches/common/mod.rs"]
mod common;

use std::alloc::System;
use std::cell::RefCell;
use std::collections::HashMap;

use common::{bench_config, bench_identity, build_corpus};
use ravel_logseg::writer::stage0;
use ravel_logseg::{LogRecord, LogStreamId, RlogWriter};
use stats_alloc::{INSTRUMENTED_SYSTEM, Stats, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

const WARMUP: usize = 1;
const RUNS: usize = 5;
const RECORDS_PER_OBJECT: usize = 20_000;

/// (name, stream_count, records_per_stream). Each shape has
/// `stream_count * records_per_stream == RECORDS_PER_OBJECT`.
const SHAPES: [(&str, usize, usize); 3] = [
    ("1_stream", 1, 20_000),
    ("1000_streams", 1_000, 20),
    ("20000_streams", 20_000, 1),
];

/// Fixed label order. "pushed" is recorded directly by the driver (between
/// the last push and `finish`); the rest fire from inside `build_object` via
/// the stage0 hook. A hook fires in modes 0 and 2 only (never mode 1).
const LABELS: [&str; 8] = [
    "pushed",
    "before_index",
    "after_index",
    "after_columns",
    "after_resolve_rows",
    "after_blocks",
    "after_sections",
    "before_return",
];

thread_local! {
    // Pre-reserved so a push during the measured window never triggers a
    // reallocation of this buffer itself, which would otherwise bias the
    // very counters it is recording. Cleared (not reallocated) between
    // iterations.
    static SAMPLES: RefCell<Vec<(&'static str, Stats)>> =
        RefCell::new(Vec::with_capacity(LABELS.len() * 2));
}

/// Net live bytes implied by the allocator's running totals: bytes requested
/// by allocations, minus bytes freed by deallocations, plus the net
/// reallocation delta (positive when resizes grew more than they shrank).
/// These are the three `Stats` fields that together account for every byte
/// the allocator has seen; `allocations`/`deallocations`/`reallocations`
/// (the operation counts) are not used.
fn live_bytes(s: &Stats) -> i64 {
    s.bytes_allocated as i64 - s.bytes_deallocated as i64 + s.bytes_reallocated as i64
}

/// The stage0 hook: a plain `fn` pointer (not a closure), as the hook type
/// requires. Reads the global allocator's cumulative stats and appends them
/// under `label`; the driver subtracts the iteration's baseline afterward.
fn stage0_hook(label: &'static str) {
    let stats = GLOBAL.stats();
    SAMPLES.with(|s| s.borrow_mut().push((label, stats)));
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

#[derive(Clone)]
struct ModeData {
    // One Vec<i64> per label in `LABELS` order, one entry per measured run
    // (post-warmup), each already baseline-subtracted.
    label_values: Vec<Vec<i64>>,
    total_allocated: Vec<i64>,
    object_bytes: Vec<u8>,
}

impl ModeData {
    fn new() -> Self {
        ModeData {
            label_values: vec![Vec::with_capacity(RUNS); LABELS.len()],
            total_allocated: Vec::with_capacity(RUNS),
            object_bytes: Vec::new(),
        }
    }

    fn label_index(label: &str) -> usize {
        LABELS.iter().position(|l| *l == label).expect("known label")
    }

    fn values_for(&self, label: &str) -> &[i64] {
        &self.label_values[Self::label_index(label)]
    }

    fn index_bytes(&self) -> i64 {
        let before = self.values_for("before_index");
        let after = self.values_for("after_index");
        let deltas: Vec<i64> = before
            .iter()
            .zip(after.iter())
            .map(|(b, a)| a - b)
            .collect();
        median(&deltas)
    }

    /// The largest raw sample across every label and run, plus the label it
    /// occurred at.
    fn peak_sample(&self) -> (i64, &'static str) {
        let mut best = i64::MIN;
        let mut best_label = LABELS[0];
        for (li, label) in LABELS.iter().enumerate() {
            for &v in &self.label_values[li] {
                if v > best {
                    best = v;
                    best_label = label;
                }
            }
        }
        (best, best_label)
    }

    fn total_allocated_median(&self) -> i64 {
        median(&self.total_allocated)
    }
}

struct ShapeResult {
    name: &'static str,
    streams: usize,
    capacity: usize,
    entries: u64,
    mode0: ModeData,
    mode2: ModeData,
}

fn run_iteration(corpus: Vec<LogRecord>, mode: u8) -> (HashMap<&'static str, i64>, i64, Vec<u8>) {
    use std::sync::atomic::Ordering::Relaxed;
    SAMPLES.with(|s| s.borrow_mut().clear());
    stage0::MODE.store(mode, Relaxed);

    let baseline = GLOBAL.stats();
    let mut w = RlogWriter::new(bench_config(), bench_identity());
    for r in corpus {
        w.push(r).expect("push");
    }
    stage0_hook("pushed");
    let object = w.finish().expect("finish");
    let end = GLOBAL.stats();

    let total_allocated = end.bytes_allocated as i64 - baseline.bytes_allocated as i64;
    let baseline_live = live_bytes(&baseline);

    let samples = SAMPLES.with(|s| s.borrow().clone());
    if samples.len() != LABELS.len() {
        fail(format!(
            "expected {} labels, got {}: {:?}",
            LABELS.len(),
            samples.len(),
            samples.iter().map(|(l, _)| *l).collect::<Vec<_>>()
        ));
    }
    let mut seen: HashMap<&'static str, i64> = HashMap::new();
    for (label, stats) in &samples {
        if seen.contains_key(label) {
            fail(format!("label {label} fired more than once in one encode"));
        }
        seen.insert(label, live_bytes(stats) - baseline_live);
    }
    for label in LABELS {
        if !seen.contains_key(label) {
            fail(format!("label {label} never fired"));
        }
    }

    (seen, total_allocated, object)
}

fn measure_shape(name: &'static str, streams: usize, records_per_stream: usize) -> ShapeResult {
    let corpus = build_corpus(streams, records_per_stream);
    assert_eq!(corpus.len(), RECORDS_PER_OBJECT, "{name}: corpus size");

    // Mode 1 encode, once: reuses the existing entries counter (issue #2426
    // scaffolding) to confirm the map holds one entry per distinct stream.
    {
        use std::sync::atomic::Ordering::Relaxed;
        stage0::ENTRIES.store(0, Relaxed);
        stage0::MODE.store(1, Relaxed);
        let mut w = RlogWriter::new(bench_config(), bench_identity());
        for r in corpus.clone() {
            w.push(r).expect("push");
        }
        let _ = w.finish().expect("finish");
        let entries = stage0::ENTRIES.load(Relaxed);
        if entries != streams as u64 {
            fail(format!(
                "{name}: map entries {entries} != stream count {streams}"
            ));
        }
    }

    // The capacity `HashMap::with_capacity(streams)` resolves to for the
    // same key/value/hasher the writer uses internally (`HashMap<LogStreamId,
    // u32>`, default `RandomState`), computed here rather than read off the
    // writer (which exposes no getter for it).
    let capacity = HashMap::<LogStreamId, u32>::with_capacity(streams).capacity();

    let mut mode0 = ModeData::new();
    let mut mode2 = ModeData::new();

    for run in 0..WARMUP + RUNS {
        for &mode in &[0u8, 2u8] {
            let corpus = corpus.clone();
            let (samples, total_allocated, object) = run_iteration(corpus, mode);
            let data = if mode == 0 { &mut mode0 } else { &mut mode2 };
            if run == WARMUP {
                data.object_bytes = object.clone();
            }
            if run >= WARMUP {
                for (li, label) in LABELS.iter().enumerate() {
                    data.label_values[li].push(samples[label]);
                }
                data.total_allocated.push(total_allocated);
            }
            std::hint::black_box(&object);
        }
    }

    // Mode 2's map build is skipped entirely: "before_index" and
    // "after_index" must show zero movement on every run, not just on
    // median.
    for &delta in &mode2
        .values_for("before_index")
        .iter()
        .zip(mode2.values_for("after_index").iter())
        .map(|(b, a)| a - b)
        .collect::<Vec<i64>>()
    {
        if delta != 0 {
            fail(format!(
                "{name}: mode 2 INDEX_BYTES nonzero on a run ({delta})"
            ));
        }
    }

    if mode0.object_bytes != mode2.object_bytes {
        fail(format!(
            "{name}: mode 0 and mode 2 object bytes differ ({} vs {} bytes)",
            mode0.object_bytes.len(),
            mode2.object_bytes.len()
        ));
    }
    println!(
        "{name}: mode0==mode2 object bytes identical, len={}",
        mode0.object_bytes.len()
    );

    ShapeResult {
        name,
        streams,
        capacity,
        entries: streams as u64,
        mode0,
        mode2,
    }
}

fn main() {
    stage0::HOOK
        .set(stage0_hook)
        .unwrap_or_else(|_| fail("stage0 hook already set".to_string()));

    let map_entry_size = std::mem::size_of::<(LogStreamId, u32)>();

    let mut results = Vec::new();
    for &(name, streams, records_per_stream) in &SHAPES {
        results.push(measure_shape(name, streams, records_per_stream));
    }

    // Bands pre-registered on the epic (issue #2428), not tuned to.
    struct Band {
        index_lo: i64,
        index_hi: i64,
        share_max_pct: f64,
    }
    let bands: HashMap<&str, Band> = HashMap::from([
        (
            "1_stream",
            Band {
                index_lo: 60,
                index_hi: 200,
                share_max_pct: 0.01,
            },
        ),
        (
            "1000_streams",
            Band {
                index_lo: 43_000,
                index_hi: 43_100,
                share_max_pct: 1.0,
            },
        ),
        (
            "20000_streams",
            Band {
                index_lo: 688_100,
                index_hi: 688_200,
                share_max_pct: 5.0,
            },
        ),
    ]);

    let mut md = String::new();
    md.push_str("# Stage 0b: ref_of map memory measurement (issue #2428)\n\n");
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
    md.push_str("Command: `cargo run --release -p ravel-bench --bin logseg_ref_of_alloc`\n\n");
    md.push_str(
        "Stats fields used: `live_bytes = bytes_allocated - bytes_deallocated + bytes_reallocated` \
         (the allocator's cumulative running totals, sampled via `StatsAlloc::stats()`; \
         `allocations`/`deallocations`/`reallocations` operation counts are not used). \
         A baseline is sampled immediately before `RlogWriter::new` in each iteration and \
         subtracted from every later sample in that iteration. `TOTAL_ALLOCATED` is the \
         separate, unsubtracted `bytes_allocated` delta from that same baseline to immediately \
         after `finish()` returns.\n\n",
    );
    md.push_str(
        "**`stats_alloc` reports running totals, not a high-water mark.** A byte freed and a \
         new byte allocated between two samples cancel out in `live_bytes`, so a transient \
         allocation that peaked and was freed between two label points is invisible here. \
         `PEAK_SAMPLE` is therefore a LOWER bound on the true peak live memory, and `SHARE` \
         (the map's share of `PEAK_SAMPLE`) is correspondingly an UPPER bound on the map's \
         share of the true peak: the true peak can only be larger, never smaller, which can \
         only shrink the map's true share.\n\n",
    );
    md.push_str(&format!(
        "Map entry type `(LogStreamId, u32)` size_of: {map_entry_size} bytes.\n\n"
    ));

    for r in &results {
        md.push_str(&format!("## {} ({} streams)\n\n", r.name, r.streams));
        md.push_str(&format!(
            "Map entries (mode 1 check): {} (expected {})\n\n",
            r.entries, r.streams
        ));
        md.push_str("| label | mode0 live bytes (median [min..max]) | mode2 live bytes (median [min..max]) |\n");
        md.push_str("|---|---|---|\n");
        for label in LABELS {
            let m0 = r.mode0.values_for(label);
            let m2 = r.mode2.values_for(label);
            md.push_str(&format!(
                "| {label} | {} [{}..{}] | {} [{}..{}] |\n",
                median(m0),
                m0.iter().min().unwrap(),
                m0.iter().max().unwrap(),
                median(m2),
                m2.iter().min().unwrap(),
                m2.iter().max().unwrap(),
            ));
        }
        md.push('\n');

        let index_bytes_0 = r.mode0.index_bytes();
        let index_bytes_2 = r.mode2.index_bytes();
        let (peak_0, peak_0_label) = r.mode0.peak_sample();
        let (peak_2, peak_2_label) = r.mode2.peak_sample();
        let share = index_bytes_0 as f64 / peak_0 as f64 * 100.0;
        let total_0 = r.mode0.total_allocated_median();
        let total_2 = r.mode2.total_allocated_median();

        md.push_str(&format!(
            "INDEX_BYTES (mode0 after_index - before_index, median over {RUNS} runs): {index_bytes_0}\n\n"
        ));
        md.push_str(&format!(
            "INDEX_BYTES (mode2, must be 0): {index_bytes_2}\n\n"
        ));
        md.push_str(&format!(
            "PEAK_SAMPLE (mode0): {peak_0} bytes, at label `{peak_0_label}`\n\n"
        ));
        md.push_str(&format!(
            "PEAK_SAMPLE (mode2): {peak_2} bytes, at label `{peak_2_label}`\n\n"
        ));
        md.push_str(&format!("SHARE (mode0 INDEX_BYTES / mode0 PEAK_SAMPLE): {share:.4}%\n\n"));
        md.push_str(&format!(
            "TOTAL_ALLOCATED per encode (median over {RUNS} runs): mode0 {total_0} bytes, mode2 {total_2} bytes\n\n"
        ));

        if let Some(band) = bands.get(r.name) {
            let index_status = if index_bytes_0 >= band.index_lo && index_bytes_0 <= band.index_hi
            {
                "inside"
            } else {
                "MISS"
            };
            md.push_str(&format!(
                "INDEX_BYTES band [{}, {}]: {} (measured {})\n\n",
                band.index_lo, band.index_hi, index_status, index_bytes_0
            ));
            if index_status == "MISS" {
                md.push_str(&format!(
                    "Map capacity() for {} streams: {} (map entry size {} bytes; \
                     capacity * entry size = {} bytes, vs measured INDEX_BYTES {})\n\n",
                    r.streams,
                    r.capacity,
                    map_entry_size,
                    r.capacity * map_entry_size,
                    index_bytes_0
                ));
            }
            let share_status = if share < band.share_max_pct {
                "inside"
            } else {
                "MISS"
            };
            md.push_str(&format!(
                "SHARE band (< {}%): {} (measured {:.4}%)\n\n",
                band.share_max_pct, share_status, share
            ));
        }
    }

    println!("{md}");
    std::fs::write(
        concat!(env!("CARGO_MANIFEST_DIR"), "/../../stage0b-ref-of-memory.md"),
        md,
    )
    .expect("write report");
}
