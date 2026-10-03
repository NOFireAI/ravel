//! Stage 0d block-stage memory attribution for issue #2477 (epic #2467):
//! stage 0c (issue #2475, `logseg_row_vs_columnar_alloc.rs`) showed the block
//! loop and trailing sections of `build_object_columnar` (ADR-0109) adding
//! several MB of live bytes, with nothing attributing that memory to
//! specific structures. This bin samples around every structure listed by
//! reading `build_object_columnar` (see the `STRUCTURE LIST` constant below)
//! through the `stage0::BLOCK_SAMPLE` extension added by this task, reusing
//! the existing `stage0::HOOK` labels for the sampling-off stage deltas.
//! Report-only; never wired into `cargo bench`. Uses `stats_alloc`'s
//! instrumented global allocator exactly as `logseg_row_vs_columnar_alloc.rs`
//! does; its `unsafe impl GlobalAlloc` lives entirely inside that crate, no
//! `unsafe` appears in this file.
//!
//! Run directly:
//!   cargo run -p ravel-bench --release --bin logseg_block_stage_alloc
#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "../../../ravel-logseg/benches/common/mod.rs"]
mod common;

use std::alloc::System;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};

use common::{bench_config, bench_identity, build_corpus};
use ravel_logseg::columnar_batch::ColumnarLogBatch;
use ravel_logseg::footer::kind;
use ravel_logseg::writer::stage0;
use ravel_logseg::{LogRecord, RlogWriter};
use stats_alloc::{INSTRUMENTED_SYSTEM, Stats, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

const RECORDS_PER_OBJECT: usize = 20_000;

const SHAPES: [(&str, usize, usize); 3] = [
    ("1_stream", 1, 20_000),
    ("1000_streams", 1_000, 20),
    ("20000_streams", 20_000, 1),
];

/// The 7 labels `build_object_columnar` fires through the shared
/// `stage0::HOOK`, same set `logseg_row_vs_columnar_alloc.rs` uses.
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

static RECORD_SAMPLES: AtomicBool = AtomicBool::new(false);

fn stage0_hook(label: &'static str) {
    if !RECORD_SAMPLES.load(Relaxed) {
        return;
    }
    let stats = GLOBAL.stats();
    SAMPLES.with(|s| s.borrow_mut().push((label, stats)));
}

fn live_bytes(s: &Stats) -> i64 {
    s.bytes_allocated as i64 - s.bytes_deallocated as i64 + s.bytes_reallocated as i64
}

/// Installed into `stage0::STATS_SAMPLER` so the library's block/section
/// sampling call sites (gated on `stage0::BLOCK_SAMPLE`) can take a real
/// `(live_bytes, alloc_count)` reading without the library depending on
/// `stats_alloc` itself.
fn stats_sampler() -> (i64, u64) {
    let s = GLOBAL.stats();
    (live_bytes(&s), s.allocations as u64)
}

fn fail(msg: String) -> ! {
    eprintln!("ASSERTION FAILED: {msg}");
    std::process::exit(1);
}

fn read_hook_samples() -> HashMap<&'static str, i64> {
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
            .map(|(label, stats)| (*label, live_bytes(stats)))
            .collect()
    })
}

/// Runs the columnar-with-records-dropped arm once: `from_records`, drop the
/// source records, `push_columnar`, `finish_with_stats`. Returns the encoded
/// bytes and (if `RECORD_SAMPLES` was on) the hook-label live-byte samples,
/// relative to the baseline taken before `from_records`.
fn run_columnar_dropped(corpus: &[LogRecord]) -> (Vec<u8>, HashMap<&'static str, i64>) {
    SAMPLES.with(|s| s.borrow_mut().clear());
    let recording = RECORD_SAMPLES.load(Relaxed);
    let baseline = live_bytes(&GLOBAL.stats());

    let source = corpus.to_vec();
    let batch = ColumnarLogBatch::from_records(&source);
    drop(source);

    let mut w = RlogWriter::new(bench_config(), bench_identity());
    w.push_columnar(batch).expect("push_columnar");
    let (bytes, _stats) = w.finish_with_stats().expect("finish");

    let labels = if recording {
        read_hook_samples()
            .into_iter()
            .map(|(l, b)| (l, b - baseline))
            .collect()
    } else {
        HashMap::new()
    };
    (bytes, labels)
}

// One figure name per shape must be printed exactly once (deliverable 4,
// assertion 3). Every numeric figure this bin reports is registered here.
thread_local! {
    static FIGURES: RefCell<HashSet<String>> = RefCell::new(HashSet::new());
}

fn note_figure(shape: &str, name: &str) {
    FIGURES.with(|f| {
        let key = format!("{shape}::{name}");
        if !f.borrow_mut().insert(key.clone()) {
            fail(format!("figure {key} printed more than once"));
        }
    });
}

/// One block-stage or section-stage bucket: its name, its byte/alloc deltas
/// (summed across the whole encode), and the pre-registered category it maps
/// to, if any (epic #2467, explicitly not tuned toward).
struct Bucket {
    name: &'static str,
    bytes: i64,
    allocs: u64,
    category: Option<&'static str>,
}

fn block_buckets() -> Vec<Bucket> {
    use stage0::*;
    vec![
        Bucket {
            name: "BLOCK_MATERIALIZE (per-row columnar occurrences, stamp output, plan bookkeeping)",
            bytes: BLOCK_MATERIALIZE_BYTES.load(Relaxed),
            allocs: BLOCK_MATERIALIZE_ALLOCS.load(Relaxed),
            category: None,
        },
        Bucket {
            name: "BLOCK_VECS (column-major value pages, transient per block)",
            bytes: BLOCK_VECS_BYTES.load(Relaxed),
            allocs: BLOCK_VECS_ALLOCS.load(Relaxed),
            category: None,
        },
        Bucket {
            name: "BLOCK_ENCODE (write_block_columnar output, held until BlocksBuilder::push)",
            bytes: BLOCK_ENCODE_BYTES.load(Relaxed),
            allocs: BLOCK_ENCODE_ALLOCS.load(Relaxed),
            category: Some("encoded block bytes held before the object is assembled"),
        },
        Bucket {
            name: "BLOCK_POSTINGS (postings_terms/postings_capped growth)",
            bytes: BLOCK_POSTINGS_BYTES.load(Relaxed),
            allocs: BLOCK_POSTINGS_ALLOCS.load(Relaxed),
            category: Some("postings accumulators"),
        },
        Bucket {
            name: "BLOCK_BLOOM (per-block BloomBuilder + bloom_entries.push)",
            bytes: BLOCK_BLOOM_BYTES.load(Relaxed),
            allocs: BLOCK_BLOOM_ALLOCS.load(Relaxed),
            category: Some("bloom inputs"),
        },
        Bucket {
            name: "BLOCK_DIRS_STATS (first_blk/last_blk/col_blocks growth)",
            bytes: BLOCK_DIRS_STATS_BYTES.load(Relaxed),
            allocs: BLOCK_DIRS_STATS_ALLOCS.load(Relaxed),
            category: Some("directories and stats"),
        },
        Bucket {
            name: "BLOCK_DICT_BUILDER (row-group string dictionary interning)",
            bytes: BLOCK_DICT_BUILDER_BYTES.load(Relaxed),
            allocs: BLOCK_DICT_BUILDER_ALLOCS.load(Relaxed),
            category: Some("row-group dictionaries and their builders"),
        },
        Bucket {
            name: "BLOCK_ASSEMBLY_ENCODED (periodic flush_group, net of dict-builder release)",
            bytes: BLOCK_ASSEMBLY_ENCODED_BYTES.load(Relaxed),
            allocs: BLOCK_ASSEMBLY_ENCODED_ALLOCS.load(Relaxed),
            category: Some("encoded block bytes held before the object is assembled"),
        },
    ]
}

fn section_buckets() -> Vec<Bucket> {
    use stage0::*;
    vec![
        Bucket {
            name: "SECTION_STREAM_DIR_BUILD (StreamEntry constrution, blob.to_vec() copy)",
            bytes: SECTION_STREAM_DIR_BUILD_BYTES.load(Relaxed),
            allocs: SECTION_STREAM_DIR_BUILD_ALLOCS.load(Relaxed),
            category: Some("stream directory copies"),
        },
        Bucket {
            name: "SECTION_FIELD_DIR_BUILD (FieldEntry construction)",
            bytes: SECTION_FIELD_DIR_BUILD_BYTES.load(Relaxed),
            allocs: SECTION_FIELD_DIR_BUILD_ALLOCS.load(Relaxed),
            category: None,
        },
        Bucket {
            name: "SECTION_BLOCKS_SKIP_BUILD (BlocksBuilder::finish_checked + SkipIndex::build)",
            bytes: SECTION_BLOCKS_SKIP_BUILD_BYTES.load(Relaxed),
            allocs: SECTION_BLOCKS_SKIP_BUILD_ALLOCS.load(Relaxed),
            category: None,
        },
        Bucket {
            name: "SECTION_POSTINGS_FIELDS_BUILD (postings_fields assembly from postings_terms)",
            bytes: SECTION_POSTINGS_FIELDS_BUILD_BYTES.load(Relaxed),
            allocs: SECTION_POSTINGS_FIELDS_BUILD_ALLOCS.load(Relaxed),
            category: None,
        },
        Bucket {
            name: "SECTION_STREAM_DIR_ENCODE (StreamDir::encode second blob copy)",
            bytes: SECTION_STREAM_DIR_ENCODE_BYTES.load(Relaxed),
            allocs: SECTION_STREAM_DIR_ENCODE_ALLOCS.load(Relaxed),
            category: Some("stream directory copies"),
        },
        Bucket {
            name: "SECTION_REMAINING_DIRS_ENCODE (FIELD_DIR+BLOCKS+SKIP_IDX+PAGE_DIR push_section)",
            bytes: SECTION_REMAINING_DIRS_ENCODE_BYTES.load(Relaxed),
            allocs: SECTION_REMAINING_DIRS_ENCODE_ALLOCS.load(Relaxed),
            category: None,
        },
        Bucket {
            name: "SECTION_BLOOM_ENCODE (BLOOM push_section)",
            bytes: SECTION_BLOOM_ENCODE_BYTES.load(Relaxed),
            allocs: SECTION_BLOOM_ENCODE_ALLOCS.load(Relaxed),
            category: None,
        },
        Bucket {
            name: "SECTION_POSTINGS_ENCODE (POSTINGS push_section, when any indexed field present)",
            bytes: SECTION_POSTINGS_ENCODE_BYTES.load(Relaxed),
            allocs: SECTION_POSTINGS_ENCODE_ALLOCS.load(Relaxed),
            category: None,
        },
    ]
}

fn band_status_range(pct: f64, lo: f64, hi: f64) -> &'static str {
    if pct < lo {
        "below band"
    } else if pct > hi {
        "above band"
    } else {
        "inside"
    }
}

fn band_status_max(pct: f64, max: f64) -> &'static str {
    if pct > max { "above band" } else { "inside" }
}

fn band_status_min(pct: f64, min: f64) -> &'static str {
    if pct < min { "below band" } else { "inside" }
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

/// Deliverable 1, written verbatim into the report: every structure built
/// before/inside the block loop and still alive at `after_blocks`, and every
/// structure the trailing sections build and keep alive through
/// `after_sections`/`before_return`. Line numbers are current as of this
/// commit in `crates/ravel-logseg/src/writer.rs`'s `build_object_columnar`.
const STRUCTURE_LIST: &str = "\
### Block stage (created before/inside the block loop, alive at `after_blocks`)

| structure | type | filled at | last read | could drop earlier? |\n|---|---|---|---|---|\n\
| `blocks.pending` | `Vec<BlockWriteOut>` (field of `BlocksBuilder`) | `BlocksBuilder::push`, line 3294 (`self.pending.push(out)`) | `BlocksBuilder::flush_group`, drained when the group fills or at `finish_checked` (line 1831) | no: the final partial group is only known complete once the loop ends |\n\
| `blocks.str_group`/`str_bytes` | `BTreeMap<u32, Option<GroupStrColumn>>` + `usize` (fields of `BlocksBuilder`) | `BlocksBuilder::intern`, called from `push` | consumed by `mem::take` inside `flush_group` | no: the dictionary decision needs the whole row group |\n\
| `bloom_entries` | `Vec<Vec<u8>>` | line 1717 (`bloom_entries.push(builder.finish_exact())`), once per block | line 1908 (`encode_rlog_bloom_section(&bloom_covered, &bloom_entries)`) | no: needed whole-object for the BLOOM section |\n\
| `bloom_covered` | set from `bloom_coverage()` (external, `ravel_codec`) | once before the block loop (line 1133) | line 1908, same BLOOM encode call | no |\n\
| `postings_terms` | `BTreeMap<u32, BTreeMap<Vec<u8>, BTreeSet<u32>>>` | per-block postings loop (inside the block loop, before `after_blocks`) | line 1847 (`postings_terms.remove(&cid)`), drained building `postings_fields` | no: needed until POSTINGS assembly |\n\
| `postings_capped` | `BTreeSet<u32>` | per-block postings loop, when a field exceeds `postings_max_distinct` | line 1844 (`postings_capped.contains(&cid)`) | no |\n\
| `col_present` | `HashMap<u32, u64>` | per-block, after the block is placed | line 1814 (`col_present.get(cid)`), FIELD_DIR build | no |\n\
| `col_blocks` | `HashMap<u32, u32>` | per-block, after the block is placed | line 1820 (`col_blocks.get(cid)`), FIELD_DIR build | no |\n\
| `first_blk` / `last_blk` | `HashMap<u32, u32>` (two maps) | per-block, after the block is placed | line 1796 (`first_blk.get(&r)`/`last_blk.get(&r)`), STREAM_DIR build | no |\n\n\
### Trailing sections (built after `after_blocks`, alive through `after_sections`/`before_return`)

| structure | type | filled at | last read | could drop earlier? |\n|---|---|---|---|---|\n\
| `stream_entries` -> `stream_dir` | `Vec<StreamEntry>` -> `StreamDir` | lines 1776-1796 | line 1867 (`stream_dir.encode()`) | yes: nothing reads `stream_dir` after its own `push_section` call; an explicit `drop(stream_dir)` right after line 1867 would free it one push_section call earlier than end-of-function |\n\
| `field_entries` -> `field_dir` | `Vec<FieldEntry>` -> `FieldDir` | lines 1799-1813 | line 1877 (`field_dir.encode()`) | yes, same reasoning as `stream_dir` |\n\
| `blocks_bytes` | `Vec<u8>` (BLOCKS section bytes) | `blocks.finish_checked()`, line 1831 | line 1882 (`Stored::raw(blocks_bytes)`), moved by value | already optimal: consumed by value at first use |\n\
| `l0` | `Vec<Level0Entry>` | `blocks.finish_checked()`, line 1831 | `SkipIndex::build(l0)`, line 1832, moved by value | already optimal |\n\
| `skip` | `SkipIndex` | line 1832 | line 1889 (`skip.encode()`) | yes, marginally: nothing reads it after its own push_section call |\n\
| `page_dir` | `PageDir` | `blocks.finish_checked()`, line 1831 | line 1894-1898 (`page_dir.encode()`) | yes, marginally, same reasoning |\n\
| `postings_fields` | `BTreeMap<u32, FieldTerms>` | lines 1838-1853 | line 1916 (`encode_postings_section(&postings_fields, ...)`) | no: it is the last section built |\n\
| `object` | `Vec<u8>` (assembled section bytes) | every `push_section` call, lines 1863-1927 | line 1960 (`write_footer_and_trailer(&mut object, &footer)`), then returned at line 1963 | no: it is the function's return value |\n\
| `sections` | `Vec<SectionDesc>` | every `push_section` call, lines 1863-1927 | moved into `LogFooter.sections` at line 1953 | no: needed for the footer |\n";

struct ShapeResult {
    name: &'static str,
    streams: usize,
    records_per_stream: usize,
    off_block_delta: i64,
    off_sections_delta: i64,
    block_buckets: Vec<Bucket>,
    section_buckets: Vec<Bucket>,
    block_transient_high_water: i64,
    block_capacity: HashMap<&'static str, (usize, usize)>,
    section_capacity: HashMap<&'static str, (usize, usize)>,
    block_count: u64,
    encoded_len: usize,
    section_lens: Vec<(u32, u64)>,
    bytes_off_len: usize,
    bytes_on_len: usize,
    bytes_identical: bool,
}

fn measure_shape(name: &'static str, streams: usize, records_per_stream: usize) -> ShapeResult {
    let corpus = build_corpus(streams, records_per_stream);
    assert_eq!(corpus.len(), RECORDS_PER_OBJECT, "{name}: corpus size");

    // Sampling OFF: clean stage deltas from the existing hook labels.
    stage0::BLOCK_SAMPLE.store(false, Relaxed);
    RECORD_SAMPLES.store(true, Relaxed);
    let (bytes_off, labels_off) = run_columnar_dropped(&corpus);
    RECORD_SAMPLES.store(false, Relaxed);

    let off_block_delta = labels_off["after_blocks"] - labels_off["after_resolve_rows"];
    let off_sections_delta = labels_off["after_sections"] - labels_off["after_blocks"];

    // Sampling ON: per-structure buckets, via the stage0 block/section atomics.
    stage0::reset_block_samples();
    stage0::BLOCK_SAMPLE.store(true, Relaxed);
    let (bytes_on, _labels_on) = run_columnar_dropped(&corpus);
    stage0::BLOCK_SAMPLE.store(false, Relaxed);

    if bytes_on != bytes_off {
        fail(format!(
            "{name}: encoded object differs between sampling OFF ({} bytes) and sampling ON ({} bytes)",
            bytes_off.len(),
            bytes_on.len()
        ));
    }

    let block_buckets = block_buckets();
    let section_buckets = section_buckets();
    let block_transient_high_water = stage0::BLOCK_TRANSIENT_HIGH_WATER.load(Relaxed);
    const BLOCK_CAPACITY_NAMES: [&str; 8] = [
        "blocks.pending",
        "bloom_entries",
        "bloom_covered",
        "postings_terms",
        "col_present",
        "col_blocks",
        "first_blk",
        "last_blk",
    ];
    let capacity_notes = stage0::take_capacity_notes();
    let mut block_capacity = HashMap::new();
    let mut section_capacity = HashMap::new();
    for (n, len, cap) in capacity_notes {
        if BLOCK_CAPACITY_NAMES.contains(&n) {
            block_capacity.insert(n, (len, cap));
        } else {
            section_capacity.insert(n, (len, cap));
        }
    }

    let footer = ravel_logseg::footer::open(&bytes_on).expect("footer::open");
    let section_lens: Vec<(u32, u64)> = footer.sections.iter().map(|s| (s.kind, s.len)).collect();

    ShapeResult {
        name,
        streams,
        records_per_stream,
        off_block_delta,
        off_sections_delta,
        block_buckets,
        section_buckets,
        block_transient_high_water,
        block_capacity,
        section_capacity,
        block_count: footer.block_count,
        encoded_len: bytes_on.len(),
        section_lens,
        bytes_off_len: bytes_off.len(),
        bytes_on_len: bytes_on.len(),
        bytes_identical: true,
    }
}

fn kind_name(k: u32) -> &'static str {
    match k {
        kind::STREAM_DIR => "STREAM_DIR",
        kind::FIELD_DIR => "FIELD_DIR",
        kind::BLOCKS => "BLOCKS",
        kind::SKIP_IDX => "SKIP_IDX",
        kind::BLOOM => "BLOOM",
        kind::POSTINGS => "POSTINGS",
        kind::PAGE_DIR => "PAGE_DIR",
        _ => "UNKNOWN",
    }
}

fn render_shape(md: &mut String, r: &ShapeResult) {
    md.push_str(&format!(
        "## {} ({} streams, {} records/stream)\n\n",
        r.name, r.streams, r.records_per_stream
    ));
    note_figure(r.name, "off_block_delta");
    note_figure(r.name, "off_sections_delta");
    md.push_str(&format!(
        "Sampling OFF: block stage delta (after_blocks - after_resolve_rows) = {} bytes; \
         trailing sections delta (after_sections - after_blocks) = {} bytes\n\n",
        r.off_block_delta, r.off_sections_delta
    ));

    note_figure(r.name, "bytes_identity");
    md.push_str(&format!(
        "Encoded object bytes: sampling OFF {} bytes, sampling ON {} bytes (byte-identical: {})\n\n",
        r.bytes_off_len, r.bytes_on_len, r.bytes_identical
    ));

    note_figure(r.name, "block_count");
    note_figure(r.name, "encoded_len");
    md.push_str(&format!(
        "Number of blocks: {}; encoded object length: {} bytes\n\n",
        r.block_count, r.encoded_len
    ));
    md.push_str("| section | encoded length (bytes) |\n|---|---|\n");
    for (k, len) in &r.section_lens {
        note_figure(r.name, &format!("section_len::{}", kind_name(*k)));
        md.push_str(&format!("| {} | {len} |\n", kind_name(*k)));
    }
    md.push('\n');

    render_stage(
        md,
        r.name,
        "Block stage",
        r.off_block_delta,
        &r.block_buckets,
        &r.block_capacity,
        &[
            ("bloom inputs", 35.0, 65.0),
            ("postings accumulators", 5.0, 25.0),
            ("row-group dictionaries and their builders", 5.0, 25.0),
        ],
        &[
            ("encoded block bytes held before the object is assembled", 5.0),
            ("directories and stats", 10.0),
        ],
    );
    note_figure(r.name, "block_transient_high_water");
    md.push_str(&format!(
        "Per-block transient high-water (largest live-bytes seen inside one block iteration, \
         minus that iteration's start, max over all iterations): {} bytes\n\n",
        r.block_transient_high_water
    ));

    render_stage(
        md,
        r.name,
        "Trailing sections",
        r.off_sections_delta,
        &r.section_buckets,
        &r.section_capacity,
        &[],
        &[],
    );
    let stream_dir_bytes: i64 = r
        .section_buckets
        .iter()
        .filter(|b| b.category == Some("stream directory copies"))
        .map(|b| b.bytes)
        .sum();
    let stream_dir_pct = if r.off_sections_delta != 0 {
        100.0 * stream_dir_bytes as f64 / r.off_sections_delta as f64
    } else {
        0.0
    };
    note_figure(r.name, "stream_dir_share_of_sections");
    md.push_str(&format!(
        "Stream directory copies (SECTION_STREAM_DIR_BUILD + SECTION_STREAM_DIR_ENCODE) share of \
         trailing sections delta: {stream_dir_pct:.2}% (pre-registered: at least 70% of the \
         trailing sections delta at 20000 streams is stream directory copies and per-stream \
         block-range bookkeeping): {}\n\n\
         Method note: \"per-stream block-range bookkeeping\" (`first_blk`/`last_blk`) is built \
         incrementally during the BLOCK stage, under `BLOCK_DIRS_STATS`, not during the trailing \
         sections measured here; if the pre-registration meant to count it against the sections \
         delta, this figure alone understates it and `BLOCK_DIRS_STATS`'s own row (above) is the \
         other half.\n\n",
        band_status_min(stream_dir_pct, 70.0)
    ));
}

#[allow(clippy::too_many_arguments)]
fn render_stage(
    md: &mut String,
    shape: &str,
    stage_title: &str,
    delta: i64,
    buckets: &[Bucket],
    capacity: &HashMap<&'static str, (usize, usize)>,
    range_bands: &[(&'static str, f64, f64)],
    max_bands: &[(&'static str, f64)],
) {
    md.push_str(&format!("### {stage_title}\n\n"));
    md.push_str("| bucket | bytes | allocs | share of stage delta | pre-registered band | status |\n|---|---|---|---|---|---|\n");
    let mut sum: i64 = 0;
    for b in buckets {
        sum += b.bytes;
        let pct = if delta != 0 {
            100.0 * b.bytes as f64 / delta as f64
        } else {
            0.0
        };
        note_figure(shape, &format!("{stage_title}::{}::bytes", b.name));
        let (band_text, status) = match b.category {
            Some(cat) => {
                if let Some((_, lo, hi)) = range_bands.iter().find(|(c, _, _)| *c == cat) {
                    (
                        format!("{cat} [{lo:.0}%, {hi:.0}%]"),
                        band_status_range(pct, *lo, *hi),
                    )
                } else if let Some((_, max)) = max_bands.iter().find(|(c, _)| *c == cat) {
                    (
                        format!("{cat} [under {max:.0}%]"),
                        band_status_max(pct, *max),
                    )
                } else {
                    (cat.to_string(), "not pre-registered")
                }
            }
            None => ("-".to_string(), "not pre-registered"),
        };
        md.push_str(&format!(
            "| {} | {} | {} | {pct:.2}% | {band_text} | {status} |\n",
            b.name, b.bytes, b.allocs
        ));
    }
    let remainder = delta - sum;
    let remainder_pct = if delta != 0 {
        100.0 * remainder as f64 / delta as f64
    } else {
        0.0
    };
    note_figure(shape, &format!("{stage_title}::remainder"));
    md.push_str(&format!(
        "| REMAINDER (unattributed) | {remainder} | - | {remainder_pct:.2}% | - | {} |\n\n",
        if remainder_pct.abs() <= 5.0 {
            "within 5% of stage delta"
        } else {
            "above 5%: see explanation below"
        }
    ));
    if remainder_pct.abs() > 5.0 {
        md.push_str(&format!(
            "Unattributed remainder for {stage_title} exceeds 5% of the stage delta. Code not \
             attributed by any bucket above: `BloomBuilder`'s own internal state (re-exported from \
             `ravel_codec`, out of this crate's instrumentation scope) beyond the bracket around \
             its construction/use; the small gap between `BLOCK_DICT_BUILDER` and \
             `BLOCK_ASSEMBLY_ENCODED`'s brackets where `BlocksBuilder::pending.push(out)` itself \
             may reallocate; and any allocator fragmentation/bookkeeping overhead `stats_alloc` \
             attributes to the running totals but that this bin's bracket placement does not \
             assign to a specific structure.\n\n"
        ));
    }

    md.push_str("| structure | len | capacity | capacity > 2x len? |\n|---|---|---|---|\n");
    let mut names: Vec<&&'static str> = capacity.keys().collect();
    names.sort();
    for name in names {
        let (len, cap) = capacity[*name];
        note_figure(shape, &format!("{stage_title}::capacity::{name}"));
        let miss = cap > 0 && cap > 2 * len.max(1);
        md.push_str(&format!(
            "| {name} | {len} | {} | {} |\n",
            if cap == 0 {
                "n/a (map)".to_string()
            } else {
                cap.to_string()
            },
            if cap == 0 {
                "n/a"
            } else if miss {
                "CAPACITY MISS"
            } else {
                "no"
            }
        ));
    }
    md.push('\n');
}

fn main() {
    stage0::HOOK
        .set(stage0_hook)
        .unwrap_or_else(|_| fail("stage0 hook already set".to_string()));
    stage0::STATS_SAMPLER
        .set(stats_sampler)
        .unwrap_or_else(|_| fail("stage0 stats sampler already set".to_string()));

    let host = uname_a();
    let uptime_before = uptime();

    let mut results = Vec::new();
    for &(name, streams, records_per_stream) in &SHAPES {
        FIGURES.with(|f| f.borrow_mut().clear());
        results.push(measure_shape(name, streams, records_per_stream));
    }

    let uptime_after = uptime();

    let mut md = String::new();
    md.push_str("# Stage 0d: block stage memory attribution (issue #2477)\n\n");
    md.push_str(&format!("Host: `{host}`\n\n"));
    md.push_str(&format!("Uptime before: `{uptime_before}`\n\n"));
    md.push_str(&format!("Uptime after: `{uptime_after}`\n\n"));
    md.push_str("Command: `cargo run --release -p ravel-bench --bin logseg_block_stage_alloc`\n\n");
    md.push_str(
        "Method: for each shape, the columnar-with-records-dropped arm (`ColumnarLogBatch::\
         from_records`, drop the source records, `push_columnar`, `finish_with_stats`) runs \
         twice. Sampling OFF uses the existing `stage0::HOOK` labels for clean stage deltas \
         (block stage = `after_blocks` - `after_resolve_rows`; trailing sections = \
         `after_sections` - `after_blocks`). Sampling ON enables `stage0::BLOCK_SAMPLE`, which \
         brackets every structure listed in the structure list below with `stats_alloc` samples \
         taken directly inside `build_object_columnar`/`BlocksBuilder`, accumulated into named \
         atomics the library exposes read-only (no dependency on `stats_alloc` in the library \
         itself: the bucket functions take `(i64, u64)` pairs the bin computes from its own \
         sampler). The encoded object bytes are asserted identical between the two runs, so \
         sampling has no observable effect on the written object. `stats_alloc`'s counters are \
         running totals; every bucket here is a net delta between two samples, not a true peak -- \
         see `BLOCK_TRANSIENT_HIGH_WATER`'s own row for the one figure that tries to see inside \
         an iteration instead of only at its boundaries.\n\n",
    );
    md.push_str(
        "Warning (epic #2467): the orchestrator's last three pre-registrations on this code \
         missed, so expect the named buckets below to be partly wrong. The remainder row and the \
         structure list are what make this result usable when they are.\n\n",
    );

    md.push_str("## Structure list (deliverable 1)\n\n");
    md.push_str(STRUCTURE_LIST);
    md.push('\n');

    for r in &results {
        render_shape(&mut md, r);
    }

    println!("{md}");
    std::fs::write(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../stage0d-block-stage-memory.md"
        ),
        &md,
    )
    .expect("write report");
}
