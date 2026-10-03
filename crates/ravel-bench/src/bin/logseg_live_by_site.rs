//! Stage 0e live-bytes-by-allocation-site measurement for issue #2480 (epic
//! #2467). Stage 0d (issue #2477, `logseg_block_stage_alloc.rs`) found 9.49
//! MB live inside `write_block_columnar` output not released across the
//! block loop, `BlocksBuilder::finish_checked` releasing only 1.39 MB net,
//! and live bytes at `before_return` still 12.8 MB above
//! `after_resolve_rows` (1 stream). A reading of `string_shape`/`intern`/
//! `flush_group` in `crates/ravel-logseg/src/{block,writer}.rs` says the
//! row-group string interner should free most of that at the one flush
//! inside `finish_checked` (3 blocks, one row group). This bin resolves the
//! disagreement with `dhat` (heap profiling mode), which attributes LIVE
//! bytes at a chosen point in the run to the allocation call stack that
//! produced them, rather than to a hand-placed bucket.
//!
//! `dhat::Profiler` only reports heap state once, when it is dropped
//! (JSON's `eb` field per program point: `PpInfo::curr_bytes`/`curr_blocks`
//! "at termination, i.e. 'end'", confirmed from
//! `dhat-0.3.3/src/lib.rs`'s `PpInfoJson` comments). So each label is its
//! own process: this bin takes a shape and a stop label
//! (`after_resolve_rows`, `after_blocks`, `after_finish_checked`,
//! `before_return`), installs a `stage0::HOOK` that -- when the label
//! fires -- drops the profiler (flushing its report) and exits, and does
//! nothing on every other label.
//!
//! Report-only; never wired into `cargo bench`. Needs debug symbols for
//! `dhat` to resolve file:line, which this crate's release profile does not
//! carry by default: build with `CARGO_PROFILE_RELEASE_DEBUG=true` in the
//! environment rather than editing the workspace `Cargo.toml`.
//!
//! Run directly, once per (shape, label) pair:
//!   CARGO_PROFILE_RELEASE_DEBUG=true cargo run -p ravel-bench --release \
//!     --bin logseg_live_by_site -- 1_stream after_blocks
#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "../../../ravel-logseg/benches/common/mod.rs"]
mod common;

use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};

use common::{bench_config, bench_identity, build_corpus};
use ravel_logseg::columnar_batch::ColumnarLogBatch;
use ravel_logseg::writer::stage0;
use ravel_logseg::RlogWriter;

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

const RECORDS_PER_OBJECT: usize = 20_000;

const STOP_LABELS: [&str; 4] = [
    "after_resolve_rows",
    "after_blocks",
    "after_finish_checked",
    "before_return",
];

static STOP_LABEL: OnceLock<&'static str> = OnceLock::new();
static PROFILER: Mutex<Option<dhat::Profiler>> = Mutex::new(None);

fn stage0_hook(label: &'static str) {
    let Some(stop) = STOP_LABEL.get() else {
        return;
    };
    if label != *stop {
        return;
    }
    // Drop the profiler here, synchronously, so its `Drop` impl writes the
    // JSON report before the process exits. `process::exit` runs no
    // destructors, so this must happen explicitly and before the call.
    let mut guard = PROFILER.lock().expect("profiler mutex poisoned");
    drop(guard.take());
    drop(guard);
    std::process::exit(0);
}

fn shape_params(shape: &str) -> (usize, usize) {
    match shape {
        "1_stream" => (1, 20_000),
        "1000_streams" => (1_000, 20),
        "20000_streams" => (20_000, 1),
        other => {
            eprintln!(
                "unknown shape {other:?}; expected one of 1_stream, 1000_streams, 20000_streams"
            );
            std::process::exit(2);
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        eprintln!(
            "usage: logseg_live_by_site <shape: 1_stream|1000_streams|20000_streams> \
             <stop_label: after_resolve_rows|after_blocks|after_finish_checked|before_return>"
        );
        std::process::exit(2);
    }
    let shape = args[1].as_str();
    let stop_label = args[2].as_str();
    let Some(&stop_static) = STOP_LABELS.iter().find(|&&l| l == stop_label) else {
        eprintln!(
            "unknown stop label {stop_label:?}; expected one of {STOP_LABELS:?}"
        );
        std::process::exit(2);
    };
    STOP_LABEL.set(stop_static).expect("set stop label once");

    let (streams, records_per_stream) = shape_params(shape);

    stage0::HOOK
        .set(stage0_hook)
        .unwrap_or_else(|_| panic!("stage0 hook already set"));

    let corpus = build_corpus(streams, records_per_stream);
    assert_eq!(corpus.len(), RECORDS_PER_OBJECT, "{shape}: corpus size");

    let out_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.gate-logs");
    std::fs::create_dir_all(&out_dir).expect("create .gate-logs");
    let out_path = out_dir.join(format!("dhat-{shape}-{stop_label}.json"));

    // Start profiling right where the sampling-ON stats_alloc bin
    // (`logseg_block_stage_alloc.rs`) takes its baseline sample: immediately
    // before `corpus.to_vec()`/`from_records`. So this run's own totals are
    // already "added since baseline", with no further subtraction needed
    // for the `after_resolve_rows` run itself, and the later labels'
    // per-site tables are a strict superset (same deterministic prefix of
    // execution) that the `after_resolve_rows` table is subtracted from.
    *PROFILER.lock().expect("profiler mutex poisoned") =
        Some(dhat::Profiler::builder().file_name(&out_path).build());

    let source = corpus.to_vec();
    let batch = ColumnarLogBatch::from_records(&source);
    drop(source);

    let mut w = RlogWriter::new(bench_config(), bench_identity());
    w.push_columnar(batch).expect("push_columnar");
    let _ = w.finish_with_stats().expect("finish");

    // Every stop label sits on the path above (after_resolve_rows is fired
    // inside from_records/push_columnar's row-resolution loop depending on
    // which arm runs; after_blocks/after_finish_checked/before_return fire
    // inside finish_with_stats's build_object_columnar), so the hook always
    // exits before reaching here.
    eprintln!("stop label {stop_label} never fired for shape {shape}");
    std::process::exit(1);
}
