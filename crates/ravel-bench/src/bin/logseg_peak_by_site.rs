//! Stage 0f true-peak measurement for issue #2485 (epic #2467). Stage 0e
//! (issue #2480, `logseg_live_by_site.rs`) attributed live bytes at
//! profiler-drop to allocation site, and in doing so found that every
//! earlier memory figure on this epic (issues #2428, #2469, #2475, #2477)
//! was inflated 4-7x by a `stats_alloc`-based `live_bytes` formula that
//! double-counted `realloc` growth. This bin answers a different question
//! than stage 0e: not "what is live at a chosen point" but "what is the
//! TRUE PEAK (dhat's t-gmax) over the whole encode, and which sites hold
//! it" -- for the row path and both columnar arms, so the three are directly
//! comparable.
//!
//! One run is one (arm, shape) pair, one process: the profiler starts
//! before the corpus is built (so the peak can include the input batch
//! itself) and the process ends normally once the object is encoded, so
//! `dhat::Profiler`'s `Drop` impl writes its report at the true end of the
//! run rather than at a chosen stop label. Report-only; never wired into
//! `cargo bench`. Needs debug symbols for `dhat` to resolve file:line, which
//! this crate's release profile does not carry by default: build with
//! `CARGO_PROFILE_RELEASE_DEBUG=true` in the environment rather than editing
//! the workspace `Cargo.toml`.
//!
//! Run directly, once per (arm, shape) pair:
//!   CARGO_PROFILE_RELEASE_DEBUG=true cargo run -p ravel-bench --release \
//!     --bin logseg_peak_by_site -- row 1_stream
#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "../../../ravel-logseg/benches/common/mod.rs"]
mod common;

use std::path::PathBuf;

use common::{bench_config, bench_identity, build_corpus};
use ravel_logseg::columnar_batch::ColumnarLogBatch;
use ravel_logseg::RlogWriter;

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

const RECORDS_PER_OBJECT: usize = 20_000;

const ARMS: [&str; 3] = ["row", "col_dropped", "col_kept"];
const SHAPES: [&str; 3] = ["1_stream", "1000_streams", "20000_streams"];

fn shape_params(shape: &str) -> (usize, usize) {
    match shape {
        "1_stream" => (1, 20_000),
        "1000_streams" => (1_000, 20),
        "20000_streams" => (20_000, 1),
        other => {
            eprintln!("unknown shape {other:?}; expected one of {SHAPES:?}");
            std::process::exit(2);
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        eprintln!(
            "usage: logseg_peak_by_site <arm: row|col_dropped|col_kept> \
             <shape: 1_stream|1000_streams|20000_streams>"
        );
        std::process::exit(2);
    }
    let arm = args[1].as_str();
    if !ARMS.contains(&arm) {
        eprintln!("unknown arm {arm:?}; expected one of {ARMS:?}");
        std::process::exit(2);
    }
    let shape = args[2].as_str();
    let (streams, records_per_stream) = shape_params(shape);

    let out_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.gate-logs");
    std::fs::create_dir_all(&out_dir).expect("create .gate-logs");
    let out_path = out_dir.join(format!("dhat-peak-{arm}-{shape}.json"));

    // Started before the corpus is built: the true global peak (t-gmax) can
    // then fall anywhere in the run, including during corpus construction,
    // rather than excluding the input from consideration the way stage 0e's
    // baseline point did.
    let profiler = dhat::Profiler::builder().file_name(&out_path).build();

    let corpus = build_corpus(streams, records_per_stream);
    assert_eq!(corpus.len(), RECORDS_PER_OBJECT, "{shape}: corpus size");

    // `RlogWriter` defaults `stage0::MODE` to 0 (the unmodified path); this
    // bin never touches it, so `build_object`'s `ref_of` and every other
    // stage0-gated branch run exactly as production code runs them.
    let object_bytes = match arm {
        "row" => {
            let mut w = RlogWriter::new(bench_config(), bench_identity());
            for rec in corpus {
                w.push(rec).expect("push");
            }
            w.finish().expect("finish")
        }
        "col_dropped" => {
            let batch = ColumnarLogBatch::from_records(&corpus);
            drop(corpus);
            let mut w = RlogWriter::new(bench_config(), bench_identity());
            w.push_columnar(batch).expect("push_columnar");
            w.finish().expect("finish")
        }
        "col_kept" => {
            let batch = ColumnarLogBatch::from_records(&corpus);
            let mut w = RlogWriter::new(bench_config(), bench_identity());
            w.push_columnar(batch).expect("push_columnar");
            let bytes = w.finish().expect("finish");
            drop(corpus);
            bytes
        }
        other => unreachable!("arm {other:?} already validated against ARMS"),
    };

    let hash = blake3::hash(&object_bytes);
    println!("OBJECT_LEN {}", object_bytes.len());
    println!("OBJECT_HASH {}", hash.to_hex());

    // Explicit so the point at which dhat writes its report (and prints its
    // own "At t-gmax"/"At t-end" summary to stderr) is visible in this
    // function rather than implicit in the end of `main`'s scope.
    drop(profiler);
}
