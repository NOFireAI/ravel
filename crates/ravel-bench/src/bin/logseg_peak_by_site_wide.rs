//! Stage 1 width-gate true-peak measurement for issue #2563 (epic #2467,
//! ADR-2467 decision 4). Sibling of `logseg_peak_by_site.rs` (stage 0f,
//! issue #2485), which never ran a wide (105-dynamic-attribute) corpus: this
//! bin reuses that bin's exact method (one process per (arm, shape), the
//! `dhat::Profiler` started before the corpus is built so the peak can
//! include the input batch, process ends normally so `Drop` writes the
//! report at the true end of the run) over the wide corpus instead, and is
//! restricted to the two arms the width-gate question needs: `row` and
//! `col_dropped` (the accepted-decision route). `col_kept` is out of scope
//! for this task.
//!
//! Run directly, once per (arm, shape) pair:
//!   CARGO_PROFILE_RELEASE_DEBUG=true cargo run -p ravel-bench --release \
//!     --bin logseg_peak_by_site_wide -- row wide_1_stream
#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "../../../ravel-logseg/benches/common/mod.rs"]
mod common;
#[path = "wide_corpus.rs"]
mod wide_corpus;

use std::path::PathBuf;

use common::{bench_config, bench_identity};
use ravel_logseg::columnar_batch::ColumnarLogBatch;
use ravel_logseg::RlogWriter;
use wide_corpus::{build_wide_corpus, WIDE_ATTR_COUNT};

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

const RECORDS_PER_OBJECT: usize = 20_000;

const ARMS: [&str; 2] = ["row", "col_dropped"];
const SHAPES: [&str; 2] = ["wide_1_stream", "wide_1000_streams"];

fn shape_params(shape: &str) -> (usize, usize) {
    match shape {
        "wide_1_stream" => (1, 20_000),
        "wide_1000_streams" => (1_000, 20),
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
            "usage: logseg_peak_by_site_wide <arm: row|col_dropped> \
             <shape: wide_1_stream|wide_1000_streams>"
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
    let out_path = out_dir.join(format!("dhat-peak-wide-{arm}-{shape}.json"));

    let profiler = dhat::Profiler::builder().file_name(&out_path).build();

    let corpus = build_wide_corpus(streams, records_per_stream);
    assert_eq!(corpus.len(), RECORDS_PER_OBJECT, "{shape}: corpus size");
    for rec in &corpus {
        assert_eq!(
            rec.attrs.len(),
            WIDE_ATTR_COUNT,
            "{shape}: every record carries {WIDE_ATTR_COUNT} dynamic attrs"
        );
    }

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
        other => unreachable!("arm {other:?} already validated against ARMS"),
    };

    let hash = blake3::hash(&object_bytes);
    println!("OBJECT_LEN {}", object_bytes.len());
    println!("OBJECT_HASH {}", hash.to_hex());

    drop(profiler);
}
