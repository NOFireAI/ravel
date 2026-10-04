//! Stage 2 sparse-input true-peak measurement for issue #2585 (epic #2467).
//! Sibling of `logseg_peak_by_site_wide.rs` (stage 1, issue #2563): same
//! method (one process per (arm, shape), `dhat::Profiler` started before the
//! corpus is built so the peak can include the input batch, process ends
//! normally so `Drop` writes the report at the true end of the run),
//! restricted to the same two arms (`row`, `col_dropped`), over
//! `sparse_corpus` instead of `wide_corpus`: few attributes per record, drawn
//! from many distinct keys, which is the shape the stage-1 review flagged as
//! untested -- `ColumnarLogBatch::from_records` allocates one dense
//! `Vec<Option<AttrValue>>` per distinct `(name, type)` ever seen, so its
//! memory grows with records times distinct keys, not records times
//! attributes actually present per record.
//!
//! Run directly, once per (arm, shape) pair:
//!   CARGO_PROFILE_RELEASE_DEBUG=true cargo run -p ravel-bench --release \
//!     --bin logseg_peak_by_site_sparse -- row sparse_20000_100
#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "../../../ravel-logseg/benches/common/mod.rs"]
mod common;
#[path = "sparse_corpus.rs"]
mod sparse_corpus;

use std::path::PathBuf;

use common::{bench_config, bench_identity};
use ravel_logseg::columnar_batch::ColumnarLogBatch;
use ravel_logseg::{AttrValue, RlogWriter};
use sparse_corpus::{build_sparse_corpus, distinct_keys_used, ATTRS_PER_RECORD};

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

const ARMS: [&str; 2] = ["row", "col_dropped"];
const SHAPES: [&str; 4] = [
    "sparse_20000_100",
    "sparse_20000_1000",
    "sparse_20000_10000",
    "sparse_200000_1000",
];

fn shape_params(shape: &str) -> (usize, usize) {
    match shape {
        "sparse_20000_100" => (20_000, 100),
        "sparse_20000_1000" => (20_000, 1_000),
        "sparse_20000_10000" => (20_000, 10_000),
        "sparse_200000_1000" => (200_000, 1_000),
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
            "usage: logseg_peak_by_site_sparse <arm: row|col_dropped> \
             <shape: {SHAPES:?}>"
        );
        std::process::exit(2);
    }
    let arm = args[1].as_str();
    if !ARMS.contains(&arm) {
        eprintln!("unknown arm {arm:?}; expected one of {ARMS:?}");
        std::process::exit(2);
    }
    let shape = args[2].as_str();
    let (n, k) = shape_params(shape);

    println!(
        "SIZE_OF_OPTION_ATTRVALUE {}",
        std::mem::size_of::<Option<AttrValue>>()
    );
    println!(
        "DENSE_VECTOR_ESTIMATE_BYTES {}",
        k * n * std::mem::size_of::<Option<AttrValue>>()
    );

    let out_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.gate-logs");
    std::fs::create_dir_all(&out_dir).expect("create .gate-logs");
    let out_path = out_dir.join(format!("dhat-peak-sparse-{arm}-{shape}.json"));

    let profiler = dhat::Profiler::builder().file_name(&out_path).build();

    let corpus = build_sparse_corpus(n, k);
    assert_eq!(corpus.len(), n, "{shape}: corpus size");
    for rec in &corpus {
        assert_eq!(
            rec.attrs.len(),
            ATTRS_PER_RECORD,
            "{shape}: every record carries {ATTRS_PER_RECORD} attrs"
        );
    }
    let distinct_keys = distinct_keys_used(n, k);
    let total_attrs = n * ATTRS_PER_RECORD;
    println!("DISTINCT_KEYS_USED {distinct_keys}");
    println!("TOTAL_ATTR_COUNT {total_attrs}");

    let (object_bytes, stats) = match arm {
        "row" => {
            let mut w = RlogWriter::new(bench_config(), bench_identity());
            for rec in corpus {
                w.push(rec).expect("push");
            }
            w.finish_with_stats().expect("finish")
        }
        "col_dropped" => {
            let batch = ColumnarLogBatch::from_records(&corpus);
            drop(corpus);
            let mut w = RlogWriter::new(bench_config(), bench_identity());
            w.push_columnar(batch).expect("push_columnar");
            w.finish_with_stats().expect("finish")
        }
        other => unreachable!("arm {other:?} already validated against ARMS"),
    };

    let hash = blake3::hash(&object_bytes);
    println!("OBJECT_LEN {}", object_bytes.len());
    println!("OBJECT_HASH {}", hash.to_hex());
    println!(
        "DYNAMIC_COLUMNS used={} overflowed={}",
        stats.dynamic_columns_used, stats.dynamic_columns_overflowed
    );

    drop(profiler);
}
