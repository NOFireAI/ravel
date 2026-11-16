//! Acceptance test for the ADR-0109 differential harness (#606): the comparison
//! must run BOTH the row path (`LogIngestRouter::write`) and the columnar path
//! (`LogIngestRouter::write_columnar`) over the same input, so it cannot
//! silently degenerate into measuring one path twice.
//!
//! The proof is the two reachability counters. `row_batches` is bumped only in
//! the match arm that calls `write`; `columnar_batches` only in the arm that
//! calls `write_columnar`. A run that measured the row path twice would report
//! `columnar_batches == 0` for the columnar run, and this test fails there.
//!
//! Demonstrated failing before the fix (issue #606 acceptance): flipping the
//! decisive line in `measure_path` (the `LoadPath::Columnar` arm of the
//! per-batch input `match`) to `Built::Row(chunk.to_vec())` makes the columnar
//! run drive `write`, leaving `columnar_batches == 0`. The
//! `columnar_run_drove_write_columnar` assertion below then fails with:
//!   "the columnar run must drive write_columnar, got columnar_batches=0".
#![allow(clippy::expect_used, clippy::unwrap_used)]

use ravel_bench::columnar_load::{self, CorpusShape};

/// A small corpus: enough rows for two batches per path, wide enough to have a
/// real per-row pivot, but fast to build and load under the test gate.
fn small_shape() -> CorpusShape {
    CorpusShape {
        rows: 2_000,
        int_cols: 12,
        str_cols: 8,
    }
}

#[tokio::test]
async fn comparison_runs_both_paths_over_the_same_input() {
    let shape = small_shape();
    let shards = 1;
    let batch_rows = 800; // 2000 rows -> 3 batches per path.

    let parquet = columnar_load::clickbench_shaped_parquet(shape);
    let records = columnar_load::decode_corpus(&parquet).expect("decode corpus");
    assert_eq!(
        records.len(),
        shape.rows,
        "the decoded corpus must have one record per source row"
    );

    let report = columnar_load::compare(&records, shape, shards, batch_rows, parquet.len())
        .await
        .expect("comparison run");

    // Both paths saw the same input.
    assert_eq!(
        report.row.rows_processed, report.columnar.rows_processed,
        "both paths must process the same row count"
    );
    assert_eq!(
        report.row.rows_processed, shape.rows as u64,
        "the row path must process every corpus row"
    );
    assert_eq!(
        report.row.objects_written, report.columnar.objects_written,
        "the same input over the same shard count must write the same object count"
    );
    assert!(
        report.row.objects_written > 0,
        "the load must actually write objects"
    );

    // The row run drove `write` and only `write`.
    assert!(
        report.row.row_batches > 0,
        "the row run must drive write, got row_batches=0"
    );
    assert_eq!(
        report.row.columnar_batches, 0,
        "the row run must never drive write_columnar, got columnar_batches={}",
        report.row.columnar_batches
    );

    // The columnar run drove `write_columnar` and only `write_columnar`. This is
    // the assertion the pre-fix flip trips.
    assert!(
        report.columnar.columnar_batches > 0,
        "the columnar run must drive write_columnar, got columnar_batches={}",
        report.columnar.columnar_batches
    );
    assert_eq!(
        report.columnar.row_batches, 0,
        "the columnar run must never drive write, got row_batches={}",
        report.columnar.row_batches
    );

    // The two runs are distinct paths, not one path measured twice: each drove
    // its own method the same number of batches, and neither drove the other's.
    assert_eq!(
        report.row.row_batches, report.columnar.columnar_batches,
        "row and columnar runs must cover the same number of batches by their own method"
    );
}
