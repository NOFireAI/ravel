//! ADR-0109's load-bearing microbench: the per-row value **gather** inside
//! `write_block` against the full block encode, at a narrow (10) and a wide
//! (105) dynamic-column count over one 8192-row block.
//!
//! The row path stages each dynamic column's value page with three linear
//! `.iter().find()` scans over every row's column vector -- presence over
//! `columns`, values over `columns`, and stat values over `stat_winners` -- so
//! the gather is O(rows x plan_columns x columns_per_row): quadratic in column
//! count. The `gather_scans` bench isolates exactly those three passes; the
//! `write_block` bench measures the whole encode.
//!
//! Read the RATIO of `gather_scans` to `write_block`, not the absolute times,
//! which are machine-dependent. ADR-0109 decision 8 rests on that ratio
//! widening sharply with column count: at 10 columns the gather is a fraction
//! of the encode, at 105 it is the same magnitude as the entire block encode.
//! The columnar build path (`RlogWriter::push_columnar`) deletes these scans by
//! staging each column from contiguous per-column data, so this bench is the
//! standing evidence for the cost it removes.

use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;

use ravel_logseg::block::{ColumnPlan, write_block};
use ravel_logseg::record::{ColumnValue, FIRST_DYNAMIC_COL, FieldType, ResolvedRow};

const ROWS: usize = 8192;

/// One block of `ROWS` rows, each carrying `n_cols` dynamic columns (even ids
/// `I64`, odd ids `Str`) with a matching `stat_winners` entry per numeric
/// column, plus the plan list for those columns.
fn make_block(n_cols: usize) -> (Vec<ResolvedRow>, Vec<ColumnPlan>) {
    let plans: Vec<ColumnPlan> = (0..n_cols)
        .map(|i| ColumnPlan {
            column_id: FIRST_DYNAMIC_COL + i as u32,
            ty: if i % 2 == 0 {
                FieldType::I64
            } else {
                FieldType::Str
            },
        })
        .collect();

    let rows: Vec<ResolvedRow> = (0..ROWS)
        .map(|r| {
            let mut columns = Vec::with_capacity(n_cols);
            let mut stat_winners = Vec::new();
            for i in 0..n_cols {
                let cid = FIRST_DYNAMIC_COL + i as u32;
                if i % 2 == 0 {
                    columns.push((cid, ColumnValue::I64((r + i) as i64)));
                    stat_winners.push((cid, ColumnValue::I64((r + i) as i64)));
                } else {
                    columns.push((cid, ColumnValue::Str(format!("v{r}_{i}").into_bytes())));
                }
            }
            ResolvedRow {
                stream_ref: 0,
                ts_ns: r as i64,
                observed_ts_ns: r as i64,
                severity_num: 9,
                severity_text: "INFO".into(),
                body: "hello world".into(),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs_raw: None,
                columns,
                indexed_terms: Vec::new(),
                stat_winners,
            }
        })
        .collect();

    (rows, plans)
}

/// The three linear gather passes `write_block` runs per plan column per row,
/// in isolation: presence and values over `columns`, stat values over
/// `stat_winners`. Mirrors `block::row_column` / `block::winner_value`, which
/// are private, so the scans are reproduced here.
fn gather_scans(rows: &[ResolvedRow], plans: &[ColumnPlan]) -> u64 {
    let mut acc: u64 = 0;
    for plan in plans {
        let cid = plan.column_id;
        for r in rows {
            if r.columns.iter().any(|(c, _)| *c == cid) {
                acc += 1;
            }
        }
        for r in rows {
            if let Some((_, v)) = r.columns.iter().find(|(c, _)| *c == cid) {
                acc += match v {
                    ColumnValue::I64(x) => *x as u64,
                    ColumnValue::Str(b) | ColumnValue::Bytes(b) => b.len() as u64,
                    _ => 0,
                };
            }
        }
        for r in rows {
            if let Some((_, v)) = r.stat_winners.iter().find(|(c, _)| *c == cid) {
                if let ColumnValue::I64(x) = v {
                    acc = acc.wrapping_add(*x as u64);
                }
            }
        }
    }
    acc
}

fn bench(c: &mut Criterion) {
    for n_cols in [10usize, 105] {
        let (rows, plans) = make_block(n_cols);

        c.bench_function(&format!("write_block/{n_cols}"), |b| {
            b.iter(|| write_block(black_box(&rows), black_box(&plans), 3).expect("encode"));
        });

        c.bench_function(&format!("gather_scans/{n_cols}"), |b| {
            b.iter(|| black_box(gather_scans(black_box(&rows), black_box(&plans))));
        });
    }
}

criterion_group!(benches, bench);
criterion_main!(benches);
