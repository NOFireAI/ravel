//! ADR-4523 R5 (issue #4530): the metrics run fold's absorb plus finish on
//! the pre-registered fold shapes A, B and C, driven through the engine's
//! real `RunFold` by `ravel_query::run_fold_support`. Not banded; this is
//! the baseline a later step compares against. The bench builds the fold as
//! production does, apart from the absorb observer.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use ravel_query::FetchedSeriesSoa;
use ravel_query::run_fold_support::{ScalarRunFold, shape_a, shape_b, shape_c};

fn fold(calls: Vec<Vec<FetchedSeriesSoa>>) -> usize {
    let mut fold = ScalarRunFold::default();
    for call in calls {
        fold.absorb(call).expect("absorb");
    }
    fold.into_series().expect("finish").len()
}

fn bench_run_fold(c: &mut Criterion) {
    let mut group = c.benchmark_group("run_fold");
    group.sample_size(10);
    for (name, calls) in [
        ("A_20000x4", shape_a().expect("shape A")),
        ("B_8x200000", shape_b().expect("shape B")),
        ("C_2000x64x16", shape_c().expect("shape C")),
    ] {
        group.bench_function(name, |b| {
            b.iter_batched(|| calls.clone(), fold, BatchSize::LargeInput);
        });
    }
    group.finish();
}

criterion_group!(benches, bench_run_fold);
criterion_main!(benches);
