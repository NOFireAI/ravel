//! Series canonicalization + BLAKE3 id throughput.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use ravel_bench::generator::{CardinalityProfile, WorkloadConfig, generate_raw};
use ravel_types::{SeriesId, TenantId};

const SERIES_COUNT: usize = 20_000;

fn bench_series_id_hash(c: &mut Criterion) {
    let tenant = TenantId::new("bench-tenant");
    let mut group = c.benchmark_group("series_id_hash");

    for (name, profile) in [
        ("few_big_labels", CardinalityProfile::few_big(SERIES_COUNT)),
        (
            "many_small_labels",
            CardinalityProfile::many_small(SERIES_COUNT),
        ),
    ] {
        let config = WorkloadConfig {
            series_count: SERIES_COUNT,
            samples_per_series: 1,
            cardinality: profile,
            ..Default::default()
        };
        let raw = generate_raw(&config).expect("generate workload");
        let label_sets: Vec<_> = raw.into_iter().map(|(_, labels, _)| labels).collect();

        group.throughput(Throughput::Elements(label_sets.len() as u64));
        group.bench_function(name, |b| {
            b.iter(|| {
                for labels in &label_sets {
                    let id = SeriesId::compute(&tenant, "bench_gauge", labels).expect("compute");
                    std::hint::black_box(id);
                }
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_series_id_hash);
criterion_main!(benches);
