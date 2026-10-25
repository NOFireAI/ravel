//! Wall-clock cost of the metrics compaction grouping stage (issue #3254):
//! `build_parts`'s `by_series` map in `src/build.rs`, built in canonical input
//! order and drained in series order, under two maps (see
//! `common/grouping.rs` for the arms and the fixture).
//!
//! `GROUPING_ARM_ORDER` (default `AC`) sets the order the arms run in within
//! each geometry, so a second run can put the baseline last.
//!
//! This binary keeps the default system allocator with no instrumentation;
//! peak memory and allocation counts come from `compaction_grouping_alloc`.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::hint::black_box;
use std::time::Duration;

use criterion::{Criterion, SamplingMode, Throughput};

#[path = "common/grouping.rs"]
mod grouping;

use grouping::{GEOMETRIES, arm_label, arm_order, fixture, run_arm};

fn bench_grouping(c: &mut Criterion) {
    let order = std::env::var("GROUPING_ARM_ORDER").unwrap_or_else(|_| "AC".to_string());
    let arms = arm_order(&order);
    println!(
        "compaction_grouping arm_order={order} seed={:#x}",
        grouping::SEED
    );

    for g in GEOMETRIES {
        let inputs = fixture(g);
        let contributions = (g.inputs * g.series) as u64;

        // Every arm must drain the same groups in the same order with the
        // same per-group contribution order before any of them is timed.
        let reference = run_arm('A', &inputs, &mut || {});
        for &arm in &arms {
            assert_eq!(
                run_arm(arm, &inputs, &mut || {}),
                reference,
                "arm {arm} disagrees with A on geometry {}",
                g.name
            );
        }

        let mut group = c.benchmark_group(format!("grouping/{}", g.name));
        group.throughput(Throughput::Elements(contributions));
        match g.name {
            "small" => {
                group.sample_size(100);
                group.measurement_time(Duration::from_secs(5));
            }
            "large" => {
                group.sampling_mode(SamplingMode::Flat);
                group.sample_size(10);
                group.measurement_time(Duration::from_secs(30));
            }
            _ => {
                group.sampling_mode(SamplingMode::Flat);
                group.sample_size(30);
                group.measurement_time(Duration::from_secs(10));
            }
        }
        for &arm in &arms {
            let label = arm_label(arm).expect("arm_order validated the letter");
            group.bench_function(label, |b| {
                b.iter(|| black_box(run_arm(arm, black_box(&inputs), &mut || {})));
            });
        }
        group.finish();
    }
}

fn main() {
    let mut c = Criterion::default().configure_from_args();
    bench_grouping(&mut c);
    c.final_summary();
}
