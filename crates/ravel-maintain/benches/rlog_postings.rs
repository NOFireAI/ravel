//! Wall-clock cost of the RLOG writer's POSTINGS accumulation (issue #3254,
//! candidate 2): `postings_terms` in `RlogWriter::build_object` and
//! `build_object_columnar` (crates/ravel-logseg/src/writer.rs), filled per
//! indexed term in block order and handed to the encoder as std maps, under
//! three maps (see `common/postings.rs` for the arms and the fixture).
//!
//! `GROUPING_ARM_ORDER` (default `ABC`) sets the order the arms run in within
//! each geometry, as in `compaction_grouping`.
//!
//! This binary keeps the default system allocator with no instrumentation;
//! peak memory and allocation counts come from `rlog_postings_alloc`.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::hint::black_box;
use std::time::Duration;

use criterion::{Criterion, SamplingMode, Throughput};

#[path = "common/postings.rs"]
mod postings;

use postings::{GEOMETRIES, INDEXED, arm_label, arm_order, fixture, run_arm};

fn bench_postings(c: &mut Criterion) {
    let order = std::env::var("GROUPING_ARM_ORDER").unwrap_or_else(|_| "ABC".to_string());
    let arms = arm_order(&order);
    println!("rlog_postings arm_order={order} seed={:#x}", postings::SEED);

    for g in GEOMETRIES {
        let rows = fixture(g);
        let inserts = (rows.len() * INDEXED.len()) as u64;

        // Every arm must hand the encoder the same terms and block sets, in
        // the same order, before any of them is timed.
        let reference = run_arm('A', &rows, &mut || {});
        for &arm in &arms {
            assert_eq!(
                run_arm(arm, &rows, &mut || {}),
                reference,
                "arm {arm} disagrees with A on geometry {}",
                g.name
            );
        }

        let mut group = c.benchmark_group(format!("postings/{}", g.name));
        group.throughput(Throughput::Elements(inserts));
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
                b.iter(|| black_box(run_arm(arm, black_box(&rows), &mut || {})));
            });
        }
        group.finish();
    }
}

fn main() {
    let mut c = Criterion::default().configure_from_args();
    bench_postings(&mut c);
    c.final_summary();
}
