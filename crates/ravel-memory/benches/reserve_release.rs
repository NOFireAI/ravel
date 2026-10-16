//! `MemoryBudget::try_reserve` followed by `release` with the resident gate
//! open, the hot path every SQL `try_grow`, fetch reservation and catalog
//! decode charge takes (ADR-2633 section 3: within 5 % of the figure before
//! the gate). `uncontended` is one thread; `contended_4_threads` runs the
//! same pair on four threads against one budget, so the CAS line is shared
//! and a gate load that landed on it would show.
#![allow(clippy::expect_used)]

use std::hint::black_box;
use std::sync::Barrier;
use std::time::{Duration, Instant};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use ravel_memory::MemoryBudget;

const RESERVATION: u64 = 4_096;
const THREADS: u64 = 4;

fn bench_reserve_release(c: &mut Criterion) {
    let mut group = c.benchmark_group("try_reserve_release");
    group.throughput(Throughput::Elements(1));

    let budget = MemoryBudget::new(1 << 40);
    group.bench_function("uncontended", |b| {
        b.iter(|| {
            budget
                .try_reserve(black_box(RESERVATION))
                .expect("the budget never fills");
            budget.release(black_box(RESERVATION));
        });
    });

    group.throughput(Throughput::Elements(THREADS));
    group.bench_function("contended_4_threads", |b| {
        b.iter_custom(|iters| {
            let budget = MemoryBudget::new(1 << 40);
            let barrier = Barrier::new(THREADS as usize);
            let mut slowest = Duration::ZERO;
            std::thread::scope(|scope| {
                let handles: Vec<_> = (0..THREADS)
                    .map(|_| {
                        scope.spawn(|| {
                            barrier.wait();
                            let start = Instant::now();
                            for _ in 0..iters {
                                budget
                                    .try_reserve(black_box(RESERVATION))
                                    .expect("the budget never fills");
                                budget.release(black_box(RESERVATION));
                            }
                            start.elapsed()
                        })
                    })
                    .collect();
                for handle in handles {
                    slowest = slowest.max(handle.join().expect("bench thread panicked"));
                }
            });
            slowest
        });
    });
    group.finish();
}

criterion_group!(benches, bench_reserve_release);
criterion_main!(benches);
