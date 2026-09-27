//! Placement and accounting tests. Every claim is proven by ordering, never by
//! elapsed time: a job parks on an explicit release, announces that it has
//! started, and the test observes the gate's counters between those two
//! events. Each test runs under `run_with_watchdog`, so a regression that
//! wedges the runtime panics with a message instead of hanging the binary.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use ravel_test_support::run_with_watchdog;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use super::*;

/// Bounds how long a test may take before the watchdog calls it a hang.
/// Nothing is compared against a measured duration.
const WATCHDOG: Duration = Duration::from_secs(10);

fn current_thread_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// A gate with both floors at 0, so every job goes through the permit.
fn floorless_gate<S: GateSite>(permits: usize) -> Arc<CpuGate<S>> {
    Arc::new(CpuGate::new(
        CpuGateConfig {
            permits,
            inline_floor_bytes: 0,
            eval_floor_samples: 0,
        },
        Arc::new(InstantClock::new()),
    ))
}

/// A job that announces `id` and then parks until `release`'s sender is
/// dropped. Dropping the sender, not sending on it, is the release, so the
/// test thread never blocks handing it over.
fn parked_job(
    id: u32,
    began: UnboundedSender<u32>,
    release: mpsc::Receiver<()>,
) -> impl FnOnce() -> u32 + Send + 'static {
    move || {
        let _ = began.send(id);
        let _ = release.recv();
        id
    }
}

/// Yields until `gate.queued()` reaches `expected`, failing if any job
/// announces a start in the meantime.
async fn wait_until_queued<S: GateSite>(
    gate: &CpuGate<S>,
    began: &mut UnboundedReceiver<u32>,
    expected: u64,
    why: &str,
) {
    loop {
        match began.try_recv() {
            Ok(id) => panic!("job {id} started: {why}"),
            Err(TryRecvError::Empty) => {}
            Err(TryRecvError::Disconnected) => panic!("every job sender dropped: {why}"),
        }
        if gate.queued() == expected {
            return;
        }
        tokio::task::yield_now().await;
    }
}

/// A gated job runs on the blocking pool, so a job parked mid-decode leaves
/// the runtime's only thread free to finish a concurrent task.
///
/// On a `current_thread` runtime there is one async thread. If the job ran on
/// it, the parked job would hold that thread, the test's own `recv().await`
/// could never be polled again, and the watchdog would fire.
#[test]
fn gate_job_does_not_block_a_concurrent_task() {
    run_with_watchdog(
        WATCHDOG,
        || {
            format!(
                "test hung for {WATCHDOG:?}: the gated job likely ran on the runtime thread \
                 instead of the blocking pool (ADR-1702 decision 2)"
            )
        },
        || {
            current_thread_runtime().block_on(async {
                let gate = floorless_gate::<ReadSite>(1);
                let (began_tx, mut began_rx) = unbounded_channel();
                let (release_tx, release_rx) = mpsc::channel::<()>();

                let job_gate = Arc::clone(&gate);
                let job = tokio::spawn(async move {
                    job_gate
                        .run(
                            ReadSite::CatalogPart,
                            JobSize::Bytes(1 << 20),
                            parked_job(1, began_tx, release_rx),
                        )
                        .await
                });

                assert_eq!(began_rx.recv().await, Some(1));
                let probe = tokio::spawn(async { 7u32 }).await.unwrap();
                assert_eq!(
                    probe, 7,
                    "the concurrent task completes while the job is parked"
                );
                assert_eq!(gate.running(), 1);

                drop(release_tx);
                assert_eq!(job.await.unwrap(), Ok(1));
                assert_eq!(gate.running(), 0);
            });
        },
    );
}

/// `permits + 1` parked jobs: exactly `permits` run and the extra one queues.
#[test]
fn gate_caps_running_jobs() {
    run_with_watchdog(
        WATCHDOG,
        || format!("test hung for {WATCHDOG:?} waiting for the extra job to queue"),
        || {
            current_thread_runtime().block_on(async {
                const PERMITS: usize = 2;
                let gate = floorless_gate::<WriteSite>(PERMITS);
                let (began_tx, mut began_rx) = unbounded_channel();

                let mut releases = Vec::new();
                let mut jobs = Vec::new();
                for id in 0..=PERMITS as u32 {
                    let (release_tx, release_rx) = mpsc::channel::<()>();
                    releases.push(release_tx);
                    let job_gate = Arc::clone(&gate);
                    let job = parked_job(id, began_tx.clone(), release_rx);
                    jobs.push(tokio::spawn(async move {
                        job_gate
                            .run(WriteSite::MetricsFlush, JobSize::Bytes(u64::MAX), job)
                            .await
                    }));
                }

                let mut started = HashSet::new();
                for _ in 0..PERMITS {
                    started.insert(began_rx.recv().await.unwrap());
                }
                assert_eq!(started.len(), PERMITS);
                wait_until_queued(
                    &gate,
                    &mut began_rx,
                    1,
                    "a job beyond the permit count must queue, not run",
                )
                .await;

                assert_eq!(gate.running(), PERMITS as u64);
                assert_eq!(gate.queued(), 1);

                drop(releases);
                for job in jobs {
                    assert!(job.await.unwrap().is_ok());
                }
                let snapshot = gate.snapshot();
                assert_eq!(snapshot.running, 0);
                assert_eq!(snapshot.queued, 0);
                assert_eq!(snapshot.wait_count, 3);
                assert_eq!(snapshot.run_count, 3);
                let flush = snapshot.sites[WriteSite::MetricsFlush.index()];
                assert_eq!(
                    (flush.site, flush.jobs, flush.inline),
                    (WriteSite::MetricsFlush, 3, 0)
                );
            });
        },
    );
}

/// Dropping the waiter of a running job does not free its permit: `running`
/// is unchanged, a new job queues behind it, and only the job's own return
/// lets the new job start. The job is then counted as abandoned.
#[test]
fn dropped_waiter_keeps_its_permit_until_the_job_returns() {
    run_with_watchdog(
        WATCHDOG,
        || format!("test hung for {WATCHDOG:?} after dropping a running job's waiter"),
        || {
            current_thread_runtime().block_on(async {
                let gate = floorless_gate::<ReadSite>(1);
                let (began_tx, mut began_rx) = unbounded_channel();
                let (release_tx, release_rx) = mpsc::channel::<()>();

                let job_gate = Arc::clone(&gate);
                let job = parked_job(1, began_tx.clone(), release_rx);
                let first = tokio::spawn(async move {
                    job_gate
                        .run(ReadSite::LogBlock, JobSize::Bytes(u64::MAX), job)
                        .await
                });
                assert_eq!(began_rx.recv().await, Some(1));

                first.abort();
                assert!(first.await.unwrap_err().is_cancelled());
                assert_eq!(
                    gate.running(),
                    1,
                    "the dropped waiter's job still holds its permit"
                );
                assert_eq!(gate.abandoned(), 0, "the job has not returned yet");

                let job_gate = Arc::clone(&gate);
                let began = began_tx.clone();
                let second = tokio::spawn(async move {
                    job_gate
                        .run(ReadSite::LogBlock, JobSize::Bytes(u64::MAX), move || {
                            let _ = began.send(2);
                            2
                        })
                        .await
                });
                wait_until_queued(
                    &gate,
                    &mut began_rx,
                    1,
                    "the new job must stay queued while the abandoned job runs",
                )
                .await;
                assert_eq!(gate.running(), 1);

                drop(release_tx);
                assert_eq!(second.await.unwrap(), Ok(2));
                assert_eq!(began_rx.recv().await, Some(2));
                assert_eq!(gate.abandoned(), 1);
                assert_eq!(gate.running(), 0);
                assert_eq!(gate.queued(), 0);
            });
        },
    );
}

/// A waiter dropped while it is still queued never runs its job.
#[test]
fn waiter_dropped_while_queued_never_runs() {
    run_with_watchdog(
        WATCHDOG,
        || format!("test hung for {WATCHDOG:?} after dropping a queued waiter"),
        || {
            current_thread_runtime().block_on(async {
                let gate = floorless_gate::<ReadSite>(1);
                let (began_tx, mut began_rx) = unbounded_channel();
                let (release_tx, release_rx) = mpsc::channel::<()>();

                let job_gate = Arc::clone(&gate);
                let job = parked_job(1, began_tx.clone(), release_rx);
                let first = tokio::spawn(async move {
                    job_gate
                        .run(ReadSite::Scrub, JobSize::Bytes(u64::MAX), job)
                        .await
                });
                assert_eq!(began_rx.recv().await, Some(1));

                let ran = Arc::new(AtomicBool::new(false));
                let job_gate = Arc::clone(&gate);
                let job_ran = Arc::clone(&ran);
                let queued = tokio::spawn(async move {
                    job_gate
                        .run(ReadSite::Scrub, JobSize::Bytes(u64::MAX), move || {
                            job_ran.store(true, Ordering::SeqCst);
                        })
                        .await
                });
                wait_until_queued(&gate, &mut began_rx, 1, "the second job must queue").await;
                queued.abort();
                assert!(queued.await.unwrap_err().is_cancelled());
                assert_eq!(gate.queued(), 0);

                drop(release_tx);
                assert_eq!(first.await.unwrap(), Ok(1));
                assert!(
                    !ran.load(Ordering::SeqCst),
                    "a waiter dropped in the queue ran"
                );
                let snapshot = gate.snapshot();
                assert_eq!(snapshot.sites[ReadSite::Scrub.index()].jobs, 1);
                assert_eq!(snapshot.wait_count, 1);
                assert_eq!(snapshot.abandoned, 0);
            });
        },
    );
}

/// Below the floor a job runs on the calling thread and counts as inline; at
/// the floor it goes through the gate and counts as a job. The byte floor and
/// the sample floor are separate settings.
#[test]
fn job_below_the_floor_runs_inline_and_counts_as_inline() {
    run_with_watchdog(
        WATCHDOG,
        || format!("test hung for {WATCHDOG:?} running inline jobs"),
        || {
            current_thread_runtime().block_on(async {
                let gate = CpuGate::<ReadSite>::new(
                    CpuGateConfig::with_permits(1),
                    Arc::new(InstantClock::new()),
                );
                let caller = std::thread::current().id();
                let on_thread = |size| {
                    let gate = &gate;
                    async move {
                        gate.run(ReadSite::SegmentSection, size, || {
                            std::thread::current().id()
                        })
                        .await
                        .unwrap()
                    }
                };

                assert_eq!(
                    on_thread(JobSize::Bytes(DEFAULT_INLINE_FLOOR_BYTES - 1)).await,
                    caller
                );
                assert_eq!(
                    on_thread(JobSize::Samples(DEFAULT_EVAL_FLOOR_SAMPLES - 1)).await,
                    caller
                );
                assert_ne!(
                    on_thread(JobSize::Bytes(DEFAULT_INLINE_FLOOR_BYTES)).await,
                    caller
                );
                assert_ne!(
                    on_thread(JobSize::Samples(DEFAULT_EVAL_FLOOR_SAMPLES)).await,
                    caller
                );

                let snapshot = gate.snapshot();
                let site = snapshot.sites[ReadSite::SegmentSection.index()];
                assert_eq!((site.inline, site.jobs), (2, 2));
                assert_eq!(snapshot.wait_count, 2, "inline runs take no permit");
                assert_eq!(snapshot.run_count, 2);
                let other_sites: u64 = snapshot
                    .sites
                    .iter()
                    .filter(|entry| entry.site != ReadSite::SegmentSection)
                    .map(|entry| entry.jobs + entry.inline)
                    .sum();
                assert_eq!(other_sites, 0);
            });
        },
    );
}

/// A panicking job is an error for its caller, and its permit comes back.
#[test]
fn panicking_job_is_an_error_and_releases_its_permit() {
    run_with_watchdog(
        WATCHDOG,
        || format!("test hung for {WATCHDOG:?}: a panicked job kept its permit"),
        || {
            current_thread_runtime().block_on(async {
                let gate = floorless_gate::<WriteSite>(1);
                let failed = gate
                    .run(WriteSite::OtapDecode, JobSize::Bytes(1), || -> u32 {
                        panic!("decode panicked")
                    })
                    .await;
                assert_eq!(failed, Err(CpuGateError::Panicked));
                assert_eq!(gate.running(), 0);
                assert_eq!(gate.abandoned(), 0);
                let next = gate
                    .run(WriteSite::OtapDecode, JobSize::Bytes(1), || 3)
                    .await;
                assert_eq!(next, Ok(3));
                assert_eq!(gate.snapshot().run_count, 2);
            });
        },
    );
}

/// A job below the floor that panics is the same error as a gated one: the
/// caller sees `Err(Panicked)` whatever the job's size, never an unwind.
#[test]
fn panicking_inline_job_is_an_error_not_an_unwind() {
    run_with_watchdog(
        WATCHDOG,
        || format!("test hung for {WATCHDOG:?} running an inline panic"),
        || {
            current_thread_runtime().block_on(async {
                let gate = CpuGate::<ReadSite>::new(
                    CpuGateConfig::with_permits(1),
                    Arc::new(InstantClock::new()),
                );
                let failed = gate
                    .run(
                        ReadSite::SegmentSection,
                        JobSize::Bytes(DEFAULT_INLINE_FLOOR_BYTES - 1),
                        || -> u32 { panic!("inline decode panicked") },
                    )
                    .await;
                assert_eq!(failed, Err(CpuGateError::Panicked));
                let snapshot = gate.snapshot();
                let site = snapshot.sites[ReadSite::SegmentSection.index()];
                assert_eq!((site.inline, site.jobs), (1, 0));
                assert_eq!(snapshot.wait_count, 0, "an inline run takes no permit");
            });
        },
    );
}

/// Returns 0, 10, 20, ... on successive reads.
struct StepClock(AtomicU64);

impl MonotonicClock for StepClock {
    fn now_nanos(&self) -> u64 {
        self.0.fetch_add(10, Ordering::SeqCst)
    }
}

/// One uncontended job reads the clock four times in a fixed order: wait
/// start, wait end, run start, run end.
#[test]
fn wait_and_run_times_come_from_the_injected_clock() {
    run_with_watchdog(
        WATCHDOG,
        || format!("test hung for {WATCHDOG:?} running one job"),
        || {
            current_thread_runtime().block_on(async {
                let gate = CpuGate::<ReadSite>::new(
                    CpuGateConfig {
                        permits: 1,
                        inline_floor_bytes: 0,
                        eval_floor_samples: 0,
                    },
                    Arc::new(StepClock(AtomicU64::new(0))),
                );
                assert_eq!(
                    gate.run(ReadSite::Fold, JobSize::Bytes(0), || 1).await,
                    Ok(1)
                );
                let snapshot = gate.snapshot();
                assert_eq!((snapshot.wait_nanos_sum, snapshot.wait_count), (10, 1));
                assert_eq!((snapshot.run_nanos_sum, snapshot.run_count), (10, 1));
            });
        },
    );
}

#[test]
fn permits_are_floored_at_one_and_defaults_follow_the_adr() {
    let gate = CpuGate::<ReadSite>::new(
        CpuGateConfig::with_permits(0),
        Arc::new(InstantClock::new()),
    );
    assert_eq!(gate.permits(), 1);
    assert_eq!(gate.kind(), GateKind::Read);

    let cores_to_defaults: Vec<(usize, usize, usize)> = [0, 1, 2, 3, 4, 16]
        .into_iter()
        .map(|cores| {
            (
                cores,
                default_read_permits(cores),
                default_write_permits(cores),
            )
        })
        .collect();
    assert_eq!(
        cores_to_defaults,
        vec![
            (0, 1, 1),
            (1, 1, 1),
            (2, 1, 1),
            (3, 2, 1),
            (4, 3, 2),
            (16, 15, 8),
        ]
    );
}
