//! The decode stage's build workers (issue #4210): batches built on W threads
//! reach the loader in the order one decode thread produces them, W builds
//! run at once, every batch is charged to the load budget before it is built
//! and stays charged until the loader drops it, and a panic, an early
//! receiver drop or a rejected row ends the stage without a hang or a leaked
//! charge.
//!
//! Every wait in this module is released by an event the test arranges.
//! [`HANG_GUARD`] and [`RUN_GUARD`] only turn a regression that never
//! releases into a failure instead of a hung run, which [`guarded`] makes
//! hold for a decode stage stuck in a blocking wait; no assertion depends on
//! how long anything takes.

use std::sync::{Condvar, Mutex, MutexGuard};
use std::time::Duration;

use ravel_ingest::IngestByteBudgetLimit;

use super::*;

const HANG_GUARD: Duration = Duration::from_secs(30);

/// The hang guard on a whole decode run, longer than [`HANG_GUARD`] so a
/// gated build that gives up is reported by the assertion on it.
const RUN_GUARD: Duration = Duration::from_secs(120);

/// Runs one test body on its own runtime. When the body panics, a hang
/// guard's expiry included, the runtime is shut down without waiting for its
/// blocking tasks: dropping it normally would wait forever on a decode stage
/// stuck in a blocking wait, and the failure would never be reported.
fn guarded(test: impl std::future::Future<Output = ()>) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("test runtime");
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| rt.block_on(test)));
    if let Err(panic) = outcome {
        rt.shutdown_background();
        std::panic::resume_unwind(panic);
    }
}

/// One emitted outcome, reduced to what a sequence comparison needs.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Emitted {
    Batch { rows: usize, rlog: [u8; 32] },
    Done,
    Failed(String),
    Rejected { row: u64, reason: String },
}

impl Emitted {
    fn is_terminal(&self) -> bool {
        !matches!(self, Emitted::Batch { .. })
    }
}

/// Reduces one outcome, checking that a budgeted batch carries a charge equal
/// to its measured bytes.
fn emitted(out: Prefetched, budgeted: bool) -> Emitted {
    match out {
        Prefetched::Batch(Built::Columnar(batch, charge)) => {
            let heap = batch.heap_bytes() as u64;
            if budgeted {
                assert_eq!(
                    charge.as_ref().map(IngestByteCharge::bytes),
                    Some(heap),
                    "an emitted batch is charged exactly its measured bytes"
                );
            }
            drop(charge);
            let rows = batch.num_rows;
            // The RLOG writer refuses an empty object; an empty batch is
            // compared by its row count alone.
            let rlog = if rows == 0 {
                [0; 32]
            } else {
                *blake3::hash(&columnar_object(*batch)).as_bytes()
            };
            Emitted::Batch { rows, rlog }
        }
        Prefetched::Batch(Built::Row(_)) => panic!("columnar path only"),
        Prefetched::Done => Emitted::Done,
        Prefetched::BatchFailed { reason } => Emitted::Failed(reason),
        Prefetched::RowRejected { row, reason } => Emitted::Rejected { row, reason },
    }
}

/// The stride cursors `load` opens over `pq` at `read_cursors` cursors.
fn open_state(pq: &Path, read_cursors: usize, batch_rows: usize) -> StrideCursors {
    let input = FileInput { path: pq };
    let metadata = read_input_metadata(&input).expect("read metadata");
    let row_group_lens = row_group_row_counts(&metadata);
    let cursor_count = resolve_read_cursors(Some(read_cursors), 4, row_group_lens.len());
    let cursors = open_stride_cursors(
        &input,
        &metadata,
        &row_group_lens,
        cursor_count,
        batch_rows,
        reader_batch_rows(batch_rows, cursor_count),
    )
    .expect("cursors");
    StrideCursors {
        cursors,
        deal_offset: 0,
        skip_rows: 0,
    }
}

struct Decode<'a> {
    pq: &'a Path,
    mapping: &'a Mapping,
    read_cursors: usize,
    batch_rows: usize,
    limits: LogIngestLimits,
}

impl Decode<'_> {
    fn spawn(
        &self,
        workers: usize,
        hook: Option<SeqBuildHook>,
        charger: Option<BatchCharger>,
    ) -> (
        tokio::sync::mpsc::Receiver<Prefetched>,
        tokio::task::JoinHandle<()>,
    ) {
        spawn_decode_pipeline(
            open_state(self.pq, self.read_cursors, self.batch_rows),
            Arc::new(self.mapping.clone()),
            self.limits.clone(),
            Arc::new(TenantRules::none()),
            NOW_NS,
            self.batch_rows,
            LoadPath::Columnar,
            hook,
            None,
            2,
            charger,
            workers,
        )
    }

    /// Every outcome the stage emits, in emission order, until it closes the
    /// channel.
    async fn run(
        &self,
        workers: usize,
        hook: Option<SeqBuildHook>,
        charger: Option<BatchCharger>,
    ) -> Vec<Emitted> {
        let budgeted = charger.is_some();
        let (mut rx, handle) = self.spawn(workers, hook, charger);
        let drained = tokio::time::timeout(RUN_GUARD, async {
            let mut out = Vec::new();
            while let Some(p) = rx.recv().await {
                out.push(emitted(p, budgeted));
            }
            out
        })
        .await
        .expect("the decode stage finishes");
        handle.await.expect("decoder task joins");
        drained
    }
}

/// What the build hook has seen, shared with the test thread.
#[derive(Default)]
struct BuildLog {
    events: Vec<(u64, BuildEvent)>,
    started: Vec<u64>,
    delivered: Vec<u64>,
    active: usize,
    max_active: usize,
    /// A gated build gave up on [`HANG_GUARD`] instead of being released.
    timed_out: bool,
    released: bool,
}

#[derive(Default)]
struct Gate {
    log: Mutex<BuildLog>,
    changed: Condvar,
}

impl Gate {
    fn lock(&self) -> MutexGuard<'_, BuildLog> {
        self.log.lock().expect("build log lock")
    }

    /// Records `event` for `seq`, then, on a start, holds the build until
    /// `hold(seq, log)` is false.
    fn record(&self, seq: u64, event: BuildEvent, hold: impl Fn(u64, &BuildLog) -> bool) {
        let mut log = self.lock();
        log.events.push((seq, event));
        match event {
            BuildEvent::Started => {
                log.started.push(seq);
                log.active += 1;
                log.max_active = log.max_active.max(log.active);
            }
            BuildEvent::Delivered => {
                log.delivered.push(seq);
                log.active -= 1;
            }
        }
        self.changed.notify_all();
        if event == BuildEvent::Started {
            let (mut log, wait) = self
                .changed
                .wait_timeout_while(log, HANG_GUARD, |log| hold(seq, log))
                .expect("build log lock");
            if wait.timed_out() {
                log.timed_out = true;
            }
        }
    }

    /// Blocks the calling thread until `ready(log)`, at most [`HANG_GUARD`].
    fn wait_for(&self, ready: impl Fn(&BuildLog) -> bool) -> MutexGuard<'_, BuildLog> {
        let (log, _) = self
            .changed
            .wait_timeout_while(self.lock(), HANG_GUARD, |log| !ready(log))
            .expect("build log lock");
        log
    }

    fn release(&self) {
        self.lock().released = true;
        self.changed.notify_all();
    }
}

fn gated(
    gate: &Arc<Gate>,
    hold: impl Fn(u64, &BuildLog) -> bool + Send + Sync + 'static,
) -> SeqBuildHook {
    let gate = Arc::clone(gate);
    Arc::new(move |seq, event| gate.record(seq, event, &hold))
}

/// The same uneven Parquet file through one build worker and through four
/// emits the same batches, element for element, with the terminal outcome
/// last. The four-worker run holds batch 1's build until batches 2 and 3 have
/// been built and handed back, so its builds finish out of order and only the
/// re-sequencing puts batch 1 back in place.
///
/// Prove-the-test: emit each outcome as it arrives (`ready.pop_first()` for
/// `ready.remove(&self.next_emit)` in `DecodeCoordinator::run`) and the
/// sequence comparison fails, batch 2 arriving before batch 1.
#[test]
fn parallel_cursor_decode_yields_the_serial_batch_sequence() {
    guarded(parallel_cursor_decode_yields_the_serial_batch_sequence_body());
}

async fn parallel_cursor_decode_yields_the_serial_batch_sequence_body() {
    let (_dir, pq, m) = uneven_row_group_fixture();
    // Three cursors over row groups of 700/300/500/900/200 rows at 250 rows
    // a batch deal spans of unequal length from regions of unequal size.
    let decode = Decode {
        pq: &pq,
        mapping: &m,
        read_cursors: 3,
        batch_rows: 250,
        limits: LogIngestLimits::default(),
    };
    let serial = decode.run(1, None, None).await;

    let gate = Arc::new(Gate::default());
    let hold = |seq: u64, log: &BuildLog| {
        seq == 1 && !(log.delivered.contains(&2) && log.delivered.contains(&3))
    };
    let parallel = decode.run(4, Some(gated(&gate, hold)), None).await;

    assert!(serial.len() > 6, "several batches: {}", serial.len());
    assert_eq!(
        serial.last(),
        Some(&Emitted::Done),
        "the terminal outcome is last"
    );
    assert_eq!(
        serial.iter().filter(|e| e.is_terminal()).count(),
        1,
        "exactly one terminal outcome"
    );
    let log = gate.lock();
    assert!(!log.timed_out, "batch 1 was released by batches 2 and 3");
    let pos = |seq| log.delivered.iter().position(|&s| s == seq);
    assert!(
        pos(2) < pos(1) && pos(3) < pos(1),
        "batches 2 and 3 finished before batch 1: {:?}",
        log.delivered
    );
    assert_eq!(
        parallel, serial,
        "four workers emit the batches one worker does, in the same order"
    );
}

/// `rows` rows in row groups of 300, each with its index as the `seq`
/// attribute, all at [`NOW_NS`] except row `future`, which leads it by more
/// than `limits` admit.
fn future_row_fixture(
    rows: i64,
    future: i64,
    limits: &LogIngestLimits,
) -> (tempfile::TempDir, std::path::PathBuf, Mapping) {
    use parquet::arrow::ArrowWriter;

    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("future.parquet");
    let group = |first: i64, n: i64| {
        let seq: Vec<i64> = (first..first + n).collect();
        let ts = seq
            .iter()
            .map(|&r| {
                if r == future {
                    NOW_NS + limits.max_future_skew_ns + 1
                } else {
                    NOW_NS
                }
            })
            .collect();
        batch(vec![("ts", i64_col(ts)), ("seq", i64_col(seq))])
    };
    let file = std::fs::File::create(&pq).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, group(0, 0).schema(), None).expect("arrow writer");
    for first in (0..rows).step_by(300) {
        writer
            .write(&group(first, 300.min(rows - first)))
            .expect("write row group");
        writer.flush().expect("flush row group");
    }
    writer.close().expect("close writer");
    let mut m = base_mapping();
    m.attributes = vec![attr("seq", "seq", ColType::I64)];
    (dir, pq, m)
}

/// A row rejected in a later batch ends the stream there under four workers
/// as under one: every earlier batch, then the rejection, then nothing, even
/// though the two batches after it were built before it.
///
/// Prove-the-test: drop the `return` after sending a terminal outcome in
/// `DecodeCoordinator::run` (keep emitting ready batches) and the
/// four-worker run emits batches 7 and 8 after the rejection.
#[test]
fn a_terminal_outcome_follows_every_earlier_batch_and_nothing_after_it() {
    guarded(a_terminal_outcome_follows_every_earlier_batch_and_nothing_after_it_body());
}

async fn a_terminal_outcome_follows_every_earlier_batch_and_nothing_after_it_body() {
    let limits = LogIngestLimits::default();
    let (_dir, pq, m) = future_row_fixture(1_500, 1_000, &limits);
    // One cursor at 150 rows a batch puts row 1000 in batch 6 (rows
    // 900-1049), with three more batches after it.
    let decode = Decode {
        pq: &pq,
        mapping: &m,
        read_cursors: 1,
        batch_rows: 150,
        limits,
    };
    let serial = decode.run(1, None, None).await;

    let gate = Arc::new(Gate::default());
    let hold = |seq: u64, log: &BuildLog| {
        seq == 6 && !(log.delivered.contains(&7) && log.delivered.contains(&8))
    };
    let parallel = decode.run(4, Some(gated(&gate, hold)), None).await;

    assert_eq!(
        serial.len(),
        7,
        "six batches, then the rejection: {serial:?}"
    );
    assert!(
        matches!(serial.last(), Some(Emitted::Rejected { row: 1000, .. })),
        "{:?}",
        serial.last()
    );
    assert!(!gate.lock().timed_out, "batches 7 and 8 were built first");
    assert_eq!(parallel, serial);
}

/// Four workers run builds at once and one worker runs one at a time. The
/// four-worker run holds the first build to start until a second build has
/// started, so it counts two in flight without timing anything.
///
/// Prove-the-test: have `Builders::start` return `Builders::Inline` for every
/// worker count (the single-thread pipeline the dispatch ref ran) and the
/// four-worker case fails: the first build gives up on the hang guard with one
/// build in flight.
#[test]
fn four_decode_workers_run_builds_concurrently_and_one_does_not() {
    guarded(four_decode_workers_run_builds_concurrently_and_one_does_not_body());
}

async fn four_decode_workers_run_builds_concurrently_and_one_does_not_body() {
    let (_dir, pq, m) = uneven_row_group_fixture();
    let decode = Decode {
        pq: &pq,
        mapping: &m,
        read_cursors: 3,
        batch_rows: 250,
        limits: LogIngestLimits::default(),
    };

    let four = Arc::new(Gate::default());
    // Hold the first build to start until a second has started. `started` only
    // grows, so this never depends on which worker reaches its build first nor
    // on a build finishing: whichever two builds start first overlap, and both
    // are released the instant the second one starts.
    let hold = |_seq: u64, log: &BuildLog| log.started.len() < 2;
    decode.run(4, Some(gated(&four, hold)), None).await;
    {
        let log = four.lock();
        assert!(
            !log.timed_out,
            "a second build started while the first was held"
        );
        assert!(log.max_active >= 2, "max in flight {}", log.max_active);
    }

    let one = Arc::new(Gate::default());
    let batches = decode.run(1, Some(gated(&one, |_, _| false)), None).await;
    let log = one.lock();
    assert_eq!(log.max_active, 1, "one worker builds one batch at a time");
    assert_eq!(
        log.started.len(),
        batches.len() - 1,
        "every batch was built"
    );
}

/// Under a budget, the first batch builds alone and every later batch's
/// estimate is charged before its build starts: with batches 1-4 held at the
/// start of their builds, the budget already holds four estimates. Each
/// emitted batch then carries exactly its measured bytes, and the budget is
/// empty once the loader has dropped them all.
///
/// Prove-the-test: charge a batch only once it is built (skip the
/// `charges.insert` in `charge_for_build`) and the in-flight figure while
/// the builds are held is 0, not four estimates.
#[test]
fn every_batch_is_charged_before_its_build_starts() {
    guarded(every_batch_is_charged_before_its_build_starts_body());
}

async fn every_batch_is_charged_before_its_build_starts_body() {
    const BATCH_ROWS: usize = 100;
    let (_dir, pq, m) = uniform_rows_fixture(BATCH_ROWS, 8);
    let decode = Decode {
        pq: &pq,
        mapping: &m,
        read_cursors: 1,
        batch_rows: BATCH_ROWS,
        limits: LogIngestLimits::default(),
    };
    let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(1 << 30));
    let gate = Arc::new(Gate::default());
    let hold = |seq: u64, log: &BuildLog| seq >= 1 && !log.released;
    let (mut rx, handle) = decode.spawn(
        4,
        Some(gated(&gate, hold)),
        Some(BatchCharger::new(Arc::clone(&budget))),
    );

    let first = rx.recv().await.expect("batch 0");
    let Prefetched::Batch(Built::Columnar(batch, Some(charge))) = first else {
        panic!("a charged columnar batch");
    };
    assert_eq!(charge.bytes(), batch.heap_bytes() as u64);
    let estimate = (batch.heap_bytes() as u64).div_ceil(batch.num_rows as u64) * BATCH_ROWS as u64;
    drop((batch, charge));

    let gate_for_wait = Arc::clone(&gate);
    let held = tokio::task::spawn_blocking(move || {
        let log = gate_for_wait.wait_for(|log| log.active == 4);
        (log.active, log.started.clone(), log.delivered.clone())
    })
    .await
    .expect("wait joins");
    assert_eq!(held.0, 4, "four builds held: {held:?}");
    assert_eq!(
        held.1.first(),
        Some(&0),
        "batch 0 builds before the window: {held:?}"
    );
    // The workers pull the window's jobs FIFO, but which worker reaches its
    // `Started` notify first is a scheduling race, so the window is a set, not
    // an order: exactly seq 0 (built alone) plus the four that follow it.
    let mut started = held.1.clone();
    started.sort_unstable();
    assert_eq!(
        started,
        vec![0, 1, 2, 3, 4],
        "a window of four after batch 0"
    );
    assert_eq!(
        budget.in_flight_bytes(),
        4 * estimate,
        "the four held builds are charged their estimates"
    );
    gate.release();

    let rest = tokio::time::timeout(RUN_GUARD, async {
        let mut out = Vec::new();
        while let Some(p) = rx.recv().await {
            out.push(emitted(p, true));
        }
        out
    })
    .await
    .expect("the decode stage finishes");
    handle.await.expect("decoder task joins");
    let rows: usize = rest
        .iter()
        .map(|e| match e {
            Emitted::Batch { rows, .. } => *rows,
            _ => 0,
        })
        .sum();
    assert_eq!(rows, 7 * BATCH_ROWS, "the seven remaining batches' rows");
    assert_eq!(rest.last(), Some(&Emitted::Done));
    let log = gate.lock();
    assert!(!log.timed_out);
    assert_eq!(
        log.events[..2],
        [(0, BuildEvent::Started), (0, BuildEvent::Delivered)],
        "batch 0 builds alone"
    );
    assert_eq!(budget.in_flight_bytes(), 0, "every charge was returned");
}

/// Each batch's row count and measured bytes, in sequence order, from a
/// one-worker run.
async fn measured_sizes(decode: &Decode<'_>) -> Vec<(usize, u64)> {
    let (mut rx, handle) = decode.spawn(1, None, None);
    let sizes = tokio::time::timeout(RUN_GUARD, async {
        let mut sizes = Vec::new();
        while let Some(p) = rx.recv().await {
            if let Prefetched::Batch(Built::Columnar(batch, _)) = p {
                sizes.push((batch.num_rows, batch.heap_bytes() as u64));
            }
        }
        sizes
    })
    .await
    .expect("the decode stage finishes");
    handle.await.expect("decoder task joins");
    sizes
}

/// The estimate charged for a batch of `batch_rows` after a batch of
/// `measured` bytes over `batch_rows` rows.
fn estimate_after(measured: u64, batch_rows: usize) -> u64 {
    measured.div_ceil(batch_rows as u64) * batch_rows as u64
}

/// A first batch that under-states the rest leaves the oldest batch's
/// correction unable to fit while later batches hold estimates: batch 1
/// alone is larger than the budget. The stage drops the later batches'
/// built outcomes, refunds their charges only once they are gone, admits
/// batch 1 alone, rebuilds the dropped batches, and emits the serial
/// sequence.
///
/// Prove-the-test: drop the `self.discard_after(seq, builders)` call in
/// `DecodeCoordinator::settle` and batch 1's correction waits forever on the
/// estimates behind it; the run fails on the hang guard. The refund the
/// stage made before (`self.charges.clear()` with the built batches kept)
/// passes the sequence check and fails the rebuild assertion: batches 2 and
/// 3 start once.
#[test]
fn an_oldest_batch_that_outgrows_its_estimate_is_not_starved_by_later_ones() {
    guarded(an_oldest_batch_that_outgrows_its_estimate_is_not_starved_by_later_ones_body());
}

async fn an_oldest_batch_that_outgrows_its_estimate_is_not_starved_by_later_ones_body() {
    const BATCH_ROWS: usize = 100;
    let long_body = "x".repeat(2_000);
    let (_dir, pq, m) = bodied_rows_fixture(BATCH_ROWS, 8, "a", &long_body);
    let decode = Decode {
        pq: &pq,
        mapping: &m,
        read_cursors: 1,
        batch_rows: BATCH_ROWS,
        limits: LogIngestLimits::default(),
    };
    let serial = decode.run(1, None, None).await;

    // A budget of exactly four estimates admits the window of four, and
    // batch 1's measured bytes, far above its estimate, exceed it alone.
    let sizes: Vec<u64> = measured_sizes(&decode)
        .await
        .into_iter()
        .map(|(_, bytes)| bytes)
        .collect();
    let estimate = estimate_after(sizes[0], BATCH_ROWS);
    assert!(estimate > 0);
    assert!(sizes[1] > 4 * estimate, "batch 1 outgrows the budget alone");

    let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(4 * estimate));
    let gate = Arc::new(Gate::default());
    let budgeted = decode
        .run(
            4,
            Some(gated(&gate, |_, _| false)),
            Some(BatchCharger::new(Arc::clone(&budget))),
        )
        .await;
    assert_eq!(budgeted, serial);
    assert_eq!(budget.in_flight_bytes(), 0, "every charge was returned");
    // Batches 2 and 3 are always dispatched before batch 1 is emitted (two
    // estimates fit beside batch 0 and batch 1's), so both are rebuilt.
    let log = gate.lock();
    let starts = |seq| log.started.iter().filter(|&&s| s == seq).count();
    assert_eq!(
        (starts(2), starts(3)),
        (2, 2),
        "batches 2 and 3 were dropped and rebuilt: {:?}",
        log.started
    );
}

/// While the oldest batch outgrows its estimate, the budget's in-flight bytes
/// never fall below the measured bytes of the batches that exist: those the
/// test holds, and every later batch already built (held for re-sequencing,
/// waiting in the channel, or being corrected), leaving out only a batch
/// that outgrows its estimate (batch 1, and the zero-row end batch) until it
/// is emitted with its measured charge. Batch 1
/// fits beside three estimates but not beside batch 0 as well, so its
/// correction waits while the test holds batch 0, with batches 2-4 built
/// behind it. The test keeps every batch it receives and drops them only
/// when the stage waits on the budget, so it checks at each receipt and at
/// each wait.
///
/// Prove-the-test: restore the refund (in `DecodeCoordinator::settle`,
/// `if growth > 0 && !self.charges.is_empty() && !charger.fits_now(growth)
/// { self.charges.clear() }` in place of the `never_fits` discard) and the
/// check on receiving batch 1 fails: batches 3 and 4 are built and held
/// with no charge.
#[test]
fn held_batches_stay_charged_while_the_oldest_outgrows_its_estimate() {
    guarded(held_batches_stay_charged_while_the_oldest_outgrows_its_estimate_body());
}

async fn held_batches_stay_charged_while_the_oldest_outgrows_its_estimate_body() {
    const BATCH_ROWS: usize = 100;
    let long_body = "x".repeat(2_000);
    let mut bodies = vec!["a"; 8];
    bodies[1] = &long_body;
    let (_dir, pq, m) = bodies_fixture(BATCH_ROWS, &bodies);
    let decode = Decode {
        pq: &pq,
        mapping: &m,
        read_cursors: 1,
        batch_rows: BATCH_ROWS,
        limits: LogIngestLimits::default(),
    };
    let measured = measured_sizes(&decode).await;
    assert!(measured.len() >= 8, "every row group's batch: {measured:?}");
    let sizes: Vec<u64> = measured.iter().map(|&(_, bytes)| bytes).collect();
    let estimate = estimate_after(sizes[0], BATCH_ROWS);
    // A zero-row batch is estimated at zero bytes and measures its empty
    // buffers, so it too outgrows its estimate; like batch 1 it is counted
    // only once emitted.
    let outgrows: Vec<bool> = measured
        .iter()
        .enumerate()
        .map(|(seq, &(rows, _))| seq == 1 || rows == 0)
        .collect();
    for (seq, &(rows, size)) in measured.iter().enumerate() {
        if !outgrows[seq] {
            assert_eq!(rows, BATCH_ROWS, "batch {seq} is a full batch");
            // Every estimate is batch 0's per row or a larger one.
            assert!(size <= estimate, "batch {seq} fits its estimate: {size}");
        }
    }
    let limit = sizes[1] + 3 * estimate + estimate / 2;
    assert!(
        sizes[0] + 4 * estimate <= limit,
        "batch 0 and four estimates fit, so batches 1-4 are dispatched"
    );
    assert!(
        sizes[0] + sizes[1] + 3 * estimate > limit,
        "batch 1's correction does not fit beside batch 0"
    );

    let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(limit));
    let gate = Arc::new(Gate::default());
    let hold =
        |seq: u64, log: &BuildLog| seq == 1 && !(2..=4).all(|later| log.delivered.contains(&later));
    let (mut rx, handle) = decode.spawn(
        4,
        Some(gated(&gate, hold)),
        Some(BatchCharger::new(Arc::clone(&budget))),
    );

    let check = |held: &[(u64, Prefetched)], received: u64| {
        // Builds are charged before they start, so read the log first.
        let delivered = gate.lock().delivered.clone();
        let in_flight = budget.in_flight_bytes();
        let index = |seq: u64| usize::try_from(seq).expect("small seq");
        let held_bytes: u64 = held.iter().map(|(seq, _)| sizes[index(*seq)]).sum();
        let built_bytes: u64 = delivered
            .iter()
            .filter(|&&seq| seq >= received && !outgrows[index(seq)])
            .map(|&seq| sizes[index(seq)])
            .sum();
        assert!(
            in_flight >= held_bytes + built_bytes,
            "in flight {in_flight} covers the {held_bytes} bytes held by the test and \
             the {built_bytes} built behind them (received {received}, delivered \
             {delivered:?})"
        );
    };
    let (received, waited_on_batch_1) = tokio::time::timeout(RUN_GUARD, async {
        let mut held: Vec<(u64, Prefetched)> = Vec::new();
        let mut received = 0u64;
        let mut waited_on_batch_1 = false;
        loop {
            match rx.try_recv() {
                Ok(Prefetched::Done) => break,
                Ok(p @ Prefetched::Batch(_)) => {
                    held.push((received, p));
                    received += 1;
                    check(&held, received);
                }
                Ok(other) => panic!("unexpected outcome {:?}", emitted(other, true)),
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    panic!("the stage ended without Done")
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                    if budget.waiting() > 0 {
                        check(&held, received);
                        waited_on_batch_1 |= received == 1;
                        held.clear();
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        }
        (received, waited_on_batch_1)
    })
    .await
    .expect("the decode stage finishes");
    handle.await.expect("decoder task joins");
    assert_eq!(received, sizes.len() as u64, "every batch was emitted");
    assert!(
        waited_on_batch_1,
        "batch 1's correction waited while the test held batch 0"
    );
    assert!(!gate.lock().timed_out, "batches 2-4 were built first");
    assert_eq!(budget.in_flight_bytes(), 0, "every charge was returned");
}

/// A build that panics on a worker fails the load as a decode-task failure,
/// and the load returns instead of waiting on the build that never
/// answers. The third build to start panics, with four workers.
///
/// Prove-the-test: call `ctx.build(seq, &spans)` in `build_worker` without
/// the `catch_unwind` (wrapping its outcome in `Ok`) and the load never
/// returns: the coordinator waits on the dead worker's build while the other
/// workers wait on it for jobs, and the hang guard fails the test.
#[test]
fn a_build_worker_panic_fails_the_load_and_nothing_hangs() {
    guarded(a_build_worker_panic_fails_the_load_and_nothing_hangs_body());
}

async fn a_build_worker_panic_fails_the_load_and_nothing_hangs_body() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use ravel_object_store::memory::MemoryStore;

    let (_dir, pq, m) = uniform_rows_fixture(100, 8);
    let mut memory = LoadMemoryOptions::new(LoadMemoryRequest::Unbudgeted);
    memory.decode_workers = Some(4);
    let starts = Arc::new(AtomicUsize::new(0));
    let hook: BuildStartHook = {
        let starts = Arc::clone(&starts);
        Arc::new(move || {
            if starts.fetch_add(1, Ordering::SeqCst) == 2 {
                panic!("injected build panic");
            }
        })
    };
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let load_fut = load_instrumented_at(
        Arc::clone(&store),
        &pq,
        "acme",
        &m,
        1,
        100,
        0,
        Some(1),
        Some(1),
        DEFAULT_MAX_INFLIGHT_FLUSHES,
        DEFAULT_DECODE_QUEUE_BATCHES,
        Some(PER_BATCH_TARGET_BYTES),
        None,
        NOW_NS,
        TestClock::new(NOW_NS) as Arc<dyn Clock>,
        LoadPath::Columnar,
        Some(hook),
        None,
        RlogZstdLevel::default(),
        RlogGroupTargetBlocks::DEFAULT,
        RlogChunkOrder::DEFAULT,
        Some(memory),
        false,
        Arc::new(TenantRules::none()),
    );
    let result = tokio::time::timeout(RUN_GUARD, load_fut) // allow-wall-clock: hang guard only; an uncaught worker panic would otherwise hang the load
        .await
        .expect("the load returns");
    match result {
        Err(LoadError::BatchFailed { reason, .. }) => assert!(
            reason.starts_with("Parquet decode/build task failed")
                && reason.contains("injected build panic"),
            "the panic is the decode task's failure: {reason}"
        ),
        other => panic!("expected a decode-task failure: {other:?}"),
    }
    assert!(
        starts.load(Ordering::SeqCst) >= 3,
        "the third build started"
    );
}

/// The loader dropping its receiver while four builds are in flight ends the
/// decode stage once they finish: every worker exits, the task joins, and no
/// batch after the window is built.
///
/// Prove-the-test: in `build_worker`, `continue` instead of `return` when the
/// job queue is closed, and the workers never exit; the join fails on the
/// hang guard.
#[test]
fn dropping_the_receiver_mid_build_ends_every_worker() {
    guarded(dropping_the_receiver_mid_build_ends_every_worker_body());
}

async fn dropping_the_receiver_mid_build_ends_every_worker_body() {
    let (_dir, pq, m) = uniform_rows_fixture(100, 8);
    let decode = Decode {
        pq: &pq,
        mapping: &m,
        read_cursors: 1,
        batch_rows: 100,
        limits: LogIngestLimits::default(),
    };
    let gate = Arc::new(Gate::default());
    let hold = |seq: u64, log: &BuildLog| seq >= 1 && !log.released;
    let (mut rx, handle) = decode.spawn(4, Some(gated(&gate, hold)), None);
    assert!(
        matches!(rx.recv().await, Some(Prefetched::Batch(_))),
        "batch 0"
    );
    let gate_for_wait = Arc::clone(&gate);
    let active =
        tokio::task::spawn_blocking(move || gate_for_wait.wait_for(|log| log.active == 4).active)
            .await
            .expect("wait joins");
    assert_eq!(active, 4, "four builds in flight");

    drop(rx);
    gate.release();
    tokio::time::timeout(HANG_GUARD, handle)
        .await
        .expect("the decode stage ends once its receiver is gone")
        .expect("decoder task joins");
    let log = gate.lock();
    assert!(!log.timed_out);
    let mut started = log.started.clone();
    started.sort_unstable();
    assert_eq!(
        started,
        vec![0, 1, 2, 3, 4],
        "nothing after the window built"
    );
}

/// A row rejected mid-stream under a budget leaves nothing charged once the
/// stage has ended: the batches built after the rejection are refunded with
/// the coordinator, not leaked.
///
/// Prove-the-test: on the terminal `return` in `DecodeCoordinator::run`,
/// leak the outstanding estimates first
/// (`std::mem::forget(std::mem::take(&mut self.charges))`) and the final
/// in-flight figure is the leaked estimates of the batches built after the
/// rejection, not 0.
#[test]
fn a_row_rejection_under_a_budget_returns_every_charge() {
    guarded(a_row_rejection_under_a_budget_returns_every_charge_body());
}

async fn a_row_rejection_under_a_budget_returns_every_charge_body() {
    let limits = LogIngestLimits::default();
    let (_dir, pq, m) = future_row_fixture(1_500, 1_000, &limits);
    let decode = Decode {
        pq: &pq,
        mapping: &m,
        read_cursors: 1,
        batch_rows: 150,
        limits,
    };
    let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(1 << 30));
    let gate = Arc::new(Gate::default());
    let hold = |seq: u64, log: &BuildLog| {
        seq == 6 && !(log.delivered.contains(&7) && log.delivered.contains(&8))
    };
    let out = decode
        .run(
            4,
            Some(gated(&gate, hold)),
            Some(BatchCharger::new(Arc::clone(&budget))),
        )
        .await;
    assert_eq!(out.len(), 7, "six batches, then the rejection: {out:?}");
    assert!(
        matches!(out.last(), Some(Emitted::Rejected { row: 1000, .. })),
        "{:?}",
        out.last()
    );
    assert!(
        !gate.lock().timed_out,
        "batches 7 and 8 were built, and charged, before the rejection"
    );
    assert_eq!(budget.in_flight_bytes(), 0, "every charge was returned");
}
