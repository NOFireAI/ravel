//! The decode stage's build workers (issue #4210): batches built on W threads
//! reach the loader in the order one decode thread produces them, W builds
//! run at once, and every batch is charged to the load budget before it is
//! built.
//!
//! Every wait in this module is released by an event the test arranges.
//! [`HANG_GUARD`] and [`RUN_GUARD`] only turn a regression that never
//! releases into a failure instead of a hung run; no assertion depends on how
//! long anything takes.

use std::sync::{Condvar, Mutex, MutexGuard};
use std::time::Duration;

use ravel_ingest::IngestByteBudgetLimit;

use super::*;

const HANG_GUARD: Duration = Duration::from_secs(30);

/// The hang guard on a whole decode run, longer than [`HANG_GUARD`] so a
/// gated build that gives up is reported by the assertion on it.
const RUN_GUARD: Duration = Duration::from_secs(120);

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
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parallel_cursor_decode_yields_the_serial_batch_sequence() {
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
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_terminal_outcome_follows_every_earlier_batch_and_nothing_after_it() {
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
/// four-worker run holds batch 0's build until a second build has started,
/// so it counts two in flight without timing anything.
///
/// Prove-the-test: have `Builders::start` return `Builders::Inline` for every
/// worker count (the single-thread pipeline the dispatch ref ran) and the
/// four-worker case fails: batch 0 gives up on the hang guard with one build
/// in flight.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn four_decode_workers_run_builds_concurrently_and_one_does_not() {
    let (_dir, pq, m) = uneven_row_group_fixture();
    let decode = Decode {
        pq: &pq,
        mapping: &m,
        read_cursors: 3,
        batch_rows: 250,
        limits: LogIngestLimits::default(),
    };

    let four = Arc::new(Gate::default());
    let hold = |seq: u64, log: &BuildLog| seq == 0 && log.active < 2;
    decode.run(4, Some(gated(&four, hold)), None).await;
    let log = four.lock();
    assert!(
        !log.timed_out,
        "a second build started while batch 0 was held"
    );
    assert!(log.max_active >= 2, "max in flight {}", log.max_active);
    drop(log);

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
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_batch_is_charged_before_its_build_starts() {
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
        held.1,
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

/// A first batch that under-states the rest leaves the oldest batch's
/// correction unable to fit while later batches hold estimates. The stage
/// refunds those estimates rather than wait on charges only it can release,
/// charges each of those batches in full at its own emission, and emits the
/// serial sequence.
///
/// Prove-the-test: drop the `self.charges.clear()` in
/// `DecodeCoordinator::settle` and batch 1's correction waits forever on the
/// estimates behind it; the run fails on the hang guard.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_oldest_batch_that_outgrows_its_estimate_is_not_starved_by_later_ones() {
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

    // A probe run reads the first batch's estimate per batch; a budget of
    // exactly four estimates admits the window of four, and a later batch's
    // measured bytes, far above its estimate, cannot fit beside the others.
    let probe = IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited);
    let (mut rx, handle) = decode.spawn(1, None, Some(BatchCharger::new(Arc::clone(&probe))));
    let Some(Prefetched::Batch(Built::Columnar(first, _))) = rx.recv().await else {
        panic!("a columnar first batch");
    };
    let estimate = (first.heap_bytes() as u64).div_ceil(first.num_rows as u64) * BATCH_ROWS as u64;
    drop(rx);
    let _ = handle.await;
    assert!(estimate > 0);

    let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(4 * estimate));
    let budgeted = decode
        .run(4, None, Some(BatchCharger::new(Arc::clone(&budget))))
        .await;
    assert_eq!(budgeted, serial);
    assert_eq!(budget.in_flight_bytes(), 0, "every charge was returned");
}
