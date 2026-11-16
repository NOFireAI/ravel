//! The flush encode's place on the ADR-1702 write gate (decision 3).
//!
//! A router's shard actors exist from the router's construction, but the
//! server attaches the gate afterwards with `with_write_gate`, the same order
//! `with_budget` has. So every actor holds a clone of one [`WriteGateSlot`]
//! and reads it once per flush; a later generation's actors read the same
//! slot.

use std::collections::VecDeque;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, mpsc};

use ravel_cpu_gate::{CpuGateError, JobSize, WriteGate, WriteSite};
use ravel_logseg::{LogSegError, PageEncodeDone, PageEncodeExecutor, PageEncodeJob};

/// The `Abandoned` message for a flush whose lifetime ended before its gated
/// encode returned.
///
/// The three flush tasks (`shard.rs`, `log_shard.rs`, `span_shard.rs`) end a
/// gated encode one of three ways, and each arm refers here:
///
/// - **Gate fault** (the job panicked or the runtime cancelled it): nothing
///   was written, and the job's charges dropped with its result. The fault is
///   the server's, not the input's, so the waiters get the retryable
///   `ShardUnavailable` an encode panic gave when it took the actor down. The
///   flush counts on `flush_gate_faults` and in no `abandoned_*` counter.
/// - **Deadline** (the flush lifetime elapsed first): the job's charges drop
///   with it, now if it never started, when it returns if it did. The waiters
///   get `Abandoned` with this message, counted on `abandoned_queue_deadline`.
/// - **Result**: the flush holds the returned charges to its terminal outcome.
pub(crate) const GATE_DEADLINE_MESSAGE: &str =
    "flush lifetime elapsed before its write gate encode returned";

/// The input bytes of every flush encode a router ran, in completion order
/// across its shards: the figure each encode was sized by against the write
/// gate's inline floor. Attached with
/// [`crate::IngestRouter::with_flush_input_bytes`] by a bench; clones share
/// one list, which grows by one entry per flush until [`Self::take`] empties
/// it. Compiled only with the `flush-input-bytes` feature.
#[cfg(feature = "flush-input-bytes")]
#[derive(Clone, Default)]
pub struct FlushInputBytes {
    sizes: Arc<Mutex<Vec<u64>>>,
}

#[cfg(feature = "flush-input-bytes")]
impl FlushInputBytes {
    fn record(&self, input_bytes: u64) {
        self.sizes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(input_bytes);
    }

    /// Every size recorded since the last call, emptying the list.
    pub fn take(&self) -> Vec<u64> {
        std::mem::take(
            &mut *self
                .sizes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
    }
}

/// The write gate a router's flushes encode on, or none.
#[derive(Clone, Default)]
pub(crate) struct WriteGateSlot {
    gate: Arc<RwLock<Option<Arc<WriteGate>>>>,
    #[cfg(feature = "flush-input-bytes")]
    input_recorder: Arc<RwLock<Option<FlushInputBytes>>>,
    /// Makes the next gated encode fail with this error, so a test can reach
    /// the flush's gate-error arm: `Panicked` panics inside the job on the
    /// gate, anything else is returned without running the job, as the gate
    /// does for a job the runtime cancelled.
    #[cfg(test)]
    fail_next: Arc<Mutex<Option<CpuGateError>>>,
    #[cfg(test)]
    page_probe: Arc<PageProbe>,
}

/// What the gated page encodes of one slot did, and switches that steer them,
/// for tests.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct PageProbe {
    /// Page jobs a flush thread sealed itself, the serial path's included.
    pub(crate) flush_sealed: AtomicU64,
    /// Page jobs a page task claimed under its own permit. Each is one
    /// `log_flush` job on the gate; a page task that claimed nothing is none.
    pub(crate) task_claimed: AtomicU64,
    /// Makes the next page job a page task claims panic.
    pub(crate) panic_next: AtomicBool,
    /// Makes the flush thread of every split block wait for one page task's
    /// result before it takes a job itself, so at least one page job per
    /// split block is sealed by a page task.
    pub(crate) task_first: AtomicBool,
    /// Runs once, on the flush thread of the next block that spawns page
    /// tasks, after it decided how many and before it spawns them.
    #[allow(clippy::type_complexity)]
    pub(crate) before_page_tasks: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl WriteGateSlot {
    pub(crate) fn set(&self, gate: Arc<WriteGate>) {
        *self
            .gate
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(gate);
    }

    fn get(&self) -> Option<Arc<WriteGate>> {
        self.gate
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    #[cfg(feature = "flush-input-bytes")]
    pub(crate) fn set_input_recorder(&self, recorder: FlushInputBytes) {
        *self
            .input_recorder
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(recorder);
    }

    #[cfg(feature = "flush-input-bytes")]
    fn record_input(&self, input_bytes: u64) {
        if let Some(recorder) = self
            .input_recorder
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
        {
            recorder.record(input_bytes);
        }
    }

    #[cfg(test)]
    pub(crate) fn fail_next_encode(&self, err: CpuGateError) {
        *self
            .fail_next
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(err);
    }

    /// Runs one flush's encode for `site`, sized by the bytes the flush
    /// buffered so the gate's inline floor applies (decision 4). With no gate
    /// attached `job` runs inline on the flush task, as it always has, and
    /// cannot fail.
    ///
    /// `job` owns everything it encodes and returns everything the flush
    /// still needs, the byte charges included, so a flush task dropped while
    /// its job waits or runs releases nothing the job is still using
    /// (decision 6).
    pub(crate) async fn encode<F, R>(
        &self,
        site: WriteSite,
        input_bytes: u64,
        job: F,
    ) -> Result<R, CpuGateError>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.encode_on(self.get(), site, input_bytes, job).await
    }

    /// [`Self::encode`] on the gate the caller read from the slot.
    async fn encode_on<F, R>(
        &self,
        gate: Option<Arc<WriteGate>>,
        site: WriteSite,
        input_bytes: u64,
        job: F,
    ) -> Result<R, CpuGateError>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        #[cfg(feature = "flush-input-bytes")]
        self.record_input(input_bytes);
        let Some(gate) = gate else {
            return Ok(job());
        };
        #[cfg(test)]
        let job = {
            let injected = self
                .fail_next
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take();
            let panic = match injected {
                None => false,
                Some(CpuGateError::Panicked) => true,
                Some(err) => return Err(err),
            };
            move || {
                assert!(!panic, "injected flush encode panic");
                job()
            }
        };
        gate.run(site, JobSize::Bytes(input_bytes), job).await
    }

    /// [`Self::encode`] for an encode that can split its column pages into
    /// jobs (ADR-3492 decision 7; ADR-1702, the parallel page encode
    /// amendment). `job` gets `None` when the flush runs inline (no gate, or
    /// `input_bytes` below the gate's floor) and seals its pages serially, as
    /// before. At or above the floor it gets a [`GatedPageEncode`] for the
    /// writer's `finish_on`: the flush's own job holds one permit and seals
    /// page jobs under it, and page tasks that get a permit of their own seal
    /// the rest.
    ///
    /// A page job the gate could not run fails the flush with that gate
    /// error, as the flush's own job failing on the gate does, and drops the
    /// result `job` built. The slot is read once per flush, for both the
    /// split decision and the flush's own job.
    pub(crate) async fn encode_pages<F, R>(
        &self,
        site: WriteSite,
        input_bytes: u64,
        job: F,
    ) -> Result<R, CpuGateError>
    where
        F: FnOnce(Option<&mut dyn PageEncodeExecutor>) -> R + Send + 'static,
        R: Send + 'static,
    {
        let gate = self.get();
        let pages = gate
            .as_ref()
            .filter(|gate| !gate.runs_inline(JobSize::Bytes(input_bytes)))
            .map(|gate| GatedPageEncode {
                gate: Arc::clone(gate),
                runtime: tokio::runtime::Handle::current(),
                site,
                fault: None,
                #[cfg(test)]
                probe: Arc::clone(&self.page_probe),
            });
        let encoded = self
            .encode_on(gate, site, input_bytes, move || match pages {
                None => Ok(job(None)),
                Some(mut pages) => {
                    let out = job(Some(&mut pages));
                    match pages.fault {
                        Some(err) => Err(err),
                        None => Ok(out),
                    }
                }
            })
            .await;
        encoded.and_then(|out| out)
    }

    #[cfg(test)]
    pub(crate) fn page_probe(&self) -> Arc<PageProbe> {
        Arc::clone(&self.page_probe)
    }
}

#[cfg(test)]
impl PageProbe {
    fn run_before_page_tasks(&self) {
        let hook = self
            .before_page_tasks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(hook) = hook {
            hook();
        }
    }

    /// Counts one page task's claim; true when that job must panic.
    fn claim(&self) -> bool {
        self.task_claimed.fetch_add(1, Ordering::SeqCst);
        self.panic_next.swap(false, Ordering::SeqCst)
    }
}

fn next_job(queue: &Mutex<VecDeque<PageEncodeJob>>) -> Option<PageEncodeJob> {
    queue
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .pop_front()
}

/// Runs a block's page encode jobs on the write gate, from inside a flush's
/// own gated job on the blocking pool, which holds one permit.
///
/// The block's jobs go into one queue. The flush thread takes jobs from it
/// and seals them under the flush's own permit until the queue is empty.
/// Beside it, one page task per further job, capped at the gate's free
/// permits when the block's jobs are ready, waits on the gate through
/// [`WriteGate::run_claimed`]: a page task that gets a permit takes the next
/// job from the queue and seals it on the blocking pool, and one that finds
/// the queue empty releases its permit at once without work. A block with one
/// job, or with no free permit, spawns no page task and the flush thread
/// seals every job. Once the queue is empty the flush thread waits only for
/// the jobs page tasks already took, each running under that task's own
/// permit: the flush makes progress on its own block under its own permit,
/// so it never waits for a permit, and no job waits for a permit while the
/// flush thread could seal it. Results return in completion order; the writer places
/// them by job index.
///
/// At the gate's site the flush is one job and each page task that took a
/// job is one more; a page task that found the queue empty counts nothing.
pub(crate) struct GatedPageEncode {
    gate: Arc<WriteGate>,
    runtime: tokio::runtime::Handle,
    site: WriteSite,
    /// The first gate error a page job returned.
    fault: Option<CpuGateError>,
    #[cfg(test)]
    probe: Arc<PageProbe>,
}

impl GatedPageEncode {
    fn take(
        &mut self,
        sealed: Result<PageEncodeDone, CpuGateError>,
        done: &mut Vec<PageEncodeDone>,
    ) {
        match sealed {
            Ok(page) => done.push(page),
            Err(err) => {
                self.fault.get_or_insert(err);
            }
        }
    }

    /// With [`PageProbe::task_first`] set, waits for one page task's result
    /// and returns how many it took (0 or 1).
    #[cfg(test)]
    fn take_task_first(
        &mut self,
        sealed_rx: &mpsc::Receiver<Result<PageEncodeDone, CpuGateError>>,
        done: &mut Vec<PageEncodeDone>,
    ) -> usize {
        if !self.probe.task_first.load(Ordering::SeqCst) {
            return 0;
        }
        match sealed_rx.recv() {
            Ok(sealed) => {
                self.take(sealed, done);
                1
            }
            Err(_) => 0,
        }
    }
}

impl PageEncodeExecutor for GatedPageEncode {
    fn execute(&mut self, jobs: Vec<PageEncodeJob>) -> Result<Vec<PageEncodeDone>, LogSegError> {
        let total = jobs.len();
        let page_tasks = total.saturating_sub(1).min(self.gate.free_permits());
        if page_tasks == 0 {
            #[cfg(test)]
            self.probe
                .flush_sealed
                .fetch_add(total as u64, Ordering::SeqCst);
            return Ok(jobs.into_iter().map(PageEncodeJob::run).collect());
        }
        #[cfg(test)]
        self.probe.run_before_page_tasks();
        let queue = Arc::new(Mutex::new(VecDeque::from(jobs)));
        let (sealed_tx, sealed_rx) = mpsc::channel();
        for _ in 0..page_tasks {
            let gate = Arc::clone(&self.gate);
            let queue = Arc::clone(&queue);
            let sealed_tx = sealed_tx.clone();
            let site = self.site;
            #[cfg(test)]
            let probe = Arc::clone(&self.probe);
            self.runtime.spawn(async move {
                let mut took = false;
                let sealed = gate
                    .run_claimed(site, || {
                        let job = next_job(&queue)?;
                        took = true;
                        #[cfg(test)]
                        let panic = probe.claim();
                        Some(move || {
                            #[cfg(test)]
                            assert!(!panic, "injected page job panic");
                            job.run()
                        })
                    })
                    .await;
                if took {
                    // A taken job always comes back as `Some` or an error.
                    let sealed = sealed.and_then(|page| page.ok_or(CpuGateError::Cancelled));
                    let _ = sealed_tx.send(sealed);
                }
            });
        }
        drop(sealed_tx);

        let mut done = Vec::with_capacity(total);
        #[cfg(test)]
        let received = self.take_task_first(&sealed_rx, &mut done);
        #[cfg(not(test))]
        let received = 0;
        let mut sealed_here = 0;
        while let Some(job) = next_job(&queue) {
            done.push(job.run());
            sealed_here += 1;
        }
        #[cfg(test)]
        self.probe
            .flush_sealed
            .fetch_add(sealed_here as u64, Ordering::SeqCst);
        for _ in received + sealed_here..total {
            match sealed_rx.recv() {
                Ok(sealed) => self.take(sealed, &mut done),
                Err(_) => {
                    // Every page task is gone with a taken job unreported:
                    // the runtime dropped it while shutting down.
                    self.fault.get_or_insert(CpuGateError::Cancelled);
                    break;
                }
            }
        }
        match self.fault {
            Some(err) => Err(LogSegError::PageEncode(format!("page job: {err}"))),
            None => Ok(done),
        }
    }
}
