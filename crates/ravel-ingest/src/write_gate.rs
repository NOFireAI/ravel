//! The flush encode's place on the ADR-1702 write gate (decision 3).
//!
//! A router's shard actors exist from the router's construction, but the
//! server attaches the gate afterwards with `with_write_gate`, the same order
//! `with_budget` has. So every actor holds a clone of one [`WriteGateSlot`]
//! and reads it once per flush; a later generation's actors read the same
//! slot.

#[cfg(feature = "flush-input-bytes")]
use std::sync::Mutex;
use std::sync::{Arc, RwLock};

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
    fail_next: Arc<std::sync::Mutex<Option<CpuGateError>>>,
    /// Makes the next flush that splits its pages panic in one page job it
    /// dispatched to the gate.
    #[cfg(test)]
    page_job_panic: Arc<std::sync::Mutex<bool>>,
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
        #[cfg(feature = "flush-input-bytes")]
        self.record_input(input_bytes);
        let Some(gate) = self.get() else {
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
    /// writer's `finish_on`: the flush's own job holds one permit, and each
    /// further page job takes one more on the blocking pool.
    ///
    /// A page job the gate could not run fails the flush with that gate
    /// error, as the flush's own job failing on the gate does, and drops the
    /// result `job` built.
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
        let pages = self
            .get()
            .filter(|gate| !gate.runs_inline(JobSize::Bytes(input_bytes)))
            .map(|gate| GatedPageEncode {
                gate,
                runtime: tokio::runtime::Handle::current(),
                site,
                fault: None,
                #[cfg(test)]
                panic_job: self.take_page_job_panic(),
            });
        let encoded = self
            .encode(site, input_bytes, move || match pages {
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
    pub(crate) fn panic_next_page_job(&self) {
        *self
            .page_job_panic
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
    }

    #[cfg(test)]
    fn take_page_job_panic(&self) -> bool {
        std::mem::take(
            &mut *self
                .page_job_panic
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
    }
}

/// Runs a block's page encode jobs on the write gate, from inside a flush's
/// own gated job on the blocking pool.
///
/// Serial fallback: a block with one job, or one whose jobs are ready while
/// the gate has no free permit, is sealed on the flush's own thread under the
/// flush's own permit, so a flush never waits on the gate for its pages while
/// holding a permit no page job can use. Otherwise every job but the first
/// goes to the gate through [`WriteGate::run_permitted`], one permit each,
/// the first is sealed on the flush's thread meanwhile, and the flush then
/// waits for the rest. Results return in job order; the writer places them by
/// job index either way.
pub(crate) struct GatedPageEncode {
    gate: Arc<WriteGate>,
    runtime: tokio::runtime::Handle,
    site: WriteSite,
    /// The first gate error a page job returned.
    fault: Option<CpuGateError>,
    /// Makes the last dispatched page job of the next split block panic.
    #[cfg(test)]
    panic_job: bool,
}

impl PageEncodeExecutor for GatedPageEncode {
    fn execute(&mut self, jobs: Vec<PageEncodeJob>) -> Result<Vec<PageEncodeDone>, LogSegError> {
        if jobs.len() < 2 || self.gate.free_permits() == 0 {
            return Ok(jobs.into_iter().map(PageEncodeJob::run).collect());
        }
        #[cfg(test)]
        let panic_index = std::mem::take(&mut self.panic_job).then_some(jobs.len() - 1);
        let mut jobs = jobs.into_iter();
        let own = jobs.next();
        let dispatched: Vec<_> = jobs
            .map(|job| {
                let gate = Arc::clone(&self.gate);
                let site = self.site;
                #[cfg(test)]
                let panic = panic_index == Some(job.index());
                self.runtime.spawn(async move {
                    gate.run_permitted(site, move || {
                        #[cfg(test)]
                        assert!(!panic, "injected page job panic");
                        job.run()
                    })
                    .await
                })
            })
            .collect();
        let mut done: Vec<PageEncodeDone> = own.into_iter().map(PageEncodeJob::run).collect();
        let joined = self.runtime.block_on(futures::future::join_all(dispatched));
        for result in joined {
            let err = match result {
                Ok(Ok(page)) => {
                    done.push(page);
                    continue;
                }
                Ok(Err(err)) => err,
                Err(join) if join.is_panic() => CpuGateError::Panicked,
                Err(_) => CpuGateError::Cancelled,
            };
            self.fault.get_or_insert(err);
        }
        match self.fault {
            Some(err) => Err(LogSegError::PageEncode(format!("page job: {err}"))),
            None => Ok(done),
        }
    }
}
