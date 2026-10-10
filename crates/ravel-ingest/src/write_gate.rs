//! The flush encode's place on the ADR-1702 write gate (decision 3).
//!
//! A router's shard actors exist from the router's construction, but the
//! server attaches the gate afterwards with `with_write_gate`, the same order
//! `with_budget` has. So every actor holds a clone of one [`WriteGateSlot`]
//! and reads it once per flush; a later generation's actors read the same
//! slot.

use std::sync::{Arc, RwLock};

use ravel_cpu_gate::{CpuGateError, JobSize, WriteGate, WriteSite};

/// The write gate a router's flushes encode on, or none.
#[derive(Clone, Default)]
pub(crate) struct WriteGateSlot {
    gate: Arc<RwLock<Option<Arc<WriteGate>>>>,
    /// Makes the next gated encode panic on the gate, so a test can reach the
    /// flush's gate-error arm.
    #[cfg(test)]
    panic_next: Arc<std::sync::atomic::AtomicBool>,
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

    #[cfg(test)]
    pub(crate) fn panic_next_encode(&self) {
        self.panic_next
            .store(true, std::sync::atomic::Ordering::SeqCst);
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
        let Some(gate) = self.get() else {
            return Ok(job());
        };
        #[cfg(test)]
        let job = {
            let panic = self
                .panic_next
                .swap(false, std::sync::atomic::Ordering::SeqCst);
            move || {
                assert!(!panic, "injected flush encode panic");
                job()
            }
        };
        gate.run(site, JobSize::Bytes(input_bytes), job).await
    }
}
