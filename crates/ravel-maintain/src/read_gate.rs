//! Maintenance codec work on the ADR-1702 read gate (decisions 3 and 4).
//!
//! The segment codecs stay synchronous; the gate is applied at their async
//! callers here. With no gate a job runs inline on the calling task, exactly as
//! it did before the gate existed. With one, [`ReadGate::run`] decides between
//! inline (below its floor, counted as inline) and the blocking pool (counted
//! as a job). A job owns everything it reads and returns everything its caller
//! keeps, so nothing it uses is released while it waits for a permit or after
//! its caller stops waiting (decision 6).

use std::fmt;
use std::sync::Arc;

use ravel_cpu_gate::{JobSize, ReadGate, ReadSite};

use crate::error::{MaintainError, Result};

/// The read gate maintenance submits to, or none.
#[derive(Clone, Default)]
pub struct MaintainReadGate(Option<Arc<ReadGate>>);

impl MaintainReadGate {
    /// Runs every gated maintenance unit on `gate`.
    pub fn new(gate: Arc<ReadGate>) -> Self {
        MaintainReadGate(Some(gate))
    }

    /// Runs `job` for `site`, sized by `bytes` against the gate's inline floor.
    /// A job the gate returns no result for fails with
    /// [`MaintainError::GateJob`].
    pub(crate) async fn run<F, R>(&self, site: ReadSite, bytes: u64, job: F) -> Result<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        match &self.0 {
            None => Ok(job()),
            Some(gate) => gate
                .run(site, JobSize::Bytes(bytes), job)
                .await
                .map_err(MaintainError::GateJob),
        }
    }
}

impl From<Option<Arc<ReadGate>>> for MaintainReadGate {
    fn from(gate: Option<Arc<ReadGate>>) -> Self {
        MaintainReadGate(gate)
    }
}

impl fmt::Debug for MaintainReadGate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            None => f.write_str("MaintainReadGate(None)"),
            Some(gate) => write!(f, "MaintainReadGate(permits={})", gate.permits()),
        }
    }
}
