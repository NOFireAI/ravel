//! The process's two ADR-1702 CPU gates (decision 3): a read gate for query,
//! catalog and maintenance decode, and a separate write gate for flush encode
//! and ingest payload decode, so a burst of wide scans cannot delay the
//! flushes acknowledgements wait on.

use std::sync::Arc;

use ravel_cpu_gate::{CpuGateConfig, InstantClock, MonotonicClock, ReadGate, WriteGate};

use crate::config::CpuGatePermits;

/// Both gates, built once in [`crate::start`] and shared by `Arc`.
#[derive(Clone)]
pub struct CpuGates {
    pub read: Arc<ReadGate>,
    pub write: Arc<WriteGate>,
}

impl CpuGates {
    /// Both gates with the resolved permit counts and the default inline
    /// floors, measuring with one monotonic clock.
    pub fn new(permits: CpuGatePermits) -> Self {
        let clock: Arc<dyn MonotonicClock> = Arc::new(InstantClock::new());
        CpuGates {
            read: Arc::new(ReadGate::new(
                CpuGateConfig::with_permits(permits.read),
                Arc::clone(&clock),
            )),
            write: Arc::new(WriteGate::new(
                CpuGateConfig::with_permits(permits.write),
                clock,
            )),
        }
    }
}
