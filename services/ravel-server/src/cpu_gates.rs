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
    /// Both gates with the resolved permit counts, the read gate at the
    /// default inline floors and the write gate at the resolved byte floor,
    /// measuring with one monotonic clock.
    pub fn new(permits: CpuGatePermits) -> Self {
        let clock: Arc<dyn MonotonicClock> = Arc::new(InstantClock::new());
        CpuGates {
            read: Arc::new(ReadGate::new(
                CpuGateConfig::with_permits(permits.read),
                Arc::clone(&clock),
            )),
            write: Arc::new(WriteGate::new(
                CpuGateConfig {
                    inline_floor_bytes: permits.write_inline_floor_bytes,
                    ..CpuGateConfig::with_permits(permits.write)
                },
                clock,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_floor_reaches_only_the_write_gate() {
        let stock = CpuGates::new(CpuGatePermits::default());
        assert_eq!(
            stock.write.inline_floor_bytes(),
            ravel_cpu_gate::DEFAULT_INLINE_FLOOR_BYTES
        );
        let raised = CpuGates::new(CpuGatePermits {
            write_inline_floor_bytes: 1 << 20,
            ..CpuGatePermits::default()
        });
        assert_eq!(raised.write.inline_floor_bytes(), 1 << 20);
        assert_eq!(
            raised.read.inline_floor_bytes(),
            ravel_cpu_gate::DEFAULT_INLINE_FLOOR_BYTES
        );
    }
}
