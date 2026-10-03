//! Read CPU gate fixtures shared by the fetcher and engine tests (ADR-1702).

use std::sync::Arc;

use ravel_cpu_gate::{CpuGateConfig, InstantClock, ReadGate, ReadSite};

/// A read gate whose byte floor is 0, so every decode unit is a gate job, with
/// PromQL evaluations at or above `eval_floor_samples` gated as well.
pub(crate) fn gate_with_eval_floor(eval_floor_samples: u64) -> Arc<ReadGate> {
    Arc::new(ReadGate::new(
        CpuGateConfig {
            permits: 2,
            inline_floor_bytes: 0,
            eval_floor_samples,
        },
        Arc::new(InstantClock::new()),
    ))
}

/// A read gate with both floors at 0.
pub(crate) fn floor_zero_gate() -> Arc<ReadGate> {
    gate_with_eval_floor(0)
}

/// `(jobs, inline)` for `site` on `gate`.
pub(crate) fn site_counts(gate: &ReadGate, site: ReadSite) -> (u64, u64) {
    gate.snapshot()
        .sites
        .iter()
        .find(|counts| counts.site == site)
        .map_or((0, 0), |counts| (counts.jobs, counts.inline))
}

/// The inline count summed over every site of `gate`.
pub(crate) fn total_inline(gate: &ReadGate) -> u64 {
    gate.snapshot()
        .sites
        .iter()
        .map(|counts| counts.inline)
        .sum()
}
