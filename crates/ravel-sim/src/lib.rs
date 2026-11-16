//! Deterministic whole-system simulation harness (ADR-0068).
//!
//! Dev-only: never a dependency of any shipping crate. `cargo test -p
//! ravel-sim` is the entire reachability surface.
//!
//! This crate covers the full ingest -> fold -> compact -> sweep ->
//! query cycle (ADR-0068 decisions 3-5): a seeded runtime and workload
//! generator, the `RngSource` seam for deterministic jitter/identity,
//! a fault-schedule generator, a driver that drives `ravel-maintain`'s real
//! compaction and sweep entry points under the seeded clock and injected
//! faults, the invariants checked every cycle (read-your-write,
//! strict-ack-implies-durable, compaction query equivalence, record-count
//! conservation, and no orphan/unreferenced leaks past the sweep horizon),
//! and a reproducibility digest. Any invariant violation prints the master
//! seed and a one-command replay line.

pub mod digest;
pub mod driver;
pub mod fault_plan;
pub mod seed;
pub mod workload;

pub use digest::{Digest, DigestBuilder};
pub use driver::{
    CountingGateHandle, CycleConfig, CycleError, CycleOutcome, GateRelease, run_cycle,
};
pub use fault_plan::{FaultSchedule, FaultScheduleConfig, GateScript};
pub use seed::MasterSeed;
pub use workload::{CardinalityShape, WorkloadConfig};
