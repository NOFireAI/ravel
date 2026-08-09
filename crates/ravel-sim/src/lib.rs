//! Deterministic whole-system simulation harness (ADR-0068).
//!
//! Dev-only: never a dependency of any shipping crate. `cargo test -p
//! ravel-sim` is the entire reachability surface until epic #808 wave 4
//! wires a nightly seed batch into CI.
//!
//! This crate now covers the full ingest -> fold -> compact -> sweep ->
//! query cycle (ADR-0068 decisions 3-5): a seeded runtime and workload
//! generator, the `RngSource` seam for deterministic jitter/identity (#816),
//! a fault-schedule generator (#818 deliverable 1), a driver that drives
//! `ravel-maintain`'s real compaction and sweep entry points under the seeded
//! clock and injected faults (#818 deliverable 2), the invariants checked
//! every cycle (read-your-write, strict-ack-implies-durable, compaction query
//! equivalence, record-count conservation, and no orphan/unreferenced leaks
//! past the sweep horizon; #818 deliverable 3), and a reproducibility digest.
//! Any invariant violation prints the master seed and a one-command replay
//! line (#818 deliverable 4).

pub mod digest;
pub mod driver;
pub mod fault_plan;
pub mod seed;
pub mod workload;

pub use digest::{Digest, DigestBuilder};
pub use driver::{CycleConfig, CycleError, CycleOutcome, run_cycle};
pub use fault_plan::{FaultSchedule, FaultScheduleConfig, GateScript};
pub use seed::MasterSeed;
pub use workload::{CardinalityShape, WorkloadConfig};
