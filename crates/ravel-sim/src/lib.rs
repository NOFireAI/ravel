//! Deterministic whole-system simulation harness (ADR-0068).
//!
//! Dev-only: never a dependency of any shipping crate. `cargo test -p
//! ravel-sim` is the entire reachability surface until epic #808 wave 4
//! wires a nightly seed batch into CI.
//!
//! This crate covers deliverables 1, 3, and the ingest -> fold -> query
//! slice of 5 and 6 from the ADR: a seeded runtime and workload generator,
//! a single-cycle driver with the strict-ack-implies-durable and
//! read-your-write invariants, and a reproducibility digest. The
//! `RngSource` seam (production jitter/identity determinism, deliverable 2)
//! and the fault-schedule generator (deliverable 4) are later tasks (#816,
//! #818); this crate's driver runs one clean cycle with no injected faults.

pub mod digest;
pub mod driver;
pub mod seed;
pub mod workload;

pub use digest::{Digest, DigestBuilder};
pub use driver::{CycleConfig, CycleError, CycleOutcome, run_cycle};
pub use seed::MasterSeed;
pub use workload::{CardinalityShape, WorkloadConfig};
