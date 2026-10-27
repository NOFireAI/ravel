//! Deterministic simulation of Ravel Agents (ADR-3331 decision 13).
//!
//! Dev-only. This crate will hold the independent sequential reference
//! machine, the multi-worker step scheduler, and the deterministic drivers
//! that run the production store and runtime crates over `MemoryStore`,
//! `FaultStore` and the `ravel-sim` seams. ADR-0068's dev-only rule is not
//! enforced by cargo, so `ravel-agents-core`'s test
//! `sim_crate::no_workspace_crate_depends_on_the_sim_crate` checks that no
//! workspace crate depends on this one.
//!
//! Empty until task T8 of epic #3331 fills it.
