//! Deterministic simulation of Ravel Agents (ADR-3331 decision 13).
//!
//! Dev-only: nothing under `crates/` or `services/` may depend on this
//! crate. ADR-0068's dev-only rule is not enforced by cargo, so
//! `ravel-agents-core`'s test
//! `sim_crate::no_workspace_crate_depends_on_the_sim_crate` checks it.
//!
//! What it holds:
//!
//! - [`reference`]: the independent sequential reference machine, written
//!   from the ADR's text rather than from the kernel, which answers the
//!   reply a client or worker should observe for each abstract event.
//! - [`history`]: recorded client histories and the checker that replays
//!   one through the reference machine and reports the first divergence.
//! - [`scheduler`]: a step scheduler that runs worker and client tasks on a
//!   paused `current_thread` runtime, holds every store call at a
//!   `FaultStore` gate, and releases or crashes them in a seed-derived order.
//! - [`seeds`]: the agent seed domains split from one `MasterSeed`, and the
//!   seeded `RngSource` for runtime ids.
//!
//! Task T13 (#3346) adds the drivers that run the production store and
//! runtime crates through the scheduler and check their recorded histories
//! here. Until then this crate's own tests are its only caller.

pub mod drive;
pub mod history;
pub mod reference;
pub mod scheduler;
#[cfg(test)]
mod schedules;
pub mod seeds;
