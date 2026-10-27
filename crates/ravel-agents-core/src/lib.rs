//! Ravel Agents contracts (ADR-3331): the key layout under `t/<tenant>/ag/`,
//! identity hashing, the HEAD record model and its codecs, typed commands
//! and errors, the pure transition kernel, and the HTTP wire types.
//!
//! Nothing on `main` calls this crate yet. `ravel-agents-store` (task T7)
//! and `ravel-agents-runtime` (task T9) consume it, and task T3 wires it into
//! `ravel-server` behind the `agents` feature. Until then it is exercised
//! only by its own tests.
//!
//! The crate does no I/O: [`kernel::Kernel::apply`] is a function of
//! `(head, command, now_unix_ns, ids)` returning either a typed error or the
//! one root to compare-and-swap together with the effects to run after it.

pub mod command;
pub mod error;
pub mod head;
pub mod identity;
pub mod kernel;
pub mod keys;
pub mod objects;
pub mod state;
pub mod wire;

#[cfg(test)]
mod sim_crate;
