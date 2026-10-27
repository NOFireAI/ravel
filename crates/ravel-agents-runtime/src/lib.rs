//! Ravel Agents coordinator (ADR-3331 decisions 6 to 8).
//!
//! This crate will hold activation and ownership epochs, the worker protocol,
//! FIFO turn dispatch, the step and effect journal, timers, wake hints, the
//! authoritative reconciliation sweep, unloading and the history projection.
//!
//! Empty until task T9 of epic #3331 fills it. It will drive the transition
//! kernel in `ravel-agents-core` through `ravel-agents-store`.
