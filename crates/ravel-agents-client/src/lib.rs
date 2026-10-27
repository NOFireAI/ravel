//! Rust HTTP client for the Ravel Agents API (ADR-3331 decisions 1 and 11).
//!
//! This crate will cover create, send, call, result, resolve, cancel, state
//! and events over the routes under `/api/v1/agents/`, using the wire types
//! in `ravel_agents_core::wire`. It depends on neither the store nor the
//! runtime crate, so a client application does not build the coordinator.
//!
//! Empty until task T21 of epic #3331 fills it.
