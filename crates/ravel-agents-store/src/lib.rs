//! Ravel Agents publication skeleton (ADR-3331 decision 4).
//!
//! This crate will hold the one path every durable agent command takes over
//! `ObjectStoreBackend`: read HEAD and keep its version, resolve the
//! operation id against the receipt index, validate preconditions, store the
//! immutable dependencies with `CreateIfAbsent`, and CAS HEAD. It will also
//! own the logical quotas and the per-phase request counters.
//!
//! Empty until task T7 of epic #3331 fills it. The keys, codecs and
//! transition kernel it will drive are in `ravel-agents-core`.
