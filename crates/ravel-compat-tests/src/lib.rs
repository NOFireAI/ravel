//! Compatibility drills for the retained-data contract (ADR-4233 decision 9).
//!
//! This crate is excluded from the workspace and enables the segment crates'
//! test-only `format-drill` feature in its dev-dependencies only, so the
//! drill window never reaches a workspace build or a published binary. Its
//! tests run against an in-memory store with an injected clock; see
//! `tests/certify_flags_teardown_pair.rs`. The released-binary drill itself
//! is `scripts/compat/released-binary-drill.sh`.
