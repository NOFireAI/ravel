//! Compatibility drills for the retained-data contract (ADR-4233 decision 9).
//!
//! This crate is excluded from the workspace and enables the segment crates'
//! test-only `format-drill` feature in its dev-dependencies only, so the
//! drill window never reaches a workspace build or a published binary. Its
//! examples run against an isolated drill store; see
//! `examples/drill_census.rs` and `scripts/compat/released-binary-drill.sh`.
