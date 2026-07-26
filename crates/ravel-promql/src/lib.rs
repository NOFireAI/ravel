//! PromQL front end and evaluator (ADR-0007).
//!
//! Phase 1 scope: instant and range queries over vector selectors (all
//! matcher types, offset), 5m lookback, staleness-aware iteration. The
//! evaluator consumes a storage-agnostic series stream trait.
