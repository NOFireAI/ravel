//! OTLP decode and normalization into Ravel canonical metric batches.
//!
//! Phase 1 scope: gauge and sum NumberDataPoints. Resource attributes flatten
//! into labels per the standard Prometheus mapping; see ADR-0005 note.
