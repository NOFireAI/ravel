//! Ingest pipeline: shard router, single-threaded shard actors, adaptive
//! flush, L0 build + upload + commit, strict/buffered acknowledgement.
//!
//! Implementer contract: docs/architecture.md, ADR-0001, ADR-0002. Bounded
//! mpsc everywhere; no task-per-sample; backpressure to the gateway.
