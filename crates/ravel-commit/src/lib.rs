//! Commit protocol: commit records, key layout, idempotent publication.
//!
//! Implementer contract: docs/catalog-and-mvcc.md, ADR-0002, ADR-0010.

pub mod erasure;
pub mod keys;
pub mod publish;
pub mod record;
pub mod rng;
pub mod signal;

pub use rng::{RngSource, SeededRng, SystemRng};
