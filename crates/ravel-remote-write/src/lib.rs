//! Prometheus Remote Write ingest (ADR-0015, docs/ingest-breadth-plan.md
//! section 3, ticket A1).
//!
//! Phase 1 covers RW 1.0 only: snappy block decompression
//! ([`snappy`]), protobuf decode into the version-blind [`resolved`]
//! intermediate ([`rw1`]), and normalization to Ravel canonical points
//! ([`normalize`]). RW 2.0 decode (ticket A2) will add an `rw2` module that
//! produces the same [`resolved::ResolvedRequest`] shape, so [`normalize`]
//! never needs to change for it.

pub mod proto {
    #![allow(clippy::all)]
    pub mod prometheus {
        include!(concat!(env!("OUT_DIR"), "/prometheus.rs"));
    }
}

pub mod normalize;
pub mod resolved;
pub mod rw1;
pub mod snappy;

pub use normalize::{RwNormalizeOutput, RwRejection, normalize_resolved};
pub use resolved::{ResolvedRequest, ResolvedSample, ResolvedSeries};
pub use rw1::{Rw1DecodeError, decode_write_request};
pub use snappy::SnappyError;
