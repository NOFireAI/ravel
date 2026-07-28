//! Prometheus Remote Write ingest (ADR-0015, docs/ingest-breadth-plan.md
//! section 3, tickets A1 and A2).
//!
//! Two decoders converge on one version-blind [`resolved`] intermediate:
//! RW 1.0 ([`rw1`], inline string labels) and RW 2.0 ([`rw2`], a per-request
//! symbol table with every label, help, and unit string referenced by
//! index). Both sit on top of snappy block decompression ([`snappy`]).
//! [`normalize`] consumes only [`resolved::ResolvedRequest`], never a wire
//! message directly, so it never needs to change per protocol version.

pub mod proto {
    #![allow(clippy::all)]
    pub mod prometheus {
        include!(concat!(env!("OUT_DIR"), "/prometheus.rs"));
    }
    pub mod write_v2 {
        include!(concat!(env!("OUT_DIR"), "/io.prometheus.write.v2.rs"));
    }
}

pub mod normalize;
pub mod resolved;
pub mod rw1;
pub mod rw2;
pub mod snappy;

pub use normalize::{RwNormalizeOutput, RwRejection, normalize_resolved};
pub use resolved::{ResolvedRequest, ResolvedSample, ResolvedSeries};
pub use rw1::{Rw1DecodeError, decode_write_request};
pub use rw2::{Rw2DecodeError, decode_request};
pub use snappy::SnappyError;
