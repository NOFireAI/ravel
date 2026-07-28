//! P4 of docs/compaction-retention-plan.md: sealed-bucket L0-to-L1
//! compaction. Scans sealed (tenant, shard, hour) buckets, merges their L0
//! inputs into RSEG v4 parts via verbatim page copy, and publishes them
//! idempotently per plan section 3.4.
//!
//! Retention (age-based tombstones, P7), the resolver/differential proof
//! (P5), the L1 sweeper (P6), and service wiring/CLI (P8) are out of scope
//! for this crate.

mod clock;
mod config;
mod cursor;
mod error;
mod input;
mod scan;

pub use clock::{Clock, SystemClock};
pub use config::MaintainConfig;
pub use error::MaintainError;
pub use input::{CompactionInput, hash16, input_set_hash, sort_inputs_canonically};
pub use scan::{SealedBucket, scan_next};
