//! Ravel maintenance: the L0-to-L1 compactor (phase 4 of
//! docs/compaction-retention-plan.md, issue #111).
//!
//! A compactor is a disposable process that rewrites a sealed ingest-hour
//! bucket's many small L0 segments into a handful of large RSEG v5 L1 parts,
//! without ever decoding a sample: it copies each series' TS/VAL/HIST pages
//! verbatim and lets query-time merge remain the single implementation of the
//! dedup total order (plan §7 "Dedup axis"). Object storage is the only
//! durable state; every step is idempotent and re-runnable from a bare LIST
//! (plan §3.6).
//!
//! # Pipeline (one bucket)
//!
//! 1. [`compact::compact_bucket`] gates on the seal rule and trigger
//!    ([`bucket::Bucket::is_sealed`], plan §3.2), then:
//! 2. [`read::list_bucket`] partitions the bucket listing by key shape and
//!    [`read::load_inputs`] decodes and canonically orders the L0 records,
//!    yielding a stable [`read::input_set_hash`];
//! 3. [`read::load_input_catalog`] decodes each input's catalog to per-run
//!    absolute page ranges (only metadata retained);
//! 4. [`build::build_parts`] groups every input catalog's series into one
//!    whole-bucket `BTreeMap` keyed by series id, then iterates it in id
//!    order, fetching and copying verbatim pages into size-capped v5 parts
//!    (a group-by-then-iterate; only the page bytes stream, and every
//!    completed part's bytes are retained until publish for convergence
//!    repair);
//! 5. [`publish::publish_record`] publishes the [`CompactionRecord`] with
//!    `CreateIfAbsent`, converging on any racing winner (plan §3.4).
//!
//! [`scan::scan_and_compact`] drives step 1 across a `(tenant, signal,
//! shard)`'s hours with the advisory CAS cursor.
//!
//! The clock is always injected ([`clock::Clock`]); this crate never reads
//! `SystemTime::now()`.
//!
//! [`CompactionRecord`]: ravel_proto::commit::v1::CompactionRecord

pub mod bucket;
pub mod build;
pub mod clock;
pub mod codec;
pub mod compact;
pub mod config;
pub mod error;
pub mod legal_hold;
pub mod publish;
pub mod read;
pub mod retention;
pub mod rlog;
pub mod rspan_codec;
pub mod scan;
pub mod sweep;

pub use bucket::Bucket;
pub use clock::{Clock, FixedClock};
pub use codec::{RsegCodec, SegmentCodec};
pub use compact::{CompactionOutcome, compact_bucket};
pub use config::{CompactorConfig, RetentionConfig, RetentionConfigError, RetentionPolicy};
pub use error::{MaintainError, Result};
pub use legal_hold::{AUDIT_HOLD_SHARD, LegalHoldCheck, write_hold_clear, write_hold_set};
pub use publish::PublishOutcome;
pub use retention::{RetentionOutcome, maintain_bucket, retention_sweep_bucket};
pub use rlog::RlogCodec;
pub use rspan_codec::SpanCodec;
pub use scan::{MaintainReport, ScanReport, scan_and_compact, scan_and_maintain};
pub use sweep::{
    LeaseCheck, NoLeases, SweepReport, sweep_orphans, sweep_shard, sweep_superseded,
    sweep_unreferenced_parts,
};
