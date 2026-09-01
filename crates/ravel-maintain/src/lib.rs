//! Ravel maintenance: the L0-to-L1 compactor.
//!
//! A compactor is a disposable process that rewrites a sealed ingest-hour
//! bucket's many small L0 segments into a handful of large RSEG v5 L1 parts,
//! without ever decoding a sample: it copies each series' TS/VAL/HIST pages
//! verbatim and lets query-time merge remain the single implementation of the
//! dedup total order. Object storage is the only
//! durable state; every step is idempotent and re-runnable from a bare LIST.
//!
//! # Pipeline (one bucket)
//!
//! 1. [`compact::compact_bucket`] gates on the seal rule and trigger
//!    ([`bucket::Bucket::is_sealed`]), then:
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
//!    `CreateIfAbsent`, converging on any racing winner.
//!
//! [`scan::scan_and_compact`] drives step 1 across a `(tenant, signal,
//! shard)`'s hours with the advisory CAS cursor.
//!
//! The clock is always injected ([`clock::Clock`]); this crate never reads
//! `SystemTime::now()`.
//!
//! [`CompactionRecord`]: ravel_proto::commit::v1::CompactionRecord

pub mod audit_pipeline;
pub mod audit_retention;
mod audit_write;
pub mod bucket;
pub mod build;
pub mod clock;
pub mod codec;
pub mod compact;
pub mod config;
pub mod discover;
pub mod erasure_rewrite;
pub mod error;
pub mod gc_config;
pub mod legal_hold;
pub mod memo_snapshot;
pub mod migrate;
pub mod provision_audit;
pub mod publish;
pub mod query_audit;
pub mod read;
pub mod retention;
pub mod rewrite;
pub mod rlog;
pub mod rspan_codec;
pub mod scan;
pub mod scrub;
pub mod sweep;
/// Worker membership and rendezvous-hash work partitioning (ADR-0065 decisions
/// 1 and 2). Relocated to the `ravel-fleet` crate (ADR-0071) and re-exported
/// here at its original path, so maintain's callers compile unchanged.
pub use ravel_fleet::worker_set;

pub use audit_pipeline::{AuditEvent, AuditPipeline, NoopQueryAuditSink, QueryAuditSink};
pub use audit_retention::{AuditRetentionOutcome, sweep_audit_retention};
pub use bucket::Bucket;
pub use clock::{Clock, FixedClock};
pub use codec::{RsegCodec, SegmentCodec};
pub use compact::{CompactionOutcome, compact_bucket};
pub use config::{
    AdmissionMode, AuditMode, AuditPipelineConfig, CompactorConfig, MergeMemoryTracker,
    MergePhasePeaks, RetentionConfig, RetentionConfigError, RetentionPolicy,
};
pub use discover::discover_tenants;
pub use erasure_rewrite::{
    ApplicableRequest, BucketErasureCompletion, ErasureMatcher, ErasureRewriteOutcome,
    PendingErasureRequest, RewriteBuild, RewriteSupersession, bucket_erasure_completion,
    bucket_may_overlap, build_rewrite, erasure_rewrite_bucket, pending_erasure_requests,
    publish_rewrite_record,
};
pub use error::{MaintainError, Result};
pub use gc_config::{
    GC_CONFIG_KEY, GcConfigError, GcConfigValues, SetOutcome, bootstrap_gc_config, read_gc_config,
    set_gc_config, validate_flight_ceiling, validate_maintain, validate_maintain_skew,
    validate_query_deadline,
};
pub use legal_hold::{
    AUDIT_HOLD_SHARD, LegalHoldCheck, shard_hold_scopes, write_hold_clear, write_hold_set,
};
pub use memo_snapshot::{MEMO_PREFIX, memo_key, read_all_memo_snapshots, write_memo_snapshot};
pub use migrate::{
    FamilyMigrateReport, MigrateBudget, Verification, count_below_target, migrate_family,
};
pub use provision_audit::write_reshard_audit;
pub use publish::{
    ConservationPredicate, PublishOutcome, conserve_exact, publish_record,
    publish_record_with_conservation,
};
pub use query_audit::{QUERY_AUDIT_SHARD, QueryStatus, query_audit_event, write_query_audit};
pub use ravel_fleet::worker_set::{WorkerSet, owner, owns, run_bounded, unit_key};
pub use retention::{
    RetentionOutcome, SnapshotBlock, SnapshotReachability, maintain_bucket,
    maintain_bucket_with_reach, retention_sweep_bucket, retention_sweep_bucket_with_reach,
};
pub use rewrite::{MigrateOutcome, RewriteOutcome, migrate_bucket_format, rewrite_and_publish};
pub use rlog::RlogCodec;
pub use rspan_codec::SpanCodec;
pub use scan::{
    DEFAULT_MEMO_SNAPSHOT_STALENESS_NS, MaintainMemo, MaintainReport, MemoSnapshotError,
    ScanReport, SeedStats, scan_and_compact, scan_and_maintain,
};
pub use scrub::{
    CoveringPostings, ScrubBudget, ScrubCursor, ScrubResult, ScrubSlice, ScrubTarget,
    advance_cursor, per_tick_byte_budget, scrub_one_object,
};
pub use sweep::{
    CatalogSweepOutcome, ErasureRequestSweepOutcome, IdemSweepOutcome, LeaseCheck, NoLeases,
    OrphanSweepOutcome, SweepReport, sweep_erasure_requests, sweep_idempotency_markers,
    sweep_orphans, sweep_shard, sweep_shard_zoned, sweep_superseded,
    sweep_unreferenced_catalog_objects, sweep_unreferenced_parts,
};

#[cfg(test)]
mod reexport_tests {
    //! The worker-set move (ADR-0071, #863) must keep every path maintain's
    //! callers already use resolvable here. This is a compile-level assertion:
    //! if any re-export regressed, these `use`s fail to compile.

    #[allow(unused_imports)]
    use crate::worker_set::{
        DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_LIVENESS_FACTOR, DEFAULT_UNIT_CONCURRENCY, WorkerSet,
        owner, owns, run_bounded, unit_key,
    };
    #[allow(unused_imports)]
    use crate::{WorkerSet as CrateRootWorkerSet, owner as crate_root_owner};
}
