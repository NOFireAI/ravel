//! Ingest pipeline: shard router, single-threaded shard actors, adaptive
//! flush, L0 build + upload + commit, strict/buffered acknowledgement.
//!
//! Implementer contract: docs/ingest.md, docs/consistency-model.md,
//! docs/catalog-and-mvcc.md, ADR-0001, ADR-0002, ADR-0010. Bounded mpsc
//! everywhere; no task-per-sample; backpressure to the gateway.

mod admission;
mod attribution;
mod budget;
mod clock;
mod config;
mod deferral;
mod error;
mod flush_share;
mod format_floor;
mod generation;
mod idempotency;
mod indexed_fields;
mod lifecycle;
mod log_declared_stats;
mod log_error;
mod log_metrics;
mod log_router;
mod log_shard;
mod metrics;
mod metrics_meta_sink;
mod pending_ack;
mod reconcile;
mod router;
mod shard;
mod span_error;
mod span_metrics;
mod span_router;
mod span_shard;
mod stage_timing;
mod storage_layout;
mod value;
mod write_gate;

pub use admission::{
    AdmissionController, AdmissionLimits, CountLimit, IdentityAdmission, RateLimit,
    RequestRejection, RequestRejectionReason, TenantUsage,
};
pub use attribution::{
    MAX_TRACKED_TENANTS, PUTS_PER_FLUSH, TenantCount, TenantPutAttribution, TenantPutCount,
};
pub use budget::{IngestByteBudget, IngestByteBudgetLimit, IngestByteCharge, IngestByteShed};
pub use clock::{Clock, SystemClock};
pub use config::{
    IngestConfig, IngestConfigError, LOG_SEGMENT_FORMAT_VERSION, MAX_FLUSH_ALL_PASSES,
    MAX_FLUSH_CLOCK_HOLD_NS, MIN_PLAUSIBLE_INGEST_CLOCK_NS, RlogZstdLevel, RlogZstdLevelError,
    SEGMENT_FORMAT_VERSION, SPAN_SEGMENT_FORMAT_VERSION, STRICT_VISIBILITY_RESERVE_NS,
    buffer_memory_backstop_bytes, plausible_ingest_clock,
};
pub use error::WriteError;
pub use format_floor::{
    BelowFormatFloor, FloorAboveWriter, FloorAdmission, FormatFamily, FormatFloorFamilySnapshot,
    FormatFloorMetrics, FormatFloorPolicy, global_metrics as format_floor_metrics,
    warn_floors_above_writer,
};
pub use generation::{DEFAULT_REFRESH_INTERVAL_NS, GenerationSwitch, HandBackReason, Routed};
pub use idempotency::{
    IDEM_MARKER_FORWARD_SKEW_TOLERANCE_HOURS, IdempotencyReceipt, LookupOutcome, MARKER_SUFFIX,
    MarkerError, MarkerLookupError, MarkerWriteError, WriteOutcome, decode_marker,
    idempotency_probe_gets, keyhash32, marker_key, read_marker, write_marker,
};
pub use indexed_fields::{DEFAULT_INDEXED_FIELDS_REREAD_BACKOFF_NS, IndexedFieldsOverlay};
pub use lifecycle::{
    DEFAULT_HARD_STALENESS_MULTIPLE, DEFAULT_LIFECYCLE_REFRESH_INTERVAL_NS, StalenessGate,
    overlay_admission_limits, refresh_tenant_limits_once,
};
pub use log_error::LogWriteError;
pub use log_metrics::{FlushTriggerMix, LogIngestMetrics, LogIngestMetricsSnapshot};
pub use log_router::{
    LogIndexedFields, LogIngestRouter, LogSubmitted, LogWriteReceipt, NoIndexedFields,
};
pub use log_shard::{est_columnar_log_batch_object_bytes, est_log_records_object_bytes};
pub use metrics::{FlushTrigger, IngestMetrics, IngestMetricsSnapshot, ShardSkewStats};
pub use metrics_meta_sink::{
    DEFAULT_MAX_CAS_RETRIES, DEFAULT_METADATA_FLUSH_WINDOW, FlushSummary, MetadataSink,
    MetadataSinkConfig, catalog_metric_kind,
};
pub use pending_ack::{PendingAckBudget, PendingAckPermit};
pub use reconcile::{
    DEFAULT_ADMISSION_RECONCILE_INTERVAL, ReconcileCycleStats, reconcile_once, snapshot_key,
};
pub use router::{IngestRouter, Submitted, WriteMode, WriteReceipt};
pub use span_error::SpanWriteError;
pub use span_metrics::{SpanIngestMetrics, SpanIngestMetricsSnapshot};
pub use span_router::{SpanIngestRouter, SpanSubmitted, SpanWriteReceipt, shard_for_span};
pub use span_shard::est_spans_object_bytes;
#[cfg(feature = "stage-timing")]
pub use stage_timing::{
    LogStage, LogStageSnapshot, LogStageTimings, MetricStage, MetricStageSnapshot,
    MetricStageTimings, StageTotals,
};
pub use value::{IngestExemplar, IngestPoint, IngestValue, est_points_object_bytes};
pub use write_gate::FlushInputBytes;
