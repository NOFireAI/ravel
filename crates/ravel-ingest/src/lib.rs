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
mod error;
mod generation;
mod idempotency;
mod indexed_fields;
mod lifecycle;
mod log_error;
mod log_metrics;
mod log_router;
mod log_shard;
mod metrics;
mod reconcile;
mod router;
mod shard;
mod span_error;
mod span_metrics;
mod span_router;
mod span_shard;
mod value;

pub use admission::{
    AdmissionController, AdmissionLimits, CountLimit, IdentityAdmission, RateLimit,
    RequestRejection, RequestRejectionReason, TenantUsage,
};
pub use attribution::{MAX_TRACKED_TENANTS, PUTS_PER_FLUSH, TenantPutAttribution, TenantPutCount};
pub use budget::{IngestByteBudget, IngestByteBudgetLimit, IngestByteCharge, IngestByteShed};
pub use clock::{Clock, SystemClock};
pub use config::{
    IngestConfig, LOG_SEGMENT_FORMAT_VERSION, MIN_PLAUSIBLE_INGEST_CLOCK_NS,
    SEGMENT_FORMAT_VERSION, SPAN_SEGMENT_FORMAT_VERSION, STRICT_VISIBILITY_RESERVE_NS,
    plausible_ingest_clock,
};
pub use error::WriteError;
pub use generation::{DEFAULT_REFRESH_INTERVAL_NS, GenerationSwitch, Routed};
pub use idempotency::{
    IDEM_MARKER_FORWARD_SKEW_TOLERANCE_HOURS, IdempotencyReceipt, LookupOutcome, MARKER_SUFFIX,
    MarkerError, MarkerWriteError, WriteOutcome, decode_marker, keyhash32, marker_key, read_marker,
    write_marker,
};
pub use indexed_fields::{DEFAULT_INDEXED_FIELDS_REREAD_BACKOFF_NS, IndexedFieldsOverlay};
pub use lifecycle::{
    DEFAULT_HARD_STALENESS_MULTIPLE, DEFAULT_LIFECYCLE_REFRESH_INTERVAL_NS, StalenessGate,
    overlay_admission_limits, refresh_tenant_limits_once,
};
pub use log_error::LogWriteError;
pub use log_metrics::{LogIngestMetrics, LogIngestMetricsSnapshot};
pub use log_router::{LogIndexedFields, LogIngestRouter, LogWriteReceipt, NoIndexedFields};
pub use metrics::{FlushTrigger, IngestMetrics, IngestMetricsSnapshot};
pub use reconcile::{DEFAULT_ADMISSION_RECONCILE_INTERVAL, reconcile_once, snapshot_key};
pub use router::{IngestRouter, WriteMode, WriteReceipt};
pub use span_error::SpanWriteError;
pub use span_metrics::{SpanIngestMetrics, SpanIngestMetricsSnapshot};
pub use span_router::{SpanIngestRouter, SpanWriteReceipt, shard_for_span};
pub use value::{IngestExemplar, IngestPoint, IngestValue};
