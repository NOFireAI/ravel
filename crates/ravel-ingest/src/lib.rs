//! Ingest pipeline: shard router, single-threaded shard actors, adaptive
//! flush, L0 build + upload + commit, strict/buffered acknowledgement.
//!
//! Implementer contract: docs/ingest.md, docs/consistency-model.md,
//! docs/catalog-and-mvcc.md, ADR-0001, ADR-0002, ADR-0010. Bounded mpsc
//! everywhere; no task-per-sample; backpressure to the gateway.

mod clock;
mod config;
mod error;
mod log_error;
mod log_metrics;
mod metrics;
mod router;
mod shard;
mod value;

pub use clock::{Clock, SystemClock};
pub use config::{IngestConfig, SEGMENT_FORMAT_VERSION};
pub use error::WriteError;
pub use log_error::LogWriteError;
pub use log_metrics::{LogIngestMetrics, LogIngestMetricsSnapshot};
pub use metrics::{FlushTrigger, IngestMetrics, IngestMetricsSnapshot};
pub use router::{IngestRouter, WriteMode, WriteReceipt};
pub use value::{IngestPoint, IngestValue};
