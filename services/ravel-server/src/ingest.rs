//! OTLP-transport-agnostic ingest logic shared by the HTTP and gRPC handlers.

use std::sync::Arc;
use std::time::Duration;

use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsPartialSuccess, ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use ravel_ingest::{IngestPoint, IngestRouter, WriteError, WriteMode};
use ravel_otlp::{IngestLimits, normalize_metrics};
use ravel_types::{CommitToken, TenantId};

pub struct IngestState {
    pub router: Arc<IngestRouter>,
    pub limits: IngestLimits,
    pub ack_deadline: Duration,
}

pub struct IngestOutcome {
    pub response: ExportMetricsServiceResponse,
    pub tokens: Vec<CommitToken>,
}

pub async fn handle_export(
    state: &IngestState,
    tenant: TenantId,
    mode: WriteMode,
    request: ExportMetricsServiceRequest,
    ingest_ts_ns: i64,
) -> Result<IngestOutcome, WriteError> {
    let normalized = normalize_metrics(&tenant, request, &state.limits, ingest_ts_ns);
    let rejected_count: usize = normalized.rejected.iter().map(|r| r.rejected_count()).sum();

    // Scalar and native-histogram points arrive in separate vectors; both
    // feed one ingest write so a request's points share a single receipt.
    let mut points: Vec<IngestPoint> =
        Vec::with_capacity(normalized.points.len() + normalized.histogram_points.len());
    points.extend(normalized.points.into_iter().map(IngestPoint::from));
    points.extend(
        normalized
            .histogram_points
            .into_iter()
            .map(IngestPoint::from),
    );

    let receipt = state
        .router
        .write_values(tenant, points, mode, state.ack_deadline)
        .await?;

    let partial_success = if rejected_count > 0 {
        let error_message = normalized
            .rejected
            .iter()
            .map(|r| r.to_string())
            .collect::<Vec<_>>()
            .join("; ");
        Some(ExportMetricsPartialSuccess {
            rejected_data_points: rejected_count as i64,
            error_message,
        })
    } else {
        None
    };

    Ok(IngestOutcome {
        response: ExportMetricsServiceResponse { partial_success },
        tokens: receipt.tokens,
    })
}
