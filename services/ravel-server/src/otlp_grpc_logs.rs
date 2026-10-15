//! OTLP gRPC `LogsService::export`, the log-pipeline counterpart of
//! [`crate::otlp_grpc`]. It shares that module's metadata-to-header
//! conversion, so a client authenticates and selects a write mode the same
//! way on either service.

use std::sync::Arc;

use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::LogsService;
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use ravel_types::Signal;
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status};

use crate::ingest_admission::admission_rejection_status;
use crate::ingest_mode::resolve_write_mode;
use crate::logs_ingest::LogIngestRequestError;
use crate::otlp_grpc::metadata_to_headers;
use crate::otlp_http::{COMMIT_TOKEN_HEADER, GatewayState, idempotency_key_from_headers, now_ns};
use crate::wire_byte_count::wire_request_bytes;

pub struct GrpcLogsService {
    state: Arc<GatewayState>,
}

impl GrpcLogsService {
    pub fn new(state: Arc<GatewayState>) -> Self {
        GrpcLogsService { state }
    }
}

#[tonic::async_trait]
impl LogsService for GrpcLogsService {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        // See `otlp_grpc::export`: normally a clone of the release handle
        // `GrpcIngestAdmissionLayer` took on the request head (issue #1705),
        // and a permit taken here only on the direct-call path. Read before
        // `into_inner`, which drops the extensions.
        let permit =
            crate::ingest_admission::admit_grpc_request(&self.state.ingest_concurrency, &request)?;

        let headers = metadata_to_headers(request.metadata());
        let tenant = crate::ingest_admission::grpc_request_tenant(
            self.state.tenant_resolver.as_ref(),
            &request,
            &headers,
        )?;
        let mode = resolve_write_mode(&headers, self.state.strict_flush_policy);
        // See `otlp_grpc::export`: the layer's pending strict-ack permit, or
        // one taken here on the direct-call path (issue #2805).
        let pending_ack = crate::ingest_admission::admit_grpc_pending_ack(
            self.state.logs_ingest.router.pending_ack_budget(),
            mode,
            &request,
            &permit,
        )?;

        // Layer 2 (ADR-0051 section 2): byte rate counted by
        // `WireByteCountLayer` as tonic's decoder reads the request body. A
        // gzip-compressed frame is charged its decoded message size, not the
        // compressed wire length (ADR-0084 decision 4).
        let charge = wire_request_bytes(&request)?;
        if let Err(rejection) = self.state.admission.check_byte_rate(
            &tenant,
            Signal::Logs,
            charge.charge_bytes,
            now_ns(),
        ) {
            return Err(admission_rejection_status(rejection));
        }
        self.state
            .ingest_byte_metrics
            .record_wire_bytes(&tenant, Signal::Logs, charge.wire_bytes);

        let idempotency_key = idempotency_key_from_headers(&headers);

        let outcome = crate::logs_ingest::handle_export_logs_releasing(
            &self.state.logs_ingest,
            tenant,
            mode,
            request.into_inner(),
            now_ns(),
            idempotency_key,
            &permit,
            &pending_ack,
        )
        .await
        .map_err(|err| match err {
            LogIngestRequestError::Admission(rejection) => admission_rejection_status(rejection),
            err @ LogIngestRequestError::ClockImplausible(_) => {
                Status::unavailable(err.to_string())
            }
            err @ LogIngestRequestError::InvalidIdempotencyKey { .. } => {
                Status::invalid_argument(err.to_string())
            }
            LogIngestRequestError::Provisioning(prov_err) => Status::internal(prov_err.to_string()),
            // Buffer-budget shed (ADR-0069): RESOURCE_EXHAUSTED, not the
            // UNAVAILABLE the other retryable write failures take.
            LogIngestRequestError::Write(
                write_err @ ravel_ingest::LogWriteError::BufferBudgetExceeded,
            ) => Status::resource_exhausted(write_err.to_string()),
            LogIngestRequestError::Write(ravel_ingest::LogWriteError::PendingAckLimitReached) => {
                crate::ingest_admission::pending_ack_shed_status()
            }
            // A shard at the flush deferral cap refused the write before
            // buffering it: the same backpressure class as the budget shed.
            LogIngestRequestError::Write(
                write_err @ ravel_ingest::LogWriteError::DeferralCapReached,
            ) => Status::resource_exhausted(write_err.to_string()),
            LogIngestRequestError::Write(write_err) if write_err.is_retryable() => {
                Status::unavailable(write_err.to_string())
            }
            LogIngestRequestError::Write(write_err) => Status::internal(write_err.to_string()),
        })?;

        // Verbatim on a replay, encoded-from-tokens otherwise: identical to the
        // HTTP path's header choice. Built before `outcome.response` is moved.
        let commit_token = outcome.commit_token_header();
        let mut response = Response::new(outcome.response);
        if let Some(encoded) = commit_token
            && let Ok(value) = MetadataValue::try_from(encoded.as_str())
        {
            response.metadata_mut().insert(COMMIT_TOKEN_HEADER, value);
        }
        Ok(response)
    }
}
