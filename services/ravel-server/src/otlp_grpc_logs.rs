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

use crate::logs_ingest::LogIngestRequestError;
use crate::otlp_grpc::{
    admission_rejection_status, ingest_concurrency_shed_status, metadata_to_headers,
};
use crate::otlp_http::{
    COMMIT_TOKEN_HEADER, GatewayState, idempotency_key_from_headers, now_ns,
    write_mode_from_headers,
};
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
        let _permit = self
            .state
            .ingest_concurrency
            .try_admit()
            .map_err(|_| ingest_concurrency_shed_status())?;

        let headers = metadata_to_headers(request.metadata());
        let tenant = self
            .state
            .tenant_resolver
            .resolve(&headers)
            .map_err(|_| Status::unauthenticated("invalid or missing tenant credentials"))?;
        let mode = write_mode_from_headers(&headers);

        // Layer 2 (ADR-0051 section 2): byte rate on wire bytes, counted by
        // `WireByteCountLayer` as tonic's decoder reads them off the request
        // body, before this request reaches `handle_export_logs`.
        let request_bytes = wire_request_bytes(&request)?;
        if let Err(rejection) =
            self.state
                .admission
                .check_byte_rate(&tenant, Signal::Logs, request_bytes, now_ns())
        {
            return Err(admission_rejection_status(rejection));
        }

        let idempotency_key = idempotency_key_from_headers(&headers);

        let outcome = crate::logs_ingest::handle_export_logs(
            &self.state.logs_ingest,
            tenant,
            mode,
            request.into_inner(),
            now_ns(),
            idempotency_key,
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
