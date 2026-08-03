//! OTLP gRPC `LogsService::export`, the log-pipeline counterpart of
//! [`crate::otlp_grpc`]. It shares that module's metadata-to-header
//! conversion, so a client authenticates and selects a write mode the same
//! way on either service.

use std::sync::Arc;

use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::LogsService;
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use prost::Message;
use ravel_types::Signal;
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status};

use crate::logs_ingest::LogIngestRequestError;
use crate::otlp_grpc::{admission_rejection_status, metadata_to_headers};
use crate::otlp_http::{COMMIT_TOKEN_HEADER, GatewayState, now_ns, write_mode_from_headers};

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
        let headers = metadata_to_headers(request.metadata());
        let tenant = self
            .state
            .tenant_resolver
            .resolve(&headers)
            .map_err(|_| Status::unauthenticated("invalid or missing tenant credentials"))?;
        let mode = write_mode_from_headers(&headers);

        // Layer 2 (ADR-0051 section 2): byte rate on the decoded message's
        // encoded length as a proxy for wire bytes, before this request
        // reaches `handle_export_logs`.
        let request_bytes = request.get_ref().encoded_len() as u64;
        if let Err(rejection) =
            self.state
                .admission
                .check_byte_rate(&tenant, Signal::Logs, request_bytes, now_ns())
        {
            return Err(admission_rejection_status(rejection));
        }

        let outcome = crate::logs_ingest::handle_export_logs(
            &self.state.logs_ingest,
            tenant,
            mode,
            request.into_inner(),
            now_ns(),
        )
        .await
        .map_err(|err| match err {
            LogIngestRequestError::Admission(rejection) => admission_rejection_status(rejection),
            LogIngestRequestError::Write(write_err) if write_err.is_retryable() => {
                Status::unavailable(write_err.to_string())
            }
            LogIngestRequestError::Write(write_err) => Status::internal(write_err.to_string()),
        })?;

        let mut response = Response::new(outcome.response);
        if !outcome.tokens.is_empty() {
            let encoded = outcome
                .tokens
                .iter()
                .map(|token| token.encode())
                .collect::<Vec<_>>()
                .join(",");
            if let Ok(value) = MetadataValue::try_from(encoded.as_str()) {
                response.metadata_mut().insert(COMMIT_TOKEN_HEADER, value);
            }
        }
        Ok(response)
    }
}
