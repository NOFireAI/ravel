//! OTLP gRPC `LogsService::export`, the log-pipeline counterpart of
//! [`crate::otlp_grpc`]. It shares that module's metadata-to-header
//! conversion, so a client authenticates and selects a write mode the same
//! way on either service.

use std::sync::Arc;

use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::LogsService;
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status};

use crate::otlp_grpc::metadata_to_headers;
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

        let outcome = crate::logs_ingest::handle_export_logs(
            &self.state.logs_ingest,
            tenant,
            mode,
            request.into_inner(),
            now_ns(),
        )
        .await
        .map_err(|err| {
            if err.is_retryable() {
                Status::unavailable(err.to_string())
            } else {
                Status::internal(err.to_string())
            }
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
