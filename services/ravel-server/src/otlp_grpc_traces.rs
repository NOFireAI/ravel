//! OTLP gRPC `TraceService::export`, the span-pipeline counterpart of
//! [`crate::otlp_grpc_logs`]. It shares [`crate::otlp_grpc`]'s
//! metadata-to-header conversion, so a client authenticates and selects a
//! write mode the same way on any of the three services.

use std::sync::Arc;

use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceService;
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status};

use crate::otlp_grpc::metadata_to_headers;
use crate::otlp_http::{COMMIT_TOKEN_HEADER, GatewayState, now_ns, write_mode_from_headers};

pub struct GrpcTraceService {
    state: Arc<GatewayState>,
}

impl GrpcTraceService {
    pub fn new(state: Arc<GatewayState>) -> Self {
        GrpcTraceService { state }
    }
}

#[tonic::async_trait]
impl TraceService for GrpcTraceService {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let headers = metadata_to_headers(request.metadata());
        let tenant = self
            .state
            .tenant_resolver
            .resolve(&headers)
            .map_err(|_| Status::unauthenticated("invalid or missing tenant credentials"))?;
        let mode = write_mode_from_headers(&headers);

        let outcome = crate::traces_ingest::handle_export_traces(
            &self.state.traces_ingest,
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
