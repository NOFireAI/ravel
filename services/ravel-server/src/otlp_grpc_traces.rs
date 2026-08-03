//! OTLP gRPC `TraceService::export`, the span-pipeline counterpart of
//! [`crate::otlp_grpc_logs`]. It shares [`crate::otlp_grpc`]'s
//! metadata-to-header conversion, so a client authenticates and selects a
//! write mode the same way on any of the three services.

use std::sync::Arc;

use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceService;
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use prost::Message;
use ravel_types::Signal;
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status};

use crate::otlp_grpc::{admission_rejection_status, metadata_to_headers};
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

        // Layer 2 (ADR-0051 section 2): byte rate applies uniformly to every
        // signal including spans, even though spans get no layer-4 admission
        // (ADR-0051 excludes spans from series/stream admission).
        let request_bytes = request.get_ref().encoded_len() as u64;
        if let Err(rejection) =
            self.state
                .admission
                .check_byte_rate(&tenant, Signal::Spans, request_bytes, now_ns())
        {
            return Err(admission_rejection_status(rejection));
        }

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
