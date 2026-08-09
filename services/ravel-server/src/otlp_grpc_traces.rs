//! OTLP gRPC `TraceService::export`, the span-pipeline counterpart of
//! [`crate::otlp_grpc_logs`]. It shares [`crate::otlp_grpc`]'s
//! metadata-to-header conversion, so a client authenticates and selects a
//! write mode the same way on any of the three services.

use std::sync::Arc;

use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceService;
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use ravel_types::Signal;
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status};

use crate::otlp_grpc::{
    admission_rejection_status, ingest_concurrency_shed_status, metadata_to_headers,
};
use crate::otlp_http::{
    COMMIT_TOKEN_HEADER, GatewayState, idempotency_key_from_headers, now_ns,
    write_mode_from_headers,
};
use crate::traces_ingest::SpanIngestRequestError;
use crate::wire_byte_count::wire_request_bytes;

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

        // Layer 2 (ADR-0051 section 2): byte rate applies uniformly to every
        // signal including spans, even though spans get no layer-4 admission
        // (ADR-0051 excludes spans from series/stream admission). Counted by
        // `WireByteCountLayer` on wire bytes, not the decoded message's
        // encoded length.
        let request_bytes = wire_request_bytes(&request)?;
        if let Err(rejection) =
            self.state
                .admission
                .check_byte_rate(&tenant, Signal::Spans, request_bytes, now_ns())
        {
            return Err(admission_rejection_status(rejection));
        }

        let idempotency_key = idempotency_key_from_headers(&headers);

        let outcome = crate::traces_ingest::handle_export_traces(
            &self.state.traces_ingest,
            tenant,
            mode,
            request.into_inner(),
            now_ns(),
            idempotency_key,
        )
        .await
        .map_err(|err| match err {
            err @ SpanIngestRequestError::InvalidIdempotencyKey { .. } => {
                Status::invalid_argument(err.to_string())
            }
            err if err.is_retryable() => Status::unavailable(err.to_string()),
            err => Status::internal(err.to_string()),
        })?;

        // Verbatim on a replay, encoded-from-tokens otherwise. Built before
        // `outcome.response` is moved.
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
