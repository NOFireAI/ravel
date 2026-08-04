//! OTLP gRPC `MetricsService::export`.

use std::sync::Arc;

use axum::http::{HeaderMap, HeaderName, HeaderValue};
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsService;
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use prost::Message;
use ravel_ingest::RequestRejection;
use ravel_types::Signal;
use tonic::metadata::{KeyAndValueRef, MetadataMap, MetadataValue};
use tonic::{Request, Response, Status};

use crate::ingest::IngestRequestError;
use crate::otlp_http::{COMMIT_TOKEN_HEADER, GatewayState, now_ns, write_mode_from_headers};

pub struct GrpcMetricsService {
    state: Arc<GatewayState>,
}

impl GrpcMetricsService {
    pub fn new(state: Arc<GatewayState>) -> Self {
        GrpcMetricsService { state }
    }
}

pub(crate) fn metadata_to_headers(metadata: &MetadataMap) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for entry in metadata.iter() {
        if let KeyAndValueRef::Ascii(key, value) = entry {
            let Ok(value_str) = value.to_str() else {
                continue;
            };
            let Ok(name) = HeaderName::from_bytes(key.as_str().as_bytes()) else {
                continue;
            };
            let Ok(header_value) = HeaderValue::from_str(value_str) else {
                continue;
            };
            headers.insert(name, header_value);
        }
    }
    headers
}

/// A layer-2/layer-4 whole-request rejection (ADR-0051 section 1) as a gRPC
/// status: `RESOURCE_EXHAUSTED`, no retry-info attached (the ADR's gRPC
/// column carries no header equivalent to HTTP's `Retry-After`). Shared by
/// every gRPC ingest service.
pub(crate) fn admission_rejection_status(rejection: RequestRejection) -> Status {
    Status::resource_exhausted(rejection.reason.to_string())
}

#[tonic::async_trait]
impl MetricsService for GrpcMetricsService {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let headers = metadata_to_headers(request.metadata());
        let tenant = self
            .state
            .tenant_resolver
            .resolve(&headers)
            .map_err(|_| Status::unauthenticated("invalid or missing tenant credentials"))?;
        let mode = write_mode_from_headers(&headers);

        // Layer 2 (ADR-0051 section 2): byte rate on the decoded message's
        // encoded length as a proxy for wire bytes (tonic has already decoded
        // by the time this handler runs), before this request reaches
        // `handle_export`.
        let request_bytes = request.get_ref().encoded_len() as u64;
        if let Err(rejection) =
            self.state
                .admission
                .check_byte_rate(&tenant, Signal::Metrics, request_bytes, now_ns())
        {
            return Err(admission_rejection_status(rejection));
        }

        let outcome = crate::ingest::handle_export(
            &self.state.ingest,
            tenant,
            mode,
            request.into_inner(),
            now_ns(),
        )
        .await
        .map_err(|err| match err {
            IngestRequestError::Admission(rejection) => admission_rejection_status(rejection),
            IngestRequestError::Provisioning(prov_err) => Status::internal(prov_err.to_string()),
            IngestRequestError::Write(write_err) if write_err.is_retryable() => {
                Status::unavailable(write_err.to_string())
            }
            IngestRequestError::Write(write_err) => Status::internal(write_err.to_string()),
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
