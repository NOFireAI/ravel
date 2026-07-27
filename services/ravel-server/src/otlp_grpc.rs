//! OTLP gRPC `MetricsService::export`.

use std::sync::Arc;

use axum::http::{HeaderMap, HeaderName, HeaderValue};
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsService;
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use tonic::metadata::{KeyAndValueRef, MetadataMap, MetadataValue};
use tonic::{Request, Response, Status};

use crate::otlp_http::{COMMIT_TOKEN_HEADER, GatewayState, now_ns, write_mode_from_headers};

pub struct GrpcMetricsService {
    state: Arc<GatewayState>,
}

impl GrpcMetricsService {
    pub fn new(state: Arc<GatewayState>) -> Self {
        GrpcMetricsService { state }
    }
}

fn metadata_to_headers(metadata: &MetadataMap) -> HeaderMap {
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

        let outcome = crate::ingest::handle_export(
            &self.state.ingest,
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
