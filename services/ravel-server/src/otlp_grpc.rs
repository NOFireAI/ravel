//! OTLP gRPC `MetricsService::export`.

use std::sync::Arc;

use axum::http::{HeaderMap, HeaderName, HeaderValue};
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsService;
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use ravel_ingest::WriteError;
use ravel_types::Signal;
use tonic::metadata::{KeyAndValueRef, MetadataMap, MetadataValue};
use tonic::{Request, Response, Status};

use crate::ingest::IngestRequestError;
use crate::ingest_admission::{admission_rejection_status, pending_ack_shed_status};
use crate::ingest_mode::resolve_write_mode;
use crate::otlp_http::{COMMIT_TOKEN_HEADER, GatewayState, now_ns};
use crate::wire_byte_count::wire_request_bytes;

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

#[tonic::async_trait]
impl MetricsService for GrpcMetricsService {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        // Normally already taken by `GrpcIngestAdmissionLayer` on the request
        // head, before tonic read or decoded this message (issue #1705), and
        // this is a clone of its release handle; on the direct-call path,
        // where nothing in front of the handler took one, the permit is taken
        // here and this handle owns it alone. Either way exactly one permit
        // covers the request, and it is released once the write is enqueued.
        // Read before `into_inner`, which drops the extensions.
        let permit =
            crate::ingest_admission::admit_grpc_request(&self.state.ingest_concurrency, &request)?;

        let headers = metadata_to_headers(request.metadata());
        let tenant = crate::ingest_admission::grpc_request_tenant(
            self.state.tenant_resolver.as_ref(),
            &request,
            &headers,
        )?;
        let mode = resolve_write_mode(&headers, self.state.strict_flush_policy);
        // The pending strict-ack permit, the same way: normally the layer's,
        // taken on the head; on the direct-call path taken here, before
        // anything normalizes or submits the message (issue #2805).
        let pending_ack = crate::ingest_admission::admit_grpc_pending_ack(
            self.state.ingest.router.pending_ack_budget(),
            self.state.strict_flush_policy,
            &request,
            &headers,
            &permit,
        )?;

        // Layer 2 (ADR-0051 section 2): byte rate counted by
        // `WireByteCountLayer` as tonic's decoder reads the request body. For a
        // gzip-compressed frame the charge is the decoded message size, not the
        // compressed wire length (ADR-0084 decision 4), so gRPC and HTTP charge
        // the same for identical telemetry.
        let charge = wire_request_bytes(&request)?;
        if let Err(rejection) = self.state.admission.check_byte_rate(
            &tenant,
            Signal::Metrics,
            charge.charge_bytes,
            now_ns(),
        ) {
            return Err(admission_rejection_status(rejection));
        }
        self.state.ingest_byte_metrics.record_wire_bytes(
            &tenant,
            Signal::Metrics,
            charge.wire_bytes,
        );

        let outcome = crate::ingest::handle_export_releasing(
            &self.state.ingest,
            tenant,
            mode,
            request.into_inner(),
            now_ns(),
            &permit,
            &pending_ack,
        )
        .await
        .map_err(|err| match err {
            IngestRequestError::Admission(rejection) => admission_rejection_status(rejection),
            // Receiver-clock floor (ADR-0051 amendment): UNAVAILABLE,
            // the replica's fault; a retry against a healthy one succeeds.
            err @ IngestRequestError::ClockImplausible(_) => Status::unavailable(err.to_string()),
            IngestRequestError::Provisioning(prov_err) => Status::internal(prov_err.to_string()),
            // The buffer-budget shed (ADR-0069) is RESOURCE_EXHAUSTED, the same
            // status the byte-rate and in-flight sheds use, not the UNAVAILABLE
            // the other retryable write failures take.
            IngestRequestError::Write(WriteError::BufferBudgetExceeded) => {
                Status::resource_exhausted(WriteError::BufferBudgetExceeded.to_string())
            }
            // The pending strict-ack bound (ADR-2793 decision 4): the same
            // status as the in-flight shed, with its own message.
            IngestRequestError::Write(WriteError::PendingAckLimitReached) => {
                pending_ack_shed_status()
            }
            // A shard at the flush deferral cap refused the write before
            // buffering it: the same backpressure class as the budget shed.
            IngestRequestError::Write(WriteError::DeferralCapReached) => {
                Status::resource_exhausted(WriteError::DeferralCapReached.to_string())
            }
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

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use ravel_ingest::AdmissionLimits;
    use tonic::Code;

    use super::*;
    use crate::otlp_http::tests::{state_with_limits, value_kind_mismatch_request};
    use crate::wire_byte_count::WireByteCounter;

    /// A permanent, non-retryable write rejection
    /// (`WriteError::SeriesValueKindMismatch`) must stay outside the OTLP-spec
    /// retryable gRPC code set (CANCELLED, DEADLINE_EXCEEDED, ABORTED,
    /// OUT_OF_RANGE, UNAVAILABLE, DATA_LOSS): a well-behaved exporter that
    /// retries only on those codes must not retry a request that can never
    /// succeed. This pins the gRPC side of the same property the HTTP
    /// handlers now enforce as 400 instead of 503, without changing gRPC's
    /// own (already-correct) status mapping.
    ///
    /// `export` calls `wire_request_bytes` before it ever reaches the
    /// Write-error match arms, and that call fails closed without a
    /// `WireByteCounter` in the request's extensions (normally placed there
    /// by `WireByteCountLayer` on the real tonic listener). This test calls
    /// `export` directly, bypassing that layer, so it must seed the
    /// extension itself or it fails at admission instead of exercising the
    /// mapping under test.
    #[tokio::test]
    async fn permanent_write_error_stays_outside_retryable_grpc_codes() {
        let state = state_with_limits(AdmissionLimits::default());
        let service = GrpcMetricsService::new(state);

        let mut request = Request::new(value_kind_mismatch_request());
        request
            .extensions_mut()
            .insert(WireByteCounter::for_test_with_message(false, 64));

        let result = service.export(request).await;

        let status = result.expect_err("a series value-kind mismatch must not be admitted");
        assert!(
            matches!(status.code(), Code::Internal | Code::InvalidArgument),
            "a permanent write error must map to a non-retryable gRPC code, got {:?}",
            status.code()
        );
    }

    /// The counters a log export moves before its router submit: the
    /// tenant's admission usage for logs, its wire bytes, and the log
    /// router's own snapshot.
    fn log_counters(
        state: &GatewayState,
    ) -> (
        Vec<ravel_ingest::TenantUsage>,
        Vec<crate::ingest_byte_metrics::TenantWireBytes>,
        ravel_ingest::LogIngestMetricsSnapshot,
    ) {
        let usage = state
            .admission
            .usage_snapshot()
            .into_iter()
            .filter(|row| row.signal == Signal::Logs)
            .collect();
        let wire = state
            .ingest_byte_metrics
            .snapshot()
            .into_iter()
            .filter(|row| row.signal == Signal::Logs)
            .collect();
        (usage, wire, state.logs_ingest.router.metrics_snapshot())
    }

    fn strict_log_export(body: &str) -> Request<ExportLogsServiceRequest> {
        use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};

        let mut request = Request::new(ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        time_unix_nano: now_ns() as u64,
                        body: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                            value: Some(
                                opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                                    body.to_string(),
                                ),
                            ),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        });
        request.metadata_mut().insert(
            crate::otlp_http::INGEST_MODE_HEADER,
            "strict-arrival".parse().expect("ascii metadata"),
        );
        request
    }

    /// A direct call to a gRPC export service method takes its pending-ack
    /// permit from the same budget as the layer: with a budget of 1 held by a
    /// strict export that came through the real listener's layer stack and
    /// waits on its held provisioning-record PUT, a direct
    /// `GrpcLogsService::export` is refused `RESOURCE_EXHAUSTED` before it
    /// charges admission, records wire bytes, or reaches the store or the
    /// router, and the layered export still acks once the PUT is released.
    /// That PUT is issued after the handler read its permit out of the
    /// extensions and before `submit`, so the permit holding the budget there
    /// is the one taken on the head, not one the router took.
    #[tokio::test]
    async fn direct_call_export_is_refused_while_a_layered_export_holds_the_pending_ack_budget() {
        use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;
        use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::{
            LogsService, LogsServiceServer,
        };
        use ravel_ingest::{IngestByteBudget, IngestByteBudgetLimit, PendingAckBudget};
        use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op};
        use ravel_object_store::memory::MemoryStore;
        use ravel_types::TenantId;

        use crate::ingest_admission::{GrpcIngestAdmissionLayer, GrpcIngestServices};
        use crate::otlp_grpc_logs::GrpcLogsService;
        use crate::otlp_http::tests::{TENANT, state_with_pending_ack};

        const WAIT: std::time::Duration = std::time::Duration::from_secs(15);

        let faults = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
        let gate = faults.hold(
            Op::Put,
            Some(ravel_catalog::provisioning_key(
                &TenantId::new(TENANT).hash(),
                ravel_types::Signal::Logs,
            )),
            Occurrence::Always,
        );
        let pending_ack = Arc::new(PendingAckBudget::new(1));
        let mut state = Arc::into_inner(state_with_pending_ack(
            faults.clone(),
            AdmissionLimits::default(),
            IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited),
            pending_ack.clone(),
        ))
        .expect("the fixture's state is not shared yet");
        state.logs_ingest.provisioning = Some(Arc::new(
            crate::provisioning::ProvisioningRecordWriter::new(faults, 1),
        ));
        let state = Arc::new(state);

        let server = tonic::transport::Server::builder()
            .layer(crate::wire_byte_count::WireByteCountLayer)
            .layer(GrpcIngestAdmissionLayer::new(
                state.ingest_concurrency.clone(),
                state.tenant_resolver.clone(),
                GrpcIngestServices {
                    otlp: true,
                    otap: false,
                },
                pending_ack.clone(),
                state.strict_flush_policy,
            ))
            .add_service(LogsServiceServer::new(GrpcLogsService::new(state.clone())));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let addr = listener.local_addr().expect("local addr");
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            server
                .serve_with_incoming_shutdown(
                    tonic::transport::server::TcpIncoming::from(listener),
                    async {
                        let _ = stopped.await;
                    },
                )
                .await
                .expect("listener serves");
        });

        let mut client = LogsServiceClient::connect(format!("http://{addr}"))
            .await
            .expect("gRPC client connects");
        let mut layered = tokio::spawn(async move {
            client
                .export(strict_log_export("layered strict log line"))
                .await
        });
        tokio::time::timeout(WAIT, gate.wait_until_held(1))
            .await
            .expect("the layered export reaches its provisioning PUT");
        let before = log_counters(&state);

        let mut direct = strict_log_export("direct strict log line");
        direct
            .extensions_mut()
            .insert(WireByteCounter::for_test_with_message(false, 64));
        let direct_service = GrpcLogsService::new(state.clone());
        let status = tokio::time::timeout(WAIT, async {
            tokio::select! {
                out = direct_service.export(direct) => out,
                () = gate.wait_until_held(2) => panic!(
                    "the direct call was admitted and issued its own provisioning PUT"
                ),
            }
        })
        .await
        .expect("the direct call is answered")
        .expect_err("the direct call is over the pending-ack budget");
        assert_eq!(status.code(), Code::ResourceExhausted, "{status:?}");
        assert_eq!(
            status.message(),
            "process pending strict-acknowledgement limit reached"
        );
        let (usage, wire, router) = before;
        assert_eq!(
            log_counters(&state),
            (
                usage,
                wire,
                ravel_ingest::LogIngestMetricsSnapshot {
                    pending_ack_shed: router.pending_ack_shed + 1,
                    ..router
                }
            ),
            "the direct call was refused before admission, wire bytes and the router, \
             and only the shed counter moved"
        );
        assert_eq!(pending_ack.shed_total(), 1);
        assert_eq!(pending_ack.held(), 1);

        let response = tokio::time::timeout(WAIT, async {
            loop {
                tokio::select! {
                    out = &mut layered => return out,
                    () = gate.wait_until_held(1) => {
                        for id in gate.held() {
                            gate.release(id);
                        }
                    }
                }
            }
        })
        .await
        .expect("the layered export completes once its PUT is released")
        .expect("export task");
        response.expect("the layered export acks");
        assert_eq!(pending_ack.held(), 0, "the ack returned the permit");
        let _ = stop.send(());
    }
}
