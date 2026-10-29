//! The sinks the agents projector delivers to (ADR-3331 decision 12): this
//! process's own log and span ingest routers, or OTLP gRPC to a gateway.
//!
//! Both sinks write the same records, built here from a
//! [`ProjectionBatch`]: one log record per event, its body the
//! [`ProjectedEvent::body`] text so the stable event id is searchable, and
//! one span per turn. The payload reaches a record only through that body
//! and the hash and size attributes, all three built by the projector's
//! redaction gate.

use std::sync::Arc;
use std::time::Duration;

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use parking_lot::Mutex;
use ravel_agents_runtime::projection::{
    ProjectedEvent, ProjectedTurn, ProjectionBatch, ProjectionSink, SinkError, SinkFuture,
};
use ravel_agents_store::Clock;
use ravel_ingest::{LogIngestRouter, SpanIngestRouter, WriteMode};
use ravel_ipcrypt::rules::TenantRules;
use ravel_otlp::traces_normalize::normalize_traces;
use ravel_otlp::{LogIngestLimits, SpanIngestLimits, normalize_logs};
use ravel_types::TenantId;

use crate::config::ProjectionTap;

/// The `service.name` every projected record carries.
pub const SERVICE_NAME: &str = "ravel-agents";
/// The span name of one projected turn.
pub const TURN_SPAN_NAME: &str = "agent.turn";
/// The gRPC metadata header the OTLP sink names the tenant in.
pub const TENANT_HEADER: &str = "x-ravel-tenant";
/// How long one delivery waits for its durable acknowledgement.
pub const DELIVERY_DEADLINE: Duration = Duration::from_secs(10);

const SEVERITY_INFO: i32 = 9;

fn kv(key: &str, value: Value) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue { value: Some(value) }),
        ..Default::default()
    }
}

fn text(key: &str, value: impl Into<String>) -> KeyValue {
    kv(key, Value::StringValue(value.into()))
}

fn int(key: &str, value: u64) -> KeyValue {
    kv(
        key,
        Value::IntValue(i64::try_from(value).unwrap_or(i64::MAX)),
    )
}

fn resource(tenant: &TenantId) -> Resource {
    Resource {
        attributes: vec![
            text("service.name", SERVICE_NAME),
            text("ravel.tenant", tenant.as_str()),
        ],
        ..Default::default()
    }
}

fn log_record(event: &ProjectedEvent) -> LogRecord {
    let ts = u64::try_from(event.observed_unix_ns).unwrap_or(0);
    let mut attributes = vec![
        text("agent.event_id", event.event_id.to_hex()),
        text("agent.id", event.agent.to_hex()),
        int("agent.incarnation", event.incarnation),
        int("agent.event_seq", event.event_seq),
        int("agent.accept_seq", event.accept_seq),
        text("agent.turn_id", event.turn_id.to_hex()),
        text("agent.event_type", event.event_type.clone()),
    ];
    if let Some(hash) = &event.payload.hash {
        attributes.push(text("agent.payload_hash", hash.clone()));
        attributes.push(int("agent.payload_size", event.payload.size_bytes));
    }
    LogRecord {
        time_unix_nano: ts,
        observed_time_unix_nano: ts,
        severity_number: SEVERITY_INFO,
        severity_text: "INFO".to_string(),
        body: Some(AnyValue {
            value: Some(Value::StringValue(event.body())),
        }),
        attributes,
        trace_id: event.trace_id.to_vec(),
        span_id: event.span_id.to_vec(),
        ..Default::default()
    }
}

fn span(turn: &ProjectedTurn) -> Span {
    let start = u64::try_from(turn.start_unix_ns).unwrap_or(0);
    let end = u64::try_from(turn.end_unix_ns).unwrap_or(0).max(start);
    Span {
        trace_id: turn.trace_id.to_vec(),
        span_id: turn.span_id.to_vec(),
        name: TURN_SPAN_NAME.to_string(),
        start_time_unix_nano: start,
        end_time_unix_nano: end,
        attributes: vec![
            text("agent.id", turn.agent.to_hex()),
            int("agent.incarnation", turn.incarnation),
            int("agent.accept_seq", turn.accept_seq),
            text("agent.turn_id", turn.turn_id.to_hex()),
            int("agent.first_event_seq", turn.first_event_seq),
            int("agent.last_event_seq", turn.last_event_seq),
        ],
        ..Default::default()
    }
}

/// The batch's events as one OTLP logs request.
pub fn logs_request(tenant: &TenantId, batch: &ProjectionBatch) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(resource(tenant)),
            scope_logs: vec![ScopeLogs {
                log_records: batch.events.iter().map(log_record).collect(),
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

/// The batch's turns as one OTLP traces request.
pub fn traces_request(tenant: &TenantId, batch: &ProjectionBatch) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(resource(tenant)),
            scope_spans: vec![ScopeSpans {
                spans: batch.turns.iter().map(span).collect(),
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

/// Writes through this process's ingest routers, durably acknowledged.
pub struct InProcessSink {
    pub tenant: TenantId,
    pub logs: Arc<LogIngestRouter>,
    pub spans: Arc<SpanIngestRouter>,
    pub clock: Arc<dyn Clock>,
}

impl InProcessSink {
    async fn write(&self, batch: &ProjectionBatch) -> Result<(), SinkError> {
        let now = self.clock.now_unix_ns();
        if !batch.events.is_empty() {
            let out = normalize_logs(
                logs_request(&self.tenant, batch),
                &LogIngestLimits::default(),
                &TenantRules::none(),
                now,
            );
            if !out.rejected.is_empty() {
                return Err(SinkError(format!(
                    "log ingest rejected projected records: {:?}",
                    out.rejected
                )));
            }
            self.logs
                .write(
                    self.tenant.clone(),
                    out.records,
                    WriteMode::StrictArrival,
                    DELIVERY_DEADLINE,
                )
                .await
                .map_err(|e| SinkError(format!("log ingest write failed: {e}")))?;
        }
        if !batch.turns.is_empty() {
            let out = normalize_traces(
                traces_request(&self.tenant, batch),
                &SpanIngestLimits::default(),
                &TenantRules::none(),
                now,
            );
            if !out.rejected.is_empty() {
                return Err(SinkError(format!(
                    "span ingest rejected projected spans: {:?}",
                    out.rejected
                )));
            }
            self.spans
                .write(
                    self.tenant.clone(),
                    out.spans,
                    WriteMode::StrictArrival,
                    DELIVERY_DEADLINE,
                )
                .await
                .map_err(|e| SinkError(format!("span ingest write failed: {e}")))?;
        }
        Ok(())
    }
}

impl ProjectionSink for InProcessSink {
    fn deliver<'a>(&'a self, batch: &'a ProjectionBatch) -> SinkFuture<'a> {
        Box::pin(self.write(batch))
    }
}

/// Sends OTLP gRPC to a gateway, the tenant named in [`TENANT_HEADER`].
pub struct OtlpSink {
    pub tenant: TenantId,
    pub endpoint: String,
    channel: Mutex<Option<tonic::transport::Channel>>,
}

impl OtlpSink {
    pub fn new(tenant: TenantId, endpoint: String) -> OtlpSink {
        OtlpSink {
            tenant,
            endpoint,
            channel: Mutex::new(None),
        }
    }

    fn channel(&self) -> Result<tonic::transport::Channel, SinkError> {
        let mut slot = self.channel.lock();
        if let Some(channel) = slot.as_ref() {
            return Ok(channel.clone());
        }
        let channel = tonic::transport::Endpoint::from_shared(self.endpoint.clone())
            .map_err(|e| SinkError(format!("projection endpoint {}: {e}", self.endpoint)))?
            .connect_lazy();
        *slot = Some(channel.clone());
        Ok(channel)
    }

    fn request<T>(&self, body: T) -> Result<tonic::Request<T>, SinkError> {
        let mut request = tonic::Request::new(body);
        let tenant = tonic::metadata::MetadataValue::try_from(self.tenant.as_str())
            .map_err(|e| SinkError(format!("tenant is not a header value: {e}")))?;
        request.metadata_mut().insert(TENANT_HEADER, tenant);
        request.set_timeout(DELIVERY_DEADLINE);
        Ok(request)
    }

    async fn send(&self, batch: &ProjectionBatch) -> Result<(), SinkError> {
        let channel = self.channel()?;
        if !batch.events.is_empty() {
            let response = LogsServiceClient::new(channel.clone())
                .export(self.request(logs_request(&self.tenant, batch))?)
                .await
                .map_err(|e| SinkError(format!("OTLP logs export failed: {e}")))?;
            if let Some(partial) = response.into_inner().partial_success
                && partial.rejected_log_records > 0
            {
                return Err(SinkError(format!(
                    "gateway rejected {} projected records: {}",
                    partial.rejected_log_records, partial.error_message
                )));
            }
        }
        if !batch.turns.is_empty() {
            let response = TraceServiceClient::new(channel)
                .export(self.request(traces_request(&self.tenant, batch))?)
                .await
                .map_err(|e| SinkError(format!("OTLP traces export failed: {e}")))?;
            if let Some(partial) = response.into_inner().partial_success
                && partial.rejected_spans > 0
            {
                return Err(SinkError(format!(
                    "gateway rejected {} projected spans: {}",
                    partial.rejected_spans, partial.error_message
                )));
            }
        }
        Ok(())
    }
}

impl ProjectionSink for OtlpSink {
    fn deliver<'a>(&'a self, batch: &'a ProjectionBatch) -> SinkFuture<'a> {
        Box::pin(self.send(batch))
    }
}

/// Runs the configured [`ProjectionTap`] with the batch's event ids before
/// the inner sink; a tap `Err` fails the delivery without reaching it.
pub struct TappedSink {
    pub tap: ProjectionTap,
    pub inner: Arc<dyn ProjectionSink>,
}

impl ProjectionSink for TappedSink {
    fn deliver<'a>(&'a self, batch: &'a ProjectionBatch) -> SinkFuture<'a> {
        Box::pin(async move {
            if let Some(tap) = &self.tap.0 {
                let ids: Vec<String> = batch.events.iter().map(|e| e.event_id.to_hex()).collect();
                tap(&ids).map_err(SinkError)?;
            }
            self.inner.deliver(batch).await
        })
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use ravel_agents_core::identity::{AgentId, Id128};
    use ravel_agents_runtime::projection::ProjectedPayload;

    use super::*;

    fn event(payload: ProjectedPayload) -> ProjectedEvent {
        ProjectedEvent {
            event_id: Id128([1; 16]),
            agent: AgentId([2; 16]),
            incarnation: 1,
            event_seq: 3,
            accept_seq: 1,
            turn_id: Id128([4; 16]),
            event_type: "note".to_string(),
            payload,
            trace_id: [5; 16],
            span_id: [6; 8],
            observed_unix_ns: 1_000,
        }
    }

    fn attrs(record: &LogRecord) -> Vec<(String, String)> {
        record
            .attributes
            .iter()
            .map(|kv| {
                let value = match kv.value.as_ref().and_then(|v| v.value.as_ref()) {
                    Some(Value::StringValue(s)) => s.clone(),
                    Some(Value::IntValue(i)) => i.to_string(),
                    other => format!("{other:?}"),
                };
                (kv.key.clone(), value)
            })
            .collect()
    }

    /// A redacted event's record names the hash and size and nowhere carries
    /// the payload text; every record carries the event id in its body and
    /// as an attribute, with the trace context set.
    #[test]
    fn a_redacted_record_carries_hash_and_size_and_never_the_payload() {
        let redacted = event(ProjectedPayload {
            hash: Some("abc".to_string()),
            size_bytes: 6,
            body: None,
        });
        let request = logs_request(
            &TenantId::new("acme"),
            &ProjectionBatch {
                events: vec![redacted.clone()],
                turns: Vec::new(),
            },
        );
        let record = &request.resource_logs[0].scope_logs[0].log_records[0];
        let body = format!("{:?}", record.body);
        assert!(body.contains(&format!("event_id={}", redacted.event_id.to_hex())));
        assert!(body.contains("payload_hash=abc payload_size=6 payload=redacted"));
        let attrs = attrs(record);
        assert!(attrs.contains(&("agent.event_id".to_string(), redacted.event_id.to_hex())));
        assert!(attrs.contains(&("agent.payload_hash".to_string(), "abc".to_string())));
        assert!(attrs.contains(&("agent.payload_size".to_string(), "6".to_string())));
        assert_eq!(record.trace_id, vec![5; 16]);
        assert_eq!(record.span_id, vec![6; 8]);
        let resource = format!("{:?}", request.resource_logs[0].resource);
        assert!(resource.contains("ravel-agents") && resource.contains("acme"));
    }
}
