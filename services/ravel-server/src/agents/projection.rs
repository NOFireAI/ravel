//! The sinks the agents projector delivers to (ADR-3331 decision 12): this
//! process's own log and span ingest routers, or OTLP gRPC to a gateway.
//!
//! Both sinks write the same records, built here from a
//! [`ProjectionBatch`]: one log record per event, its body the
//! [`ProjectedEvent::body`] text so the stable event id is searchable, and
//! one span per turn. The payload reaches a record only through that body
//! and the hash and size attributes, all three built by the projector's
//! redaction gate.
//!
//! Before a batch is sent each record is screened alone against the
//! receiving side's normalization: one that fails for a reason a retry
//! cannot change (older than the ingest-lag bound, malformed, over a size
//! limit) is left out and reported as a rejection, so it never holds up the
//! records behind it. A refusal a retry can get past (a time ahead of the
//! skew bound, admission, a rate, the active-stream cap, a buffer budget,
//! the store, the network) fails the whole batch, which the projector
//! retries under the same ids.

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
use prost::Message;
use ravel_agents_runtime::projection::{
    Delivered, ProjectedEvent, ProjectedTurn, ProjectionBatch, ProjectionSink, SinkError,
    SinkFuture,
};
use ravel_agents_store::Clock;
use ravel_ingest::WriteMode;
use ravel_ipcrypt::rules::TenantRules;
use ravel_otlp::traces_normalize::normalize_traces;
use ravel_otlp::{LogIngestLimits, LogRejection, SpanIngestLimits, SpanRejection, normalize_logs};
use ravel_types::{Signal, TenantId};

use crate::config::ProjectionTap;
use crate::ipcrypt::IpcryptPolicy;
use crate::logs_ingest::handle_export_logs;
use crate::otlp_http::GatewayState;
use crate::traces_ingest::handle_export_traces;

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

fn flag(key: &str) -> KeyValue {
    kv(key, Value::BoolValue(true))
}

/// One event's log record, its body cut to `max_body_len`. The record's
/// time is the event's own, never the delivery time.
fn log_record(event: &ProjectedEvent, max_body_len: usize) -> LogRecord {
    let ts = u64::try_from(event.time_unix_ns).unwrap_or(0);
    let body = event.body_within(max_body_len);
    let mut attributes = vec![
        text("agent.event_id", event.event_id.to_hex()),
        text("agent.id", event.agent.to_hex()),
        int("agent.incarnation", event.incarnation),
        int("agent.event_seq", event.event_seq),
        int("agent.accept_seq", event.accept_seq),
        text("agent.turn_id", event.turn_id.to_hex()),
        text("agent.event_type", event.event_type.clone()),
        text("agent.time_source", event.time_source.as_str()),
    ];
    if let Some(hash) = &event.payload.hash {
        attributes.push(text("agent.payload_hash", hash.clone()));
        attributes.push(int("agent.payload_size", event.payload.size_bytes));
    }
    if body.truncated {
        attributes.push(flag("agent.payload_truncated"));
    }
    LogRecord {
        time_unix_nano: ts,
        observed_time_unix_nano: ts,
        severity_number: SEVERITY_INFO,
        severity_text: "INFO".to_string(),
        body: Some(AnyValue {
            value: Some(Value::StringValue(body.text)),
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

/// The events as one OTLP logs request, each body cut to `max_body_len`.
pub fn logs_request<'a>(
    tenant: &TenantId,
    events: impl IntoIterator<Item = &'a ProjectedEvent>,
    max_body_len: usize,
) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(resource(tenant)),
            scope_logs: vec![ScopeLogs {
                log_records: events
                    .into_iter()
                    .map(|event| log_record(event, max_body_len))
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

/// The turns as one OTLP traces request.
pub fn traces_request<'a>(
    tenant: &TenantId,
    turns: impl IntoIterator<Item = &'a ProjectedTurn>,
) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(resource(tenant)),
            scope_spans: vec![ScopeSpans {
                spans: turns.into_iter().map(span).collect(),
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

/// A rejection a later retry can get past: a time ahead of the skew bound
/// is admitted once the receiver's clock catches up. Every other
/// normalization rejection fails the same record identically on retry.
fn log_rejection_is_transient(rejection: &LogRejection) -> bool {
    match rejection {
        LogRejection::FutureSkew { .. } => true,
        LogRejection::Grouped { reason, .. } => log_rejection_is_transient(reason),
        _ => false,
    }
}

/// [`log_rejection_is_transient`] for spans.
fn span_rejection_is_transient(rejection: &SpanRejection) -> bool {
    match rejection {
        SpanRejection::FutureSkew { .. } => true,
        SpanRejection::Grouped { reason, .. } => span_rejection_is_transient(reason),
        _ => false,
    }
}

/// What is left to send of a batch once each record has been screened.
struct Screened<'a> {
    events: Vec<&'a ProjectedEvent>,
    turns: Vec<&'a ProjectedTurn>,
    delivered: Delivered,
}

/// Normalizes every event and turn on its own under the receiver's limits,
/// rules and clock. A permanent rejection leaves that record out and is
/// counted in the result; a transient one fails the whole batch so the
/// projector retries it. A rejection that only drops one attribute keeps
/// the record, as the ingest path does.
fn screen<'a>(
    tenant: &TenantId,
    batch: &'a ProjectionBatch,
    logs: &LogIngestLimits,
    spans: &SpanIngestLimits,
    rules: &TenantRules,
    now_unix_ns: i64,
) -> Result<Screened<'a>, SinkError> {
    let mut screened = Screened {
        events: Vec::with_capacity(batch.events.len()),
        turns: Vec::with_capacity(batch.turns.len()),
        delivered: Delivered::default(),
    };
    for event in &batch.events {
        let request = logs_request(tenant, [event], logs.max_body_len);
        let out = normalize_logs(request, logs, rules, now_unix_ns);
        match out.rejected.iter().find(|r| r.rejected_count() > 0) {
            None => screened.events.push(event),
            Some(r) if log_rejection_is_transient(r) => {
                return Err(SinkError(format!(
                    "log ingest refused event {} for now: {r}",
                    event.event_id.to_hex()
                )));
            }
            Some(r) => {
                screened.delivered.rejected_events += 1;
                screened.delivered.rejection =
                    Some(format!("event {}: {r}", event.event_id.to_hex()));
            }
        }
    }
    for turn in &batch.turns {
        let out = normalize_traces(traces_request(tenant, [turn]), spans, rules, now_unix_ns);
        match out.rejected.iter().find(|r| r.rejected_count() > 0) {
            None => screened.turns.push(turn),
            Some(r) if span_rejection_is_transient(r) => {
                return Err(SinkError(format!(
                    "span ingest refused turn {} for now: {r}",
                    turn.turn_id.to_hex()
                )));
            }
            Some(r) => {
                screened.delivered.rejected_spans += 1;
                screened.delivered.rejection = Some(format!("turn {}: {r}", turn.turn_id.to_hex()));
            }
        }
    }
    Ok(screened)
}

/// Writes through this process's OTLP export handlers, the ones `/v1/logs`
/// and `/v1/traces` call: byte-rate and stream admission, the buffer
/// budget, the configured ingest-lag bound and the tenant's IPCrypt rules
/// apply as to any client, and a write returns once durably acknowledged.
/// No idempotency key is sent, since a key would replay a partial write
/// and drop its unwritten remainder.
pub struct InProcessSink {
    pub tenant: TenantId,
    pub ingest: Arc<GatewayState>,
    pub clock: Arc<dyn Clock>,
}

impl InProcessSink {
    fn admit_bytes(&self, signal: Signal, bytes: usize, now: i64) -> Result<(), SinkError> {
        let bytes = bytes as u64;
        self.ingest
            .admission
            .check_byte_rate(&self.tenant, signal, bytes, now)
            .map_err(|r| SinkError(format!("{signal:?} ingest byte rate: {}", r.reason)))?;
        self.ingest
            .ingest_byte_metrics
            .record_wire_bytes(&self.tenant, signal, bytes);
        Ok(())
    }

    async fn write(&self, batch: &ProjectionBatch) -> Result<Delivered, SinkError> {
        let now = self.clock.now_unix_ns();
        let logs = &self.ingest.logs_ingest;
        let spans = &self.ingest.traces_ingest;
        let rules = logs
            .ipcrypt
            .rules_for(&self.tenant)
            .map_err(|e| SinkError(format!("tenant IPCrypt rules unavailable: {e}")))?;
        let screened = screen(
            &self.tenant,
            batch,
            &logs.limits,
            &spans.limits,
            &rules,
            now,
        )?;
        if !screened.events.is_empty() {
            let request = logs_request(
                &self.tenant,
                screened.events.iter().copied(),
                logs.limits.max_body_len,
            );
            self.admit_bytes(Signal::Logs, request.encoded_len(), now)?;
            let outcome = handle_export_logs(
                logs,
                self.tenant.clone(),
                WriteMode::StrictArrival,
                request,
                now,
                None,
            )
            .await
            .map_err(|e| SinkError(format!("log ingest refused projected records: {e}")))?;
            if let Some(partial) = outcome.response.partial_success
                && partial.rejected_log_records > 0
            {
                return Err(SinkError(format!(
                    "log ingest refused {} projected records: {}",
                    partial.rejected_log_records, partial.error_message
                )));
            }
        }
        if !screened.turns.is_empty() {
            let request = traces_request(&self.tenant, screened.turns.iter().copied());
            self.admit_bytes(Signal::Spans, request.encoded_len(), now)?;
            let outcome = handle_export_traces(
                spans,
                self.tenant.clone(),
                WriteMode::StrictArrival,
                request,
                now,
                None,
            )
            .await
            .map_err(|e| SinkError(format!("span ingest refused projected spans: {e}")))?;
            if let Some(partial) = outcome.response.partial_success
                && partial.rejected_spans > 0
            {
                return Err(SinkError(format!(
                    "span ingest refused {} projected spans: {}",
                    partial.rejected_spans, partial.error_message
                )));
            }
        }
        Ok(screened.delivered)
    }
}

impl ProjectionSink for InProcessSink {
    fn deliver<'a>(&'a self, batch: &'a ProjectionBatch) -> SinkFuture<'a> {
        Box::pin(self.write(batch))
    }
}

/// Sends OTLP gRPC to a gateway, the tenant named in [`TENANT_HEADER`].
/// Records are screened first under the default OTLP limits, no IPCrypt
/// rules and this process's clock; the gateway applies its own, and a
/// record it refuses fails the batch, retried like an outage.
pub struct OtlpSink {
    pub tenant: TenantId,
    pub endpoint: String,
    clock: Arc<dyn Clock>,
    /// The empty policy: the gateway applies the tenant's rules.
    ipcrypt: Arc<IpcryptPolicy>,
    channel: Mutex<Option<tonic::transport::Channel>>,
}

impl OtlpSink {
    pub fn new(tenant: TenantId, endpoint: String, clock: Arc<dyn Clock>) -> OtlpSink {
        OtlpSink {
            tenant,
            endpoint,
            clock,
            ipcrypt: IpcryptPolicy::disabled(),
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

    async fn send(&self, batch: &ProjectionBatch) -> Result<Delivered, SinkError> {
        let logs = LogIngestLimits::default();
        let rules = self
            .ipcrypt
            .rules_for(&self.tenant)
            .map_err(|e| SinkError(e.to_string()))?;
        let screened = screen(
            &self.tenant,
            batch,
            &logs,
            &SpanIngestLimits::default(),
            &rules,
            self.clock.now_unix_ns(),
        )?;
        let channel = self.channel()?;
        if !screened.events.is_empty() {
            let request = logs_request(
                &self.tenant,
                screened.events.iter().copied(),
                logs.max_body_len,
            );
            let response = LogsServiceClient::new(channel.clone())
                .export(self.request(request)?)
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
        if !screened.turns.is_empty() {
            let request = traces_request(&self.tenant, screened.turns.iter().copied());
            let response = TraceServiceClient::new(channel)
                .export(self.request(request)?)
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
        Ok(screened.delivered)
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
    use ravel_agents_runtime::projection::{ProjectedPayload, TimeSource};

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
            time_unix_ns: 1_000,
            time_source: TimeSource::TurnFinished,
        }
    }

    fn batch(events: Vec<ProjectedEvent>) -> ProjectionBatch {
        ProjectionBatch {
            events,
            turns: Vec::new(),
        }
    }

    const HOUR_NS: i64 = 3_600_000_000_000;

    fn no_payload() -> ProjectedPayload {
        ProjectedPayload {
            hash: None,
            size_bytes: 0,
            body: None,
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
        let request = logs_request(&TenantId::new("acme"), [&redacted], usize::MAX);
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
        assert!(attrs.contains(&("agent.time_source".to_string(), "turn_finished".to_string())));
        assert_eq!(record.time_unix_nano, 1_000);
        assert_eq!(record.observed_time_unix_nano, 1_000);
    }

    /// A carried payload over the body limit is cut to fit and the record
    /// says so, in the body and as an attribute; normalization then accepts
    /// it instead of rejecting it as too long.
    #[test]
    fn a_carried_payload_over_the_body_limit_is_cut_and_marked() {
        let limits = LogIngestLimits::default();
        let big = event(ProjectedPayload {
            hash: Some("abc".to_string()),
            size_bytes: 70_000,
            body: Some("x".repeat(70_000)),
        });
        let request = logs_request(&TenantId::new("acme"), [&big], limits.max_body_len);
        let record = &request.resource_logs[0].scope_logs[0].log_records[0];
        let Some(Value::StringValue(body)) = record.body.as_ref().and_then(|b| b.value.clone())
        else {
            panic!("string body expected");
        };
        assert!(body.len() <= limits.max_body_len, "{} bytes", body.len());
        assert!(body.contains("payload_truncated=true"));
        assert!(attrs(record).contains(&(
            "agent.payload_truncated".to_string(),
            "Some(BoolValue(true))".to_string()
        )));
        let tenant = TenantId::new("acme");
        let batch = batch(vec![big]);
        let screened = screen(
            &tenant,
            &batch,
            &limits,
            &SpanIngestLimits::default(),
            &TenantRules::none(),
            1_000,
        )
        .expect("a cut body is admitted");
        assert_eq!(screened.events.len(), 1);
        assert_eq!(screened.delivered, Delivered::default());
    }

    /// A record older than the lag bound is left out and counted, the rest
    /// of the batch still sent; one ahead of the skew bound fails the batch
    /// so it is retried rather than skipped.
    #[test]
    fn screening_skips_a_permanent_rejection_and_fails_on_a_transient_one() {
        let tenant = TenantId::new("acme");
        let limits = LogIngestLimits::default();
        let now = 1_000 * HOUR_NS;
        let mut old = event(no_payload());
        old.time_unix_ns = now - limits.max_ingest_lag_ns - HOUR_NS;
        let mut fresh = event(no_payload());
        fresh.event_id = Id128([9; 16]);
        fresh.time_unix_ns = now;
        let batch_of_two = batch(vec![old.clone(), fresh]);
        let screened = screen(
            &tenant,
            &batch_of_two,
            &limits,
            &SpanIngestLimits::default(),
            &TenantRules::none(),
            now,
        )
        .expect("a permanent rejection does not fail the batch");
        assert_eq!(screened.events.len(), 1);
        assert_eq!(screened.events[0].event_id, Id128([9; 16]));
        assert_eq!(screened.delivered.rejected_events, 1);
        let reason = screened.delivered.rejection.expect("a reason is kept");
        assert!(reason.contains(&old.event_id.to_hex()), "{reason}");
        assert!(reason.contains("max ingest lag"), "{reason}");

        let mut ahead = event(no_payload());
        ahead.time_unix_ns = now + limits.max_future_skew_ns + HOUR_NS;
        let Err(SinkError(error)) = screen(
            &tenant,
            &batch(vec![ahead]),
            &limits,
            &SpanIngestLimits::default(),
            &TenantRules::none(),
            now,
        ) else {
            panic!("a record ahead of the skew bound must fail the batch");
        };
        assert!(error.contains("future skew"), "{error}");
    }
}
