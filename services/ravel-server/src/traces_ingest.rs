//! OTLP-transport-agnostic trace ingest logic shared by the HTTP and gRPC
//! handlers, the span-pipeline counterpart of [`crate::logs_ingest`].

use std::sync::Arc;
use std::time::Duration;

use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTracePartialSuccess, ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use ravel_ingest::{SpanIngestRouter, SpanWriteError, WriteMode};
use ravel_otlp::{SpanIngestLimits, SpanRejection, normalize_traces};
use ravel_types::{CommitToken, TenantId};

pub struct SpanIngestState {
    pub router: Arc<SpanIngestRouter>,
    pub limits: SpanIngestLimits,
    pub ack_deadline: Duration,
}

pub struct SpanIngestOutcome {
    pub response: ExportTraceServiceResponse,
    pub tokens: Vec<CommitToken>,
}

/// Upper bound on the assembled `error_message` byte length, the same cap and
/// for the same reason as [`crate::logs_ingest`]'s log equivalent (#209): a
/// request rejected across many distinct reasons would otherwise produce an
/// unbounded response string even after aggregation collapses identical
/// reasons.
const MAX_ERROR_MESSAGE_BYTES: usize = 4096;

pub async fn handle_export_traces(
    state: &SpanIngestState,
    tenant: TenantId,
    mode: WriteMode,
    request: ExportTraceServiceRequest,
    ingest_ts_ns: i64,
) -> Result<SpanIngestOutcome, SpanWriteError> {
    let normalized = normalize_traces(request, &state.limits, ingest_ts_ns);
    // `rejected_spans` counts only spans that never reached storage. An
    // attribute-level rejection (one oversized/malformed attribute, an
    // over-long events/links blob, a bad parent_span_id) drops a single field
    // of a span that still lands, and `SpanRejection::rejected_count` returns 0
    // for it, so it does not inflate this total (#364).
    let rejected_spans: usize = normalized.rejected.iter().map(|r| r.rejected_count()).sum();

    let receipt = state
        .router
        .write(tenant, normalized.spans, mode, state.ack_deadline)
        .await?;

    // Emit partial-success whenever anything was rejected, not only when a
    // whole span was lost: an attribute-only drop must still be surfaced to the
    // sender through `error_message`, with `rejected_spans` reported as 0 (the
    // OTLP-sanctioned warning channel). Gating on the span count instead would
    // silently swallow attribute drops on an otherwise clean request.
    let partial_success = if normalized.rejected.is_empty() {
        None
    } else {
        let error_message = build_error_message(&normalized.rejected);
        Some(ExportTracePartialSuccess {
            rejected_spans: rejected_spans as i64,
            error_message,
        })
    };

    Ok(SpanIngestOutcome {
        response: ExportTraceServiceResponse { partial_success },
        tokens: receipt.tokens,
    })
}

/// Build the OTLP partial-success `error_message` from `rejected`: one entry
/// per distinct reason with the total span count it covers, rather than
/// joining one string per rejected span. Mirrors [`crate::logs_ingest`]'s
/// `build_error_message`, deliberately duplicated rather than generalized: the
/// rejection enums are different types with different wording, and the logs
/// helper is private to a module this change does not touch. The assembled
/// message is capped at [`MAX_ERROR_MESSAGE_BYTES`]; if more distinct reasons
/// exist than fit, the message is truncated with a count of how many were
/// omitted.
fn build_error_message(rejected: &[SpanRejection]) -> String {
    let mut order: Vec<String> = Vec::new();
    let mut counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for r in rejected {
        let key = r.to_string();
        let n = r.rejected_count();
        counts
            .entry(key.clone())
            .and_modify(|count| *count += n)
            .or_insert_with(|| {
                order.push(key);
                n
            });
    }

    let mut message = String::new();
    let mut shown = 0usize;
    for reason in &order {
        let count = counts[reason];
        let entry = if count > 1 {
            format!("{reason} (x{count})")
        } else {
            reason.clone()
        };
        let sep_len = if message.is_empty() { 0 } else { 2 };
        if message.len() + sep_len + entry.len() > MAX_ERROR_MESSAGE_BYTES {
            break;
        }
        if !message.is_empty() {
            message.push_str("; ");
        }
        message.push_str(&entry);
        shown += 1;
    }

    if shown < order.len() {
        message.push_str(&format!(
            "; ... {} more distinct rejection reason(s) omitted",
            order.len() - shown
        ));
    }

    message
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, ArrayValue, KeyValue};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
    use ravel_ingest::{IngestConfig, SystemClock};
    use ravel_object_store::ObjectStoreBackend;
    use ravel_object_store::memory::MemoryStore;

    fn state() -> SpanIngestState {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let router = Arc::new(SpanIngestRouter::new(
            IngestConfig {
                shard_count: 1,
                ..IngestConfig::default()
            },
            store,
            Arc::new(SystemClock),
        ));
        SpanIngestState {
            router,
            limits: SpanIngestLimits::default(),
            ack_deadline: Duration::from_secs(5),
        }
    }

    fn string_kv(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(AnyValueVariant::StringValue(value.to_string())),
            }),
            ..Default::default()
        }
    }

    fn request(spans: Vec<Span>) -> ExportTraceServiceRequest {
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![string_kv("service.name", "checkout")],
                    ..Default::default()
                }),
                scope_spans: vec![ScopeSpans {
                    spans,
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn span(name: &str, attrs: Vec<KeyValue>) -> Span {
        Span {
            trace_id: vec![7u8; 16],
            span_id: vec![3u8; 8],
            name: name.to_string(),
            start_time_unix_nano: 1_000,
            end_time_unix_nano: 2_000,
            attributes: attrs,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn one_oversized_attribute_is_reported_and_the_span_still_lands() {
        let state = state();
        let oversized = SpanIngestLimits::default().max_attribute_value_len + 1;
        let request = request(vec![span(
            "GET /checkout",
            vec![string_kv("huge", &"x".repeat(oversized))],
        )]);

        let outcome = handle_export_traces(
            &state,
            TenantId::new("acme"),
            WriteMode::Strict,
            request,
            1_000,
        )
        .await
        .expect("strict write publishes");

        let partial_success = outcome
            .response
            .partial_success
            .expect("one attribute rejected");
        assert_eq!(
            partial_success.rejected_spans, 0,
            "the span landed; only one attribute was dropped, so no span was rejected (#364)"
        );
        assert!(
            partial_success.error_message.contains("attribute value is"),
            "the attribute drop is still surfaced through error_message: {}",
            partial_success.error_message
        );
        assert_eq!(
            outcome.tokens.len(),
            1,
            "the span itself is admitted, so one shard commits"
        );
    }

    #[tokio::test]
    async fn one_bad_attribute_among_several_spans_rejects_zero_spans() {
        // N spans exported, exactly one attribute on one of them oversized: the
        // reported rejected_spans must be 0 because every span still lands, while
        // the attribute drop is still surfaced through error_message (#364).
        let state = state();
        let oversized = SpanIngestLimits::default().max_attribute_value_len + 1;
        let request = request(vec![
            span("GET /checkout", vec![string_kv("ok", "v")]),
            span("GET /cart", vec![string_kv("huge", &"x".repeat(oversized))]),
            span("GET /pay", vec![string_kv("ok", "v")]),
        ]);

        let outcome = handle_export_traces(
            &state,
            TenantId::new("acme"),
            WriteMode::Strict,
            request,
            1_000,
        )
        .await
        .expect("strict write publishes");

        assert_eq!(
            outcome.tokens.len(),
            1,
            "all three spans are admitted, so one shard commits"
        );
        let partial_success = outcome
            .response
            .partial_success
            .expect("the dropped attribute is reported");
        assert_eq!(
            partial_success.rejected_spans, 0,
            "no span was lost, only one attribute"
        );
        assert!(
            partial_success.error_message.contains("attribute value is"),
            "got: {}",
            partial_success.error_message
        );
    }

    #[tokio::test]
    async fn a_whole_span_rejection_among_clean_spans_still_counts_one() {
        // Guard against undercounting: a genuine whole-span rejection (inverted
        // time range) mixed with clean spans must still report rejected_spans == 1.
        let state = state();
        let mut inverted = span("inverted", Vec::new());
        inverted.start_time_unix_nano = 5_000;
        inverted.end_time_unix_nano = 1_000;
        let request = request(vec![
            span("GET /checkout", vec![string_kv("k", "v")]),
            inverted,
        ]);

        let outcome = handle_export_traces(
            &state,
            TenantId::new("acme"),
            WriteMode::Strict,
            request,
            1_000,
        )
        .await
        .expect("strict write publishes");

        assert_eq!(
            outcome.tokens.len(),
            1,
            "the one valid span still commits on its shard"
        );
        let partial_success = outcome
            .response
            .partial_success
            .expect("the inverted span is rejected");
        assert_eq!(
            partial_success.rejected_spans, 1,
            "the whole-span rejection still counts as one lost span"
        );
        assert!(
            partial_success.error_message.contains("before it starts"),
            "got: {}",
            partial_success.error_message
        );
    }

    #[tokio::test]
    async fn all_spans_rejected_yields_no_tokens_and_a_partial_success() {
        let state = state();
        // A malformed trace_id drops the whole span: it is RSPAN's sort key.
        let mut bad = span("GET /checkout", Vec::new());
        bad.trace_id = vec![1, 2, 3];

        let outcome = handle_export_traces(
            &state,
            TenantId::new("acme"),
            WriteMode::Strict,
            request(vec![bad]),
            1_000,
        )
        .await
        .expect("a write with zero admitted spans never fails");

        assert!(outcome.tokens.is_empty(), "nothing was admitted to flush");
        let partial_success = outcome
            .response
            .partial_success
            .expect("the span was rejected");
        assert_eq!(partial_success.rejected_spans, 1);
    }

    #[tokio::test]
    async fn nothing_rejected_reports_no_partial_success() {
        let state = state();
        let outcome = handle_export_traces(
            &state,
            TenantId::new("acme"),
            WriteMode::Buffered,
            request(vec![span("GET /checkout", vec![string_kv("k", "v")])]),
            1_000,
        )
        .await
        .expect("buffered write never blocks past enqueue");
        assert!(outcome.response.partial_success.is_none());
        assert!(
            outcome.tokens.is_empty(),
            "buffered mode acks at enqueue, before any commit"
        );
    }

    #[tokio::test]
    async fn an_array_valued_resource_attribute_costs_one_attribute_not_the_spans() {
        // The span pipeline's deliberate departure from the log pipeline: a
        // resource attribute is data here, not identity, so an SDK that sets
        // `process.command_args` still gets its spans stored.
        let state = state();
        let mut req = request(vec![span("GET /checkout", Vec::new())]);
        if let Some(resource) = req.resource_spans[0].resource.as_mut() {
            resource.attributes.push(KeyValue {
                key: "process.command_args".to_string(),
                value: Some(AnyValue {
                    value: Some(AnyValueVariant::ArrayValue(ArrayValue {
                        values: vec![AnyValue {
                            value: Some(AnyValueVariant::StringValue("--serve".into())),
                        }],
                    })),
                }),
                ..Default::default()
            });
        }

        let outcome =
            handle_export_traces(&state, TenantId::new("acme"), WriteMode::Strict, req, 1_000)
                .await
                .expect("strict write publishes");
        assert_eq!(outcome.tokens.len(), 1, "the span still commits");
        let partial_success = outcome
            .response
            .partial_success
            .expect("the dropped attribute is reported");
        assert_eq!(
            partial_success.rejected_spans, 0,
            "the array-valued resource attribute costs one attribute, not the span (#364)"
        );
        assert!(
            partial_success
                .error_message
                .contains("nested array or map"),
            "the attribute drop is still surfaced through error_message: {}",
            partial_success.error_message
        );
    }

    #[test]
    fn build_error_message_collapses_one_grouped_reason_into_one_entry_with_count() {
        let rejected = vec![SpanRejection::Grouped {
            reason: Box::new(SpanRejection::TooManyResourceAttributes {
                count: 200,
                max: 128,
            }),
            count: 50_000,
        }];
        let message = build_error_message(&rejected);
        assert!(message.contains("x50000"), "got: {message}");
        assert!(message.len() < 500);
    }

    #[test]
    fn build_error_message_bounded_for_many_distinct_reasons() {
        // Structurally distinct rejections aggregation cannot collapse: the
        // length cap and truncation indicator must still bound the result.
        let rejected: Vec<SpanRejection> = (0..10_000)
            .map(|i| SpanRejection::MissingAttributeValue {
                key: format!("key_{i}"),
            })
            .collect();
        let message = build_error_message(&rejected);
        assert!(
            message.len() <= MAX_ERROR_MESSAGE_BYTES + 128,
            "message not bounded: {} bytes",
            message.len()
        );
        assert!(
            message.contains("more distinct rejection reason(s) omitted"),
            "expected a truncation indicator, got: {message}"
        );
    }
}
