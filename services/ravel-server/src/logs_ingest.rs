//! OTLP-transport-agnostic log ingest logic shared by the HTTP and gRPC
//! handlers, the log-pipeline counterpart of [`crate::ingest`].

use std::sync::Arc;
use std::time::Duration;

use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsPartialSuccess, ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use ravel_ingest::{LogIngestRouter, LogWriteError, WriteMode};
use ravel_otlp::{LogIngestLimits, LogRejection, normalize_logs};
use ravel_types::{CommitToken, TenantId};

pub struct LogIngestState {
    pub router: Arc<LogIngestRouter>,
    pub limits: LogIngestLimits,
    pub ack_deadline: Duration,
}

pub struct LogIngestOutcome {
    pub response: ExportLogsServiceResponse,
    pub tokens: Vec<CommitToken>,
}

/// Upper bound on the assembled `error_message` byte length, the same cap and
/// for the same reason as [`crate::ingest`]'s metrics equivalent (#209): a
/// request rejected across many distinct reasons would otherwise produce an
/// unbounded response string even after aggregation collapses identical
/// reasons.
const MAX_ERROR_MESSAGE_BYTES: usize = 4096;

pub async fn handle_export_logs(
    state: &LogIngestState,
    tenant: TenantId,
    mode: WriteMode,
    request: ExportLogsServiceRequest,
    ingest_ts_ns: i64,
) -> Result<LogIngestOutcome, LogWriteError> {
    let normalized = normalize_logs(request, &state.limits, ingest_ts_ns);
    let rejected_count: usize = normalized.rejected.iter().map(|r| r.rejected_count()).sum();

    let receipt = state
        .router
        .write(tenant, normalized.records, mode, state.ack_deadline)
        .await?;

    let partial_success = if rejected_count > 0 {
        let error_message = build_error_message(&normalized.rejected);
        Some(ExportLogsPartialSuccess {
            rejected_log_records: rejected_count as i64,
            error_message,
        })
    } else {
        None
    };

    Ok(LogIngestOutcome {
        response: ExportLogsServiceResponse { partial_success },
        tokens: receipt.tokens,
    })
}

/// Build the OTLP partial-success `error_message` from `rejected`: one entry
/// per distinct reason with the total record count it covers, rather than
/// joining one string per rejected record. Mirrors [`crate::ingest`]'s
/// `build_error_message` for metrics, deliberately duplicated rather than
/// generalized: the two rejection enums are different types with different
/// wording, and the metrics helper is private to a module this change does
/// not touch. The assembled message is capped at [`MAX_ERROR_MESSAGE_BYTES`];
/// if more distinct reasons exist than fit, the message is truncated with a
/// count of how many were omitted.
fn build_error_message(rejected: &[LogRejection]) -> String {
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
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use ravel_ingest::{IngestConfig, SystemClock};
    use ravel_object_store::ObjectStoreBackend;
    use ravel_object_store::memory::MemoryStore;

    fn state() -> LogIngestState {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let router = Arc::new(LogIngestRouter::new(
            IngestConfig {
                shard_count: 1,
                ..IngestConfig::default()
            },
            store,
            Arc::new(SystemClock),
        ));
        LogIngestState {
            router,
            limits: LogIngestLimits::default(),
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

    fn request(records: Vec<LogRecord>) -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![string_kv("service.name", "api")],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    log_records: records,
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn record(body: &str, attrs: Vec<KeyValue>) -> LogRecord {
        LogRecord {
            time_unix_nano: 1_000,
            observed_time_unix_nano: 1_000,
            severity_number: 9,
            severity_text: "INFO".to_string(),
            body: Some(AnyValue {
                value: Some(AnyValueVariant::StringValue(body.to_string())),
            }),
            attributes: attrs,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn one_oversized_attribute_is_reported_and_the_record_still_lands() {
        let state = state();
        let oversized = LogIngestLimits::default().max_attribute_value_len + 1;
        let request = request(vec![record(
            "hello",
            vec![string_kv("huge", &"x".repeat(oversized))],
        )]);

        let outcome = handle_export_logs(
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
        assert_eq!(partial_success.rejected_log_records, 1);
        assert!(
            partial_success.error_message.contains("attribute value is"),
            "got: {}",
            partial_success.error_message
        );
        assert_eq!(
            outcome.tokens.len(),
            1,
            "the record itself is admitted, so one shard commits"
        );
    }

    #[tokio::test]
    async fn all_records_rejected_yields_no_tokens_and_a_partial_success() {
        let state = state();
        // An ArrayValue body is LogRejection::UnsupportedBodyKind, which drops
        // the whole record.
        let mut rec = record("unused", Vec::new());
        rec.body = Some(AnyValue {
            value: Some(AnyValueVariant::ArrayValue(
                opentelemetry_proto::tonic::common::v1::ArrayValue { values: vec![] },
            )),
        });

        let outcome = handle_export_logs(
            &state,
            TenantId::new("acme"),
            WriteMode::Strict,
            request(vec![rec]),
            1_000,
        )
        .await
        .expect("a write with zero admitted records never fails");

        assert!(outcome.tokens.is_empty(), "nothing was admitted to flush");
        let partial_success = outcome
            .response
            .partial_success
            .expect("the record was rejected");
        assert_eq!(partial_success.rejected_log_records, 1);
    }

    #[tokio::test]
    async fn nothing_rejected_reports_no_partial_success() {
        let state = state();
        let outcome = handle_export_logs(
            &state,
            TenantId::new("acme"),
            WriteMode::Buffered,
            request(vec![record("hello", vec![string_kv("k", "v")])]),
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

    #[test]
    fn build_error_message_collapses_one_grouped_reason_into_one_entry_with_count() {
        let rejected = vec![LogRejection::Grouped {
            reason: Box::new(LogRejection::TooManyResourceAttributes {
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
        let rejected: Vec<LogRejection> = (0..10_000)
            .map(|i| LogRejection::MissingAttributeValue {
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
