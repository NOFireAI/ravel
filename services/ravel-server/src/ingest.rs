//! OTLP-transport-agnostic ingest logic shared by the HTTP and gRPC handlers.

use std::sync::Arc;
use std::time::Duration;

use std::collections::HashSet;

use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsPartialSuccess, ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use ravel_ingest::{
    AdmissionController, IngestPoint, IngestRouter, RequestRejection, WriteError, WriteMode,
};
use ravel_otlp::{IngestLimits, Rejection, normalize_metrics};
use ravel_types::{CommitToken, SeriesId, TenantId};

pub struct IngestState {
    pub router: Arc<IngestRouter>,
    pub limits: IngestLimits,
    pub ack_deadline: Duration,
    /// Tenant admission (ADR-0051): series-creation-rate and active-series
    /// cap (layer 4). Body size and byte rate (layers 1-2) are enforced
    /// upstream of `handle_export`, at the transport handler, on undecoded
    /// wire bytes.
    pub admission: Arc<AdmissionController>,
}

pub struct IngestOutcome {
    pub response: ExportMetricsServiceResponse,
    pub tokens: Vec<CommitToken>,
}

/// Failure from [`handle_export`]: either the tenant's admission limits
/// rejected the request (layer 4, ADR-0051) or the write itself failed.
#[derive(Debug, Clone)]
pub enum IngestRequestError {
    /// A whole-request, retryable-later rejection: series-creation-rate
    /// exceeded. No tokens are consumed on rejection.
    Admission(RequestRejection),
    Write(WriteError),
}

impl std::fmt::Display for IngestRequestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IngestRequestError::Admission(rejection) => write!(f, "{}", rejection.reason),
            IngestRequestError::Write(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for IngestRequestError {}

impl IngestRequestError {
    /// Whether a client may reasonably retry the whole request. An
    /// admission rejection is always retryable later, once the tenant's
    /// bucket refills; delegates to [`WriteError::is_retryable`] otherwise.
    pub fn is_retryable(&self) -> bool {
        match self {
            IngestRequestError::Admission(_) => true,
            IngestRequestError::Write(err) => err.is_retryable(),
        }
    }
}

/// Upper bound on the assembled `error_message` byte length. Without a cap,
/// a request rejected across many distinct reasons (e.g. one per data point,
/// each with a different oversized label) would still produce an unbounded
/// response string even after aggregation collapses identical reasons
/// (#209). Chosen to comfortably fit a handful of readable rejection
/// messages (each well under 200 bytes) while staying far under typical
/// HTTP header/body sanity limits.
const MAX_ERROR_MESSAGE_BYTES: usize = 4096;

pub async fn handle_export(
    state: &IngestState,
    tenant: TenantId,
    mode: WriteMode,
    request: ExportMetricsServiceRequest,
    ingest_ts_ns: i64,
) -> Result<IngestOutcome, IngestRequestError> {
    let normalized = normalize_metrics(&tenant, request, &state.limits, ingest_ts_ns);
    let mut rejected_count: usize = normalized.rejected.iter().map(|r| r.rejected_count()).sum();

    // Scalar and native-histogram points arrive in separate vectors; both
    // feed one ingest write so a request's points share a single receipt.
    let mut points: Vec<IngestPoint> =
        Vec::with_capacity(normalized.points.len() + normalized.histogram_points.len());
    points.extend(normalized.points.into_iter().map(IngestPoint::from));
    points.extend(
        normalized
            .histogram_points
            .into_iter()
            .map(IngestPoint::from),
    );

    // Layer 4 (ADR-0051 section 1): series-creation-rate is a whole-request
    // rate limit checked first (breach rejects the whole request, no tokens
    // consumed); the active-series cap that follows is per-series partial
    // success, never a whole-request rejection.
    let candidate_series: Vec<SeriesId> = points.iter().map(|p| p.series_id).collect();
    state
        .admission
        .check_series_creation_rate(&tenant, &candidate_series, ingest_ts_ns)
        .map_err(IngestRequestError::Admission)?;
    let admission = state
        .admission
        .admit_series(&tenant, candidate_series, ingest_ts_ns);
    let series_cap_rejected = admission.rejected.len();
    if series_cap_rejected > 0 {
        let admitted: HashSet<SeriesId> = admission.admitted.into_iter().collect();
        points.retain(|p| admitted.contains(&p.series_id));
        rejected_count += series_cap_rejected;
    }

    let receipt = state
        .router
        .write_values(tenant, points, mode, state.ack_deadline)
        .await
        .map_err(IngestRequestError::Write)?;

    let partial_success = if rejected_count > 0 {
        let error_message = build_error_message(&normalized.rejected, series_cap_rejected);
        Some(ExportMetricsPartialSuccess {
            rejected_data_points: rejected_count as i64,
            error_message,
        })
    } else {
        None
    };

    Ok(IngestOutcome {
        response: ExportMetricsServiceResponse { partial_success },
        tokens: receipt.tokens,
    })
}

/// Build the OTLP partial-success `error_message` from `rejected`: one entry
/// per distinct reason with the total point count it covers, rather than
/// joining one string per rejected point. Distinct reasons are rare relative
/// to rejected points in the pathological case this guards against (a
/// whole-resource or whole-metric rejection covering a huge batch collapses
/// to a single reason), so this stays cheap even when `rejected` is large.
/// The assembled message is capped at [`MAX_ERROR_MESSAGE_BYTES`]; if more
/// distinct reasons exist than fit, the message is truncated with a count of
/// how many were omitted.
///
/// `series_cap_rejected` folds in the layer-4 active-series-cap count (0
/// when nothing was capped) as one more reason, aggregated the same way as
/// every normalization rejection.
fn build_error_message(rejected: &[Rejection], series_cap_rejected: usize) -> String {
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
    if series_cap_rejected > 0 {
        let key = "active series cap exceeded".to_string();
        counts
            .entry(key.clone())
            .and_modify(|count| *count += series_cap_rejected)
            .or_insert_with(|| {
                order.push(key);
                series_cap_rejected
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
    use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
    use ravel_ingest::{AdmissionLimits, IngestConfig, SystemClock};
    use ravel_object_store::ObjectStoreBackend;
    use ravel_object_store::memory::MemoryStore;
    use ravel_types::Signal;

    fn state() -> IngestState {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let router = Arc::new(IngestRouter::new(
            IngestConfig::default(),
            store,
            Signal::Metrics,
            Arc::new(SystemClock),
        ));
        IngestState {
            router,
            limits: IngestLimits::default(),
            ack_deadline: Duration::from_secs(5),
            admission: Arc::new(AdmissionController::new(
                Arc::new(SystemClock),
                AdmissionLimits::default(),
            )),
        }
    }

    fn empty_request() -> ExportMetricsServiceRequest {
        ExportMetricsServiceRequest {
            resource_metrics: vec![],
        }
    }

    #[test]
    fn build_error_message_collapses_one_grouped_reason_into_one_entry_with_count() {
        let rejected = vec![Rejection::Grouped {
            reason: Box::new(Rejection::ComplexAttributeValue),
            count: 50_000,
        }];
        let message = build_error_message(&rejected, 0);
        assert!(message.contains("x50000"), "got: {message}");
        assert!(message.len() < 500);
    }

    #[test]
    fn build_error_message_bounded_for_many_distinct_reasons() {
        // Many structurally distinct rejections (different label names), the
        // kind aggregation-by-identical-reason cannot collapse: the length
        // cap and truncation indicator must still bound the result.
        let rejected: Vec<Rejection> = (0..10_000)
            .map(|i| Rejection::DuplicateLabelName(format!("label_{i}")))
            .collect();
        let message = build_error_message(&rejected, 0);
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

    #[tokio::test]
    async fn handle_export_error_message_stays_bounded_for_huge_rejected_batch() {
        use opentelemetry_proto::tonic::common::v1::AnyValue;
        use opentelemetry_proto::tonic::common::v1::KeyValue;
        use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
        use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
        use opentelemetry_proto::tonic::metrics::v1::{
            Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
            metric::Data as MetricData,
        };
        use opentelemetry_proto::tonic::resource::v1::Resource;

        const POINT_COUNT: usize = 50_000;
        let points: Vec<NumberDataPoint> = (0..POINT_COUNT)
            .map(|i| NumberDataPoint {
                time_unix_nano: 1_000,
                value: Some(NumberValue::AsDouble(i as f64)),
                ..Default::default()
            })
            .collect();
        let rm = ResourceMetrics {
            // A bytes-valued service.name fails whole-resource label
            // building, rejecting every point under it (#209's scenario).
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(AnyValueVariant::BytesValue(vec![1, 2, 3])),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "widgets".to_string(),
                    data: Some(MetricData::Gauge(Gauge {
                        data_points: points,
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![rm],
        };

        let state = state();
        let outcome = handle_export(
            &state,
            TenantId::new("acme"),
            WriteMode::Buffered,
            request,
            1_000,
        )
        .await
        .expect("buffered write with zero admitted points never fails");

        let partial_success = outcome
            .response
            .partial_success
            .expect("all points rejected");
        assert_eq!(partial_success.rejected_data_points, POINT_COUNT as i64);
        assert!(
            partial_success.error_message.len() <= MAX_ERROR_MESSAGE_BYTES + 128,
            "error_message not bounded: {} bytes",
            partial_success.error_message.len()
        );
    }

    #[tokio::test]
    async fn handle_export_reports_no_partial_success_when_nothing_rejected() {
        let state = state();
        let outcome = handle_export(
            &state,
            TenantId::new("acme"),
            WriteMode::Buffered,
            empty_request(),
            1_000,
        )
        .await
        .expect("empty request never fails");
        assert!(outcome.response.partial_success.is_none());
    }
}
