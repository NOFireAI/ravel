//! OTLP-transport-agnostic ingest logic shared by the HTTP and gRPC handlers.

use std::sync::Arc;
use std::time::Duration;

use std::collections::HashSet;

use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsPartialSuccess, ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use ravel_ingest::{
    AdmissionController, IngestExemplar, IngestPoint, IngestRouter, MetadataSink, RequestRejection,
    WriteError, WriteMode, plausible_ingest_clock,
};
use ravel_otlp::normalize::normalize_metrics_with_metadata;
use ravel_otlp::{IngestLimits, Rejection};
use ravel_types::{CommitToken, ExemplarCap, SeriesId, TenantId};

pub struct IngestState {
    pub router: Arc<IngestRouter>,
    pub limits: IngestLimits,
    pub ack_deadline: Duration,
    /// Tenant admission (ADR-0051): series-creation-rate and active-series
    /// cap (layer 4). Body size and byte rate (layers 1-2) are enforced
    /// upstream of `handle_export`, at the transport handler, on undecoded
    /// wire bytes.
    pub admission: Arc<AdmissionController>,
    /// Recovery-manifest writer (ADR-0050 section 3), `Some` only on a keyed
    /// bucket. `handle_export` ensures the tenant's manifest before its first
    /// write; `None` (an unkeyed bucket) is a no-op.
    pub recovery: Option<Arc<crate::tenancy::RecoveryManifestWriter>>,
    /// Durable shard_count provisioning-record writer (ADR-0050 section 5).
    /// `handle_export` pins the (tenant, Metrics) record on the tenant's first
    /// write and fails the request on a `shard_count` mismatch. `Some` in the
    /// ingest modes; `None` (e.g. a unit test) is a no-op.
    pub provisioning: Option<Arc<crate::provisioning::ProvisioningRecordWriter>>,
    /// The one-per-process metric metadata sink (ADR-0085 decision 1). Every
    /// ingest surface shares this one `Arc`; `handle_export` hands it what
    /// normalization decoded, synchronously and off the acknowledgement path.
    /// `None` in a unit test or a mode that captures no metadata.
    pub metadata_sink: Option<Arc<MetadataSink>>,
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
    /// The receiver's admission-time clock was implausible (below the 2020
    /// floor or non-representable, ADR-0051 amendment). No per-record
    /// decision is meaningful when the reference clock is nonsense, so the
    /// whole request is rejected with HTTP 503 / gRPC `UNAVAILABLE`; the fault
    /// is the replica's, and a retry against a healthy replica succeeds.
    ClockImplausible(String),
    /// The configured `shard_count` disagrees with this (tenant, signal)'s
    /// durable provisioning record (ADR-0050 section 5). The request fails
    /// rather than writing into a shard topology that hides the tenant's
    /// existing data; an operator must reconcile config with the record.
    Provisioning(String),
    Write(WriteError),
}

impl std::fmt::Display for IngestRequestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IngestRequestError::Admission(rejection) => write!(f, "{}", rejection.reason),
            IngestRequestError::ClockImplausible(msg) => write!(f, "{msg}"),
            IngestRequestError::Provisioning(msg) => write!(f, "{msg}"),
            IngestRequestError::Write(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for IngestRequestError {}

impl IngestRequestError {
    /// Whether a client may reasonably retry the whole request. An
    /// admission rejection is always retryable later, once the tenant's
    /// bucket refills; delegates to [`WriteError::is_retryable`] otherwise. A
    /// provisioning mismatch is an operator misconfiguration, not a transient
    /// condition, so a naive client retry cannot succeed.
    pub fn is_retryable(&self) -> bool {
        match self {
            IngestRequestError::Admission(_) => true,
            // The bad clock is the replica's; a retry against a healthy one works.
            IngestRequestError::ClockImplausible(_) => true,
            IngestRequestError::Provisioning(_) => false,
            IngestRequestError::Write(err) => err.is_retryable(),
        }
    }
}

/// Upper bound on the assembled `error_message` byte length. Without a cap,
/// a request rejected across many distinct reasons (e.g. one per data point,
/// each with a different oversized label) would still produce an unbounded
/// response string even after aggregation collapses identical reasons.
/// Chosen to comfortably fit a handful of readable rejection
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
    // Receiver-clock plausibility (ADR-0051 amendment): the admission
    // clock must sit above the 2020 floor and yield a representable hour bucket
    // before it is used to build the normalize context. Checked first, before
    // any work: a nonsense reference clock makes no per-record decision
    // meaningful. Whole-request 503 / UNAVAILABLE, counted reason="clock".
    if let Err(msg) = plausible_ingest_clock(ingest_ts_ns) {
        state
            .admission
            .record_clock_rejection(&tenant, ravel_types::Signal::Metrics);
        return Err(IngestRequestError::ClockImplausible(msg));
    }
    // Record the tenant's recovery manifest on its first write in this process
    // (ADR-0050 section 3). Best-effort and off the durability path: see
    // `crate::tenancy::ensure_recovery_manifest`.
    crate::tenancy::ensure_recovery_manifest(&state.recovery, &tenant, ingest_ts_ns).await;

    // Pin (and validate) the (tenant, Metrics) shard_count provisioning record
    // on the tenant's first write (ADR-0050 section 5). A hard mismatch fails
    // this one request with a typed error; a store blip or corrupt record is
    // logged inside `ensure_provisioning_record` and ingest proceeds.
    crate::provisioning::ensure_provisioning_record(
        &state.provisioning,
        &tenant,
        ravel_types::Signal::Metrics,
        ingest_ts_ns,
    )
    .await
    .map_err(|e| IngestRequestError::Provisioning(e.to_string()))?;

    // Admit points AND exemplars through a per-request cap, rather than the
    // `normalize_metrics` wrapper that builds the same throwaway cap and then
    // counts every admitted exemplar as dropped because it has nowhere to
    // store them. ADR-0047 decision 2 enforces the real per-series-per-window
    // budget per shard actor, with no cross-shard coordination
    // (`crates/ravel-ingest/src/shard.rs`); a transport-level cap only needs
    // to exist long enough to admit this request's exemplars into that path,
    // so it is built fresh here and dropped at the end of the call, exactly
    // like every other OTLP normalize entry point. A tenant-lived cap at this
    // layer was tried and reverted: `ExemplarCap`'s per-series map has no
    // eviction, so anything that outlives one request grows unbounded with
    // tenant x lifetime series cardinality (the same growth vector
    // `shard.rs`'s per-flush cap comment already rejected building one layer
    // lower).
    let mut cap = ExemplarCap::new(state.limits.exemplar_cap_window_ns);
    let (result, metadata) =
        normalize_metrics_with_metadata(&tenant, request, &state.limits, ingest_ts_ns, &mut cap);
    // Capture this request's `(family, type, help, unit)` tuples (ADR-0085
    // decision 1). Synchronous, no I/O, infallible: it only compares
    // fingerprints and stores the changed ones for the background flush window,
    // so it is deliberately called here, right after normalization, rather than
    // anywhere near the write's await. A point is acked on its data write and is
    // never blocked on or failed by the metadata record.
    if let Some(sink) = &state.metadata_sink {
        sink.observe(&tenant, tenant.hash(), metadata);
    }
    let exemplars: Vec<IngestExemplar> = result
        .exemplars
        .into_iter()
        .map(IngestExemplar::from)
        .collect();
    let normalized = result.output;
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
        .write_values_with_exemplars(tenant, points, exemplars, mode, state.ack_deadline)
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

    /// Fixed post-floor fixture base, 2026-01-01T00:00:00Z in nanoseconds
    /// (ADR-0051 amendment): the fixture ingest clock anchors to it so
    /// the receiver-clock plausibility floor admits the request. Never
    /// `SystemTime::now()`, so tests stay deterministic.
    const BASE_TS_NS: i64 = 1_767_225_600_000_000_000;

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
            recovery: None,
            provisioning: None,
            metadata_sink: None,
        }
    }

    /// An ingest state whose metric metadata sink writes to the same store the
    /// router does, plus that store and sink, so a test can drive the real
    /// `handle_export` and then flush the window it armed.
    fn state_with_metadata_sink() -> (IngestState, Arc<dyn ObjectStoreBackend>, Arc<MetadataSink>) {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let router = Arc::new(IngestRouter::new(
            IngestConfig::default(),
            store.clone(),
            Signal::Metrics,
            Arc::new(SystemClock),
        ));
        let sink = Arc::new(MetadataSink::new(
            store.clone(),
            ravel_ingest::MetadataSinkConfig::default(),
            router.metrics_handle(),
        ));
        let state = IngestState {
            router,
            limits: IngestLimits::default(),
            ack_deadline: Duration::from_secs(5),
            admission: Arc::new(AdmissionController::new(
                Arc::new(SystemClock),
                AdmissionLimits::default(),
            )),
            recovery: None,
            provisioning: None,
            metadata_sink: Some(sink.clone()),
        };
        (state, store, sink)
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
                time_unix_nano: BASE_TS_NS as u64,
                value: Some(NumberValue::AsDouble(i as f64)),
                ..Default::default()
            })
            .collect();
        let rm = ResourceMetrics {
            // A bytes-valued service.name fails whole-resource label
            // building, rejecting every point under it.
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
            BASE_TS_NS,
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

    /// The OTLP surface must hand every ingested family's `(type, help, unit)`
    /// to the process metadata sink (ADR-0085 decision 1), keyed by the family
    /// name after the Decision 2 suffix pass. Drives the real `handle_export`,
    /// then flushes the window it armed and reads the durable record back: this
    /// is the wiring, not the sink's own behavior (unit-tested in
    /// `ravel-ingest`), that it proves.
    #[tokio::test]
    async fn handle_export_observes_metric_metadata_for_the_flush_window() {
        use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
        use opentelemetry_proto::tonic::metrics::v1::{
            Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, metric::Data as MetricData,
        };

        let (state, store, sink) = state_with_metadata_sink();
        let tenant = TenantId::new("acme");
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "http_request_size".to_string(),
                        description: "size of each request".to_string(),
                        unit: "By".to_string(),
                        data: Some(MetricData::Sum(Sum {
                            data_points: vec![NumberDataPoint {
                                time_unix_nano: BASE_TS_NS as u64,
                                value: Some(NumberValue::AsDouble(1.0)),
                                ..Default::default()
                            }],
                            aggregation_temporality: 2,
                            is_monotonic: true,
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        handle_export(
            &state,
            tenant.clone(),
            WriteMode::Buffered,
            request,
            BASE_TS_NS,
        )
        .await
        .expect("buffered write succeeds");

        let summary = sink.flush_once(BASE_TS_NS).await;
        assert_eq!(summary.tenants_flushed, 1);
        assert_eq!(summary.gets, 1);
        assert_eq!(summary.puts, 1);

        let entries = ravel_catalog::read_metrics_meta(store.as_ref(), &tenant.hash())
            .await
            .expect("read metadata record")
            .expect("record written by the flush")
            .0;
        assert_eq!(entries.len(), 1);
        // Suffixed name (unit word, then `_total` for a monotonic Sum), the
        // mapped unit word, and the OTLP description as help.
        assert_eq!(entries[0].family_name, "http_request_size_bytes_total");
        assert_eq!(entries[0].kind, ravel_catalog::MetricKind::Counter);
        assert_eq!(entries[0].help, "size of each request");
        assert_eq!(entries[0].unit, "bytes");
    }

    #[tokio::test]
    async fn handle_export_reports_no_partial_success_when_nothing_rejected() {
        let state = state();
        let outcome = handle_export(
            &state,
            TenantId::new("acme"),
            WriteMode::Buffered,
            empty_request(),
            BASE_TS_NS,
        )
        .await
        .expect("empty request never fails");
        assert!(outcome.response.partial_success.is_none());
    }

    /// On a keyed bucket, a tenant's first ingest write must
    /// record its recovery manifest. This drives the real `handle_export`
    /// through an `IngestState` carrying a `RecoveryManifestWriter`, then
    /// asserts `sys/t/<tenant_hash>` exists and decrypts to the tenant id. It is
    /// the wiring, not the writer's own round-trip (unit-tested in `tenancy`),
    /// that this proves.
    #[tokio::test]
    async fn first_ingest_write_records_recovery_manifest_on_keyed_bucket() {
        use ravel_object_store::GetRange;
        use ravel_types::TenantHashScheme;

        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let key = Box::new([0x5Au8; 32]);
        let writer = Arc::new(crate::tenancy::RecoveryManifestWriter::new(
            store.clone(),
            key.clone(),
        ));
        let router = Arc::new(IngestRouter::new(
            IngestConfig::default(),
            store.clone(),
            Signal::Metrics,
            Arc::new(SystemClock),
        ));
        let state = IngestState {
            router,
            limits: IngestLimits::default(),
            ack_deadline: Duration::from_secs(5),
            admission: Arc::new(AdmissionController::new(
                Arc::new(SystemClock),
                AdmissionLimits::default(),
            )),
            recovery: Some(writer),
            provisioning: None,
            metadata_sink: None,
        };

        let tenant = TenantId::new("acme");
        handle_export(
            &state,
            tenant.clone(),
            WriteMode::Buffered,
            empty_request(),
            BASE_TS_NS,
        )
        .await
        .expect("ingest succeeds");

        let hash = TenantHashScheme::v2_from_deployment_key(&key).hash(&tenant);
        let obj = store
            .get(
                &crate::tenancy::recovery_manifest_key(&hash),
                GetRange::Full,
            )
            .await
            .expect("recovery manifest must exist after the first write");
        let recovered = crate::tenancy::open_recovery_manifest(&key, &hash, &obj.data)
            .expect("manifest decrypts under the deployment key");
        assert_eq!(recovered, tenant, "manifest recovers the tenant id");
    }

    /// a metrics/OTLP request whose receiver (ingest) clock is below the
    /// 2020 floor must be rejected as the whole-request `ClockImplausible`
    /// error (which the transport maps to gRPC `UNAVAILABLE` / HTTP 503, via
    /// `is_retryable() == true`), and the `reason="clock"` admission counter
    /// for the tenant's Metrics signal must increment. The clock is injected as
    /// a fixed sub-floor timestamp, never `SystemTime::now()`, so the test is
    /// deterministic.
    ///
    /// Non-vacuity: delete the `plausible_ingest_clock` guard at the top of
    /// `handle_export` (ingest.rs, the `if let Err(msg) = ...` block) and this
    /// test fails, because the empty request then writes cleanly and returns
    /// `Ok` instead of the expected error, and the counter stays at 0.
    #[tokio::test]
    async fn receiver_clock_below_floor_rejects_unavailable_with_reason_clock() {
        use ravel_ingest::MIN_PLAUSIBLE_INGEST_CLOCK_NS;

        let state = state();
        let tenant = TenantId::new("acme");
        // One nanosecond below the 2020 floor: an implausible receiver clock.
        let sub_floor = MIN_PLAUSIBLE_INGEST_CLOCK_NS - 1;

        // `IngestOutcome` is not `Debug`, so match rather than `expect_err`.
        let err = match handle_export(
            &state,
            tenant.clone(),
            WriteMode::Strict,
            empty_request(),
            sub_floor,
        )
        .await
        {
            Ok(_) => panic!("a sub-floor receiver clock must reject the whole request"),
            Err(err) => err,
        };

        // (a) The typed rejection is ClockImplausible, and it is retryable, so
        // the transport maps it to gRPC UNAVAILABLE / HTTP 503.
        assert!(
            matches!(err, IngestRequestError::ClockImplausible(_)),
            "expected ClockImplausible, got: {err:?}"
        );
        assert!(
            err.is_retryable(),
            "ClockImplausible is retryable, which the transport maps to UNAVAILABLE/503"
        );

        // (b) The admission rejected counter increments under reason=\"clock\"
        // for this tenant's Metrics signal.
        let row = state
            .admission
            .usage_snapshot()
            .into_iter()
            .find(|r| r.tenant_hash == tenant.hash() && r.signal == Signal::Metrics)
            .expect("a metrics usage row exists after the clock rejection");
        assert_eq!(
            row.requests_rejected_clock_total, 1,
            "the reason=\"clock\" rejected counter incremented exactly once"
        );
    }
}
