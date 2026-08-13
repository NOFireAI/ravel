//! OTLP-transport-agnostic trace ingest logic shared by the HTTP and gRPC
//! handlers, the span-pipeline counterpart of [`crate::logs_ingest`].

use std::sync::Arc;
use std::time::Duration;

use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTracePartialSuccess, ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use ravel_ingest::{
    AdmissionController, IdempotencyReceipt, LookupOutcome, SpanIngestRouter, SpanWriteError,
    WriteMode, plausible_ingest_clock, read_marker, write_marker,
};
use ravel_maintain::config::DEFAULT_IDEM_DEDUP_WINDOW_HOURS;
use ravel_object_store::ObjectStoreBackend;
use ravel_otlp::{SpanIngestLimits, SpanRejection, normalize_traces};
use ravel_types::{CommitToken, Signal, TenantId};

use crate::otlp_http::{
    MAX_IDEMPOTENCY_KEY_BYTES, encode_commit_tokens, request_ingest_hour_bucket,
};

pub struct SpanIngestState {
    pub router: Arc<SpanIngestRouter>,
    pub limits: SpanIngestLimits,
    pub ack_deadline: Duration,
    /// Tenant admission controller. Spans get no layer-4 series/stream
    /// admission (ADR-0051 excludes them), so this is used only to record the
    /// receiver-clock rejection counter (`reason="clock"`, ADR-0051 amendment,
    /// S1-12); the shared `GatewayState.admission` charges span byte rate.
    pub admission: Arc<AdmissionController>,
    /// Object store, for the idempotency marker read/write (ADR-0051 section
    /// 5), the span-pipeline counterpart of
    /// [`crate::logs_ingest::LogIngestState::store`].
    pub store: Arc<dyn ObjectStoreBackend>,
    /// Recovery-manifest writer (ADR-0050 section 3), `Some` only on a keyed
    /// bucket. Ensured before the first write; `None` (unkeyed) is a no-op.
    pub recovery: Option<Arc<crate::tenancy::RecoveryManifestWriter>>,
    /// Durable shard_count provisioning-record writer (ADR-0050 section 5),
    /// pins the (tenant, Spans) record on the tenant's first span write.
    pub provisioning: Option<Arc<crate::provisioning::ProvisioningRecordWriter>>,
}

pub struct SpanIngestOutcome {
    pub response: ExportTraceServiceResponse,
    pub tokens: Vec<CommitToken>,
    /// Set only on an idempotency replay: the original request's
    /// `x-ravel-commit-token` header value, replayed verbatim. See
    /// [`crate::logs_ingest::LogIngestOutcome::commit_token_header`].
    pub replayed_commit_token: Option<String>,
}

impl SpanIngestOutcome {
    /// The `x-ravel-commit-token` header value: the verbatim replayed value on
    /// a dedup hit, otherwise this request's own encoded tokens.
    pub fn commit_token_header(&self) -> Option<String> {
        self.replayed_commit_token
            .clone()
            .or_else(|| encode_commit_tokens(&self.tokens))
    }
}

/// Failure from [`handle_export_traces`]. Spans get no layer-4 admission
/// (ADR-0051 excludes them), so unlike [`crate::logs_ingest`] the only
/// non-write failure is an invalid idempotency key.
#[derive(Debug, Clone)]
pub enum SpanIngestRequestError {
    /// The receiver's admission-time clock was implausible (ADR-0051
    /// amendment, S1-12). Whole-request HTTP 503 / gRPC `UNAVAILABLE`; the
    /// replica's fault, retryable against a healthy replica.
    ClockImplausible(String),
    /// The supplied `x-ravel-idempotency-key` exceeds
    /// [`crate::otlp_http::MAX_IDEMPOTENCY_KEY_BYTES`]; mapped to HTTP 400 /
    /// gRPC `InvalidArgument`. Rejected rather than truncated.
    InvalidIdempotencyKey {
        len: usize,
    },
    /// The configured `shard_count` disagrees with this (tenant, Spans)'s
    /// durable provisioning record (ADR-0050 section 5). Operator
    /// misconfiguration, not client fault; the request fails.
    Provisioning(String),
    Write(SpanWriteError),
}

impl std::fmt::Display for SpanIngestRequestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpanIngestRequestError::ClockImplausible(msg) => write!(f, "{msg}"),
            SpanIngestRequestError::InvalidIdempotencyKey { len } => write!(
                f,
                "idempotency key is {len} bytes, exceeds the {MAX_IDEMPOTENCY_KEY_BYTES}-byte limit"
            ),
            SpanIngestRequestError::Provisioning(msg) => write!(f, "{msg}"),
            SpanIngestRequestError::Write(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for SpanIngestRequestError {}

impl SpanIngestRequestError {
    /// Whether a client may reasonably retry the whole request. An over-long
    /// key cannot succeed on retry unchanged; a write error defers to
    /// [`SpanWriteError::is_retryable`]; a provisioning mismatch is an operator
    /// misconfiguration and cannot succeed on a naive retry.
    pub fn is_retryable(&self) -> bool {
        match self {
            // The bad clock is the replica's; a retry against a healthy one works.
            SpanIngestRequestError::ClockImplausible(_) => true,
            SpanIngestRequestError::InvalidIdempotencyKey { .. } => false,
            SpanIngestRequestError::Provisioning(_) => false,
            SpanIngestRequestError::Write(err) => err.is_retryable(),
        }
    }
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
    idempotency_key: Option<Vec<u8>>,
) -> Result<SpanIngestOutcome, SpanIngestRequestError> {
    // Receiver-clock plausibility (ADR-0051 amendment, S1-12): checked before
    // any other work; the hour bucket the marker path and normalize both
    // derive is nonsense on a bad clock. Whole-request 503 / UNAVAILABLE,
    // counted reason="clock".
    if let Err(msg) = plausible_ingest_clock(ingest_ts_ns) {
        state
            .admission
            .record_clock_rejection(&tenant, Signal::Spans);
        return Err(SpanIngestRequestError::ClockImplausible(msg));
    }
    // Validate the opt-in idempotency key up front; an over-long key is a
    // typed rejection, never truncated.
    if let Some(key) = &idempotency_key
        && key.len() > MAX_IDEMPOTENCY_KEY_BYTES
    {
        return Err(SpanIngestRequestError::InvalidIdempotencyKey { len: key.len() });
    }
    // Record the tenant's recovery manifest on its first write (ADR-0050
    // section 3), best-effort and off the durability path.
    crate::tenancy::ensure_recovery_manifest(&state.recovery, &tenant, ingest_ts_ns).await;

    // Pin/validate the (tenant, Spans) shard_count provisioning record on first
    // write (ADR-0050 section 5); a hard mismatch fails this request.
    crate::provisioning::ensure_provisioning_record(
        &state.provisioning,
        &tenant,
        ravel_types::Signal::Spans,
        ingest_ts_ns,
    )
    .await
    .map_err(|e| SpanIngestRequestError::Provisioning(e.to_string()))?;
    // One hour-bucket computation shared by the lookup and the marker write.
    let hour_bucket = request_ingest_hour_bucket(ingest_ts_ns);

    // Replay (ADR-0051 section 5): a keyed retry whose marker is still inside
    // the dedup window skips normalize and the router write and returns the
    // stored receipt directly. The lookup runs before any of that work.
    if let (Some(key), Some(bucket)) = (idempotency_key.as_deref(), hour_bucket) {
        match read_marker(
            state.store.as_ref(),
            &tenant,
            Signal::Spans,
            key,
            bucket,
            DEFAULT_IDEM_DEDUP_WINDOW_HOURS,
        )
        .await
        {
            Ok(LookupOutcome::Hit(receipt)) => {
                // A replay reports the original outcome, not a fresh rejection
                // count.
                return Ok(SpanIngestOutcome {
                    response: ExportTraceServiceResponse {
                        partial_success: None,
                    },
                    tokens: Vec::new(),
                    replayed_commit_token: Some(receipt.commit_token),
                });
            }
            // Miss and Corrupt both fail open to the normal write path
            // (ADR-0051 section 5); Corrupt is a miss, never a caller error.
            // Corrupt still gets a signal: it means bytes in object storage
            // failed to decode, unlike a plain Miss which is the expected
            // common case.
            Ok(LookupOutcome::Miss) => {}
            Ok(LookupOutcome::Corrupt) => {
                tracing::warn!("idempotency marker found but failed to decode; treating as a miss");
            }
            Err(err) => {
                tracing::warn!(
                    %err,
                    "idempotency marker lookup failed; proceeding as a normal write"
                );
            }
        }
    }

    let normalized = normalize_traces(request, &state.limits, ingest_ts_ns);
    // `rejected_spans` counts only spans that never reached storage. An
    // attribute-level rejection (one oversized/malformed attribute, an
    // over-long events/links blob, a bad parent_span_id) drops a single field
    // of a span that still lands, and `SpanRejection::rejected_count` returns 0
    // for it, so it does not inflate this total (#364).
    let rejected_spans: usize = normalized.rejected.iter().map(|r| r.rejected_count()).sum();

    // Spans this request actually writes, captured before `spans` is moved.
    let written_count = normalized.spans.len() as u64;

    let receipt = state
        .router
        .write(tenant.clone(), normalized.spans, mode, state.ack_deadline)
        .await
        .map_err(SpanIngestRequestError::Write)?;

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

    // Ordering (ADR-0051 section 5, experiment L6): write the marker after the
    // durable commit and before returning, so a retry of this keyed request
    // finds it. Only a real commit (tokens present) gets a marker.
    if let (Some(key), Some(bucket)) = (idempotency_key.as_deref(), hour_bucket)
        && let Some(commit_token) = encode_commit_tokens(&receipt.tokens)
    {
        let marker = IdempotencyReceipt {
            written_count,
            commit_token,
        };
        if let Err(err) = write_marker(
            state.store.as_ref(),
            &tenant,
            Signal::Spans,
            key,
            bucket,
            &marker,
        )
        .await
        {
            // The data is already durable; a marker-write failure must not
            // fail the response. Log and ack; the retry reingests
            // (at-least-once) since no marker exists.
            tracing::warn!(
                %err,
                "idempotency marker write failed after a durable commit; acking anyway"
            );
        }
    }

    Ok(SpanIngestOutcome {
        response: ExportTraceServiceResponse { partial_success },
        tokens: receipt.tokens,
        replayed_commit_token: None,
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
    use ravel_ingest::{AdmissionController, AdmissionLimits, IngestConfig, SystemClock};
    use ravel_object_store::ObjectStoreBackend;
    use ravel_object_store::memory::MemoryStore;

    /// Fixed post-floor fixture base, 2026-01-01T00:00:00Z in nanoseconds
    /// (ADR-0051 amendment, S1-12): every fixture clock and span timestamp is
    /// anchored to it so the receiver-clock plausibility floor admits the
    /// request. Never `SystemTime::now()`, so tests stay deterministic.
    const BASE_TS_NS: i64 = 1_767_225_600_000_000_000;

    fn state() -> SpanIngestState {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let router = Arc::new(SpanIngestRouter::new(
            IngestConfig {
                shard_count: 1,
                ..IngestConfig::default()
            },
            store.clone(),
            Arc::new(SystemClock),
        ));
        SpanIngestState {
            router,
            limits: SpanIngestLimits::default(),
            ack_deadline: Duration::from_secs(5),
            admission: Arc::new(AdmissionController::new(
                Arc::new(SystemClock),
                AdmissionLimits::default(),
            )),
            store,
            recovery: None,
            provisioning: None,
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
            // Base-relative so the span's end sits inside the admission window
            // around the fixture ingest clock (BASE_TS_NS).
            start_time_unix_nano: BASE_TS_NS as u64,
            end_time_unix_nano: (BASE_TS_NS + 1_000) as u64,
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
            BASE_TS_NS,
            None,
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
            BASE_TS_NS,
            None,
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
        // end precedes start: an InvalidTimeRange rejection, checked before the
        // skew bounds. Base-relative like every other fixture timestamp.
        inverted.start_time_unix_nano = (BASE_TS_NS + 5_000) as u64;
        inverted.end_time_unix_nano = (BASE_TS_NS + 1_000) as u64;
        let request = request(vec![
            span("GET /checkout", vec![string_kv("k", "v")]),
            inverted,
        ]);

        let outcome = handle_export_traces(
            &state,
            TenantId::new("acme"),
            WriteMode::Strict,
            request,
            BASE_TS_NS,
            None,
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
            BASE_TS_NS,
            None,
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
            BASE_TS_NS,
            None,
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

        let outcome = handle_export_traces(
            &state,
            TenantId::new("acme"),
            WriteMode::Strict,
            req,
            BASE_TS_NS,
            None,
        )
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

    #[tokio::test]
    async fn corrupt_marker_falls_through_to_a_normal_write_not_an_error() {
        use ravel_ingest::marker_key;
        use ravel_object_store::{GetRange, PutMode, PutOptions};

        let state = state();
        let tenant = TenantId::new("acme");
        // Just after the span helper's base-relative end (BASE_TS_NS + 1_000),
        // so the event-time skew bounds admit it and the clock floor passes.
        let ingest_ts_ns: i64 = BASE_TS_NS + 2_000;
        let key = b"corrupt-key".to_vec();
        let bucket = request_ingest_hour_bucket(ingest_ts_ns).expect("valid bucket");

        // Seed a marker, then flip a byte so it fails checksum: a later keyed
        // read must see LookupOutcome::Corrupt.
        let seed = IdempotencyReceipt {
            written_count: 99,
            commit_token: "stale".to_string(),
        };
        write_marker(
            state.store.as_ref(),
            &tenant,
            Signal::Spans,
            &key,
            bucket,
            &seed,
        )
        .await
        .expect("seed marker");
        let marker = marker_key(&tenant, Signal::Spans, &key, bucket);
        let mut bytes = state
            .store
            .get(&marker, GetRange::Full)
            .await
            .expect("marker exists")
            .data
            .to_vec();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        state
            .store
            .put(
                &marker,
                bytes.into(),
                PutOptions {
                    mode: PutMode::Overwrite,
                    checksum: None,
                },
            )
            .await
            .expect("overwrite with corrupt bytes");

        // The corrupt marker must be treated as a miss: a fresh normal write,
        // not the stale receipt and not an error.
        let outcome = handle_export_traces(
            &state,
            tenant,
            WriteMode::Strict,
            request(vec![span("GET /x", vec![string_kv("k", "v")])]),
            ingest_ts_ns,
            Some(key),
        )
        .await
        .expect("a corrupt marker is fail-open, never an error to the caller");
        assert!(
            outcome.replayed_commit_token.is_none(),
            "the stale/corrupt receipt must not be replayed"
        );
        assert_eq!(
            outcome.tokens.len(),
            1,
            "the request wrote fresh data through the normal path"
        );
    }

    /// #991: a traces request whose receiver (ingest) clock is below the 2020
    /// floor must be rejected as the whole-request `ClockImplausible` error
    /// (retryable, so the transport maps it to gRPC `UNAVAILABLE` / HTTP 503),
    /// and the `reason="clock"` admission counter for the tenant's Spans signal
    /// must increment. The clock is a fixed sub-floor timestamp, never
    /// `SystemTime::now()`.
    ///
    /// Non-vacuity: delete the `plausible_ingest_clock` guard at the top of
    /// `handle_export_traces` (traces_ingest.rs, the `if let Err(msg) = ...`
    /// block) and this test fails, because the empty request then writes
    /// cleanly and returns `Ok`, and the counter stays at 0.
    #[tokio::test]
    async fn receiver_clock_below_floor_rejects_unavailable_with_reason_clock() {
        use ravel_ingest::MIN_PLAUSIBLE_INGEST_CLOCK_NS;

        let state = state();
        let tenant = TenantId::new("acme");
        // One nanosecond below the 2020 floor: an implausible receiver clock.
        let sub_floor = MIN_PLAUSIBLE_INGEST_CLOCK_NS - 1;

        let err = match handle_export_traces(
            &state,
            tenant.clone(),
            WriteMode::Strict,
            request(Vec::new()),
            sub_floor,
            None,
        )
        .await
        {
            Ok(_) => panic!("a sub-floor receiver clock must reject the whole request"),
            Err(err) => err,
        };

        // (a) The typed rejection is ClockImplausible, and it is retryable, so
        // the transport maps it to gRPC UNAVAILABLE / HTTP 503.
        assert!(
            matches!(err, SpanIngestRequestError::ClockImplausible(_)),
            "expected ClockImplausible, got: {err:?}"
        );
        assert!(
            err.is_retryable(),
            "ClockImplausible is retryable, which the transport maps to UNAVAILABLE/503"
        );

        // (b) The admission rejected counter increments under reason=\"clock\"
        // for this tenant's Spans signal.
        let row = state
            .admission
            .usage_snapshot()
            .into_iter()
            .find(|r| r.tenant_hash == tenant.hash() && r.signal == Signal::Spans)
            .expect("a spans usage row exists after the clock rejection");
        assert_eq!(
            row.requests_rejected_clock_total, 1,
            "the reason=\"clock\" rejected counter incremented exactly once"
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
