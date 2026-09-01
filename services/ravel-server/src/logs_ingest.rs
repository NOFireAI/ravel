//! OTLP-transport-agnostic log ingest logic shared by the HTTP and gRPC
//! handlers, the log-pipeline counterpart of [`crate::ingest`].

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsPartialSuccess, ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use ravel_ingest::{
    AdmissionController, IdempotencyReceipt, LogIngestRouter, LogWriteError, LookupOutcome,
    RequestRejection, WriteMode, plausible_ingest_clock, read_marker, write_marker,
};
use ravel_maintain::config::DEFAULT_IDEM_DEDUP_WINDOW_HOURS;
use ravel_object_store::ObjectStoreBackend;
use ravel_otlp::{LogIngestLimits, LogRejection, normalize_logs};
use ravel_types::logstream::LogStreamId;
use ravel_types::{CommitToken, Signal, TenantId};

use crate::otlp_http::{
    MAX_IDEMPOTENCY_KEY_BYTES, encode_commit_tokens, request_ingest_hour_bucket,
};

pub struct LogIngestState {
    pub router: Arc<LogIngestRouter>,
    pub limits: LogIngestLimits,
    pub ack_deadline: Duration,
    /// Tenant admission (ADR-0051): stream-creation-rate and active-stream
    /// cap (layer 4), the log-pipeline counterpart of
    /// [`crate::ingest::IngestState::admission`].
    pub admission: Arc<AdmissionController>,
    /// Object store, for the idempotency marker read/write (ADR-0051 section
    /// 5). The router owns its own handle for flushes; the marker path needs
    /// one at the gateway, outside any shard actor.
    pub store: Arc<dyn ObjectStoreBackend>,
    /// Recovery-manifest writer (ADR-0050 section 3), `Some` only on a keyed
    /// bucket. Ensured before the first write; `None` (unkeyed) is a no-op.
    pub recovery: Option<Arc<crate::tenancy::RecoveryManifestWriter>>,
    /// Durable shard_count provisioning-record writer (ADR-0050 section 5),
    /// pins the (tenant, Logs) record on the tenant's first log write.
    pub provisioning: Option<Arc<crate::provisioning::ProvisioningRecordWriter>>,
}

#[derive(Debug)]
pub struct LogIngestOutcome {
    pub response: ExportLogsServiceResponse,
    pub tokens: Vec<CommitToken>,
    /// Set only on an idempotency replay: the `x-ravel-commit-token` header
    /// value the original request produced, stored in the marker and replayed
    /// verbatim. `None` on a normal write, whose header is built from
    /// [`Self::tokens`]. See [`Self::commit_token_header`].
    pub replayed_commit_token: Option<String>,
}

impl LogIngestOutcome {
    /// The `x-ravel-commit-token` header value for this outcome: the verbatim
    /// replayed value on a dedup hit, otherwise the encoding of this request's
    /// own tokens. Keeps the two transports from re-deriving the choice.
    pub fn commit_token_header(&self) -> Option<String> {
        self.replayed_commit_token
            .clone()
            .or_else(|| encode_commit_tokens(&self.tokens))
    }
}

/// Failure from [`handle_export_logs`], the log-pipeline counterpart of
/// [`crate::ingest::IngestRequestError`].
#[derive(Debug, Clone)]
pub enum LogIngestRequestError {
    /// A whole-request, retryable-later rejection: stream-creation-rate
    /// exceeded. No tokens are consumed on rejection.
    Admission(RequestRejection),
    /// The receiver's admission-time clock was implausible (ADR-0051
    /// amendment). Whole-request HTTP 503 / gRPC `UNAVAILABLE`; the
    /// replica's fault, retryable against a healthy replica.
    ClockImplausible(String),
    /// The supplied `x-ravel-idempotency-key` exceeds
    /// [`crate::otlp_http::MAX_IDEMPOTENCY_KEY_BYTES`]. Not retryable as-is
    /// (the client must send a shorter key); mapped to HTTP 400 / gRPC
    /// `InvalidArgument`. Rejected rather than truncated: truncation would
    /// silently merge two distinct keys into one dedup identity.
    InvalidIdempotencyKey {
        len: usize,
    },
    /// The configured `shard_count` disagrees with this (tenant, Logs)'s
    /// durable provisioning record (ADR-0050 section 5). Operator
    /// misconfiguration, not client fault; the request fails.
    Provisioning(String),
    Write(LogWriteError),
}

impl std::fmt::Display for LogIngestRequestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogIngestRequestError::Admission(rejection) => write!(f, "{}", rejection.reason),
            LogIngestRequestError::ClockImplausible(msg) => write!(f, "{msg}"),
            LogIngestRequestError::InvalidIdempotencyKey { len } => write!(
                f,
                "idempotency key is {len} bytes, exceeds the {MAX_IDEMPOTENCY_KEY_BYTES}-byte limit"
            ),
            LogIngestRequestError::Provisioning(msg) => write!(f, "{msg}"),
            LogIngestRequestError::Write(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for LogIngestRequestError {}

impl LogIngestRequestError {
    /// Whether a client may reasonably retry the whole request; mirrors
    /// [`crate::ingest::IngestRequestError::is_retryable`].
    pub fn is_retryable(&self) -> bool {
        match self {
            LogIngestRequestError::Admission(_) => true,
            // The bad clock is the replica's; a retry against a healthy one works.
            LogIngestRequestError::ClockImplausible(_) => true,
            // Retrying the identical over-long key cannot succeed; the client
            // must change it.
            LogIngestRequestError::InvalidIdempotencyKey { .. } => false,
            // An operator misconfiguration, not transient.
            LogIngestRequestError::Provisioning(_) => false,
            LogIngestRequestError::Write(err) => err.is_retryable(),
        }
    }
}

/// Upper bound on the assembled `error_message` byte length, the same cap and
/// for the same reason as [`crate::ingest`]'s metrics equivalent: a
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
    idempotency_key: Option<Vec<u8>>,
) -> Result<LogIngestOutcome, LogIngestRequestError> {
    // Receiver-clock plausibility (ADR-0051 amendment): checked before
    // any other work, since the hour bucket the marker path and normalize both
    // derive is nonsense on a bad clock. Whole-request 503 / UNAVAILABLE,
    // counted reason="clock".
    if let Err(msg) = plausible_ingest_clock(ingest_ts_ns) {
        state
            .admission
            .record_clock_rejection(&tenant, Signal::Logs);
        return Err(LogIngestRequestError::ClockImplausible(msg));
    }
    // Validate the opt-in idempotency key up front: an over-long key is a
    // typed rejection, never truncated (silent truncation would collapse two
    // distinct keys into one dedup identity).
    if let Some(key) = &idempotency_key
        && key.len() > MAX_IDEMPOTENCY_KEY_BYTES
    {
        return Err(LogIngestRequestError::InvalidIdempotencyKey { len: key.len() });
    }
    // Record the tenant's recovery manifest on its first write (ADR-0050
    // section 3), best-effort and off the durability path.
    crate::tenancy::ensure_recovery_manifest(&state.recovery, &tenant, ingest_ts_ns).await;

    // Pin/validate the (tenant, Logs) shard_count provisioning record on first
    // write (ADR-0050 section 5); a hard mismatch fails this request.
    crate::provisioning::ensure_provisioning_record(
        &state.provisioning,
        &tenant,
        ravel_types::Signal::Logs,
        ingest_ts_ns,
    )
    .await
    .map_err(|e| LogIngestRequestError::Provisioning(e.to_string()))?;
    // One hour-bucket computation, shared by the lookup and the marker write
    // so they cannot drift within a request (see `request_ingest_hour_bucket`).
    let hour_bucket = request_ingest_hour_bucket(ingest_ts_ns);

    // Replay (ADR-0051 section 5): a keyed retry whose marker is still inside
    // the dedup window skips admission, normalize, and the router write, and
    // returns the stored receipt directly. `read_marker` runs before any of
    // that work, per the ordering the L6 experiment pins.
    if let (Some(key), Some(bucket)) = (idempotency_key.as_deref(), hour_bucket) {
        match read_marker(
            state.store.as_ref(),
            &tenant,
            Signal::Logs,
            key,
            bucket,
            DEFAULT_IDEM_DEDUP_WINDOW_HOURS,
        )
        .await
        {
            Ok(LookupOutcome::Hit(receipt)) => {
                // A replay reports the original outcome, not a fresh
                // rejection count: the first request already accounted for
                // its own rejections at write time.
                return Ok(LogIngestOutcome {
                    response: ExportLogsServiceResponse {
                        partial_success: None,
                    },
                    tokens: Vec::new(),
                    replayed_commit_token: Some(receipt.commit_token),
                });
            }
            // Miss and Corrupt both fail open to the normal write path
            // (ADR-0051 section 5); a Corrupt marker is a miss, never an
            // error surfaced to the caller. Corrupt is still worth a signal:
            // it means bytes in object storage failed to decode, unlike a
            // plain Miss which is the expected common case.
            Ok(LookupOutcome::Miss) => {}
            Ok(LookupOutcome::Corrupt) => {
                tracing::warn!("idempotency marker found but failed to decode; treating as a miss");
            }
            // A genuine store error on the lookup is fail-open too: nothing
            // is written yet, so proceeding re-ingests (safe at-least-once)
            // rather than failing a retry the client can only repeat.
            Err(err) => {
                tracing::warn!(
                    %err,
                    "idempotency marker lookup failed; proceeding as a normal write"
                );
            }
        }
    }

    let normalized = normalize_logs(request, &state.limits, ingest_ts_ns);
    let mut rejected_count: usize = normalized.rejected.iter().map(|r| r.rejected_count()).sum();
    let mut records = normalized.records;

    // Layer 4 (ADR-0051 section 1): stream-creation-rate is a whole-request
    // rate limit checked first (breach rejects the whole request, no tokens
    // consumed); the active-stream cap that follows is per-record partial
    // success, never a whole-request rejection.
    let candidate_streams: Vec<LogStreamId> = records.iter().map(|r| r.stream_id).collect();
    state
        .admission
        .check_stream_creation_rate(&tenant, &candidate_streams, ingest_ts_ns)
        .map_err(LogIngestRequestError::Admission)?;
    let admission = state
        .admission
        .admit_streams(&tenant, candidate_streams, ingest_ts_ns);
    let stream_cap_rejected = admission.rejected.len();
    if stream_cap_rejected > 0 {
        let admitted: HashSet<LogStreamId> = admission.admitted.into_iter().collect();
        records.retain(|r| admitted.contains(&r.stream_id));
        rejected_count += stream_cap_rejected;
    }

    // Rows this request actually writes, captured before `records` is moved
    // into the router: this is the marker's `written_count`.
    let written_count = records.len() as u64;

    let receipt = state
        .router
        .write(tenant.clone(), records, mode, state.ack_deadline)
        .await
        .map_err(|err| {
            // A PartialWrite's durable siblings are real, durably committed
            // data (docs/consistency-model.md "Opt-in client idempotency
            // key"). OTLP has no error-response channel to hand their tokens
            // back to the client (unlike ravel-cli's `load`, which reports
            // them via LoadError::Flush), and no idempotency marker is
            // written for them either: the marker replay path always reports
            // the original commit token with zero rejections, so marking a
            // partial commit as the request's receipt would make the next
            // retry skip resending the shard that never committed and
            // permanently lose it, rather than the honest at-least-once
            // duplication an unkeyed retry gets today (issue #460). Logging
            // the count is the only recovery this path offers.
            let durable_shard_count = err.durable_tokens().len();
            if durable_shard_count > 0 {
                tracing::warn!(
                    durable_shard_count,
                    "log write partially committed before a sibling shard \
                     failed; no idempotency marker written for the durable \
                     siblings"
                );
            }
            LogIngestRequestError::Write(err)
        })?;

    let partial_success = if rejected_count > 0 {
        let error_message = build_error_message(&normalized.rejected, stream_cap_rejected);
        Some(ExportLogsPartialSuccess {
            rejected_log_records: rejected_count as i64,
            error_message,
        })
    } else {
        None
    };

    // Ordering (ADR-0051 section 5): the data is durably
    // committed once `router.write` returns its tokens. Write the marker here,
    // before this function returns and thus before the client can observe any
    // ack, so a retry of this keyed request finds it. Only a real commit
    // (tokens present, so `encode_commit_tokens` is `Some`) gets a marker:
    // buffered mode and fully-rejected requests ack no durable data, and there
    // is nothing to replay.
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
            Signal::Logs,
            key,
            bucket,
            &marker,
        )
        .await
        {
            // The data is already durable; failing the response here would
            // falsely tell the client its committed write was lost. Log
            // and still ack. The retry simply reingests (at-least-once,
            // the documented fallback) since no marker exists.
            tracing::warn!(
                %err,
                "idempotency marker write failed after a durable commit; acking anyway"
            );
        }
    }

    Ok(LogIngestOutcome {
        response: ExportLogsServiceResponse { partial_success },
        tokens: receipt.tokens,
        replayed_commit_token: None,
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
///
/// `stream_cap_rejected` folds in the layer-4 active-stream-cap count (0
/// when nothing was capped) as one more reason, the same way
/// [`crate::ingest`]'s equivalent folds in its series-cap count.
fn build_error_message(rejected: &[LogRejection], stream_cap_rejected: usize) -> String {
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
    if stream_cap_rejected > 0 {
        let key = "active stream cap exceeded".to_string();
        counts
            .entry(key.clone())
            .and_modify(|count| *count += stream_cap_rejected)
            .or_insert_with(|| {
                order.push(key);
                stream_cap_rejected
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
    use ravel_ingest::{AdmissionController, AdmissionLimits, IngestConfig, SystemClock};
    use ravel_object_store::ObjectStoreBackend;
    use ravel_object_store::memory::MemoryStore;

    /// Fixed post-floor fixture base, 2026-01-01T00:00:00Z in nanoseconds
    /// (ADR-0051 amendment): the fixture ingest clock and every log
    /// record timestamp anchor to it so the receiver-clock plausibility floor
    /// admits the request. Never `SystemTime::now()`, so tests stay
    /// deterministic.
    const BASE_TS_NS: i64 = 1_767_225_600_000_000_000;

    fn state() -> LogIngestState {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let router = Arc::new(LogIngestRouter::new(
            IngestConfig {
                shard_count: 1,
                ..IngestConfig::default()
            },
            store.clone(),
            Arc::new(SystemClock),
        ));
        LogIngestState {
            router,
            limits: LogIngestLimits::default(),
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
            // Base-relative so the resolved record ts sits inside the admission
            // window around the fixture ingest clock (BASE_TS_NS).
            time_unix_nano: BASE_TS_NS as u64,
            observed_time_unix_nano: BASE_TS_NS as u64,
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
            BASE_TS_NS,
            None,
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
            BASE_TS_NS,
            None,
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
    async fn oversized_idempotency_key_is_rejected_not_truncated() {
        let state = state();
        // One byte past the cap: a typed rejection, before any normalize,
        // admission, or write happens.
        let key = vec![b'k'; MAX_IDEMPOTENCY_KEY_BYTES + 1];
        let err = handle_export_logs(
            &state,
            TenantId::new("acme"),
            WriteMode::Strict,
            request(vec![record("hello", vec![string_kv("k", "v")])]),
            BASE_TS_NS,
            Some(key),
        )
        .await
        .expect_err("an over-long idempotency key must be rejected");
        assert!(
            matches!(
                err,
                LogIngestRequestError::InvalidIdempotencyKey { len }
                    if len == MAX_IDEMPOTENCY_KEY_BYTES + 1
            ),
            "got: {err:?}"
        );
        assert!(
            !err.is_retryable(),
            "the identical over-long key cannot succeed on retry"
        );
        // A key exactly at the cap is accepted (boundary is inclusive).
        let at_cap = vec![b'k'; MAX_IDEMPOTENCY_KEY_BYTES];
        handle_export_logs(
            &state,
            TenantId::new("acme"),
            WriteMode::Strict,
            request(vec![record("hello", vec![string_kv("k", "v")])]),
            BASE_TS_NS,
            Some(at_cap),
        )
        .await
        .expect("a key exactly at the cap is accepted");
    }

    /// a logs request whose receiver (ingest) clock is below the 2020
    /// floor must be rejected as the whole-request `ClockImplausible` error
    /// (retryable, so the transport maps it to gRPC `UNAVAILABLE` / HTTP 503),
    /// and the `reason="clock"` admission counter for the tenant's Logs signal
    /// must increment. The clock is a fixed sub-floor timestamp, never
    /// `SystemTime::now()`.
    ///
    /// Non-vacuity: delete the `plausible_ingest_clock` guard at the top of
    /// `handle_export_logs` (logs_ingest.rs, the `if let Err(msg) = ...` block)
    /// and this test fails, because the empty request then writes cleanly and
    /// returns `Ok`, and the counter stays at 0.
    #[tokio::test]
    async fn receiver_clock_below_floor_rejects_unavailable_with_reason_clock() {
        use ravel_ingest::MIN_PLAUSIBLE_INGEST_CLOCK_NS;

        let state = state();
        let tenant = TenantId::new("acme");
        // One nanosecond below the 2020 floor: an implausible receiver clock.
        let sub_floor = MIN_PLAUSIBLE_INGEST_CLOCK_NS - 1;

        // `LogIngestOutcome` is `Debug`, but match to mirror the other surfaces.
        let err = match handle_export_logs(
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
            matches!(err, LogIngestRequestError::ClockImplausible(_)),
            "expected ClockImplausible, got: {err:?}"
        );
        assert!(
            err.is_retryable(),
            "ClockImplausible is retryable, which the transport maps to UNAVAILABLE/503"
        );

        // (b) The admission rejected counter increments under reason=\"clock\"
        // for this tenant's Logs signal.
        let row = state
            .admission
            .usage_snapshot()
            .into_iter()
            .find(|r| r.tenant_hash == tenant.hash() && r.signal == Signal::Logs)
            .expect("a logs usage row exists after the clock rejection");
        assert_eq!(
            row.requests_rejected_clock_total, 1,
            "the reason=\"clock\" rejected counter incremented exactly once"
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
        let message = build_error_message(&rejected, 0);
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

    /// A minimal `tracing_subscriber::Layer` that records every WARN event
    /// carrying a `durable_shard_count` field, so the next test can prove
    /// issue #460's log line fires (and only fires) on a genuine partial
    /// write, without depending on stdout capture.
    #[derive(Default, Clone)]
    struct DurableShardCountCapture(Arc<parking_lot::Mutex<Vec<u64>>>);

    impl<S> tracing_subscriber::Layer<S> for DurableShardCountCapture
    where
        S: tracing::Subscriber,
    {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if *event.metadata().level() != tracing::Level::WARN {
                return;
            }
            struct Visitor(Option<u64>);
            impl tracing::field::Visit for Visitor {
                fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
                    if field.name() == "durable_shard_count" {
                        self.0 = Some(value);
                    }
                }
                fn record_debug(
                    &mut self,
                    _field: &tracing::field::Field,
                    _value: &dyn std::fmt::Debug,
                ) {
                }
            }
            let mut visitor = Visitor(None);
            event.record(&mut visitor);
            if let Some(count) = visitor.0 {
                self.0.lock().push(count);
            }
        }
    }

    /// First resource-attribute set (by an incrementing `host` label) whose
    /// OTLP-normalized `stream_id` routes to `want_shard`, matching exactly
    /// what [`ravel_otlp::logs_normalize::normalize_logs`] computes (empty
    /// scope name/version/attrs, since these fixtures never set `scope`).
    fn resource_attrs_for_shard(want_shard: u32, shard_count: u32) -> Vec<KeyValue> {
        use ravel_types::logstream::{AttrValue, log_stream_id};
        for i in 0..100_000u32 {
            let host = i.to_string();
            let attrs = vec![
                (
                    "service.name".to_string(),
                    AttrValue::Str("api".to_string()),
                ),
                ("host".to_string(), AttrValue::Str(host.clone())),
            ];
            let stream_id = log_stream_id(&attrs, "", "", &[]);
            if ravel_types::shard_for_log(&stream_id, shard_count) == want_shard {
                return vec![string_kv("service.name", "api"), string_kv("host", &host)];
            }
        }
        panic!("no host found for shard {want_shard} of {shard_count}");
    }

    fn state_with_store(shard_count: u32, store: Arc<dyn ObjectStoreBackend>) -> LogIngestState {
        let router = Arc::new(LogIngestRouter::new(
            IngestConfig {
                shard_count,
                ..IngestConfig::default()
            },
            store.clone(),
            Arc::new(SystemClock),
        ));
        LogIngestState {
            router,
            limits: LogIngestLimits::default(),
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

    /// Issue #460: a multi-shard Strict OTLP write where one shard's flush is
    /// permanently abandoned while a sibling commits durably in the same
    /// call must (a) log the recovered durable-shard count for operators,
    /// since OTLP has no error-response channel to hand the caller the
    /// tokens `LogWriteError::durable_tokens` recovers, and (b) still write
    /// no idempotency marker even though a key was supplied and part of the
    /// write did commit durably -- a marker here would make the next retry
    /// believe the whole request (including the shard that never committed)
    /// already landed, permanently losing it.
    ///
    /// Non-vacuity: before this fix, `map_err(LogIngestRequestError::Write)`
    /// discarded the error's durable tokens with no side effect, so this
    /// test's `assert_eq!(captured.lock().as_slice(), &[1])` fails against
    /// that code (the capture stays empty) even though the marker-absence
    /// assertion alone would pass either way.
    #[tokio::test]
    async fn partial_write_logs_durable_count_and_writes_no_marker() {
        use ravel_object_store::fault::{
            FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
        };
        use tracing_subscriber::layer::SubscriberExt as _;

        let shard_count = 2;
        // Shard 0's data-object PUT fails permanently; shard 1's must survive.
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::Put,
                ScriptedFault::Permanent("simulated permanent data-object PUT failure".into()),
            )
            .with_key_contains("/l0/0000/")
            .with_occurrence(Occurrence::Always),
        );
        let store: Arc<dyn ObjectStoreBackend> =
            Arc::new(FaultStore::new(MemoryStore::new(), plan));
        let state = state_with_store(shard_count, store);

        let victim_resource = resource_attrs_for_shard(0, shard_count);
        let survivor_resource = resource_attrs_for_shard(1, shard_count);
        assert_ne!(
            victim_resource, survivor_resource,
            "the fixture must span two distinct shards"
        );
        let otlp_request = ExportLogsServiceRequest {
            resource_logs: vec![
                ResourceLogs {
                    resource: Some(Resource {
                        attributes: victim_resource,
                        ..Default::default()
                    }),
                    scope_logs: vec![ScopeLogs {
                        log_records: vec![record("victim", Vec::new())],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                ResourceLogs {
                    resource: Some(Resource {
                        attributes: survivor_resource,
                        ..Default::default()
                    }),
                    scope_logs: vec![ScopeLogs {
                        log_records: vec![record("survivor", Vec::new())],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
        };

        let captured: Arc<parking_lot::Mutex<Vec<u64>>> = Arc::default();
        let subscriber =
            tracing_subscriber::registry().with(DurableShardCountCapture(captured.clone()));
        let _guard = tracing::subscriber::set_default(subscriber);

        let key = b"idem-460".to_vec();
        let err = handle_export_logs(
            &state,
            TenantId::new("acme"),
            WriteMode::Strict,
            otlp_request,
            BASE_TS_NS,
            Some(key.clone()),
        )
        .await
        .expect_err("shard 0's flush was permanently abandoned");

        let LogIngestRequestError::Write(write_err) = &err else {
            panic!("expected a Write error, got {err:?}");
        };
        assert_eq!(
            write_err.durable_tokens().len(),
            1,
            "exactly the surviving shard's token is recoverable, got {write_err:?}"
        );

        assert_eq!(
            captured.lock().as_slice(),
            &[1u64],
            "the warn line must fire exactly once, reporting exactly one durable shard"
        );

        let bucket = request_ingest_hour_bucket(BASE_TS_NS).expect("valid ingest ts");
        let lookup = read_marker(
            state.store.as_ref(),
            &TenantId::new("acme"),
            Signal::Logs,
            &key,
            bucket,
            DEFAULT_IDEM_DEDUP_WINDOW_HOURS,
        )
        .await
        .expect("marker lookup itself must not error");
        assert!(
            matches!(lookup, LookupOutcome::Miss),
            "a partial commit must not write an idempotency marker, got {lookup:?}"
        );
    }
}
