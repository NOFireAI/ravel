//! OTLP-transport-agnostic log ingest logic shared by the HTTP and gRPC
//! handlers, the log-pipeline counterpart of [`crate::ingest`].

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsPartialSuccess, ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use ravel_ingest::{
    AdmissionController, IdempotencyReceipt, LogIngestRouter, LogWriteError, LookupOutcome,
    MarkerLookupError, RequestRejection, WriteMode, plausible_ingest_clock, read_marker,
    write_marker,
};
use ravel_maintain::config::DEFAULT_IDEM_DEDUP_WINDOW_HOURS;
use ravel_object_store::ObjectStoreBackend;
use ravel_otlp::{LogIngestLimits, LogRejection, NormalizeRejectCounts, normalize_logs};
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
    /// Normalization's own admission decisions for this signal, the log
    /// counterpart of [`crate::ingest::IngestState::normalize_metrics`]. Also
    /// counts structured bodies converted rather than rejected, which is not a
    /// rejection and gets its own family.
    pub normalize_metrics: Arc<crate::normalize_reject_metrics::NormalizeRejectMetrics>,
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

/// The client-facing message of a keyed write refused because its marker
/// lookup failed. It carries no key, tenant hash or store error text; those
/// go to the server-side log [`marker_lookup_failure`] writes.
pub(crate) const MARKER_LOOKUP_FAILED_MESSAGE: &str = "idempotency marker lookup failed; the \
     write was not accepted and is safe to retry";

/// Keyed writes refused because a marker lookup probe failed with a store
/// error, logs then spans, rendered as
/// `ravel_ingest_idempotency_lookup_failures_total`. Process-global with a
/// single source per signal, like [`crate::provisioning::shard_count_mismatch_count`].
static IDEMPOTENCY_LOOKUP_FAILURES: [AtomicU64; 2] = [AtomicU64::new(0), AtomicU64::new(0)];

fn idempotency_lookup_failure_slot(signal: Signal) -> Option<&'static AtomicU64> {
    match signal {
        Signal::Logs => Some(&IDEMPOTENCY_LOOKUP_FAILURES[0]),
        Signal::Spans => Some(&IDEMPOTENCY_LOOKUP_FAILURES[1]),
        _ => None,
    }
}

/// The lookup-failure count for `signal`, for the `/metrics` renderer. Zero
/// for a signal that writes no markers.
pub(crate) fn idempotency_lookup_failures(signal: Signal) -> u64 {
    idempotency_lookup_failure_slot(signal).map_or(0, |slot| slot.load(Ordering::Relaxed))
}

/// Record a keyed write refused because its marker lookup failed
/// (`read_marker` maps `NotFound` to a miss, so this is never absence): log
/// the failed GET, its key and the store error at WARN, count it, and return
/// the client-facing message, which names none of them.
pub(crate) fn marker_lookup_failure(signal: Signal, err: &MarkerLookupError) -> String {
    tracing::warn!(
        signal = signal.key_prefix(),
        request = "GET",
        key = %err.key,
        error = %err.source,
        "idempotency marker lookup failed; refusing the keyed write"
    );
    if let Some(slot) = idempotency_lookup_failure_slot(signal) {
        slot.fetch_add(1, Ordering::Relaxed);
    }
    MARKER_LOOKUP_FAILED_MESSAGE.to_string()
}

/// Record a keyed write refused because its marker lookup did not finish
/// within `deadline`: the same counter and client-facing message as
/// [`marker_lookup_failure`], and a WARN line that names the deadline instead
/// of a key, since several probes may still have been in flight.
pub(crate) fn marker_lookup_deadline(signal: Signal, deadline: Duration) -> String {
    tracing::warn!(
        signal = signal.key_prefix(),
        request = "GET",
        deadline = ?deadline,
        "idempotency marker lookup ran past the write deadline; refusing the keyed write"
    );
    if let Some(slot) = idempotency_lookup_failure_slot(signal) {
        slot.fetch_add(1, Ordering::Relaxed);
    }
    MARKER_LOOKUP_FAILED_MESSAGE.to_string()
}

/// The share of a keyed request's `ack_deadline` its marker lookup may use:
/// half. The router write enqueues the request's records into the shard
/// channels before it waits for their acknowledgement, so a write left with
/// little or no budget can time out after its data is durable; no marker is
/// written, and the client's retry finds none and ingests the batch again.
/// Bounding the lookup to half leaves the write the other half.
pub(crate) fn marker_lookup_share(ack_deadline: Duration) -> Duration {
    ack_deadline / 2
}

/// The marker lookup of a keyed write (ADR-0051 section 5), bounded by
/// `deadline`, the request's [`marker_lookup_share`] of its `ack_deadline`;
/// the caller gives the router write what the lookup leaves of the whole
/// budget ([`router_write_budget`]). A lookup that fails or runs past
/// `deadline` returns the client-facing refusal message, already logged and
/// counted; the caller refuses the write with it.
pub(crate) async fn lookup_marker_within(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    signal: Signal,
    client_key: &[u8],
    hour_bucket: u32,
    deadline: Duration,
) -> Result<LookupOutcome, String> {
    let lookup = read_marker(
        store,
        tenant,
        signal,
        client_key,
        hour_bucket,
        DEFAULT_IDEM_DEDUP_WINDOW_HOURS,
    );
    match tokio::time::timeout(deadline, lookup).await {
        Ok(Ok(outcome)) => Ok(outcome),
        Ok(Err(err)) => Err(marker_lookup_failure(signal, &err)),
        Err(_elapsed) => Err(marker_lookup_deadline(signal, deadline)),
    }
}

/// What is left at this instant of a request's `ack_deadline` budget that
/// ends at `deadline`, zero once it has passed.
pub(crate) fn remaining_budget(deadline: tokio::time::Instant) -> Duration {
    deadline.saturating_duration_since(tokio::time::Instant::now())
}

/// The budget the router write of a request gets. A keyed request passes the
/// end of the `ack_deadline` budget its marker lookup shares with the write
/// and gets what is left of it ([`remaining_budget`]); an unkeyed request
/// looks nothing up and gets the whole `ack_deadline`.
pub(crate) fn router_write_budget(
    keyed_deadline: Option<tokio::time::Instant>,
    ack_deadline: Duration,
) -> Duration {
    let budget = keyed_deadline.map_or(ack_deadline, remaining_budget);
    #[cfg(test)]
    marker_lookup_test_support::record_router_write_budget(budget);
    budget
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
    // One `ack_deadline` budget per keyed request, started before the lookup:
    // the lookup may use its share and the router write gets what is left.
    let keyed_deadline = idempotency_key
        .is_some()
        .then(|| tokio::time::Instant::now() + state.ack_deadline);

    // Replay (ADR-0051 section 5): a keyed retry whose marker is still inside
    // the dedup window skips admission, normalize, and the router write, and
    // returns the stored receipt directly. The lookup runs before any of
    // that work, per the ordering the L6 experiment pins.
    if let (Some(key), Some(bucket)) = (idempotency_key.as_deref(), hour_bucket) {
        match lookup_marker_within(
            state.store.as_ref(),
            &tenant,
            Signal::Logs,
            key,
            bucket,
            marker_lookup_share(state.ack_deadline),
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
            // A store error on the lookup, or a lookup past the deadline,
            // fails the write closed: writing anyway would store a duplicate
            // whenever the marker exists and the lookup could not see it. The
            // request's own data is not written yet, so the retryable error is
            // safe for the client to retry.
            Err(message) => {
                return Err(LogIngestRequestError::Write(LogWriteError::Abandoned(
                    message,
                )));
            }
        }
    }

    let normalized = normalize_logs(request, &state.limits, ingest_ts_ns);
    let mut rejected_count: usize = normalized.rejected.iter().map(|r| r.rejected_count()).sum();
    // Layer 3's rejections, counted where they are observed. The body
    // conversions alongside them are not rejections: those records passed
    // normalization and are in `normalized.records`, still facing the
    // active-stream cap below and the router write (see
    // docs/guides/observability.md, "Reading the `reason` label", for what
    // each counter does and does not claim).
    state.normalize_metrics.record(
        &tenant,
        ravel_types::Signal::Logs,
        NormalizeRejectCounts::from_log_rejections(&normalized.rejected),
    );
    state.normalize_metrics.record_body_conversions(
        &tenant,
        ravel_types::Signal::Logs,
        normalized.body_conversions,
    );
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
        .write(
            tenant.clone(),
            records,
            mode,
            router_write_budget(keyed_deadline, state.ack_deadline),
        )
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

    // Gate on whether anything was rejected at all, never on the unit count: a
    // zero-count rejection still has to reach the sender. The rule and the
    // reasoning are in docs/guides/ingest.md, "Zero-count partial success".
    //
    // Logs carry a second term because layer 4's active-stream-cap count is
    // tracked outside `normalized.rejected` and never appears in it, so a gate
    // reading only that list would report a fully clean write on a request whose
    // records normalized cleanly and were then turned away by the cap.
    let partial_success = if normalized.rejected.is_empty() && stream_cap_rejected == 0 {
        None
    } else {
        let error_message = build_error_message(&normalized.rejected, stream_cap_rejected);
        Some(ExportLogsPartialSuccess {
            rejected_log_records: rejected_count as i64,
            error_message,
        })
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

    use crate::logs_ingest::marker_lookup_test_support::{
        FIRST_BATCH_PROBES, LOOKUP_DEADLINE_WARNING, LOOKUP_FAILED_WARNING, LOOKUP_FAILURE_COUNTER,
        LookupFailureWarning, LookupFailureWarnings, PAUSED_CLOCK_SLACK, STORE_ERROR_TEXT,
        SlowLookupStalledWriteStore, marker_get_refusals, marker_probe_store, put_count,
        take_router_write_budgets,
    };
    use crate::normalize_reject_metrics::NormalizeRejectMetrics;

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
            normalize_metrics: Arc::new(NormalizeRejectMetrics::new()),
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

    fn int_kv(key: &str, value: i64) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(AnyValueVariant::IntValue(value)),
            }),
            ..Default::default()
        }
    }

    /// `(skew, structural, body_conversions)` summed over the logs rows of the
    /// normalize-reject counters, so a test can read the same figures before
    /// and after an export and assert the delta rather than the total.
    fn logs_normalize_totals(state: &LogIngestState) -> (u64, u64, u64) {
        state
            .normalize_metrics
            .snapshot()
            .into_iter()
            .filter(|row| row.signal == Signal::Logs)
            .fold((0, 0, 0), |acc, row| {
                (
                    acc.0 + row.skew_total,
                    acc.1 + row.structural_total,
                    acc.2 + row.body_conversions_total,
                )
            })
    }

    /// Reads back every log record the tenant's L0 data objects hold, so a
    /// test can assert on what was actually stored rather than on what the
    /// normalize layer returned.
    async fn stored_log_records(
        store: &dyn ObjectStoreBackend,
        tenant: &str,
    ) -> Vec<ravel_logseg::LogRecord> {
        use ravel_logseg::{Predicate, RlogConfig, RlogReader};
        use ravel_object_store::{GetRange, list_all};

        let prefix = format!("t/{}/l/l0/", TenantId::new(tenant).hash().to_hex());
        let objects = list_all(store, &prefix)
            .await
            .expect("list log data objects");
        let mut out = Vec::new();
        for object in objects {
            let bytes = store
                .get(&object.key, GetRange::Full)
                .await
                .expect("get log data object")
                .data;
            let reader = RlogReader::new(&bytes, &RlogConfig::default()).expect("open RLOG object");
            let (records, _stats) = reader
                .scan(&Predicate::And(Vec::new()))
                .expect("unfiltered scan");
            out.extend(records);
        }
        out
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
            .expect("the dropped attribute is reported");
        assert_eq!(
            partial_success.rejected_log_records, 0,
            "no record was lost, only one attribute"
        );
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
        // A string-table reference body is LogRejection::UnsupportedBodyKind,
        // which drops the whole record: the table it indexes lives on the OTLP
        // request, not on the record, so nothing here can resolve it. Array and
        // kvlist bodies used to reach this same rejection and no longer do;
        // they convert to canonical JSON and are stored.
        let mut rec = record("unused", Vec::new());
        rec.body = Some(AnyValue {
            value: Some(AnyValueVariant::StringValueStrindex(3)),
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

    /// A kvlist body is stored as canonical JSON, and everything else on the
    /// record survives the conversion. The counters see a conversion, not a
    /// rejection.
    #[tokio::test]
    async fn kvlist_body_is_stored_as_canonical_json_with_the_record_intact() {
        const TRACE_ID: [u8; 16] = [0x11; 16];
        const SPAN_ID: [u8; 8] = [0x22; 8];

        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let state = state_with_store(1, store.clone());

        let mut rec = record("unused", vec![string_kv("http.route", "/v1/logs")]);
        rec.body = Some(AnyValue {
            value: Some(AnyValueVariant::KvlistValue(
                opentelemetry_proto::tonic::common::v1::KeyValueList {
                    // Reverse key order on the wire: the stored form must be
                    // the canonical order, not the sender's.
                    values: vec![string_kv("zeta", "z"), int_kv("alpha", 7)],
                },
            )),
        });
        rec.trace_id = TRACE_ID.to_vec();
        rec.span_id = SPAN_ID.to_vec();

        let before = logs_normalize_totals(&state);
        let outcome = handle_export_logs(
            &state,
            TenantId::new("acme"),
            WriteMode::Strict,
            request(vec![rec]),
            BASE_TS_NS,
            None,
        )
        .await
        .expect("strict write publishes");
        assert!(
            outcome.response.partial_success.is_none(),
            "a converted body is not a rejection, got {:?}",
            outcome.response.partial_success
        );
        let after = logs_normalize_totals(&state);
        assert_eq!(
            (after.0 - before.0, after.1 - before.1, after.2 - before.2),
            (0, 0, 1),
            "exactly one conversion, no skew or structural rejection"
        );

        let stored = stored_log_records(store.as_ref(), "acme").await;
        assert_eq!(stored.len(), 1, "the record was stored, not dropped");
        let stored = &stored[0];
        assert_eq!(stored.body, r#"{"alpha":7,"zeta":"z"}"#);
        assert_eq!(stored.ts_ns, BASE_TS_NS);
        assert_eq!(stored.trace_id, Some(TRACE_ID));
        assert_eq!(stored.span_id, Some(SPAN_ID));
        assert_eq!(
            stored.attrs,
            vec![(
                "http.route".to_string(),
                ravel_types::logstream::AttrValue::Str("/v1/logs".to_string())
            )],
            "record attributes survive the body conversion"
        );
    }

    /// The array-body counterpart of
    /// `kvlist_body_is_stored_as_canonical_json_with_the_record_intact`:
    /// element order is the sender's, not sorted.
    #[tokio::test]
    async fn array_body_is_stored_as_canonical_json_with_the_record_intact() {
        const TRACE_ID: [u8; 16] = [0x33; 16];
        const SPAN_ID: [u8; 8] = [0x44; 8];

        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let state = state_with_store(1, store.clone());

        let mut rec = record("unused", vec![string_kv("http.route", "/v1/logs")]);
        rec.body = Some(AnyValue {
            value: Some(AnyValueVariant::ArrayValue(
                opentelemetry_proto::tonic::common::v1::ArrayValue {
                    values: vec![
                        AnyValue {
                            value: Some(AnyValueVariant::IntValue(1)),
                        },
                        AnyValue {
                            value: Some(AnyValueVariant::StringValue("two".to_string())),
                        },
                        AnyValue {
                            value: Some(AnyValueVariant::BoolValue(true)),
                        },
                    ],
                },
            )),
        });
        rec.trace_id = TRACE_ID.to_vec();
        rec.span_id = SPAN_ID.to_vec();

        let before = logs_normalize_totals(&state);
        let outcome = handle_export_logs(
            &state,
            TenantId::new("acme"),
            WriteMode::Strict,
            request(vec![rec]),
            BASE_TS_NS,
            None,
        )
        .await
        .expect("strict write publishes");
        assert!(
            outcome.response.partial_success.is_none(),
            "a converted body is not a rejection, got {:?}",
            outcome.response.partial_success
        );
        let after = logs_normalize_totals(&state);
        assert_eq!(
            (after.0 - before.0, after.1 - before.1, after.2 - before.2),
            (0, 0, 1),
            "exactly one conversion, no skew or structural rejection"
        );

        let stored = stored_log_records(store.as_ref(), "acme").await;
        assert_eq!(stored.len(), 1, "the record was stored, not dropped");
        let stored = &stored[0];
        assert_eq!(stored.body, r#"[1,"two",true]"#);
        assert_eq!(stored.ts_ns, BASE_TS_NS);
        assert_eq!(stored.trace_id, Some(TRACE_ID));
        assert_eq!(stored.span_id, Some(SPAN_ID));
        assert_eq!(
            stored.attrs,
            vec![(
                "http.route".to_string(),
                ravel_types::logstream::AttrValue::Str("/v1/logs".to_string())
            )],
            "record attributes survive the body conversion"
        );
    }

    /// Event-time rejections land under the skew reason, one per rejected
    /// record, and move nothing else.
    #[tokio::test]
    async fn event_time_rejections_count_as_skew() {
        let state = state();
        let too_old = BASE_TS_NS - state.limits.max_ingest_lag_ns - 1;
        let records: Vec<LogRecord> = (0..3)
            .map(|i| {
                let mut rec = record("hello", vec![string_kv("k", "v")]);
                rec.time_unix_nano = (too_old - i) as u64;
                rec.observed_time_unix_nano = (too_old - i) as u64;
                rec
            })
            .collect();

        let before = logs_normalize_totals(&state);
        let outcome = handle_export_logs(
            &state,
            TenantId::new("acme"),
            WriteMode::Strict,
            request(records),
            BASE_TS_NS,
            None,
        )
        .await
        .expect("a write with zero admitted records never fails");
        let after = logs_normalize_totals(&state);

        assert_eq!(
            outcome
                .response
                .partial_success
                .expect("all three records were rejected")
                .rejected_log_records,
            3
        );
        assert_eq!(
            (after.0 - before.0, after.1 - before.1, after.2 - before.2),
            (3, 0, 0),
            "three skew rejections, nothing structural, no conversion"
        );
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
            normalize_metrics: Arc::new(NormalizeRejectMetrics::new()),
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

    /// Issue #2462: a keyed write whose marker probe the store refuses fails
    /// with the retryable `Abandoned` error before any of its own data is
    /// written. The client-facing message names no key, tenant hash, LIST or
    /// store error; the failed GET of the newest hour, its key and the store
    /// error go to one WARN line and one count of
    /// `ravel_ingest_idempotency_lookup_failures_total`. The recovery manifest
    /// and provisioning record are created before the lookup on a tenant's
    /// first write and are out of this assertion's scope (both are disabled in
    /// this fixture).
    ///
    /// Non-vacuity: restoring a log-and-write arm for the lookup's `Err` in
    /// `handle_export_logs` makes the write succeed, so `expect_err` fails;
    /// appending the store error to the message fails the
    /// message equality check; dropping the `tracing::warn!` or the
    /// `fetch_add` in `marker_lookup_failure` fails the capture or the counter
    /// check.
    #[tokio::test]
    async fn keyed_write_whose_marker_lookup_is_refused_fails_retryable_and_writes_nothing() {
        use tracing_subscriber::layer::SubscriberExt as _;

        let _counter = LOOKUP_FAILURE_COUNTER.lock().await;
        let store = marker_probe_store(true);
        let state = state_with_store(1, store.clone());
        let tenant = TenantId::new("acme");
        let key = b"idem-2462".to_vec();
        let warnings = LookupFailureWarnings::default();
        let _guard =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(warnings.clone()));
        let failures_before = idempotency_lookup_failures(Signal::Logs);

        let err = handle_export_logs(
            &state,
            tenant.clone(),
            WriteMode::Strict,
            request(vec![record("hello", Vec::new())]),
            BASE_TS_NS,
            Some(key.clone()),
        )
        .await
        .expect_err("a refused marker lookup must fail the keyed write");

        let LogIngestRequestError::Write(LogWriteError::Abandoned(message)) = &err else {
            panic!("expected the retryable Abandoned write error, got {err:?}");
        };
        assert!(err.is_retryable(), "{err:?} must be retryable");
        assert_eq!(message, MARKER_LOOKUP_FAILED_MESSAGE);
        let bucket = request_ingest_hour_bucket(BASE_TS_NS).expect("valid ingest ts");
        let first_probe = ravel_ingest::marker_key(
            &tenant,
            Signal::Logs,
            &key,
            bucket + ravel_ingest::IDEM_MARKER_FORWARD_SKEW_TOLERANCE_HOURS,
        );
        let client_text = err.to_string();
        for forbidden in [
            first_probe.as_str(),
            &tenant.hash().to_hex(),
            &ravel_ingest::keyhash32(&tenant, &key),
            "LIST",
            STORE_ERROR_TEXT,
        ] {
            assert!(
                !client_text.contains(forbidden),
                "the client-facing message must not carry {forbidden:?}: {client_text}"
            );
        }

        assert_eq!(
            marker_get_refusals(&store),
            FIRST_BATCH_PROBES,
            "the GET fault must fire on each probe of the first batch"
        );
        assert_eq!(
            put_count(&store),
            0,
            "none of the request's own data is written"
        );
        assert_eq!(
            warnings.0.lock().as_slice(),
            &[LookupFailureWarning {
                message: LOOKUP_FAILED_WARNING.to_string(),
                signal: "l".to_string(),
                request: "GET".to_string(),
                key: first_probe,
                error: format!("permanent error: {STORE_ERROR_TEXT}"),
                deadline: String::new(),
            }],
            "exactly one WARN line, naming the GET, its key and the store error"
        );
        assert_eq!(
            idempotency_lookup_failures(Signal::Logs) - failures_before,
            1,
            "the lookup failure must be counted exactly once"
        );
    }

    /// Issue #2462: a keyed write whose marker GETs never answer is refused
    /// once the lookup has run for its share of the write's `ack_deadline`,
    /// exactly half of it, with the same retryable message and counter as a
    /// refused GET and a WARN line that names that share. None of the
    /// request's own data is written; the recovery manifest and provisioning
    /// record are created before the lookup on a tenant's first write and are
    /// out of this assertion's scope (both are disabled in this fixture). The
    /// clock is paused, so the elapsed time is the timer's, not the machine's.
    ///
    /// Non-vacuity: returning the whole `ack_deadline` from
    /// `marker_lookup_share` refuses after five seconds, not two and a half,
    /// and fails the elapsed check; calling `read_marker` without the
    /// `tokio::time::timeout` in `lookup_marker_within` never returns, so the
    /// test hangs; dropping the `tracing::warn!` or the `fetch_add` in
    /// `marker_lookup_deadline` fails the capture or the counter check.
    #[tokio::test(start_paused = true)]
    async fn keyed_write_whose_marker_lookup_hangs_is_refused_at_half_the_deadline() {
        use ravel_object_store::fault::{Occurrence, Op};
        use tracing_subscriber::layer::SubscriberExt as _;

        let _counter = LOOKUP_FAILURE_COUNTER.lock().await;
        let store = marker_probe_store(false);
        let held = store.hold(Op::Get, Some("/idem/".to_string()), Occurrence::Always);
        let state = state_with_store(1, store.clone());
        let warnings = LookupFailureWarnings::default();
        let _guard =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(warnings.clone()));
        let failures_before = idempotency_lookup_failures(Signal::Logs);

        let started = tokio::time::Instant::now();
        let err = handle_export_logs(
            &state,
            TenantId::new("acme"),
            WriteMode::Strict,
            request(vec![record("hello", Vec::new())]),
            BASE_TS_NS,
            Some(b"idem-2462-deadline".to_vec()),
        )
        .await
        .expect_err("a lookup past its share of the deadline must fail the keyed write");
        let elapsed = started.elapsed();

        let LogIngestRequestError::Write(LogWriteError::Abandoned(message)) = &err else {
            panic!("expected the retryable Abandoned write error, got {err:?}");
        };
        assert!(err.is_retryable(), "{err:?} must be retryable");
        assert_eq!(message, MARKER_LOOKUP_FAILED_MESSAGE);
        let share = state.ack_deadline / 2;
        // hygiene-allow: wall-clock -- `started` is a tokio Instant under a
        // paused clock, so `elapsed` is the timer's deadline, not the machine's.
        assert!(
            elapsed >= share && elapsed <= share + PAUSED_CLOCK_SLACK,
            "refused after {elapsed:?}, half the deadline is {share:?}"
        );
        assert_eq!(
            held.held_count(),
            FIRST_BATCH_PROBES as usize,
            "the first batch was in flight when the deadline fired"
        );
        assert_eq!(
            put_count(&store),
            0,
            "none of the request's own data is written"
        );
        assert_eq!(
            warnings.0.lock().as_slice(),
            &[LookupFailureWarning {
                message: LOOKUP_DEADLINE_WARNING.to_string(),
                signal: "l".to_string(),
                request: "GET".to_string(),
                deadline: format!("{share:?}"),
                ..LookupFailureWarning::default()
            }],
            "exactly one WARN line, naming the deadline"
        );
        assert_eq!(
            idempotency_lookup_failures(Signal::Logs) - failures_before,
            1,
            "the deadline refusal must be counted exactly once"
        );
    }

    /// Issue #2462: a keyed request's lookup and its router write share one
    /// `ack_deadline`. A lookup that takes two of the five seconds leaves the
    /// strict write three seconds to be acknowledged, so a write whose data
    /// PUT never answers fails with the router's retryable ack timeout five
    /// seconds after the request started, not seven. The clock is paused, so
    /// the elapsed time is the timer's, not the machine's.
    ///
    /// Non-vacuity: passing `state.ack_deadline` to `router.write` instead of
    /// `router_write_budget(..)` refuses the write after seven seconds.
    #[tokio::test(start_paused = true)]
    async fn keyed_write_gets_only_the_budget_its_lookup_left() {
        let lookup_takes = Duration::from_secs(2);
        let store = Arc::new(SlowLookupStalledWriteStore::new(lookup_takes));
        let state = state_with_store(1, store.clone());
        assert!(lookup_takes < state.ack_deadline / 2);

        let started = tokio::time::Instant::now();
        let err = handle_export_logs(
            &state,
            TenantId::new("acme"),
            WriteMode::Strict,
            request(vec![record("hello", Vec::new())]),
            BASE_TS_NS,
            Some(b"idem-2462-budget".to_vec()),
        )
        .await
        .expect_err("a write whose data PUT never answers must time out");
        let elapsed = started.elapsed();

        assert!(
            matches!(err, LogIngestRequestError::Write(LogWriteError::AckTimeout)),
            "expected the router's ack timeout, got {err:?}"
        );
        assert!(err.is_retryable(), "{err:?} must be retryable");
        // The lookup ran to a miss: every hour of the default window, the
        // forward skew hour included, was probed once.
        assert_eq!(
            store.marker_gets(),
            u64::from(DEFAULT_IDEM_DEDUP_WINDOW_HOURS) + FIRST_BATCH_PROBES - 1
        );
        // hygiene-allow: wall-clock -- `started` is a tokio Instant under a
        // paused clock, so `elapsed` is the timer's deadline, not the machine's.
        assert!(
            elapsed >= state.ack_deadline && elapsed <= state.ack_deadline + PAUSED_CLOCK_SLACK,
            "refused after {elapsed:?}, one budget is {:?}",
            state.ack_deadline
        );
    }

    /// Issue #2462: a lookup that answers just inside its share of the
    /// deadline leaves the router write at least the other half. The lookup
    /// takes one millisecond less than half of `ack_deadline`, so the router
    /// write receives half of it plus that millisecond. The clock is paused.
    ///
    /// Non-vacuity: returning a third of `ack_deadline` from
    /// `marker_lookup_share` refuses the lookup before it answers, so no
    /// router write runs and the expected budget is never captured.
    #[tokio::test(start_paused = true)]
    async fn keyed_write_whose_lookup_answers_inside_its_share_keeps_half_the_budget() {
        let ack_deadline = Duration::from_secs(5);
        let lookup_takes = ack_deadline / 2 - Duration::from_millis(1);
        let store = Arc::new(SlowLookupStalledWriteStore::new(lookup_takes));
        let state = state_with_store(1, store.clone());
        assert_eq!(state.ack_deadline, ack_deadline);
        take_router_write_budgets();

        let err = handle_export_logs(
            &state,
            TenantId::new("acme"),
            WriteMode::Strict,
            request(vec![record("hello", Vec::new())]),
            BASE_TS_NS,
            Some(b"idem-2462-share".to_vec()),
        )
        .await
        .expect_err("a write whose data PUT never answers must time out");

        assert!(
            matches!(err, LogIngestRequestError::Write(LogWriteError::AckTimeout)),
            "the lookup must answer and the write time out, got {err:?}"
        );
        let received = ack_deadline - lookup_takes;
        assert!(received >= ack_deadline / 2);
        assert_eq!(
            take_router_write_budgets(),
            [received],
            "the router write receives what the lookup left, at least half"
        );
    }

    /// An unkeyed write looks nothing up, so its router write receives the
    /// whole `ack_deadline`, not a budget counted from the handler's start.
    /// The clock is not paused, so time passes between that start and the
    /// router write and a counted budget comes out short.
    ///
    /// Non-vacuity: starting the shared deadline for an unkeyed request too
    /// (`Some(..)` in place of `idempotency_key.is_some().then(..)`) makes the
    /// captured budget fall short of `ack_deadline`.
    #[tokio::test]
    async fn unkeyed_write_gets_the_whole_ack_deadline() {
        let state = state();
        take_router_write_budgets();

        handle_export_logs(
            &state,
            TenantId::new("acme"),
            WriteMode::Strict,
            request(vec![record("hello", Vec::new())]),
            BASE_TS_NS,
            None,
        )
        .await
        .expect("an unkeyed write succeeds");

        assert_eq!(take_router_write_budgets(), [state.ack_deadline]);
    }

    /// A hanging marker GET does not touch a request without a key: it never
    /// looks a marker up, so it writes as before.
    #[tokio::test]
    async fn unkeyed_write_is_unaffected_by_a_hanging_marker_probe() {
        use ravel_object_store::fault::{Occurrence, Op};

        let store = marker_probe_store(false);
        let held = store.hold(Op::Get, Some("/idem/".to_string()), Occurrence::Always);
        let state = state_with_store(1, store.clone());

        let outcome = handle_export_logs(
            &state,
            TenantId::new("acme"),
            WriteMode::Strict,
            request(vec![record("hello", Vec::new())]),
            BASE_TS_NS,
            None,
        )
        .await
        .expect("an unkeyed write never probes a marker");

        assert_eq!(outcome.tokens.len(), 1);
        assert_eq!(held.held_count(), 0);
        assert!(
            put_count(&store) > 0,
            "the unkeyed write must store its data"
        );
    }

    /// The same refusal does not touch a request without a key: it never
    /// looks a marker up, so it writes as before.
    #[tokio::test]
    async fn unkeyed_write_is_unaffected_by_a_refused_marker_probe() {
        let store = marker_probe_store(true);
        let state = state_with_store(1, store.clone());

        let outcome = handle_export_logs(
            &state,
            TenantId::new("acme"),
            WriteMode::Strict,
            request(vec![record("hello", Vec::new())]),
            BASE_TS_NS,
            None,
        )
        .await
        .expect("an unkeyed write never probes a marker");

        assert_eq!(outcome.tokens.len(), 1);
        assert_eq!(marker_get_refusals(&store), 0);
        assert!(
            put_count(&store) > 0,
            "the unkeyed write must store its data"
        );
    }

    /// A keyed write whose lookup succeeds and finds no marker writes its data
    /// and then its marker: absence is a miss, not a store error.
    #[tokio::test]
    async fn keyed_write_whose_marker_lookup_finds_nothing_still_writes() {
        let store = marker_probe_store(false);
        let state = state_with_store(1, store.clone());
        let tenant = TenantId::new("acme");
        let key = b"idem-2462".to_vec();

        let outcome = handle_export_logs(
            &state,
            tenant.clone(),
            WriteMode::Strict,
            request(vec![record("hello", Vec::new())]),
            BASE_TS_NS,
            Some(key.clone()),
        )
        .await
        .expect("a lookup that finds no marker proceeds to the write");

        assert_eq!(outcome.tokens.len(), 1);
        assert!(outcome.replayed_commit_token.is_none());
        assert!(put_count(&store) > 0, "the keyed write must store its data");
        let bucket = request_ingest_hour_bucket(BASE_TS_NS).expect("valid ingest ts");
        let lookup = read_marker(
            state.store.as_ref(),
            &tenant,
            Signal::Logs,
            &key,
            bucket,
            DEFAULT_IDEM_DEDUP_WINDOW_HOURS,
        )
        .await
        .expect("marker lookup");
        assert!(
            matches!(lookup, LookupOutcome::Hit(_)),
            "the write must leave its marker, got {lookup:?}"
        );
    }
}

/// Fixtures the logs and traces ingest tests share for a keyed write whose
/// marker lookup fails.
#[cfg(test)]
#[allow(clippy::expect_used)]
pub(crate) mod marker_lookup_test_support {
    use std::sync::Arc;

    use ravel_object_store::fault::{
        FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault, Sequence, SequenceStep,
    };
    use ravel_object_store::memory::MemoryStore;

    /// The store error a refused marker GET carries. The client-facing message
    /// must not repeat it.
    pub(crate) const STORE_ERROR_TEXT: &str = "AccessDenied: injected refusal of the marker GET";

    /// More passthrough steps than any one test's PUTs, so the `Op::Put`
    /// sequence's progress is the exact PUT count.
    const PUT_COUNTER_STEPS: usize = 256;

    /// GETs in `read_marker`'s first batch: the forward skew hours, the
    /// current hour and the one before it.
    pub(crate) const FIRST_BATCH_PROBES: u64 =
        ravel_ingest::IDEM_MARKER_FORWARD_SKEW_TOLERANCE_HOURS as u64 + 2;

    /// How far past the deadline a paused-clock test accepts the refusal: the
    /// timer wheel's millisecond resolution.
    pub(crate) const PAUSED_CLOCK_SLACK: std::time::Duration = std::time::Duration::from_millis(1);

    /// Held by every test that reads a delta of the process-global
    /// lookup-failure counter, so two such tests on one signal cannot count
    /// each other's refusals.
    pub(crate) static LOOKUP_FAILURE_COUNTER: tokio::sync::Mutex<()> =
        tokio::sync::Mutex::const_new(());

    /// A store that counts every PUT (sequence 0) and, when
    /// `refuse_marker_get` is set, refuses every GET of a key under an `idem/`
    /// directory the way S3 refuses a GetObject the policy does not grant.
    pub(crate) fn marker_probe_store(refuse_marker_get: bool) -> Arc<FaultStore<MemoryStore>> {
        let mut plan = FaultPlan::empty().with_sequence(
            Sequence::new(Op::Put).with_steps(vec![SequenceStep::Passthrough; PUT_COUNTER_STEPS]),
        );
        if refuse_marker_get {
            plan = plan.with_rule(
                Rule::new(Op::Get, ScriptedFault::Permanent(STORE_ERROR_TEXT.into()))
                    .with_key_contains("/idem/"),
            );
        }
        Arc::new(FaultStore::new(MemoryStore::new(), plan))
    }

    pub(crate) fn put_count(store: &FaultStore<MemoryStore>) -> u64 {
        let puts = store.sequence_progress(0);
        assert!(
            (puts as usize) < PUT_COUNTER_STEPS,
            "the PUT counter saturated at {puts}"
        );
        puts
    }

    pub(crate) fn marker_get_refusals(store: &FaultStore<MemoryStore>) -> u64 {
        store.fault_count(Op::Get, FaultKind::Permanent)
    }

    /// A store whose first marker GET answers only after `lookup_delay` on the
    /// tokio clock, so a keyed write's lookup takes exactly that long, and
    /// whose PUTs outside `idem/` never answer, so a strict write waits for its
    /// acknowledgement until its own deadline.
    pub(crate) struct SlowLookupStalledWriteStore {
        inner: MemoryStore,
        lookup_delay: std::time::Duration,
        marker_gets: std::sync::atomic::AtomicU64,
    }

    impl SlowLookupStalledWriteStore {
        pub(crate) fn new(lookup_delay: std::time::Duration) -> Self {
            Self {
                inner: MemoryStore::new(),
                lookup_delay,
                marker_gets: std::sync::atomic::AtomicU64::new(0),
            }
        }

        pub(crate) fn marker_gets(&self) -> u64 {
            self.marker_gets.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl ravel_object_store::ObjectStoreBackend for SlowLookupStalledWriteStore {
        async fn put(
            &self,
            key: &str,
            data: bytes::Bytes,
            opts: ravel_object_store::PutOptions,
        ) -> Result<ravel_object_store::PutOutcome, ravel_object_store::StoreError> {
            if !key.contains("/idem/") {
                return std::future::pending().await;
            }
            self.inner.put(key, data, opts).await
        }

        async fn get(
            &self,
            key: &str,
            range: ravel_object_store::GetRange,
        ) -> Result<ravel_object_store::GetOutcome, ravel_object_store::StoreError> {
            if key.contains("/idem/")
                && self
                    .marker_gets
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                    == 0
            {
                tokio::time::sleep(self.lookup_delay).await;
            }
            self.inner.get(key, range).await
        }

        async fn put_multipart<'a>(
            &'a self,
            key: &str,
        ) -> Result<Box<dyn ravel_object_store::MultipartUpload + 'a>, ravel_object_store::StoreError>
        {
            if !key.contains("/idem/") {
                return std::future::pending().await;
            }
            self.inner.put_multipart(key).await
        }

        async fn head(
            &self,
            key: &str,
        ) -> Result<ravel_object_store::ObjectMeta, ravel_object_store::StoreError> {
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<ravel_object_store::PageToken>,
        ) -> Result<ravel_object_store::ListPage, ravel_object_store::StoreError> {
            self.inner.list(prefix, page).await
        }

        async fn list_delimited(
            &self,
            prefix: &str,
        ) -> Result<ravel_object_store::DelimitedList, ravel_object_store::StoreError> {
            self.inner.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), ravel_object_store::StoreError> {
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> ravel_object_store::Capabilities {
            self.inner.capabilities()
        }
    }

    thread_local! {
        static ROUTER_WRITE_BUDGETS: std::cell::RefCell<Vec<std::time::Duration>> =
            const { std::cell::RefCell::new(Vec::new()) };
    }

    /// Record the budget a router write received, on this thread. A
    /// `#[tokio::test]` runs its handler on the test's own thread, so a test
    /// reads exactly its own writes.
    pub(crate) fn record_router_write_budget(budget: std::time::Duration) {
        ROUTER_WRITE_BUDGETS.with_borrow_mut(|budgets| budgets.push(budget));
    }

    /// Take the budgets every router write on this thread received so far,
    /// oldest first.
    pub(crate) fn take_router_write_budgets() -> Vec<std::time::Duration> {
        ROUTER_WRITE_BUDGETS.with_borrow_mut(std::mem::take)
    }

    /// The WARN message `marker_lookup_failure` writes.
    pub(crate) const LOOKUP_FAILED_WARNING: &str =
        "idempotency marker lookup failed; refusing the keyed write";

    /// The WARN message `marker_lookup_deadline` writes.
    pub(crate) const LOOKUP_DEADLINE_WARNING: &str =
        "idempotency marker lookup ran past the write deadline; refusing the keyed write";

    /// The fields of one WARN event that `marker_lookup_failure` or
    /// `marker_lookup_deadline` writes; a field the event does not carry is
    /// empty.
    #[derive(Debug, Default, Clone, PartialEq, Eq)]
    pub(crate) struct LookupFailureWarning {
        pub(crate) message: String,
        pub(crate) signal: String,
        pub(crate) request: String,
        pub(crate) key: String,
        pub(crate) error: String,
        pub(crate) deadline: String,
    }

    /// Records every WARN event whose message is a marker lookup refusal.
    #[derive(Default, Clone)]
    pub(crate) struct LookupFailureWarnings(
        pub(crate) Arc<parking_lot::Mutex<Vec<LookupFailureWarning>>>,
    );

    impl<S> tracing_subscriber::Layer<S> for LookupFailureWarnings
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
            #[derive(Default)]
            struct Visitor {
                fields: LookupFailureWarning,
            }
            impl tracing::field::Visit for Visitor {
                fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                    self.record_debug(field, &value);
                }
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    let text = format!("{value:?}").trim_matches('"').to_string();
                    match field.name() {
                        "message" => self.fields.message = text,
                        "signal" => self.fields.signal = text,
                        "request" => self.fields.request = text,
                        "key" => self.fields.key = text,
                        "error" => self.fields.error = text,
                        "deadline" => self.fields.deadline = text,
                        _ => {}
                    }
                }
            }
            let mut visitor = Visitor::default();
            event.record(&mut visitor);
            if [LOOKUP_FAILED_WARNING, LOOKUP_DEADLINE_WARNING]
                .contains(&visitor.fields.message.as_str())
            {
                self.0.lock().push(visitor.fields);
            }
        }
    }
}
