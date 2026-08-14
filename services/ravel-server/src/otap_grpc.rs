//! OTAP `ArrowMetricsService::arrow_metrics`: the metrics Arrow stream, the
//! columnar counterpart of [`crate::otlp_grpc`]. Compiled only under the
//! `otap` feature (docs/otap-ingest.md risk mitigation: the gateway builds
//! without the arrow dependency tree by default until the bench panel
//! justifies default-on).
//!
//! It carries `BatchArrowRecords` instead of protobuf `ExportMetricsService-
//! Request`s and replies `BatchStatus` per `batch_id` instead of a plain
//! ack, but the ingest contract is identical to OTLP's gRPC path: resolve the
//! tenant from stream metadata, decode + normalize each batch into the same
//! `IngestPoint`s, write them through the same `IngestRouter`, and -- this is
//! phase 3's point (docs/otap-ingest.md "Strict ack") -- reply only after the
//! batch's flushes are durable, carrying the resulting commit tokens back so
//! read-your-write works over OTAP the same way `x-ravel-commit-token` carries
//! them on the OTLP path.
//!
//! Per-batch error handling mirrors the [`StreamState`] contract exactly: a
//! malformed batch ([`DecodeError::Batch`]) nacks only that `batch_id` and the
//! stream keeps going, while a corrupt IPC stream ([`DecodeError::Stream`])
//! nacks the batch and ends the gRPC stream so the client re-establishes it.

use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use ravel_ingest::{IngestPoint, WriteMode, plausible_ingest_clock};
use ravel_otap::normalize::normalize_decoded;
use ravel_otap::proto::experimental::arrow::v1::arrow_metrics_service_server::ArrowMetricsService;
use ravel_otap::proto::experimental::arrow::v1::{BatchArrowRecords, BatchStatus, StatusCode};
use ravel_otap::stream::{DecodeError, DecodedBatch, StreamConfig, StreamState};
use ravel_types::{CommitToken, SeriesId, Signal, TenantId};
use tonic::{Request, Response, Status, Streaming};

use crate::ingest::{IngestRequestError, IngestState};
use crate::otlp_grpc::metadata_to_headers;
use crate::otlp_http::{GatewayState, now_ns, write_mode_from_headers};
use crate::wire_byte_count::{WireByteCounter, wire_byte_counter};

pub struct GrpcArrowMetricsService {
    state: Arc<GatewayState>,
}

impl GrpcArrowMetricsService {
    pub fn new(state: Arc<GatewayState>) -> Self {
        GrpcArrowMetricsService { state }
    }
}

/// Per-stream state threaded through [`futures::stream::unfold`]: the inbound
/// `BatchArrowRecords` stream, the decode state machine, and the tenant/mode
/// resolved once from the stream-open metadata. `finished` latches the stream
/// closed after a terminal item (a poisoned decoder or a transport error) so
/// the next poll returns `None`.
struct StreamCtx {
    inbound: Streaming<BatchArrowRecords>,
    decoder: StreamState,
    state: Arc<GatewayState>,
    tenant: TenantId,
    mode: WriteMode,
    finished: bool,
    /// Queue of completed gRPC message-frame byte lengths `WireByteCountLayer`
    /// has parsed off this stream's request body, in wire order.
    /// `process_batch` pops exactly one entry per batch it decodes: each
    /// `.message()` call that yields a batch corresponds to exactly one
    /// completed frame the layer already queued, since the layer parses the
    /// same bytes the decoder consumes.
    wire_bytes: WireByteCounter,
}

type BatchStatusStream = Pin<Box<dyn Stream<Item = Result<BatchStatus, Status>> + Send>>;

#[tonic::async_trait]
impl ArrowMetricsService for GrpcArrowMetricsService {
    type ArrowMetricsStream = BatchStatusStream;

    async fn arrow_metrics(
        &self,
        request: Request<Streaming<BatchArrowRecords>>,
    ) -> Result<Response<Self::ArrowMetricsStream>, Status> {
        // Tenant and write mode are read once from the stream-open metadata,
        // exactly as the OTLP gRPC services read them from a unary request's
        // metadata. Both are per-connection: OTAP has no per-batch metadata we
        // interpret (the optional hpack `headers` field is not read here).
        let headers = metadata_to_headers(request.metadata());
        let tenant = self
            .state
            .tenant_resolver
            .resolve(&headers)
            .map_err(|_| Status::unauthenticated("invalid or missing tenant credentials"))?;
        let mode = write_mode_from_headers(&headers);
        let wire_bytes = wire_byte_counter(&request)?;

        let ctx = StreamCtx {
            inbound: request.into_inner(),
            decoder: StreamState::new(StreamConfig::default()),
            state: self.state.clone(),
            tenant,
            mode,
            finished: false,
            wire_bytes,
        };

        let stream = futures::stream::unfold(ctx, |mut ctx| async move {
            if ctx.finished {
                return None;
            }
            match ctx.inbound.message().await {
                Ok(Some(batch)) => {
                    let (status, terminate) = process_batch(&mut ctx, batch).await;
                    ctx.finished = terminate;
                    Some((Ok(status), ctx))
                }
                // Client half-closed cleanly: end the response stream.
                Ok(None) => None,
                // A transport-level error reading the request stream is the
                // terminal item; surface it and stop.
                Err(status) => {
                    ctx.finished = true;
                    Some((Err(status), ctx))
                }
            }
        });

        Ok(Response::new(Box::pin(stream)))
    }
}

/// Decode, normalize, and write one `BatchArrowRecords`, returning the
/// `BatchStatus` to reply and whether the stream must be torn down after it.
async fn process_batch(ctx: &mut StreamCtx, batch: BatchArrowRecords) -> (BatchStatus, bool) {
    // Captured before `decode` consumes the batch, so a decode error can still
    // name the batch it rejects.
    let batch_id = batch.batch_id;

    // Process-wide in-flight ingest ceiling (extended to OTAP): admit or shed
    // this batch before it does any ingest work -- the
    // same gate each unary OTLP gRPC export applies via `try_admit`
    // (otlp_grpc.rs), so a burst of OTAP batches cannot bypass the
    // admission-shed backpressure OTLP already respects. The `_permit` binding
    // holds the slot across the rest of this batch (byte-rate accounting,
    // decode, and the durable write); its RAII drop when `process_batch`
    // returns releases the slot on every return arm below, including the error
    // paths. A shed is RESOURCE_EXHAUSTED, matching OTLP's shed status
    // (`ingest_concurrency_shed_status`) exactly; `IngestShed`'s Display is the
    // single source of that message string.
    //
    // Like the byte-rate rejection below, a shed happens before
    // `ctx.decoder.decode` ever sees this batch, so it tears the gRPC stream
    // down (returns `true`): a skipped batch's Schema/DictionaryBatch IPC
    // messages never reach the stateful per-`DecoderKey` decoder, and every
    // later batch would then decode against state that silently missed one.
    // The client re-establishes a fresh stream with fresh decoder state.
    let _permit = match ctx.state.ingest_concurrency.try_admit() {
        Ok(permit) => permit,
        Err(shed) => {
            return (
                nack(batch_id, StatusCode::ResourceExhausted, shed.to_string()),
                true,
            );
        }
    };

    // Layer 2 (ADR-0051 section 1, layer 2): byte rate on this
    // batch's wire bytes, charged before decode so over-rate bytes cost one
    // buffered body and nothing else. `WireByteCountLayer` parses this
    // stream's request body into discrete gRPC message frames as
    // `ctx.inbound` reads them and queues each frame's length in wire order;
    // the `.message().await` call that produced this batch corresponds to
    // exactly one queued frame, so popping one entry here charges this
    // batch's own bytes, not an arbitrary running total sampled at whatever
    // moment the transport happened to hand bytes to the body (a stream can
    // have several batches' bytes arrive in one underlying read, well ahead
    // of the decoder yielding them one message at a time). This keeps the
    // same pre-decode ordering the ADR pins, without walking the decoded
    // batch with `Message::encoded_len`.
    let batch_bytes = match ctx.wire_bytes.pop_message_bytes() {
        Some(bytes) => bytes,
        // `ctx.inbound.message()` just returned a decoded batch, so the same
        // bytes must already be a completed frame in the queue; a miss here
        // means the layer and the decoder disagree about framing.
        None => {
            return (
                nack(
                    batch_id,
                    StatusCode::Internal,
                    "ingest admission: wire-byte count unavailable for this batch".to_string(),
                ),
                true,
            );
        }
    };

    // A pre-decode rejection means this batch's Schema/DictionaryBatch IPC
    // messages never reach the stateful per-`DecoderKey` `StreamDecoder`
    // (crates/ravel-otap/src/stream.rs): the decoder caches schema and
    // dictionary state across batches, so every later batch on this stream
    // would decode against state that silently skipped a batch, desyncing it.
    // A byte-rate rejection therefore tears the gRPC stream down (returns
    // `true`, matching the `DecodeError::Stream` arm below), forcing the
    // client to re-establish a fresh stream with fresh decoder state rather
    // than feeding more batches into a decoder that missed one.
    if let Err(rejection) =
        ctx.state
            .admission
            .check_byte_rate(&ctx.tenant, Signal::Metrics, batch_bytes, now_ns())
    {
        return (
            nack(
                batch_id,
                StatusCode::ResourceExhausted,
                rejection.reason.to_string(),
            ),
            true,
        );
    }

    match ctx.decoder.decode(batch) {
        Ok(decoded) => {
            match write_batch(&ctx.state.ingest, &ctx.tenant, ctx.mode, &decoded, now_ns()).await {
                Ok(outcome) => (
                    ack(batch_id, &outcome.tokens, outcome.dropped_points),
                    false,
                ),
                Err(IngestRequestError::Admission(rejection)) => (
                    nack(
                        batch_id,
                        StatusCode::ResourceExhausted,
                        rejection.reason.to_string(),
                    ),
                    false,
                ),
                // Receiver-clock floor (ADR-0051 amendment): UNAVAILABLE,
                // the replica's fault. The batch already decoded cleanly, so the
                // decoder is unharmed and the stream stays open (false).
                Err(IngestRequestError::ClockImplausible(msg)) => {
                    (nack(batch_id, StatusCode::Unavailable, msg), false)
                }
                // A hard shard_count provisioning failure (ADR-0050 section 5):
                // the configured value would resolve over a subset of shards, or
                // the record is unreadable so the true shard_count is unknown.
                // Not retryable and not the client's fault; nack this batch as an
                // internal fault, matching the OTLP gRPC path (otlp_grpc.rs), and
                // keep the stream open so unrelated batches still flow.
                Err(IngestRequestError::Provisioning(msg)) => {
                    (nack(batch_id, StatusCode::Internal, msg), false)
                }
                Err(IngestRequestError::Write(err)) => {
                    // Same retry classification the OTLP gRPC path applies: a
                    // retryable write is RESOURCE_EXHAUSTED/UNAVAILABLE-shaped
                    // backpressure, anything else is an internal fault. The
                    // buffer-budget shed (ADR-0069) is specifically
                    // RESOURCE_EXHAUSTED, matching the byte-rate rejection.
                    let code = match err {
                        ravel_ingest::WriteError::BufferBudgetExceeded => {
                            StatusCode::ResourceExhausted
                        }
                        _ if err.is_retryable() => StatusCode::Unavailable,
                        _ => StatusCode::Internal,
                    };
                    (nack(batch_id, code, err.to_string()), false)
                }
            }
        }
        // A malformed batch nacks only itself; the decoder is unharmed and the
        // stream continues (stream.rs `BatchError` contract).
        Err(DecodeError::Batch(err)) => (
            nack(batch_id, StatusCode::InvalidArgument, err.to_string()),
            false,
        ),
        // The IPC stream state is corrupt and every future decode would return
        // `Poisoned`; nack this batch and end the gRPC stream so the client
        // tears it down and re-establishes it (stream.rs `StreamError`).
        Err(DecodeError::Stream(err)) => {
            (nack(batch_id, StatusCode::Internal, err.to_string()), true)
        }
    }
}

/// The result of a successful [`write_batch`]: the commit tokens a strict
/// write produces, and how many data points the active-series cap dropped
/// from the batch before the write. `dropped_points` is what the ack must
/// surface so an OTAP client sees a partial success rather than a clean ack
/// (ADR-0051 layer 4: active-series-cap breaches are per-series partial
/// success, "OK + partial success" in the rejection-status table).
struct WriteOutcome {
    tokens: Vec<CommitToken>,
    dropped_points: usize,
}

/// Normalize a decoded batch and write its points through the shared ingest
/// router, returning the commit tokens a strict write produces and the count
/// of points the active-series cap dropped. Mirrors
/// [`crate::ingest::handle_export`]'s point assembly and layer-4 admission
/// (scalar and native-histogram points feed one write so a batch shares a
/// single receipt, and series-creation-rate/active-series-cap are enforced
/// the same way); this is a genuinely separate code path from
/// `handle_export` (it normalizes and builds points itself rather than
/// calling it), so it carries its own copy of that wiring. OTAP carries only
/// scalar metric points, so `histogram_points` here is the
/// exploded-histogram output, not native histograms.
async fn write_batch(
    ingest: &IngestState,
    tenant: &TenantId,
    mode: WriteMode,
    decoded: &DecodedBatch,
    ingest_ts_ns: i64,
) -> Result<WriteOutcome, IngestRequestError> {
    // Receiver-clock plausibility (ADR-0051 amendment): checked before
    // the normalize window is computed. A nonsense reference clock rejects the
    // batch UNAVAILABLE (counted reason="clock"); the fault is the replica's.
    if let Err(msg) = plausible_ingest_clock(ingest_ts_ns) {
        ingest
            .admission
            .record_clock_rejection(tenant, ravel_types::Signal::Metrics);
        return Err(IngestRequestError::ClockImplausible(msg));
    }

    // Record the tenant's recovery manifest on its first write in this process
    // (ADR-0050 section 3). Best-effort and off the durability path: see
    // `crate::tenancy::ensure_recovery_manifest`. OTAP is a genuinely separate
    // code path from `handle_export` (see this function's own doc comment),
    // so it needs its own call, matching ingest.rs/logs_ingest.rs/
    // traces_ingest.rs/remote_write.rs.
    crate::tenancy::ensure_recovery_manifest(&ingest.recovery, tenant, ingest_ts_ns).await;

    // Pin/validate the (tenant, Metrics) shard_count provisioning record on
    // first write (ADR-0050 section 5); a hard mismatch fails this request.
    crate::provisioning::ensure_provisioning_record(
        &ingest.provisioning,
        tenant,
        ravel_types::Signal::Metrics,
        ingest_ts_ns,
    )
    .await
    .map_err(|e| IngestRequestError::Provisioning(e.to_string()))?;

    let normalized = normalize_decoded(tenant, decoded, &ingest.limits, ingest_ts_ns);
    let mut points: Vec<IngestPoint> =
        Vec::with_capacity(normalized.points.len() + normalized.histogram_points.len());
    points.extend(normalized.points.into_iter().map(IngestPoint::from));
    points.extend(
        normalized
            .histogram_points
            .into_iter()
            .map(IngestPoint::from),
    );

    let candidate_series: Vec<SeriesId> = points.iter().map(|p| p.series_id).collect();
    ingest
        .admission
        .check_series_creation_rate(tenant, &candidate_series, ingest_ts_ns)
        .map_err(IngestRequestError::Admission)?;
    let admission = ingest
        .admission
        .admit_series(tenant, candidate_series, ingest_ts_ns);
    let points_before = points.len();
    if !admission.rejected.is_empty() {
        let admitted: HashSet<SeriesId> = admission.admitted.into_iter().collect();
        points.retain(|p| admitted.contains(&p.series_id));
    }
    // Points dropped by the active-series cap. Reported on the ack so the
    // client sees the partial success rather than a clean OK (ADR-0051
    // layer 4), never silently discarded.
    let dropped_points = points_before - points.len();

    let receipt = ingest
        .router
        .write_values(tenant.clone(), points, mode, ingest.ack_deadline)
        .await
        .map_err(IngestRequestError::Write)?;
    Ok(WriteOutcome {
        tokens: receipt.tokens,
        dropped_points,
    })
}

/// An OK `BatchStatus` for a written batch.
///
/// With no dropped points, `status_message` carries the batch's commit
/// tokens, comma-joined, the way the OTLP path returns them in the
/// `x-ravel-commit-token` header (empty when the write produced no token: a
/// buffered write, or a batch that normalized to zero admitted points).
///
/// When the active-series cap dropped points, the status stays `Ok` (ADR-0051
/// layer 4 is "OK + partial success", not a nack) but `status_message` leads
/// with a `partial-success:` line reporting the dropped-point count before the
/// `commit-tokens=` list, so a client sees the drop instead of reading a clean
/// ack as if the whole batch landed. This is OTAP's analogue of OTLP's
/// `ExportMetricsPartialSuccess.rejected_data_points`; `BatchStatus` has only
/// the one `status_message` string to carry it.
fn ack(batch_id: i64, tokens: &[CommitToken], dropped_points: usize) -> BatchStatus {
    let tokens_joined = tokens
        .iter()
        .map(|token| token.encode())
        .collect::<Vec<_>>()
        .join(",");
    let status_message = if dropped_points == 0 {
        tokens_joined
    } else {
        format!(
            "partial-success: {dropped_points} data points rejected (active-series cap); \
             commit-tokens={tokens_joined}"
        )
    };
    BatchStatus {
        batch_id,
        status_code: StatusCode::Ok as i32,
        status_message,
    }
}

fn nack(batch_id: i64, code: StatusCode, message: String) -> BatchStatus {
    BatchStatus {
        batch_id,
        status_code: code as i32,
        status_message: message,
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_ingest::{
        AdmissionController, AdmissionLimits, IngestConfig, IngestRouter,
        MIN_PLAUSIBLE_INGEST_CLOCK_NS, SystemClock,
    };
    use ravel_object_store::ObjectStoreBackend;
    use ravel_object_store::memory::MemoryStore;
    use ravel_otap::stream::DecodedBatch;
    use std::time::Duration;

    fn ingest_state() -> IngestState {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let router = Arc::new(IngestRouter::new(
            IngestConfig::default(),
            store,
            Signal::Metrics,
            Arc::new(SystemClock),
        ));
        IngestState {
            router,
            limits: ravel_otlp::IngestLimits::default(),
            ack_deadline: Duration::from_secs(5),
            admission: Arc::new(AdmissionController::new(
                Arc::new(SystemClock),
                AdmissionLimits::default(),
            )),
            recovery: None,
            provisioning: None,
        }
    }

    /// an OTAP batch whose receiver (ingest) clock is below the 2020 floor
    /// must be rejected by `write_batch` as the whole-batch `ClockImplausible`
    /// error (which the OTAP driver maps to a nacked `BatchStatus`;
    /// `is_retryable() == true` is the UNAVAILABLE class), and the
    /// `reason="clock"` admission counter for the tenant's Metrics signal must
    /// increment. The clock is a fixed sub-floor timestamp, never
    /// `SystemTime::now()`.
    ///
    /// The clock guard is the first thing `write_batch` does, before
    /// `normalize_decoded` ever touches the payloads, so an empty `DecodedBatch`
    /// exercises the rejection path exactly.
    ///
    /// Non-vacuity: delete the `plausible_ingest_clock` guard at the top of
    /// `write_batch` (otap_grpc.rs, the `if let Err(msg) = ...` block) and this
    /// test fails, because the empty batch then writes cleanly and returns `Ok`,
    /// and the counter stays at 0.
    #[tokio::test]
    async fn receiver_clock_below_floor_rejects_unavailable_with_reason_clock() {
        let ingest = ingest_state();
        let tenant = TenantId::new("acme");
        // One nanosecond below the 2020 floor: an implausible receiver clock.
        let sub_floor = MIN_PLAUSIBLE_INGEST_CLOCK_NS - 1;
        let batch = DecodedBatch {
            batch_id: 1,
            payloads: Vec::new(),
        };

        // `WriteOutcome` is not `Debug`, so match rather than `expect_err`.
        let err = match write_batch(&ingest, &tenant, WriteMode::Strict, &batch, sub_floor).await {
            Ok(_) => panic!("a sub-floor receiver clock must reject the whole batch"),
            Err(err) => err,
        };

        // (a) The typed rejection is ClockImplausible, and it is retryable, so
        // the OTAP nack is the UNAVAILABLE class.
        assert!(
            matches!(err, IngestRequestError::ClockImplausible(_)),
            "expected ClockImplausible, got: {err:?}"
        );
        assert!(
            err.is_retryable(),
            "ClockImplausible is retryable, the UNAVAILABLE class on this surface"
        );

        // (b) The admission rejected counter increments under reason=\"clock\"
        // for this tenant's Metrics signal.
        let row = ingest
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
