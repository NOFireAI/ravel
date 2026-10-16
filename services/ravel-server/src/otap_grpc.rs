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

use std::borrow::Borrow;
use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use ravel_ingest::{
    IngestByteShed, IngestPoint, Submitted, WriteError, WriteMode, plausible_ingest_clock,
};
use ravel_otap::normalize::{normalize_prepared_with_metadata, prepare_batch};
use ravel_otap::proto::experimental::arrow::v1::arrow_metrics_service_server::ArrowMetricsService;
use ravel_otap::proto::experimental::arrow::v1::{BatchArrowRecords, BatchStatus, StatusCode};
use ravel_otap::stream::{DecodeError, DecodedBatch, StreamConfig, StreamError, StreamState};
use ravel_otlp::NormalizeRejectCounts;
use ravel_types::{CommitToken, ExemplarCap, SeriesId, Signal, TenantId};
use tonic::{Request, Response, Status, Streaming};

use crate::ingest::{IngestRequestError, IngestState};
use crate::ingest_mode::resolve_write_mode;
use crate::otlp_grpc::metadata_to_headers;
use crate::otlp_http::{GatewayState, now_ns};
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
        // `GrpcIngestAdmissionLayer` normally authenticated the tenant on the
        // stream's request head already (issue #1705).
        let headers = metadata_to_headers(request.metadata());
        let tenant = crate::ingest_admission::grpc_request_tenant(
            self.state.tenant_resolver.as_ref(),
            &request,
            &headers,
        )?;
        let mode = resolve_write_mode(&headers, self.state.strict_flush_policy);
        let wire_bytes = wire_byte_counter(&request)?;

        let ctx = StreamCtx {
            inbound: request.into_inner(),
            decoder: StreamState::new(StreamConfig::default())
                .with_write_gate(self.state.write_gate.clone()),
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
    // this batch before it does any ingest work, the same gate each unary
    // OTLP gRPC export applies, so a burst of OTAP batches cannot bypass the
    // admission-shed backpressure OTLP already respects. The permit covers
    // byte-rate accounting, decode, and the submit; it is returned once the
    // write is enqueued, after the decoded batch has dropped, so the strict
    // ack wait holds neither (ADR-2793 decision 4). Every earlier return arm
    // below drops it with `process_batch`. A shed is RESOURCE_EXHAUSTED,
    // matching OTLP's shed status (`ingest_concurrency_shed_status`) exactly;
    // `IngestShed`'s Display is the single source of that message string.
    //
    // Like the byte-rate rejection below, a shed happens before
    // `ctx.decoder.decode` ever sees this batch, so it tears the gRPC stream
    // down (returns `true`): a skipped batch's Schema/DictionaryBatch IPC
    // messages never reach the stateful per-`DecoderKey` decoder, and every
    // later batch would then decode against state that silently missed one.
    // The client re-establishes a fresh stream with fresh decoder state.
    let permit = match ctx.state.ingest_concurrency.try_admit() {
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

    match ctx.decoder.decode_gated(batch).await {
        Ok(decoded) => {
            let ingest = &ctx.state.ingest;
            let (tenant, mode) = (&ctx.tenant, ctx.mode);
            let submitted = submit_and_release(decoded, permit, |decoded| {
                submit_batch(ingest, tenant, mode, decoded, now_ns())
            })
            .await;
            let written = match submitted {
                Ok(pending) => pending.acks(tenant).await,
                Err(err) => Err(err),
            };
            write_reply(batch_id, written)
        }
        Err(err) => decode_error_reply(batch_id, err),
    }
}

/// Runs `submit` on `batch`, then drops `permit`. `submit` consumes the batch,
/// so the batch is gone by the time the future resolves, and the permit
/// outlives both: the in-flight ceiling keeps bounding a batch's memory until
/// that memory is freed, and stops bounding the request at enqueue.
async fn submit_and_release<B, P, F, R>(batch: B, permit: P, submit: impl FnOnce(B) -> F) -> R
where
    F: Future<Output = R>,
{
    let submitted = submit(batch).await;
    drop(permit);
    submitted
}

/// The reply to a decoded batch's write. The batch already decoded cleanly,
/// so every arm keeps the stream open.
fn write_reply(
    batch_id: i64,
    written: Result<WriteOutcome, IngestRequestError>,
) -> (BatchStatus, bool) {
    match written {
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
        // Receiver-clock floor (ADR-0051 amendment): UNAVAILABLE, the
        // replica's fault.
        Err(IngestRequestError::ClockImplausible(msg)) => {
            (nack(batch_id, StatusCode::Unavailable, msg), false)
        }
        // A hard shard_count provisioning failure (ADR-0050 section 5): the
        // configured value would resolve over a subset of shards, or the
        // record is unreadable so the true shard_count is unknown. Not
        // retryable and not the client's fault; nack this batch as an internal
        // fault, matching the OTLP gRPC path (otlp_grpc.rs), and keep the
        // stream open so unrelated batches still flow.
        Err(IngestRequestError::Provisioning(msg)) => {
            (nack(batch_id, StatusCode::Internal, msg), false)
        }
        Err(IngestRequestError::Write(err)) => {
            // Same retry classification the OTLP gRPC path applies: a
            // retryable write is RESOURCE_EXHAUSTED/UNAVAILABLE-shaped
            // backpressure, anything else is an internal fault. The
            // buffer-budget shed (ADR-0069), including the label-projection
            // charge (ADR-2708 D2), is specifically RESOURCE_EXHAUSTED,
            // matching the byte-rate rejection.
            let code = match err {
                ravel_ingest::WriteError::BufferBudgetExceeded
                | ravel_ingest::WriteError::DeferralCapReached
                | ravel_ingest::WriteError::PendingAckLimitReached => StatusCode::ResourceExhausted,
                _ if err.is_retryable() => StatusCode::Unavailable,
                _ => StatusCode::Internal,
            };
            (nack(batch_id, code, err.to_string()), false)
        }
    }
}

/// The reply to a batch `StreamState::decode_gated` rejected, and whether the
/// stream must be torn down after it.
fn decode_error_reply(batch_id: i64, err: DecodeError) -> (BatchStatus, bool) {
    match err {
        // A malformed batch nacks only itself and the stream continues
        // (stream.rs `BatchError` contract).
        DecodeError::Batch(err) => (
            nack(batch_id, StatusCode::InvalidArgument, err.to_string()),
            false,
        ),
        // The write gate returned no decompression result for one payload
        // after the batch's earlier payloads had already fed their decoders
        // (stream.rs `StreamError::Gate`). The stream ends as on any other
        // `StreamError`, which is what keeps a retry off this stream's
        // advanced decoder state: a resend reaches a fresh stream. The code
        // follows the HTTP gate answers rather than the corruption arm's
        // INTERNAL, because the batch itself was not at fault: INTERNAL for a
        // panicked job, the retryable UNAVAILABLE for a cancelled one.
        DecodeError::Stream(StreamError::Gate(gate_err)) => {
            let code = match gate_err {
                ravel_cpu_gate::CpuGateError::Panicked => StatusCode::Internal,
                ravel_cpu_gate::CpuGateError::Cancelled | ravel_cpu_gate::CpuGateError::Closed => {
                    StatusCode::Unavailable
                }
            };
            let err = StreamError::Gate(gate_err);
            (nack(batch_id, code, err.to_string()), true)
        }
        // The IPC stream state is corrupt and every future decode would return
        // `Poisoned`; nack this batch and end the gRPC stream so the client
        // tears it down and re-establishes it (stream.rs `StreamError`).
        DecodeError::Stream(err) => (nack(batch_id, StatusCode::Internal, err.to_string()), true),
    }
}

/// The result of a successful [`submit_batch`] and its acks: the commit tokens a strict
/// write produces, and how many data points the active-series cap dropped
/// from the batch before the write. `dropped_points` is what the ack must
/// surface so an OTAP client sees a partial success rather than a clean ack
/// (ADR-0051 layer 4: active-series-cap breaches are per-series partial
/// success, "OK + partial success" in the rejection-status table).
struct WriteOutcome {
    tokens: Vec<CommitToken>,
    dropped_points: usize,
}

/// Normalize a decoded batch and submit its points to the shared ingest
/// router, carrying the count of points the active-series cap dropped. Mirrors
/// [`crate::ingest::handle_export`]'s point assembly and layer-4 admission
/// (scalar and native-histogram points feed one write so a batch shares a
/// single receipt, and series-creation-rate/active-series-cap are enforced
/// the same way); this is a genuinely separate code path from
/// `handle_export` (it normalizes and builds points itself rather than
/// calling it), so it carries its own copy of that wiring. OTAP carries only
/// scalar metric points, so `histogram_points` here is the
/// exploded-histogram output, not native histograms.
///
/// Returns once the points are enqueued; [`PendingBatch::acks`] awaits a
/// strict write's durability. The stream path passes the batch by value so it
/// drops before this returns, which is what lets the caller return the
/// in-flight permit without the batch outliving it.
async fn submit_batch<'a>(
    ingest: &'a IngestState,
    tenant: &TenantId,
    mode: WriteMode,
    decoded: impl Borrow<DecodedBatch>,
    ingest_ts_ns: i64,
) -> Result<PendingBatch<'a>, IngestRequestError> {
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

    // OTAP mirrors OTLP's normalize point-for-point, metadata included: the same
    // `(family, type, help, unit)` tuple, from the METRICS table's
    // `metric_type`/`description`/`unit` columns (ADR-0085 decision 1). The
    // exemplar-carrying inner call is unchanged, so this stays the
    // `normalize_decoded` wrapper's behavior plus the metadata.
    //
    // The request-scoped `ExemplarCap` and the discarded `result.exemplars` are
    // what the `normalize_decoded` wrapper builds internally: nothing on this
    // path stores exemplars (it writes through `write_values`, not
    // `write_values_with_exemplars`), and the wrapper's drop-count rejection
    // goes into a `rejected` vector this function does not read either. So this
    // call writes the points the wrapper would, plus the metadata tuple.
    // The same resolved-label-byte bound and budget charge as the OTLP path
    // (ADR-2708 D2): see `crate::ingest::charge_projected_labels`. The batch
    // is flattened and resolved once; no label exists until the normalize
    // below has passed the bound.
    let prepared = prepare_batch(decoded.borrow(), &ingest.limits);
    let label_charge = crate::ingest::charge_projected_labels(
        &ingest.budget,
        prepared.projected_label_bytes().get(),
        ingest.limits.max_resolved_label_bytes_per_request,
    )
    .map_err(|IngestByteShed| IngestRequestError::Write(WriteError::BufferBudgetExceeded))?;
    let mut cap = ExemplarCap::new(ingest.limits.exemplar_cap_window_ns);
    let (result, metadata) =
        normalize_prepared_with_metadata(tenant, prepared, &ingest.limits, ingest_ts_ns, &mut cap);
    let normalized = result.output;
    drop(decoded);
    // Layer 3's rejections, counted exactly as the OTLP path counts them
    // (ADR-0051 section 3). OTAP has no partial-success field to carry them,
    // so before this the only trace of a structurally rejected batch was the
    // absence of its points: switching transport silently moved the signal.
    ingest.normalize_metrics.record(
        tenant,
        ravel_types::Signal::Metrics,
        NormalizeRejectCounts::from_metric_rejections(&normalized.rejected),
    );
    // OTAP builds resource labels with the OTLP builder (ADR-2708 D4), so its
    // non-allowlisted resource attributes count the same way.
    ingest.normalize_metrics.record_resource_attrs_dropped(
        tenant,
        ravel_types::Signal::Metrics,
        ravel_otlp::resource_attrs_dropped_from_rejections(&normalized.rejected),
    );
    // Synchronous, no I/O, off the acknowledgement path: see the twin call in
    // `crate::ingest::handle_export`.
    if let Some(sink) = &ingest.metadata_sink {
        sink.observe(tenant, tenant.hash(), metadata);
    }
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

    drop(label_charge);
    let submitted = ingest
        .router
        .submit_values(tenant.clone(), points, mode, ingest.ack_deadline)
        .await
        .map_err(IngestRequestError::Write)?;
    Ok(PendingBatch {
        submitted,
        dropped_points,
    })
}

/// A batch [`submit_batch`] enqueued, waiting on its strict acks.
struct PendingBatch<'a> {
    submitted: Submitted<'a>,
    dropped_points: usize,
}

impl PendingBatch<'_> {
    async fn acks(self, tenant: &TenantId) -> Result<WriteOutcome, IngestRequestError> {
        let receipt = self.submitted.acks().await.map_err(|err| {
            // A partial multi-shard commit: the durable siblings are real
            // data, logged the way the OTLP paths log them since OTAP has no
            // channel to hand their tokens back.
            let durable_shard_count = err.durable_tokens().len();
            if durable_shard_count > 0 {
                tracing::warn!(
                    tenant_hash = %tenant.hash().to_hex(),
                    durable_shard_count,
                    "metric write partially committed before a sibling shard \
                     failed"
                );
            }
            IngestRequestError::Write(err)
        })?;
        Ok(WriteOutcome {
            tokens: receipt.tokens,
            dropped_points: self.dropped_points,
        })
    }
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
        AdmissionController, AdmissionLimits, IngestByteBudget, IngestByteBudgetLimit,
        IngestConfig, IngestRouter, MIN_PLAUSIBLE_INGEST_CLOCK_NS, PendingAckBudget, SystemClock,
    };
    use ravel_object_store::ObjectStoreBackend;
    use ravel_object_store::memory::MemoryStore;
    use ravel_otap::stream::DecodedBatch;
    use std::sync::Mutex;
    use std::time::Duration;

    async fn write_batch(
        ingest: &IngestState,
        tenant: &TenantId,
        mode: WriteMode,
        decoded: &DecodedBatch,
        ingest_ts_ns: i64,
    ) -> Result<WriteOutcome, IngestRequestError> {
        submit_batch(ingest, tenant, mode, decoded, ingest_ts_ns)
            .await?
            .acks(tenant)
            .await
    }

    /// Records its label into a shared log when dropped.
    struct DropProbe {
        label: &'static str,
        log: Arc<Mutex<Vec<&'static str>>>,
    }

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.log
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(self.label);
        }
    }

    /// The batch drops inside the submit, and the permit only after the
    /// submit resolves: the in-flight ceiling never stops counting a batch
    /// whose memory is still live.
    #[tokio::test]
    async fn submit_and_release_drops_the_batch_before_the_permit() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let probe = |label| DropProbe {
            label,
            log: log.clone(),
        };
        let submit_log = log.clone();
        let released = submit_and_release(probe("batch"), probe("permit"), |batch| async move {
            submit_log
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push("submitted");
            drop(batch);
            "receipt"
        })
        .await;
        assert_eq!(released, "receipt");
        let order = log
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        assert_eq!(order, ["submitted", "batch", "permit"]);
    }

    fn ingest_state() -> IngestState {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let router = Arc::new(IngestRouter::new(
            IngestConfig::default(),
            store,
            Signal::Metrics,
            Arc::new(SystemClock),
            Arc::new(PendingAckBudget::new(0)),
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
            metadata_sink: None,
            normalize_metrics: Arc::new(
                crate::normalize_reject_metrics::NormalizeRejectMetrics::new(),
            ),
            budget: ravel_ingest::IngestByteBudget::shared(
                ravel_ingest::IngestByteBudgetLimit::Unlimited,
            ),
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

    /// Fixed post-floor fixture base, 2026-01-01T00:00:00Z in nanoseconds,
    /// the same anchor the OTLP surface's tests use. Never `SystemTime::now()`.
    const BASE_TS_NS: i64 = 1_767_225_600_000_000_000;

    /// The same delta-temporality rejection the OTLP surface counts must be
    /// counted identically here: the reason label is a property of the
    /// rejection, not of the transport that carried the batch. OTAP has no
    /// partial-success field, so this counter is the only operator-visible
    /// trace of the drop.
    #[tokio::test]
    async fn delta_sum_batch_counts_every_point_as_structural() {
        use ravel_otap::encode::{DataPointRow, MetricKind, MetricRow, MetricsStreamEncoder};
        use ravel_otap::normalize::AGGREGATION_TEMPORALITY_DELTA;
        use ravel_otap::stream::{StreamConfig, StreamState};

        const POINT_COUNT: usize = 3;

        let ingest = ingest_state();
        let tenant = TenantId::new("acme");
        let mut encoder = MetricsStreamEncoder::new("a9").expect("encoder");
        let batch = encoder
            .encode_batch(
                0,
                &[MetricRow {
                    name: "requests".to_string(),
                    kind: MetricKind::Sum {
                        temporality: AGGREGATION_TEMPORALITY_DELTA,
                        is_monotonic: true,
                    },
                    data_points: (0..POINT_COUNT)
                        .map(|i| DataPointRow {
                            time_unix_nano: BASE_TS_NS,
                            value: i as f64,
                            flags: 0,
                            exemplars: vec![],
                            attrs: vec![],
                        })
                        .collect(),
                }],
            )
            .expect("encode a delta Sum batch");
        let mut state = StreamState::new(StreamConfig::default());
        let decoded = state.decode(batch).expect("decode the batch back");

        let before = normalize_totals(&ingest, &tenant);
        let outcome =
            write_batch(&ingest, &tenant, WriteMode::Buffered, &decoded, BASE_TS_NS).await;
        assert!(
            outcome.is_ok(),
            "a fully rejected batch is still an OK write with no points"
        );
        let after = normalize_totals(&ingest, &tenant);

        assert_eq!(
            (after.0 - before.0, after.1 - before.1),
            (0, POINT_COUNT as u64),
            "every rejected point counts once under structural, none under skew"
        );
    }

    /// `(skew, structural)` for the tenant's Metrics rows of the
    /// normalize-reject counters.
    fn normalize_totals(ingest: &IngestState, tenant: &TenantId) -> (u64, u64) {
        ingest
            .normalize_metrics
            .snapshot()
            .into_iter()
            .filter(|row| row.tenant_hash == tenant.hash() && row.signal == Signal::Metrics)
            .fold((0, 0), |acc, row| {
                (acc.0 + row.skew_total, acc.1 + row.structural_total)
            })
    }

    /// A state whose router charges `router_budget` and whose label projection
    /// charges `label_budget`, which may be the same `Arc`.
    fn state_with_budgets(
        router_budget: Arc<IngestByteBudget>,
        label_budget: Arc<IngestByteBudget>,
    ) -> IngestState {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let router = Arc::new(
            IngestRouter::new(
                IngestConfig::default(),
                store,
                Signal::Metrics,
                Arc::new(SystemClock),
                Arc::new(PendingAckBudget::new(0)),
            )
            .with_budget(router_budget),
        );
        IngestState {
            budget: label_budget,
            router,
            ..ingest_state()
        }
    }

    /// Four gauge points at `ts_ns`, each with its own attribute value, so the
    /// projection is several hundred bytes.
    fn labelled_gauge_batch(ts_ns: i64) -> DecodedBatch {
        use ravel_otap::encode::{
            AttrRow, AttrValue, DataPointRow, MetricKind, MetricRow, MetricsStreamEncoder,
        };
        use ravel_otap::stream::{StreamConfig, StreamState};

        let mut encoder = MetricsStreamEncoder::new("a9").expect("encoder");
        let batch = encoder
            .encode_batch(
                0,
                &[MetricRow {
                    name: "temperature".to_string(),
                    kind: MetricKind::Gauge,
                    data_points: (0..4)
                        .map(|i| DataPointRow {
                            time_unix_nano: ts_ns,
                            value: i as f64,
                            flags: 0,
                            exemplars: vec![],
                            attrs: vec![AttrRow {
                                key: "room".to_string(),
                                value: AttrValue::Str(format!("room-{i}")),
                            }],
                        })
                        .collect(),
                }],
            )
            .expect("encode a labelled gauge batch");
        StreamState::new(StreamConfig::default())
            .decode(batch)
            .expect("decode the batch back")
    }

    fn unlimited() -> Arc<IngestByteBudget> {
        IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited)
    }

    /// ADR-2708 D2 on the OTAP path: a budget one byte short of the projected
    /// label bytes sheds the batch RESOURCE_EXHAUSTED before normalization, so
    /// the stale points it carries move no counter and the router is never
    /// charged.
    #[tokio::test]
    async fn a_budget_short_of_the_label_projection_sheds_the_batch() {
        let too_old = BASE_TS_NS - ravel_otlp::IngestLimits::default().max_ingest_lag_ns - 1;
        let decoded = labelled_gauge_batch(too_old);
        let projected = prepare_batch(&decoded, &ravel_otlp::IngestLimits::default())
            .projected_label_bytes()
            .get();
        assert!(projected > 0, "the fixture projects some label bytes");
        let tenant = TenantId::new("acme");

        let label_budget =
            IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(projected as u64 - 1));
        let router_budget = unlimited();
        let ingest = state_with_budgets(router_budget.clone(), label_budget.clone());
        let written =
            write_batch(&ingest, &tenant, WriteMode::Buffered, &decoded, BASE_TS_NS).await;
        assert!(
            matches!(
                written,
                Err(IngestRequestError::Write(WriteError::BufferBudgetExceeded))
            ),
            "expected the buffer-budget shed"
        );
        let (status, teardown) = write_reply(9, written);
        assert_eq!(
            (status.batch_id, status.status_code, teardown),
            (9, StatusCode::ResourceExhausted as i32, false)
        );
        assert_eq!(label_budget.shed_total(), 1);
        assert_eq!(label_budget.in_flight_bytes(), 0);
        assert_eq!(router_budget.peak_bytes(), 0, "nothing reached the router");
        assert_eq!(
            normalize_totals(&ingest, &tenant),
            (0, 0),
            "normalization must not run for a shed batch"
        );

        // The same batch under a budget that fits reaches normalization.
        let ingest = state_with_budgets(
            unlimited(),
            IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(projected as u64)),
        );
        let written =
            write_batch(&ingest, &tenant, WriteMode::Buffered, &decoded, BASE_TS_NS).await;
        assert!(written.is_ok(), "a projection that fits is admitted");
        assert_eq!(normalize_totals(&ingest, &tenant), (4, 0));
    }

    /// The projection charge is released before the router charges the
    /// normalized batch, so on one shared budget the peak is the larger of the
    /// two charges, never their sum.
    #[tokio::test]
    async fn label_charge_is_released_before_the_router_charge() {
        let decoded = labelled_gauge_batch(BASE_TS_NS);
        let tenant = TenantId::new("acme");

        let label_only = unlimited();
        let router_only = unlimited();
        let ingest = state_with_budgets(router_only.clone(), label_only.clone());
        let written =
            write_batch(&ingest, &tenant, WriteMode::Buffered, &decoded, BASE_TS_NS).await;
        assert!(written.is_ok(), "the gauge write succeeds");
        let label_bytes = label_only.peak_bytes();
        let router_bytes = router_only.peak_bytes();
        assert_eq!(
            label_bytes as usize,
            prepare_batch(&decoded, &ravel_otlp::IngestLimits::default())
                .projected_label_bytes()
                .get(),
            "the label charge is exactly the projection"
        );
        assert!(label_bytes > 0, "the projection charged the labels");
        assert!(router_bytes > 0, "the router charged the batch");
        assert_eq!(
            label_only.in_flight_bytes(),
            0,
            "the label charge is refunded"
        );

        let shared = unlimited();
        let ingest = state_with_budgets(shared.clone(), shared.clone());
        let written =
            write_batch(&ingest, &tenant, WriteMode::Buffered, &decoded, BASE_TS_NS).await;
        assert!(written.is_ok(), "the gauge write succeeds");
        assert_eq!(
            shared.peak_bytes(),
            label_bytes.max(router_bytes),
            "the two charges must not coexist (label {label_bytes}, router {router_bytes})"
        );
    }

    /// A payload decompression the write gate returned no result for nacks
    /// INTERNAL when the job panicked and UNAVAILABLE when it was cancelled,
    /// and in both cases ends the stream, as a corrupted decoder does: the
    /// batch's earlier payloads already advanced the decoders, so a resend
    /// must reach a fresh stream. A malformed batch still keeps it open.
    #[test]
    fn a_gate_failure_nacks_and_ends_the_stream() {
        use ravel_cpu_gate::CpuGateError;
        use ravel_otap::stream::BatchError;

        let reply = |err| {
            let (status, teardown) = decode_error_reply(7, err);
            assert_eq!(status.batch_id, 7);
            (status.status_code, teardown)
        };
        assert_eq!(
            reply(StreamError::Gate(CpuGateError::Panicked).into()),
            (StatusCode::Internal as i32, true)
        );
        assert_eq!(
            reply(StreamError::Gate(CpuGateError::Cancelled).into()),
            (StatusCode::Unavailable as i32, true)
        );
        assert_eq!(
            reply(BatchError::MalformedIpc("x".into()).into()),
            (StatusCode::InvalidArgument as i32, false)
        );
    }
}
