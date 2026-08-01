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

use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use ravel_ingest::{IngestPoint, WriteError, WriteMode};
use ravel_otap::normalize::normalize_decoded;
use ravel_otap::proto::experimental::arrow::v1::arrow_metrics_service_server::ArrowMetricsService;
use ravel_otap::proto::experimental::arrow::v1::{BatchArrowRecords, BatchStatus, StatusCode};
use ravel_otap::stream::{DecodeError, DecodedBatch, StreamConfig, StreamState};
use ravel_types::{CommitToken, TenantId};
use tonic::{Request, Response, Status, Streaming};

use crate::ingest::IngestState;
use crate::otlp_grpc::metadata_to_headers;
use crate::otlp_http::{GatewayState, now_ns, write_mode_from_headers};

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

        let ctx = StreamCtx {
            inbound: request.into_inner(),
            decoder: StreamState::new(StreamConfig::default()),
            state: self.state.clone(),
            tenant,
            mode,
            finished: false,
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
    match ctx.decoder.decode(batch) {
        Ok(decoded) => {
            match write_batch(&ctx.state.ingest, &ctx.tenant, ctx.mode, &decoded).await {
                Ok(tokens) => (ack(batch_id, &tokens), false),
                Err(err) => {
                    // Same retry classification the OTLP gRPC path applies: a
                    // retryable write is RESOURCE_EXHAUSTED/UNAVAILABLE-shaped
                    // backpressure, anything else is an internal fault.
                    let code = if err.is_retryable() {
                        StatusCode::Unavailable
                    } else {
                        StatusCode::Internal
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

/// Normalize a decoded batch and write its points through the shared ingest
/// router, returning the commit tokens a strict write produces. Mirrors
/// [`crate::ingest::handle_export`]'s point assembly (scalar and native-
/// histogram points feed one write so a batch shares a single receipt); OTAP
/// carries only scalar metric points, so `histogram_points` here is the
/// exploded-histogram output, not native histograms.
async fn write_batch(
    ingest: &IngestState,
    tenant: &TenantId,
    mode: WriteMode,
    decoded: &DecodedBatch,
) -> Result<Vec<CommitToken>, WriteError> {
    let normalized = normalize_decoded(tenant, decoded, &ingest.limits, now_ns());
    let mut points: Vec<IngestPoint> =
        Vec::with_capacity(normalized.points.len() + normalized.histogram_points.len());
    points.extend(normalized.points.into_iter().map(IngestPoint::from));
    points.extend(
        normalized
            .histogram_points
            .into_iter()
            .map(IngestPoint::from),
    );

    let receipt = ingest
        .router
        .write_values(tenant.clone(), points, mode, ingest.ack_deadline)
        .await?;
    Ok(receipt.tokens)
}

/// An OK `BatchStatus` whose `status_message` carries the batch's commit
/// tokens, comma-joined, the way the OTLP path returns them in the
/// `x-ravel-commit-token` header. Empty when the write produced no token
/// (a buffered write, or a batch that normalized to zero admitted points).
fn ack(batch_id: i64, tokens: &[CommitToken]) -> BatchStatus {
    let status_message = tokens
        .iter()
        .map(|token| token.encode())
        .collect::<Vec<_>>()
        .join(",");
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
