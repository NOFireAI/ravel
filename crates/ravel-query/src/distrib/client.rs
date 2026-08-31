//! The coordinator's view of a slice worker: the [`SliceFetcher`] seam and its
//! `tonic`-backed [`RemoteSliceFetcher`] (ADR-0071).
//!
//! [`SliceFetcher`] is the one seam the merge layer holds. A [`RemoteSliceFetcher`]
//! drives a real gRPC worker; a test double can implement the same trait to
//! return crafted frames. Either way the coordinator receives the identical
//! [`SliceResponse`] shape, so the merge cannot tell a remote slice from a
//! local one.

use ravel_logseg::LogRecord;
use ravel_proto::queryfrag::v1 as pb;
use ravel_types::accounting::QueryAccountingSnapshot;
use tonic::transport::Channel;

use crate::distrib::codec::{self, CodecError};
use crate::distrib::proto::series_fetch_client::SeriesFetchClient;
use crate::fetcher::{FetchStats, FetchedHistogramSeries, FetchedSeriesSoa};
use crate::phase_accounting::PhaseAccountingSnapshot;
use crate::span_fetcher::SpanRow;

/// A distributed fetch failed in a way that is not a per-slice typed status.
/// Distinct from a [`pb::Status`] a worker returns in a summary (which the
/// coordinator maps to a query outcome directly): this is transport or framing
/// breakage.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DistribError {
    /// The gRPC call itself failed (connection, deadline, worker crash).
    #[error("slice transport failed: {0}")]
    Transport(String),
    /// A frame could not be decoded.
    #[error(transparent)]
    Codec(#[from] CodecError),
    /// The stream ended without the mandatory terminal summary frame.
    #[error("slice stream ended without a summary frame")]
    NoSummary,
    /// More than one summary frame arrived (a slice must send exactly one).
    #[error("slice stream carried more than one summary frame")]
    MultipleSummaries,
    /// A frame carried no `frame` oneof variant.
    #[error("slice stream carried an empty frame")]
    EmptyFrame,
    /// The remote streamed a frame kind this decoder's call site cannot
    /// consume: a per-signal record frame (a log record or a span) for a signal
    /// this coordinator's metrics fetch path does not handle, or a
    /// `PartialAggregate` (ADR-0103 decision 2) on a log or span slice, where a
    /// worker-computed scalar aggregate is never expected (the metrics decoder
    /// does consume it).
    /// Unreachable from a real query today: the metrics coordinator only ever
    /// dispatches `Signal::Metrics`, and the log/span coordinators that would
    /// receive these frames are built by #284/#285. The `frame` oneof is
    /// exhaustive, so every decoder must still name the variants: this is a
    /// well-formed frame this build does not consume, not corruption.
    #[error(
        "remote returned a {0} frame this metrics coordinator cannot decode across the slice boundary"
    )]
    FrameSignalUnsupported(&'static str),
}

/// One slice's fully-decoded response, in the same in-memory shapes the local
/// fetch produces. `status` is the worker's terminal typed status; the
/// coordinator inspects it before trusting `scalar` (a non-OK slice
/// contributes nothing to the merge).
#[derive(Debug)]
pub struct SliceResponse {
    /// Decoded scalar series, one [`FetchedSeriesSoa`] per run. Post-erasure
    /// (the worker applied the request's predicates). Only meaningful when
    /// `status` is `Ok`.
    pub scalar: Vec<FetchedSeriesSoa>,
    /// Decoded native-histogram series, one [`FetchedHistogramSeries`] per run
    /// (ADR-0096 decision 3 step 4). Post-erasure (the worker applied the
    /// request's predicates). Only meaningful when `status` is `Ok`.
    pub histogram: Vec<FetchedHistogramSeries>,
    /// Decoded worker-computed partial aggregates, one per series the worker
    /// held (ADR-0103 decision 2). Non-empty only for a slice whose request
    /// carried a `partial_aggregate`, and mutually exclusive with `scalar` on
    /// such a slice: a worker returns all partials or all raw frames, never a
    /// mix. Nothing combines these into a query result yet -- the
    /// coordinator-side combine and the planner integration are the next task,
    /// so today's callers of [`decode_slice_frames`] ignore this field.
    pub partials: Vec<codec::PartialAggregate>,
    /// The worker's per-slice cost accounting, pooled across every phase.
    pub accounting: ravel_types::accounting::QueryAccountingSnapshot,
    /// The same cost [`accounting`](Self::accounting) pools, split by the phase
    /// that issued it on the worker (issue #959). `None` means the worker
    /// reported no split (a build predating the summary's `phase_accounting`
    /// field), in which case a coordinator that wants a split charges the pooled
    /// total to its own scan phase -- exactly what every coordinator did before
    /// this field existed (`distrib::fold_phases`). A split that arrived but
    /// cannot be attributed (an unknown or repeated phase) never reaches this
    /// field as `None`: it is a typed [`DistribError::Codec`] at decode time.
    ///
    /// Every `s3_bytes` figure in it is WIRE bytes as transferred, on the same
    /// basis as `accounting`: coalesced-range slack and retries included,
    /// cache-served bytes excluded (those stay on `cache_bytes`). It sums back to
    /// `accounting` field-wise, so the two are two views of one number, never
    /// two costs to add up.
    pub phase_accounting: Option<PhaseAccountingSnapshot>,
    /// The worker's per-slice `FetchStats` page counters, folded (summed) by
    /// the coordinator so a distributed query reports the same raw-page cost in
    /// its stats JSON the local path would (ADR-0071).
    pub stats: FetchStats,
    /// Series the worker reported returning (for coordinator budget re-checks).
    pub series_returned: u64,
    /// Samples the worker reported returning.
    pub samples_returned: u64,
    /// The worker's terminal typed status.
    pub status: pb::status::Code,
    /// The status' human-readable detail (empty for `Ok`).
    pub status_message: String,
}

/// One RLOG-family slice's fully-decoded response (Logs, Alerts, Audit). The
/// log sibling of [`SliceResponse`]: `records` carries the worker's decoded
/// per-segment merged view, post-erasure, in the worker's local scan order (the
/// coordinator re-orders them under the stated total order and does not dedup,
/// so this arrival order is not load-bearing).
/// Only meaningful when `status` is `Ok`.
#[derive(Debug)]
pub struct SliceLogResponse {
    /// Decoded RLOG records, each carrying its full per-segment merged view.
    pub records: Vec<LogRecord>,
    /// The worker's per-slice cost accounting, pooled across every phase.
    pub accounting: QueryAccountingSnapshot,
    /// Per-phase split of [`accounting`](Self::accounting); see
    /// [`SliceResponse::phase_accounting`] for the exact meaning and the `None`
    /// degradation.
    pub phase_accounting: Option<PhaseAccountingSnapshot>,
    /// The worker's per-slice `FetchStats` page counters.
    pub stats: FetchStats,
    /// Records the worker reported returning (carried in the summary's
    /// `series_returned` field, reused for the record count on this signal).
    pub records_returned: u64,
    /// The worker's terminal typed status.
    pub status: pb::status::Code,
    /// The status' human-readable detail (empty for `Ok`).
    pub status_message: String,
}

impl SliceLogResponse {
    /// A synthetic `Unsupported` response carrying no records, for a
    /// [`SliceFetcher`] that has not wired the log path. The coordinator maps
    /// `Unsupported` to whole-query local fallback (ADR-0071 failure
    /// semantics), never a wrong or partial result.
    pub fn unsupported(message: impl Into<String>) -> Self {
        SliceLogResponse {
            records: Vec::new(),
            accounting: QueryAccountingSnapshot::default(),
            // A synthetic response, not a worker's: no request was issued at
            // all, so there is no phase to attribute and no split to report.
            phase_accounting: None,
            stats: FetchStats::default(),
            records_returned: 0,
            status: pb::status::Code::Unsupported,
            status_message: message.into(),
        }
    }
}

/// One Spans slice's fully-decoded response (#285). The span sibling of
/// [`SliceLogResponse`]: `spans` carries the worker's decoded per-segment merged
/// view (each [`SpanRow`] is a rebuilt `SpanRecord` plus its lifted
/// `service_name`), post-erasure, in the worker's local scan order. The
/// coordinator re-orders them under the stated span total order and does NOT
/// dedup, so this arrival order is not load-bearing. Only meaningful when
/// `status` is `Ok`.
#[derive(Debug)]
pub struct SliceSpanResponse {
    /// Decoded spans, each carrying its full per-segment merged view.
    pub spans: Vec<SpanRow>,
    /// The worker's per-slice cost accounting, pooled across every phase.
    pub accounting: QueryAccountingSnapshot,
    /// Per-phase split of [`accounting`](Self::accounting); see
    /// [`SliceResponse::phase_accounting`] for the exact meaning and the `None`
    /// degradation.
    pub phase_accounting: Option<PhaseAccountingSnapshot>,
    /// The worker's per-slice `FetchStats` page counters.
    pub stats: FetchStats,
    /// Spans the worker reported returning (carried in the summary's
    /// `series_returned` field, reused for the span count on this signal).
    pub spans_returned: u64,
    /// The worker's terminal typed status.
    pub status: pb::status::Code,
    /// The status' human-readable detail (empty for `Ok`).
    pub status_message: String,
}

impl SliceSpanResponse {
    /// A synthetic `Unsupported` response carrying no spans, for a
    /// [`SliceFetcher`] that has not wired the span path. The coordinator maps
    /// `Unsupported` to whole-query local fallback (ADR-0071 failure
    /// semantics), never a wrong or partial result.
    pub fn unsupported(message: impl Into<String>) -> Self {
        SliceSpanResponse {
            spans: Vec::new(),
            accounting: QueryAccountingSnapshot::default(),
            // A synthetic response, not a worker's: no request was issued at
            // all, so there is no phase to attribute and no split to report.
            phase_accounting: None,
            stats: FetchStats::default(),
            spans_returned: 0,
            status: pb::status::Code::Unsupported,
            status_message: message.into(),
        }
    }
}

/// The seam between the coordinator merge and a slice worker. Object-safe (via
/// `async_trait`) so the engine holds one `dyn SliceFetcher`.
#[async_trait::async_trait]
pub trait SliceFetcher: Send + Sync {
    /// Dispatches one slice request and collects its full response.
    async fn fetch(&self, request: pb::FetchRequest) -> Result<SliceResponse, DistribError>;

    /// Dispatches one RLOG-family (Logs/Alerts/Audit) slice request and collects
    /// its decoded records (#284).
    ///
    /// The default implementation reports [`pb::status::Code::Unsupported`], so
    /// a `SliceFetcher` that has not wired the log fetch path degrades to
    /// whole-query local execution rather than erroring. A real transport
    /// (`RemoteSliceFetcher`) overrides it to drain the worker's stream and
    /// decode its [`pb::LogRecordFrame`]s. This mirrors the base ADR's silent
    /// version-skew fallback: an unimplemented log fetch is a coverage gap the
    /// coordinator fills locally, never a hard failure.
    async fn fetch_logs(
        &self,
        _request: pb::FetchRequest,
    ) -> Result<SliceLogResponse, DistribError> {
        Ok(SliceLogResponse::unsupported(
            "distributed log fetch is not implemented by this slice fetcher",
        ))
    }

    /// Dispatches one Spans slice request and collects its decoded spans (#285).
    ///
    /// The default implementation reports [`pb::status::Code::Unsupported`], so
    /// a `SliceFetcher` that has not wired the span fetch path degrades to
    /// whole-query local execution rather than erroring. A real transport
    /// (`RemoteSliceFetcher`) overrides it to drain the worker's stream and
    /// decode its [`pb::SpanFrame`]s. Mirrors [`fetch_logs`](Self::fetch_logs)
    /// exactly.
    async fn fetch_spans(
        &self,
        _request: pb::FetchRequest,
    ) -> Result<SliceSpanResponse, DistribError> {
        Ok(SliceSpanResponse::unsupported(
            "distributed span fetch is not implemented by this slice fetcher",
        ))
    }
}

/// A [`SliceFetcher`] backed by a real gRPC worker over a `tonic` channel. The
/// channel is cheap to clone, so one fetcher serves many concurrent slices.
pub struct RemoteSliceFetcher {
    channel: Channel,
}

impl RemoteSliceFetcher {
    pub fn new(channel: Channel) -> Self {
        RemoteSliceFetcher { channel }
    }

    /// Dispatches one slice request and drains the worker's response stream into
    /// a flat `Vec` of frames. The signal-agnostic transport half shared by
    /// [`fetch`](SliceFetcher::fetch),
    /// [`fetch_logs`](SliceFetcher::fetch_logs), and
    /// [`fetch_spans`](SliceFetcher::fetch_spans): each of those calls this, then
    /// applies its own per-signal decode to the frames. Any transport failure (the
    /// call itself or a mid-stream `message()`) is mapped to
    /// [`DistribError::Transport`]; framing/decode is the caller's concern.
    async fn collect_frames(
        &self,
        request: pb::FetchRequest,
    ) -> Result<Vec<pb::FetchResponse>, DistribError> {
        let mut client = SeriesFetchClient::new(self.channel.clone());
        let response = client
            .fetch(request)
            .await
            .map_err(|s| DistribError::Transport(s.to_string()))?;
        let mut stream = response.into_inner();
        let mut frames = Vec::new();
        while let Some(frame) = stream
            .message()
            .await
            .map_err(|s| DistribError::Transport(s.to_string()))?
        {
            frames.push(frame);
        }
        Ok(frames)
    }
}

#[async_trait::async_trait]
impl SliceFetcher for RemoteSliceFetcher {
    async fn fetch(&self, request: pb::FetchRequest) -> Result<SliceResponse, DistribError> {
        let frames = self.collect_frames(request).await?;
        decode_slice_frames(frames)
    }

    async fn fetch_logs(
        &self,
        request: pb::FetchRequest,
    ) -> Result<SliceLogResponse, DistribError> {
        let frames = self.collect_frames(request).await?;
        decode_log_slice_frames(frames)
    }

    async fn fetch_spans(
        &self,
        request: pb::FetchRequest,
    ) -> Result<SliceSpanResponse, DistribError> {
        let frames = self.collect_frames(request).await?;
        decode_span_slice_frames(frames)
    }
}

/// Decode a slice's full frame sequence into a [`SliceResponse`].
///
/// This is the single decode implementation for a slice's frames.
/// [`RemoteSliceFetcher`] drains its gRPC stream into a `Vec` and
/// calls this; the server-side coordinator paths in `services/ravel-server`
/// (the local no-hop fetch, an intra-cluster remote dispatch, and cross-cluster
/// federation) collect their frames and call the same function, so the two
/// sites cannot drift.
///
/// Every malformation is a typed [`DistribError`], never a panic: a series
/// frame that fails to decode ([`DistribError::Codec`]), a native-histogram
/// frame that fails to decode ([`DistribError::Codec`], the same as a malformed
/// scalar frame -- as of `PROTOCOL_VERSION` 3 a `Hist` frame is real data this
/// build consumes, so a decode failure is corruption, never a coverage gap), a
/// malformed `PartialAggregate` frame ([`DistribError::Codec`], for the same
/// reason: a worker only sends one when the coordinator asked for it, so a frame
/// that fails to decode is corruption), a
/// frame carrying no oneof variant ([`DistribError::EmptyFrame`]), a second
/// summary ([`DistribError::MultipleSummaries`]), a stream that ended with no
/// summary ([`DistribError::NoSummary`]), a summary with no status, or an
/// unknown status code.
///
/// `PartialAggregate` frames (ADR-0103 decision 2) decode into
/// [`SliceResponse::partials`]. This is the decode side only: no caller acts on
/// them yet, so a slice that returns partials contributes nothing to a query
/// result today. The coordinator-side combine and the planner integration that
/// make a pushdown-computed answer reachable from a real query are the next task.
pub fn decode_slice_frames(frames: Vec<pb::FetchResponse>) -> Result<SliceResponse, DistribError> {
    let mut scalar = Vec::new();
    let mut histogram = Vec::new();
    let mut partials = Vec::new();
    let mut summary: Option<pb::Summary> = None;
    for frame in frames {
        match frame.frame {
            Some(pb::fetch_response::Frame::Series(sf)) => {
                scalar.extend(codec::decode_series_frame(sf)?);
            }
            Some(pb::fetch_response::Frame::Hist(hf)) => {
                histogram.extend(codec::decode_histogram_frame(hf)?);
            }
            Some(pb::fetch_response::Frame::LogRecord(_)) => {
                return Err(DistribError::FrameSignalUnsupported("log-record"));
            }
            Some(pb::fetch_response::Frame::Span(_)) => {
                return Err(DistribError::FrameSignalUnsupported("span"));
            }
            Some(pb::fetch_response::Frame::PartialAggregate(pa)) => {
                partials.push(codec::decode_partial_aggregate(pa)?);
            }
            Some(pb::fetch_response::Frame::Summary(s)) => {
                if summary.is_some() {
                    return Err(DistribError::MultipleSummaries);
                }
                summary = Some(s);
            }
            None => return Err(DistribError::EmptyFrame),
        }
    }

    let summary = summary.ok_or(DistribError::NoSummary)?;
    let status = summary
        .status
        .ok_or(DistribError::Codec(CodecError::MissingStatus))?;
    let code = codec::decode_status_code(status.code)?;
    let accounting = summary
        .accounting
        .map(codec::decode_accounting)
        .unwrap_or_default();
    // Absent on a worker that predates the summary's per-phase field: `None`
    // reaches the coordinator's fold, which then charges the pooled total to
    // one phase exactly as it did before the field existed (issue #959). A
    // present-but-malformed split (an unknown or repeated phase) is a typed
    // codec error, never silently reinterpreted as absent: that would hide a
    // real disagreement about which phase paid what behind a plausible number.
    let phase_accounting = summary
        .phase_accounting
        .map(codec::decode_phase_accounting)
        .transpose()?;
    Ok(SliceResponse {
        scalar,
        histogram,
        partials,
        accounting,
        phase_accounting,
        stats: FetchStats {
            raw_f64_pages: summary.raw_f64_pages,
            raw_f64_bytes: summary.raw_f64_bytes,
        },
        series_returned: summary.series_returned,
        samples_returned: summary.samples_returned,
        status: code,
        status_message: status.message,
    })
}

/// Decode an RLOG-family slice's full frame sequence into a [`SliceLogResponse`]
/// (#284). The log sibling of [`decode_slice_frames`]: it accepts
/// [`pb::LogRecordFrame`]s and exactly one terminal [`pb::Summary`], and rejects
/// any metric/histogram/span frame as [`DistribError::FrameSignalUnsupported`]
/// (a log slice must not carry them). Every malformation is a typed error, never
/// a panic: a malformed record ([`DistribError::Codec`]), a mixed-signal frame,
/// an empty frame, a second summary, a missing summary, a summary with no
/// status, or an unknown status code.
pub fn decode_log_slice_frames(
    frames: Vec<pb::FetchResponse>,
) -> Result<SliceLogResponse, DistribError> {
    let mut records = Vec::new();
    let mut summary: Option<pb::Summary> = None;
    for frame in frames {
        match frame.frame {
            Some(pb::fetch_response::Frame::LogRecord(lr)) => {
                records.push(codec::decode_log_record(lr)?);
            }
            Some(pb::fetch_response::Frame::Series(_)) => {
                return Err(DistribError::FrameSignalUnsupported("series"));
            }
            Some(pb::fetch_response::Frame::Hist(_)) => {
                return Err(DistribError::FrameSignalUnsupported("histogram"));
            }
            Some(pb::fetch_response::Frame::Span(_)) => {
                return Err(DistribError::FrameSignalUnsupported("span"));
            }
            Some(pb::fetch_response::Frame::PartialAggregate(_)) => {
                return Err(DistribError::FrameSignalUnsupported("partial-aggregate"));
            }
            Some(pb::fetch_response::Frame::Summary(s)) => {
                if summary.is_some() {
                    return Err(DistribError::MultipleSummaries);
                }
                summary = Some(s);
            }
            None => return Err(DistribError::EmptyFrame),
        }
    }

    let summary = summary.ok_or(DistribError::NoSummary)?;
    let status = summary
        .status
        .ok_or(DistribError::Codec(CodecError::MissingStatus))?;
    let code = codec::decode_status_code(status.code)?;
    let accounting = summary
        .accounting
        .map(codec::decode_accounting)
        .unwrap_or_default();
    // Absent on a worker that predates the summary's per-phase field: `None`
    // reaches the coordinator's fold, which then charges the pooled total to
    // one phase exactly as it did before the field existed (issue #959). A
    // present-but-malformed split (an unknown or repeated phase) is a typed
    // codec error, never silently reinterpreted as absent: that would hide a
    // real disagreement about which phase paid what behind a plausible number.
    let phase_accounting = summary
        .phase_accounting
        .map(codec::decode_phase_accounting)
        .transpose()?;
    Ok(SliceLogResponse {
        records,
        accounting,
        phase_accounting,
        stats: FetchStats {
            raw_f64_pages: summary.raw_f64_pages,
            raw_f64_bytes: summary.raw_f64_bytes,
        },
        records_returned: summary.series_returned,
        status: code,
        status_message: status.message,
    })
}

/// Decode a Spans slice's full frame sequence into a [`SliceSpanResponse`]
/// (#285). The span sibling of [`decode_log_slice_frames`]: it accepts
/// [`pb::SpanFrame`]s and exactly one terminal [`pb::Summary`], and rejects any
/// metric/histogram/log frame as [`DistribError::FrameSignalUnsupported`] (a
/// span slice must not carry them). Every malformation is a typed error, never a
/// panic: a malformed span ([`DistribError::Codec`]), a mixed-signal frame, an
/// empty frame, a second summary, a missing summary, a summary with no status,
/// or an unknown status code.
pub fn decode_span_slice_frames(
    frames: Vec<pb::FetchResponse>,
) -> Result<SliceSpanResponse, DistribError> {
    let mut spans = Vec::new();
    let mut summary: Option<pb::Summary> = None;
    for frame in frames {
        match frame.frame {
            Some(pb::fetch_response::Frame::Span(sf)) => {
                spans.push(codec::decode_span_frame(sf)?);
            }
            Some(pb::fetch_response::Frame::Series(_)) => {
                return Err(DistribError::FrameSignalUnsupported("series"));
            }
            Some(pb::fetch_response::Frame::Hist(_)) => {
                return Err(DistribError::FrameSignalUnsupported("histogram"));
            }
            Some(pb::fetch_response::Frame::LogRecord(_)) => {
                return Err(DistribError::FrameSignalUnsupported("log-record"));
            }
            Some(pb::fetch_response::Frame::PartialAggregate(_)) => {
                return Err(DistribError::FrameSignalUnsupported("partial-aggregate"));
            }
            Some(pb::fetch_response::Frame::Summary(s)) => {
                if summary.is_some() {
                    return Err(DistribError::MultipleSummaries);
                }
                summary = Some(s);
            }
            None => return Err(DistribError::EmptyFrame),
        }
    }

    let summary = summary.ok_or(DistribError::NoSummary)?;
    let status = summary
        .status
        .ok_or(DistribError::Codec(CodecError::MissingStatus))?;
    let code = codec::decode_status_code(status.code)?;
    let accounting = summary
        .accounting
        .map(codec::decode_accounting)
        .unwrap_or_default();
    // Absent on a worker that predates the summary's per-phase field: `None`
    // reaches the coordinator's fold, which then charges the pooled total to
    // one phase exactly as it did before the field existed (issue #959). A
    // present-but-malformed split (an unknown or repeated phase) is a typed
    // codec error, never silently reinterpreted as absent: that would hide a
    // real disagreement about which phase paid what behind a plausible number.
    let phase_accounting = summary
        .phase_accounting
        .map(codec::decode_phase_accounting)
        .transpose()?;
    Ok(SliceSpanResponse {
        spans,
        accounting,
        phase_accounting,
        stats: FetchStats {
            raw_f64_pages: summary.raw_f64_pages,
            raw_f64_bytes: summary.raw_f64_bytes,
        },
        spans_returned: summary.series_returned,
        status: code,
        status_message: status.message,
    })
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use ravel_types::accounting::{AccountedOp, QueryAccountingSnapshot};
    use ravel_types::{Label, LabelSet, SeriesId};

    use super::*;
    use crate::distrib::codec::{self, CodecError};
    use crate::phase_accounting::{PhaseAccounting, QueryPhase};

    fn label_set() -> LabelSet {
        LabelSet::new(vec![Label {
            name: "__name__".to_string(),
            value: "m".to_string(),
        }])
        .expect("valid labels")
    }

    fn series_soa() -> FetchedSeriesSoa {
        FetchedSeriesSoa {
            series_id: SeriesId([1u8; 16]),
            labels: label_set(),
            timestamps: vec![10, 20, 30],
            values: vec![1.0, 2.0, 3.0],
            created_unix_ns: 7,
            writer_epoch: 1,
            writer_seq: 2,
            per_sample_priorities: None,
        }
    }

    fn series_frame(soa: &FetchedSeriesSoa) -> pb::FetchResponse {
        pb::FetchResponse {
            frame: Some(pb::fetch_response::Frame::Series(
                codec::encode_series_frame(soa),
            )),
        }
    }

    /// A well-formed terminal summary carrying `code`, real accounting, and the
    /// given counts.
    ///
    /// The accounting is what a current worker sends: the per-phase split (1
    /// probe GET of 29 bytes, 2 scan GETs of 70 bytes) plus the pooled total
    /// that split sums to (3 GETs, 99 bytes) in the legacy field. The two agree
    /// by construction, as they do on the worker (`service::summary_frame`
    /// derives the pooled field from the split).
    fn summary_frame(code: pb::status::Code) -> pb::FetchResponse {
        let phase = phase_split();
        pb::FetchResponse {
            frame: Some(pb::fetch_response::Frame::Summary(pb::Summary {
                accounting: Some(codec::encode_accounting(&phase.pooled())),
                series_returned: 1,
                samples_returned: 3,
                status: Some(pb::Status {
                    code: code as i32,
                    message: String::new(),
                }),
                raw_f64_pages: 5,
                raw_f64_bytes: 40,
                phase_accounting: Some(codec::encode_phase_accounting(&phase)),
            })),
        }
    }

    /// The per-phase split [`summary_frame`] carries: 1 probe GET of 29 bytes
    /// and 2 scan GETs of 70 bytes, pooling to the 3 GETs / 99 bytes every
    /// pre-existing assertion in this module reads off the legacy field.
    fn phase_split() -> PhaseAccountingSnapshot {
        let phases = PhaseAccounting::new();
        phases.probe().record_s3_request(AccountedOp::Get);
        phases.probe().add_s3_bytes(AccountedOp::Get, 29);
        phases.scan().record_s3_request(AccountedOp::Get);
        phases.scan().record_s3_request(AccountedOp::Get);
        phases.scan().add_s3_bytes(AccountedOp::Get, 70);
        phases.snapshot()
    }

    /// The happy path: a series frame plus one terminal summary decodes to a
    /// `SliceResponse` carrying the decoded series and folded summary fields.
    #[test]
    fn series_then_summary_decodes() {
        let soa = series_soa();
        let response = decode_slice_frames(vec![
            series_frame(&soa),
            summary_frame(pb::status::Code::Ok),
        ])
        .expect("valid frame sequence decodes");
        assert_eq!(response.scalar.len(), 1);
        assert_eq!(response.scalar[0].timestamps, soa.timestamps);
        assert_eq!(response.status, pb::status::Code::Ok);
        assert_eq!(response.series_returned, 1);
        assert_eq!(response.samples_returned, 3);
        assert_eq!(response.stats.raw_f64_pages, 5);
        assert_eq!(response.accounting.s3_requests(AccountedOp::Get), 3);
    }

    /// A malformed series frame (a 15-byte series id) is a typed `Codec` error,
    /// never a panic and never a truncated series.
    #[test]
    fn malformed_series_frame_is_typed_codec_error() {
        let bad = pb::FetchResponse {
            frame: Some(pb::fetch_response::Frame::Series(pb::SeriesFrame {
                series_id: vec![0u8; 15],
                labels: vec![pb::Label {
                    name: "__name__".to_string(),
                    value: "m".to_string(),
                }],
                runs: Vec::new(),
            })),
        };
        assert!(matches!(
            decode_slice_frames(vec![bad, summary_frame(pb::status::Code::Ok)]),
            Err(DistribError::Codec(CodecError::BadSeriesId { got: 15 }))
        ));
    }

    /// A stream that never sent its mandatory terminal summary is `NoSummary`.
    #[test]
    fn missing_summary_is_typed_error() {
        let soa = series_soa();
        assert!(matches!(
            decode_slice_frames(vec![series_frame(&soa)]),
            Err(DistribError::NoSummary)
        ));
    }

    /// Two summary frames violate the exactly-one-summary rule.
    #[test]
    fn duplicate_summary_is_typed_error() {
        assert!(matches!(
            decode_slice_frames(vec![
                summary_frame(pb::status::Code::Ok),
                summary_frame(pb::status::Code::Ok),
            ]),
            Err(DistribError::MultipleSummaries)
        ));
    }

    /// A frame carrying no `frame` oneof variant is `EmptyFrame`.
    #[test]
    fn empty_frame_is_typed_error() {
        assert!(matches!(
            decode_slice_frames(vec![pb::FetchResponse { frame: None }]),
            Err(DistribError::EmptyFrame)
        ));
    }

    /// A well-formed native-histogram frame plus a terminal summary decodes
    /// through the live `decode_slice_frames` path (ADR-0096 decision 3 step 4):
    /// `SliceResponse.histogram` carries the decoded series, proving the coordinator
    /// consumes `Hist` frames as real data rather than refusing them. This exercises
    /// the production decode arm, not just the unit-level codec function.
    #[test]
    fn histogram_frame_round_trips_through_decode_slice_frames() {
        use ravel_segment::{HistogramCounts, HistogramSpan, HistogramValue, ResetHint};

        let hs = FetchedHistogramSeries {
            series_id: SeriesId([2u8; 16]),
            labels: label_set(),
            timestamps: vec![10, 20],
            values: vec![
                HistogramValue {
                    scale: 0,
                    zero_threshold: 0.0,
                    sum: Some(1.5),
                    custom_values: None,
                    positive_spans: vec![HistogramSpan {
                        offset: 0,
                        length: 1,
                    }],
                    negative_spans: Vec::new(),
                    counts: HistogramCounts::Int {
                        zero_count: 0,
                        count: 1,
                        positive: vec![1],
                        negative: Vec::new(),
                    },
                    reset_hint: ResetHint::Unknown,
                },
                HistogramValue {
                    scale: 0,
                    zero_threshold: 0.0,
                    sum: Some(3.0),
                    custom_values: None,
                    positive_spans: vec![HistogramSpan {
                        offset: 0,
                        length: 1,
                    }],
                    negative_spans: Vec::new(),
                    counts: HistogramCounts::Int {
                        zero_count: 0,
                        count: 2,
                        positive: vec![2],
                        negative: Vec::new(),
                    },
                    reset_hint: ResetHint::Unknown,
                },
            ],
            created_unix_ns: 7,
            writer_epoch: 1,
            writer_seq: 2,
            per_sample_priorities: None,
        };
        let hist = pb::FetchResponse {
            frame: Some(pb::fetch_response::Frame::Hist(
                codec::encode_histogram_frame(&hs),
            )),
        };
        let response = decode_slice_frames(vec![hist, summary_frame(pb::status::Code::Ok)])
            .expect("a well-formed histogram frame decodes");
        assert!(response.scalar.is_empty());
        assert_eq!(response.histogram.len(), 1);
        let got = &response.histogram[0];
        assert_eq!(got.series_id, hs.series_id);
        assert_eq!(got.timestamps, hs.timestamps);
        // Bit-exact on the wire: the decoded records equal the source records.
        assert_eq!(
            codec::encode_histogram_records(&got.values),
            codec::encode_histogram_records(&hs.values)
        );
    }

    /// A summary that carries no status is a typed `MissingStatus` codec error.
    #[test]
    fn summary_without_status_is_typed_error() {
        let no_status = pb::FetchResponse {
            frame: Some(pb::fetch_response::Frame::Summary(pb::Summary {
                accounting: None,
                series_returned: 0,
                samples_returned: 0,
                status: None,
                raw_f64_pages: 0,
                raw_f64_bytes: 0,
                phase_accounting: None,
            })),
        };
        assert!(matches!(
            decode_slice_frames(vec![no_status]),
            Err(DistribError::Codec(CodecError::MissingStatus))
        ));
    }

    /// A summary naming a status code discriminant this build does not model is
    /// a typed error, never a silent success.
    #[test]
    fn unknown_status_code_is_typed_error() {
        let bad_code = pb::FetchResponse {
            frame: Some(pb::fetch_response::Frame::Summary(pb::Summary {
                accounting: None,
                series_returned: 0,
                samples_returned: 0,
                status: Some(pb::Status {
                    code: -1,
                    message: String::new(),
                }),
                raw_f64_pages: 0,
                raw_f64_bytes: 0,
                phase_accounting: None,
            })),
        };
        assert!(matches!(
            decode_slice_frames(vec![bad_code]),
            Err(DistribError::Codec(CodecError::UnknownStatusCode(-1)))
        ));
    }

    // --- decode_span_slice_frames (#307) -----------------------------------
    //
    // The span decoder mirrors `decode_slice_frames`' terminal paths (missing
    // summary, duplicate summary, empty frame, missing status, unknown status)
    // and adds its own: a series, histogram, or log-record frame in a span
    // stream is rejected as `FrameSignalUnsupported`, never silently skipped.

    /// A well-formed span frame (root span, `Unset` status, no attributes). Its
    /// contents never reach a reject arm, so only the shape matters.
    fn span_frame() -> pb::FetchResponse {
        pb::FetchResponse {
            frame: Some(pb::fetch_response::Frame::Span(pb::SpanFrame {
                trace_id: vec![0xAAu8; 16],
                span_id: vec![1u8; 8],
                parent_span_id: Vec::new(),
                name: "op".to_string(),
                start_ts_ns: 10,
                end_ts_ns: 11,
                status_code: 0,
                status_message: None,
                attrs: Vec::new(),
                service_name: None,
            })),
        }
    }

    /// The happy path: a span frame plus one terminal summary decodes to a
    /// `SliceSpanResponse` carrying the decoded span and the summary fields.
    #[test]
    fn span_then_summary_decodes() {
        let response =
            decode_span_slice_frames(vec![span_frame(), summary_frame(pb::status::Code::Ok)])
                .expect("valid span frame sequence decodes");
        assert_eq!(response.spans.len(), 1);
        assert_eq!(response.spans[0].record.trace_id, [0xAAu8; 16]);
        assert_eq!(response.status, pb::status::Code::Ok);
        // The summary's `series_returned` field is reused as the span count.
        assert_eq!(response.spans_returned, 1);
        assert_eq!(response.stats.raw_f64_pages, 5);
        assert_eq!(response.accounting.s3_requests(AccountedOp::Get), 3);
    }

    /// A span stream that never sent its mandatory terminal summary is
    /// `NoSummary`.
    #[test]
    fn span_missing_summary_is_typed_error() {
        assert!(matches!(
            decode_span_slice_frames(vec![span_frame()]),
            Err(DistribError::NoSummary)
        ));
    }

    /// Two summary frames violate the exactly-one-summary rule.
    #[test]
    fn span_duplicate_summary_is_typed_error() {
        assert!(matches!(
            decode_span_slice_frames(vec![
                summary_frame(pb::status::Code::Ok),
                summary_frame(pb::status::Code::Ok),
            ]),
            Err(DistribError::MultipleSummaries)
        ));
    }

    /// A frame carrying no `frame` oneof variant is `EmptyFrame`.
    #[test]
    fn span_empty_frame_is_typed_error() {
        assert!(matches!(
            decode_span_slice_frames(vec![pb::FetchResponse { frame: None }]),
            Err(DistribError::EmptyFrame)
        ));
    }

    /// A summary that carries no status is a typed `MissingStatus` codec error.
    #[test]
    fn span_summary_without_status_is_typed_error() {
        let no_status = pb::FetchResponse {
            frame: Some(pb::fetch_response::Frame::Summary(pb::Summary {
                accounting: None,
                series_returned: 0,
                samples_returned: 0,
                status: None,
                raw_f64_pages: 0,
                raw_f64_bytes: 0,
                phase_accounting: None,
            })),
        };
        assert!(matches!(
            decode_span_slice_frames(vec![no_status]),
            Err(DistribError::Codec(CodecError::MissingStatus))
        ));
    }

    /// A summary naming a status code discriminant this build does not model is a
    /// typed error, never a silent success.
    #[test]
    fn span_unknown_status_code_is_typed_error() {
        let bad_code = pb::FetchResponse {
            frame: Some(pb::fetch_response::Frame::Summary(pb::Summary {
                accounting: None,
                series_returned: 0,
                samples_returned: 0,
                status: Some(pb::Status {
                    code: -1,
                    message: String::new(),
                }),
                raw_f64_pages: 0,
                raw_f64_bytes: 0,
                phase_accounting: None,
            })),
        };
        assert!(matches!(
            decode_span_slice_frames(vec![bad_code]),
            Err(DistribError::Codec(CodecError::UnknownStatusCode(-1)))
        ));
    }

    /// A series frame in a span stream is rejected as `FrameSignalUnsupported`,
    /// never decoded as a span and never skipped. If the explicit
    /// `Frame::Series(_)` reject arm in `decode_span_slice_frames` were relaxed
    /// to a permissive wildcard (`_ => continue`), the frame would be dropped and
    /// the trailing summary would decode to an empty `Ok` response, so this
    /// `expect_err`-style match would fail.
    #[test]
    fn span_decoder_rejects_series_frame_as_unsupported() {
        let soa = series_soa();
        assert!(matches!(
            decode_span_slice_frames(vec![
                series_frame(&soa),
                summary_frame(pb::status::Code::Ok),
            ]),
            Err(DistribError::FrameSignalUnsupported("series"))
        ));
    }

    /// A native-histogram frame in a span stream is rejected as
    /// `FrameSignalUnsupported`. As with the series case, relaxing the explicit
    /// `Frame::Hist(_)` reject arm to a wildcard would drop the frame and decode
    /// to an empty `Ok`, failing this assertion.
    #[test]
    fn span_decoder_rejects_histogram_frame_as_unsupported() {
        let hist = pb::FetchResponse {
            frame: Some(pb::fetch_response::Frame::Hist(pb::HistogramFrame {
                series_id: vec![0u8; 16],
                labels: Vec::new(),
                runs: Vec::new(),
            })),
        };
        assert!(matches!(
            decode_span_slice_frames(vec![hist, summary_frame(pb::status::Code::Ok)]),
            Err(DistribError::FrameSignalUnsupported("histogram"))
        ));
    }

    /// A log-record frame in a span stream is rejected as
    /// `FrameSignalUnsupported`. Relaxing the explicit `Frame::LogRecord(_)`
    /// reject arm to a wildcard would drop the frame and decode to an empty `Ok`,
    /// failing this assertion.
    #[test]
    fn span_decoder_rejects_log_record_frame_as_unsupported() {
        let log = pb::FetchResponse {
            frame: Some(pb::fetch_response::Frame::LogRecord(
                pb::LogRecordFrame::default(),
            )),
        };
        assert!(matches!(
            decode_span_slice_frames(vec![log, summary_frame(pb::status::Code::Ok)]),
            Err(DistribError::FrameSignalUnsupported("log-record"))
        ));
    }

    /// A well-formed `PartialAggregate` frame (ADR-0103 decision 2). Its bounds
    /// are `-0.0` and a NaN payload so a decode that round-tripped them through
    /// a proto double instead of the bit pattern would be visible.
    fn partial_frame() -> pb::FetchResponse {
        pb::FetchResponse {
            frame: Some(pb::fetch_response::Frame::PartialAggregate(
                pb::PartialAggregate {
                    series_id: vec![4u8; 16],
                    labels: vec![pb::Label {
                        name: "__name__".to_string(),
                        value: "m".to_string(),
                    }],
                    count: Some(3),
                    min_bits: Some((-0.0f64).to_bits()),
                    max_bits: Some(0x7ff8_0000_0000_0abc),
                },
            )),
        }
    }

    /// The metrics decoder consumes a `PartialAggregate` frame as real data
    /// (ADR-0103 decision 2): it lands in `SliceResponse::partials`, bit-exact,
    /// and does not disturb the raw `scalar`/`histogram` collections. Restoring
    /// the former `FrameSignalUnsupported` reject arm at this one call site fails
    /// the `expect` below.
    #[test]
    fn metrics_decoder_collects_partial_aggregates() {
        let response =
            decode_slice_frames(vec![partial_frame(), summary_frame(pb::status::Code::Ok)])
                .expect("a well-formed partial aggregate decodes");
        assert!(response.scalar.is_empty());
        assert!(response.histogram.is_empty());
        assert_eq!(response.partials.len(), 1);
        let got = &response.partials[0];
        assert_eq!(got.series_id, SeriesId([4u8; 16]));
        assert_eq!(got.count, Some(3));
        // Bit patterns, never `==`: -0.0 and a NaN payload must survive exactly.
        assert_eq!(got.min.map(f64::to_bits), Some((-0.0f64).to_bits()));
        assert_eq!(got.max.map(f64::to_bits), Some(0x7ff8_0000_0000_0abc));
    }

    /// A malformed `PartialAggregate` (a 15-byte series id) on the metrics path
    /// is a typed `Codec` error, never a panic and never a silently dropped
    /// group.
    #[test]
    fn malformed_partial_aggregate_is_typed_codec_error() {
        let bad = pb::FetchResponse {
            frame: Some(pb::fetch_response::Frame::PartialAggregate(
                pb::PartialAggregate {
                    series_id: vec![0u8; 15],
                    labels: Vec::new(),
                    count: Some(1),
                    min_bits: None,
                    max_bits: None,
                },
            )),
        };
        assert!(matches!(
            decode_slice_frames(vec![bad, summary_frame(pb::status::Code::Ok)]),
            Err(DistribError::Codec(CodecError::BadSeriesId { got: 15 }))
        ));
    }

    /// The log and span decoders keep rejecting a `PartialAggregate` as
    /// `FrameSignalUnsupported`: a worker-computed scalar aggregate is never
    /// expected on those slices (ADR-0103 is metrics-only). Relaxing either
    /// explicit reject arm to a wildcard would drop the frame and decode to an
    /// empty `Ok`, failing these assertions.
    #[test]
    fn log_and_span_decoders_reject_partial_aggregate_as_unsupported() {
        assert!(matches!(
            decode_log_slice_frames(vec![partial_frame(), summary_frame(pb::status::Code::Ok)]),
            Err(DistribError::FrameSignalUnsupported("partial-aggregate"))
        ));
        assert!(matches!(
            decode_span_slice_frames(vec![partial_frame(), summary_frame(pb::status::Code::Ok)]),
            Err(DistribError::FrameSignalUnsupported("partial-aggregate"))
        ));
    }

    // --- version skew across the per-phase field (issue #959) ---------------
    //
    // The per-phase split is an additive protobuf field (`Summary.phase_accounting`,
    // number 7) and carries NO `PROTOCOL_VERSION` bump, so both skew directions
    // must interoperate rather than fall back. The two tests below pin each
    // direction at the wire level, encoding with prost and decoding with the
    // other side's message shape.

    /// The `Summary` shape a build predating field 7 compiles: fields 1..6 only,
    /// at the same numbers. Decoding new bytes with this proves an old
    /// coordinator ignores the additive field (proto3 unknown-field rule) and
    /// still reads the pooled total; encoding with it produces exactly the bytes
    /// an old worker sends.
    #[derive(Clone, PartialEq, prost::Message)]
    struct LegacySummary {
        #[prost(message, optional, tag = "1")]
        accounting: Option<pb::QueryAccountingSnapshot>,
        #[prost(uint64, tag = "2")]
        series_returned: u64,
        #[prost(uint64, tag = "3")]
        samples_returned: u64,
        #[prost(message, optional, tag = "4")]
        status: Option<pb::Status>,
        #[prost(uint64, tag = "5")]
        raw_f64_pages: u64,
        #[prost(uint64, tag = "6")]
        raw_f64_bytes: u64,
    }

    /// NEW worker -> OLD coordinator. A current worker's summary, decoded by a
    /// build that has no `phase_accounting` field, decodes cleanly and reports
    /// the pooled total: 3 GETs and 99 bytes, the field-wise sum of the split
    /// the worker also sent. Exactly today's behavior, never a decode failure
    /// and never a lost slice cost.
    ///
    /// This is what makes the additive field safe without a PROTOCOL_VERSION
    /// bump. It fails if the worker ever stops writing field 1 (or writes
    /// something other than the split's `pooled()` into it): the old
    /// coordinator's only cost figure would then be zero or wrong.
    #[test]
    fn new_worker_summary_read_by_an_old_coordinator_reports_the_pooled_total() {
        use prost::Message;

        let Some(pb::fetch_response::Frame::Summary(new_summary)) =
            summary_frame(pb::status::Code::Ok).frame
        else {
            panic!("summary_frame builds a summary frame");
        };
        assert!(
            new_summary.phase_accounting.is_some(),
            "the fixture must be a NEW worker's summary, or this proves nothing"
        );

        let bytes = new_summary.encode_to_vec();
        let old = LegacySummary::decode(bytes.as_slice())
            .expect("an old coordinator decodes a new worker's summary");

        let accounting = codec::decode_accounting(old.accounting.expect("pooled accounting"));
        assert_eq!(accounting.s3_requests(AccountedOp::Get), 3);
        assert_eq!(accounting.s3_bytes(AccountedOp::Get), 99);
        // The pooled field is the split's own sum, so the old coordinator's
        // total is the new coordinator's total.
        let pooled = phase_split().pooled();
        assert_eq!(accounting, pooled);
        // Every other field an old coordinator reads is untouched.
        assert_eq!(old.series_returned, 1);
        assert_eq!(old.samples_returned, 3);
        assert_eq!(old.raw_f64_pages, 5);
        assert_eq!(old.raw_f64_bytes, 40);
        assert_eq!(
            old.status.expect("status").code,
            pb::status::Code::Ok as i32
        );
    }

    /// OLD worker -> NEW coordinator. An old worker's summary bytes (no field
    /// 7 on the wire at all) decode into the current `Summary` with
    /// `phase_accounting: None`, and `decode_slice_frames` carries that `None`
    /// through to the `SliceResponse` while the pooled `accounting` is intact.
    ///
    /// `None` is the coordinator's signal to charge the pooled total to its scan
    /// phase, which is what every coordinator did before the split existed; the
    /// coordinator-side half of this degradation is pinned by
    /// `worker_reporting_no_phase_split_is_charged_to_scan` in `distrib::tests`.
    /// It fails if a decoder ever fabricates a split for an old worker (say by
    /// defaulting the field to a zero split instead of `None`): a fabricated
    /// zero split would silently drop the slice's whole cost.
    #[test]
    fn old_worker_summary_read_by_a_new_coordinator_carries_no_split() {
        use prost::Message;

        let mut pooled = QueryAccountingSnapshot::default();
        pooled.s3_requests[AccountedOp::Get.index()] = 3;
        pooled.s3_bytes[AccountedOp::Get.index()] = 99;
        let old = LegacySummary {
            accounting: Some(codec::encode_accounting(&pooled)),
            series_returned: 1,
            samples_returned: 3,
            status: Some(pb::Status {
                code: pb::status::Code::Ok as i32,
                message: String::new(),
            }),
            raw_f64_pages: 5,
            raw_f64_bytes: 40,
        };

        let bytes = old.encode_to_vec();
        let new = pb::Summary::decode(bytes.as_slice())
            .expect("a new coordinator decodes an old worker's summary");
        assert!(
            new.phase_accounting.is_none(),
            "an old worker sends no split, so the field must stay absent"
        );

        let response = decode_slice_frames(vec![pb::FetchResponse {
            frame: Some(pb::fetch_response::Frame::Summary(new)),
        }])
        .expect("an old worker's summary decodes");
        assert_eq!(response.phase_accounting, None);
        assert_eq!(response.accounting, pooled);
        assert_eq!(response.stats.raw_f64_pages, 5);
        assert_eq!(response.status, pb::status::Code::Ok);
    }

    /// A current worker's split reaches the coordinator per phase, on all three
    /// signals' decoders: the `SliceResponse` carries the four phase snapshots
    /// the worker sent (1 probe GET, 2 scan GETs, nothing on resolve or plan),
    /// and the pooled field is their sum, not a second cost.
    ///
    /// It fails if a decoder drops the field (`phase_accounting: None`) or maps
    /// it onto the wrong phase: the probe/scan assertions are asymmetric, so a
    /// swapped pair is visible.
    #[test]
    fn phase_split_survives_the_wire_on_every_signal_decoder() {
        let split = phase_split();
        let expect = |got: Option<PhaseAccountingSnapshot>, pooled: QueryAccountingSnapshot| {
            let got = got.expect("the worker sent a split");
            assert_eq!(got, split);
            assert_eq!(
                got.phase(QueryPhase::Resolve).s3_requests(AccountedOp::Get),
                0
            );
            assert_eq!(got.phase(QueryPhase::Plan).s3_requests(AccountedOp::Get), 0);
            assert_eq!(
                got.phase(QueryPhase::Probe).s3_requests(AccountedOp::Get),
                1
            );
            assert_eq!(got.phase(QueryPhase::Probe).s3_bytes(AccountedOp::Get), 29);
            assert_eq!(got.phase(QueryPhase::Scan).s3_requests(AccountedOp::Get), 2);
            assert_eq!(got.phase(QueryPhase::Scan).s3_bytes(AccountedOp::Get), 70);
            assert_eq!(got.pooled(), pooled, "the split sums to the pooled field");
        };

        let metrics = decode_slice_frames(vec![summary_frame(pb::status::Code::Ok)])
            .expect("metrics summary decodes");
        expect(metrics.phase_accounting, metrics.accounting);

        let logs = decode_log_slice_frames(vec![summary_frame(pb::status::Code::Ok)])
            .expect("log summary decodes");
        expect(logs.phase_accounting, logs.accounting);

        let spans = decode_span_slice_frames(vec![summary_frame(pb::status::Code::Ok)])
            .expect("span summary decodes");
        expect(spans.phase_accounting, spans.accounting);
    }

    /// A summary whose split this build cannot attribute is a typed error on the
    /// production decode path, never a silently reinterpreted or partially
    /// applied cost. Three malformations, each with its own variant:
    ///
    /// - a phase discriminant this build does not model (a future fifth phase,
    ///   or the never-sent `UNSPECIFIED` zero): the cost belongs to a phase this
    ///   coordinator cannot name;
    /// - the same phase twice: two costs for one phase cannot be merged into
    ///   one, and summing them would report a figure the worker never sent;
    /// - an entry with no accounting submessage, distinct from an omitted entry.
    ///
    /// It fails if a decoder ever swallows a malformed split (mapping it to
    /// `None`, or skipping the bad entry): the slice's cost would then be
    /// misattributed or dropped behind a response that looks well-formed.
    #[test]
    fn a_split_this_build_cannot_attribute_is_a_typed_error() {
        let with_phases = |phases: Vec<pb::PhaseCost>| {
            let mut pooled = QueryAccountingSnapshot::default();
            pooled.s3_requests[AccountedOp::Get.index()] = 1;
            vec![pb::FetchResponse {
                frame: Some(pb::fetch_response::Frame::Summary(pb::Summary {
                    accounting: Some(codec::encode_accounting(&pooled)),
                    series_returned: 0,
                    samples_returned: 0,
                    status: Some(pb::Status {
                        code: pb::status::Code::Ok as i32,
                        message: String::new(),
                    }),
                    raw_f64_pages: 0,
                    raw_f64_bytes: 0,
                    phase_accounting: Some(pb::PhaseAccountingSnapshot { phases }),
                })),
            }]
        };
        let scan = |accounting: Option<pb::QueryAccountingSnapshot>| pb::PhaseCost {
            phase: pb::QueryPhase::Scan as i32,
            accounting,
        };
        let zero = || Some(codec::encode_accounting(&QueryAccountingSnapshot::default()));

        // An unmodelled discriminant, and proto3's never-sent zero.
        for bad in [99, pb::QueryPhase::Unspecified as i32] {
            let frames = with_phases(vec![pb::PhaseCost {
                phase: bad,
                accounting: zero(),
            }]);
            assert!(
                matches!(
                    decode_slice_frames(frames),
                    Err(DistribError::Codec(CodecError::UnknownQueryPhase(got))) if got == bad
                ),
                "phase discriminant {bad} must be a typed error"
            );
        }

        // The same phase twice.
        assert!(matches!(
            decode_slice_frames(with_phases(vec![scan(zero()), scan(zero())])),
            Err(DistribError::Codec(CodecError::DuplicateQueryPhase("scan")))
        ));

        // An entry that names a phase but carries no counters.
        assert!(matches!(
            decode_slice_frames(with_phases(vec![scan(None)])),
            Err(DistribError::Codec(CodecError::MissingPhaseAccounting(
                "scan"
            )))
        ));

        // The log and span decoders reject the same malformations: they share
        // the one decode implementation, so a divergence here would mean one
        // signal's slice cost is trusted where another's is not.
        assert!(matches!(
            decode_log_slice_frames(with_phases(vec![scan(zero()), scan(zero())])),
            Err(DistribError::Codec(CodecError::DuplicateQueryPhase("scan")))
        ));
        assert!(matches!(
            decode_span_slice_frames(with_phases(vec![scan(zero()), scan(zero())])),
            Err(DistribError::Codec(CodecError::DuplicateQueryPhase("scan")))
        ));
    }

    /// A phase a worker simply omitted decodes as that phase's zero counters,
    /// not as a missing split: a worker that issued no request in a phase has
    /// nothing to report for it, and the remaining phases must still be charged.
    /// A `Some(..)` with an EMPTY list stays distinct from `None`, which is the
    /// version-skew signal (`phase_accounting` absent entirely).
    #[test]
    fn an_omitted_phase_decodes_as_zero_and_an_empty_split_is_not_absent() {
        let mut pooled = QueryAccountingSnapshot::default();
        pooled.s3_requests[AccountedOp::Get.index()] = 4;
        let build = |phases: Vec<pb::PhaseCost>| {
            vec![pb::FetchResponse {
                frame: Some(pb::fetch_response::Frame::Summary(pb::Summary {
                    accounting: Some(codec::encode_accounting(&pooled)),
                    series_returned: 0,
                    samples_returned: 0,
                    status: Some(pb::Status {
                        code: pb::status::Code::Ok as i32,
                        message: String::new(),
                    }),
                    raw_f64_pages: 0,
                    raw_f64_bytes: 0,
                    phase_accounting: Some(pb::PhaseAccountingSnapshot { phases }),
                })),
            }]
        };

        // Only `probe` reported: the other three decode as zero.
        let mut probe_only = QueryAccountingSnapshot::default();
        probe_only.s3_requests[AccountedOp::Get.index()] = 4;
        let response = decode_slice_frames(build(vec![pb::PhaseCost {
            phase: pb::QueryPhase::Probe as i32,
            accounting: Some(codec::encode_accounting(&probe_only)),
        }]))
        .expect("a partial split decodes");
        let split = response.phase_accounting.expect("a split was reported");
        assert_eq!(split.probe.s3_requests(AccountedOp::Get), 4);
        assert_eq!(split.resolve, QueryAccountingSnapshot::default());
        assert_eq!(split.plan, QueryAccountingSnapshot::default());
        assert_eq!(split.scan, QueryAccountingSnapshot::default());

        // An empty list is a reported split of nothing, NOT the absent field.
        let empty = decode_slice_frames(build(Vec::new())).expect("an empty split decodes");
        assert_eq!(
            empty.phase_accounting,
            Some(PhaseAccountingSnapshot::default()),
            "an empty list must stay distinct from the absent field, which means \
             the worker reported no split at all"
        );
    }
}
