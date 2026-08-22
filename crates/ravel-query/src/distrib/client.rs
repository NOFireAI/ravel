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
    /// The worker's per-slice cost accounting.
    pub accounting: ravel_types::accounting::QueryAccountingSnapshot,
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
    /// The worker's per-slice cost accounting.
    pub accounting: QueryAccountingSnapshot,
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
    /// The worker's per-slice cost accounting.
    pub accounting: QueryAccountingSnapshot,
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
    Ok(SliceResponse {
        scalar,
        histogram,
        partials,
        accounting,
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
    Ok(SliceLogResponse {
        records,
        accounting,
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
    Ok(SliceSpanResponse {
        spans,
        accounting,
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
    fn summary_frame(code: pb::status::Code) -> pb::FetchResponse {
        let mut snap = QueryAccountingSnapshot::default();
        snap.s3_requests[AccountedOp::Get.index()] = 3;
        snap.s3_bytes[AccountedOp::Get.index()] = 99;
        pb::FetchResponse {
            frame: Some(pb::fetch_response::Frame::Summary(pb::Summary {
                accounting: Some(codec::encode_accounting(&snap)),
                series_returned: 1,
                samples_returned: 3,
                status: Some(pb::Status {
                    code: code as i32,
                    message: String::new(),
                }),
                raw_f64_pages: 5,
                raw_f64_bytes: 40,
            })),
        }
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
}
