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
use crate::fetcher::{FetchStats, FetchedSeriesSoa};

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
    /// The remote streamed a native-histogram frame, which this build cannot
    /// decode across the slice boundary yet (worker histogram decode is a later
    /// ticket). This is NOT corruption: the frame is well-formed, the remote
    /// simply carries data kind this coordinator cannot consume. The federation
    /// layer treats it as a coverage gap (skippable under `skip_unavailable`),
    /// never a malformed-response hard fault.
    #[error(
        "remote returned native-histogram data this build cannot decode across the slice boundary"
    )]
    HistogramUnsupported,
    /// The remote streamed a per-signal record frame (a log record or a span)
    /// for a signal this coordinator's metrics fetch path cannot consume.
    /// Unreachable from a real query today: the metrics coordinator only ever
    /// dispatches `Signal::Metrics`, and the log/span coordinators that would
    /// receive these frames are built by #284/#285. The `frame` oneof is
    /// exhaustive, so the metrics decoder must still name the variants; like
    /// [`HistogramUnsupported`](Self::HistogramUnsupported) this is a
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
}

#[async_trait::async_trait]
impl SliceFetcher for RemoteSliceFetcher {
    async fn fetch(&self, request: pb::FetchRequest) -> Result<SliceResponse, DistribError> {
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
        decode_slice_frames(frames)
    }

    async fn fetch_logs(
        &self,
        request: pb::FetchRequest,
    ) -> Result<SliceLogResponse, DistribError> {
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
        decode_log_slice_frames(frames)
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
/// frame this build cannot decode across the slice boundary
/// ([`DistribError::HistogramUnsupported`] -- well-formed, not corruption, so
/// the federation layer treats it as a coverage gap skippable under
/// `skip_unavailable` rather than a malformed response, and the data is never
/// silently dropped), a frame carrying no oneof variant
/// ([`DistribError::EmptyFrame`]), a second summary
/// ([`DistribError::MultipleSummaries`]), a stream that ended with no summary
/// ([`DistribError::NoSummary`]), a summary with no status, or an unknown
/// status code.
pub fn decode_slice_frames(frames: Vec<pb::FetchResponse>) -> Result<SliceResponse, DistribError> {
    let mut scalar = Vec::new();
    let mut summary: Option<pb::Summary> = None;
    for frame in frames {
        match frame.frame {
            Some(pb::fetch_response::Frame::Series(sf)) => {
                scalar.extend(codec::decode_series_frame(sf)?);
            }
            Some(pb::fetch_response::Frame::Hist(_)) => {
                return Err(DistribError::HistogramUnsupported);
            }
            Some(pb::fetch_response::Frame::LogRecord(_)) => {
                return Err(DistribError::FrameSignalUnsupported("log-record"));
            }
            Some(pb::fetch_response::Frame::Span(_)) => {
                return Err(DistribError::FrameSignalUnsupported("span"));
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

    /// A native-histogram frame this build cannot decode across the slice
    /// boundary is the distinct `HistogramUnsupported` coverage-gap error, never
    /// a generic malformed-frame error and never a panic.
    #[test]
    fn histogram_frame_is_histogram_unsupported() {
        let hist = pb::FetchResponse {
            frame: Some(pb::fetch_response::Frame::Hist(pb::HistogramFrame {
                series_id: vec![0u8; 16],
                labels: Vec::new(),
                runs: Vec::new(),
            })),
        };
        assert!(matches!(
            decode_slice_frames(vec![hist]),
            Err(DistribError::HistogramUnsupported)
        ));
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
}
