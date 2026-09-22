//! The coordinator's view of a slice worker: the [`SliceFetcher`] seam and its
//! `tonic`-backed [`RemoteSliceFetcher`] (ADR-0071).
//!
//! [`SliceFetcher`] is the one seam the merge layer holds. A [`RemoteSliceFetcher`]
//! drives a real gRPC worker for the METRICS signal; a test double can implement
//! the same trait to return crafted frames. Either way the coordinator receives
//! the identical [`SliceResponse`] shape, so the merge cannot tell a remote slice
//! from a local one.
//!
//! No transport in this crate decodes a remote's log or span slice. The
//! log and span fetches are the [`SliceFetcher`] trait defaults, which report
//! [`pb::status::Code::Unsupported`] and send the coordinator to whole-query
//! local execution; whoever wires those signals across the slice boundary writes
//! a bounded incremental decoder for them, the shape
//! [`SliceStreamDecoder`](crate::distrib::SliceStreamDecoder) establishes (issue
//! #1912).

use ravel_logseg::LogRecord;
use ravel_proto::queryfrag::v1 as pb;
use ravel_types::accounting::QueryAccountingSnapshot;
use tonic::transport::Channel;

use crate::config::EngineConfig;
use crate::distrib::SliceStreamDecoder;
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
    /// Unreachable from a real query today: this crate only ever dispatches
    /// `Signal::Metrics` across the slice boundary, and nothing here decodes a
    /// remote's log or span slice (issue #1912). The `frame` oneof is
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
    /// whole-query local execution rather than erroring. This mirrors the base
    /// ADR's silent version-skew fallback: an unimplemented log fetch is a
    /// coverage gap the coordinator fills locally, never a hard failure.
    ///
    /// Nothing overrides it today, [`RemoteSliceFetcher`] included (issue
    /// #1912): an override has to decode the worker's [`pb::LogRecordFrame`]s
    /// incrementally under a per-slice frame and wire-byte cap, the way
    /// [`SliceStreamDecoder`](crate::distrib::SliceStreamDecoder) does for
    /// metrics, so that a remote cannot decide how much the coordinator buffers.
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
    /// whole-query local execution rather than erroring. Nothing overrides it
    /// today, and an override carries the same bounded-decode obligation.
    /// Mirrors [`fetch_logs`](Self::fetch_logs) exactly.
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
///
/// Metrics only. It reads the worker's stream one frame at a time through a
/// [`SliceStreamDecoder`], so the per-slice frame and wire-byte caps are checked
/// before each frame is decoded and the first breach stops the read (issue
/// #1912). It does not override [`SliceFetcher::fetch_logs`] or
/// [`SliceFetcher::fetch_spans`]: those report `Unsupported` and the coordinator
/// falls back to whole-query local execution.
pub struct RemoteSliceFetcher {
    channel: Channel,
    /// `None` leaves the decoder's own frame cap
    /// ([`codec::MAX_SLICE_RESPONSE_FRAMES`]) in force.
    max_frames: Option<usize>,
    /// `None` leaves the decoder's own byte cap ([`codec::slice_byte_cap`]) in
    /// force.
    max_bytes: Option<u64>,
}

impl RemoteSliceFetcher {
    pub fn new(channel: Channel) -> Self {
        RemoteSliceFetcher {
            channel,
            max_frames: None,
            max_bytes: None,
        }
    }

    /// Replace this fetcher's per-slice frame cap, the
    /// [`SliceStreamDecoder::with_max_frames`] seam one level up: the real
    /// constant is sized so no ordinary slice reaches it, which also makes it
    /// impractical to drive over a real stream. A fetcher that does not call
    /// this is bounded by the constant. Test-only (`pub(crate)`): nothing
    /// outside this crate's test modules calls it, and production code sets
    /// caps on [`SliceStreamDecoder`] directly.
    #[cfg(test)]
    pub(crate) fn with_max_frames(mut self, max_frames: usize) -> Self {
        self.max_frames = Some(max_frames);
        self
    }

    /// Replace this fetcher's per-slice wire-byte cap, the
    /// [`SliceStreamDecoder::with_max_bytes`] seam one level up, for the same
    /// reason. Test-only (`pub(crate)`), for the same reason as
    /// [`with_max_frames`](Self::with_max_frames).
    #[cfg(test)]
    pub(crate) fn with_max_bytes(mut self, max_bytes: u64) -> Self {
        self.max_bytes = Some(max_bytes);
        self
    }

    /// A decoder carrying this fetcher's caps. The config it is built from only
    /// selects the byte cap through [`codec::slice_byte_cap`], which is the
    /// fixed [`codec::MAX_SLICE_RESPONSE_BYTES`] ceiling for every config, so a
    /// default one is the same cap a coordinator's own config would give.
    fn decoder(&self) -> SliceStreamDecoder {
        let mut decoder = SliceStreamDecoder::new(&EngineConfig::default());
        if let Some(max_frames) = self.max_frames {
            decoder = decoder.with_max_frames(max_frames);
        }
        if let Some(max_bytes) = self.max_bytes {
            decoder = decoder.with_max_bytes(max_bytes);
        }
        decoder
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
        let mut decoder = self.decoder();
        let mut stream = response.into_inner();
        // A cap breach returns from here rather than pulling another message:
        // the caps bound what this coordinator holds only if the read stops.
        while let Some(frame) = stream
            .message()
            .await
            .map_err(|s| DistribError::Transport(s.to_string()))?
        {
            decoder.push(frame)?;
        }
        decoder.finish()
    }
}

/// Decode a slice's full frame sequence into a [`SliceResponse`].
///
/// This is the whole-sequence decode: it takes every frame at once, so its
/// caller already holds the whole slice. One caller remains, and it holds the
/// frames for a reason that is not a remote's to decide: `ravel-server`'s local
/// no-hop fetch (`FragmentService::run_local` in
/// `services/ravel-server/src/distrib.rs`) calls it on frames this same process
/// just produced in-process.
///
/// No path that reads a REMOTE's stream calls this. An intra-cluster remote
/// dispatch (`RoutingSliceFetcher::remote_fetch`), cross-cluster federation
/// (`FederationSliceFetcher::fetch`), and [`RemoteSliceFetcher`] all decode
/// incrementally through
/// [`SliceStreamDecoder`](crate::distrib::SliceStreamDecoder), which applies the
/// per-slice frame and wire-byte caps before each frame is decoded (issue #1687
/// part B, extended to `RemoteSliceFetcher` by #1912).
///
/// So there are now two per-frame decode implementations, not one. Nothing
/// structural holds them together:
/// [`SliceStreamDecoder::push`](crate::distrib::SliceStreamDecoder::push)
/// repeats the match below rather than calling into it, and a change to the
/// frames a slice may carry has to be made in both. What keeps them from
/// drifting is a test,
/// `tests::the_incremental_decoder_agrees_with_the_whole_sequence_decode`, which
/// feeds the same frame sequences to both and asserts they produce the same
/// response and the same typed errors.
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
            // A metrics slice returns its histogram-kind series as their own
            // frames (`histogram` above) rather than dropping them, so nothing
            // was skipped for a caller to be warned about.
            histogram_series_skipped: 0,
        },
        series_returned: summary.series_returned,
        samples_returned: summary.samples_returned,
        status: code,
        status_message: status.message,
    })
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::pin::Pin;

    use futures::Stream;
    use ravel_types::accounting::{AccountedOp, QueryAccountingSnapshot};
    use ravel_types::{Label, LabelSet, SeriesId};
    use tokio::runtime::Runtime;
    use tonic::transport::Server;
    use tonic::transport::server::TcpIncoming;

    use super::*;
    use crate::distrib::codec::{self, CodecError};
    use crate::distrib::proto::series_fetch_server::{SeriesFetch, SeriesFetchServer};

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
        summary_frame_with_counts(code, 1, 3)
    }

    /// As [`summary_frame`], with `series_returned`/`samples_returned` set to
    /// `series_returned`/`samples_returned` instead of the fixed 1/3: a caller
    /// that streams a different number of series frames builds its summary from
    /// what it actually sent, so the two cannot silently disagree.
    fn summary_frame_with_counts(
        code: pb::status::Code,
        series_returned: u64,
        samples_returned: u64,
    ) -> pb::FetchResponse {
        let mut snap = QueryAccountingSnapshot::default();
        snap.s3_requests[AccountedOp::Get.index()] = 3;
        snap.s3_bytes[AccountedOp::Get.index()] = 99;
        pb::FetchResponse {
            frame: Some(pb::fetch_response::Frame::Summary(pb::Summary {
                accounting: Some(codec::encode_accounting(&snap)),
                series_returned,
                samples_returned,
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

    /// The anti-drift pin for the two per-frame decode implementations (issue
    /// #1687 part B). Since the coordinator's remote paths moved to
    /// `SliceStreamDecoder`, `push`/`finish` repeat this function's match rather
    /// than calling into it, so the frames a slice may carry are decoded in two
    /// places. This feeds identical sequences to both and asserts they agree, on
    /// the accepted response and on the typed error, over the SERIES and
    /// SUMMARY frames only. The sequences below do not cover a malformed
    /// `Hist` or `PartialAggregate` frame, the two `FrameSignalUnsupported`
    /// arms (`LogRecord` and `Span`), or an unknown status code, so drift on
    /// those arms is not caught here. Widening the pin to the arms this
    /// function names is issue #1933.
    ///
    /// `SliceResponse` and `DistribError` are `Debug` but neither is `PartialEq`,
    /// so the comparison is over their `Debug` rendering, which covers every
    /// field of both.
    #[test]
    fn the_incremental_decoder_agrees_with_the_whole_sequence_decode() {
        use crate::EngineConfig;
        use crate::distrib::SliceStreamDecoder;

        /// Run `frames` through the incremental decoder exactly as a
        /// coordinator does: push until the first error, then `finish`.
        fn incremental(frames: Vec<pb::FetchResponse>) -> Result<SliceResponse, DistribError> {
            let mut decoder = SliceStreamDecoder::new(&EngineConfig::default());
            for frame in frames {
                decoder.push(frame)?;
            }
            decoder.finish()
        }

        let soa = series_soa();
        let bad_series = pb::FetchResponse {
            frame: Some(pb::fetch_response::Frame::Series(pb::SeriesFrame {
                series_id: vec![0u8; 15],
                labels: Vec::new(),
                runs: Vec::new(),
            })),
        };
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

        let sequences: Vec<(&str, Vec<pb::FetchResponse>)> = vec![
            (
                "series then summary",
                vec![series_frame(&soa), summary_frame(pb::status::Code::Ok)],
            ),
            (
                "summary alone",
                vec![summary_frame(pb::status::Code::BudgetExceeded)],
            ),
            ("no summary at all", vec![series_frame(&soa)]),
            (
                "two summaries",
                vec![
                    summary_frame(pb::status::Code::Ok),
                    summary_frame(pb::status::Code::Ok),
                ],
            ),
            (
                "a malformed series frame",
                vec![bad_series, summary_frame(pb::status::Code::Ok)],
            ),
            ("an empty frame", vec![pb::FetchResponse { frame: None }]),
            ("a summary with no status", vec![no_status]),
        ];

        for (what, frames) in sequences {
            let whole = decode_slice_frames(frames.clone());
            let streamed = incremental(frames);
            assert_eq!(
                format!("{whole:?}"),
                format!("{streamed:?}"),
                "the two decoders disagree on {what}"
            );
        }
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

    // --- The remote read path is bounded (#1912) ---------------------------
    //
    // `RemoteSliceFetcher::fetch` is the only code in this crate that reads a
    // remote worker's stream. It decodes through `SliceStreamDecoder`, so a
    // worker that streams without end is refused at the cap instead of being
    // drained into the coordinator's memory. The tests below drive it over a
    // real loopback gRPC worker to prove that on the live path, not on the
    // decoder in isolation (`crate::distrib::slice_cap_tests` covers that).

    /// A worker that answers every fetch with `count` well-formed series frames
    /// and never sends the terminal summary. Nothing in the sequence is
    /// malformed, so a fetch that refuses it refused it for its SIZE.
    #[derive(Clone)]
    struct FloodWorker {
        count: usize,
        /// Append a well-formed terminal summary after the frames.
        ///
        /// The two cap tests leave this off: they refuse part way through and
        /// never reach the end of the stream, so the summary is irrelevant to
        /// them. The control turns it on, because only a summary lets the
        /// fetch return `Ok` and so lets the control assert that frames
        /// DECODED rather than merely that a stream ended.
        with_summary: bool,
    }

    #[tonic::async_trait]
    impl SeriesFetch for FloodWorker {
        type FetchStream =
            Pin<Box<dyn Stream<Item = Result<pb::FetchResponse, tonic::Status>> + Send + 'static>>;

        async fn fetch(
            &self,
            _request: tonic::Request<pb::FetchRequest>,
        ) -> Result<tonic::Response<Self::FetchStream>, tonic::Status> {
            let soa = series_soa();
            let mut frames: Vec<_> = (0..self.count).map(|_| Ok(series_frame(&soa))).collect();
            if self.with_summary {
                // The declared counts must track what this worker actually put
                // on the wire (`self.count` series frames, each carrying
                // `soa.timestamps.len()` samples): a summary hardcoded to a
                // fixed count would agree with a decode that silently dropped
                // frames, which is exactly the gap issue #1912's fix round
                // found in `remote_fetch_under_the_caps_decodes_the_slice`.
                let series_returned = self.count as u64;
                let samples_returned = (self.count * soa.timestamps.len()) as u64;
                frames.push(Ok(summary_frame_with_counts(
                    pb::status::Code::Ok,
                    series_returned,
                    samples_returned,
                )));
            }
            Ok(tonic::Response::new(Box::pin(futures::stream::iter(
                frames,
            ))))
        }
    }

    /// Serve `count` frames from a loopback worker and run one `RemoteSliceFetcher`
    /// fetch against it, with the caps `caps` installs.
    fn fetch_from_flood_worker(
        count: usize,
        caps: impl FnOnce(RemoteSliceFetcher) -> RemoteSliceFetcher,
    ) -> Result<SliceResponse, DistribError> {
        fetch_from_worker(count, false, caps)
    }

    /// As [`fetch_from_flood_worker`], with the terminal summary a successful
    /// fetch needs.
    fn fetch_from_worker_with_summary(
        count: usize,
        caps: impl FnOnce(RemoteSliceFetcher) -> RemoteSliceFetcher,
    ) -> Result<SliceResponse, DistribError> {
        fetch_from_worker(count, true, caps)
    }

    fn fetch_from_worker(
        count: usize,
        with_summary: bool,
        caps: impl FnOnce(RemoteSliceFetcher) -> RemoteSliceFetcher,
    ) -> Result<SliceResponse, DistribError> {
        let runtime = Runtime::new().expect("tokio runtime");
        runtime.block_on(async move {
            let incoming =
                TcpIncoming::bind("127.0.0.1:0".parse().expect("addr")).expect("bind loopback");
            let addr = incoming.local_addr().expect("local addr");
            let server = tokio::spawn(async move {
                Server::builder()
                    .add_service(SeriesFetchServer::new(FloodWorker {
                        count,
                        with_summary,
                    }))
                    .serve_with_incoming(incoming)
                    .await
            });
            let channel = Channel::from_shared(format!("http://{addr}"))
                .expect("endpoint")
                .connect_lazy();
            let result = caps(RemoteSliceFetcher::new(channel))
                .fetch(pb::FetchRequest::default())
                .await;
            server.abort();
            result
        })
    }

    /// The acceptance test for issue #1912: no path in this crate collects a
    /// remote's frames before deciding whether it may hold them.
    ///
    /// A worker streams 64 frames at a fetcher whose per-slice frame cap is 4.
    /// The fetch refuses with `SliceFrameCapExceeded { frames: 5, max: 4 }`: the
    /// five is the cap plus the one frame the decoder must receive to notice the
    /// cap was passed.
    ///
    /// What that pins, precisely: the decoder REFUSED at the cap instead of
    /// decoding all 64. `frames` is `SliceStreamDecoder::frames`, incremented
    /// per `push`, so it counts pushes and NOT what the transport buffered --
    /// a shape that drained the stream into a `Vec` and then pushed frame by
    /// frame would still report 5 and still pass. This test therefore does not
    /// by itself prove nothing upstream of the decoder buffers; what it proves
    /// is that no caller decodes past the cap, and the deleted
    /// `decode_slice_frames` shape (which had no cap at all) fails it with
    /// `Err(NoSummary)` after decoding all 64. The absence of a collecting path
    /// is held by the guard on the code itself: `collect_frames` and both
    /// signal-specific decoders are gone, and `RemoteSliceFetcher::fetch` is
    /// the only fetch here.
    #[test]
    fn no_unbounded_slice_decode_path_remains() {
        let result = fetch_from_flood_worker(64, |fetcher| fetcher.with_max_frames(4));
        match result {
            Err(DistribError::Codec(CodecError::SliceFrameCapExceeded { frames, max })) => {
                assert_eq!(max, 4);
                assert_eq!(frames, 5, "the read must stop one frame past the cap");
            }
            other => panic!("expected a frame-cap refusal, got {other:?}"),
        }
    }

    /// The same live path under the absolute wire-byte ceiling, which is the cap
    /// that binds when a worker sends few but enormous frames. One series frame
    /// is larger than this one-byte cap, so the first frame refuses the slice.
    #[test]
    fn remote_fetch_refuses_a_slice_past_the_byte_cap() {
        let result = fetch_from_flood_worker(64, |fetcher| fetcher.with_max_bytes(1));
        match result {
            Err(DistribError::Codec(CodecError::SliceByteCapExceeded { bytes, max })) => {
                assert_eq!(max, 1);
                assert!(bytes > 1, "the refusal counts the frame that tripped it");
            }
            other => panic!("expected a byte-cap refusal, got {other:?}"),
        }
    }

    /// The control for both cap tests: the same transport, under caps the stream
    /// fits inside, decodes normally. Without this, a fetch broken in any way at
    /// all would satisfy the two refusal assertions above by accident.
    ///
    /// The worker sends a terminal summary here so the fetch can return `Ok`,
    /// and the assertions are on the DECODED payload (`response.scalar`), not
    /// on the summary's counts: `series_returned`/`samples_returned` are
    /// copied straight out of the terminal summary by `SliceStreamDecoder::
    /// finish` and never touch what `push` actually decoded, so a fetch that
    /// accepted every frame and dropped each one on the floor -- reaching the
    /// same summary-carrying end of stream -- would satisfy a summary-only
    /// assertion. `FloodWorker` now derives that summary from `self.count` and
    /// `soa.timestamps.len()` (the frames it actually streamed), so a
    /// summary-count check is asserted too, but only as a cross-check on top
    /// of the decoded-payload assertions below, which are the ones a
    /// frame-dropping decode fails.
    #[test]
    fn remote_fetch_under_the_caps_decodes_the_slice() {
        let soa = series_soa();
        let response = fetch_from_worker_with_summary(3, |fetcher| {
            fetcher.with_max_frames(4).with_max_bytes(1 << 20)
        })
        .expect("a stream inside both caps decodes");
        assert_eq!(
            response.scalar.len(),
            3,
            "all 3 streamed series frames must land in the decoded payload"
        );
        for (i, got) in response.scalar.iter().enumerate() {
            assert_eq!(
                got.series_id, soa.series_id,
                "decoded series {i} must carry the worker's series id"
            );
            assert_eq!(
                got.timestamps, soa.timestamps,
                "decoded series {i} must carry the worker's timestamps"
            );
            assert_eq!(
                got.values, soa.values,
                "decoded series {i} must carry the worker's values"
            );
        }
        assert_eq!(
            response.series_returned, 3,
            "the summary's series count must match what the worker actually streamed"
        );
        assert_eq!(
            response.samples_returned, 9,
            "the summary's sample count must match what the worker actually streamed"
        );
    }
}
