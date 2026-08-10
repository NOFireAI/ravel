//! The coordinator's view of a slice worker: the [`SliceFetcher`] seam and its
//! `tonic`-backed [`RemoteSliceFetcher`] (ADR-0071, issue #864).
//!
//! [`SliceFetcher`] is the one seam the merge layer holds. A [`RemoteSliceFetcher`]
//! drives a real gRPC worker; a test double can implement the same trait to
//! return crafted frames. Either way the coordinator receives the identical
//! [`SliceResponse`] shape, so the merge cannot tell a remote slice from a
//! local one.

use ravel_proto::queryfrag::v1 as pb;
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

/// The seam between the coordinator merge and a slice worker. Object-safe (via
/// `async_trait`) so the engine holds one `dyn SliceFetcher`.
#[async_trait::async_trait]
pub trait SliceFetcher: Send + Sync {
    /// Dispatches one slice request and collects its full response.
    async fn fetch(&self, request: pb::FetchRequest) -> Result<SliceResponse, DistribError>;
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

        let mut scalar = Vec::new();
        let mut summary: Option<pb::Summary> = None;
        while let Some(frame) = stream
            .message()
            .await
            .map_err(|s| DistribError::Transport(s.to_string()))?
        {
            match frame.frame {
                Some(pb::fetch_response::Frame::Series(sf)) => {
                    scalar.extend(codec::decode_series_frame(sf)?);
                }
                // A histogram frame from a worker means it did not fall back to
                // Unsupported; this build never emits one, but decoding it is a
                // later ticket, so treat it as framing breakage rather than
                // silently dropping data.
                Some(pb::fetch_response::Frame::Hist(_)) => {
                    return Err(DistribError::Codec(CodecError::EmptyFrame));
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
}
