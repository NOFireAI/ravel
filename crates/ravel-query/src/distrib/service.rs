//! The ADR-0071 slice worker service and the remote client that drives it
//! (issue #864).
//!
//! A coordinator dispatches one [`pb::FetchRequest`] per slice; the worker
//! ([`SeriesFetchService`]) executes the existing local fetch path over only
//! that slice's segments, applies the request's erasure predicates exactly as
//! the local path would, and streams the decoded series back, ending with
//! exactly one [`pb::Summary`] frame (slice atomicity: the slice contributes to
//! the coordinator merge only after its terminal summary arrives).
//!
//! # Scalar-only, for now
//!
//! Distribution carries scalar series only. The histogram span payload grammar
//! is a later ticket (`proto/ravel/queryfrag.proto`'s `HistogramRun`), so a
//! slice whose segments decode any native-histogram series returns
//! [`pb::status::Code::Unsupported`]; the coordinator then silently falls back
//! to fully local execution for the whole query (ADR-0071 failure semantics),
//! never a partial or wrong result.
//!
//! # Segment identity resolution
//!
//! A worker receives durable [`pb::SegmentIdentity`] values, not object keys or
//! trusted bytes (ADR-0071 reconstruct-don't-trust). Turning an identity back
//! into the [`SegmentRef`] to fetch needs the `ravel-commit` key
//! reconstruction that ticket #865 owns. Until that lands, a [`SegmentResolver`]
//! maps an identity to a ref by its content hash; [`SnapshotSegmentResolver`]
//! is the interim implementation, resolving against the same pinned snapshot
//! the coordinator dispatched from.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use ravel_catalog::SegmentRef;
use ravel_proto::queryfrag::v1 as pb;
use ravel_types::TenantHash;
use ravel_types::accounting::{QueryAccounting, QueryAccountingSnapshot};

use crate::distrib::codec;
use crate::distrib::proto::series_fetch_server::{SeriesFetch, SeriesFetchServer};
use crate::fetcher::{FetchError, SegmentFetcher};

/// Resolves a shipped [`pb::SegmentIdentity`] back to the [`SegmentRef`] a
/// worker fetches. The production resolver (ticket #865) reconstructs the
/// object key from the identity and verifies the footer; see the module docs
/// for why an interim content-hash resolver stands in for now.
pub trait SegmentResolver: Send + Sync {
    /// The ref this identity names, or `None` if it is unknown to this worker
    /// (the segment vanished under a concurrent GC/compaction, which the
    /// coordinator maps to a snapshot invalidation).
    fn resolve(&self, identity: &pb::SegmentIdentity) -> Option<SegmentRef>;
}

/// Interim [`SegmentResolver`] that resolves identities by content hash against
/// a fixed set of known segments (the pinned snapshot the coordinator
/// dispatched from). Stands in for the #865 reconstruct-from-identity path.
pub struct SnapshotSegmentResolver {
    by_content_hash: HashMap<[u8; 32], SegmentRef>,
}

impl SnapshotSegmentResolver {
    /// Builds a resolver over the given segments, keyed by content hash. A
    /// content-hash collision (never expected: blake3 over immutable objects)
    /// keeps the last segment inserted.
    pub fn new(segments: impl IntoIterator<Item = SegmentRef>) -> Self {
        let by_content_hash = segments
            .into_iter()
            .map(|seg| (seg.content_hash, seg))
            .collect();
        SnapshotSegmentResolver { by_content_hash }
    }
}

impl SegmentResolver for SnapshotSegmentResolver {
    fn resolve(&self, identity: &pb::SegmentIdentity) -> Option<SegmentRef> {
        let hash = codec::identity_content_hash(identity).ok()?;
        self.by_content_hash.get(&hash).cloned()
    }
}

/// The worker-side fragment service. Holds a [`SegmentFetcher`] over the same
/// object store the coordinator's snapshot pins, and a [`SegmentResolver`] to
/// turn shipped identities into refs.
pub struct SeriesFetchService<R: SegmentResolver + 'static> {
    fetcher: SegmentFetcher,
    resolver: Arc<R>,
}

impl<R: SegmentResolver + 'static> SeriesFetchService<R> {
    pub fn new(fetcher: SegmentFetcher, resolver: Arc<R>) -> Self {
        SeriesFetchService { fetcher, resolver }
    }

    /// Wraps this service in the generated gRPC server, ready to add to a
    /// `tonic` router.
    pub fn into_server(self) -> SeriesFetchServer<Self> {
        SeriesFetchServer::new(self)
    }

    /// Runs the slice fetch, returning the full frame sequence to stream. A
    /// terminal [`pb::Summary`] is always the last element; on any typed
    /// failure the returned frames are just that one summary carrying the
    /// mapped status (slice atomicity: no partial series precede a non-OK
    /// summary).
    async fn run_slice(&self, request: pb::FetchRequest) -> Vec<pb::FetchResponse> {
        match self.run_slice_inner(request).await {
            Ok(frames) => frames,
            Err((code, message)) => vec![summary_frame(
                &QueryAccountingSnapshot::default(),
                0,
                0,
                code,
                message,
            )],
        }
    }

    async fn run_slice_inner(
        &self,
        request: pb::FetchRequest,
    ) -> Result<Vec<pb::FetchResponse>, (pb::status::Code, String)> {
        // Version skew: the coordinator falls back to local when it sees this.
        codec::check_protocol_version(request.protocol_version)
            .map_err(|e| (pb::status::Code::Unsupported, e.to_string()))?;

        let tenant_hash =
            decode_tenant_hash(&request.tenant_hash).map_err(|m| (pb::status::Code::BadData, m))?;

        let matchers = codec::decode_matchers(request.matchers)
            .map_err(|e| (pb::status::Code::BadData, e.to_string()))?;
        let erasure = codec::decode_erasure(request.erasure);

        let identities = match request.scope {
            Some(pb::fetch_request::Scope::Pinned(pinned)) => pinned.segments,
            // Cross-cluster resolve scope is a later ticket; fall back to local.
            Some(pb::fetch_request::Scope::Resolve(_)) | None => {
                return Err((
                    pb::status::Code::Unsupported,
                    "resolve-scope slices are not supported yet".to_string(),
                ));
            }
        };

        // Reconstruct each ref from its shipped identity. An unknown identity
        // means the pinned segment vanished under a concurrent GC/compaction:
        // the coordinator's single re-resolve/retry handles it.
        let mut segments = Vec::with_capacity(identities.len());
        for identity in &identities {
            match self.resolver.resolve(identity) {
                Some(seg) => segments.push(seg),
                None => {
                    return Err((
                        pb::status::Code::SnapshotInvalidated,
                        "pinned segment not found on worker".to_string(),
                    ));
                }
            }
        }

        // One fresh accounting handle per slice: the coordinator folds the
        // returned snapshot into the query's aggregate (ADR-0071).
        let accounting = QueryAccounting::new();
        let mut scalar = Vec::new();
        let mut any_histograms = false;
        for seg in &segments {
            let (seg_scalar, _stats, seg_hist) = self
                .fetcher
                .fetch_soa_and_histograms_accounted(tenant_hash, seg, &matchers, &accounting)
                .await
                .map_err(map_fetch_error)?;
            if !seg_hist.is_empty() {
                any_histograms = true;
            }
            scalar.push(seg_scalar);
        }

        // Scalar-only distribution: hand a histogram-bearing slice back to the
        // coordinator's local fallback rather than return partial results.
        if any_histograms {
            return Err((
                pb::status::Code::Unsupported,
                "histogram series are not distributed yet".to_string(),
            ));
        }

        // Selective-erasure exclusion, applied post-decode exactly as the local
        // path applies it (ADR-0064, ADR-0071): the coordinator does not
        // re-apply, so worker-side application must match the local rule.
        if !erasure.is_empty() {
            for series in &mut scalar {
                crate::erasure::retain_series_soa(series, &erasure);
            }
        }

        let mut frames = Vec::new();
        let mut series_returned = 0u64;
        let mut samples_returned = 0u64;
        for segment_series in scalar {
            for fs in segment_series {
                series_returned += 1;
                samples_returned += fs.timestamps.len() as u64;
                frames.push(pb::FetchResponse {
                    frame: Some(pb::fetch_response::Frame::Series(
                        codec::encode_series_frame(&fs),
                    )),
                });
            }
        }
        frames.push(summary_frame(
            &accounting.snapshot(),
            series_returned,
            samples_returned,
            pb::status::Code::Ok,
            String::new(),
        ));
        Ok(frames)
    }
}

/// The stream type the generated trait requires: a boxed stream of already-
/// built frames. The slice is fetched eagerly (this ticket does not stream
/// mid-fetch), then replayed as a stream to satisfy the server-streaming RPC.
type FrameStream =
    Pin<Box<dyn Stream<Item = Result<pb::FetchResponse, tonic::Status>> + Send + 'static>>;

#[tonic::async_trait]
impl<R: SegmentResolver + 'static> SeriesFetch for SeriesFetchService<R> {
    type FetchStream = FrameStream;

    async fn fetch(
        &self,
        request: tonic::Request<pb::FetchRequest>,
    ) -> Result<tonic::Response<Self::FetchStream>, tonic::Status> {
        let frames = self.run_slice(request.into_inner()).await;
        let stream = futures::stream::iter(frames.into_iter().map(Ok));
        Ok(tonic::Response::new(Box::pin(stream)))
    }
}

fn summary_frame(
    accounting: &QueryAccountingSnapshot,
    series_returned: u64,
    samples_returned: u64,
    code: pb::status::Code,
    message: String,
) -> pb::FetchResponse {
    pb::FetchResponse {
        frame: Some(pb::fetch_response::Frame::Summary(pb::Summary {
            accounting: Some(codec::encode_accounting(accounting)),
            series_returned,
            samples_returned,
            status: Some(pb::Status {
                code: code as i32,
                message,
            }),
        })),
    }
}

fn decode_tenant_hash(bytes: &[u8]) -> Result<TenantHash, String> {
    let arr: [u8; 16] = bytes
        .try_into()
        .map_err(|_| format!("tenant hash is {} bytes, expected 16", bytes.len()))?;
    Ok(TenantHash(arr))
}

/// Maps a fetch-path error to the worker's typed status. A vanished object is
/// a snapshot invalidation (retryable); a corrupt or etag-changed object is
/// terminal.
fn map_fetch_error(err: FetchError) -> (pb::status::Code, String) {
    match err {
        FetchError::Store {
            source: ravel_object_store::StoreError::NotFound,
            ..
        } => (pb::status::Code::SnapshotInvalidated, err.to_string()),
        FetchError::Store { .. } => (pb::status::Code::Unavailable, err.to_string()),
        FetchError::Corrupt { .. } => (pb::status::Code::Corrupt, err.to_string()),
        FetchError::EtagChanged { .. } => (pb::status::Code::Corrupt, err.to_string()),
    }
}
