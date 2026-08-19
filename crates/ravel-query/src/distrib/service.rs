//! The ADR-0071 slice worker service and the remote client that drives it
//!.
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
//! is future work (`proto/ravel/queryfrag.proto`'s `HistogramRun`), so a
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
//! reconstruction that is not yet implemented. Until that lands, a [`SegmentResolver`]
//! maps an identity to a ref by its content hash; [`SnapshotSegmentResolver`]
//! is the interim implementation, resolving against the same pinned snapshot
//! the coordinator dispatched from.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use ravel_catalog::SegmentRef;
use ravel_proto::queryfrag::v1 as pb;
use ravel_types::accounting::{QueryAccounting, QueryAccountingSnapshot};
use ravel_types::{Signal, TenantHash};

use ravel_rspan::SpanQuery;

use crate::config::ByteLimit;
use crate::distrib::codec;
use crate::distrib::proto::series_fetch_server::{SeriesFetch, SeriesFetchServer};
use crate::engine::bytes_scanned_exceeded;
use crate::erasure::is_erased_span;
use crate::fetcher::{FetchError, FetchStats, SegmentFetcher};
use crate::log_fetcher::{LogFetchError, LogQuery, LogSegmentFetcher};
use crate::span_fetcher::{SpanFetchError, SpanSegmentFetcher};

/// Resolves a shipped [`pb::SegmentIdentity`] back to the [`SegmentRef`] a
/// worker fetches. The production resolver reconstructs the
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
/// dispatched from). Stands in for the reconstruct-from-identity path.
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
    /// The RLOG-family fetcher (Logs, Alerts, Audit), wired via
    /// [`with_log_fetcher`](Self::with_log_fetcher). `None` (the default) means
    /// this worker cannot serve a distributed log fetch: a Logs/Alerts/Audit
    /// slice returns `Unsupported` so the coordinator falls back to local
    /// execution. Kept optional so the crate-internal `new` constructor (and the
    /// out-of-crate coordinator wiring that calls it) is unchanged; a worker that
    /// wants to serve logs opts in with the builder.
    log_fetcher: Option<LogSegmentFetcher>,
    /// The RSPAN span fetcher, wired via [`with_span_fetcher`](Self::with_span_fetcher).
    /// `None` (the default) means this worker cannot serve a distributed span
    /// fetch: a Spans slice returns `Unsupported` so the coordinator falls back
    /// to local execution. Kept optional for the same reason `log_fetcher` is:
    /// the crate-internal `new` constructor and its out-of-crate callers are
    /// unchanged; a worker that wants to serve spans opts in with the builder.
    span_fetcher: Option<SpanSegmentFetcher>,
    resolver: Arc<R>,
}

impl<R: SegmentResolver + 'static> SeriesFetchService<R> {
    pub fn new(fetcher: SegmentFetcher, resolver: Arc<R>) -> Self {
        SeriesFetchService {
            fetcher,
            log_fetcher: None,
            span_fetcher: None,
            resolver,
        }
    }

    /// Wires the RLOG-family fetch path (#284) onto this worker. Built over the
    /// same object store the metrics [`SegmentFetcher`] reads, so a Logs,
    /// Alerts, or Audit slice fetches the same pinned segments the resolver
    /// maps. Without this, those signals return `Unsupported` and the
    /// coordinator runs the query locally.
    #[must_use]
    pub fn with_log_fetcher(mut self, log_fetcher: LogSegmentFetcher) -> Self {
        self.log_fetcher = Some(log_fetcher);
        self
    }

    /// Wires the Spans fetch path (#285) onto this worker. Built over the same
    /// object store the metrics [`SegmentFetcher`] reads, so a Spans slice
    /// fetches the same pinned RSPAN segments the resolver maps. Without this, a
    /// Spans slice returns `Unsupported` and the coordinator runs the query
    /// locally.
    #[must_use]
    pub fn with_span_fetcher(mut self, span_fetcher: SpanSegmentFetcher) -> Self {
        self.span_fetcher = Some(span_fetcher);
        self
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
            // Pre-fetch/transport-shaped failures (version skew, malformed
            // request, a vanished pinned segment, a hard fetch error) carry no
            // accounting: nothing was scanned, or the query fails outright and
            // the aggregate is moot. Post-fetch outcomes that *did* spend
            // (budget trip, histogram fallback) build their own summary with
            // real accounting inside `run_slice_inner`.
            Err((code, message)) => vec![summary_frame(
                &QueryAccountingSnapshot::default(),
                0,
                0,
                code,
                message,
                &FetchStats::default(),
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

        // Decode the signal discriminant, then the signal-agnostic request
        // shape (tenant, matchers, erasure), and only then dispatch on the
        // signal. An unknown discriminant is malformed (BadData);
        // `signal_from_u32` is otherwise only exercised by codec round-trip
        // tests, this is its production caller. The per-signal dispatch is a
        // real match arm reached AFTER this decode (not the former pre-decode
        // blanket rejection), so a signal's own fetch path can build on the
        // decoded request rather than re-parsing it (#283).
        let signal = codec::signal_from_u32(request.signal)
            .map_err(|e| (pb::status::Code::BadData, e.to_string()))?;

        let tenant_hash =
            decode_tenant_hash(&request.tenant_hash).map_err(|m| (pb::status::Code::BadData, m))?;

        let matchers = codec::decode_matchers(request.matchers)
            .map_err(|e| (pb::status::Code::BadData, e.to_string()))?;
        let erasure = codec::decode_erasure(request.erasure);

        match signal {
            // Metrics keeps its exact path: resolve the pinned scope, fetch,
            // and encode scalar series. Unchanged from before #283.
            Signal::Metrics => {
                self.run_slice_metrics(
                    request.scope,
                    request.budgets,
                    tenant_hash,
                    matchers,
                    erasure,
                )
                .await
            }
            // The RLOG family (Logs, Alerts, Audit) fetches through the same
            // `LogSegmentFetcher`/`RlogReader` funnel the local logs/alerts/audit
            // paths use (#284). All three signals share one fetch path: they are
            // the same RLOG object family, distinguished only by the object-key
            // prefix the coordinator resolved, which the worker never re-derives
            // (it fetches the pinned segments the resolver maps). A worker with
            // no log fetcher wired returns Unsupported inside `run_slice_logs`,
            // so the coordinator falls back to local.
            Signal::Logs | Signal::Alerts | Signal::Audit => {
                self.run_slice_logs(
                    request.scope,
                    request.budgets,
                    tenant_hash,
                    matchers,
                    erasure,
                    request.window_start_ns,
                    request.window_end_ns,
                )
                .await
            }
            // Spans fetch through the RSPAN `SpanSegmentFetcher` funnel promoted
            // into this crate (#285), the same one a local `spans` SQL read uses,
            // so the per-span merged attribute view and the erasure exclusion are
            // byte-identical to a local read. A worker with no span fetcher wired
            // returns Unsupported inside `run_slice_spans`, so the coordinator
            // falls back to local.
            Signal::Spans => {
                self.run_slice_spans(
                    request.scope,
                    request.budgets,
                    tenant_hash,
                    matchers,
                    erasure,
                    request.window_start_ns,
                    request.window_end_ns,
                )
                .await
            }
            // Profiles has no distributed fetch path planned in this lane, so it
            // stays rejected exactly as every non-Metrics signal was before
            // #283: Unsupported, whole-query local fallback.
            Signal::Profiles => Err((
                pb::status::Code::Unsupported,
                format!("signal {signal:?} is not distributed"),
            )),
        }
    }

    /// The Metrics slice path: resolve the pinned scope to refs, fetch each
    /// segment's scalar (and histogram) series, enforce the per-slice
    /// bytes-scanned budget, apply erasure, and stream scalar frames ending in
    /// a terminal summary. Extracted from `run_slice_inner` unchanged so the
    /// per-signal dispatch there can call it as one arm; Metrics behavior is
    /// bit-for-bit what it was before #283.
    async fn run_slice_metrics(
        &self,
        scope: Option<pb::fetch_request::Scope>,
        budgets: Option<pb::Budgets>,
        tenant_hash: TenantHash,
        matchers: Vec<ravel_promql::LabelMatcher>,
        erasure: Vec<crate::erasure::ErasurePredicate>,
    ) -> Result<Vec<pb::FetchResponse>, (pb::status::Code, String)> {
        let identities = match scope {
            Some(pb::fetch_request::Scope::Pinned(pinned)) => pinned.segments,
            // Cross-cluster resolve scope is future work; fall back to local.
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

        // Per-slice bytes-scanned budget, enforced per completed segment
        // exactly as the local path does (ADR-0061 decision 1): `0` is the wire
        // sentinel for "no cap" (a real fetch never scans zero bytes).
        let byte_limit = match budgets.as_ref().map(|b| b.max_bytes_scanned) {
            Some(0) | None => ByteLimit::Unlimited,
            Some(max) => ByteLimit::Bounded(max),
        };

        // One fresh accounting handle per slice: the coordinator folds the
        // returned snapshot into the query's aggregate (ADR-0071).
        let accounting = QueryAccounting::new();
        let mut scalar = Vec::new();
        let mut any_histograms = false;
        let mut stats = FetchStats::default();
        for seg in &segments {
            let (seg_scalar, seg_stats, seg_hist) = self
                .fetcher
                .fetch_soa_and_histograms_accounted(tenant_hash, seg, &matchers, &accounting)
                .await
                .map_err(map_fetch_error)?;
            if !seg_hist.is_empty() {
                any_histograms = true;
            }
            stats.raw_f64_pages += seg_stats.raw_f64_pages;
            stats.raw_f64_bytes += seg_stats.raw_f64_bytes;
            scalar.push(seg_scalar);
            // Short-circuit the moment a completed segment fetch pushes this
            // slice over budget, matching the local per-segment check. The
            // terminal summary carries the accounting spent so far so the
            // coordinator folds the slice's real cost before failing the query
            // (never a lost double-spend); the message is the same typed
            // `TooManyBytesScanned` string the local path produces.
            if let Some(err) =
                bytes_scanned_exceeded(accounting.snapshot().total_s3_bytes(), byte_limit)
            {
                return Ok(vec![summary_frame(
                    &accounting.snapshot(),
                    0,
                    0,
                    pb::status::Code::BudgetExceeded,
                    err.to_string(),
                    &stats,
                )]);
            }
        }

        // Scalar-only distribution: hand a histogram-bearing slice back to the
        // coordinator's local fallback rather than return partial results. The
        // summary still carries this slice's accounting/stats so the
        // coordinator folds its spend before falling back to local (ADR-0071);
        // otherwise the histogram query would pay for this fetch twice and
        // report it once.
        if any_histograms {
            return Ok(vec![summary_frame(
                &accounting.snapshot(),
                0,
                0,
                pb::status::Code::Unsupported,
                "histogram series are not distributed yet".to_string(),
                &stats,
            )]);
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
            &stats,
        ));
        Ok(frames)
    }

    /// The RLOG-family slice path (Logs, Alerts, Audit): resolve the pinned
    /// scope to refs, fetch each segment through the production
    /// [`LogSegmentFetcher`] funnel (so the per-segment merged attribute view
    /// and the erasure exclusion are byte-identical to a local read), enforce
    /// the per-slice bytes-scanned budget, and stream one `LogRecordFrame` per
    /// decoded record ending in a terminal summary.
    ///
    /// Correctness under ADR-0052 resharding rests on segment self-containment,
    /// not slice atomicity: every RLOG segment embeds the resource+scope
    /// `stream_attrs` blob for the streams it carries, so a worker reading only
    /// this slice's segments produces the exact per-record view `RlogReader`
    /// produces locally, and applies erasure identically per segment, whether or
    /// not a stream's segments straddle two slices. The coordinator (see
    /// [`crate::distrib::merge_log_records`]) re-orders under a total order but
    /// never dedups (logs have no query-time dedup); this path never assumes a
    /// stream maps to one slice.
    #[allow(clippy::too_many_arguments)]
    async fn run_slice_logs(
        &self,
        scope: Option<pb::fetch_request::Scope>,
        budgets: Option<pb::Budgets>,
        tenant_hash: TenantHash,
        matchers: Vec<ravel_promql::LabelMatcher>,
        erasure: Vec<crate::erasure::ErasurePredicate>,
        window_start_ns: i64,
        window_end_ns: i64,
    ) -> Result<Vec<pb::FetchResponse>, (pb::status::Code, String)> {
        let Some(log_fetcher) = self.log_fetcher.as_ref() else {
            return Err((
                pb::status::Code::Unsupported,
                "distributed log fetch is not configured on this worker".to_string(),
            ));
        };

        // Matcher pushdown for logs has no defined `LogQuery` mapping in this
        // lane yet (that is the SQL lane / engine log-fetch wiring, out of
        // #284's scope). Ignoring matchers would under-filter, so fail closed to
        // the coordinator's local fallback rather than return a wrong result.
        if !matchers.is_empty() {
            return Err((
                pb::status::Code::Unsupported,
                "distributed log fetch does not support matcher pushdown yet".to_string(),
            ));
        }

        let identities = match scope {
            Some(pb::fetch_request::Scope::Pinned(pinned)) => pinned.segments,
            Some(pb::fetch_request::Scope::Resolve(_)) | None => {
                return Err((
                    pb::status::Code::Unsupported,
                    "resolve-scope slices are not supported yet".to_string(),
                ));
            }
        };

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

        let byte_limit = match budgets.as_ref().map(|b| b.max_bytes_scanned) {
            Some(0) | None => ByteLimit::Unlimited,
            Some(max) => ByteLimit::Bounded(max),
        };

        // The slice's event-time window is the ts range for the fetch. The
        // coordinator sets it to the envelope of this slice's pinned segments, a
        // superset of every one of them, so the `TsRange` filter drops no record
        // a local read (over the whole-snapshot window) would keep. Erasure is
        // threaded through the same funnel, applied per segment after decode.
        let query = LogQuery::new(window_start_ns, window_end_ns).with_erasure(erasure);

        let accounting = QueryAccounting::new();
        let mut records: Vec<ravel_logseg::LogRecord> = Vec::new();
        // Logs carry no raw-f64 page counters (a metric-path concept), so
        // `FetchStats` stays zero, exactly what a local log read reports.
        let stats = FetchStats::default();
        for seg in &segments {
            let out = log_fetcher
                .fetch_accounted_with_tenant(seg, tenant_hash, &query, &accounting)
                .await
                .map_err(map_log_fetch_error)?;
            if let Some(output) = out {
                records.extend(output.records);
            }
            // Per-segment bytes-scanned short-circuit, matching the metric path
            // and the local per-segment check. The terminal summary carries the
            // spend so far so the coordinator folds this slice's real cost
            // before failing the query.
            if let Some(err) =
                bytes_scanned_exceeded(accounting.snapshot().total_s3_bytes(), byte_limit)
            {
                return Ok(vec![summary_frame(
                    &accounting.snapshot(),
                    0,
                    0,
                    pb::status::Code::BudgetExceeded,
                    err.to_string(),
                    &stats,
                )]);
            }
        }

        let records_returned = records.len() as u64;
        let mut frames = Vec::with_capacity(records.len() + 1);
        for record in &records {
            frames.push(pb::FetchResponse {
                frame: Some(pb::fetch_response::Frame::LogRecord(
                    codec::encode_log_record(record),
                )),
            });
        }
        // The record count rides the summary's `series_returned` field (reused
        // per signal); `decode_log_slice_frames` reads it back as
        // `records_returned`.
        frames.push(summary_frame(
            &accounting.snapshot(),
            records_returned,
            0,
            pb::status::Code::Ok,
            String::new(),
            &stats,
        ));
        Ok(frames)
    }

    /// The Spans slice path (#285): resolve the pinned scope to refs, fetch each
    /// segment through the production [`SpanSegmentFetcher`] funnel (so the
    /// per-span merged attribute view is byte-identical to a local `spans`
    /// read), enforce the per-slice bytes-scanned budget, apply erasure per
    /// segment through the same `is_erased_span` funnel the local scan uses, and
    /// stream one `SpanFrame` per surviving span ending in a terminal summary.
    ///
    /// Correctness under ADR-0052 resharding rests on segment self-containment,
    /// not slice atomicity: RSPAN rebuilds a span's whole merged `attrs` map
    /// from one segment alone (the reader re-inserts the lifted `service.name`
    /// column), so a worker reading only this slice's segments produces the
    /// exact per-span view a local read produces, and applies erasure identically
    /// per segment, whether or not a trace's spans straddle two slices (a trace
    /// whose spans were written on both sides of a reshard activation does). The
    /// coordinator (see [`crate::distrib::merge_spans`]) re-orders under a total
    /// order but never dedups (spans have no query-time dedup); this path never
    /// assumes a trace maps to one slice.
    #[allow(clippy::too_many_arguments)]
    async fn run_slice_spans(
        &self,
        scope: Option<pb::fetch_request::Scope>,
        budgets: Option<pb::Budgets>,
        tenant_hash: TenantHash,
        matchers: Vec<ravel_promql::LabelMatcher>,
        erasure: Vec<crate::erasure::ErasurePredicate>,
        window_start_ns: i64,
        window_end_ns: i64,
    ) -> Result<Vec<pb::FetchResponse>, (pb::status::Code, String)> {
        let Some(span_fetcher) = self.span_fetcher.as_ref() else {
            return Err((
                pb::status::Code::Unsupported,
                "distributed span fetch is not configured on this worker".to_string(),
            ));
        };

        // Span matcher pushdown (service_name/name/duration/status) has no
        // defined mapping in this lane yet (that is the SQL lane / spans pushdown,
        // out of #285's scope). Ignoring matchers would under-filter, so fail
        // closed to the coordinator's local fallback rather than a wrong result,
        // exactly as the log path does.
        if !matchers.is_empty() {
            return Err((
                pb::status::Code::Unsupported,
                "distributed span fetch does not support matcher pushdown yet".to_string(),
            ));
        }

        let identities = match scope {
            Some(pb::fetch_request::Scope::Pinned(pinned)) => pinned.segments,
            Some(pb::fetch_request::Scope::Resolve(_)) | None => {
                return Err((
                    pb::status::Code::Unsupported,
                    "resolve-scope slices are not supported yet".to_string(),
                ));
            }
        };

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

        let byte_limit = match budgets.as_ref().map(|b| b.max_bytes_scanned) {
            Some(0) | None => ByteLimit::Unlimited,
            Some(max) => ByteLimit::Bounded(max),
        };

        // The slice's event-time window is the ts range for the scan. The
        // coordinator sets it to the envelope of this slice's pinned segments, a
        // superset of every one of them, so the interval-overlap filter drops no
        // span a local read (over the whole-snapshot window) would keep. A bare
        // time-range query with no trace_id, no duration/status prune, and no
        // bloom predicates: matcher pushdown fell back above, so nothing narrows
        // the scan beyond the window.
        let query = SpanQuery::ts_range(window_start_ns, window_end_ns);

        let accounting = QueryAccounting::new();
        let mut spans: Vec<crate::span_fetcher::SpanRow> = Vec::new();
        // Spans carry no raw-f64 page counters (a metric-path concept), so
        // `FetchStats` stays zero, exactly what a local span read reports.
        let stats = FetchStats::default();
        for seg in &segments {
            let out = span_fetcher
                .fetch_accounted(seg, tenant_hash, &query, None, None, &[], &accounting)
                .await
                .map_err(map_span_fetch_error)?;
            if let Some(output) = out {
                spans.extend(output.records);
            }
            // Per-segment bytes-scanned short-circuit, matching the metric and
            // log paths and the local per-segment check. The terminal summary
            // carries the spend so far so the coordinator folds this slice's real
            // cost before failing the query.
            if let Some(err) =
                bytes_scanned_exceeded(accounting.snapshot().total_s3_bytes(), byte_limit)
            {
                return Ok(vec![summary_frame(
                    &accounting.snapshot(),
                    0,
                    0,
                    pb::status::Code::BudgetExceeded,
                    err.to_string(),
                    &stats,
                )]);
            }
        }

        // Selective-erasure exclusion, applied post-decode exactly as the local
        // `spans` scan applies it (ADR-0064 decision 2, `is_erased_span`): the
        // coordinator does not re-apply, so worker-side application must match
        // the local rule. Correct under a straddling trace because each segment
        // is self-contained.
        if !erasure.is_empty() {
            spans
                .retain(|row| !is_erased_span(&row.record.attrs, row.record.start_ts_ns, &erasure));
        }

        let spans_returned = spans.len() as u64;
        let mut frames = Vec::with_capacity(spans.len() + 1);
        for row in &spans {
            frames.push(pb::FetchResponse {
                frame: Some(pb::fetch_response::Frame::Span(codec::encode_span_frame(
                    row,
                ))),
            });
        }
        // The span count rides the summary's `series_returned` field (reused per
        // signal); `decode_span_slice_frames` reads it back as `spans_returned`.
        frames.push(summary_frame(
            &accounting.snapshot(),
            spans_returned,
            0,
            pb::status::Code::Ok,
            String::new(),
            &stats,
        ));
        Ok(frames)
    }
}

/// The stream type the generated trait requires: a boxed stream of already-
/// built frames. The slice is fetched eagerly (it does not stream
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
    stats: &FetchStats,
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
            raw_f64_pages: stats.raw_f64_pages,
            raw_f64_bytes: stats.raw_f64_bytes,
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

/// Maps an RLOG fetch-path error to the worker's typed status, mirroring
/// [`map_fetch_error`]: a vanished object is a snapshot invalidation
/// (retryable), a transient store error is unavailable, and a corrupt object is
/// terminal.
fn map_log_fetch_error(err: LogFetchError) -> (pb::status::Code, String) {
    match err {
        LogFetchError::Store {
            source: ravel_object_store::StoreError::NotFound,
            ..
        } => (pb::status::Code::SnapshotInvalidated, err.to_string()),
        LogFetchError::Store { .. } => (pb::status::Code::Unavailable, err.to_string()),
        LogFetchError::Corrupt { .. } => (pb::status::Code::Corrupt, err.to_string()),
    }
}

/// Maps a span fetch-path error to the worker's typed status, mirroring
/// [`map_fetch_error`]/[`map_log_fetch_error`]: a vanished object is a snapshot
/// invalidation (retryable), a transient store error is unavailable, a corrupt
/// object is terminal, and an object belonging to another tenant is terminal
/// corruption of the coordinator's contract (a pinned segment must be this
/// tenant's), never a retry or a silent skip.
fn map_span_fetch_error(err: SpanFetchError) -> (pb::status::Code, String) {
    match err {
        SpanFetchError::Store {
            source: ravel_object_store::StoreError::NotFound,
            ..
        } => (pb::status::Code::SnapshotInvalidated, err.to_string()),
        SpanFetchError::Store { .. } => (pb::status::Code::Unavailable, err.to_string()),
        SpanFetchError::Corrupt { .. } => (pb::status::Code::Corrupt, err.to_string()),
        SpanFetchError::TenantMismatch { .. } => (pb::status::Code::Corrupt, err.to_string()),
    }
}
