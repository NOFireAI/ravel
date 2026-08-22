//! Distributed read fan-out core (ADR-0071).
//!
//! The coordinator resolves ONE pinned snapshot, then (when the cost gate
//! trips) [`Distributed::fetch`] partitions it into shard-major slices,
//! dispatches each to a worker through a [`SliceFetcher`], and returns the
//! decoded per-slice results in the exact in-memory shapes the local fetch
//! produces. The engine's merge layer (`merge_soa_runs`) then runs unchanged;
//! because that merge is order-insensitive over the flat pool of decoded runs,
//! the coordinator-merged distributed result is bit-for-bit identical to the
//! local-path result. See the acceptance test
//! `distributed_merge_equals_local_bitwise`.
//!
//! Distribution is **off by default**: a [`QueryEngine`](crate::QueryEngine)
//! with no distributed context (the default) runs the local path untouched.
//! The seam is [`QueryEngine::with_distributed`](crate::QueryEngine::with_distributed).

pub mod client;
pub mod codec;
pub mod federation;
pub mod partition;
pub mod pushdown;
pub mod service;

pub use federation::{DEFAULT_REMOTE_SOFT_TIMEOUT, Federation, FederationOutcome, RemoteCluster};
pub use pushdown::{all_hours_in_one_stable_generation, is_pushdown_eligible};

#[cfg(test)]
mod tests;

/// Generated `SeriesFetch` gRPC stubs (ADR-0071). The message types are the
/// frozen `ravel_proto::queryfrag::v1` ones (reused via `extern_path` in
/// `build.rs`); only the service client/server live here.
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/ravel.queryfrag.svc.v1.rs"));
}

use std::collections::HashSet;
use std::sync::Arc;

use futures::{StreamExt, stream};
use ravel_catalog::{SegmentRef, Snapshot};
use ravel_logseg::LogRecord;
use ravel_promql::LabelMatcher;
use ravel_proto::queryfrag::v1 as pb;
use ravel_types::accounting::{QueryAccounting, QueryAccountingSnapshot};
use ravel_types::logstream::canonical_attr_bytes;
use ravel_types::{SeriesId, Signal, TenantHash};

use crate::config::{ByteLimit, EngineConfig};
use crate::distrib::client::{DistribError, SliceFetcher, SliceLogResponse, SliceSpanResponse};
use crate::distrib::partition::{DistribThresholds, partition_snapshot};
use crate::engine::bytes_scanned_exceeded;
use crate::erasure::ErasurePredicate;
use crate::error::QueryError;
use crate::fetcher::{FetchError, FetchStats, FetchedHistogramSeries, FetchedSeriesSoa};
use crate::span_fetcher::SpanRow;

pub use partition::{DISTRIBUTE_MIN_SEGMENTS, DISTRIBUTE_MIN_STORE_BYTES};

/// The scalar/histogram/stats triple the local fetch returns, and the shape
/// [`Distributed::fetch`] reconstructs from slices so the engine's merge step
/// is identical for local and distributed queries.
pub type FetchedTriple = (
    Vec<Vec<FetchedSeriesSoa>>,
    FetchStats,
    Vec<Vec<FetchedHistogramSeries>>,
);

/// The closed event-time envelope `[min, max]` of a slice's pinned segments,
/// carried on the fragment request so the worker resolves its interim
/// content-hash resolver over just this window instead of
/// the whole timestamp domain.
///
/// The envelope contains every pinned segment's own `[min_event_ts_ns,
/// max_event_ts_ns]` range, so a `Catalog::resolve` bounded to it still returns
/// every pinned segment (resolve returns every segment whose events overlap the
/// window): the resolved snapshot stays a superset of the dispatched pins and
/// single-snapshot isolation is preserved. `partition_snapshot` never emits an
/// empty slice; the empty fallback keeps the full window (the old behavior) so
/// the superset property holds unconditionally.
fn slice_event_window(segments: &[SegmentRef]) -> (i64, i64) {
    if segments.is_empty() {
        return (i64::MIN, i64::MAX);
    }
    let mut start = i64::MAX;
    let mut end = i64::MIN;
    for seg in segments {
        start = start.min(seg.min_event_ts_ns);
        end = end.max(seg.max_event_ts_ns);
    }
    (start, end)
}

/// The engine's distributed-execution context: the cost gate/fan-out width and
/// the slice-fetcher seam. Held in an `Option` on the engine; `None` (the
/// default) means fully local execution.
pub struct Distributed {
    thresholds: DistribThresholds,
    fetcher: Arc<dyn SliceFetcher>,
}

impl Distributed {
    /// Builds a distributed context around a slice fetcher and its thresholds.
    pub fn new(fetcher: Arc<dyn SliceFetcher>, thresholds: DistribThresholds) -> Self {
        Distributed {
            fetcher,
            thresholds,
        }
    }

    /// The cost gate and fan-out width.
    pub fn thresholds(&self) -> &DistribThresholds {
        &self.thresholds
    }

    /// Partitions the snapshot, dispatches one request per slice, and folds the
    /// results incrementally as each slice completes (so a budget trip or a
    /// hard error short-circuits without draining the rest).
    ///
    /// `partial_aggregate` (ADR-0103) is `Some` only for an eligible instant
    /// `count_over_time` query: the coordinator asks every slice for a
    /// per-series precomputed partial over the request's reduction window
    /// instead of raw runs, and the returned `Vec<codec::PartialAggregate>` is
    /// the collected result (empty when the field is `None`, today's every real
    /// query). `None` is byte-identical to the pre-ADR-0103 behavior.
    ///
    /// Returns:
    /// - `Ok(Some((triple, partials)))` when every slice succeeded: the
    ///   coordinator merges `triple` exactly as the local path merges its own
    ///   fetch, and `partials` carries any collected worker partials.
    /// - `Ok(None)` when the query must fall back to fully local execution: a
    ///   worker reported [`pb::status::Code::Unsupported`] (version skew or a
    ///   resolve-scope slice). ADR-0071's silent fallback, never an error to the
    ///   user. As of `PROTOCOL_VERSION` 3 a native-histogram or run-merged scalar
    ///   slice is served over the wire, not refused (ADR-0096 decision 3 step 4).
    /// - `Err(QueryError::Fetch(Store { NotFound }))` when a slice reported
    ///   [`pb::status::Code::SnapshotInvalidated`]: mapped to the exact error
    ///   the local path raises for a vanished segment, so the engine's existing
    ///   `resolve_snapshot_with_retry` re-resolves the snapshot and re-dispatches
    ///   the whole query once (not once per slice).
    /// - `Err(..)` for a terminal slice failure (corrupt segment, transport,
    ///   framing) or a budget overrun the coordinator re-enforces.
    #[allow(clippy::too_many_arguments)]
    pub async fn fetch(
        &self,
        tenant_hash: TenantHash,
        signal: Signal,
        snapshot: &Snapshot,
        matchers: &[LabelMatcher],
        erasure: &[ErasurePredicate],
        accounting: &QueryAccounting,
        config: &EngineConfig,
        deadline_unix_ns: i64,
        partial_aggregate: Option<pb::PartialAggregateRequest>,
    ) -> Result<Option<(FetchedTriple, Vec<codec::PartialAggregate>)>, QueryError> {
        let slices = partition_snapshot(snapshot, self.thresholds.max_parallel_slices);
        if slices.is_empty() {
            // Nothing to fetch: an empty snapshot merges to an empty result
            // identically whether local or distributed.
            return Ok(Some((
                (Vec::new(), FetchStats::default(), Vec::new()),
                Vec::new(),
            )));
        }

        let encoded_matchers = codec::encode_matchers(matchers);
        let encoded_erasure = codec::encode_erasure(erasure);
        let budgets = encode_budgets(config);
        let tenant_bytes = tenant_hash.0.to_vec();
        let signal_disc = codec::signal_to_u32(signal);
        // One query id for the whole query (ADR-0071 amendment, decision 2): the
        // server-side coordinator mints one capability per query from this id and
        // the absolute deadline, and every slice of the query, including a
        // re-dispatch, carries the same id so the same capability authorizes them
        // all. Derived deterministically from the query's own identity so it is
        // stable across the slices of one fetch without threading a random source
        // through the engine; the value only needs to be a stable 16 bytes the
        // mint and verify sides agree on. `deadline_unix_ns` is the query's
        // absolute deadline (the same one the engine enforces), which the mint
        // uses as the capability expiry.
        let query_id = query_id_bytes(&tenant_bytes, signal_disc, deadline_unix_ns, snapshot);

        let concurrency = self.thresholds.max_parallel_slices.max(1);
        let mut stream = stream::iter(slices)
            .map(|slice| {
                // Carry the event-time envelope of this slice's pinned segments
                // The worker resolves its interim
                // content-hash resolver over exactly this window instead of the
                // whole timestamp domain, so a self-mapped or dispatched slice
                // no longer pays a whole-history catalog resolve on the query
                // critical path. The envelope is a superset of every pinned
                // segment's own event range by construction, so a resolve
                // bounded to it still finds every pinned content hash (a
                // `Catalog::resolve` over a window returns every segment whose
                // events overlap that window); single-snapshot isolation is
                // preserved exactly.
                let (window_start_ns, window_end_ns) = slice_event_window(&slice.segments);
                let request = pb::FetchRequest {
                    protocol_version: codec::PROTOCOL_VERSION,
                    query_id: query_id.to_vec(),
                    tenant_hash: tenant_bytes.clone(),
                    signal: signal_disc,
                    scope: Some(pb::fetch_request::Scope::Pinned(pb::PinnedScope {
                        segments: slice
                            .segments
                            .iter()
                            .map(codec::encode_segment_identity)
                            .collect(),
                    })),
                    matchers: encoded_matchers.clone(),
                    window_start_ns,
                    window_end_ns,
                    budgets: Some(budgets),
                    deadline_unix_ns,
                    erasure: encoded_erasure.clone(),
                    trace_context: String::new(),
                    // The Pinned fragment capability (ADR-0071 amendment,
                    // decision 2) is minted per query from this request's
                    // query_id/tenant/signal/deadline and attached at dispatch
                    // time by the server-side SliceFetcher
                    // (RoutingSliceFetcher::mint_capability); the crate-internal
                    // builder leaves the bytes empty. A coordinator-local slice
                    // needs no capability.
                    fragment_capability: Vec::new(),
                    // ADR-0103 aggregation pushdown: `Some` only for an eligible
                    // instant `count_over_time` query (the coordinator's
                    // per-matcher-set decision in `engine::prefetch`), in which
                    // case every slice of this matcher set carries the identical
                    // request so each worker computes a per-series count over
                    // the same reduction window. `None` (every other query) asks
                    // for raw runs, byte-identical to the pre-ADR-0103 builder.
                    // The request is `Copy` (all-scalar fields), so each
                    // per-slice build copies it rather than moving the shared
                    // value out of this `FnMut`.
                    partial_aggregate,
                };
                let fetcher = Arc::clone(&self.fetcher);
                async move { fetcher.fetch(request).await }
            })
            .buffer_unordered(concurrency);

        // Process slices incrementally as each completes, so the coordinator's
        // budget re-enforcement short-circuits at slice granularity: a hard
        // error or a budget trip returns immediately, dropping `stream`, which
        // stops polling and cancels the in-flight slice futures (they are
        // polled inline by `buffer_unordered`, never `tokio::spawn`ed, so there
        // is nothing to leak). Soft outcomes (a snapshot invalidation or an
        // Unsupported fallback) are recorded and draining continues, so a later
        // hard error still dominates them (precedence: hard > invalidated >
        // unsupported), matching the former collect-then-classify behavior.
        let mut distinct: HashSet<SeriesId> = HashSet::new();
        let mut distinct_hist: HashSet<SeriesId> = HashSet::new();
        let mut per_slice: Vec<Vec<FetchedSeriesSoa>> = Vec::new();
        let mut per_slice_hist: Vec<Vec<FetchedHistogramSeries>> = Vec::new();
        // ADR-0103 collected worker partials, and a HashSet DEDICATED to
        // detecting a duplicate series id across `.partials` entries (possibly
        // from different slices/workers). This is a distinct mechanism from the
        // `distinct`/`distinct_hist` cap-enforcement sets above: a legitimate
        // cap truncation is not a duplicate-series bug, so the two must never
        // share a set. Under decision 1's eligibility gate every series lives on
        // exactly one worker, so a repeat means the gate was violated and the
        // query must fail closed (below), never silently keep one of two values.
        let mut collected_partials: Vec<codec::PartialAggregate> = Vec::new();
        let mut partial_series: HashSet<SeriesId> = HashSet::new();
        // Running fold of the per-slice accounting snapshots (ADR-0071),
        // combined via the saturating merge so a worker near `u64::MAX` clamps
        // rather than wrapping past the bytes-scanned budget. This is the
        // coordinator's own view for the incremental budget check; each slice
        // is also folded into the live `accounting` handle so the query's
        // reported cost reflects every dispatched fetch even when the query
        // then fails or falls back.
        let mut running = QueryAccountingSnapshot::default();
        let mut stats = FetchStats::default();
        let mut invalidated = false;
        let mut unsupported = false;

        while let Some(result) = stream.next().await {
            let response = result.map_err(distrib_error)?;
            match response.status {
                pb::status::Code::Ok => {
                    fold_slice(accounting, &mut running, &mut stats, &response);
                    // Series cap re-enforcement: a lying worker cannot overrun
                    // the query's distinct-series cap. The sample cap is
                    // enforced downstream by the k-way merge, after
                    // cross-segment dedup, which is where a sample total is
                    // meaningful.
                    for fs in &response.scalar {
                        if !distinct.contains(&fs.series_id) && distinct.len() >= config.max_series
                        {
                            return Err(QueryError::TooManySeries {
                                count: distinct.len() + 1,
                                max: config.max_series,
                            });
                        }
                        distinct.insert(fs.series_id);
                    }
                    // The identical distinct-series re-enforcement for histogram
                    // series (ADR-0096 decision 3 step 4): a lying worker cannot
                    // overrun the query's distinct-series cap by streaming
                    // histogram runs any more than scalar ones, and a histogram
                    // series is heavier per series than a scalar one, so the
                    // collection-time memory bound matters at least as much here.
                    // `merge_histogram_soa_runs` re-enforces `max_series` once
                    // more at the final merge (its own `by_series` map is capped
                    // the same way `merge_soa_runs` caps scalars), so this is
                    // defense-in-depth layered on that, not a replacement.
                    for hs in &response.histogram {
                        if !distinct_hist.contains(&hs.series_id)
                            && distinct_hist.len() >= config.max_series
                        {
                            return Err(QueryError::TooManySeries {
                                count: distinct_hist.len() + 1,
                                max: config.max_series,
                            });
                        }
                        distinct_hist.insert(hs.series_id);
                    }
                    // Bytes-scanned cap re-enforcement over the folded total so
                    // far, so a distributed query is bounded as tightly as a
                    // local one even if a worker under-reports its own trip.
                    if let Some(err) =
                        bytes_scanned_exceeded(running.total_s3_bytes(), config.max_bytes_scanned)
                    {
                        return Err(err);
                    }
                    // ADR-0103: fold this slice's worker partials into the
                    // per-fetch collection, hard-erroring on any repeated series
                    // id (across this or any prior slice). Empty for a raw fetch
                    // (`partial_aggregate` was `None`), so this is inert on every
                    // query shape other than an eligible `count_over_time`.
                    for pa in response.partials {
                        if !partial_series.insert(pa.series_id) {
                            return Err(QueryError::DuplicatePushdownSeries {
                                series_id: pa.series_id.to_hex(),
                            });
                        }
                        collected_partials.push(pa);
                    }
                    per_slice.push(response.scalar);
                    per_slice_hist.push(response.histogram);
                }
                pb::status::Code::SnapshotInvalidated => invalidated = true,
                pb::status::Code::Unsupported => {
                    // Fold the slice's spend before the fallback: the whole
                    // query re-runs locally, so without this the already-paid
                    // remote fetch would be silently dropped from the reported
                    // cost (a histogram-bearing query would pay ~2x, report 1x).
                    fold_slice(accounting, &mut running, &mut stats, &response);
                    unsupported = true;
                }
                pb::status::Code::BudgetExceeded => {
                    // Fold the slice's real spend, then fail with the same
                    // typed error the local path raises. The worker tripped on
                    // the full per-slice budget (which equals the query's), so
                    // the folded total is at or over it.
                    fold_slice(accounting, &mut running, &mut stats, &response);
                    return Err(bytes_scanned_exceeded(
                        running.total_s3_bytes(),
                        config.max_bytes_scanned,
                    )
                    .unwrap_or_else(|| QueryError::Distrib {
                        reason: format!("slice tripped its budget: {}", response.status_message),
                    }));
                }
                pb::status::Code::Corrupt => {
                    // ADR-0071 deliverable 3: a worker-reported corruption is a
                    // real defect, terminal and typed. The SliceFetcher never
                    // retries or falls back around it (that would mask the
                    // corruption behind a possibly-clean local read), so it
                    // arrives here directly and fails the query typed.
                    return Err(QueryError::Distrib {
                        reason: format!(
                            "slice reported a corrupt segment: {}",
                            response.status_message
                        ),
                    });
                }
                pb::status::Code::Unavailable => {
                    // Terminal only. The RoutingSliceFetcher already
                    // re-dispatched to the next rendezvous worker and then ran
                    // the slice coordinator-local (ADR-0071 deliverable 1);
                    // reaching here means every attempt, local included, was
                    // unavailable. Fail typed, never with a partial merge.
                    return Err(QueryError::Distrib {
                        reason: format!(
                            "slice unavailable after re-dispatch and local execution: {}",
                            response.status_message
                        ),
                    });
                }
                other => {
                    return Err(QueryError::Distrib {
                        reason: format!("slice returned {other:?}: {}", response.status_message),
                    });
                }
            }
        }

        if invalidated {
            // Map to the same error the local fetch raises for a vanished
            // segment, so `resolve_snapshot_with_retry` handles it identically.
            // Accounting folded above is retained on the live handle: the local
            // path likewise accumulates the cost of a fetch that then hits a
            // NotFound and retries.
            return Err(QueryError::Fetch(FetchError::Store {
                key: "distributed-slice".to_string(),
                source: ravel_object_store::StoreError::NotFound,
            }));
        }
        if unsupported {
            return Ok(None);
        }

        Ok(Some((
            (per_slice, stats, per_slice_hist),
            collected_partials,
        )))
    }

    /// Partitions the snapshot, dispatches one RLOG-family (Logs, Alerts, Audit)
    /// slice request per slice, and merges the decoded records into the single
    /// cross-segment total order a local multi-segment read produces (#284). The
    /// log sibling of [`fetch`](Self::fetch).
    ///
    /// Returns:
    /// - `Ok(Some(records))` when every slice succeeded: the coordinator-ordered
    ///   record set (no dedup -- see [`merge_log_records`]), bit-identical to a
    ///   local `LogSegmentFetcher` read over the same segments merged under the
    ///   same rule.
    /// - `Ok(None)` for whole-query local fallback: a worker reported
    ///   `Unsupported` (version skew, a worker with no log fetcher wired, matcher
    ///   pushdown, or a resolve-scope slice).
    /// - `Err(QueryError::Fetch(Store { NotFound }))` for a `SnapshotInvalidated`
    ///   slice, so the engine re-resolves and re-dispatches once.
    /// - `Err(..)` for a terminal slice failure or a re-enforced budget overrun.
    ///
    /// This machinery is correct and tested, but no caller in the engine or SQL
    /// layer dispatches a non-Metrics distributed fetch yet
    /// (`fetch_samples_and_histograms_maybe_distributed` only ever passes
    /// `Signal::Metrics`); wiring a real log caller is out of #284's scope.
    #[allow(clippy::too_many_arguments)]
    pub async fn fetch_logs(
        &self,
        tenant_hash: TenantHash,
        signal: Signal,
        snapshot: &Snapshot,
        matchers: &[LabelMatcher],
        erasure: &[ErasurePredicate],
        accounting: &QueryAccounting,
        config: &EngineConfig,
        deadline_unix_ns: i64,
    ) -> Result<Option<Vec<LogRecord>>, QueryError> {
        let slices = partition_snapshot(snapshot, self.thresholds.max_parallel_slices);
        if slices.is_empty() {
            // An empty snapshot merges to an empty record set identically whether
            // local or distributed.
            return Ok(Some(Vec::new()));
        }

        let encoded_matchers = codec::encode_matchers(matchers);
        let encoded_erasure = codec::encode_erasure(erasure);
        let budgets = encode_budgets(config);
        let tenant_bytes = tenant_hash.0.to_vec();
        let signal_disc = codec::signal_to_u32(signal);
        let query_id = query_id_bytes(&tenant_bytes, signal_disc, deadline_unix_ns, snapshot);

        let concurrency = self.thresholds.max_parallel_slices.max(1);
        let mut stream = stream::iter(slices)
            .map(|slice| {
                let (window_start_ns, window_end_ns) = slice_event_window(&slice.segments);
                let request = pb::FetchRequest {
                    protocol_version: codec::PROTOCOL_VERSION,
                    query_id: query_id.to_vec(),
                    tenant_hash: tenant_bytes.clone(),
                    signal: signal_disc,
                    scope: Some(pb::fetch_request::Scope::Pinned(pb::PinnedScope {
                        segments: slice
                            .segments
                            .iter()
                            .map(codec::encode_segment_identity)
                            .collect(),
                    })),
                    matchers: encoded_matchers.clone(),
                    window_start_ns,
                    window_end_ns,
                    budgets: Some(budgets),
                    deadline_unix_ns,
                    erasure: encoded_erasure.clone(),
                    trace_context: String::new(),
                    fragment_capability: Vec::new(),
                    // Pushdown is metrics-only (ADR-0103); a log slice never
                    // carries an aggregate request.
                    partial_aggregate: None,
                };
                let fetcher = Arc::clone(&self.fetcher);
                async move { fetcher.fetch_logs(request).await }
            })
            .buffer_unordered(concurrency);

        // Collect each slice's records into a flat per-slice pool, folding its
        // accounting into the query's aggregate and re-enforcing the
        // bytes-scanned budget over the running total, exactly as the metrics
        // path does. Precedence on soft outcomes matches `fetch`: a later hard
        // error dominates an invalidation, which dominates an Unsupported
        // fallback.
        let mut per_slice: Vec<Vec<LogRecord>> = Vec::new();
        let mut running = QueryAccountingSnapshot::default();
        let mut invalidated = false;
        let mut unsupported = false;

        while let Some(result) = stream.next().await {
            let response = result.map_err(distrib_error)?;
            match response.status {
                pb::status::Code::Ok => {
                    fold_log_slice(accounting, &mut running, &response);
                    if let Some(err) =
                        bytes_scanned_exceeded(running.total_s3_bytes(), config.max_bytes_scanned)
                    {
                        return Err(err);
                    }
                    per_slice.push(response.records);
                }
                pb::status::Code::SnapshotInvalidated => invalidated = true,
                pb::status::Code::Unsupported => {
                    fold_log_slice(accounting, &mut running, &response);
                    unsupported = true;
                }
                pb::status::Code::BudgetExceeded => {
                    fold_log_slice(accounting, &mut running, &response);
                    return Err(bytes_scanned_exceeded(
                        running.total_s3_bytes(),
                        config.max_bytes_scanned,
                    )
                    .unwrap_or_else(|| QueryError::Distrib {
                        reason: format!("slice tripped its budget: {}", response.status_message),
                    }));
                }
                pb::status::Code::Corrupt => {
                    return Err(QueryError::Distrib {
                        reason: format!(
                            "slice reported a corrupt segment: {}",
                            response.status_message
                        ),
                    });
                }
                pb::status::Code::Unavailable => {
                    return Err(QueryError::Distrib {
                        reason: format!(
                            "slice unavailable after re-dispatch and local execution: {}",
                            response.status_message
                        ),
                    });
                }
                other => {
                    return Err(QueryError::Distrib {
                        reason: format!("slice returned {other:?}: {}", response.status_message),
                    });
                }
            }
        }

        if invalidated {
            return Err(QueryError::Fetch(FetchError::Store {
                key: "distributed-slice".to_string(),
                source: ravel_object_store::StoreError::NotFound,
            }));
        }
        if unsupported {
            return Ok(None);
        }

        Ok(Some(merge_log_records(per_slice)))
    }

    /// Partitions the snapshot, dispatches one Spans slice request per slice, and
    /// merges the decoded spans into the single cross-segment total order a local
    /// multi-segment read produces (#285). The span sibling of
    /// [`fetch_logs`](Self::fetch_logs).
    ///
    /// Returns:
    /// - `Ok(Some(spans))` when every slice succeeded: the coordinator-ordered
    ///   span set (no dedup -- see [`merge_spans`]), the same multiset a local
    ///   `SpanSegmentFetcher` read over the same segments produces, merged under
    ///   the same total order.
    /// - `Ok(None)` for whole-query local fallback: a worker reported
    ///   `Unsupported` (version skew, a worker with no span fetcher wired,
    ///   matcher pushdown, or a resolve-scope slice).
    /// - `Err(QueryError::Fetch(Store { NotFound }))` for a `SnapshotInvalidated`
    ///   slice, so the engine re-resolves and re-dispatches once.
    /// - `Err(..)` for a terminal slice failure or a re-enforced budget overrun.
    ///
    /// This machinery is correct and tested, but no caller in the engine or SQL
    /// layer dispatches a `Signal::Spans` distributed fetch yet
    /// (`fetch_samples_and_histograms_maybe_distributed` only ever passes
    /// `Signal::Metrics`, and the SQL span scan is single-process); wiring a real
    /// span caller is a later task's scope.
    #[allow(clippy::too_many_arguments)]
    pub async fn fetch_spans(
        &self,
        tenant_hash: TenantHash,
        signal: Signal,
        snapshot: &Snapshot,
        matchers: &[LabelMatcher],
        erasure: &[ErasurePredicate],
        accounting: &QueryAccounting,
        config: &EngineConfig,
        deadline_unix_ns: i64,
    ) -> Result<Option<Vec<SpanRow>>, QueryError> {
        let slices = partition_snapshot(snapshot, self.thresholds.max_parallel_slices);
        if slices.is_empty() {
            // An empty snapshot merges to an empty span set identically whether
            // local or distributed.
            return Ok(Some(Vec::new()));
        }

        let encoded_matchers = codec::encode_matchers(matchers);
        let encoded_erasure = codec::encode_erasure(erasure);
        let budgets = encode_budgets(config);
        let tenant_bytes = tenant_hash.0.to_vec();
        let signal_disc = codec::signal_to_u32(signal);
        let query_id = query_id_bytes(&tenant_bytes, signal_disc, deadline_unix_ns, snapshot);

        let concurrency = self.thresholds.max_parallel_slices.max(1);
        let mut stream = stream::iter(slices)
            .map(|slice| {
                let (window_start_ns, window_end_ns) = slice_event_window(&slice.segments);
                let request = pb::FetchRequest {
                    protocol_version: codec::PROTOCOL_VERSION,
                    query_id: query_id.to_vec(),
                    tenant_hash: tenant_bytes.clone(),
                    signal: signal_disc,
                    scope: Some(pb::fetch_request::Scope::Pinned(pb::PinnedScope {
                        segments: slice
                            .segments
                            .iter()
                            .map(codec::encode_segment_identity)
                            .collect(),
                    })),
                    matchers: encoded_matchers.clone(),
                    window_start_ns,
                    window_end_ns,
                    budgets: Some(budgets),
                    deadline_unix_ns,
                    erasure: encoded_erasure.clone(),
                    trace_context: String::new(),
                    fragment_capability: Vec::new(),
                    // Pushdown is metrics-only (ADR-0103); a span slice never
                    // carries an aggregate request.
                    partial_aggregate: None,
                };
                let fetcher = Arc::clone(&self.fetcher);
                async move { fetcher.fetch_spans(request).await }
            })
            .buffer_unordered(concurrency);

        // Collect each slice's spans into a flat per-slice pool, folding its
        // accounting into the query's aggregate and re-enforcing the
        // bytes-scanned budget over the running total, exactly as the logs path
        // does. Precedence on soft outcomes matches `fetch`: a later hard error
        // dominates an invalidation, which dominates an Unsupported fallback.
        let mut per_slice: Vec<Vec<SpanRow>> = Vec::new();
        let mut running = QueryAccountingSnapshot::default();
        let mut invalidated = false;
        let mut unsupported = false;

        while let Some(result) = stream.next().await {
            let response = result.map_err(distrib_error)?;
            match response.status {
                pb::status::Code::Ok => {
                    fold_span_slice(accounting, &mut running, &response);
                    if let Some(err) =
                        bytes_scanned_exceeded(running.total_s3_bytes(), config.max_bytes_scanned)
                    {
                        return Err(err);
                    }
                    per_slice.push(response.spans);
                }
                pb::status::Code::SnapshotInvalidated => invalidated = true,
                pb::status::Code::Unsupported => {
                    fold_span_slice(accounting, &mut running, &response);
                    unsupported = true;
                }
                pb::status::Code::BudgetExceeded => {
                    fold_span_slice(accounting, &mut running, &response);
                    return Err(bytes_scanned_exceeded(
                        running.total_s3_bytes(),
                        config.max_bytes_scanned,
                    )
                    .unwrap_or_else(|| QueryError::Distrib {
                        reason: format!("slice tripped its budget: {}", response.status_message),
                    }));
                }
                pb::status::Code::Corrupt => {
                    return Err(QueryError::Distrib {
                        reason: format!(
                            "slice reported a corrupt segment: {}",
                            response.status_message
                        ),
                    });
                }
                pb::status::Code::Unavailable => {
                    return Err(QueryError::Distrib {
                        reason: format!(
                            "slice unavailable after re-dispatch and local execution: {}",
                            response.status_message
                        ),
                    });
                }
                other => {
                    return Err(QueryError::Distrib {
                        reason: format!("slice returned {other:?}: {}", response.status_message),
                    });
                }
            }
        }

        if invalidated {
            return Err(QueryError::Fetch(FetchError::Store {
                key: "distributed-slice".to_string(),
                source: ravel_object_store::StoreError::NotFound,
            }));
        }
        if unsupported {
            return Ok(None);
        }

        Ok(Some(merge_spans(per_slice)))
    }
}

/// Folds one OK/Unsupported slice's reported cost into both the query's live
/// accounting handle (so the reported total reflects it) and the coordinator's
/// running snapshot (the basis for the incremental bytes-scanned check), and
/// sums its `FetchStats` page counters. Both accounting folds saturate, so a
/// worker reporting a counter near `u64::MAX` clamps rather than wrapping.
fn fold_slice(
    live: &QueryAccounting,
    running: &mut QueryAccountingSnapshot,
    stats: &mut FetchStats,
    response: &client::SliceResponse,
) {
    live.merge_snapshot(&response.accounting);
    *running = running.saturating_merge(&response.accounting);
    stats.raw_f64_pages = stats
        .raw_f64_pages
        .saturating_add(response.stats.raw_f64_pages);
    stats.raw_f64_bytes = stats
        .raw_f64_bytes
        .saturating_add(response.stats.raw_f64_bytes);
}

/// Folds one RLOG-family slice's accounting into the query's live handle and the
/// coordinator's running snapshot (the basis for the incremental bytes-scanned
/// check). The log sibling of [`fold_slice`]; a log slice carries no `FetchStats`
/// page counters (a metric-path concept), so there is none to sum.
fn fold_log_slice(
    live: &QueryAccounting,
    running: &mut QueryAccountingSnapshot,
    response: &SliceLogResponse,
) {
    live.merge_snapshot(&response.accounting);
    *running = running.saturating_merge(&response.accounting);
}

/// Folds one Spans slice's accounting into the query's live handle and the
/// coordinator's running snapshot (the basis for the incremental bytes-scanned
/// check). The span sibling of [`fold_log_slice`]; a span slice carries no
/// `FetchStats` page counters (a metric-path concept), so there is none to sum.
fn fold_span_slice(
    live: &QueryAccounting,
    running: &mut QueryAccountingSnapshot,
    response: &SliceSpanResponse,
) {
    live.merge_snapshot(&response.accounting);
    *running = running.saturating_merge(&response.accounting);
}

/// The stated cross-segment total order for RLOG records (no dedup: see below),
/// and the coordinator merge that reproduces it (#284, ADR-0071 amendment
/// decision 4).
///
/// # The invariant this reproduces
///
/// A distributed Logs/Alerts/Audit fetch must be bit-identical to reading every
/// pinned segment locally with `LogSegmentFetcher` and combining the records
/// under one defined total order. Because ADR-0052 online resharding routes a
/// single stream to different shard indices across generations, a stream's
/// segments can land in different slices; the merge is therefore defined purely
/// on record identity and this total order, never on slice or shard arrival
/// order (mirroring the metrics lane's `merge_soa_runs`, which is order-
/// independent over the flat run pool).
///
/// The **total order** over records is ascending, lexicographic, over the whole
/// record content in a fixed field sequence:
///
/// `(ts_ns, stream_id, stream_attrs, observed_ts_ns, severity_num,
///   severity_text, body, trace_id, span_id, flags, attrs)`
///
/// where `attrs` is compared by the concatenation of each `(key, value)` pair's
/// frozen `canonical_attr_bytes` encoding **in the record's own attribute
/// order** (so `f64` values compare by bit pattern, preserving NaN/-0.0, and two
/// records differing only in attribute order are ordered, not conflated). Every
/// field participates, so the key is a total order: two records with an equal
/// key are byte-identical.
///
/// **No dedup.** Unlike the metrics `(series_id, ts)` dedup, an equal key here
/// (two byte-identical records) is NOT collapsed. `docs/consistency-model.md`
/// ("logs and spans") and ADR-0051 section 5 are explicit that logs/alerts/audit
/// have no query-time dedup: a retry after a lost ack produces byte-identical
/// rows that are legitimately duplicate user data and must stay visible, not
/// silently dropped. The total order above still matters for merge determinism
/// (a stable sort under a total key gives the same output regardless of slice
/// arrival order), it just never doubles as an identity for collapsing records.
///
/// The naive alternative -- concatenating each slice's records in slice arrival
/// order -- is wrong precisely when one stream's segments straddle two slices:
/// it emits one slice's records (some with large `ts`) before another slice's
/// (some with small `ts`), an order the local read never produces. The
/// `sort_by` below is the line that fixes it; replacing this merge with a
/// slice-order concatenation fails the reshard-straddling differential test.
///
/// The merge is order-independent over the flat pool: sorting the flattened
/// multiset of records is a pure function of that multiset, so the per-slice
/// grouping (which differs from the local per-segment grouping) never changes
/// the result -- exactly the property the differential test exercises.
pub(crate) fn merge_log_records(per_slice: Vec<Vec<LogRecord>>) -> Vec<LogRecord> {
    let mut keyed: Vec<(LogOrderKey, LogRecord)> = per_slice
        .into_iter()
        .flatten()
        .map(|record| (log_record_order_key(&record), record))
        .collect();
    // A stable sort on the precomputed full-content key. Equal-key records are
    // byte-identical, but they are NOT collapsed: the consistency model
    // (docs/consistency-model.md, "logs and spans") forbids query-time dedup
    // for logs/alerts/audit, because a retry after a lost ack produces
    // byte-identical rows that are legitimately duplicate USER DATA and must
    // stay visible. Every record in the pool is returned; only the metric path
    // (dedup by (series_id, ts), where duplicates are harmless) dedups.
    keyed.sort_by(|a, b| a.0.cmp(&b.0));
    keyed.into_iter().map(|(_, record)| record).collect()
}

/// The total-order key for one RLOG record (see [`merge_log_records`]). A tuple
/// so `Ord` derives structurally; the trailing `Vec<u8>` is the order-preserving
/// canonical encoding of the record's attributes.
type LogOrderKey = (
    i64,
    [u8; 16],
    Vec<u8>,
    i64,
    u8,
    String,
    String,
    Option<[u8; 16]>,
    Option<[u8; 8]>,
    u32,
    Vec<u8>,
);

pub(crate) fn log_record_order_key(record: &LogRecord) -> LogOrderKey {
    // Encode each attribute pair with the frozen canonical grammar and
    // concatenate in the record's own order. Each single-entry encoding is
    // self-delimiting (leading count 1, then `len(key) key encode_value`), so
    // the concatenation is injective and order-preserving, and `f64` values are
    // compared by `to_bits` (NaN payloads and -0.0 significant).
    let mut attrs_key = Vec::new();
    for pair in &record.attrs {
        attrs_key.extend_from_slice(&canonical_attr_bytes(std::slice::from_ref(pair)));
    }
    (
        record.ts_ns,
        record.stream_id.0,
        record.stream_attrs.clone(),
        record.observed_ts_ns,
        record.severity_num,
        record.severity_text.clone(),
        record.body.clone(),
        record.trace_id,
        record.span_id,
        record.flags,
        attrs_key,
    )
}

/// The stated cross-segment total order for spans (no dedup: see below), and the
/// coordinator merge that reproduces it (#285, ADR-0071 amendment decision 4).
///
/// # The invariant this reproduces
///
/// A distributed Spans fetch must equal, as a multiset under one defined total
/// order, reading every pinned RSPAN segment locally with `SpanSegmentFetcher`
/// and combining the spans under that order. RSPAN sorts records by
/// `(trace_id, start_ts)` within one object, but across objects there is no
/// global order on disk, and ADR-0052 online resharding routes one trace's
/// spans to different shard indices across generations, so a trace's segments
/// can land in different slices. Within a single shard generation a trace's
/// spans land on one shard (`shard_for_span`,
/// `one_trace_lands_in_exactly_one_shard`), but `SpanIngestRouter` carries the
/// same `GenerationSwitch` the log streams do, so a trace whose spans were
/// written on both sides of a reshard activation straddles shards. The merge is
/// therefore defined purely on span identity and this total order, never on
/// slice or shard arrival order (mirroring `merge_log_records` and the metrics
/// lane's `merge_soa_runs`, both order-independent over the flat pool).
///
/// The **total order** over spans is ascending, lexicographic, over the whole
/// span content in a fixed field sequence:
///
/// `(trace_id, span_id, start_ts_ns, end_ts_ns, parent_span_id, name,
///   status_code, status_message, service_name, attrs)`
///
/// where `attrs` is the record's merged `(key, value)` map compared as a
/// `Vec<(String, String)>` (RSPAN canonicalizes it to ascending key order on
/// write, so two reads of the same object compare equal). Every field
/// participates, so the key is a total order: two spans with an equal key are
/// byte-identical.
///
/// **No dedup.** An equal key here (two byte-identical spans) is NOT collapsed.
/// `docs/consistency-model.md` ("logs and spans") and ADR-0051 section 5 are
/// explicit that spans have no query-time dedup: a retry after a lost ack
/// produces byte-identical spans that are legitimately duplicate user data and
/// must stay visible, not silently dropped. The total order above still matters
/// for merge determinism (a stable sort under a total key gives the same output
/// regardless of slice arrival order), it just never doubles as an identity for
/// collapsing spans.
///
/// The naive alternative -- concatenating each slice's spans in slice arrival
/// order -- is wrong precisely when one trace's spans straddle two slices: it
/// emits one slice's spans before another's, an order the local read never
/// produces. The `sort_by` below is the line that fixes it; replacing this merge
/// with a slice-order concatenation fails the reshard-straddling differential
/// test.
///
/// The merge is order-independent over the flat pool: sorting the flattened
/// multiset of spans is a pure function of that multiset, so the per-slice
/// grouping (which differs from the local per-segment grouping) never changes
/// the result -- exactly the property the differential test exercises.
pub(crate) fn merge_spans(per_slice: Vec<Vec<SpanRow>>) -> Vec<SpanRow> {
    let mut spans: Vec<SpanRow> = per_slice.into_iter().flatten().collect();
    // A stable sort under the full-content total-order comparator. Equal-key
    // spans are byte-identical, but they are NOT collapsed: the consistency
    // model (docs/consistency-model.md, "logs and spans") forbids query-time
    // dedup for spans, because a retry after a lost ack produces byte-identical
    // spans that are legitimately duplicate USER DATA and must stay visible.
    // Every span in the pool is returned. `span_cmp` compares borrowed fields in
    // place, so unlike an owned `SpanOrderKey` per span it clones nothing.
    spans.sort_by(span_cmp);
    spans
}

/// The total order over spans (see [`merge_spans`]): a `sort_by` comparator over
/// borrowed fields, in the exact field sequence
/// `(trace_id, span_id, start_ts_ns, end_ts_ns, parent_span_id, name,
/// status_code, status_message, service_name, attrs)`. It compares the same
/// fields, in the same order, as the [`span_order_key`] tuple (whose `Ord`
/// derives structurally), but without allocating an owned key per span: no
/// clones of `name`, `status_message`, `service_name`, or `attrs`. Span
/// attribute values are plain strings, so `attrs` compares directly as a
/// `Vec<(String, String)>` (no canonical byte encoding, unlike the typed log
/// attribute values). `service_name` and `status_message` are `Option<String>`,
/// so `None` orders before `Some`, keeping them distinct from an empty string.
/// `span_cmp_agrees_with_span_order_key` pins the two definitions together so
/// they cannot silently drift.
pub(crate) fn span_cmp(a: &SpanRow, b: &SpanRow) -> std::cmp::Ordering {
    let ra = &a.record;
    let rb = &b.record;
    ra.trace_id
        .cmp(&rb.trace_id)
        .then_with(|| ra.span_id.cmp(&rb.span_id))
        .then_with(|| ra.start_ts_ns.cmp(&rb.start_ts_ns))
        .then_with(|| ra.end_ts_ns.cmp(&rb.end_ts_ns))
        .then_with(|| ra.parent_span_id.cmp(&rb.parent_span_id))
        .then_with(|| ra.name.cmp(&rb.name))
        .then_with(|| ra.status_code.to_u8().cmp(&rb.status_code.to_u8()))
        .then_with(|| ra.status_message.cmp(&rb.status_message))
        .then_with(|| a.service_name.cmp(&b.service_name))
        .then_with(|| ra.attrs.cmp(&rb.attrs))
}

/// The total-order key for one span (see [`merge_spans`]). A tuple so `Ord`
/// derives structurally; span attribute values are plain strings, so the merged
/// `attrs` map is compared directly as a `Vec<(String, String)>` (no canonical
/// byte encoding needed, unlike the typed log attribute values). `service_name`
/// and `status_message` are `Option<String>`, so `None` orders before `Some`,
/// keeping them distinct from an empty string.
///
/// The production merge no longer builds this owned key ([`merge_spans`] sorts
/// with the allocation-free [`span_cmp`]); it stays as the reference definition
/// of the span total order that the coordinator tests assert against directly,
/// so it is compiled only under `cfg(test)`.
#[cfg(test)]
type SpanOrderKey = (
    [u8; 16],
    [u8; 8],
    i64,
    i64,
    Option<[u8; 8]>,
    String,
    u8,
    Option<String>,
    Option<String>,
    Vec<(String, String)>,
);

#[cfg(test)]
pub(crate) fn span_order_key(row: &SpanRow) -> SpanOrderKey {
    let record = &row.record;
    (
        record.trace_id,
        record.span_id,
        record.start_ts_ns,
        record.end_ts_ns,
        record.parent_span_id,
        record.name.clone(),
        record.status_code.to_u8(),
        record.status_message.clone(),
        row.service_name.clone(),
        record.attrs.clone(),
    )
}

/// Maps a per-slice `Budgets` share from the engine config. `Unlimited` bytes
/// map to `0`, the wire's "no cap" sentinel (a real query never scans zero
/// bytes, so the value is unambiguous).
fn encode_budgets(config: &EngineConfig) -> pb::Budgets {
    pb::Budgets {
        max_series: config.max_series as u64,
        max_samples: config.max_samples as u64,
        max_bytes_scanned: match config.max_bytes_scanned {
            ByteLimit::Bounded(n) => n,
            ByteLimit::Unlimited => 0,
        },
        max_segments: config.max_segments as u64,
    }
}

/// A stable 16-byte query id for one distributed query's fragment capabilities
/// (ADR-0071 amendment, decision 2). Derived from the query's own identity
/// (tenant, signal, absolute deadline, and the pinned segment set) so every
/// slice of one `fetch` call, including a re-dispatch, carries the same id and
/// the same minted capability authorizes them all, without threading a random
/// source through the engine. The value only needs to be a stable 16 bytes the
/// mint and verify sides agree on; the first 16 bytes of a BLAKE3 hash suffice.
fn query_id_bytes(
    tenant_bytes: &[u8],
    signal_disc: u32,
    deadline_unix_ns: i64,
    snapshot: &Snapshot,
) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(tenant_bytes);
    hasher.update(&signal_disc.to_be_bytes());
    hasher.update(&deadline_unix_ns.to_be_bytes());
    for segment in &snapshot.segments {
        hasher.update(&segment.content_hash);
    }
    let digest = hasher.finalize();
    let mut id = [0u8; 16];
    id.copy_from_slice(&digest.as_bytes()[..16]);
    id
}

fn distrib_error(err: DistribError) -> QueryError {
    QueryError::Distrib {
        reason: err.to_string(),
    }
}
