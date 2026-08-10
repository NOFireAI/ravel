//! Distributed read fan-out core (ADR-0071, issue #864).
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
pub mod partition;
pub mod service;

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
use ravel_catalog::Snapshot;
use ravel_promql::LabelMatcher;
use ravel_proto::queryfrag::v1 as pb;
use ravel_types::accounting::{QueryAccounting, QueryAccountingSnapshot};
use ravel_types::{SeriesId, Signal, TenantHash};

use crate::config::{ByteLimit, EngineConfig};
use crate::distrib::client::{DistribError, SliceFetcher};
use crate::distrib::partition::{DistribThresholds, partition_snapshot};
use crate::engine::bytes_scanned_exceeded;
use crate::erasure::ErasurePredicate;
use crate::error::QueryError;
use crate::fetcher::{FetchError, FetchStats, FetchedHistogramSeries, FetchedSeriesSoa};

pub use partition::{DISTRIBUTE_MIN_SEGMENTS, DISTRIBUTE_MIN_STORE_BYTES};

/// The scalar/histogram/stats triple the local fetch returns, and the shape
/// [`Distributed::fetch`] reconstructs from slices so the engine's merge step
/// is identical for local and distributed queries.
pub type FetchedTriple = (
    Vec<Vec<FetchedSeriesSoa>>,
    FetchStats,
    Vec<Vec<FetchedHistogramSeries>>,
);

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
    /// Returns:
    /// - `Ok(Some(triple))` when every slice succeeded: the coordinator merges
    ///   it exactly as the local path merges its own fetch.
    /// - `Ok(None)` when the query must fall back to fully local execution: a
    ///   worker reported [`pb::status::Code::Unsupported`] (version skew, a
    ///   histogram-bearing slice, or a resolve-scope slice). ADR-0071's silent
    ///   fallback, never an error to the user.
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
    ) -> Result<Option<FetchedTriple>, QueryError> {
        let slices = partition_snapshot(snapshot, self.thresholds.max_parallel_slices);
        if slices.is_empty() {
            // Nothing to fetch: an empty snapshot merges to an empty result
            // identically whether local or distributed.
            return Ok(Some((Vec::new(), FetchStats::default(), Vec::new())));
        }

        let encoded_matchers = codec::encode_matchers(matchers);
        let encoded_erasure = codec::encode_erasure(erasure);
        let budgets = encode_budgets(config);
        let tenant_bytes = tenant_hash.0.to_vec();
        let signal_disc = codec::signal_to_u32(signal);

        let concurrency = self.thresholds.max_parallel_slices.max(1);
        let mut stream = stream::iter(slices)
            .map(|slice| {
                let request = pb::FetchRequest {
                    protocol_version: codec::PROTOCOL_VERSION,
                    query_id: Vec::new(),
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
                    window_start_ns: 0,
                    window_end_ns: 0,
                    budgets: Some(budgets),
                    deadline_unix_ns: 0,
                    erasure: encoded_erasure.clone(),
                    trace_context: String::new(),
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
        let mut per_slice: Vec<Vec<FetchedSeriesSoa>> = Vec::new();
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
                    // Bytes-scanned cap re-enforcement over the folded total so
                    // far, so a distributed query is bounded as tightly as a
                    // local one even if a worker under-reports its own trip.
                    if let Some(err) =
                        bytes_scanned_exceeded(running.total_s3_bytes(), config.max_bytes_scanned)
                    {
                        return Err(err);
                    }
                    per_slice.push(response.scalar);
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

        Ok(Some((per_slice, stats, Vec::new())))
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

fn distrib_error(err: DistribError) -> QueryError {
    QueryError::Distrib {
        reason: err.to_string(),
    }
}
