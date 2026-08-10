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
use ravel_types::accounting::QueryAccounting;
use ravel_types::{SeriesId, Signal, TenantHash};

use crate::config::{ByteLimit, EngineConfig};
use crate::distrib::client::{DistribError, SliceFetcher};
use crate::distrib::partition::{DistribThresholds, partition_snapshot};
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

    /// Partitions the snapshot, dispatches one request per slice, and collects
    /// the results.
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
        let responses: Vec<Result<client::SliceResponse, DistribError>> = stream::iter(slices)
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
            .buffer_unordered(concurrency)
            .collect()
            .await;

        // Classify the slice outcomes. A terminal hard error dominates (fail
        // the query); a snapshot invalidation triggers the engine's single
        // re-resolve/retry; an Unsupported status triggers the silent local
        // fallback. Only when every slice is OK do we build the merged triple.
        let mut ok = Vec::with_capacity(responses.len());
        let mut invalidated = false;
        let mut unsupported = false;
        for response in responses {
            let response = response.map_err(distrib_error)?;
            match response.status {
                pb::status::Code::Ok => ok.push(response),
                pb::status::Code::SnapshotInvalidated => invalidated = true,
                pb::status::Code::Unsupported => unsupported = true,
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
            return Err(QueryError::Fetch(FetchError::Store {
                key: "distributed-slice".to_string(),
                source: ravel_object_store::StoreError::NotFound,
            }));
        }
        if unsupported {
            return Ok(None);
        }

        // Coordinator budget re-enforcement (ADR-0071): a lying worker cannot
        // overrun the query's series cap. Count distinct series across every
        // slice against `max_series`, independent of any per-slice budget the
        // worker was supposed to honor. The sample cap is enforced downstream
        // by the k-way merge, after cross-segment dedup, which is where a
        // sample total is meaningful.
        let mut distinct: HashSet<SeriesId> = HashSet::new();
        let mut per_slice: Vec<Vec<FetchedSeriesSoa>> = Vec::with_capacity(ok.len());
        for response in ok {
            accounting.merge_snapshot(&response.accounting);
            for fs in &response.scalar {
                if !distinct.contains(&fs.series_id) && distinct.len() >= config.max_series {
                    return Err(QueryError::TooManySeries {
                        count: distinct.len() + 1,
                        max: config.max_series,
                    });
                }
                distinct.insert(fs.series_id);
            }
            per_slice.push(response.scalar);
        }

        Ok(Some((per_slice, FetchStats::default(), Vec::new())))
    }
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
