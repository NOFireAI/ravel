//! Cross-cluster federation fan-out (ADR-0071 Resolve scope).
//!
//! Intra-cluster distribution ([`Distributed`](crate::distrib::Distributed))
//! resolves ONE pinned snapshot and fans its segments out to workers in the
//! same cluster. Federation is the other half of ADR-0071's read fan-out: the
//! coordinator sends the query's matchers and window to each configured remote
//! cluster, and every remote resolves *its own* snapshot, enforces *its own*
//! admission/limits/erasure through its ordinary query path, and returns
//! decoded series. The coordinator folds those series into the same flat merge
//! pool the local fetch produces, so a federated result is the union of what
//! each cluster computes locally, and "distribution changes where bytes are
//! fetched, never what the query computes" holds across cluster boundaries too.
//!
//! # Trust boundary
//!
//! A [`RemoteCluster`]'s [`SliceFetcher`] carries the *operator-configured*
//! credential for that remote, baked in at construction. The federation seam
//! never sees a client credential and has no way to forward one: the remote
//! authenticates the request as an ordinary tenant credential and resolves the
//! tenant from *its own* auth, never from the wire. This is structural, not a
//! runtime check -- there is no client-credential parameter to misuse here.
//!
//! # Availability
//!
//! Each remote is dispatched under a per-cluster soft timeout. A remote that
//! times out or reports itself unavailable is treated as unavailable. With
//! `skip_unavailable = false` (the default) that fails the whole query typed
//! ([`QueryError::Federation`]); with `skip_unavailable = true` the query
//! continues, records the skipped cluster's name in the Prometheus-compatible
//! `warnings`, and marks the coverage partial. A budget overrun is never
//! skippable: the coordinator re-enforces `max_bytes_scanned` over the folded
//! remote spend and fails typed regardless of `skip_unavailable`, because a
//! budget cap is a correctness bound, not an availability property.
//!
//! # Merge semantics and the cross-cluster tie-break limitation
//!
//! Federated runs join the same k-way merge the local fetch feeds, keyed by
//! `(series_id, ts)`. Within one cluster, two samples at the same
//! `(series_id, ts)` resolve under the deterministic total order in
//! `is_greater` (`created_unix_ns`, `writer_epoch`, `writer_seq`, in-page
//! index) -- a globally meaningful order because those provenance fields are
//! assigned by one cluster's writers.
//!
//! Across clusters that order is NOT globally meaningful: two clusters assign
//! `writer_epoch`/`writer_seq` independently, so if the same `(series_id, ts)`
//! is present in two clusters with different values, which one wins is decided
//! by provenance fields that are only comparable within a cluster. Federation
//! therefore assumes each cluster owns a DISJOINT slice of series identity (the
//! intended cross-cluster deployment: one tenant's data lives in exactly one
//! cluster, or clusters hold non-overlapping series). When that holds, every
//! `(series_id, ts)` comes from exactly one cluster and the tie-break never
//! fires across the boundary. When it does not hold, the result is still
//! deterministic for a fixed set of inputs but the choice of winner between two
//! clusters' conflicting samples is unspecified. This is a known limitation of
//! ADR-0071 wave 5, documented in docs/query-engine.md; a globally meaningful
//! cross-cluster ordering is out of scope here.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use ravel_promql::LabelMatcher;
use ravel_proto::queryfrag::v1 as pb;
use ravel_types::accounting::{QueryAccounting, QueryAccountingSnapshot};
use ravel_types::{SeriesId, Signal, TenantHash};

use crate::config::EngineConfig;
use crate::distrib::client::{DistribError, SliceFetcher, SliceResponse};
use crate::distrib::{codec, encode_budgets};
use crate::engine::bytes_scanned_exceeded;
use crate::erasure::ErasurePredicate;
use crate::error::QueryError;
use crate::fetcher::{FetchStats, FetchedSeriesSoa};

/// The default per-remote soft timeout. A remote that has not answered within
/// this bound is treated as unavailable (skipped or fatal per
/// `skip_unavailable`). Chosen to be generous relative to a healthy
/// cross-cluster round trip but well under a typical query deadline, so a
/// wedged remote never dominates the query's own bound.
pub const DEFAULT_REMOTE_SOFT_TIMEOUT: Duration = Duration::from_secs(10);

/// One operator-configured remote cluster to federate reads into.
///
/// The `fetcher` carries this remote's credential (see the module trust-boundary
/// note); the federation seam only dispatches through it.
pub struct RemoteCluster {
    /// The operator-assigned cluster name. This is the identity reported in the
    /// `warnings` field when the cluster is skipped, so it must be stable and
    /// operator-recognizable. Remotes never appear in query text.
    pub name: String,
    /// The credentialed slice fetcher for this remote.
    pub fetcher: Arc<dyn SliceFetcher>,
    /// When true, an unavailable/timed-out remote is skipped (with a warning)
    /// rather than failing the query. Defaults to false at the config layer.
    pub skip_unavailable: bool,
    /// Per-remote soft timeout. Exceeding it marks the remote unavailable.
    pub soft_timeout: Duration,
}

/// The joined result of one spawned remote-dispatch task: the inner
/// `Result` is the fetch outcome, the outer the soft-timeout verdict.
type Joined = Result<Result<SliceResponse, DistribError>, tokio::time::error::Elapsed>;

/// Owns the spawned remote-dispatch tasks and aborts any still running when the
/// fan-out returns early (a fatal remote, a tripped budget, or this future being
/// cancelled). Dropping a bare `JoinHandle` only *detaches* its task, which
/// would leave in-flight remote fetches running against a query that has already
/// failed; aborting stops them. Aborting an already-finished handle is a no-op,
/// so aborting the whole set on drop is safe even after every task was joined.
struct DispatchGuard {
    handles: Vec<tokio::task::JoinHandle<Joined>>,
}

impl Drop for DispatchGuard {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
    }
}

/// The federation execution context: the set of remote clusters to fan a
/// federated read out to. Held in an `Option` on the engine; `None` (the
/// default) means no federation and a fully cluster-local query.
pub struct Federation {
    remotes: Vec<RemoteCluster>,
}

/// The result of fanning one query's matchers/window out to every remote.
///
/// `series` is one decoded run-list per contributing remote, in the exact
/// [`FetchedSeriesSoa`] shape the local fetch produces, so the engine unions
/// them into its flat merge pool with no special-casing. `warnings` and
/// `skipped` carry the Prometheus-compatible partial-coverage signal.
#[derive(Debug, Default)]
pub struct FederationOutcome {
    /// Decoded scalar series from every healthy remote, one inner `Vec` per
    /// remote. The engine flattens these into its merge pool.
    pub series: Vec<Vec<FetchedSeriesSoa>>,
    /// Summed remote `FetchStats` page counters, folded into the query's stats.
    pub stats: FetchStats,
    /// Human-readable warnings (one per skipped cluster), surfaced in the
    /// PromQL response's `warnings` field.
    pub warnings: Vec<String>,
    /// Names of clusters that were skipped because they were unavailable.
    pub skipped: Vec<String>,
    /// True when at least one configured remote was skipped, so the query's
    /// coverage is partial.
    pub partial: bool,
}

impl Federation {
    /// Builds a federation context over a set of remote clusters.
    pub fn new(remotes: Vec<RemoteCluster>) -> Self {
        Federation { remotes }
    }

    /// The configured remotes (for diagnostics and tests).
    pub fn remotes(&self) -> &[RemoteCluster] {
        &self.remotes
    }

    /// Fans the query's matchers and window out to every remote under its soft
    /// timeout, folding each healthy remote's series and cost into the result.
    ///
    /// Returns [`FederationOutcome`] on success (possibly with skipped clusters
    /// recorded in `warnings`/`skipped`), or a typed [`QueryError`]:
    /// - [`QueryError::Federation`] when a remote is unavailable/timed-out and
    ///   its `skip_unavailable` is false.
    /// - [`QueryError::TooManyBytesScanned`] when the folded remote spend trips
    ///   the coordinator's `max_bytes_scanned` (never skippable).
    /// - [`QueryError::TooManySeries`] when the combined remote series overrun
    ///   the distinct-series cap.
    ///
    /// The `accounting` handle is folded with every remote's reported cost
    /// (saturating), so the query's reported total reflects federated fetches
    /// even when the query then fails.
    ///
    /// Every parameter is taken **by value**, not by reference. A named
    /// `async fn` that holds a borrow across its inner `async_trait`
    /// [`SliceFetcher`] await yields a future that is `Send` only for a fixed
    /// lifetime, not higher-ranked over all of them, which fails axum's
    /// `Handler` blanket-impl `Send` bound at the router call site upstream.
    /// `QueryAccounting` is `Arc`-backed so an owned clone still folds into the
    /// same shared handle, and `EngineConfig` is `Copy`.
    #[allow(clippy::too_many_arguments)]
    pub async fn fetch(
        &self,
        tenant_hash: TenantHash,
        signal: Signal,
        matchers: Vec<LabelMatcher>,
        erasure: Vec<ErasurePredicate>,
        window_start_ns: i64,
        window_end_ns: i64,
        min_commit_tokens: Vec<String>,
        accounting: QueryAccounting,
        config: EngineConfig,
    ) -> Result<FederationOutcome, QueryError> {
        let mut outcome = FederationOutcome::default();
        if self.remotes.is_empty() {
            return Ok(outcome);
        }

        let encoded_matchers = codec::encode_matchers(&matchers);
        let encoded_erasure = codec::encode_erasure(&erasure);
        let budgets = encode_budgets(&config);
        let tenant_bytes = tenant_hash.0.to_vec();
        let signal_disc = codec::signal_to_u32(signal);

        // Dispatch one Resolve-scope request per remote on its own spawned task,
        // each under its own soft timeout, then join them. Fan-out is narrow
        // (one request per configured cluster, not one per segment), so a task
        // per remote is cheap.
        //
        // Why `tokio::spawn` and not a `buffer_unordered` stream: the
        // `async_trait` `SliceFetcher::fetch` future carries a borrow-derived
        // lifetime, and holding it inside a stream across `stream.next().await`
        // couples THIS future to that fixed lifetime, so it is `Send` for one
        // lifetime rather than higher-ranked over all of them -- which fails
        // axum's `Handler` blanket-impl `Send` bound at the router call site
        // upstream. Each spawned task is an independent `'static + Send` future
        // (it owns its `Arc`-cloned fetcher and request), so this future only
        // ever holds `JoinHandle`s across an await, and the lifetime-bearing
        // fetch future stays encapsulated in the task.
        let mut guard = DispatchGuard {
            handles: Vec::with_capacity(self.remotes.len()),
        };
        // Metadata parallel to `guard.handles` by index: the tasks own their
        // request/fetcher, so the lifetime-bearing fetch future stays inside the
        // task and this future only ever holds `JoinHandle`s across an await.
        let mut meta: Vec<(String, bool, Duration)> = Vec::with_capacity(self.remotes.len());
        for remote in &self.remotes {
            let name = remote.name.clone();
            let skip_unavailable = remote.skip_unavailable;
            let timeout = remote.soft_timeout;
            let fetcher = Arc::clone(&remote.fetcher);
            let request = pb::FetchRequest {
                protocol_version: codec::PROTOCOL_VERSION,
                query_id: Vec::new(),
                tenant_hash: tenant_bytes.clone(),
                signal: signal_disc,
                scope: Some(pb::fetch_request::Scope::Resolve(pb::ResolveScope {
                    min_commit_token: min_commit_tokens.clone(),
                })),
                matchers: encoded_matchers.clone(),
                window_start_ns,
                window_end_ns,
                budgets: Some(budgets),
                deadline_unix_ns: 0,
                erasure: encoded_erasure.clone(),
                trace_context: String::new(),
            };
            let handle =
                tokio::spawn(
                    async move { tokio::time::timeout(timeout, fetcher.fetch(request)).await },
                );
            guard.handles.push(handle);
            meta.push((name, skip_unavailable, timeout));
        }

        let mut distinct: HashSet<SeriesId> = HashSet::new();
        let mut running = QueryAccountingSnapshot::default();

        for (i, (name, skip_unavailable, timeout)) in meta.into_iter().enumerate() {
            // Join the spawned task by unique index. A `JoinError` means the
            // task panicked (or was aborted): a panic is a coordinator-side bug,
            // not a remote availability signal, so it fails the query typed
            // regardless of `skip_unavailable` -- never silently dropped. On any
            // early return below, `guard` drops and aborts the tasks not yet
            // joined, so a failed query leaves no remote fetch running.
            // Bind the awaited value on its own statement so the `&mut` borrow
            // of `guard.handles` ends here: a later early return in this arm
            // must be free to drop (and thus abort) `guard`.
            let joined = (&mut guard.handles[i]).await;
            let dispatched = match joined {
                Ok(dispatched) => dispatched,
                Err(join_err) => {
                    return Err(QueryError::Federation {
                        cluster: name.clone(),
                        reason: format!("federation dispatch task failed: {join_err}"),
                    });
                }
            };

            // Timeout: the future never resolved within the soft bound. Treat
            // as unavailable.
            let result = match dispatched {
                Ok(result) => result,
                Err(_elapsed) => {
                    handle_unavailable(
                        &name,
                        skip_unavailable,
                        format!("soft timeout of {timeout:?} elapsed"),
                        &mut outcome,
                    )?;
                    continue;
                }
            };

            // Transport failure: the remote is unreachable/crashed. Unavailable.
            // A framing/codec fault is NOT unavailability -- the remote answered
            // with a malformed frame, which is corruption, so it is a hard typed
            // error that no `skip_unavailable` masks (trust boundary: a remote's
            // frames get the same decode validation as intra-cluster frames).
            let response = match result {
                Ok(response) => response,
                Err(DistribError::Transport(msg)) => {
                    handle_unavailable(
                        &name,
                        skip_unavailable,
                        format!("transport failed: {msg}"),
                        &mut outcome,
                    )?;
                    continue;
                }
                // A remote carrying native-histogram data this build cannot
                // decode yet is a coverage gap, not corruption: the frame is
                // well-formed, the remote simply serves a data kind the
                // coordinator cannot consume across the boundary. Route it
                // through the same skip_unavailable path an unavailable remote
                // takes, with a truthful reason -- never the "malformed
                // response" hard fault, which would be both untrue and
                // non-skippable.
                Err(DistribError::HistogramUnsupported) => {
                    handle_unavailable(
                        &name,
                        skip_unavailable,
                        "remote returned native-histogram data this build cannot decode across \
                         clusters yet"
                            .to_string(),
                        &mut outcome,
                    )?;
                    continue;
                }
                Err(err) => {
                    return Err(QueryError::Federation {
                        cluster: name.clone(),
                        reason: format!("remote returned a malformed response: {err}"),
                    });
                }
            };

            match response.status {
                pb::status::Code::Ok => {
                    fold_remote(&accounting, &mut running, &mut outcome.stats, &response);
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
                    // Bytes-scanned re-enforcement over the folded remote spend.
                    // Always fatal, never skippable: a budget cap bounds
                    // correctness/cost, not availability.
                    if let Some(err) =
                        bytes_scanned_exceeded(running.total_s3_bytes(), config.max_bytes_scanned)
                    {
                        return Err(err);
                    }
                    outcome.series.push(response.scalar);
                }
                pb::status::Code::Unavailable => {
                    handle_unavailable(
                        &name,
                        skip_unavailable,
                        format!("remote reported unavailable: {}", response.status_message),
                        &mut outcome,
                    )?;
                }
                pb::status::Code::BudgetExceeded => {
                    // Fold the real spend, then fail typed regardless of
                    // skip_unavailable (budget is never skippable).
                    fold_remote(&accounting, &mut running, &mut outcome.stats, &response);
                    return Err(bytes_scanned_exceeded(
                        running.total_s3_bytes(),
                        config.max_bytes_scanned,
                    )
                    .unwrap_or_else(|| QueryError::Federation {
                        cluster: name.clone(),
                        reason: format!("remote tripped its budget: {}", response.status_message),
                    }));
                }
                other => {
                    // SnapshotInvalidated/Unsupported/Corrupt/etc. A remote
                    // resolves its own snapshot, so none of these is an
                    // availability signal: they are hard faults, typed, never
                    // masked by skip_unavailable.
                    return Err(QueryError::Federation {
                        cluster: name.clone(),
                        reason: format!("remote returned {other:?}: {}", response.status_message),
                    });
                }
            }
        }

        Ok(outcome)
    }
}

/// Records an unavailable remote: skip it (warning + partial coverage) when its
/// `skip_unavailable` is set, otherwise fail the query typed. A free function
/// (not a method) so it never re-borrows `&self`, keeping the fan-out future
/// free of borrows that would break the upstream `Send` bound.
///
/// `reason` carries the raw transport/timeout detail (remote endpoint IP:port,
/// errno, remote-supplied status text). That detail is an internal identifier
/// that must never reach a client body, exactly like the object keys and tenant
/// hashes redacted at the `QueryError` HTTP boundary (http/error.rs):
/// it is logged server-side and, on the fatal path, carried in the typed
/// `QueryError::Federation` (which http/error.rs redacts to `MSG_UNAVAILABLE`),
/// but the client-facing `skip_unavailable` warning is a stable message naming
/// only the operator-facing cluster.
fn handle_unavailable(
    name: &str,
    skip_unavailable: bool,
    reason: String,
    outcome: &mut FederationOutcome,
) -> Result<(), QueryError> {
    if skip_unavailable {
        // Log the full reason (endpoint/errno) for operator diagnosis, but keep
        // it out of the client-facing warning.
        tracing::warn!(
            cluster = %name,
            reason = %reason,
            "federated remote unavailable; skipped (skip_unavailable), coverage partial"
        );
        outcome.warnings.push(skipped_cluster_warning(name));
        outcome.skipped.push(name.to_string());
        outcome.partial = true;
        Ok(())
    } else {
        Err(QueryError::Federation {
            cluster: name.to_string(),
            reason,
        })
    }
}

/// The stable, redacted client-facing warning for a skipped remote cluster. It
/// names the operator-facing cluster (so a client and operator can tell which
/// data is missing) but carries no endpoint, errno, or remote-supplied text.
fn skipped_cluster_warning(name: &str) -> String {
    format!("remote cluster {name} unavailable; results are partial")
}

/// Folds one healthy remote's reported cost into the query's live accounting
/// handle (so the reported total reflects it) and the coordinator's running
/// snapshot (the basis for the bytes-scanned re-check), and sums its
/// `FetchStats` page counters. Both accounting folds saturate.
fn fold_remote(
    live: &QueryAccounting,
    running: &mut QueryAccountingSnapshot,
    stats: &mut FetchStats,
    response: &SliceResponse,
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

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    /// A remote whose fetch always fails at transport, carrying a message that
    /// embeds internal identifiers (IP:port, errno) exactly as a real tonic
    /// transport error would. The redaction boundary must keep all of it out of
    /// any client-facing warning.
    struct TransportErrFetcher(String);

    #[async_trait]
    impl SliceFetcher for TransportErrFetcher {
        async fn fetch(&self, _r: pb::FetchRequest) -> Result<SliceResponse, DistribError> {
            Err(DistribError::Transport(self.0.clone()))
        }
    }

    /// A remote that never answers within any soft timeout.
    struct SlowFetcher;

    #[async_trait]
    impl SliceFetcher for SlowFetcher {
        async fn fetch(&self, _r: pb::FetchRequest) -> Result<SliceResponse, DistribError> {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            unreachable!("the soft timeout fires long before this")
        }
    }

    /// A remote that streams native-histogram data this build cannot decode
    /// across the slice boundary (S5). Well-formed, not corruption.
    struct HistUnsupportedFetcher;

    #[async_trait]
    impl SliceFetcher for HistUnsupportedFetcher {
        async fn fetch(&self, _r: pb::FetchRequest) -> Result<SliceResponse, DistribError> {
            Err(DistribError::HistogramUnsupported)
        }
    }

    fn one_remote(fetcher: Arc<dyn SliceFetcher>, skip: bool, timeout: Duration) -> Federation {
        Federation::new(vec![RemoteCluster {
            name: "eu-west".to_string(),
            fetcher,
            skip_unavailable: skip,
            soft_timeout: timeout,
        }])
    }

    async fn run(fed: &Federation) -> Result<FederationOutcome, QueryError> {
        fed.fetch(
            TenantHash([1u8; 16]),
            Signal::Metrics,
            Vec::new(),
            Vec::new(),
            0,
            1_000,
            Vec::new(),
            QueryAccounting::new(),
            EngineConfig::default(),
        )
        .await
    }

    /// The internal identifiers a client-facing warning must never carry.
    const LEAKS: &[&str] = &[
        "10.1.2.3",
        "9443",
        "111",
        "os error",
        "connection refused",
        "transport",
    ];

    /// BLOCK 1: `skip_unavailable = true` over an unavailable remote returns a
    /// partial result whose warning names the skipped cluster and leaks no
    /// endpoint/errno.
    ///
    /// Flip-line proof: change `handle_unavailable` to push the raw `reason`
    /// (`outcome.warnings.push(reason)`) instead of `skipped_cluster_warning`,
    /// and the leak assertions below fire.
    #[tokio::test]
    async fn skip_unavailable_marks_partial_and_redacts_the_warning() {
        let fetcher = Arc::new(TransportErrFetcher(
            "connection refused (os error 111) dialing 10.1.2.3:9443".to_string(),
        ));
        let fed = one_remote(fetcher, true, Duration::from_secs(5));
        let outcome = run(&fed)
            .await
            .expect("skip_unavailable keeps the query alive");

        assert!(
            outcome.partial,
            "coverage is partial when a remote is skipped"
        );
        assert_eq!(outcome.skipped, vec!["eu-west".to_string()]);
        assert_eq!(outcome.warnings.len(), 1, "one warning per skipped cluster");
        let w = &outcome.warnings[0];
        assert!(
            w.contains("eu-west"),
            "warning names the operator-facing cluster: {w}"
        );
        for leak in LEAKS {
            assert!(
                !w.contains(leak),
                "client warning leaked internal identifier {leak:?}: {w}"
            );
        }
    }

    /// BLOCK 1 / S2: the same redaction holds on the soft-timeout path (S3: the
    /// skipped-cluster warning names the cluster there too).
    #[tokio::test]
    async fn soft_timeout_marks_partial_and_redacts_the_warning() {
        let fed = one_remote(Arc::new(SlowFetcher), true, Duration::from_millis(20));
        let outcome = run(&fed)
            .await
            .expect("skip_unavailable keeps the query alive");

        assert!(outcome.partial);
        assert_eq!(outcome.skipped, vec!["eu-west".to_string()]);
        assert_eq!(outcome.warnings.len(), 1);
        let w = &outcome.warnings[0];
        assert!(
            w.contains("eu-west"),
            "timeout warning names the cluster: {w}"
        );
        // The reason string ("soft timeout of ... elapsed") must not reach it.
        assert!(
            !w.contains("timeout"),
            "timeout warning is the stable redacted message: {w}"
        );
        assert!(
            !w.contains("elapsed"),
            "timeout warning is the stable redacted message: {w}"
        );
    }

    /// BLOCK 1: with `skip_unavailable = false` (the default), an unavailable
    /// remote fails the query typed and never returns a partial result. The
    /// typed error names the cluster; the raw reason it carries is redacted at
    /// the HTTP boundary (http/error.rs), not here.
    #[tokio::test]
    async fn fatal_without_skip_unavailable() {
        let fetcher = Arc::new(TransportErrFetcher(
            "connection refused (os error 111) dialing 10.1.2.3:9443".to_string(),
        ));
        let fed = one_remote(fetcher, false, Duration::from_secs(5));
        let err = run(&fed)
            .await
            .expect_err("an unavailable remote fails the query");
        match err {
            QueryError::Federation { cluster, .. } => assert_eq!(cluster, "eu-west"),
            other => panic!("expected QueryError::Federation, got {other:?}"),
        }
    }

    /// S5: a histogram-bearing remote is a coverage gap, not corruption. With
    /// `skip_unavailable = true` it is skipped (partial + a warning naming the
    /// cluster and nothing internal), exactly like an unavailable remote --
    /// never the non-skippable "malformed response" hard fault.
    #[tokio::test]
    async fn histogram_remote_is_skippable_not_a_hard_fault() {
        let fed = one_remote(
            Arc::new(HistUnsupportedFetcher),
            true,
            Duration::from_secs(5),
        );
        let outcome = run(&fed)
            .await
            .expect("a histogram remote is skippable under skip_unavailable");
        assert!(
            outcome.partial,
            "coverage is partial when the remote is skipped"
        );
        assert_eq!(outcome.skipped, vec!["eu-west".to_string()]);
        assert_eq!(outcome.warnings.len(), 1);
        let w = &outcome.warnings[0];
        assert!(w.contains("eu-west"), "warning names the cluster: {w}");
        assert!(
            !w.contains("histogram"),
            "client warning is the stable redacted message, not the internal reason: {w}"
        );
    }

    /// S5: with `skip_unavailable = false` the histogram remote still fails the
    /// query typed (never silently dropped), but as a `Federation` error naming
    /// the cluster -- the truthful skippable path -- not a masked drop.
    #[tokio::test]
    async fn histogram_remote_without_skip_fails_typed() {
        let fed = one_remote(
            Arc::new(HistUnsupportedFetcher),
            false,
            Duration::from_secs(5),
        );
        let err = run(&fed)
            .await
            .expect_err("a histogram remote fails the query when not skippable");
        match err {
            QueryError::Federation { cluster, .. } => assert_eq!(cluster, "eu-west"),
            other => panic!("expected QueryError::Federation, got {other:?}"),
        }
    }
}
