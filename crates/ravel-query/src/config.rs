//! Engine-wide tunables (docs/query-engine.md "Budgets").

use std::time::Duration;

use ravel_types::cost_profile::StoreCostProfile;

/// Default cap on segments a single query may fan out over.
pub const DEFAULT_MAX_SEGMENTS: usize = 1024;
/// Default cap on distinct series a single query may materialize.
pub const DEFAULT_MAX_SERIES: usize = 10_000;
/// Default cap on total samples (summed across series, after cross-segment
/// dedup) a single query may materialize.
pub const DEFAULT_MAX_SAMPLES: usize = 10_000_000;
/// Default wall-clock deadline for a single query.
pub const DEFAULT_DEADLINE: Duration = Duration::from_secs(30);
/// Default bound on concurrent in-flight segment fetches per query.
pub const DEFAULT_FETCH_CONCURRENCY: usize = 8;
/// Default step for a subquery that omits its own (`expr[5m:]`), matching
/// Prometheus' global `evaluation_interval` default.
pub const DEFAULT_EVALUATION_INTERVAL: Duration = Duration::from_secs(60);
/// Numerator/denominator of the headroom factor applied to the per-shard
/// open-hour segment count when deriving the S3 request budget: 3/2, i.e. 50%
/// above the one-GET-per-recent-segment floor. That floor is the true cost of
/// a cold query over a busy shard's open hour; the extra half covers
/// per-segment fetch retries and the occasional cold-fetch footer read,
/// without widening the cap so far that it stops bounding a runaway query.
pub const REQUEST_BUDGET_HEADROOM_NUM: u64 = 3;
pub const REQUEST_BUDGET_HEADROOM_DEN: u64 = 2;

/// Shard-independent slack in the derived S3 request budget: resolve (catalog
/// manifest, fold, and token reads) plus the sealed-segment fetch tail, whose
/// count is bounded by `max_segments` and the catalog rather than by shard
/// count. Added once, not per shard.
pub const REQUEST_BUDGET_FIXED_OVERHEAD: u64 = 5_000;

/// Reference inputs for [`EngineConfig::default`]'s S3 request budget. The
/// running server does NOT use these: it derives the budget from its actual
/// `--shards` and ingest flush cadence (see [`derive_max_s3_requests`], wired
/// through `ravel-server`'s config path). They exist only so an `EngineConfig`
/// built with no deployment context (tests, alerting, other non-server
/// callers) still gets a sane multi-shard budget instead of a single-shard
/// one. ravel-ingest depends on ravel-query, so this crate cannot import
/// `IngestConfig`'s defaults to share them; ravel-server's reachability test
/// pins that the server threads the real values rather than these references.
pub const DEFAULT_BUDGET_REFERENCE_SHARDS: u32 = 4;
pub const DEFAULT_BUDGET_REFERENCE_FLUSH_DELAY: Duration = Duration::from_millis(500);

/// Derives the per-query S3 request budget from a deployment's shard count and
/// ingest flush cadence (ADR-0075 decisions 1 and 2):
///
/// ```text
/// budget = per_shard_allowance * shard_count + REQUEST_BUDGET_FIXED_OVERHEAD
/// per_shard_allowance = ceil(3600s / max_flush_delay) * NUM / DEN
/// ```
///
/// The request budget's cost is per shard-hour, not per query. A busy tenant
/// seals `ceil(3600s / max_flush_delay)` segments per shard per open hour
/// (7,200 at the 500ms default cadence), and a cold query over that hour GETs
/// each one, on every shard. The old flat 25,000 cap was correct only at 3
/// shards or fewer; at the default 4 it rejected the worst legitimate open
/// hour (4 x 7,200 = 28,800) before it could answer. Deriving from
/// `max_flush_delay` rather than hardcoding 7,200 means a deployment that
/// raises the flush delay (a supported cost lever) gets a correct cap with no
/// hand recomputation.
pub fn derive_max_s3_requests(shard_count: u32, max_flush_delay: Duration) -> u64 {
    // Guard a zero/absurd cadence: a zero delay is not a real configuration,
    // but dividing by it would panic. Clamp to one shard for the same reason a
    // zero-shard deployment is nonsensical.
    let flush_ms = max_flush_delay.as_millis().max(1);
    // ceil(3_600_000ms / flush_ms): the most segments one shard can seal in an
    // open hour, which is the most GETs a cold query pays for that shard.
    let segments_per_shard_hour = 3_600_000u128.div_ceil(flush_ms) as u64;
    let per_shard_allowance = segments_per_shard_hour.saturating_mul(REQUEST_BUDGET_HEADROOM_NUM)
        / REQUEST_BUDGET_HEADROOM_DEN;
    per_shard_allowance
        .saturating_mul(u64::from(shard_count.max(1)))
        .saturating_add(REQUEST_BUDGET_FIXED_OVERHEAD)
}

/// A per-tenant cap on the total S3 bytes a single query may scan, or an
/// explicit opt-in to no cap at all (ADR-0061 decision 1).
///
/// Mirrors `ravel_ingest::admission::CountLimit`'s shape deliberately: this
/// is the same enum operators already learned for ingest admission limits,
/// applied to a query-side resource. Enforcement is a typed error, never a
/// truncation; `Unlimited` is the explicit, config-review-visible way to opt
/// out of the cap rather than a silent absence of one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteLimit {
    Bounded(u64),
    Unlimited,
}

impl ByteLimit {
    /// True when `bytes_scanned` has passed a bounded cap. `Unlimited` never
    /// trips, so a caller that does not opt in behaves exactly as before this
    /// limit existed.
    pub fn is_exceeded_by(self, bytes_scanned: u64) -> bool {
        match self {
            ByteLimit::Bounded(max) => bytes_scanned > max,
            ByteLimit::Unlimited => false,
        }
    }
}

/// A per-tenant cap on the total S3 requests a single query may issue, or an
/// explicit opt-in to no cap at all (ADR-0073 decision 3). Mirrors
/// [`ByteLimit`]'s shape: the recent-hour exemption from `max_segments`
/// (ADR-0073 decision 2) needs a governor that is not a count check, and this
/// is that governor, enforced the same incremental way the bytes-scanned
/// budget already is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestLimit {
    Bounded(u64),
    Unlimited,
}

impl RequestLimit {
    /// True when `requests` has passed a bounded cap. `Unlimited` never
    /// trips.
    pub fn is_exceeded_by(self, requests: u64) -> bool {
        match self {
            RequestLimit::Bounded(max) => requests > max,
            RequestLimit::Unlimited => false,
        }
    }
}

/// Default fetch bound ([`EngineConfig::logs_max_fetch_run_bytes`], ADR-0996
/// decision 2). Bounds one covering GET's length: an object at or under it is
/// read in a single [`crate::log_fetcher`] covering GET, and a larger one is
/// read as `ceil(size / bound)` sequential covering sub-range GETs, so no
/// single request moves more than this many bytes.
pub const DEFAULT_LOG_MAX_FETCH_RUN_BYTES: u64 = 64 * 1024 * 1024;

/// Bytes in one GiB, the unit both of a [`StoreCostProfile`]'s per-GiB prices
/// are quoted in. The cost-based derivation below needs a per-byte rate, so it
/// multiplies the per-request price by this before dividing by the per-GiB
/// byte price (ADR-0996 decision 2, "multiplies BEFORE it divides, in u128").
const BYTES_PER_GIB: u128 = 1 << 30;

/// The operator's logs fetch-policy intent (ADR-0996 decision 2). One knob
/// (`--logs-fetch-policy`) that resolves, at startup, to the byte-denominated
/// request cost the fetch layer already runs on
/// ([`crate::BlockRangeFetcher`]'s `request_cost_bytes`) plus the routing
/// threshold. It never reaches the fetch layer as a policy: prices and intent
/// live here, the fetch layer learns only byte quantities (ADR-0904's layering,
/// preserved).
///
/// The policy must never be derivable from query text, headers, or tickets
/// (ADR-0904 decision 4, inverted: under request billing a tenant forcing
/// `ByteMinimal` per query would multiply the deployment's request bill). It is
/// an operator surface only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LogsFetchPolicy {
    /// Minimize request count: saturate the exchange rate so every object is
    /// read whole in one covering GET, no probe and no ranged read. The
    /// cost-preferring shape on an intra-region deployment where transfer is
    /// free.
    RequestMinimal,
    /// ADR-0904's byte-minimizing behaviour, byte for byte for objects at or
    /// under the fetch bound: the derived latency break-even request cost, with
    /// ranged reads wherever they save more bytes than a request costs. Kept for
    /// egress-billed and network-constrained deployments.
    ByteMinimal,
    /// Derive the request cost from the active [`StoreCostProfile`]. At the
    /// reference (intra-region) profile this resolves to `RequestMinimal`
    /// behaviour; at egress prices it resolves to a small byte cost the floors
    /// clamp. The default, so a reference deployment gets request-minimal
    /// fetching with no operator action.
    #[default]
    CostBased,
}

impl LogsFetchPolicy {
    /// The policy name as it appears on the `--logs-fetch-policy` flag and in a
    /// provenance stamp.
    pub fn as_str(self) -> &'static str {
        match self {
            LogsFetchPolicy::RequestMinimal => "request-minimal",
            LogsFetchPolicy::ByteMinimal => "byte-minimal",
            LogsFetchPolicy::CostBased => "cost-based",
        }
    }
}

/// A resolved fetch policy: the byte quantities and routing decisions the fetch
/// layer runs on (ADR-0996 decision 2), plus the facts a startup path logs.
///
/// [`resolve_logs_fetch`] produces this from the policy, the active profile, and
/// the explicit ADR-0904 overrides. The server (task 996-5) hands
/// [`Self::request_cost_bytes`] and [`Self::block_range_threshold`] to the
/// fetcher and logs [`Self::overridden_block_range_threshold`] and
/// [`Self::saturated_profile`]; nothing here reads a price.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedLogsFetch {
    /// The byte-denominated request cost, the single quantity every
    /// range-vs-whole-object decision in the fetch layer is driven from
    /// ([`EngineConfig::logs_request_cost_bytes`]). `u64::MAX` under
    /// request-minimal (and a cost-based derivation whose byte price is zero or
    /// near-zero), which saturates every derived crossover to whole-object.
    pub request_cost_bytes: u64,
    /// The routing threshold ([`EngineConfig::logs_block_range_threshold`]).
    /// Saturated to `u64::MAX` whenever the resolved rate saturates, so no
    /// object is ever routed to the ranged path, overriding an explicit
    /// `--logs-block-range-threshold`.
    pub block_range_threshold: u64,
    /// Set to the operator's explicit `--logs-block-range-threshold` when the
    /// resolution overrode it, so the startup path can log the overridden flag
    /// (ADR-0996 decision 2). `None` when no explicit threshold was set or the
    /// resolution left it in force.
    pub overridden_block_range_threshold: Option<u64>,
    /// Set to the active profile's name when a cost-based derivation saturated
    /// the rate at `u64::MAX`, so the startup path can log the saturation naming
    /// the profile. `None` otherwise.
    pub saturated_profile: Option<String>,
}

/// Resolve a [`LogsFetchPolicy`] to the byte quantities and routing decisions
/// the fetch layer runs on (ADR-0996 decision 2).
///
/// This is the one place a price becomes a byte rate; the resulting
/// [`ResolvedLogsFetch`] carries only bytes and booleans, so the fetch layer
/// never learns a price (ADR-0904 layering). The startup path (task 996-5)
/// calls this and hands the byte quantities to the fetcher.
///
/// Precedence, from ADR-0996 decision 2 and its ADR-0904 alignment:
///
/// - An explicit `--logs-request-cost-bytes` (`explicit_request_cost_bytes`)
///   WINS over policy for the rate: policy is the intent layer, the byte flag
///   the expert escape hatch.
/// - A SATURATED resolved rate overrides BOTH routing thresholds, including an
///   explicitly set `--logs-block-range-threshold`. A rate of `u64::MAX` means
///   "one covering GET per object, always", and the fetch layer cannot express
///   that through the rate alone: `LogSegmentFetcher::with_block_range_threshold`
///   pins the inner crossover to the outer flag's value, bypassing the
///   `5 x request_cost` derivation entirely, so a threshold left at 512 KiB
///   would keep sending narrow projections of larger objects down the ranged
///   path through `ranged_projection_pays`. Cost-based at a free-byte profile
///   resolves to exactly that rate, so it must route exactly the way
///   request-minimal does; the override is therefore keyed on the rate, not on
///   the policy that produced it.
/// - `RequestMinimal` overrides the routing threshold even when an explicit
///   `--logs-request-cost-bytes` replaced its saturated rate: the byte flag is
///   an escape hatch for the rate, not for the routing intent.
///
/// The cost-based derivation multiplies before it divides, in `u128`:
/// `get_class_nanodollars * BYTES_PER_GIB / (transfer + retrieval)`,
/// floor-rounded with a one-byte minimum and saturated high at `u64::MAX`. It
/// saturates to request-minimal only when BOTH byte prices are zero. The result
/// is NOT clamped to the fetch bound (that would let a projection saving more
/// than the bound re-select ranged routing under an effectively request-minimal
/// policy); the 64 KiB gap and 512 KiB crossover floors are applied downstream
/// in the fetch layer.
pub fn resolve_logs_fetch(
    policy: LogsFetchPolicy,
    profile: &StoreCostProfile,
    explicit_request_cost_bytes: Option<u64>,
    configured_request_cost_bytes: u64,
    configured_block_range_threshold: u64,
    explicit_block_range_threshold: Option<u64>,
) -> ResolvedLogsFetch {
    // The rate: an explicit byte flag wins over policy; otherwise the policy
    // decides. Only a cost-based derivation can saturate for a numeric reason
    // worth logging.
    let (request_cost_bytes, saturated_profile) = match explicit_request_cost_bytes {
        Some(explicit) => (explicit, None),
        None => match policy {
            LogsFetchPolicy::RequestMinimal => (u64::MAX, None),
            // byte-minimal is today's behaviour byte for byte, which includes a
            // configured (non-default) `--logs-request-cost-bytes`: ADR-0904's
            // knob keeps its meaning under this policy rather than being
            // silently replaced by the compiled default.
            LogsFetchPolicy::ByteMinimal => (configured_request_cost_bytes, None),
            LogsFetchPolicy::CostBased => resolve_cost_based_rate(profile),
        },
    };

    // Routing: a saturated rate saturates the threshold too, whichever policy
    // produced it, overriding an explicit flag (which is then logged).
    let saturates_routing =
        request_cost_bytes == u64::MAX || matches!(policy, LogsFetchPolicy::RequestMinimal);
    let (block_range_threshold, overridden_block_range_threshold) = if saturates_routing {
        (u64::MAX, explicit_block_range_threshold)
    } else {
        (configured_block_range_threshold, None)
    };

    ResolvedLogsFetch {
        request_cost_bytes,
        block_range_threshold,
        overridden_block_range_threshold,
        saturated_profile,
    }
}

/// The cost-based byte rate and, when it saturated at `u64::MAX`, the profile
/// name to log. `get_class_nanodollars * BYTES_PER_GIB / (transfer + retrieval)`
/// in `u128`, floored at one byte, saturated high; both byte prices zero
/// saturates to request-minimal.
fn resolve_cost_based_rate(profile: &StoreCostProfile) -> (u64, Option<String>) {
    let byte_price = u128::from(profile.transfer_nanodollars_per_gib)
        .saturating_add(u128::from(profile.retrieval_nanodollars_per_gib));
    if byte_price == 0 {
        // Free bytes: a saved request is worth an unbounded number of free
        // bytes, so whole-object always. The reference profile lands here, which
        // is why cost-based defaults to request-minimal behaviour there.
        return (u64::MAX, Some(profile.name.clone()));
    }
    let quotient = u128::from(profile.get_class_nanodollars) * BYTES_PER_GIB / byte_price;
    match u64::try_from(quotient) {
        // Floor at one byte so a sub-nanodollar-per-byte price can never resolve
        // to a zero rate (which would make every crossover trivially true).
        Ok(rate) => (rate.max(1), None),
        // A near-free byte price against a large GET price overflows u64: an
        // astronomically high rate means whole-object always, so saturate and
        // log the profile.
        Err(_) => (u64::MAX, Some(profile.name.clone())),
    }
}

/// Why an [`EngineConfig`] could not be resolved into fetch-layer quantities
/// (ADR-0996 decision 2). A rejected value is always one of these, never a
/// panic or a silent clamp.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum EngineConfigError {
    /// [`EngineConfig::logs_max_fetch_run_bytes`] was zero. The segmented
    /// covering fallback divides the object size by it, so zero is refused
    /// rather than dividing by zero.
    #[error(
        "logs_max_fetch_run_bytes must be non-zero: the segmented covering fallback divides by it"
    )]
    ZeroFetchBound,
}

/// [`crate::QueryEngine`] resource limits and concurrency. Every limit is
/// enforced as a typed error (docs/query-engine.md "never silent partial
/// results"), never a truncation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineConfig {
    pub max_segments: usize,
    pub max_series: usize,
    pub max_samples: usize,
    /// Per-tenant cap on total S3 bytes a single query may scan, checked once
    /// per completed segment fetch inside the engine's two fetch fan-outs
    /// (`fetch_all_series` and `fetch_all_samples_and_histograms`), the stage
    /// that owns segment concurrency, so a tripped budget cancels the
    /// remaining in-flight fetches mid-scan (ADR-0061 decision 1). Defaults to
    /// [`ByteLimit::Unlimited`]: a bounded default would silently start
    /// rejecting existing deployments' large-but-legitimate queries on
    /// upgrade with no config change, so opting in is explicit.
    pub max_bytes_scanned: ByteLimit,
    /// Per-tenant cap on total S3 requests a single query may issue, checked
    /// at the same incremental points as `max_bytes_scanned` (ADR-0073
    /// decision 3). Governs the cost of segments exempted from `max_segments`
    /// by decision 2 (recent and token-resolved). The running server sets this
    /// to a value DERIVED from its shard count and flush cadence
    /// (ADR-0075, [`derive_max_s3_requests`]); [`EngineConfig::default`] uses
    /// the derivation at [`DEFAULT_BUDGET_REFERENCE_SHARDS`] shards as a
    /// no-deployment-context fallback.
    pub max_s3_requests: RequestLimit,
    pub deadline: Duration,
    pub fetch_concurrency: usize,
    /// Step for a subquery that does not specify its own (`expr[5m:]`).
    pub default_evaluation_interval: Duration,
    /// Object size above which a logs scan reads only the pruning-relevant
    /// blocks of an RLOG object instead of the whole object (ADR-0107), i.e.
    /// [`crate::LogSegmentFetcher::with_block_range_threshold`]. Not an engine
    /// limit like the fields above: it rides here because this is the one config
    /// the server folds its query flags into and hands to every fetcher it
    /// constructs (`services/ravel-server/src/query.rs`'s `build_sql_state`),
    /// which is where the logs fetcher is built.
    ///
    /// Defaults to [`crate::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD`] (512 KiB).
    /// `u64::MAX` reads every object whole, the pre-ADR-0107 behavior and the
    /// mitigation for an operator who hits a regression on the block-range path;
    /// `0` sends every object through it.
    pub logs_block_range_threshold: u64,
    /// Cost of one object-store round trip, denominated in transfer bytes: a
    /// saved request is worth this many saved bytes (ADR-0904 decision 1). Like
    /// `logs_block_range_threshold` this is not an engine limit; it rides here
    /// because this is the config the server folds its query flags into and
    /// hands to every fetcher it builds.
    ///
    /// A property of the store and the instance (round-trip latency and
    /// single-stream bandwidth at the fetch concurrency in use), not of the
    /// RLOG format, which is why it is configurable rather than frozen. One
    /// value drives three derived decisions in the logs fetch layer, so
    /// recalibrating the store recalibrates all of them at once: the coalescing
    /// gap, the pre-probe whole-object crossover, and the projection routing of
    /// the whole-segment fast path (#887).
    ///
    /// Defaults to [`crate::DEFAULT_LOG_REQUEST_COST_BYTES`], whose doc comment
    /// carries the derivation. Raising it above the largest object a deployment
    /// writes collapses all three decisions to whole-object reads.
    pub logs_request_cost_bytes: u64,
    /// The operator's fetch-policy intent (ADR-0996 decision 2), resolved at
    /// startup by [`resolve_logs_fetch`] into [`Self::logs_request_cost_bytes`]
    /// and [`Self::logs_block_range_threshold`]. Carried here, like the two
    /// fields it resolves into, because this is the config the server folds its
    /// query flags into and hands to every fetcher it builds. Defaults to
    /// [`LogsFetchPolicy::CostBased`].
    pub logs_fetch_policy: LogsFetchPolicy,
    /// The fetch bound (ADR-0996 decision 2): one covering GET's maximum length.
    /// An object at or under it is one covering GET; a larger one is read as
    /// `ceil(size / bound)` sequential covering sub-range GETs, so no single
    /// request moves more than this. Zero is refused at resolution
    /// ([`Self::validate`]): the segmented fallback divides by it. Defaults to
    /// [`DEFAULT_LOG_MAX_FETCH_RUN_BYTES`] (64 MiB).
    pub logs_max_fetch_run_bytes: u64,
}

impl EngineConfig {
    /// Refuse a configuration the fetch layer cannot run on (ADR-0996 decision
    /// 2). Called at startup resolution; a bad value is a typed
    /// [`EngineConfigError`], never a silent clamp.
    pub fn validate(&self) -> Result<(), EngineConfigError> {
        if self.logs_max_fetch_run_bytes == 0 {
            return Err(EngineConfigError::ZeroFetchBound);
        }
        Ok(())
    }
}

impl Default for EngineConfig {
    fn default() -> Self {
        EngineConfig {
            max_segments: DEFAULT_MAX_SEGMENTS,
            max_series: DEFAULT_MAX_SERIES,
            max_samples: DEFAULT_MAX_SAMPLES,
            max_bytes_scanned: ByteLimit::Unlimited,
            max_s3_requests: RequestLimit::Bounded(derive_max_s3_requests(
                DEFAULT_BUDGET_REFERENCE_SHARDS,
                DEFAULT_BUDGET_REFERENCE_FLUSH_DELAY,
            )),
            deadline: DEFAULT_DEADLINE,
            fetch_concurrency: DEFAULT_FETCH_CONCURRENCY,
            default_evaluation_interval: DEFAULT_EVALUATION_INTERVAL,
            logs_block_range_threshold: crate::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
            logs_request_cost_bytes: crate::DEFAULT_LOG_REQUEST_COST_BYTES,
            logs_fetch_policy: LogsFetchPolicy::default(),
            logs_max_fetch_run_bytes: DEFAULT_LOG_MAX_FETCH_RUN_BYTES,
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn default_request_cost_is_the_compiled_constant() {
        assert_eq!(
            EngineConfig::default().logs_request_cost_bytes,
            crate::DEFAULT_LOG_REQUEST_COST_BYTES
        );
        // The constant itself, not merely "nonzero": the knob ships with the
        // measured q20 latency break-even and no behavior change (ADR-0904
        // decision 5).
        assert_eq!(crate::DEFAULT_LOG_REQUEST_COST_BYTES, 1_887_437);
    }

    #[test]
    fn default_fetch_policy_is_cost_based_and_bound_is_64_mib() {
        let cfg = EngineConfig::default();
        assert_eq!(cfg.logs_fetch_policy, LogsFetchPolicy::CostBased);
        assert_eq!(
            cfg.logs_max_fetch_run_bytes,
            DEFAULT_LOG_MAX_FETCH_RUN_BYTES
        );
        assert_eq!(DEFAULT_LOG_MAX_FETCH_RUN_BYTES, 64 * 1024 * 1024);
    }

    /// The policy-to-rate mapping table pinned exactly (ADR-0996 decision 2's
    /// acceptance): saturate / default / profile-derived-with-floors /
    /// high-saturation boundary. Each row states the rate to the byte.
    #[test]
    fn policy_to_rate_mapping_table_is_pinned() {
        let reference = StoreCostProfile::reference();

        // request-minimal saturates the rate AND the routing threshold,
        // regardless of profile or an explicit block-range threshold. An
        // explicitly set threshold is reported for the startup log.
        let rm = resolve_logs_fetch(
            LogsFetchPolicy::RequestMinimal,
            &reference,
            None,
            crate::DEFAULT_LOG_REQUEST_COST_BYTES,
            crate::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
            Some(4096),
        );
        assert_eq!(rm.request_cost_bytes, u64::MAX, "request-minimal saturates");
        assert_eq!(rm.block_range_threshold, u64::MAX);
        assert_eq!(
            rm.overridden_block_range_threshold,
            Some(4096),
            "an explicitly set block-range threshold is overridden and logged"
        );
        assert_eq!(rm.saturated_profile, None);

        // byte-minimal keeps today's configured request cost byte for byte, and
        // leaves the routing threshold alone.
        let bm = resolve_logs_fetch(
            LogsFetchPolicy::ByteMinimal,
            &reference,
            None,
            crate::DEFAULT_LOG_REQUEST_COST_BYTES,
            crate::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
            Some(4096),
        );
        assert_eq!(bm.request_cost_bytes, crate::DEFAULT_LOG_REQUEST_COST_BYTES);
        assert_eq!(
            bm.block_range_threshold,
            crate::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD
        );
        assert_eq!(bm.overridden_block_range_threshold, None);

        // cost-based at the reference (intra-region, free bytes) profile
        // resolves to request-minimal behaviour: both byte prices zero saturate
        // the rate, naming the profile.
        let cb_ref = resolve_logs_fetch(
            LogsFetchPolicy::CostBased,
            &reference,
            None,
            crate::DEFAULT_LOG_REQUEST_COST_BYTES,
            crate::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
            None,
        );
        assert_eq!(cb_ref.request_cost_bytes, u64::MAX);
        assert_eq!(
            cb_ref.saturated_profile.as_deref(),
            Some("s3-intra-region-2026")
        );
        // ... and the routing threshold saturates WITH the rate. The fetch layer
        // pins its inner crossover to whatever threshold it is handed
        // (`with_block_range_threshold` sets `whole_object_threshold`, which
        // `effective_whole_object_threshold` then returns verbatim, bypassing the
        // `5 x request_cost` derivation and its floors), so leaving this at
        // 512 KiB would route a narrow projection of any larger object ranged and
        // deliver none of ADR-0996's outcome at the default policy.
        assert_eq!(cb_ref.block_range_threshold, u64::MAX);

        // cost-based at egress prices resolves to a small byte cost:
        //   400 * 2^30 / (90_000_000 + 10_000_000)
        //   = 429_496_729_600 / 100_000_000 = 4294 (floor). ~4.3 KB, the
        // ADR-0904 worked value, which the downstream floors then clamp.
        let egress = StoreCostProfile {
            name: "egress-billed".to_string(),
            put_class_nanodollars: 5_000,
            get_class_nanodollars: 400,
            delete_class_nanodollars: 0,
            transfer_nanodollars_per_gib: 90_000_000,
            retrieval_nanodollars_per_gib: 10_000_000,
        };
        let cb_egress = resolve_logs_fetch(
            LogsFetchPolicy::CostBased,
            &egress,
            None,
            crate::DEFAULT_LOG_REQUEST_COST_BYTES,
            crate::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
            None,
        );
        assert_eq!(
            cb_egress.request_cost_bytes, 4294,
            "profile-derived rate is get*2^30/(transfer+retrieval), floored"
        );
        assert_eq!(cb_egress.saturated_profile, None);
        // A finite rate leaves the routing threshold in force: only saturation
        // overrides it.
        assert_eq!(
            cb_egress.block_range_threshold,
            crate::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD
        );
        // The raw rate is well below both floors, so the downstream fetch layer
        // clamps it: the gap floors to 64 KiB and the crossover to 512 KiB.
        assert!(cb_egress.request_cost_bytes < crate::DEFAULT_LOG_COALESCE_GAP);
        assert!(cb_egress.request_cost_bytes < crate::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD);

        // transfer=0, retrieval>0 routes byte-minimally from the retrieval
        // price, NOT request-minimal: a naive per-byte pre-division would
        // truncate this to zero and be unreachable.
        //   400 * 2^30 / 10_000_000 = 429_496_729_600 / 10_000_000 = 42949.
        let retrieval_only = StoreCostProfile {
            name: "retrieval-only".to_string(),
            put_class_nanodollars: 5_000,
            get_class_nanodollars: 400,
            delete_class_nanodollars: 0,
            transfer_nanodollars_per_gib: 0,
            retrieval_nanodollars_per_gib: 10_000_000,
        };
        let cb_retrieval = resolve_logs_fetch(
            LogsFetchPolicy::CostBased,
            &retrieval_only,
            None,
            crate::DEFAULT_LOG_REQUEST_COST_BYTES,
            crate::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
            None,
        );
        assert_eq!(cb_retrieval.request_cost_bytes, 42949);
        assert_eq!(cb_retrieval.saturated_profile, None);
        assert_eq!(
            cb_retrieval.block_range_threshold,
            crate::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD
        );
    }

    /// byte-minimal is "today's behaviour, byte for byte", which includes a
    /// configured non-default `--logs-request-cost-bytes` (ADR-0904's knob). The
    /// resolution must thread the configured value through rather than
    /// substituting the compiled default, or selecting byte-minimal would
    /// silently discard the operator's calibration.
    ///
    /// Prove-the-test: resolve the byte-minimal arm from
    /// `crate::DEFAULT_LOG_REQUEST_COST_BYTES` instead of
    /// `configured_request_cost_bytes` and the first assertion reads 1_887_437
    /// against the expected 700_000.
    #[test]
    fn byte_minimal_keeps_a_configured_request_cost() {
        let reference = StoreCostProfile::reference();
        let configured = 700_000u64;
        assert_ne!(
            configured,
            crate::DEFAULT_LOG_REQUEST_COST_BYTES,
            "the fixture must differ from the default or it proves nothing"
        );
        let bm = resolve_logs_fetch(
            LogsFetchPolicy::ByteMinimal,
            &reference,
            None,
            configured,
            crate::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
            None,
        );
        assert_eq!(
            bm.request_cost_bytes, configured,
            "byte-minimal keeps the configured request cost, not the compiled default"
        );
        assert_eq!(
            bm.block_range_threshold,
            crate::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
            "a finite rate leaves the routing threshold in force"
        );

        // The profile is irrelevant to byte-minimal: the same configured value
        // resolves at an egress-billed profile too.
        let egress = StoreCostProfile {
            name: "egress-billed".to_string(),
            put_class_nanodollars: 5_000,
            get_class_nanodollars: 400,
            delete_class_nanodollars: 0,
            transfer_nanodollars_per_gib: 90_000_000,
            retrieval_nanodollars_per_gib: 10_000_000,
        };
        let bm_egress = resolve_logs_fetch(
            LogsFetchPolicy::ByteMinimal,
            &egress,
            None,
            configured,
            crate::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
            None,
        );
        assert_eq!(bm_egress.request_cost_bytes, configured);
    }

    /// A saturated rate saturates the routing threshold whichever policy
    /// produced it, and an explicitly set `--logs-block-range-threshold` is
    /// overridden and reported for the startup log -- exactly as the
    /// request-minimal arm already does.
    ///
    /// Prove-the-test: key the override on `matches!(policy,
    /// LogsFetchPolicy::RequestMinimal)` alone (the pre-fix condition) and both
    /// assertions below fail: the threshold reads 512 KiB and the overridden
    /// flag reads `None`.
    #[test]
    fn a_saturated_cost_based_rate_overrides_an_explicit_routing_threshold() {
        let reference = StoreCostProfile::reference();
        let r = resolve_logs_fetch(
            LogsFetchPolicy::CostBased,
            &reference,
            None,
            crate::DEFAULT_LOG_REQUEST_COST_BYTES,
            4096,
            Some(4096),
        );
        assert_eq!(r.request_cost_bytes, u64::MAX);
        assert_eq!(
            r.block_range_threshold,
            u64::MAX,
            "a saturated rate saturates the routing threshold too"
        );
        assert_eq!(
            r.overridden_block_range_threshold,
            Some(4096),
            "the overridden flag is reported for the startup log"
        );
    }

    /// The high-saturation boundary at a one-nanodollar-per-GiB byte price
    /// (ADR-0996 decision 2): the quotient `get * 2^30` crosses `u64::MAX` at
    /// `get = 2^34`. One below the boundary is a large finite rate; at the
    /// boundary it saturates and names the profile.
    #[test]
    fn cost_based_high_saturation_boundary_is_pinned() {
        let below = StoreCostProfile {
            name: "one-nd-per-gib-below".to_string(),
            put_class_nanodollars: 0,
            get_class_nanodollars: (1u64 << 34) - 1,
            delete_class_nanodollars: 0,
            transfer_nanodollars_per_gib: 1,
            retrieval_nanodollars_per_gib: 0,
        };
        let r = resolve_logs_fetch(
            LogsFetchPolicy::CostBased,
            &below,
            None,
            crate::DEFAULT_LOG_REQUEST_COST_BYTES,
            crate::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
            None,
        );
        // ((2^34 - 1) * 2^30) / 1 = 2^64 - 2^30, one GiB below u64::MAX+1: finite.
        assert_eq!(r.request_cost_bytes, u64::MAX - (1u64 << 30) + 1);
        assert_eq!(
            r.saturated_profile, None,
            "one below the boundary is finite"
        );
        assert_eq!(
            r.block_range_threshold,
            crate::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
            "a finite rate, however large, leaves the routing threshold in force"
        );

        let at = StoreCostProfile {
            name: "one-nd-per-gib-at".to_string(),
            get_class_nanodollars: 1u64 << 34,
            ..below.clone()
        };
        let r = resolve_logs_fetch(
            LogsFetchPolicy::CostBased,
            &at,
            None,
            crate::DEFAULT_LOG_REQUEST_COST_BYTES,
            crate::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
            None,
        );
        // 2^34 * 2^30 = 2^64 > u64::MAX: saturates, names the profile.
        assert_eq!(r.request_cost_bytes, u64::MAX);
        assert_eq!(r.saturated_profile.as_deref(), Some("one-nd-per-gib-at"));
        assert_eq!(
            r.block_range_threshold,
            u64::MAX,
            "the overflow saturation routes whole-object like the zero-price one"
        );
    }

    #[test]
    fn explicit_request_cost_bytes_wins_over_policy() {
        // The expert escape hatch: an explicit --logs-request-cost-bytes wins
        // over the policy's derived rate, even request-minimal's saturation.
        let profile = StoreCostProfile::reference();
        let r = resolve_logs_fetch(
            LogsFetchPolicy::RequestMinimal,
            &profile,
            Some(123_456),
            crate::DEFAULT_LOG_REQUEST_COST_BYTES,
            crate::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
            None,
        );
        assert_eq!(r.request_cost_bytes, 123_456);
        // request-minimal still overrides the routing threshold: only the rate
        // is an escape hatch, not the routing intent.
        assert_eq!(r.block_range_threshold, u64::MAX);

        // The same escape hatch under cost-based: an explicit finite rate
        // replaces the profile's saturated one, so nothing saturates and the
        // configured routing threshold stays in force.
        let cb = resolve_logs_fetch(
            LogsFetchPolicy::CostBased,
            &profile,
            Some(123_456),
            crate::DEFAULT_LOG_REQUEST_COST_BYTES,
            crate::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
            None,
        );
        assert_eq!(cb.request_cost_bytes, 123_456);
        assert_eq!(
            cb.block_range_threshold,
            crate::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD
        );
        assert_eq!(cb.saturated_profile, None);
    }

    #[test]
    fn zero_fetch_bound_is_refused_with_a_typed_error() {
        let mut cfg = EngineConfig::default();
        assert_eq!(cfg.validate(), Ok(()));
        cfg.logs_max_fetch_run_bytes = 0;
        assert_eq!(cfg.validate(), Err(EngineConfigError::ZeroFetchBound));
    }
}
