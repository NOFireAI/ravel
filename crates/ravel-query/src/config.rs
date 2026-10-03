//! Engine-wide tunables (docs/query-engine.md "Budgets").

use std::time::Duration;

use ravel_types::cost_profile::StoreCostProfile;

use crate::fetcher::MAX_GETS_PER_L0_SEGMENT_FETCH;

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
/// allowance when deriving the S3 request budget: 3/2, i.e. 50% above the
/// per-flush cost over [`covered_span`]. The extra half is the retry allowance
/// (ADR-0075, ADR-1306 decision 2), without widening the cap so far that it
/// stops bounding a runaway query.
pub const REQUEST_BUDGET_HEADROOM_NUM: u64 = 3;
pub const REQUEST_BUDGET_HEADROOM_DEN: u64 = 2;

/// Shard-independent slack in the derived S3 request budget: resolve (catalog
/// manifest, fold, and token reads) plus the sealed-segment fetch tail, whose
/// count is bounded by `max_segments` and the catalog rather than by shard
/// count. Added once, not per shard.
pub const REQUEST_BUDGET_FIXED_OVERHEAD: u64 = 5_000;

/// Cold requests one unsealed flush inside the query's range costs, per shard
/// (ADR-1306 decision 4), for a segment at or under the fetcher's 512 KiB
/// whole-object threshold; a larger segment pays a footer read and range reads
/// on top. `cold_recent_query_requests_per_unsealed_flush_by_phase` pins it.
/// It is not an upper bound above that threshold, where one flush costs at
/// least 3 requests (ADR-1306, "Amendment (2026-09-26)"); that bound is
/// [`MAX_REQUESTS_PER_UNSEALED_FLUSH`].
pub const REQUESTS_PER_UNSEALED_FLUSH: u64 = 2;

/// Most cold requests one unsealed flush inside the query's range costs, per
/// shard, at any object size: its commit-record GET plus the fetcher's
/// [`MAX_GETS_PER_L0_SEGMENT_FETCH`] (first GET, footer chase, catalog GET and
/// at most 4 page-range GETs), 8 in all. Retries are left to the headroom.
/// `cold_requests_per_unsealed_flush_above_whole_object_threshold` measures
/// flushes of several MiB against it.
pub const MAX_REQUESTS_PER_UNSEALED_FLUSH: u64 = 1 + MAX_GETS_PER_L0_SEGMENT_FETCH;

/// The per-flush term the derived budget sizes each unsealed flush at: the
/// larger of the measured small-flush cost and the ceiling above the
/// whole-object threshold, so ADR-1306 decision 2's per-flush condition holds
/// whatever size ingest flushes at.
pub const BUDGETED_REQUESTS_PER_UNSEALED_FLUSH: u64 =
    if MAX_REQUESTS_PER_UNSEALED_FLUSH > REQUESTS_PER_UNSEALED_FLUSH {
        MAX_REQUESTS_PER_UNSEALED_FLUSH
    } else {
        REQUESTS_PER_UNSEALED_FLUSH
    };

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

/// The shipped `RavelCatalogFoldStalled` alert's `for:` (ADR-1306 decision 1).
/// Its threshold is the seal margin, so the seal margin plus this is the time
/// from the last successful fold to the alert firing.
pub const FOLD_STALL_ALERT_FOR: Duration = Duration::from_secs(600);

/// Time between the fold-stall alert firing and a page reaching an operator:
/// scrape interval, rule evaluation interval and Alertmanager's `group_wait`
/// (ADR-1306 decision 1).
pub const ALERT_DELIVERY_SLACK: Duration = Duration::from_secs(300);

/// The open ingest hour a healthy fold's watermark leaves unsealed on top of
/// the seal margin (ADR-1306 decision 1, `healthy_tail_max`).
const OPEN_INGEST_HOUR: Duration = Duration::from_secs(3_600);

/// `CatalogConfig::default`'s `max_flush_lifetime`, 1 h: the reference input
/// for [`SealMargin::REFERENCE`] (ADR-1306 decision 3).
pub const REFERENCE_MAX_FLUSH_LIFETIME: Duration =
    Duration::from_nanos(ravel_catalog::DEFAULT_MAX_FLUSH_LIFETIME_NS.unsigned_abs());
/// `CatalogConfig::default`'s `clock_skew_allowance`, 5 m: the reference input
/// for [`SealMargin::REFERENCE`] (ADR-1306 decision 3).
pub const REFERENCE_CLOCK_SKEW_ALLOWANCE: Duration =
    Duration::from_nanos(ravel_catalog::DEFAULT_CLOCK_SKEW_ALLOWANCE_NS.unsigned_abs());
/// `CatalogConfig::default`'s `fold_safety_margin`, 15 m: the reference input
/// for [`SealMargin::REFERENCE`] (ADR-1306 decision 3).
pub const REFERENCE_FOLD_SAFETY_MARGIN: Duration =
    Duration::from_nanos(ravel_catalog::DEFAULT_FOLD_SAFETY_MARGIN_NS.unsigned_abs());

/// The three durations whose sum is the fold's seal margin, taken from the
/// `CatalogConfig` the fold and resolve run with (ADR-1306 decision 3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealMargin {
    pub max_flush_lifetime: Duration,
    pub clock_skew_allowance: Duration,
    pub fold_safety_margin: Duration,
}

impl SealMargin {
    /// The catalog's compiled-in defaults, 1 h + 5 m + 15 m (ADR-1306
    /// decision 3). What [`derive_max_s3_requests`] and
    /// [`EngineConfig::default`] size from when no catalog config is at hand.
    pub const REFERENCE: SealMargin = SealMargin {
        max_flush_lifetime: REFERENCE_MAX_FLUSH_LIFETIME,
        clock_skew_allowance: REFERENCE_CLOCK_SKEW_ALLOWANCE,
        fold_safety_margin: REFERENCE_FOLD_SAFETY_MARGIN,
    };

    /// The seal margin the given catalog config folds with (ADR-1306
    /// decision 3). A negative duration is not a real configuration and
    /// counts as zero.
    pub fn from_catalog_config(config: &ravel_catalog::CatalogConfig) -> SealMargin {
        let nanos = |ns: i64| Duration::from_nanos(u64::try_from(ns).unwrap_or(0));
        SealMargin {
            max_flush_lifetime: nanos(config.max_flush_lifetime_ns),
            clock_skew_allowance: nanos(config.clock_skew_allowance_ns),
            fold_safety_margin: nanos(config.fold_safety_margin_ns),
        }
    }

    /// `max_flush_lifetime + clock_skew_allowance + fold_safety_margin`
    /// (ADR-1306 decision 1).
    pub fn total(&self) -> Duration {
        self.max_flush_lifetime
            .saturating_add(self.clock_skew_allowance)
            .saturating_add(self.fold_safety_margin)
    }
}

impl Default for SealMargin {
    fn default() -> Self {
        SealMargin::REFERENCE
    }
}

/// The longest unsealed tail a healthy catalog carries, the seal margin plus
/// the open hour (ADR-1306 decision 1, `healthy_tail_max`).
pub fn healthy_tail_max(seal_margin: SealMargin) -> Duration {
    seal_margin.total().saturating_add(OPEN_INGEST_HOUR)
}

/// `services/ravel-server/src/fold.rs`'s `DEFAULT_FOLD_INTERVAL`, 5 minutes:
/// how long a keeping-up fold waits between cycles, so how long a tail keeps
/// growing after the fold that last shortened it. Named here rather than
/// imported: ravel-server depends on ravel-query, not the other way round;
/// ravel-server pins the two equal in a test.
pub const REFERENCE_FOLD_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// `CatalogConfig::default`'s `head_cache_ttl`
/// (`ravel_catalog::DEFAULT_HEAD_CACHE_TTL_NS`), 30 s: how stale the HEAD a
/// resolve reads through the TTL cache may be, so how far behind the fold's
/// real watermark the watermark a resolve resolves against may be.
pub const REFERENCE_HEAD_CACHE_TTL: Duration =
    Duration::from_nanos(ravel_catalog::DEFAULT_HEAD_CACHE_TTL_NS.unsigned_abs());

/// The longest unsealed tail a catalog whose fold is keeping up can present to
/// one resolve, and so the tail at which a request-budget refusal starts naming
/// fold lag (ADR-1306 decision 6, "Amendment (2026-09-27, #1306)").
///
/// Three terms, not one. `healthy_tail_max` bounds the tail a fold leaves at the
/// instant it runs. The fold then does not run again for `fold_interval`, and
/// the tail grows one second per second meanwhile. The watermark a resolve sees
/// is read from a HEAD served through a `head_cache_ttl` cache, so it may be
/// that much older again. Classifying against `healthy_tail_max` alone blames a
/// fold that is keeping up for the last `fold_interval + head_cache_ttl` of
/// every cycle.
pub fn fold_lag_tail_threshold(
    seal_margin: SealMargin,
    fold_interval: Duration,
    head_cache_ttl: Duration,
) -> Duration {
    healthy_tail_max(seal_margin)
        .saturating_add(fold_interval)
        .saturating_add(head_cache_ttl)
}

/// The span the per-shard request allowance is sized from (ADR-1306
/// decisions 1 and 2): the healthy tail plus the time a stalled fold takes to
/// page, `healthy_tail_max + seal_margin + FOLD_STALL_ALERT_FOR +
/// ALERT_DELIVERY_SLACK`. 14,100 s at the reference seal margin.
pub fn covered_span(seal_margin: SealMargin) -> Duration {
    healthy_tail_max(seal_margin)
        .saturating_add(seal_margin.total())
        .saturating_add(FOLD_STALL_ALERT_FOR)
        .saturating_add(ALERT_DELIVERY_SLACK)
}

/// The shard-independent parts of the derived request budget (ADR-1306
/// decision 1), kept apart so a caller can rebuild the budget with a
/// different headroom.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestBudgetParts {
    /// Requests one shard's unsealed flushes over [`covered_span`] cost
    /// before headroom: `ceil(covered_span / max_flush_delay) x
    /// BUDGETED_REQUESTS_PER_UNSEALED_FLUSH`.
    pub per_shard_allowance: u64,
    /// Headroom multiplier numerator applied to the per-shard allowance.
    pub headroom_num: u64,
    /// Headroom multiplier denominator; zero is treated as one.
    pub headroom_den: u64,
    /// Requests that do not scale with flushes, added once.
    pub fixed_overhead: u64,
}

impl RequestBudgetParts {
    /// `per_shard_allowance x headroom x shard_count + fixed_overhead`
    /// (ADR-1306 decision 1), with a zero shard count read as one.
    pub fn budget(&self, shard_count: u32) -> u64 {
        let per_shard =
            self.per_shard_allowance.saturating_mul(self.headroom_num) / self.headroom_den.max(1);
        per_shard
            .saturating_mul(u64::from(shard_count.max(1)))
            .saturating_add(self.fixed_overhead)
    }
}

/// The budget parts for a flush cadence and seal margin, with the ADR-0075
/// 3/2 headroom and the 5,000 fixed overhead (ADR-1306 decisions 1 and 4).
pub fn request_budget_parts(
    max_flush_delay: Duration,
    seal_margin: SealMargin,
) -> RequestBudgetParts {
    // A zero cadence is not a real configuration; clamp it rather than divide
    // by zero.
    let flush_ns = max_flush_delay.as_nanos().max(1);
    let flushes = covered_span(seal_margin).as_nanos().div_ceil(flush_ns);
    let flushes = u64::try_from(flushes).unwrap_or(u64::MAX);
    RequestBudgetParts {
        per_shard_allowance: flushes.saturating_mul(BUDGETED_REQUESTS_PER_UNSEALED_FLUSH),
        headroom_num: REQUEST_BUDGET_HEADROOM_NUM,
        headroom_den: REQUEST_BUDGET_HEADROOM_DEN,
        fixed_overhead: REQUEST_BUDGET_FIXED_OVERHEAD,
    }
}

/// The derived per-query S3 request budget for a deployment's shard count,
/// flush cadence and seal margin (ADR-1306 decisions 1 to 3).
pub fn derive_max_s3_requests_for(
    shard_count: u32,
    max_flush_delay: Duration,
    seal_margin: SealMargin,
) -> u64 {
    request_budget_parts(max_flush_delay, seal_margin).budget(shard_count)
}

/// Derives the per-query S3 request budget from a deployment's shard count and
/// ingest flush cadence at the catalog's reference seal margin (ADR-1306
/// decisions 1 to 3, on top of ADR-0075 decisions 1 and 2):
///
/// ```text
/// budget = per_shard_allowance * NUM / DEN * shard_count + REQUEST_BUDGET_FIXED_OVERHEAD
/// per_shard_allowance = ceil(covered_span / max_flush_delay)
///                       * BUDGETED_REQUESTS_PER_UNSEALED_FLUSH
/// ```
///
/// The cost is per shard and per unsealed flush a query resolves, sized at the
/// per-flush ceiling so a flush above the whole-object threshold fits too. The
/// span is [`covered_span`], the longest tail a healthy catalog carries plus
/// the time a stalled fold takes to page, so a query is not refused for fold
/// lag before the fold-stall alert reaches an operator: 343,400 at 4 shards
/// and a 2 s cadence. Deriving from `max_flush_delay` means a deployment that raises the
/// flush delay (a supported cost lever) gets a correct cap with no hand
/// recomputation. A caller with a non-default seal margin uses
/// [`derive_max_s3_requests_for`].
pub fn derive_max_s3_requests(shard_count: u32, max_flush_delay: Duration) -> u64 {
    derive_max_s3_requests_for(shard_count, max_flush_delay, SealMargin::REFERENCE)
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
    /// Derive the request cost from the active [`StoreCostProfile`]: the
    /// larger of its price term and its time term ([`RateTerm`], ADR-2414
    /// decision A3). At the reference (intra-region) profile the price term
    /// saturates and the time term gives 6,300,000 bytes per request, so a
    /// narrow projection of an object above the projection break-even
    /// ([`ResolvedLogsFetch::projection_break_even_bytes`]) reads ranged and
    /// every object at or below it reads whole; at egress prices it resolves
    /// to a small byte cost the floors clamp. The default.
    #[default]
    CostBased,
    /// Trade money for wall-clock (issue #1196): resolves the rate and routing
    /// threshold EXACTLY as [`Self::ByteMinimal`] (ADR-0904's ranged behaviour).
    /// It is an intent, not a tuning constant: it says "spend requests to save
    /// wall time", and the engine decides how; it carries no concurrency
    /// default of its own (ADR-1196). The trade is a ratio measured at a named
    /// commit, not a property of the policy. At `740f94b97`, on the reference
    /// corpus (#1185, 42 statements, true cold, warm-up-empty) over 3 reps at
    /// [`LATENCY_FIRST_MEASURED_CONCURRENCY`]: 5.30x the GET requests (570,752
    /// vs 107,781) for 52% less cold wall-clock (235.7s vs 493.0s mean), with a
    /// per-rep range of 50.3% to 54.2%. Against the `cost-based` default at
    /// `s3-intra-region-2026` prices, where at that commit free transfer and
    /// retrieval saturated the cost-based rate to whole-object reads (the
    /// profile carried no time term yet, ADR-2414 decision A3). Cost-first stays
    /// the default because it is right for the bill; this is an operator
    /// opt-in for the deployments where the clock matters more than the
    /// request bill, at a concurrency the operator raises explicitly. It
    /// carries a memory precondition: in-flight fetch memory at the
    /// concurrency it needs to pay off is not bounded by a process-wide
    /// budget today (#1170, #1007).
    LatencyFirst,
}

/// The process-wide object-store GET concurrency
/// [`LogsFetchPolicy::LatencyFirst`]'s cold-time measurement ran at
/// (issue #1196): a documentation constant only, naming the concurrency
/// `--fetch-concurrency 256` set for the GET permits, the SQL partition
/// count, and the PromQL fan-out together. `latency-first` resolves no
/// concurrency default from this value; an operator who wants the measured
/// trade sets `--store-get-concurrency` (and, for SQL,
/// `--sql-partition-count`) to it explicitly.
pub const LATENCY_FIRST_MEASURED_CONCURRENCY: usize = 256;

impl LogsFetchPolicy {
    /// The policy name as it appears on the `--logs-fetch-policy` flag and in a
    /// provenance stamp.
    pub fn as_str(self) -> &'static str {
        match self {
            LogsFetchPolicy::RequestMinimal => "request-minimal",
            LogsFetchPolicy::ByteMinimal => "byte-minimal",
            LogsFetchPolicy::CostBased => "cost-based",
            LogsFetchPolicy::LatencyFirst => "latency-first",
        }
    }
}

/// Which term of the active [`StoreCostProfile`] produced a cost-based rate
/// (ADR-2414 decision A3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateTerm {
    /// The price term, `get_class_nanodollars * 2^30 / (transfer +
    /// retrieval)`: finite, and at least the time term when the profile has
    /// one.
    Price,
    /// The time term, [`StoreCostProfile::request_cost_bytes_from_timings`]:
    /// the price term saturated, or was smaller.
    Time,
    /// Neither term is finite: the profile records no byte price (or one so
    /// small the quotient overflows) and no timings, or its timings' product
    /// itself saturates, so the rate is `u64::MAX`.
    Saturated,
}

impl RateTerm {
    /// The term's name as the startup stamp prints it.
    pub fn as_str(self) -> &'static str {
        match self {
            RateTerm::Price => "price",
            RateTerm::Time => "time",
            RateTerm::Saturated => "saturated",
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
    /// request-minimal, and under a cost-based derivation whose
    /// [`Self::rate_term`] is [`RateTerm::Saturated`], which saturates every
    /// derived crossover to whole-object.
    pub request_cost_bytes: u64,
    /// The profile term a cost-based derivation took [`Self::request_cost_bytes`]
    /// from. `None` when no derivation ran: an explicit
    /// `--logs-request-cost-bytes` set the rate, or the policy is not
    /// cost-based.
    pub rate_term: Option<RateTerm>,
    /// The routing threshold ([`EngineConfig::logs_block_range_threshold`]).
    /// Saturated to `u64::MAX` whenever the resolved rate saturates, so no
    /// object is ever routed to the ranged path, overriding an explicit
    /// `--logs-block-range-threshold`.
    pub block_range_threshold: u64,
    /// The projection break-even under cost-based with a finite rate
    /// (ADR-2414 decision A3): `max(block_range_threshold,
    /// WHOLE_OBJECT_REQUEST_MULTIPLE * request_cost_bytes)`, the bytes a
    /// narrow projection must save before the fast path reads it ranged and
    /// before the ranged fetch skips its whole-object size crossover
    /// ([`EngineConfig::logs_projection_break_even_bytes`]). `None` under every
    /// other policy and under a saturated rate, where the break-even stays the
    /// routing threshold verbatim.
    pub projection_break_even_bytes: Option<u64>,
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

impl ResolvedLogsFetch {
    /// Where [`Self::request_cost_bytes`] came from, as a startup or bench
    /// stamp prints it: `flag` when `explicit_request_cost_bytes` (the
    /// operator set `--logs-request-cost-bytes`), else the [`RateTerm`] name
    /// of a cost-based derivation, else `none` (the policy set the rate
    /// without one).
    pub fn rate_term_label(&self, explicit_request_cost_bytes: bool) -> &'static str {
        if explicit_request_cost_bytes {
            return "flag";
        }
        self.rate_term.map_or("none", RateTerm::as_str)
    }
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
///   that records no timings resolves to exactly that rate, so it must route
///   exactly the way
///   request-minimal does; the override is therefore keyed on the rate, not on
///   the policy that produced it.
/// - `RequestMinimal` overrides the routing threshold even when an explicit
///   `--logs-request-cost-bytes` replaced its saturated rate: the byte flag is
///   an escape hatch for the rate, not for the routing intent.
///
/// The cost-based derivation takes the larger of two terms (ADR-2414 decision
/// A3). The price term multiplies before it divides, in `u128`:
/// `get_class_nanodollars * BYTES_PER_GIB / (transfer + retrieval)`,
/// floor-rounded with a one-byte minimum, and it saturates when both byte
/// prices are zero or the quotient reaches `u64::MAX`. The time term is
/// [`StoreCostProfile::request_cost_bytes_from_timings`]. A saturated price
/// term yields to the time term, so the rate saturates only when the profile
/// records neither a byte price nor timings ([`RateTerm::Saturated`]). The
/// result is NOT clamped to the fetch bound (that would let a projection saving
/// more than the bound re-select ranged routing under an effectively
/// request-minimal policy); the 64 KiB gap and 512 KiB crossover floors are
/// applied downstream in the fetch layer.
///
/// Under cost-based with a finite rate the projection break-even is
/// `max(block_range_threshold, WHOLE_OBJECT_REQUEST_MULTIPLE *
/// request_cost_bytes)`, which an explicit `--logs-request-cost-bytes` feeds
/// too: the routing threshold bounds which objects take the block-range path
/// at all, and the break-even bounds which narrow projections are worth a
/// ranged read. The other policies keep the routing threshold as the
/// break-even.
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
    let (request_cost_bytes, rate_term, saturated_profile) = match explicit_request_cost_bytes {
        Some(explicit) => (explicit, None, None),
        None => match policy {
            LogsFetchPolicy::RequestMinimal => (u64::MAX, None, None),
            // byte-minimal is today's behaviour byte for byte, which includes a
            // configured (non-default) `--logs-request-cost-bytes`: ADR-0904's
            // knob keeps its meaning under this policy rather than being
            // silently replaced by the compiled default. latency-first resolves
            // the byte quantities exactly the same way (issue #1196): the
            // GET-requests-for-wall-clock trade it makes is an operator-set
            // concurrency, never a change to what the fetch layer sees here.
            LogsFetchPolicy::ByteMinimal | LogsFetchPolicy::LatencyFirst => {
                (configured_request_cost_bytes, None, None)
            }
            LogsFetchPolicy::CostBased => {
                let (rate, term, saturated) = resolve_cost_based_rate(profile);
                (rate, Some(term), saturated)
            }
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

    let projection_break_even_bytes = (policy == LogsFetchPolicy::CostBased && !saturates_routing)
        .then(|| {
            block_range_threshold
                .max(request_cost_bytes.saturating_mul(crate::WHOLE_OBJECT_REQUEST_MULTIPLE))
        });

    ResolvedLogsFetch {
        request_cost_bytes,
        rate_term,
        block_range_threshold,
        projection_break_even_bytes,
        overridden_block_range_threshold,
        saturated_profile,
    }
}

/// The cost-based byte rate, the [`RateTerm`] that produced it, and, when it
/// saturated at `u64::MAX`, the profile name to log (ADR-2414 decision A3):
/// the larger of the finite terms, the time term when the price term
/// saturates, and a saturated rate only when neither term is finite.
fn resolve_cost_based_rate(profile: &StoreCostProfile) -> (u64, RateTerm, Option<String>) {
    let (rate, term) = match (
        price_rate(profile),
        profile.request_cost_bytes_from_timings(),
    ) {
        (Some(price), Some(time)) if time > price => (time, RateTerm::Time),
        (Some(price), _) => (price, RateTerm::Price),
        (None, Some(time)) => (time, RateTerm::Time),
        (None, None) => (u64::MAX, RateTerm::Saturated),
    };
    if rate == u64::MAX {
        // Neither term is finite, or the timings' own product saturated:
        // whole-object always. Attribute the profile so the startup override
        // log never lacks a name.
        return (u64::MAX, RateTerm::Saturated, Some(profile.name.clone()));
    }
    // Floor at one byte so a sub-microsecond latency or a sub-nanodollar
    // price can never resolve to a zero rate (which would make every
    // crossover trivially true).
    (rate.max(1), term, None)
}

/// The price term: `get_class_nanodollars * BYTES_PER_GIB / (transfer +
/// retrieval)` in `u128`, or `None` when it saturates (both byte prices zero,
/// or a quotient at or over `u64::MAX`).
fn price_rate(profile: &StoreCostProfile) -> Option<u64> {
    let byte_price = u128::from(profile.transfer_nanodollars_per_gib)
        .saturating_add(u128::from(profile.retrieval_nanodollars_per_gib));
    if byte_price == 0 {
        // Free bytes: priced alone, a saved request is worth an unbounded
        // number of them.
        return None;
    }
    let quotient = u128::from(profile.get_class_nanodollars) * BYTES_PER_GIB / byte_price;
    // A quotient of exactly u64::MAX converts cleanly but would still saturate
    // the routing threshold downstream, so it saturates here too.
    u64::try_from(quotient)
        .ok()
        .filter(|&rate| rate != u64::MAX)
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
    /// [`EngineConfig::fetch_concurrency`] was zero (ADR-1195: "zero is
    /// rejected during configuration resolution").
    #[error("fetch_concurrency must be at least 1")]
    ZeroFetchConcurrency,
    /// A [`GetLimiter`](crate::GetLimiter) was built with, or
    /// [`EngineConfig::store_get_concurrency`] resolved to, zero permits
    /// (ADR-1195's `--store-get-concurrency`): a zero-permit limiter can
    /// never issue a GET.
    #[error("store GET concurrency must be at least 1")]
    ZeroGetLimiterPermits,
    /// [`EngineConfig::sql_partition_count`] resolved to zero (ADR-1195's
    /// `--sql-partition-count`).
    #[error("sql_partition_count must be at least 1")]
    ZeroSqlPartitionCount,
    /// [`EngineConfig::promql_fetch_fanout`] resolved to zero (ADR-1195's
    /// `--promql-fetch-fanout`).
    #[error("promql_fetch_fanout must be at least 1")]
    ZeroPromqlFetchFanout,
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
    /// The seal margin the catalog this engine resolves through folds with
    /// (ADR-1306 decision 3). Not a limit: it is the first of the three terms
    /// of [`Self::fold_lag_threshold`], the tail at which a request-budget
    /// refusal starts naming fold lag (ADR-1306 decision 6). Defaults to
    /// [`SealMargin::REFERENCE`], the catalog's own compiled-in durations;
    /// ravel-server sets it from the `CatalogConfig` of the catalog it resolves
    /// through.
    pub seal_margin: SealMargin,
    /// How long the scheduled fold waits between cycles, the second term of
    /// [`Self::fold_lag_threshold`] (ADR-1306 decision 6, "Amendment
    /// (2026-09-27, #1306)"): a fold that is keeping up still lets the tail
    /// grow by this much before its next cycle shortens it again. Defaults to
    /// [`REFERENCE_FOLD_INTERVAL`], the server's own `DEFAULT_FOLD_INTERVAL`.
    /// ravel-server sets it from its own `FoldTaskConfig`, which is the real
    /// interval only where that process runs the scheduled fold (`all` and
    /// `maintain`); a `query` or `gateway` process keeps the default even when
    /// the `maintain` processes fold on a longer interval.
    pub fold_interval: Duration,
    /// How long a decoded catalog HEAD may be served from the TTL cache, the
    /// third term of [`Self::fold_lag_threshold`] (ADR-1306 decision 6,
    /// "Amendment (2026-09-27, #1306)"): the watermark a resolve resolves
    /// against may be this much older than the fold's real one. Defaults to
    /// [`REFERENCE_HEAD_CACHE_TTL`], the catalog's own
    /// `DEFAULT_HEAD_CACHE_TTL_NS`; ravel-server sets it from the
    /// `CatalogConfig` of the catalog it resolves through.
    pub head_cache_ttl: Duration,
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
    /// The projection break-even [`resolve_logs_fetch`] resolved
    /// ([`ResolvedLogsFetch::projection_break_even_bytes`], ADR-2414 decision
    /// A3), handed to the logs fetcher by
    /// [`crate::LogSegmentFetcher::with_projection_break_even_bytes`]. `None`,
    /// the default, keeps [`Self::logs_block_range_threshold`] as the
    /// break-even.
    pub logs_projection_break_even_bytes: Option<u64>,
    /// The fetch bound (ADR-0996 decision 2): one covering GET's maximum length.
    /// An object at or under it is one covering GET; a larger one is read as
    /// `ceil(size / bound)` sequential covering sub-range GETs, so no single
    /// request moves more than this. Zero is refused at resolution
    /// ([`Self::validate`]): the segmented fallback divides by it. Defaults to
    /// [`DEFAULT_LOG_MAX_FETCH_RUN_BYTES`] (64 MiB).
    pub logs_max_fetch_run_bytes: u64,
    /// Explicit override for concurrent object-store GETs, process-wide
    /// (ADR-1195's `--store-get-concurrency`), fed to the one shared
    /// [`crate::GetLimiter`] every query-side fetcher honours. `None` falls
    /// back to [`Self::fetch_concurrency`] (no default moves): use
    /// [`Self::store_get_concurrency`] to read the resolved value.
    pub store_get_concurrency: Option<usize>,
    /// Explicit override for DataFusion `target_partitions`
    /// (ADR-1195's `--sql-partition-count`); `ravel-sql`'s `session_config`
    /// sets `target_partitions` from the resolved value. `None` falls
    /// back to [`Self::fetch_concurrency`]: use
    /// [`Self::sql_partition_count`] to read the resolved value.
    pub sql_partition_count: Option<usize>,
    /// Explicit override for PromQL `buffer_unordered` fan-out width
    /// (ADR-1195's `--promql-fetch-fanout`). `None` falls back to
    /// [`Self::fetch_concurrency`]: use [`Self::promql_fetch_fanout`] to read
    /// the resolved value.
    pub promql_fetch_fanout: Option<usize>,
}

impl EngineConfig {
    /// The unsealed tail at which a request-budget refusal from this engine
    /// starts naming fold lag: [`fold_lag_tail_threshold`] of this config's
    /// seal margin, fold interval and HEAD cache TTL. 8,730 s at the
    /// defaults (8,400 + 300 + 30).
    pub fn fold_lag_threshold(&self) -> Duration {
        fold_lag_tail_threshold(self.seal_margin, self.fold_interval, self.head_cache_ttl)
    }

    /// The resolved concurrent-GET permit count: the explicit
    /// [`Self::store_get_concurrency`] override if set, else the legacy
    /// [`Self::fetch_concurrency`] (ADR-1195: no default moves).
    pub fn store_get_concurrency(&self) -> usize {
        self.store_get_concurrency.unwrap_or(self.fetch_concurrency)
    }

    /// The resolved DataFusion partition count: the explicit
    /// [`Self::sql_partition_count`] override if set, else the legacy
    /// [`Self::fetch_concurrency`] (ADR-1195: no default moves).
    pub fn sql_partition_count(&self) -> usize {
        self.sql_partition_count.unwrap_or(self.fetch_concurrency)
    }

    /// The resolved PromQL fan-out width: the explicit
    /// [`Self::promql_fetch_fanout`] override if set, else the legacy
    /// [`Self::fetch_concurrency`] (ADR-1195: no default moves).
    pub fn promql_fetch_fanout(&self) -> usize {
        self.promql_fetch_fanout.unwrap_or(self.fetch_concurrency)
    }

    /// Refuse a configuration the fetch layer cannot run on (ADR-0996 decision
    /// 2). Called at startup resolution; a bad value is a typed
    /// [`EngineConfigError`], never a silent clamp.
    pub fn validate(&self) -> Result<(), EngineConfigError> {
        if self.logs_max_fetch_run_bytes == 0 {
            return Err(EngineConfigError::ZeroFetchBound);
        }
        if self.fetch_concurrency == 0 {
            return Err(EngineConfigError::ZeroFetchConcurrency);
        }
        if self.store_get_concurrency() == 0 {
            return Err(EngineConfigError::ZeroGetLimiterPermits);
        }
        if self.sql_partition_count() == 0 {
            return Err(EngineConfigError::ZeroSqlPartitionCount);
        }
        if self.promql_fetch_fanout() == 0 {
            return Err(EngineConfigError::ZeroPromqlFetchFanout);
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
            seal_margin: SealMargin::REFERENCE,
            fold_interval: REFERENCE_FOLD_INTERVAL,
            head_cache_ttl: REFERENCE_HEAD_CACHE_TTL,
            deadline: DEFAULT_DEADLINE,
            fetch_concurrency: DEFAULT_FETCH_CONCURRENCY,
            default_evaluation_interval: DEFAULT_EVALUATION_INTERVAL,
            logs_block_range_threshold: crate::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
            logs_request_cost_bytes: crate::DEFAULT_LOG_REQUEST_COST_BYTES,
            logs_fetch_policy: LogsFetchPolicy::default(),
            logs_projection_break_even_bytes: None,
            logs_max_fetch_run_bytes: DEFAULT_LOG_MAX_FETCH_RUN_BYTES,
            store_get_concurrency: None,
            sql_partition_count: None,
            promql_fetch_fanout: None,
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    /// The reference prices with no timings: the profile shape every
    /// cost-based resolution had before ADR-2414 decision A3, and the one
    /// shape that still saturates.
    fn untimed_reference() -> StoreCostProfile {
        StoreCostProfile {
            name: "untimed".to_string(),
            request_latency_micros: None,
            per_connection_throughput_bytes_per_s: None,
            timings_measured: None,
            ..StoreCostProfile::reference()
        }
    }

    /// The egress prices ADR-0904 worked from, with no timings: a price term
    /// of 4,294 bytes.
    fn egress_untimed() -> StoreCostProfile {
        StoreCostProfile {
            name: "egress-billed".to_string(),
            transfer_nanodollars_per_gib: 90_000_000,
            retrieval_nanodollars_per_gib: 10_000_000,
            ..untimed_reference()
        }
    }

    fn cost_based(
        profile: &StoreCostProfile,
        explicit_threshold: Option<u64>,
    ) -> ResolvedLogsFetch {
        resolve_logs_fetch(
            LogsFetchPolicy::CostBased,
            profile,
            None,
            crate::DEFAULT_LOG_REQUEST_COST_BYTES,
            explicit_threshold.unwrap_or(crate::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD),
            explicit_threshold,
        )
    }

    /// ADR-2414 decision A3: the reference profile's rate is the time term,
    /// the routing threshold keeps its configured value, and the break-even
    /// is the larger of that threshold and five request costs.
    ///
    /// Prove-the-test, each shown failing: a price-only rate reads u64::MAX
    /// at the first assertion; a rate from the latency alone reads 70,000 (or
    /// 70 in milliseconds); a break-even of the threshold alone reads
    /// Some(524,288); the time term taken over a larger price term reads
    /// 6,300,000 where 10,000,004 is expected.
    #[test]
    fn cost_based_rate_on_the_reference_profile_is_the_time_term() {
        let reference = StoreCostProfile::reference();
        let r = cost_based(&reference, None);
        assert_eq!(r.request_cost_bytes, 6_300_000);
        assert_eq!(r.rate_term, Some(RateTerm::Time));
        assert_eq!(r.saturated_profile, None);
        assert_eq!(r.block_range_threshold, 524_288);
        assert_eq!(r.overridden_block_range_threshold, None);
        assert_eq!(r.projection_break_even_bytes, Some(31_500_000));

        // An explicit routing threshold below the break-even is kept, and the
        // break-even is still five request costs.
        let r = cost_based(&reference, Some(2_000_000));
        assert_eq!(r.block_range_threshold, 2_000_000);
        assert_eq!(r.overridden_block_range_threshold, None);
        assert_eq!(r.projection_break_even_bytes, Some(31_500_000));

        // One above it is the break-even itself.
        let r = cost_based(&reference, Some(64_000_000));
        assert_eq!(r.block_range_threshold, 64_000_000);
        assert_eq!(r.projection_break_even_bytes, Some(64_000_000));

        // Both terms finite: the larger wins. Egress prices give 4,294 against
        // the reference timings' 6,300,000.
        let timed_egress = StoreCostProfile {
            request_latency_micros: Some(70_000),
            per_connection_throughput_bytes_per_s: Some(90_000_000),
            ..egress_untimed()
        };
        let r = cost_based(&timed_egress, None);
        assert_eq!(r.request_cost_bytes, 6_300_000);
        assert_eq!(r.rate_term, Some(RateTerm::Time));

        // ... and when the price term is the larger it wins. A GET price of
        // 931,323 nanodollars against a 100,000,000 nanodollar-per-GiB byte
        // price is 931,323 * 2^30 / 10^8 = 10,000,004 bytes, above 6,300,000.
        let priced_high = StoreCostProfile {
            get_class_nanodollars: 931_323,
            ..timed_egress.clone()
        };
        let r = cost_based(&priced_high, None);
        assert_eq!(r.request_cost_bytes, 10_000_004);
        assert_eq!(r.rate_term, Some(RateTerm::Price));
        assert_eq!(r.projection_break_even_bytes, Some(50_000_020));

        // Egress prices and no timings: the price term alone, as before A3.
        let r = cost_based(&egress_untimed(), None);
        assert_eq!(r.request_cost_bytes, 4_294);
        assert_eq!(r.rate_term, Some(RateTerm::Price));
        assert_eq!(
            r.projection_break_even_bytes,
            Some(crate::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD),
            "five request costs under the threshold leave the threshold as the break-even"
        );

        // Zero prices and no timings: saturated exactly as before A3.
        let r = cost_based(&untimed_reference(), None);
        assert_eq!(r.request_cost_bytes, u64::MAX);
        assert_eq!(r.rate_term, Some(RateTerm::Saturated));
        assert_eq!(r.saturated_profile.as_deref(), Some("untimed"));
        assert_eq!(r.block_range_threshold, u64::MAX);
        assert_eq!(r.projection_break_even_bytes, None);

        // byte-minimal and latency-first keep the compiled-in rate and no
        // break-even, on the same reference profile.
        for policy in [LogsFetchPolicy::ByteMinimal, LogsFetchPolicy::LatencyFirst] {
            let r = resolve_logs_fetch(
                policy,
                &reference,
                None,
                crate::DEFAULT_LOG_REQUEST_COST_BYTES,
                crate::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
                None,
            );
            assert_eq!(r.request_cost_bytes, 1_887_437, "{policy:?}");
            assert_eq!(r.rate_term, None, "{policy:?}");
            assert_eq!(r.projection_break_even_bytes, None, "{policy:?}");
        }
    }

    #[test]
    fn rate_term_label_names_the_source_of_the_rate() {
        let reference = StoreCostProfile::reference();
        assert_eq!(cost_based(&reference, None).rate_term_label(false), "time");
        assert_eq!(
            cost_based(&egress_untimed(), None).rate_term_label(false),
            "price"
        );
        assert_eq!(
            cost_based(&untimed_reference(), None).rate_term_label(false),
            "saturated"
        );
        let explicit = resolve_logs_fetch(
            LogsFetchPolicy::CostBased,
            &reference,
            Some(123_456),
            123_456,
            crate::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
            None,
        );
        assert_eq!(explicit.rate_term_label(true), "flag");
        let byte_minimal = resolve_logs_fetch(
            LogsFetchPolicy::ByteMinimal,
            &reference,
            None,
            crate::DEFAULT_LOG_REQUEST_COST_BYTES,
            crate::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
            None,
        );
        assert_eq!(byte_minimal.rate_term_label(false), "none");
    }

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

        // cost-based at a free-byte profile with no timings resolves to
        // request-minimal behaviour: both byte prices zero saturate the rate,
        // naming the profile.
        let untimed = untimed_reference();
        let cb_untimed = resolve_logs_fetch(
            LogsFetchPolicy::CostBased,
            &untimed,
            None,
            crate::DEFAULT_LOG_REQUEST_COST_BYTES,
            crate::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
            None,
        );
        assert_eq!(cb_untimed.request_cost_bytes, u64::MAX);
        assert_eq!(cb_untimed.rate_term, Some(RateTerm::Saturated));
        assert_eq!(cb_untimed.saturated_profile.as_deref(), Some("untimed"));
        // ... and the routing threshold saturates WITH the rate. The fetch layer
        // pins its inner crossover to whatever threshold it is handed
        // (`with_block_range_threshold` sets `whole_object_threshold`, which
        // `effective_whole_object_threshold` then returns verbatim, bypassing the
        // `5 x request_cost` derivation and its floors), so leaving this at
        // 512 KiB would route a narrow projection of any larger object ranged.
        assert_eq!(cb_untimed.block_range_threshold, u64::MAX);
        assert_eq!(cb_untimed.projection_break_even_bytes, None);

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
            request_latency_micros: None,
            per_connection_throughput_bytes_per_s: None,
            timings_measured: None,
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
            request_latency_micros: None,
            per_connection_throughput_bytes_per_s: None,
            timings_measured: None,
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
            request_latency_micros: None,
            per_connection_throughput_bytes_per_s: None,
            timings_measured: None,
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
    /// request-minimal arm already does. Since ADR-2414 decision A3 a
    /// cost-based rate saturates only on a profile with neither byte prices
    /// nor timings; the reference profile's finite time term keeps the flag.
    ///
    /// Prove-the-test: key the override on `matches!(policy,
    /// LogsFetchPolicy::RequestMinimal)` alone (the pre-fix condition) and the
    /// untimed assertions fail: the threshold reads 4096 and the overridden
    /// flag reads `None`. Key it on the policy being cost-based instead and
    /// the reference-profile assertions fail the other way.
    #[test]
    fn a_saturated_cost_based_rate_overrides_an_explicit_routing_threshold() {
        let r = cost_based(&untimed_reference(), Some(4096));
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

        let r = cost_based(&StoreCostProfile::reference(), Some(4096));
        assert_eq!(r.request_cost_bytes, 6_300_000);
        assert_eq!(
            r.block_range_threshold, 4096,
            "a finite time term leaves the explicit routing threshold in force"
        );
        assert_eq!(r.overridden_block_range_threshold, None);
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
            request_latency_micros: None,
            per_connection_throughput_bytes_per_s: None,
            timings_measured: None,
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
        // replaces the profile's derived one, the configured routing threshold
        // stays in force, and the break-even is derived from the explicit
        // rate: max(524,288, 5 * 123,456 = 617,280).
        let cb = resolve_logs_fetch(
            LogsFetchPolicy::CostBased,
            &profile,
            Some(123_456),
            crate::DEFAULT_LOG_REQUEST_COST_BYTES,
            crate::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
            None,
        );
        assert_eq!(cb.request_cost_bytes, 123_456);
        assert_eq!(cb.rate_term, None, "no derivation ran");
        assert_eq!(
            cb.block_range_threshold,
            crate::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD
        );
        assert_eq!(cb.projection_break_even_bytes, Some(617_280));
        assert_eq!(cb.saturated_profile, None);
    }

    /// Issue #1196: `latency-first` must resolve the byte quantities exactly as
    /// `byte-minimal` does. It carries no concurrency preference of its own
    /// (ADR-1196): the trade it makes is an operator-set concurrency, not a
    /// value this resolution derives.
    ///
    /// Prove-the-test: route `LogsFetchPolicy::LatencyFirst` through the
    /// `CostBased` arm instead of `ByteMinimal`'s and execution stops at the
    /// first assertion, which prints `left: 6300000, right: 700000`: the
    /// reference profile's time term (ADR-2414 decision A3) where the
    /// configured request cost was expected.
    #[test]
    fn latency_first_resolves_like_byte_minimal() {
        let reference = StoreCostProfile::reference();
        let configured = 700_000u64;

        let bm = resolve_logs_fetch(
            LogsFetchPolicy::ByteMinimal,
            &reference,
            None,
            configured,
            crate::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
            None,
        );
        let lf = resolve_logs_fetch(
            LogsFetchPolicy::LatencyFirst,
            &reference,
            None,
            configured,
            crate::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
            None,
        );
        assert_eq!(lf.request_cost_bytes, bm.request_cost_bytes);
        assert_eq!(lf.request_cost_bytes, configured);
        assert_eq!(lf.block_range_threshold, bm.block_range_threshold);
        assert_eq!(
            lf.block_range_threshold,
            crate::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD
        );
        assert_eq!(lf.overridden_block_range_threshold, None);
        assert_eq!(lf.saturated_profile, None);
    }

    #[test]
    fn zero_fetch_bound_is_refused_with_a_typed_error() {
        let mut cfg = EngineConfig::default();
        assert_eq!(cfg.validate(), Ok(()));
        cfg.logs_max_fetch_run_bytes = 0;
        assert_eq!(cfg.validate(), Err(EngineConfigError::ZeroFetchBound));
    }

    #[test]
    fn zero_fetch_concurrency_is_refused_with_a_typed_error() {
        let cfg = EngineConfig {
            fetch_concurrency: 0,
            ..EngineConfig::default()
        };
        assert_eq!(cfg.validate(), Err(EngineConfigError::ZeroFetchConcurrency));
    }

    /// ADR-1195: each of the three split knobs is validated on its RESOLVED
    /// value, so a zero override is rejected even though `fetch_concurrency`
    /// itself stays nonzero -- an operator turning one knob to 0 must not
    /// silently fall back to the legacy value.
    #[test]
    fn zero_store_get_concurrency_override_is_refused_with_a_typed_error() {
        let cfg = EngineConfig {
            store_get_concurrency: Some(0),
            ..EngineConfig::default()
        };
        assert_eq!(
            cfg.validate(),
            Err(EngineConfigError::ZeroGetLimiterPermits)
        );
    }

    #[test]
    fn zero_sql_partition_count_override_is_refused_with_a_typed_error() {
        let cfg = EngineConfig {
            sql_partition_count: Some(0),
            ..EngineConfig::default()
        };
        assert_eq!(
            cfg.validate(),
            Err(EngineConfigError::ZeroSqlPartitionCount)
        );
    }

    #[test]
    fn zero_promql_fetch_fanout_override_is_refused_with_a_typed_error() {
        let cfg = EngineConfig {
            promql_fetch_fanout: Some(0),
            ..EngineConfig::default()
        };
        assert_eq!(
            cfg.validate(),
            Err(EngineConfigError::ZeroPromqlFetchFanout)
        );
    }

    /// ADR-1195: no default moves. With all three knobs unset, every
    /// accessor must return the legacy `fetch_concurrency` value, not some
    /// new independent default.
    #[test]
    fn unset_knobs_all_resolve_to_fetch_concurrency() {
        let cfg = EngineConfig {
            fetch_concurrency: 8,
            ..EngineConfig::default()
        };
        assert_eq!(cfg.validate(), Ok(()));
        assert_eq!(cfg.store_get_concurrency(), 8);
        assert_eq!(cfg.sql_partition_count(), 8);
        assert_eq!(cfg.promql_fetch_fanout(), 8);
    }

    /// Setting one knob overrides only that knob's accessor; the other two
    /// still fall back to `fetch_concurrency` (ADR-1195: splitting the knob
    /// changes which lever an operator turns, not what an untouched knob
    /// resolves to).
    #[test]
    fn one_explicit_knob_overrides_only_its_own_accessor() {
        let cfg = EngineConfig {
            fetch_concurrency: 8,
            promql_fetch_fanout: Some(3),
            ..EngineConfig::default()
        };
        assert_eq!(cfg.validate(), Ok(()));
        assert_eq!(cfg.promql_fetch_fanout(), 3);
        assert_eq!(cfg.store_get_concurrency(), 8);
        assert_eq!(cfg.sql_partition_count(), 8);
    }
}
