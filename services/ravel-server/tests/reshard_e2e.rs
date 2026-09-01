//! Online-resharding acceptance test (ADR-0052).
//!
//! This proves the shipped generation-versioned `shard_count` mechanism end to
//! end: the write-side router live switch, the read-side per-hour scan rule,
//! and the reshard append operation together return complete query results
//! across a reshard boundary, for both an increase and a decrease.
//!
//! # Why this drives an injected clock instead of `ravel_server::start` + HTTP
//!
//! A reshard's activation is denominated in ingest-hour buckets, and ADR-0052
//! section 3 requires an activation lead `L >= ceil(C) + 1` hours (`C` is the
//! router's provisioning-record refresh interval, `DEFAULT_REFRESH_INTERVAL_NS`
//! = 60s, so `L >= 2h`). A running `ravel_server::start` server routes and folds
//! on `SystemClock` (hardcoded in `start`), and its `/api/v1/query` handler
//! stamps `now_ns` from `SystemTime`; neither the clock nor the 60s router cache
//! is injectable through the server's public surface. Crossing a live activation
//! through the real HTTP server would therefore require the test to wait real
//! hours. That is a property of the shipped design (the fail-closed staleness
//! bound is load-bearing, ADR-0052 "Consequences"), not a gap in it.
//!
//! So this test drives the **real production ingest and read stack** with an
//! injected [`Clock`], the same discipline the repository already uses for every
//! hour-boundary test (CLAUDE.md "Time is injected"):
//!
//! - writes go through a real [`IngestRouter`] (the exact type the server
//!   embeds, built the way `ravel_server::start` builds it) over a real
//!   `MemoryStore`, so the router live switch, the fail-closed staleness re-read,
//!   real RSEG flushes, and real commit records under real shard-indexed keys are
//!   all exercised;
//! - the reshard is the real CAS-append the operator command
//!   `ravel-cli provision reshard` wraps ([`append_generation`] +
//!   [`write_reshard_audit`], see `services/ravel-cli/src/provision.rs`);
//! - queries run through the real [`Catalog`] built exactly as
//!   `ravel_server::query::build_catalog` builds it (with provisioning
//!   enforcement on) and the real [`QueryEngine`], with an injected `now_ns` that
//!   matches the injected ingest time, asserting on actual returned sample
//!   values.
//!
//! The only thing bypassed is the HTTP transport and `start`'s task supervision,
//! which carry no resharding-correctness logic and are already covered by
//! `provisioning_e2e.rs` / `tenancy_e2e.rs`. Every correctness-bearing line
//! (routing, scan fan-out, snapshot-head validation, commit-token resolution,
//! CAS append) is the production code path.
//!
//! The `FaultStore`-injected CAS-race property (one winner, loser re-reads) is
//! already covered at the unit level in
//! `crates/ravel-catalog/src/provisioning.rs`
//! (`append_generation_cas_conflict...`); it is not duplicated here.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use ravel_catalog::{
    AbsentPolicy, Catalog, DEFAULT_SCAN_SLACK_HOURS, ReshardOutcome, ShardGeneration,
    append_generation, validate_or_adopt,
};
use ravel_ingest::{
    Clock, DEFAULT_REFRESH_INTERVAL_NS, IngestConfig, IngestRouter, WriteError, WriteMode,
};
use ravel_maintain::write_reshard_audit;
use ravel_object_store::fault::{FaultPlan, Op, Rule, ScriptedFault};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, fault::FaultStore};
use ravel_otlp::NormalizedPoint;
use ravel_promql::Value;
use ravel_query::{EngineConfig, QueryEngine};
use ravel_server::query::build_catalog;
use ravel_types::{
    CommitToken, Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantHash,
    TenantId, shard_for,
};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const NS_PER_MIN: i64 = 60_000_000_000;
const TENANT: &str = "acme";
const SIGNAL: Signal = Signal::Metrics;
const ACK: Duration = Duration::from_secs(10);
/// Base ingest hour every test anchors to, far from 0 so the `now - lag` and
/// `now - fold_margin` windows never underflow into negative hours.
const BASE_HOUR: u32 = 500_000;

/// The reshard lead floor `ceil(C) + 1` the CLI enforces (ADR-0052 section 3,
/// `services/ravel-cli/src/provision.rs` `MIN_LEAD_HOURS`). Restated here so
/// [`reshard`] mirrors the operator command exactly.
const MIN_LEAD_HOURS: u32 = 2;

// --------------------------------------------------------------------------
// Injected clock: the sole time source for routing-generation selection and
// the ingest-hour bucket a flush pins. Uses the default (real-timer) `sleep`;
// with `target_bytes: 1` every write flushes by size on receipt, so the actor's
// age-based flush tick never gates a test.
// --------------------------------------------------------------------------

struct ManualClock(AtomicI64);

impl ManualClock {
    fn new(now_ns: i64) -> Arc<Self> {
        Arc::new(ManualClock(AtomicI64::new(now_ns)))
    }
    fn set(&self, now_ns: i64) {
        self.0.store(now_ns, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now_ns(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

// --------------------------------------------------------------------------
// Small builders.
// --------------------------------------------------------------------------

fn tenant() -> TenantId {
    TenantId::new(TENANT)
}

fn tenant_hash() -> TenantHash {
    tenant().hash()
}

fn labels(metric: &str) -> LabelSet {
    LabelSet::new(vec![Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: metric.to_string(),
    }])
    .expect("valid labels")
}

fn point(metric: &str, ts_ns: i64, value: f64) -> NormalizedPoint {
    let labels = labels(metric);
    let series_id = SeriesId::compute(&tenant(), metric, &labels).expect("series id");
    NormalizedPoint {
        series_id,
        labels: Arc::new(labels),
        sample: Sample { ts_ns, value },
        is_monotonic_sum: false,
    }
}

/// The shard a `metric`'s single-label series hashes to under `count`
/// (`shard_for`, the frozen write-side routing contract).
fn metric_shard(metric: &str, count: u32) -> u32 {
    let ls = labels(metric);
    let id = SeriesId::compute(&tenant(), metric, &ls).expect("series id");
    shard_for(&id, count)
}

/// A metric name (`<prefix>_<i>`) whose series lands in a shard satisfying
/// `want` under `count`. Used to place data provably in the old-only or
/// new-only shard range of a reshard.
fn metric_in_shard(prefix: &str, count: u32, want: impl Fn(u32) -> bool) -> String {
    for i in 0..1_000_000u32 {
        let name = format!("{prefix}_{i}");
        if want(metric_shard(&name, count)) {
            return name;
        }
    }
    panic!("no {prefix} series lands in the requested shard range under count {count}");
}

fn build_router(
    store: Arc<dyn ObjectStoreBackend>,
    default_count: u32,
    clock: Arc<ManualClock>,
) -> IngestRouter {
    IngestRouter::new(
        IngestConfig {
            shard_count: default_count,
            target_bytes: 1, // flush every write immediately, like the router unit tests
            ..IngestConfig::default()
        },
        store,
        SIGNAL,
        clock,
    )
}

/// Create the generation-0 provisioning record the way the server's ingest pin
/// does (`ensure_provisioning_record` -> `validate_or_adopt` with
/// `CreateFromConfig`). A reshard has nothing to append to without it.
async fn seed_gen0_record(store: &dyn ObjectStoreBackend, count: u32) {
    validate_or_adopt(
        store,
        &tenant_hash(),
        SIGNAL,
        count,
        BASE_HOUR as i64 * NS_PER_HOUR,
        AbsentPolicy::CreateFromConfig,
    )
    .await
    .expect("seed generation-0 provisioning record");
}

/// The operator reshard, exactly as `ravel-cli provision reshard` performs it
/// (`services/ravel-cli/src/provision.rs`): refuse a lead below the floor,
/// compute `activation_hour = now_hour + lead`, CAS-append the generation via
/// [`append_generation`], and write the control-plane audit record. `now_ns` is
/// injected so the append's future-activation check is evaluated against a
/// chosen clock, the same way the CLI evaluates it against `SystemTime`.
async fn reshard(
    store: &dyn ObjectStoreBackend,
    new_count: u32,
    lead_hours: u32,
    now_ns: i64,
) -> ReshardOutcome {
    assert!(
        lead_hours >= MIN_LEAD_HOURS,
        "the CLI refuses a lead below {MIN_LEAD_HOURS}"
    );
    let now_hour = u32::try_from(now_ns.div_euclid(NS_PER_HOUR).max(0)).expect("hour fits u32");
    let activation_hour = now_hour + lead_hours;
    let outcome = append_generation(
        store,
        &tenant_hash(),
        SIGNAL,
        new_count,
        activation_hour,
        now_ns,
    )
    .await
    .expect("reshard append succeeds");
    let from_count = outcome
        .generations
        .iter()
        .rev()
        .nth(1)
        .map(|g| g.shard_count)
        .unwrap_or(new_count);
    write_reshard_audit(
        store,
        &tenant_hash(),
        Uuid::new_v4(),
        now_ns,
        SIGNAL.key_prefix(),
        from_count,
        new_count,
        outcome.generation,
        activation_hour,
    )
    .await
    .expect("reshard audit write succeeds");
    outcome
}

// --------------------------------------------------------------------------
// Query helpers.
// --------------------------------------------------------------------------

/// A real `Catalog` (provisioning enforcement on) + `QueryEngine` over `store`,
/// built exactly as `ravel_server::query::build_catalog` builds the catalog.
fn engine(store: Arc<dyn ObjectStoreBackend>, catalog_count: u32) -> (Arc<Catalog>, QueryEngine) {
    let catalog = build_catalog(
        store.clone(),
        catalog_count,
        false,
        ravel_catalog::DEFAULT_BYTE_CACHE_MAX_BYTES,
        None,
    )
    .expect("catalog builds");
    let engine = QueryEngine::new(catalog.clone(), store, EngineConfig::default());
    (catalog, engine)
}

async fn instant(
    engine: &QueryEngine,
    query: &str,
    t_ms: i64,
    min_tokens: &[CommitToken],
    now_ns: i64,
) -> Value {
    let (value, _coverage) = engine
        .instant(tenant_hash(), query, t_ms, min_tokens, now_ns, ACK)
        .await
        .expect("instant query succeeds");
    value
}

/// A range query over `[start_ms, end_ms]` at `step_ms`. Used for the
/// cross-hour acceptance queries, whose two samples sit hours apart and so
/// cannot both fall inside a single instant query's lookback window: only a
/// range query evaluates every hour the reader's per-hour scan fan-out
/// differentiates across the generation boundary.
async fn range(
    engine: &QueryEngine,
    query: &str,
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
    now_ns: i64,
) -> Value {
    let (value, _coverage) = engine
        .range(
            tenant_hash(),
            query,
            start_ms,
            end_ms,
            step_ms,
            &[],
            now_ns,
            ACK,
        )
        .await
        .expect("range query succeeds");
    value
}

/// `(metric_name, value)` pairs of an instant vector; exact `f64` values.
fn samples(value: &Value) -> Vec<(String, f64)> {
    match value {
        Value::Vector(v) => {
            let mut out: Vec<(String, f64)> = v
                .iter()
                .map(|s| {
                    (
                        s.labels.get(METRIC_NAME_LABEL).unwrap_or("").to_string(),
                        s.value,
                    )
                })
                .collect();
            out.sort_by(|a, b| a.0.cmp(&b.0));
            out
        }
        other => panic!("expected an instant vector, got {other:?}"),
    }
}

fn value_of(value: &Value, metric: &str) -> Option<f64> {
    samples(value)
        .into_iter()
        .find(|(name, _)| name == metric)
        .map(|(_, v)| v)
}

/// `(metric_name, value)` pairs of a range matrix: one entry per matched
/// series, carrying that series' last in-window sample value; exact `f64`.
/// A series that had a value at any evaluated step appears exactly once.
fn matrix_samples(value: &Value) -> Vec<(String, f64)> {
    match value {
        Value::Matrix(m) => {
            let mut out: Vec<(String, f64)> = m
                .iter()
                .filter_map(|(labels, points)| {
                    points.last().map(|s| {
                        (
                            labels.get(METRIC_NAME_LABEL).unwrap_or("").to_string(),
                            s.value,
                        )
                    })
                })
                .collect();
            out.sort_by(|a, b| a.0.cmp(&b.0));
            out
        }
        other => panic!("expected a range matrix, got {other:?}"),
    }
}

fn matrix_value_of(value: &Value, metric: &str) -> Option<f64> {
    matrix_samples(value)
        .into_iter()
        .find(|(name, _)| name == metric)
        .map(|(_, v)| v)
}

// --------------------------------------------------------------------------
// Acceptance property 1a: increase, concurrent ingest, complete results.
// --------------------------------------------------------------------------

/// The epic's own acceptance test, increase direction, and the ADR's literal
/// headline sentence: "a query spanning hours on both sides resolves per-hour
/// scan sets from the generation history and returns complete results from both
/// layouts" (ADR-0052 "Consequences"). Ingest runs concurrently with the reshard
/// append; the reshard is placed a genuine `L = 2` hours in the future
/// (`activation_hour = BASE + 2`, computed against the router's own clock), so
/// three distinct instants are exercised:
///
/// 1. a write under generation 0 in hour `BASE` (old range 0..4);
/// 2. a probe write after the router's view has aged past `C` and re-read the
///    record -- the generation is now *known* but not yet *active* (hour still
///    `BASE < BASE + 2`), so it must STILL route at the old count: the
///    "no early switch" property, pinned end to end rather than only at the unit
///    level;
/// 3. a write in hour `BASE + 2` (activation), which routes on the count-8 set
///    into the new-only range 4..8.
///
/// A single range query then spans hours `BASE` and `BASE + 2`. The reader's
/// catalog is configured with the pre-reshard count 4, so finding the new-hour
/// sample (shard >= 4) is only possible if the per-hour scan fan-out is driven
/// by the generation history -- `scan_count(BASE) = 4` but
/// `scan_count(BASE + 2) = 8`. The physical-shard-placement assertions make a
/// silently-no-op reshard fail: without a real live switch the activation write
/// would land in 0..4 and the query could not distinguish the two layouts.
#[tokio::test]
async fn increase_with_concurrent_ingest_returns_complete_results() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    seed_gen0_record(store.as_ref(), 4).await;

    let clock = ManualClock::new(BASE_HOUR as i64 * NS_PER_HOUR);
    let router = build_router(store.clone(), 4, clock.clone());

    // Two samples, two ingest hours: the old one under generation 0 in hour
    // BASE, the new one under generation 1 in hour BASE + 2 (the activation
    // hour), exactly 2 hours apart so one range-query step lands on each.
    let old_ts = BASE_HOUR as i64 * NS_PER_HOUR + 5 * NS_PER_MIN;
    let new_ts = (BASE_HOUR as i64 + 2) * NS_PER_HOUR + 5 * NS_PER_MIN;
    // Old-range series: under count 4 it lands in 0..4 (always).
    let old_metric = metric_in_shard("oldrange", 4, |_| true);
    // New-only series: under count 8 it lands in 4..8, a shard the pre-reshard
    // fan-out never scanned. Under count 4 it would land in 0..4.
    let new_metric = metric_in_shard("newrange", 8, |s| s >= 4);
    // The probe shares the new series' shape (>= 4 under 8, < 4 under 4) so its
    // routed shard reveals which count the router used.
    let probe_metric = metric_in_shard("probe", 8, |s| s >= 4);

    // Ingest keeps running (the old-generation series plus background filler)
    // while the reshard is appended: concurrent, not a stop-reshard-resume
    // sequence.
    let ingest = async {
        router
            .write(
                tenant(),
                vec![point(&old_metric, old_ts, 11.0)],
                WriteMode::Strict,
                ACK,
            )
            .await
            .expect("old-generation write");
        for i in 0..5 {
            router
                .write(
                    tenant(),
                    vec![point(&format!("filler_{i}"), old_ts, i as f64)],
                    WriteMode::Strict,
                    ACK,
                )
                .await
                .expect("filler write");
        }
    };
    let do_reshard = async {
        // now_hour = BASE, lead 2 -> activation = BASE + 2, a genuine future
        // hour: the record is known before it activates.
        reshard(
            store.as_ref(),
            8,
            MIN_LEAD_HOURS,
            BASE_HOUR as i64 * NS_PER_HOUR,
        )
        .await
    };
    let (_, outcome) = tokio::join!(ingest, do_reshard);
    assert_eq!(outcome.generation, 1, "the reshard appended generation 1");
    assert_eq!(outcome.generations[1].shard_count, 8);
    assert_eq!(outcome.generations[1].activation_hour, BASE_HOUR + 2);

    // No early switch: age the view past C (still ingest hour BASE) so the next
    // write re-reads the record and now KNOWS generation 1 -- but it has not
    // activated yet (hour BASE < BASE + 2), so the write must still route at the
    // old count 4.
    clock.set(BASE_HOUR as i64 * NS_PER_HOUR + DEFAULT_REFRESH_INTERVAL_NS + 1);
    let probe = router
        .write(
            tenant(),
            vec![point(&probe_metric, old_ts, 99.0)],
            WriteMode::Strict,
            ACK,
        )
        .await
        .expect("pre-activation probe write");
    assert!(
        probe.tokens.iter().all(|t| t.shard < 4),
        "a known-but-not-yet-active generation must not switch early: the probe \
         routed on the old count 4 (shards 0..4)"
    );

    // Activation: advance to hour BASE + 2. The next write re-reads and
    // live-switches to the count-8 set.
    clock.set(new_ts);
    let receipt = router
        .write(
            tenant(),
            vec![point(&new_metric, new_ts, 22.0)],
            WriteMode::Strict,
            ACK,
        )
        .await
        .expect("new-generation write");
    assert!(
        receipt.tokens.iter().any(|t| t.shard >= 4),
        "the post-activation write physically landed in the new-only shard range"
    );
    router.flush_all().await;

    // One range query spanning both hours returns both generations' data. The
    // reader is configured with the pre-reshard count 4, so the new-hour sample
    // (shard >= 4) is reachable only via the generation-history per-hour fan-out.
    let (_catalog, engine) = engine(store.clone(), 4);
    let start_ms = old_ts / 1_000_000;
    let end_ms = new_ts / 1_000_000;
    let step_ms = end_ms - start_ms; // one step on each sample
    let now_ns = (BASE_HOUR as i64 + 2) * NS_PER_HOUR + 10 * NS_PER_MIN;
    let value = range(
        &engine,
        "{__name__=~\"oldrange.*|newrange.*\"}",
        start_ms,
        end_ms,
        step_ms,
        now_ns,
    )
    .await;

    assert_eq!(
        matrix_value_of(&value, &old_metric),
        Some(11.0),
        "old-hour series (shards 0..4, scan_count(BASE) = 4) is returned across the increase"
    );
    assert_eq!(
        matrix_value_of(&value, &new_metric),
        Some(22.0),
        "new-hour series (shards 4..8, scan_count(BASE+2) = 8) is returned across the increase"
    );
}

// --------------------------------------------------------------------------
// Acceptance property 1b: decrease, concurrent ingest, complete results.
// --------------------------------------------------------------------------

/// The acceptance test, decrease direction (8 -> 4), spanning hours on both
/// sides of a genuine future activation (`activation_hour = BASE + 2`). Data
/// routed under the retiring count-8 generation lives in shards 4..8; data
/// routed under the new count-4 generation lives in 0..4. As in the increase
/// case three instants are exercised -- an old-hour write, a no-early-switch
/// probe after the view aged past `C` (known but not yet active, so still the
/// wide count 8), and a post-activation write on the narrow count 4 -- and one
/// range query spanning hours `BASE` and `BASE + 2` returns both, because
/// `scan_count` keeps the retiring, wider count in the scan set through the
/// activation hour's slack window (ADR-0052 section 4). Decreases are not gated
/// behind a follow-up: the write- and read-side rules cover them, so the acceptance test covers one.
#[tokio::test]
async fn decrease_with_concurrent_ingest_returns_complete_results() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    seed_gen0_record(store.as_ref(), 8).await;

    let clock = ManualClock::new(BASE_HOUR as i64 * NS_PER_HOUR);
    let router = build_router(store.clone(), 8, clock.clone());

    // Old sample under the retiring count 8 in hour BASE; new sample under the
    // successor count 4 in hour BASE + 2, two hours apart.
    let old_ts = BASE_HOUR as i64 * NS_PER_HOUR + 5 * NS_PER_MIN;
    let new_ts = (BASE_HOUR as i64 + 2) * NS_PER_HOUR + 5 * NS_PER_MIN;
    // Wide-range series: under count 8 it lands in 4..8 (only the retiring
    // generation ever writes there); under count 4 it lands in 0..4.
    let wide_metric = metric_in_shard("wide", 8, |s| s >= 4);
    // Narrow series: under count 4 it lands in 0..4.
    let narrow_metric = metric_in_shard("narrow", 4, |_| true);
    // Probe shares the wide shape so its routed shard reveals the count used.
    let probe_metric = metric_in_shard("probe", 8, |s| s >= 4);

    let ingest = async {
        let receipt = router
            .write(
                tenant(),
                vec![point(&wide_metric, old_ts, 33.0)],
                WriteMode::Strict,
                ACK,
            )
            .await
            .expect("retiring-generation write");
        assert!(
            receipt.tokens.iter().any(|t| t.shard >= 4),
            "the retiring-generation write landed in the wide shard range 4..8"
        );
        for i in 0..5 {
            router
                .write(
                    tenant(),
                    vec![point(&format!("filler_{i}"), old_ts, i as f64)],
                    WriteMode::Strict,
                    ACK,
                )
                .await
                .expect("filler write");
        }
    };
    let do_reshard = async {
        // now_hour = BASE, lead 2 -> activation = BASE + 2, a genuine future hour.
        reshard(
            store.as_ref(),
            4,
            MIN_LEAD_HOURS,
            BASE_HOUR as i64 * NS_PER_HOUR,
        )
        .await
    };
    let (_, outcome) = tokio::join!(ingest, do_reshard);
    assert_eq!(outcome.generations[1].shard_count, 4);
    assert_eq!(outcome.generations[1].activation_hour, BASE_HOUR + 2);

    // No early switch: age the view past C (still hour BASE) so the next write
    // re-reads and knows generation 1, but it has not activated yet, so the
    // write must still route on the wide count 8.
    clock.set(BASE_HOUR as i64 * NS_PER_HOUR + DEFAULT_REFRESH_INTERVAL_NS + 1);
    let probe = router
        .write(
            tenant(),
            vec![point(&probe_metric, old_ts, 99.0)],
            WriteMode::Strict,
            ACK,
        )
        .await
        .expect("pre-activation probe write");
    assert!(
        probe.tokens.iter().any(|t| t.shard >= 4),
        "a known-but-not-yet-active decrease must not switch early: the probe \
         routed on the retiring count 8 (a shard >= 4)"
    );

    // Activation: advance to hour BASE + 2; the next write routes on the count-4 set.
    clock.set(new_ts);
    let receipt = router
        .write(
            tenant(),
            vec![point(&narrow_metric, new_ts, 44.0)],
            WriteMode::Strict,
            ACK,
        )
        .await
        .expect("successor-generation write");
    assert!(
        receipt.tokens.iter().all(|t| t.shard < 4),
        "the post-activation write routed on the narrow count-4 range"
    );
    router.flush_all().await;

    let (_catalog, engine) = engine(store.clone(), 8);
    let start_ms = old_ts / 1_000_000;
    let end_ms = new_ts / 1_000_000;
    let step_ms = end_ms - start_ms;
    let now_ns = (BASE_HOUR as i64 + 2) * NS_PER_HOUR + 10 * NS_PER_MIN;
    let value = range(
        &engine,
        "{__name__=~\"wide.*|narrow.*\"}",
        start_ms,
        end_ms,
        step_ms,
        now_ns,
    )
    .await;

    assert_eq!(
        matrix_value_of(&value, &wide_metric),
        Some(33.0),
        "retiring-generation data (shards 4..8, hour BASE) is still returned across the decrease"
    );
    assert_eq!(
        matrix_value_of(&value, &narrow_metric),
        Some(44.0),
        "successor-generation data (shards 0..4, hour BASE+2) is returned across the decrease"
    );
}

// --------------------------------------------------------------------------
// Acceptance property 2: decrease straggler inside the slack window.
// --------------------------------------------------------------------------

/// A write routed under the retiring (larger) generation that lands in an hour
/// just past the successor's activation but still within `DEFAULT_SCAN_SLACK_HOURS`
/// must still be found (ADR-0052 sections 3-4). The straggler is produced by the
/// real router: `refresh_generations` primes the pre-reshard view (generation 0
/// only, count 8) fresh as of the write, exactly what a lagging writer whose
/// record view predates the reshard holds, so the router routes under the old
/// count into the later hour with no re-read -- the same entry point the
/// stale-writer test uses. A companion write one hour beyond the window
/// (`activation + S`) proves the window is finite: `scan_count` drops the
/// retiring count there, so the shard is no longer scanned (a real deployment
/// sizes `S` so no straggler can land that far).
#[tokio::test]
async fn decrease_straggler_write_inside_slack_window_is_found() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    seed_gen0_record(store.as_ref(), 8).await;
    // Reshard 8 -> 4 activating at BASE (now_hour BASE-2, lead 2).
    let outcome = reshard(
        store.as_ref(),
        4,
        MIN_LEAD_HOURS,
        (BASE_HOUR as i64 - 2) * NS_PER_HOUR,
    )
    .await;
    assert_eq!(outcome.generations[1].activation_hour, BASE_HOUR);

    let slack = DEFAULT_SCAN_SLACK_HOURS; // S = 3

    // The pre-reshard generation view a lagging writer still holds: generation 0
    // only, count 8. Priming this fresh as of each write makes the real router
    // route under the retiring count into a later hour, no re-read to the
    // post-reshard record.
    let gen0_only = vec![ShardGeneration {
        generation: 0,
        shard_count: 8,
        activation_hour: 0,
        appended_unix_ns: 0,
    }];
    let clock = ManualClock::new(BASE_HOUR as i64 * NS_PER_HOUR);
    let router = build_router(store.clone(), 8, clock.clone());

    // A straggler routed under count 8 into activation+1, within the slack
    // window: it sits at a shard (>= 4) only the retiring generation writes.
    let in_slack_hour = BASE_HOUR + 1;
    let straggler_metric = metric_in_shard("straggler", 8, |s| s >= 4);
    let in_slack_ts = in_slack_hour as i64 * NS_PER_HOUR + 5 * NS_PER_MIN;
    clock.set(in_slack_ts);
    router.refresh_generations(tenant_hash(), gen0_only.clone(), in_slack_ts);
    let in_slack_receipt = router
        .write(
            tenant(),
            vec![point(&straggler_metric, in_slack_ts, 55.0)],
            WriteMode::Strict,
            ACK,
        )
        .await
        .expect("in-slack straggler write");
    assert!(
        in_slack_receipt.tokens.iter().any(|t| t.shard >= 4),
        "the straggler routed under the retiring count 8 into a shard >= 4"
    );

    // A second straggler one hour past the window (activation + S): still routed
    // under count 8 into a shard >= 4, but at an hour `scan_count` no longer
    // covers.
    let beyond_hour = BASE_HOUR + slack;
    let beyond_metric = metric_in_shard("beyond", 8, |s| s >= 4);
    let beyond_ts = beyond_hour as i64 * NS_PER_HOUR + 5 * NS_PER_MIN;
    clock.set(beyond_ts);
    router.refresh_generations(tenant_hash(), gen0_only.clone(), beyond_ts);
    let beyond_receipt = router
        .write(
            tenant(),
            vec![point(&beyond_metric, beyond_ts, 66.0)],
            WriteMode::Strict,
            ACK,
        )
        .await
        .expect("beyond-window straggler write");
    assert!(
        beyond_receipt.tokens.iter().any(|t| t.shard >= 4),
        "the beyond-window write also routed under count 8 into a shard >= 4"
    );
    router.flush_all().await;

    let (_catalog, engine) = engine(store.clone(), 8);

    // In-slack straggler is found: scan_count(activation+1) still includes the
    // retiring count 8, so shard >= 4 is scanned.
    let in_slack_value = instant(
        &engine,
        &straggler_metric,
        in_slack_ts / 1_000_000,
        &[],
        in_slack_hour as i64 * NS_PER_HOUR + 10 * NS_PER_MIN,
    )
    .await;
    assert_eq!(
        value_of(&in_slack_value, &straggler_metric),
        Some(55.0),
        "a straggler inside the slack window is still found"
    );

    // Beyond the window the retiring generation's shard is no longer scanned:
    // the window is bounded at S.
    let beyond_value = instant(
        &engine,
        &beyond_metric,
        beyond_ts / 1_000_000,
        &[],
        beyond_hour as i64 * NS_PER_HOUR + 10 * NS_PER_MIN,
    )
    .await;
    assert_eq!(
        value_of(&beyond_value, &beyond_metric),
        None,
        "past activation + S the retiring generation's shards are outside the scan set"
    );
}

// --------------------------------------------------------------------------
// Acceptance property 3: query via a pre-reshard snapshot HEAD.
// --------------------------------------------------------------------------

/// Both halves of ADR-0052 section 5, proven end to end through a real fold and
/// a real resolve.
///
/// A snapshot HEAD folded before a reshard carries a lower
/// `shard_generation_count` than the reader's post-reshard record. After the
/// reshard it must still be **accepted** -- served, not `FieldMismatch`-rejected
/// -- for the hours at or below its watermark, and Phase 1 listing must cover the
/// newer hours the HEAD predates. This test proves exactly that: the folded
/// hour's data comes back from the HEAD's snapshot parts, and data written into
/// a post-activation hour *above* the watermark (which the HEAD cannot serve)
/// comes back only because Phase 1 listing scans the post-reshard, widened range
/// for those hours. One range query spans both regions and returns both.
///
/// Note on which acceptance arm fires: through a real
/// fold the head's `shard_count` equals the ceiling the reader recomputes at the
/// head's watermark (the fold sealed only hours strictly below the reshard's
/// activation, so the widened generation does not yet raise the ceiling there),
/// so `head_generations_acceptable` accepts on its FIRST arm --
/// `shard_count == shard_ceiling` (4 == 4) -- not on the lower-generation-count
/// arm. The older-head arm is unreachable through a real fold with strictly
/// increasing activation hours, and is exercised directly in
/// `snapshot_resolve`'s unit tests. What this test guarantees is the observable
/// contract: an older HEAD is accepted and serves complete results, and the
/// listing suffix fills in the hours it lacks.
#[tokio::test]
async fn pre_reshard_snapshot_head_still_validates_after_reshard() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    seed_gen0_record(store.as_ref(), 4).await;

    let clock = ManualClock::new(BASE_HOUR as i64 * NS_PER_HOUR);
    let router = build_router(store.clone(), 4, clock.clone());
    let folded_ts = BASE_HOUR as i64 * NS_PER_HOUR + 5 * NS_PER_MIN;
    let folded_metric = metric_in_shard("folded", 4, |_| true);
    router
        .write(
            tenant(),
            vec![point(&folded_metric, folded_ts, 77.0)],
            WriteMode::Strict,
            ACK,
        )
        .await
        .expect("pre-reshard write");
    router.flush_all().await;

    // Fold at a now far enough past BASE that hour BASE is sealed, producing a
    // HEAD whose shard_generation_count is 1 (only generation 0 known).
    let (catalog, _engine) = engine(store.clone(), 4);
    let fold_now = (BASE_HOUR as i64 + 10) * NS_PER_HOUR;
    let report = catalog
        .fold(&tenant_hash(), SIGNAL, Uuid::new_v4(), fold_now, &[], None)
        .await
        .expect("fold succeeds");
    let watermark = report
        .watermark_hour
        .expect("the fold sealed the pre-reshard hour into a HEAD");
    assert!(
        watermark >= BASE_HOUR,
        "the fold's watermark {watermark} covers the folded hour {BASE_HOUR}"
    );
    let head_key = format!("t/{}/catalog/m/HEAD", tenant_hash().to_hex());
    store
        .get(&head_key, ravel_object_store::GetRange::Full)
        .await
        .expect("a snapshot HEAD was written before the reshard");

    // Now reshard (generation count becomes 2 in the reader's record), activating
    // at BASE + 10, so the folded HEAD's shard_generation_count (1) is strictly
    // lower and generation 1 (count 8) is active for hours >= BASE + 10.
    let outcome = reshard(
        store.as_ref(),
        8,
        MIN_LEAD_HOURS,
        (BASE_HOUR as i64 + 8) * NS_PER_HOUR,
    )
    .await;
    assert_eq!(outcome.generations.len(), 2);
    assert_eq!(outcome.generations[1].activation_hour, BASE_HOUR + 10);

    // Write into a post-activation hour ABOVE the HEAD's watermark: the HEAD's
    // parts cannot serve it, so only Phase 1 listing over the widened (count-8)
    // scan range for that hour can find it. It routes under generation 1 into a
    // shard >= 4, a shard the pre-reshard fan-out never scanned.
    let new_hour = BASE_HOUR + 11;
    assert!(
        new_hour > watermark,
        "the new-hour write ({new_hour}) sits above the HEAD watermark ({watermark}), \
         so the HEAD cannot serve it"
    );
    let new_ts = new_hour as i64 * NS_PER_HOUR + 5 * NS_PER_MIN;
    let new_metric = metric_in_shard("newhi", 8, |s| s >= 4);
    clock.set(new_ts);
    let receipt = router
        .write(
            tenant(),
            vec![point(&new_metric, new_ts, 88.0)],
            WriteMode::Strict,
            ACK,
        )
        .await
        .expect("post-activation write above the watermark");
    assert!(
        receipt.tokens.iter().any(|t| t.shard >= 4),
        "the post-activation write routed on the count-8 set (shard >= 4)"
    );
    router.flush_all().await;

    // One range query across both regions: the folded hour (served by the older
    // HEAD, proving it is accepted not rejected) and the post-activation hour
    // above the watermark (served only by Phase 1 listing over the widened range).
    let (_catalog2, engine2) = engine(store.clone(), 4);
    let start_ms = folded_ts / 1_000_000;
    let end_ms = new_ts / 1_000_000;
    let step_ms = end_ms - start_ms;
    let now_ns = new_hour as i64 * NS_PER_HOUR + 10 * NS_PER_MIN;
    let value = range(
        &engine2,
        "{__name__=~\"folded.*|newhi.*\"}",
        start_ms,
        end_ms,
        step_ms,
        now_ns,
    )
    .await;
    assert_eq!(
        matrix_value_of(&value, &folded_metric),
        Some(77.0),
        "the older HEAD is accepted and serves the folded hour (<= watermark) complete"
    );
    assert_eq!(
        matrix_value_of(&value, &new_metric),
        Some(88.0),
        "Phase 1 listing covers the newer hour (> watermark) the HEAD lacks, \
         over the post-reshard count-8 scan range"
    );
}

// --------------------------------------------------------------------------
// Acceptance property 4: read-your-write across a decrease.
// --------------------------------------------------------------------------

/// A commit token minted under the old (larger) generation resolves to the
/// correct object after a decrease has activated and its slack window has
/// closed, even though the scan fan-out no longer covers that token's shard.
/// Commit-token resolution reconstructs the exact key from the token's own
/// fields and never consults `shard_count` (ADR-0052 section 6): the write is
/// placed in shard >= 4 at an ingest hour past `activation + S`, so a plain
/// query does not find it, but a query carrying the token does.
#[tokio::test]
async fn read_your_write_token_resolves_after_decrease() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    seed_gen0_record(store.as_ref(), 8).await;

    let clock = ManualClock::new(BASE_HOUR as i64 * NS_PER_HOUR);
    let router = build_router(store.clone(), 8, clock.clone());
    let sample_ts = BASE_HOUR as i64 * NS_PER_HOUR + 5 * NS_PER_MIN;
    // A series that hashes to shard >= 4 under count 8: only reachable by scan
    // while the count-8 generation covers this hour.
    let ryw_metric = metric_in_shard("ryw", 8, |s| s >= 4);
    let receipt = router
        .write(
            tenant(),
            vec![point(&ryw_metric, sample_ts, 88.0)],
            WriteMode::Strict,
            ACK,
        )
        .await
        .expect("read-your-write write");
    let token = receipt
        .tokens
        .iter()
        .find(|t| t.shard >= 4)
        .cloned()
        .expect("the write minted a token in shard >= 4");
    router.flush_all().await;

    // Decrease 8 -> 4 activating at BASE-5 (now_hour BASE-7, lead 2). With
    // S = 3, activation + S = BASE-2, comfortably before the write's
    // ingest hour (BASE): scan_count(BASE) = 4 and shard >= 4 is not scanned.
    // (Activating at BASE-2 would put the write inside
    // the widened S = 3 slack and destroy the test's purpose -- see the
    // ADR-0052 2026-08-12 amendment.)
    let outcome = reshard(
        store.as_ref(),
        4,
        MIN_LEAD_HOURS,
        (BASE_HOUR as i64 - 7) * NS_PER_HOUR,
    )
    .await;
    assert_eq!(outcome.generations[1].activation_hour, BASE_HOUR - 5);

    let (_catalog, engine) = engine(store.clone(), 8);
    let t_ms = sample_ts / 1_000_000;
    let now_ns = BASE_HOUR as i64 * NS_PER_HOUR + 10 * NS_PER_MIN;

    // Without the token the scan does not reach shard >= 4 at this hour.
    let without = instant(&engine, &ryw_metric, t_ms, &[], now_ns).await;
    assert_eq!(
        value_of(&without, &ryw_metric),
        None,
        "post-decrease scan no longer covers the pre-decrease shard, so a plain query misses it"
    );

    // With the token, read-your-write holds: the exact object resolves.
    let with = instant(
        &engine,
        &ryw_metric,
        t_ms,
        std::slice::from_ref(&token),
        now_ns,
    )
    .await;
    assert_eq!(
        value_of(&with, &ryw_metric),
        Some(88.0),
        "a token minted under the old generation still resolves after the decrease"
    );
}

// --------------------------------------------------------------------------
// Acceptance property 5: stale writer fails closed.
// --------------------------------------------------------------------------

/// Staleness fail-closed, with the bounded grace window (ADR-0052
/// amendment 2026-08-12): once the router's cached view for a
/// tenant ages past `C`, the router re-reads the provisioning record before
/// routing; if that re-read cannot complete (here a store fault on the record
/// GET), the router falls back to
/// [`ravel_ingest::GenerationSwitch::try_grace_extend`] rather than failing
/// the flush immediately. Inside the grace horizon
/// (`hour_of(now_ns) < hour_of(refreshed_at_ns) + min_lead_hours(C)`) the
/// write still routes, on the last-known-good view, and the grace-extended
/// counter increments; only once the horizon is crossed does the write fail
/// closed with the typed [`WriteError::StaleProvisioningView`] and the
/// ordinary stale-flush counter (ADR-0052 section 3). Without the grace
/// window, a write at the still-within-horizon point would be a total outage
/// (an immediate `expect_err`); the grace window replaces that with
/// bounded degraded routing. Tested at the router level: forcing the record
/// re-read to fail deterministically
/// needs a `FaultStore` on the record GET, which live HTTP ingest offers no
/// clean seam for, and the `IngestRouter` is the exact component that
/// enforces this posture.
#[tokio::test]
async fn stale_view_routes_via_grace_window_then_fails_closed_past_horizon() {
    // Every GET of the provisioning record faults, so a stale view can never be
    // refreshed.
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Get, ScriptedFault::Transient("prov unavailable".into()))
            .with_key_contains("/prov"),
    );
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(FaultStore::new(MemoryStore::new(), plan));

    let t0 = BASE_HOUR as i64 * NS_PER_HOUR;
    let clock = ManualClock::new(t0);
    let router = build_router(store.clone(), 4, clock.clone());
    // Prime the cache (the background refresher's job) so the first write is not
    // a first-touch miss.
    router.refresh_generations(
        tenant_hash(),
        vec![ShardGeneration {
            generation: 0,
            shard_count: 4,
            activation_hour: 0,
            appended_unix_ns: 0,
        }],
        t0,
    );

    // Age the cached view past C: the next write must re-read, which faults,
    // but the horizon (hour BASE_HOUR + min_lead_hours(C) = BASE_HOUR + 2 for
    // the default C) has not been crossed, so it routes on the cached view
    // instead of failing closed.
    clock.set(t0 + DEFAULT_REFRESH_INTERVAL_NS + 1);
    let sample_ts = t0 + 5 * NS_PER_MIN;
    router
        .write(
            tenant(),
            vec![point("stale_series", sample_ts, 1.0)],
            WriteMode::Strict,
            ACK,
        )
        .await
        .expect("within the grace horizon, a stale view still routes");
    assert_eq!(
        router.metrics().snapshot().grace_extended_stale_flushes,
        1,
        "the grace-extended counter must increment"
    );
    assert_eq!(
        router.metrics().snapshot().stale_provisioning_flushes,
        0,
        "no flush has failed closed yet"
    );

    // Cross the horizon (hour BASE_HOUR + 2): the same unreachable store now
    // fails the write closed rather than extending the grace window further.
    clock.set((BASE_HOUR as i64 + 2) * NS_PER_HOUR);
    let err = router
        .write(
            tenant(),
            vec![point("stale_series", sample_ts, 1.0)],
            WriteMode::Strict,
            ACK,
        )
        .await
        .expect_err("past the grace horizon, a stale view whose re-read fails must fail closed");
    assert!(
        matches!(err, WriteError::StaleProvisioningView),
        "expected StaleProvisioningView, got: {err:?}"
    );
    assert_eq!(
        router.metrics().snapshot().stale_provisioning_flushes,
        1,
        "the stale-flush counter incremented"
    );

    // A successful refresh clears staleness; the next write routes again, so the
    // failure is fail-closed, not fail-permanent.
    router.refresh_generations(
        tenant_hash(),
        vec![ShardGeneration {
            generation: 0,
            shard_count: 4,
            activation_hour: 0,
            appended_unix_ns: 0,
        }],
        clock.now_ns(),
    );
    router
        .write(
            tenant(),
            vec![point("stale_series", sample_ts, 1.0)],
            WriteMode::Strict,
            ACK,
        )
        .await
        .expect("a refreshed view routes again");
}

// --------------------------------------------------------------------------
// Acceptance property 6: grace-window / read-slack coupling (ADR-0052 amendment 2026-08-12).
// --------------------------------------------------------------------------

/// Pins the exact coupling the amendment's safety argument depends on: a
/// write that the grace window routes under a stale, retiring-generation
/// view (count 8) into an hour the successor decrease generation (count 4)
/// already nominally owns on the real record -- possible under the
/// amendment's new assumption that the reshard-append process's clock may
/// lead the router's by up to `TOLERATED_CLOCK_SKEW_HOURS` -- must still be
/// found on read, because the widened read-side slack `S` covers
/// exactly that overshoot.
///
/// The reshard (8 -> 4, activating at `BASE_HOUR`) is appended for real
/// before the store is wrapped in a `FaultStore`, so the record the router
/// later fails to re-read genuinely holds both generations. The router's
/// cached view (generation 0 only, count 8) is then primed one hour after
/// that real activation and aged past `C`, so the write at hour
/// `activation + 2` goes through `try_grace_extend` (still inside the grace
/// horizon relative to the cache's own refresh time) rather than a fresh
/// read, landing in a shard only the retiring count-8 generation ever wrote.
///
/// Flipped-line proof: with `DEFAULT_SCAN_SLACK_HOURS` set to the narrower
/// value of 2 (`crates/ravel-catalog/src/provisioning.rs:398`,
/// `pub const DEFAULT_SCAN_SLACK_HOURS: u32 = FLUSH_BOUND_SLACK_HOURS +
/// TOLERATED_CLOCK_SKEW_HOURS;`, which evaluates to 2 instead of 3),
/// `scan_count` at `activation + 2` would exclude the retiring generation
/// (`h < activation_hour(successor) + S` becomes `activation + 2 <
/// activation + 2`, false), and the final assertion below (`Some(99.0)`)
/// would fail with `None`.
#[tokio::test]
async fn grace_routed_decrease_straggler_is_covered_by_widened_slack() {
    let store = MemoryStore::new();
    seed_gen0_record(&store, 8).await;

    // Decrease 8 -> 4 activating at BASE_HOUR (now_hour BASE_HOUR-2, lead 2).
    let outcome = reshard(
        &store,
        4,
        MIN_LEAD_HOURS,
        (BASE_HOUR as i64 - 2) * NS_PER_HOUR,
    )
    .await;
    assert_eq!(outcome.generations[1].activation_hour, BASE_HOUR);

    // Only now wrap the store in a FaultStore: seeding and the reshard above
    // must succeed for real first. Only the router's own stale-view re-read
    // (the first post-wrap GET of the record) faults; the query below reads
    // the same record for provisioning enforcement and must see it for real.
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Get, ScriptedFault::Transient("prov unavailable".into()))
            .with_key_contains("/prov")
            .with_occurrence(ravel_object_store::fault::Occurrence::Nth(1)),
    );
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(FaultStore::new(store, plan));

    let clock = ManualClock::new((BASE_HOUR as i64 + 1) * NS_PER_HOUR);
    let router = build_router(store.clone(), 8, clock.clone());
    // The router's last-known-good view: generation 0 only, count 8 -- stale
    // relative to the real record, which already has the decrease active as
    // of BASE_HOUR, simulating a router whose refresh predates that knowledge
    // under clock skew (the amendment's new assumption).
    router.refresh_generations(
        tenant_hash(),
        vec![ShardGeneration {
            generation: 0,
            shard_count: 8,
            activation_hour: 0,
            appended_unix_ns: 0,
        }],
        clock.now_ns(),
    );

    // Age past C with the re-read faulted, at hour activation + 2 =
    // BASE_HOUR + 2: inside the grace horizon (refreshed_at hour BASE_HOUR+1
    // + min_lead_hours(C) = BASE_HOUR+3), so the write routes via grace on
    // the stale count-8 view instead of failing closed.
    let write_hour = BASE_HOUR + 2;
    let write_ts = write_hour as i64 * NS_PER_HOUR + 5 * NS_PER_MIN;
    clock.set(write_ts);
    let straggler_metric = metric_in_shard("coupling", 8, |s| s >= 4);
    let receipt = router
        .write(
            tenant(),
            vec![point(&straggler_metric, write_ts, 99.0)],
            WriteMode::Strict,
            ACK,
        )
        .await
        .expect("grace routes the write on the stale count-8 view");
    assert!(
        receipt.tokens.iter().any(|t| t.shard >= 4),
        "the write landed in a shard only the retiring count-8 generation writes"
    );
    assert_eq!(
        router.metrics().snapshot().grace_extended_stale_flushes,
        1,
        "the write must have gone through the grace path, not a fresh read"
    );
    router.flush_all().await;

    let (_catalog, engine) = engine(store.clone(), 8);
    let value = instant(
        &engine,
        &straggler_metric,
        write_ts / 1_000_000,
        &[],
        write_hour as i64 * NS_PER_HOUR + 10 * NS_PER_MIN,
    )
    .await;
    assert_eq!(
        value_of(&value, &straggler_metric),
        Some(99.0),
        "the widened slack (S = 3) still scans the retiring generation's \
         shards at activation + 2, so the grace-routed straggler is found"
    );
}
