//! ADR-1306 follow-up task 3: the end-to-end proof that a stalled fold pages
//! before it refuses a query.
//!
//! Decision 2 states the ordering as arithmetic over spans. This runs it. One
//! simulated timeline writes flushes at a scaled cadence, folds on a schedule,
//! then wedges the fold with a `FaultStore` fault on its HEAD PUT, and at every
//! simulated minute both renders the catalog metric family and evaluates the
//! SHIPPED `RavelCatalogFoldStalled` condition against it, and runs a cold
//! last-6-hours query under the derived request budget. The two steps that come
//! out of that -- the step the alert fires at and the step the first
//! `RequestBudgetExceeded` lands at -- are the ordering.
//!
//! Everything about the timeline is injected. `fold::run_tenant_tick` reads
//! `SystemClock` directly, so this drives `Catalog::fold` with an explicit
//! `now_ns` rather than the loop, and no step waits on wall-clock time.
//!
//! Why the budget is not `derive_max_s3_requests`. At the shipped 2 s cadence
//! the covered span holds 7,050 flushes per shard, and a fixture with that many
//! segments is not a unit test. The scaled cadence (60 s, 2 shards) keeps the
//! tail in the hundreds, and the budget is rebuilt from
//! `request_budget_parts`'s own fields at that cadence, with the headroom at 1
//! (the case decision 2's proof covers, since the proof never spends the 3/2
//! retry allowance) and the per-shard allowance recomputed at this fixture's
//! per-flush cost. The shipped term sizes every flush at
//! `BUDGETED_REQUESTS_PER_UNSEALED_FLUSH` = 8, the ceiling above the
//! whole-object threshold; these flushes are one sample each, so they cost the
//! measured `REQUESTS_PER_UNSEALED_FLUSH` = 2 and a budget sized at 8 would
//! never refuse this fixture at all (ADR-1306, "Acceptance figures for tasks 3
//! to 7"). Decision 2's proof is parametric in that cost, so substituting it is
//! the scaling, not a weakening.
//!
//! Why the fixed overhead is measured rather than zero. A cold query always
//! spends requests that do not scale with the unsealed tail: the HEAD, the
//! snapshot part and postings GETs, the commit and tombstone LISTs, and one GET
//! per SEALED segment in range. At a zero overhead the scaled budget refuses
//! this timeline at step 53 on those alone, against a page at step 106 --
//! asserted below, so the measurement is load-bearing rather than decorative.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault, Sequence,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, StoreMetricsSnapshot};
use ravel_query::{
    ALERT_DELIVERY_SLACK, EngineConfig, FOLD_LAST_SUCCESS_GAUGE, FOLD_STALL_ALERT_FOR, QueryEngine,
    QueryError, QueryPhase, REQUESTS_PER_UNSEALED_FLUSH, RequestBudgetParts, RequestLimit,
    SealMargin, request_budget_parts,
};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput, VERSION_V7};
use ravel_server::Mode;
use ravel_server::mem_stats::AllocatorStats;
use ravel_server::metrics::{
    AdmissionCountersSnapshot, CatalogCountersSnapshot, IngestBufferBudgetSnapshot,
    MemoryBudgetSnapshot, render,
};
use ravel_types::accounting::AccountedOp;
use ravel_types::{
    Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantHash, TenantId,
};
use uuid::Uuid;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_MIN: i64 = 60 * NS_PER_SEC;
const NS_PER_HOUR: i64 = 3_600 * NS_PER_SEC;

/// The scaled ingest cadence: one flush per shard per simulated minute.
const CADENCE: Duration = Duration::from_secs(60);
/// The scaled shard count.
const SHARDS: u32 = 2;
const METRIC: &str = "m";
/// The cold query each step runs.
const QUERY: &str = "m[6h]";
/// Generous, because a refusal must come from the request budget and never
/// from a deadline a loaded machine blew through.
const DEADLINE: Duration = Duration::from_secs(300);

/// The ingest hour the timeline's watermark sits at once the first fold runs,
/// and stays at for the rest of the run because every later fold fails.
const WATERMARK_HOUR: u32 = 9_000;
/// `end(WATERMARK_HOUR)`, the instant the unsealed tail is measured from.
const END_OF_WATERMARK_NS: i64 = (WATERMARK_HOUR as i64 + 1) * NS_PER_HOUR;
/// Minutes of flushes written below `end(WATERMARK_HOUR)`: the sealed region
/// the snapshot covers, and the source of the sealed-segment GETs in the
/// measured fixed overhead.
const SEALED_MINUTES: i64 = 120;
/// The first flush's event timestamp, one second into the first sealed hour.
const DATA_START_NS: i64 = END_OF_WATERMARK_NS - SEALED_MINUTES * NS_PER_MIN + NS_PER_SEC;
/// Simulated minutes of flushes written before the loop starts. Two hours of
/// them sit above the watermark, so the tail at step 0 is already 2 h and the
/// next hour becomes sealable `STALL_STEP` minutes later: the worst point in
/// the hour, where the healthy tail is at its longest.
const PREFILL_MINUTES: i64 = SEALED_MINUTES + 120;
/// Ten seconds past each minute's flushes, so every flush this step wrote is
/// at or before `now`.
const STEP_OFFSET_NS: i64 = 10 * NS_PER_SEC;
/// Steps between fold attempts.
const FOLD_EVERY: i64 = 5;
/// The step at which a new hour first becomes sealable, so the first step at
/// which a fold reaches its HEAD PUT and the fault wedges it.
const STALL_STEP: i64 = 20;
/// The shipped `RavelCatalogFoldStalled` threshold in seconds: the seal margin
/// of the catalog this test folds with. `shipped_fold_stall_alert_budget.rs`
/// pins the rule file's threshold to that seal margin (follow-up task 4), so
/// deriving it here keeps this test on the threshold the rule actually ships.
fn alert_threshold_secs() -> f64 {
    SealMargin::from_catalog_config(&catalog_config())
        .total()
        .as_secs_f64()
}
/// The span ADR-0075 decision 1 sized the per-shard allowance from, before
/// ADR-1306 decision 1 replaced it with `covered_span`. The mutation proof
/// replays this timeline against it.
const OLD_SPAN: Duration = Duration::from_secs(3_600);
/// Enough steps to reach the refusal with room to spare; the loop asserts one
/// happened rather than running to the end.
const MAX_STEPS: i64 = 240;

/// `t/<hash>/catalog/m/HEAD`, the object whose PUT the fault fails.
const HEAD_KEY_SUFFIX: &str = "/catalog/m/HEAD";

fn catalog_config() -> CatalogConfig {
    CatalogConfig {
        shard_count: SHARDS,
        ..CatalogConfig::default()
    }
}

/// `now` at simulated minute `step`.
fn now_at(step: i64) -> i64 {
    DATA_START_NS - NS_PER_SEC + (PREFILL_MINUTES + step) * NS_PER_MIN + STEP_OFFSET_NS
}

/// The event timestamp of flush `index` on `shard`. One flush per shard per
/// simulated minute, the shards a second apart so no two samples of the one
/// series collide.
fn flush_ts(index: i64, shard: u32) -> i64 {
    DATA_START_NS + index * NS_PER_MIN + i64::from(shard) * NS_PER_SEC
}

/// Writes one single-sample segment and publishes its commit record. Small
/// enough to sit far under the fetcher's whole-object threshold, which is what
/// makes `REQUESTS_PER_UNSEALED_FLUSH` the exact per-flush cost here.
async fn publish_flush(
    store: &dyn ObjectStoreBackend,
    tenant_id: &TenantId,
    tenant_hash: TenantHash,
    shard: u32,
    writer_seq: u64,
    ts_ns: i64,
) {
    let label_set = LabelSet::new(vec![Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: METRIC.to_string(),
    }])
    .expect("valid labels");
    let series_id = SeriesId::compute(tenant_id, METRIC, &label_set).expect("series id");
    let inputs = vec![SeriesInput {
        series_id,
        labels: label_set,
        samples: vec![Sample {
            ts_ns,
            value: writer_seq as f64,
        }],
    }];
    let writer_id = Uuid::new_v4();
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    };
    let written = SegmentWriter::write(inputs, identity, bounds).expect("write segment");
    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard,
        writer_id,
        writer_epoch: 1,
        writer_seq,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: u32::from(VERSION_V7),
        created_unix_ns: 0,
        ingest_hour_bucket: u32::try_from(ts_ns.div_euclid(NS_PER_HOUR)).expect("hour bucket"),
    })
    .expect("valid commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    publish::put_data_object(store, &data_key, written.bytes)
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
}

/// What one cold query cost, or that it was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    /// Total object-store requests across every phase and operation kind.
    Served {
        requests: u64,
    },
    Refused,
}

/// Runs `QUERY` at `now_ns` against a FRESH `Catalog` and `QueryEngine` under
/// `budget`: every record cache, HEAD cache and byte cache starts empty, which
/// is the "cold" the ADR's cost model is stated for.
///
/// `require_fold_lag_wording` is the decision 6 check, applied only to the
/// refusal the ordering is about: the refusal a stalled fold produces must name
/// the tail and the fold-liveness gauge, not read as a budget that is too
/// small.
async fn cold_query(
    store: &Arc<FaultStore<MemoryStore>>,
    tenant_hash: TenantHash,
    now_ns: i64,
    budget: u64,
    require_fold_lag_wording: bool,
) -> Outcome {
    let backend: Arc<dyn ObjectStoreBackend> = store.clone();
    let catalog = Catalog::new(backend.clone(), catalog_config()).expect("catalog");
    let engine = QueryEngine::new(
        Arc::new(catalog),
        backend,
        EngineConfig {
            max_s3_requests: RequestLimit::Bounded(budget),
            ..EngineConfig::default()
        },
    );
    match engine
        .instant_with_stats(
            tenant_hash,
            QUERY,
            now_ns / 1_000_000,
            &[],
            now_ns,
            DEADLINE,
        )
        .await
    {
        Ok((_value, stats)) => {
            let requests: u64 = QueryPhase::ALL
                .iter()
                .map(|phase| {
                    let phase = stats.phase_accounting.phase(*phase);
                    phase.s3_requests(AccountedOp::Get)
                        + phase.s3_requests(AccountedOp::List)
                        + phase.s3_requests(AccountedOp::Head)
                })
                .sum();
            Outcome::Served { requests }
        }
        Err(err) => {
            let QueryError::RequestBudgetExceeded { .. } = &err else {
                panic!("the only refusal this timeline may produce is the request budget: {err}");
            };
            if require_fold_lag_wording {
                let text = err.to_string();
                assert!(
                    text.contains(FOLD_LAST_SUCCESS_GAUGE),
                    "ADR-1306 decision 6: a budget refusal whose resolve read a snapshot part \
                     and saw a tail over `fold_lag_threshold` must name the fold-liveness \
                     gauge, got {text:?}"
                );
            }
            Outcome::Refused
        }
    }
}

/// The value of `ravel_catalog_fold_last_success_timestamp_seconds` for the
/// metrics signal, read off the rendered exposition document rather than off
/// the counter struct: the alert an operator loads reads the rendered series,
/// so that is what this evaluates.
fn rendered_fold_last_success_secs(catalog: &Catalog) -> f64 {
    let snapshot = CatalogCountersSnapshot::from_catalog(catalog);
    let body = render(
        Mode::All,
        &StoreMetricsSnapshot::default(),
        &[],
        &snapshot,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        &AdmissionCountersSnapshot::default(),
        &[],
        0,
        IngestBufferBudgetSnapshot::default(),
        None,
        None,
        &[],
        None,
        AllocatorStats::Other { name: "test" },
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        MemoryBudgetSnapshot::default(),
        true,
    );
    let prefix = format!("{FOLD_LAST_SUCCESS_GAUGE}{{mode=\"all\",signal=\"metrics\"}} ");
    let matches: Vec<&str> = body
        .lines()
        .filter_map(|line| line.strip_prefix(prefix.as_str()))
        .collect();
    assert_eq!(
        matches.len(),
        1,
        "the fold-liveness gauge must render exactly once for the metrics signal; a family \
         that vanished or doubled is not something to average over:\n{body}"
    );
    matches[0].parse().expect("gauge renders a float")
}

/// One simulated minute's verdict.
#[derive(Debug, Clone, Copy)]
struct Step {
    /// The shipped alert's instant condition: the gauge's age past its
    /// threshold.
    condition: bool,
    /// The condition having held for the rule's `for:`, i.e. the alert firing.
    firing: bool,
    outcome: Outcome,
}

#[tokio::test(flavor = "multi_thread")]
async fn fold_stall_alert_fires_before_first_request_budget_refusal() {
    // The fold's HEAD PUT: the first one passes (that is the fold that
    // establishes the watermark this whole timeline is measured against), and
    // every later one fails, so the fold keeps being called and keeps failing
    // rather than quietly stopping. Sequences are consumed before rules, and a
    // fold that seals nothing new returns before it reaches the PUT at all, so
    // the passthrough is spent by the establishing fold and by nothing else --
    // asserted below through `sequence_progress`.
    let plan = FaultPlan::empty()
        .with_sequence(
            Sequence::new(Op::Put)
                .with_key_contains(HEAD_KEY_SUFFIX)
                .then_passthrough(),
        )
        .with_rule(
            Rule::new(
                Op::Put,
                ScriptedFault::Transient("fold HEAD PUT wedged".to_string()),
            )
            .with_key_contains(HEAD_KEY_SUFFIX),
        );
    let store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let backend: Arc<dyn ObjectStoreBackend> = store.clone();
    let tenant_id = TenantId::new("acme".to_string());
    let tenant_hash = tenant_id.hash();

    // The flushes written before the loop starts.
    for index in 0..PREFILL_MINUTES {
        for shard in 0..SHARDS {
            let seq = (index * i64::from(SHARDS) + i64::from(shard) + 1) as u64;
            publish_flush(
                store.as_ref(),
                &tenant_id,
                tenant_hash,
                shard,
                seq,
                flush_ts(index, shard),
            )
            .await;
        }
    }

    // The process's one fold catalog, carrying the fold-liveness counters the
    // metric family renders. It is NOT rebuilt per step: the gauge is a fact
    // about this process's fold history, and a fresh catalog each step would
    // report a fold that never succeeded rather than one that has stopped.
    let fold_catalog = Catalog::new(backend.clone(), catalog_config()).expect("fold catalog");

    // The budget. `request_budget_parts` at the scaled cadence, with the
    // per-shard allowance recomputed at this fixture's per-flush cost and the
    // headroom at 1 (decision 2's own case), and the fixed overhead measured
    // below rather than assumed.
    let shipped = request_budget_parts(CADENCE, SealMargin::REFERENCE);
    let covered_flushes_per_shard =
        shipped.per_shard_allowance / ravel_query::BUDGETED_REQUESTS_PER_UNSEALED_FLUSH;
    assert_eq!(
        covered_flushes_per_shard, 235,
        "sanity: `covered_span` is 14,100 s, which holds 235 flushes per shard at the scaled \
         60 s cadence"
    );

    // The establishing fold, at the same instant step 0 runs at, so the
    // measurement query below sees the watermark the rest of the run is
    // measured against.
    let report = fold_catalog
        .fold(
            &tenant_hash,
            Signal::Metrics,
            Uuid::new_v4(),
            now_at(0),
            &[],
            None,
        )
        .await
        .expect("the first fold passes the sequence's passthrough step");
    assert_eq!(
        report.watermark_hour,
        Some(WATERMARK_HOUR),
        "the establishing fold must seal exactly the hours below the tail this test measures"
    );

    // The measurement. One cold query over the state the establishing fold
    // left, with no budget in the way: its total minus the per-flush part of
    // the unsealed tail is the fixed overhead, exactly the "LISTs, a `HEAD`
    // read and sealed-segment GETs outside the tail" the ADR names. The tail
    // here is the prefilled flushes above the watermark and nothing else; the
    // loop's own first minute has not been written yet.
    let measured_tail_flushes = u64::from(SHARDS) * (PREFILL_MINUTES - SEALED_MINUTES) as u64;
    let measured = cold_query(&store, tenant_hash, now_at(0), u64::MAX, false).await;
    let Outcome::Served {
        requests: measured_total,
    } = measured
    else {
        panic!("the measurement query runs with no bounded budget and cannot be refused");
    };
    let per_flush_part = REQUESTS_PER_UNSEALED_FLUSH * measured_tail_flushes;
    assert!(
        measured_total > per_flush_part,
        "the unsealed tail cannot account for every request a cold query issues: total \
         {measured_total}, tail part {per_flush_part}"
    );
    let fixed_overhead = measured_total - per_flush_part;
    assert_eq!(
        (measured_total, per_flush_part, fixed_overhead),
        (726, 480, 246),
        "pre-registered: the tail holds 120 flushes per shard, each costing a commit-record \
         GET in resolve and a whole-object data GET in plan, and the 246 left over is 120 \
         sealed-segment GETs per shard plus the catalog HEAD, one snapshot part and one \
         postings GET, one commit LIST per shard and one tombstone LIST"
    );

    let parts = RequestBudgetParts {
        per_shard_allowance: covered_flushes_per_shard * REQUESTS_PER_UNSEALED_FLUSH,
        headroom_num: 1,
        headroom_den: 1,
        fixed_overhead,
    };
    let budget = parts.budget(SHARDS);
    // The same parts against ADR-0075's one-hour span: the tree ADR-1306 fixes.
    let old_parts = RequestBudgetParts {
        per_shard_allowance: (OLD_SPAN.as_secs() / CADENCE.as_secs()) * REQUESTS_PER_UNSEALED_FLUSH,
        headroom_num: 1,
        headroom_den: 1,
        fixed_overhead,
    };
    let old_budget = old_parts.budget(SHARDS);
    assert_eq!(
        (budget, old_budget),
        (1_186, 486),
        "pre-registered: 235 x 2 x 2 + 246 against the covered span, and 60 x 2 x 2 + 246 \
         against the one-hour span"
    );

    let mut steps: Vec<Step> = Vec::new();
    let mut condition_true_since: Option<i64> = None;
    let mut alert_step: Option<i64> = None;
    let mut refusal_step: Option<i64> = None;
    let mut old_refusal_step: Option<i64> = None;
    let mut fold_attempts: u64 = 1;

    for step in 0..MAX_STEPS {
        let now_ns = now_at(step);
        // This minute's flushes.
        let index = PREFILL_MINUTES + step;
        for shard in 0..SHARDS {
            let seq = (index * i64::from(SHARDS) + i64::from(shard) + 1) as u64;
            publish_flush(
                store.as_ref(),
                &tenant_id,
                tenant_hash,
                shard,
                seq,
                flush_ts(index, shard),
            )
            .await;
        }

        // The scheduled fold. Step 0's already ran above, as the establishing
        // fold the measurement needed.
        if step > 0 && step % FOLD_EVERY == 0 {
            fold_attempts += 1;
            let result = fold_catalog
                .fold(
                    &tenant_hash,
                    Signal::Metrics,
                    Uuid::new_v4(),
                    now_ns,
                    &[],
                    None,
                )
                .await;
            if step < STALL_STEP {
                result.expect("a fold with no new hour to seal is a healthy no-op");
            } else {
                assert!(
                    result.is_err(),
                    "from the worst point in the hour on, every fold reaches its HEAD PUT and \
                     the fault fails it; step {step} succeeded instead"
                );
            }
        }

        // The shipped alert, evaluated against the rendered gauge.
        let gauge_secs = rendered_fold_last_success_secs(&fold_catalog);
        let now_secs = now_ns as f64 / 1e9;
        let condition = now_secs - gauge_secs > alert_threshold_secs();
        match (condition, condition_true_since) {
            (true, None) => condition_true_since = Some(now_ns),
            (false, _) => condition_true_since = None,
            (true, Some(_)) => {}
        }
        let firing = condition_true_since.is_some_and(|since| {
            now_ns - since >= i64::try_from(FOLD_STALL_ALERT_FOR.as_nanos()).expect("fits i64")
        });
        if firing && alert_step.is_none() {
            alert_step = Some(step);
        }

        // The cold query, under the derived budget.
        let outcome = cold_query(
            &store,
            tenant_hash,
            now_ns,
            budget,
            // Decision 6's wording, asserted on the refusal the ordering is
            // about: one the stalled fold caused, where the tail is hours past
            // `fold_lag_threshold`. A refusal a HEALTHY tail produced (which is
            // what the one-hour span does at step 0) must NOT name fold lag,
            // and is not what this gate is for.
            refusal_step.is_none() && firing,
        )
        .await;
        if outcome == Outcome::Refused && refusal_step.is_none() {
            refusal_step = Some(step);
        }

        // The replay against ADR-0075's one-hour span, over the same timeline,
        // stopped once it has refused: that step is the mutation proof and
        // nothing after it is evidence.
        if old_refusal_step.is_none()
            && cold_query(&store, tenant_hash, now_ns, old_budget, false).await == Outcome::Refused
        {
            old_refusal_step = Some(step);
        }

        steps.push(Step {
            condition,
            firing,
            outcome,
        });
        // Stop once both ends of the ordering are known. Not on the refusal
        // alone: a budget that refuses before the page would then end the
        // timeline with no alert step to compare against, and the failure
        // would read as a missing alert rather than as the ordering inverted.
        if alert_step.is_some() && refusal_step.is_some() {
            break;
        }
    }

    // --- The ordering ---
    let alert_step = alert_step.expect("the stalled fold's alert condition must fire");
    let refusal_step =
        refusal_step.expect("the budget must eventually refuse, or this test proves nothing");
    let slack_steps = i64::try_from(ALERT_DELIVERY_SLACK.as_secs() / CADENCE.as_secs())
        .expect("the delivery slack is whole minutes");
    assert!(
        refusal_step >= alert_step + slack_steps,
        "ADR-1306 decision 2: the page must reach an operator before the first refusal. The \
         alert fired at step {alert_step} and the first refusal landed at step {refusal_step}, \
         {} minutes apart against the {slack_steps} the delivery slack allows",
        refusal_step - alert_step
    );
    assert_eq!(
        (alert_step, refusal_step),
        (106, 115),
        "pre-registered: the last healthy fold is step 15, so the gauge's age passes 4,800 s \
         at step 96 and the 600 s `for:` elapses at step 106; the query costs 730 + 4 per step \
         and passes the 1,186 budget at step 115"
    );

    // --- Non-vacuity ---
    assert!(
        !steps[0].condition,
        "the alert condition must start false, or `firing` says nothing about the stall"
    );
    assert!(
        steps[refusal_step as usize].firing,
        "the alert must still be firing at the refusal, not have resolved before it"
    );
    assert!(
        steps[..refusal_step as usize]
            .iter()
            .all(|step| matches!(step.outcome, Outcome::Served { .. })),
        "every step before the refusal must have been served; a refusal is the first one"
    );
    let served: Vec<u64> = steps
        .iter()
        .filter_map(|step| match step.outcome {
            Outcome::Served { requests } => Some(requests),
            Outcome::Refused => None,
        })
        .collect();
    assert!(
        served.windows(2).all(|pair| pair[1] > pair[0]),
        "the cold query's cost must grow every step as the tail grows, or the refusal is not \
         the tail's doing: {served:?}"
    );

    // The measured overhead is load-bearing, not decoration: at a zero
    // overhead the same timeline refuses well before the page.
    let bare_budget = RequestBudgetParts {
        fixed_overhead: 0,
        ..parts
    }
    .budget(SHARDS);
    let bare_refusal = served
        .iter()
        .position(|requests| *requests > bare_budget)
        .expect("the timeline must exceed the bare allowance too") as i64;
    assert!(
        bare_refusal < alert_step,
        "at a zero fixed overhead the scaled budget refuses at step {bare_refusal}, which must \
         be before the alert at step {alert_step}: if it were not, the measured overhead would \
         be doing no work and this test would pass against a budget that ignores it"
    );
    assert_eq!(
        bare_refusal, 53,
        "pre-registered: 730 + 4 per step passes the bare 940 allowance at step 53"
    );

    // --- The mutation proof ---
    let old_refusal_step = old_refusal_step.expect(
        "the one-hour span must refuse somewhere on this timeline, or the replay is no \
                 mutation at all",
    );
    assert!(
        old_refusal_step < alert_step,
        "ADR-1306's premise: against ADR-0075's one-hour span the refusal comes FIRST. It \
         landed at step {old_refusal_step} and the alert at step {alert_step}"
    );
    assert_eq!(
        old_refusal_step, 0,
        "pre-registered: the one-hour span leaves 240 requests for a tail that already costs \
         484 at step 0, so a cold wide query is refused at every point in a healthy hour, as \
         ADR-1306's 2026-09-26 amendment found"
    );

    // --- The fold really was called, and really failed ---
    let counters = CatalogCountersSnapshot::from_catalog(&fold_catalog);
    let metrics_fold = counters
        .fold
        .iter()
        .find(|entry| entry.signal == Signal::Metrics)
        .expect("the metrics signal renders fold counters");
    assert_eq!(
        (fold_attempts, metrics_fold.cycles, metrics_fold.failures),
        (24, 4, 20),
        "pre-registered: folds run at steps 0, 5, ... 115, which is 24 attempts; the four at \
         steps 0 to 15 succeed and every one from the stall at step 20 on fails, so \
         `ravel_catalog_fold_failures_total` moved 20 times"
    );
    assert_eq!(
        metrics_fold.last_success_unix_ns,
        now_at(STALL_STEP - FOLD_EVERY),
        "the gauge must still name the last healthy fold, which is what makes its age the \
         stall's length"
    );
    assert_eq!(
        store.fault_count(Op::Put, FaultKind::Transient),
        metrics_fold.failures,
        "the FaultStore's own counter must show the HEAD PUT fault fired once per failed fold, \
         so the stall is injected rather than merely assumed"
    );
    assert_eq!(
        store.sequence_progress(0),
        1,
        "exactly one HEAD PUT may have passed through: the establishing fold's. A second would \
         mean a later fold advanced the watermark and the tail this test measures is not the \
         one it thinks"
    );
}
