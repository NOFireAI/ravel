//! Issue #680: a high-cardinality aggregate's peak memory must not scale with
//! the scan's partition count.
//!
//! DataFusion builds one partial-aggregation hash table per input partition and
//! merges them in a single final stage. When a group key's distinct values all
//! appear in every partition -- which is the normal case for a
//! high-cardinality key over a tenant's objects -- the pre-final state is
//! roughly `partitions x distinct` entries, not `distinct`. Spilling is
//! disabled by design (ADR-0102 decision 3, ADR-0013), so that multiplier does
//! not degrade into slow, it fails the query with a typed pool error.
//!
//! `ravel_bench::groupby_scaling::run_distinct` measured it over the `logs`
//! table across `D` in {10k, 100k, 1M} and `target_partitions` in {1, 4, 16,
//! 32}: the 32-partition peak ran 5.0x to 16.2x the single-partition peak for
//! the identical dataset and query. The fix is
//! `SqlConfig::skip_partial_aggregation` (on by default), which tightens
//! DataFusion's two skip-partial-aggregation probe thresholds in
//! `session_config` so a partial partition stops building a hash table once its
//! probe shows the key does not reduce.
//!
//! The fixture is sized for the case the fix exists for: `DISTINCT` values in
//! `OBJECTS` objects means a partition owning one object sees fewer rows than
//! DataFusion's stock 100,000-row probe threshold, so the stock probe never
//! fires at all and each partition builds a full-size table. That is what
//! `target_partitions` dividing a tenant's data actually produces, and it is
//! the regime where the stock thresholds leave the multiplier unbounded.
//!
//! It drives the `logs` table directly (provider + `build_session` +
//! `execute_stream`), the same shape `memory_accounting.rs` uses, rather than
//! the executor: the property is about the session's config, and a hand-driven
//! session is the shortest path from the flipped line to the observed bytes.
//!
//! Prove-the-test: the single flipped line is `skip_partial_aggregation` in
//! `SqlConfig` (crates/ravel-sql/src/config.rs), which gates the
//! `options.execution.skip_partial_aggregation_probe_{rows,ratio}_threshold`
//! writes in `session_config` (crates/ravel-sql/src/session.rs). The one test
//! below measures the fanned-out peak both with it on and with it off -- off
//! being DataFusion's stock 0.8-after-100,000 probe, which is exactly the
//! pre-fix session -- and asserts the tightened figure is a fraction of the
//! untightened one. Stop writing either threshold and the two measurements
//! become the same session, the fraction becomes exactly 1.0, and the test
//! fails.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, execute_stream};
use futures::StreamExt;
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::LogSegmentFetcher;
use ravel_sql::{
    LogsTableProvider, SessionTable, SqlConfig, TenantMemoryAccountant, build_session,
};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use ravel_types::logstream::log_stream_id;
use uuid::Uuid;

/// Distinct values of the high-cardinality key.
///
/// Two constraints fix this number rather than taste. It must be well above
/// `ravel_sql::SKIP_PARTIAL_AGGREGATION_PROBE_ROWS` (8192), or the fixed
/// session's partial tables would be the same size as the unfixed ones and the
/// test would measure nothing. It must stay below DataFusion's stock
/// 100,000-row probe threshold, because a partition holds exactly this many
/// rows here: at or above it the stock probe fires too and the unfixed side
/// stops reproducing the defect. 50,000 sits between them with room on both
/// sides, and the whole fixture is 400,000 records.
///
/// Measured, not assumed: at 80,000 the untightened session's partition ratio
/// fell from about 6x to 3.1x, because a larger table takes longer to build and
/// fewer of them are simultaneously resident at the pool's high-water mark. A
/// bigger `DISTINCT` is not a stronger fixture here.
const DISTINCT: usize = 50_000;

/// RLOG objects. Each carries EVERY one of the `DISTINCT` values, so a
/// partition that owns exactly one object still sees all of them. Without that
/// the partition axis measures nothing: values split across objects would give
/// each partition `DISTINCT / OBJECTS` groups and the per-partition tables
/// would sum to `DISTINCT` no matter how the probe is configured.
const OBJECTS: usize = 8;

/// The scan is un-cached, so `LogsScanExec` keeps the segment-count bound
/// (`min(target_partitions, segment_count)`, ADR-0102 decision 1). Equal to
/// `OBJECTS` so the fanned-out side really reaches this many partitions.
const FANNED_OUT_PARTITIONS: usize = OBJECTS;

/// How far above the single-partition figure the 8-partition
/// aggregation-attributable peak may sit.
///
/// Both sides of this bound are structural, not tuned. The fixed session's
/// partial stage holds at most `partitions x
/// SKIP_PARTIAL_AGGREGATION_PROBE_ROWS` (8 x 8192 = 65,536 entries) on top of a
/// final stage holding all `DISTINCT`, which at 50,000 puts its worst case near
/// 3x; measured 0.11x idle and 2.79x under a fully loaded test runner. The
/// unfixed session holds eight full 50,000-entry tables, a worst case near 9x,
/// and measured 5.33x to 6.34x with only some of them simultaneously resident.
/// Four sits between the two with margin
/// on each side, which the fanned-out figure needs: partial tables are built
/// and drained concurrently, so how many are resident at once moves with
/// machine load, and the fixed figure was measured across an order of magnitude
/// between an idle and a loaded run. The third assertion in the test checks the
/// unfixed side against this same bound rather than trusting the estimate.
const MAX_PEAK_RATIO: f64 = 4.0;

/// The largest share of the untightened session's fanned-out
/// aggregation-attributable peak that the tightened one may reach.
///
/// This is the load-matched assertion, and the one whose red state is
/// structural rather than measured: if `session_config` stops writing the probe
/// thresholds, the two sides become the same session and the share is exactly
/// 1.0. Measured 0.02 to 0.52 across idle and loaded runs; 0.75 leaves room
/// above the loaded figure and well below the no-op value.
const MAX_FIXED_SHARE: f64 = 0.75;

const SCOPE_NAME: &str = "skip-partial-aggregation-test";
const SCOPE_VERSION: &str = "1.0";
const TENANT: TenantHash = TenantHash([9u8; 16]);

/// `COUNT(DISTINCT high)` over the `logs` table: the shape of the ClickBench
/// statements that fail (q05/q06), and the one that puts every distinct value
/// into a group key.
const QUERY: &str = "SELECT count(DISTINCT attrs['u']) AS distinct_high FROM logs";

/// The same scan, the same `attrs` projection and the same `get_field`, with a
/// scalar accumulator instead of a group key: the only thing it does not build
/// is per-group state. The difference between the two peaks is therefore the
/// group state's own bytes, not the projection's. A plain `count(*)` would not
/// do -- DataFusion projects no columns for it, so the scan reserves far less
/// per partition and the subtraction would charge the `attrs` projection's
/// per-partition cost to the aggregate. See [`aggregation_bytes`].
const SCAN_BASELINE: &str = "SELECT count(attrs['u']) AS rows FROM logs";

/// Write `OBJECTS` RLOG objects into `store`, each carrying all `DISTINCT`
/// values exactly once, and return a snapshot over them.
async fn write_high_cardinality_logs(store: &Arc<dyn ObjectStoreBackend>) -> Snapshot {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("hits".to_string()),
    )];
    let stream_id = log_stream_id(&resource, SCOPE_NAME, SCOPE_VERSION, &[]);
    let stream_attrs = stream_attrs_bytes(&resource, SCOPE_NAME, SCOPE_VERSION, &[]);

    let mut segments = Vec::with_capacity(OBJECTS);
    let mut ts = 0i64;
    for object in 0..OBJECTS {
        let identity = ObjectIdentity {
            tenant_hash: TENANT.0,
            shard: 0,
            writer_id: *Uuid::from_u128(0x680_0000 + object as u128).as_bytes(),
            writer_epoch: 1,
            writer_seq: object as u64 + 1,
        };
        let mut writer = RlogWriter::new(RlogConfig::default(), identity);
        let min_ts = ts;
        for value in 0..DISTINCT {
            writer
                .push(LogRecord {
                    stream_id,
                    stream_attrs: stream_attrs.clone(),
                    ts_ns: ts,
                    observed_ts_ns: ts,
                    severity_num: 9,
                    severity_text: "INFO".to_string(),
                    body: String::new(),
                    trace_id: None,
                    span_id: None,
                    flags: 0,
                    attrs: vec![("u".to_string(), AttrValue::Str(format!("u{value:012}")))],
                })
                .expect("push");
            ts += 1;
        }
        let bytes = writer.finish().expect("finish");
        let size = bytes.len() as u64;
        let key = format!("logs/skip-partial-{object}.rlog");
        store
            .put(&key, bytes::Bytes::from(bytes), PutOptions::default())
            .await
            .expect("put");
        segments.push(SegmentRef {
            data_object_key: key,
            object_size: size,
            min_event_ts_ns: min_ts,
            max_event_ts_ns: ts - 1,
            ingest_hour_bucket: 0,
            sample_count: DISTINCT as u64,
            series_count: 1,
            shard: 0,
            content_hash: [object as u8; 32],
            writer_id: Uuid::from_u128(0x680_0000 + object as u128),
            writer_epoch: 1,
            writer_seq: object as u64 + 1,
            created_unix_ns: 0,
            level: SegmentLevel::L0,
        });
    }

    Snapshot {
        segments,
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    }
}

/// The `LogsScanExec` output partition count of `plan`, read from the real
/// plan's properties.
fn logs_scan_partitions(plan: &Arc<dyn ExecutionPlan>) -> usize {
    if plan.name() == "LogsScanExec" {
        return plan.output_partitioning().partition_count();
    }
    plan.children()
        .into_iter()
        .map(logs_scan_partitions)
        .max()
        .unwrap_or(0)
}

/// One run's observations: the pool high-water mark, the scan fan-out that
/// produced it, and the answer the query returned.
struct Run {
    peak_bytes: u64,
    scan_partitions: usize,
    /// The single scalar the query returned, so a run that silently produced
    /// the wrong answer cannot contribute a peak to the ratio.
    answer: i64,
}

/// Run [`QUERY`] at `target_partitions` over the fixture, with
/// `skip_partial_aggregation` as given, and report the pool's high-water mark.
///
/// The peak comes from `QueryAccounting::peak_intermediate_bytes`, which
/// `TenantDelegatingPool` updates on every `grow`, not from polling
/// `pool.reserved()` between output batches: an aggregation emits nothing until
/// its input is consumed, so a between-batches poll would sample only after the
/// partial tables have already been drained into the final stage and miss the
/// peak entirely.
async fn run(
    store: &Arc<dyn ObjectStoreBackend>,
    snapshot: Snapshot,
    sql: &str,
    target_partitions: usize,
    skip_partial_aggregation: bool,
) -> Run {
    let mut config = SqlConfig {
        skip_partial_aggregation,
        // Comfortably above anything this fixture reaches, so the run measures
        // what the plan builds instead of tripping the ceiling.
        max_query_bytes: 8 << 30,
        ..SqlConfig::default()
    };
    config.engine.fetch_concurrency = target_partitions;

    let accounting = QueryAccounting::new();
    let accountant = TenantMemoryAccountant::new(16 << 30);
    let (pool, _breach) = config.query_pool(accountant, accounting.clone());

    let provider = Arc::new(LogsTableProvider::new(
        snapshot,
        TENANT,
        LogSegmentFetcher::new(Arc::clone(store)),
        QueryAccounting::new(),
    ));
    let ctx = build_session(&config, pool, SessionTable::Logs(provider), false).expect("session");

    let plan = ctx
        .sql(sql)
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let scan_partitions = logs_scan_partitions(&plan);

    let mut stream = execute_stream(Arc::clone(&plan), ctx.task_ctx()).expect("execute");
    let mut answer = -1i64;
    while let Some(next) = stream.next().await {
        let batch = next.expect("batch");
        if batch.num_rows() == 1 {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                .expect("a count aggregate is Int64");
            answer = column.value(0);
        }
    }
    drop(stream);

    Run {
        peak_bytes: accounting.snapshot().peak_intermediate_bytes,
        scan_partitions,
        answer,
    }
}

/// The aggregation-attributable peak at `target_partitions`: the
/// `COUNT(DISTINCT)` run's peak minus the peak of a `count(*)` over the same
/// fixture at the same partition count.
///
/// Both the scan and the aggregation grow with partition count, and only the
/// second is issue #680. `LogsScanExec` holds a decoded block plus its batches
/// per partition (ADR-0087), so an eight-partition scan legitimately reserves
/// roughly eight times a one-partition scan's bytes no matter what sits above
/// it -- on this fixture that floor alone is most of the difference. Subtracting
/// a `count(*)` over the identical plan below the aggregate leaves the bytes the
/// group state actually costs, which is the quantity the fix moves.
async fn aggregation_bytes(
    store: &Arc<dyn ObjectStoreBackend>,
    snapshot: &Snapshot,
    target_partitions: usize,
    skip_partial_aggregation: bool,
) -> u64 {
    let grouped = run(
        store,
        snapshot.clone(),
        QUERY,
        target_partitions,
        skip_partial_aggregation,
    )
    .await;
    let baseline = run(
        store,
        snapshot.clone(),
        SCAN_BASELINE,
        target_partitions,
        skip_partial_aggregation,
    )
    .await;

    assert_eq!(
        grouped.answer, DISTINCT as i64,
        "the COUNT(DISTINCT) run at {target_partitions} partitions must count \
         every distinct value"
    );
    assert_eq!(
        baseline.answer,
        (DISTINCT * OBJECTS) as i64,
        "the scalar-count baseline at {target_partitions} partitions must see every row"
    );
    for (label, observed) in [
        ("COUNT(DISTINCT)", grouped.scan_partitions),
        ("scalar count", baseline.scan_partitions),
    ] {
        assert_eq!(
            observed, target_partitions,
            "{label}: target_partitions must reach LogsScanExec, else both sides \
             of the ratio are the same plan"
        );
    }
    assert!(
        grouped.peak_bytes > baseline.peak_bytes,
        "at {target_partitions} partitions the COUNT(DISTINCT) peak ({}) is not \
         above the scalar-count baseline ({}); the subtraction below would be \
         meaningless",
        grouped.peak_bytes,
        baseline.peak_bytes
    );
    grouped.peak_bytes - baseline.peak_bytes
}

/// The pin, plus its own red demonstration, in one test.
///
/// Three figures are taken back to back over one fixture: the
/// aggregation-attributable peak at one partition, at
/// [`FANNED_OUT_PARTITIONS`] with the option on, and at
/// [`FANNED_OUT_PARTITIONS`] with the option off. Three assertions follow:
///
/// 1. The fixed fanned-out figure is at most [`MAX_FIXED_SHARE`] of the unfixed
///    one. This is the option's effect, and it is the assertion that goes red
///    the moment `session_config` stops writing the thresholds: both sides
///    would then be the same session and the share would be exactly 1.0.
/// 2. The fixed fanned-out figure is at most [`MAX_PEAK_RATIO`] times the
///    one-partition figure. This is the bound itself: what the group state
///    costs follows the key's distinct count, not the partition count.
/// 3. The UNFIXED fanned-out figure exceeds that same bound, which is what
///    keeps assertion 2 from being vacuous -- the fixture has to actually
///    reproduce the unbounded multiplier for a bound on it to mean anything.
///
/// All three live in one test on purpose. How many partial hash tables are
/// simultaneously resident is scheduling-dependent, so the fanned-out figure
/// moves with machine load: measured across idle and loaded runs, the fixed
/// figure ranged over an order of magnitude. Taking the fixed and unfixed
/// fanned-out figures inside one test puts them under the same load, which a
/// pair of separate tests (which a parallel runner may schedule minutes apart,
/// or concurrently with anything else) cannot do.
#[tokio::test(flavor = "multi_thread")]
async fn high_cardinality_aggregate_peak_does_not_scale_with_partitions() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let snapshot = write_high_cardinality_logs(&store).await;

    let serial = aggregation_bytes(&store, &snapshot, 1, true).await;
    let fixed = aggregation_bytes(&store, &snapshot, FANNED_OUT_PARTITIONS, true).await;
    let unfixed = aggregation_bytes(&store, &snapshot, FANNED_OUT_PARTITIONS, false).await;

    let share = fixed as f64 / unfixed as f64;
    assert!(
        share <= MAX_FIXED_SHARE,
        "at {FANNED_OUT_PARTITIONS} partitions the tightened probe left the \
         aggregation-attributable peak at {share:.2} of the untightened one \
         ({fixed} bytes vs {unfixed}), above the {MAX_FIXED_SHARE} bound. A \
         share of 1.0 means the two sessions are identical: check that \
         session_config still writes both \
         datafusion.execution.skip_partial_aggregation_probe_* thresholds when \
         SqlConfig::skip_partial_aggregation is on."
    );

    let fixed_ratio = fixed as f64 / serial as f64;
    assert!(
        fixed_ratio <= MAX_PEAK_RATIO,
        "the aggregation-attributable peak at {FANNED_OUT_PARTITIONS} partitions \
         ({fixed} bytes) is {fixed_ratio:.2}x the one-partition figure ({serial} \
         bytes) for D={DISTINCT}, above the {MAX_PEAK_RATIO}x bound. The partial \
         aggregation stage is keeping one full-sized hash table per partition \
         again (issue #680)."
    );

    let unfixed_ratio = unfixed as f64 / serial as f64;
    assert!(
        unfixed_ratio > MAX_PEAK_RATIO,
        "the UNFIXED session's aggregation-attributable ratio is only \
         {unfixed_ratio:.2}x ({unfixed} bytes at {FANNED_OUT_PARTITIONS} \
         partitions vs {serial} at 1), at or below the {MAX_PEAK_RATIO}x bound \
         the fixed session is held to, so the bound above proves nothing. \
         Either this fixture stopped reproducing the defect (every partition \
         must see all D distinct values, and a partition must hold fewer rows \
         than DataFusion's own probe threshold) or DataFusion now bounds the \
         partial stage on its own. Either way both the fixture and the bound \
         need rewriting, not relaxing."
    );
}
