//! Issue #4004 (ADR-4002): the top-keys final aggregate under `GROUP BY ...
//! ORDER BY <group key> LIMIT n`.
//!
//! The plan tests read the physical plan the production session builds. The
//! result tests run each statement twice on that session over in-memory
//! tables, with the rule on and with `bounded_top_keys_max_limit = 0`, and
//! compare the two answers and the figures computed from the input. Where the
//! order ties keys at the n-th row, which tied keys come back is unspecified,
//! so those tests compare the sequence of sort values with the rule-off run
//! and every returned group with its complete figure. The logs-table tests run
//! through `SqlExecutor`, which applies the tie-order rewrite.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int64Array, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::datasource::MemTable;
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, EmitTo, GroupsAccumulator, Signature, Volatility,
};
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::{ExecutionPlan, collect, displayable};
use datafusion::prelude::SessionContext;
use proptest::prelude::*;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_memory::MemoryBudget;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{LogSegmentFetcher, PhaseAccounting, SegmentFetcher};
use ravel_sql::{
    BucketedFinalAggregateExec, BucketedFinalAggregation, DeclaredColumn, DeclaredType,
    FirstGroupsExec, LogsTableProvider, SessionPrincipal, SessionTable, SpanSegmentFetcher,
    SpillDecision, SqlConfig, SqlExecutor, SqlRequest, StaticDeclaredColumns,
    TOP_KEYS_CONSUMER_PREFIX, TenantDelegatingPool, TenantMemoryAccountant, TopKeysAggregateExec,
    build_session, plan_aggregate_profile,
};
use ravel_types::accounting::QueryAccounting;
use ravel_types::logstream::AttrValue;
use ravel_types::{Signal, TenantHash, TenantId, TimeRange};
use uuid::Uuid;

const MIB: usize = 1 << 20;

fn empty_logs() -> SessionTable {
    SessionTable::Logs(Arc::new(LogsTableProvider::new(
        ravel_catalog::Snapshot {
            segments: Vec::new(),
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        },
        TenantHash([0u8; 16]),
        LogSegmentFetcher::new(Arc::new(MemoryStore::new())),
        PhaseAccounting::new(),
    )))
}

fn rule_off(config: &SqlConfig) -> SqlConfig {
    SqlConfig {
        bounded_top_keys_max_limit: 0,
        ..config.clone()
    }
}

/// The session options [`probed`] sets before planning.
const ONE_PARTITION: &[(&str, &str)] = &[("datafusion.execution.target_partitions", "1")];

/// DataFusion's partial-aggregation probe decides after the first row, so a
/// partial aggregate over keys that barely repeat passes its rows on rather
/// than holding them, and batches hold 1,024 rows: the run's fixed costs fit
/// under a 1 MiB pool.
const TIGHT: &[(&str, &str)] = &[
    (
        "datafusion.execution.skip_partial_aggregation_probe_rows_threshold",
        "1",
    ),
    (
        "datafusion.execution.skip_partial_aggregation_probe_ratio_threshold",
        "0.0",
    ),
    ("datafusion.execution.batch_size", "1024"),
];

/// The production session over `pool`, with `parts` registered as `t` and
/// each of `options` set, for a statement classified exact-typed.
fn session(
    config: &SqlConfig,
    pool: Arc<dyn MemoryPool>,
    parts: Vec<Vec<RecordBatch>>,
    options: &[(&str, &str)],
) -> SessionContext {
    session_typed(config, pool, parts, options, true)
}

/// [`session`] under the executor's `exact_typed_aggregates` classification.
/// `true` turns `repartition_aggregations` on (ADR-0094); a float `avg` is
/// never exact-typed, so production plans it under `false`.
fn session_typed(
    config: &SqlConfig,
    pool: Arc<dyn MemoryPool>,
    parts: Vec<Vec<RecordBatch>>,
    options: &[(&str, &str)],
    exact_typed: bool,
) -> SessionContext {
    let ctx = build_session(
        config,
        pool,
        empty_logs(),
        exact_typed,
        SpillDecision::Disabled,
        &SessionPrincipal::without_capabilities(TenantHash([0u8; 16])),
    )
    .expect("session builds");
    {
        let state = ctx.state_ref();
        let mut state = state.write();
        let config_options = state.config_mut().options_mut();
        for (key, value) in options {
            config_options.set(key, value).expect("option sets");
        }
    }
    let schema = parts[0][0].schema();
    let table = MemTable::try_new(schema, parts).expect("mem table builds");
    ctx.register_table("t", Arc::new(table))
        .expect("table registers");
    ctx
}

async fn plan_of(ctx: &SessionContext, sql: &str) -> Arc<dyn ExecutionPlan> {
    ctx.sql(sql)
        .await
        .expect("statement plans")
        .create_physical_plan()
        .await
        .expect("physical plan builds")
}

fn render(plan: &Arc<dyn ExecutionPlan>) -> String {
    format!("{}", displayable(plan.as_ref()).indent(false))
}

/// The `n` of every `TopKeysAggregateExec` in `plan`, in pre-order.
fn top_keys_bounds(plan: &Arc<dyn ExecutionPlan>) -> Vec<usize> {
    let mut out = Vec::new();
    if let Some(node) = plan.downcast_ref::<TopKeysAggregateExec>() {
        out.push(node.n());
    }
    for child in plan.children() {
        out.extend(top_keys_bounds(child));
    }
    out
}

/// How many nodes of `plan` satisfy `matches`.
fn count_nodes(
    plan: &Arc<dyn ExecutionPlan>,
    matches: &dyn Fn(&Arc<dyn ExecutionPlan>) -> bool,
) -> usize {
    usize::from(matches(plan))
        + plan
            .children()
            .into_iter()
            .map(|child| count_nodes(child, matches))
            .sum::<usize>()
}

/// The summed value of the metric `name` over every `TopKeysAggregateExec`.
fn top_keys_metric(plan: &Arc<dyn ExecutionPlan>, name: &str) -> usize {
    let own = plan
        .downcast_ref::<TopKeysAggregateExec>()
        .and_then(|node| node.metrics())
        .and_then(|metrics| metrics.sum_by_name(name))
        .map_or(0, |value| value.as_usize());
    own + plan
        .children()
        .into_iter()
        .map(|child| top_keys_metric(child, name))
        .sum::<usize>()
}

/// Every cell of `batches`, formatted, row by row; NULL as `NULL`.
fn cells(batches: &[RecordBatch]) -> Vec<Vec<String>> {
    let options = FormatOptions::default().with_null("NULL");
    let mut rows = Vec::new();
    for batch in batches {
        let formatters: Vec<_> = batch
            .columns()
            .iter()
            .map(|column| ArrayFormatter::try_new(column.as_ref(), &options).expect("formats"))
            .collect();
        for row in 0..batch.num_rows() {
            rows.push(
                formatters
                    .iter()
                    .map(|formatter| formatter.value(row).to_string())
                    .collect(),
            );
        }
    }
    rows
}

fn sorted(mut rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    rows.sort();
    rows
}

/// Forwards to the query's `TenantDelegatingPool` and records, after every
/// call, the bytes the process budget holds beyond the tenant's charge: the
/// aggregate envelopes, which are charged on the process budget alone. It
/// also records the reservation of each `TopKeys[` consumer and its peak.
#[derive(Debug)]
struct EnvelopeProbe {
    inner: Arc<dyn MemoryPool>,
    tenant: Arc<TenantMemoryAccountant>,
    peak: AtomicI64,
    /// `(reserved, peak)` per `TopKeys[` consumer name.
    top_keys: Mutex<BTreeMap<String, (usize, usize)>>,
}

impl EnvelopeProbe {
    fn sample(&self) {
        let envelope = self.tenant.process_reserved() as i64 - self.tenant.reserved() as i64;
        self.peak.fetch_max(envelope, Ordering::Relaxed);
    }

    /// Record `reservation` growing by `grown` bytes and shrinking by
    /// `shrunk`, when its consumer is a `TopKeys[` one.
    fn track(&self, reservation: &MemoryReservation, grown: usize, shrunk: usize) {
        let name = reservation.consumer().name();
        if !name.starts_with(TOP_KEYS_CONSUMER_PREFIX) {
            return;
        }
        let mut consumers = self.top_keys.lock().expect("probe lock");
        let (reserved, peak) = consumers.entry(name.to_string()).or_default();
        *reserved = (*reserved + grown).saturating_sub(shrunk);
        *peak = (*peak).max(*reserved);
    }

    /// The largest peak any one `TopKeys[` consumer reached, and how many
    /// such consumers registered a reservation.
    fn top_keys_peak(&self) -> (usize, usize) {
        let consumers = self.top_keys.lock().expect("probe lock");
        let peak = consumers.values().map(|(_, peak)| *peak).max().unwrap_or(0);
        (peak, consumers.len())
    }
}

impl fmt::Display for EnvelopeProbe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EnvelopeProbe({})", self.inner)
    }
}

impl MemoryPool for EnvelopeProbe {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);
        self.sample();
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
        self.sample();
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.track(reservation, additional, 0);
        self.sample();
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
        self.track(reservation, 0, shrink);
        self.sample();
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> DFResult<()> {
        let result = self.inner.try_grow(reservation, additional);
        if result.is_ok() {
            self.track(reservation, additional, 0);
        }
        self.sample();
        result
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

/// One statement's run on a probed pool.
struct Probed {
    /// The `n` of each `TopKeysAggregateExec` in the plan.
    bounds: Vec<usize>,
    /// The mode of the aggregate each `TopKeysAggregateExec` replaced.
    modes: Vec<AggregateMode>,
    plan: String,
    result: DFResult<Vec<RecordBatch>>,
    /// The summed `evictions`, `rebuilds`, `dropped_rows` and
    /// `interned_at_threshold` metrics of the plan's `TopKeysAggregateExec`
    /// nodes, after the run.
    evictions: usize,
    rebuilds: usize,
    dropped: usize,
    /// The rows the plan's `AggregateExec` nodes passed on as states without
    /// aggregating them (`skipped_aggregation_rows`).
    skipped_aggregation_rows: usize,
    interned_at_threshold: usize,
    /// The query pool's charged peak, `peak_intermediate_bytes`.
    charged: u64,
    /// The largest aggregate envelope the process budget held.
    envelope: i64,
    /// The largest reservation one `TopKeys[` consumer held, and how many
    /// such consumers reserved anything.
    top_keys_peak: usize,
    top_keys_consumers: usize,
}

impl Probed {
    fn rows(&self) -> Vec<Vec<String>> {
        cells(self.result.as_ref().expect("query runs"))
    }
}

/// The mode of the aggregate inside every `TopKeysAggregateExec` in `plan`.
fn top_keys_modes(plan: &Arc<dyn ExecutionPlan>) -> Vec<AggregateMode> {
    let mut out = Vec::new();
    if let Some(node) = plan.downcast_ref::<TopKeysAggregateExec>() {
        out.push(*node.aggregate().mode());
    }
    for child in plan.children() {
        out.extend(top_keys_modes(child));
    }
    out
}

/// Run `sql` over `parts` on the production session with `options` set, its
/// pool wrapped in an [`EnvelopeProbe`] and given the aggregate profile the
/// executor would install.
async fn probed(
    config: &SqlConfig,
    parts: Vec<Vec<RecordBatch>>,
    options: &[(&str, &str)],
    sql: &str,
) -> Probed {
    probed_typed(config, parts, options, sql, true).await
}

/// [`probed`] on a session built by [`session_typed`] with `exact_typed`.
async fn probed_typed(
    config: &SqlConfig,
    parts: Vec<Vec<RecordBatch>>,
    options: &[(&str, &str)],
    sql: &str,
    exact_typed: bool,
) -> Probed {
    let tenant =
        TenantMemoryAccountant::with_process_budget(1 << 34, Arc::new(MemoryBudget::unlimited()));
    let accounting = QueryAccounting::new();
    let (inner, _breach) = config.query_pool(Arc::clone(&tenant), accounting.clone());
    let probe = Arc::new(EnvelopeProbe {
        inner: Arc::clone(&inner),
        tenant,
        peak: AtomicI64::new(0),
        top_keys: Mutex::new(BTreeMap::new()),
    });
    let ctx = session_typed(
        config,
        Arc::clone(&probe) as Arc<dyn MemoryPool>,
        parts,
        options,
        exact_typed,
    );
    let plan = plan_of(&ctx, sql).await;
    if let Some(profile) = plan_aggregate_profile(&plan) {
        inner
            .downcast_ref::<TenantDelegatingPool>()
            .expect("the query pool is a TenantDelegatingPool")
            .set_aggregate_profile(profile);
    }
    let bounds = top_keys_bounds(&plan);
    let text = render(&plan);
    let result = collect(Arc::clone(&plan), ctx.task_ctx()).await;
    let (top_keys_peak, top_keys_consumers) = probe.top_keys_peak();
    Probed {
        bounds,
        modes: top_keys_modes(&plan),
        plan: text,
        result,
        evictions: top_keys_metric(&plan, "evictions"),
        rebuilds: top_keys_metric(&plan, "rebuilds"),
        dropped: top_keys_metric(&plan, "dropped_rows"),
        skipped_aggregation_rows: aggregate_metric(&plan, "skipped_aggregation_rows"),
        interned_at_threshold: top_keys_metric(&plan, "interned_at_threshold"),
        charged: accounting.snapshot().peak_intermediate_bytes,
        envelope: probe.peak.load(Ordering::Relaxed),
        top_keys_peak,
        top_keys_consumers,
    }
}

/// `sql` with the rule on and off, on a 1 GiB pool, at the engine's default
/// 8 partitions the peak bounds below were measured at. With 16 final
/// streams the rule-off envelope falls to about 27 MB, under the 32 MiB
/// bound that separates it from the rule-on figure.
async fn on_and_off(
    parts: impl Fn() -> Vec<Vec<RecordBatch>>,
    options: &[(&str, &str)],
    sql: &str,
) -> (Probed, Probed) {
    let config = SqlConfig {
        first_attempt_min_partitions: 0,
        ..SqlConfig::default()
    };
    on_and_off_with(&config, parts, options, sql).await
}

/// `sql` under `config`, and under `config` with the rule off.
async fn on_and_off_with(
    config: &SqlConfig,
    parts: impl Fn() -> Vec<Vec<RecordBatch>>,
    options: &[(&str, &str)],
    sql: &str,
) -> (Probed, Probed) {
    let on = probed(config, parts(), options, sql).await;
    let off = probed(&rule_off(config), parts(), options, sql).await;
    assert!(off.bounds.is_empty(), "rule off: {sql}\n{}", off.plan);
    (on, off)
}

fn int64_schema(names: &[&str], nullable: bool) -> SchemaRef {
    Arc::new(Schema::new(
        names
            .iter()
            .map(|name| Field::new(*name, DataType::Int64, nullable))
            .collect::<Vec<_>>(),
    ))
}

/// `rows` (each one value per column of `schema`) as batches of `chunk` rows.
fn int64_batches(schema: &SchemaRef, rows: &[Vec<Option<i64>>], chunk: usize) -> Vec<RecordBatch> {
    if rows.is_empty() {
        return vec![RecordBatch::new_empty(Arc::clone(schema))];
    }
    rows.chunks(chunk)
        .map(|chunk| {
            let columns: Vec<ArrayRef> = (0..schema.fields().len())
                .map(|c| {
                    Arc::new(Int64Array::from_iter(chunk.iter().map(|row| row[c]))) as ArrayRef
                })
                .collect();
            RecordBatch::try_new(Arc::clone(schema), columns).expect("batch builds")
        })
        .collect()
}

/// Arrival order of each partition's rows.
#[derive(Clone, Copy, Debug)]
enum Arrival {
    Ascending,
    Descending,
    /// A fixed pseudo-random permutation, from this seed.
    Shuffled(u64),
}

/// A splitmix64 step: the fixtures' fixed pseudo-random sequence.
fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// `(k, v)` over `partitions`: key `k` has `copies` rows, row `j` in
/// partition `(k + j) mod partitions` with `v = k`, each partition in the
/// order `arrival` names, in batches of 1,024 rows.
fn keyed_parts(keys: i64, partitions: i64, copies: i64, arrival: Arrival) -> Vec<Vec<RecordBatch>> {
    let schema = int64_schema(&["k", "v"], false);
    let mut rows: Vec<Vec<Vec<Option<i64>>>> = vec![Vec::new(); partitions as usize];
    for k in 0..keys {
        for j in 0..copies {
            rows[((k + j) % partitions) as usize].push(vec![Some(k), Some(k)]);
        }
    }
    rows.into_iter()
        .enumerate()
        .map(|(p, mut part)| {
            match arrival {
                Arrival::Ascending => {}
                Arrival::Descending => part.reverse(),
                Arrival::Shuffled(seed) => {
                    let mut state = seed ^ (p as u64).wrapping_mul(0xA24B_AED4_963E_E407);
                    for i in (1..part.len()).rev() {
                        let j = (splitmix(&mut state) % (i as u64 + 1)) as usize;
                        part.swap(i, j);
                    }
                }
            }
            int64_batches(&schema, &part, 1024)
        })
        .collect()
}

/// The `(k, count, sum(v))` rows [`keyed_parts`] gives `keys` in order.
fn keyed_rows(keys: impl IntoIterator<Item = i64>, copies: i64) -> Vec<Vec<String>> {
    keys.into_iter()
        .map(|k| vec![k.to_string(), copies.to_string(), (k * copies).to_string()])
        .collect()
}

const TIED_KEYS: i64 = 12_800;
const TIE_WIDTH: i64 = 64;
const TIED_PARTITIONS: i64 = 4;

/// Rows key `k` of [`tied_parts`] holds: 4 to 6, so one in every partition.
fn tied_rows_of(k: i64) -> i64 {
    4 + k % 3
}

/// `(g, k, v)` over 4 partitions: [`TIED_KEYS`] keys, `g = k / 64`, so 64
/// keys tie on each `g`. Row `j` of key `k` is in partition `(k + j) mod 4`
/// with `v = 10k + j`. Each partition arrives by descending `g`, the
/// adversarial order for `ORDER BY g`, and within one `g` in an order that
/// differs per partition, so partitions meet tied keys in different orders.
fn tied_parts() -> Vec<Vec<RecordBatch>> {
    let schema = int64_schema(&["g", "k", "v"], false);
    let mut rows: Vec<Vec<(i64, i64, i64)>> = vec![Vec::new(); TIED_PARTITIONS as usize];
    for k in 0..TIED_KEYS {
        for j in 0..tied_rows_of(k) {
            rows[((k + j) % TIED_PARTITIONS) as usize].push((k / TIE_WIDTH, k, 10 * k + j));
        }
    }
    rows.into_iter()
        .enumerate()
        .map(|(p, mut part)| {
            let p = p as i64;
            part.sort_by_key(|&(g, k, v)| (std::cmp::Reverse(g), (k * 37 + p * 13) % TIE_WIDTH, v));
            let part: Vec<Vec<Option<i64>>> = part
                .into_iter()
                .map(|(g, k, v)| vec![Some(g), Some(k), Some(v)])
                .collect();
            int64_batches(&schema, &part, 512)
        })
        .collect()
}

/// The complete `(g, k, count, sum(v), max(v))` of key `k` of
/// [`tied_parts`].
fn tied_group(k: i64) -> [i64; 5] {
    let rows = tied_rows_of(k);
    let sum = (0..rows).map(|j| 10 * k + j).sum();
    [k / TIE_WIDTH, k, rows, sum, 10 * k + rows - 1]
}

/// Assert every row of `rows`, `(g, k, aggregates...)`, is a distinct key
/// of [`tied_parts`] whose aggregates equal `want(k)`, the complete figures.
fn assert_complete(rows: &[Vec<String>], want: impl Fn(i64) -> Vec<String>, sql: &str) {
    let mut keys = BTreeSet::new();
    for row in rows {
        let k: i64 = row[1].parse().expect("k is an integer");
        assert_eq!(row, &want(k), "group {k} is not complete: {sql}");
        assert!(keys.insert(k), "key {k} returned twice: {sql}");
    }
}

/// The first column of each row.
fn sort_values(rows: &[Vec<String>]) -> Vec<String> {
    rows.iter().map(|row| row[0].clone()).collect()
}

/// Every key of [`tied_parts`] has a row in each of the four partitions, and
/// the partitions arrive by descending `g`, so a final stage that cuts on
/// what it has seen so far meets the best keys last. The statements run on
/// the default session (a `FinalPartitioned` stage over the hash exchange)
/// and on one partition (a `Final` stage over a `CoalescePartitionsExec`,
/// where one partition's threshold is the global one). With `ORDER BY g`, 64
/// keys tie on each `g` and `LIMIT 100` cuts through the second: which 36 of
/// those come back is unspecified, so the test asserts the sequence of `g`
/// equals the rule-off run and every group is complete. With `ORDER BY g, k`
/// the answer is determined and must equal the rule-off run row for row.
///
/// FLIP 1 (`ranked` keeps the first `n` group indices, first-n-seen): the
/// partition keeps the worst `g` it met first and the `g` sequence differs.
/// FLIP 2 (the rule replaces the partial aggregate below the exchange): each
/// input partition cuts the tied `g = 1` keys in its own order, and groups
/// come back short of their rows.
/// FLIP 3 (`ranked` breaks ties newest index first, so an evicted key whose
/// later rows tie the threshold re-enters ahead of the kept key it tied
/// with): groups come back short of their rows.
#[tokio::test]
async fn the_n_best_keys_carry_every_row_from_every_input_partition() {
    let want = |k: i64| {
        let [g, k, count, sum, _] = tied_group(k);
        vec![
            g.to_string(),
            k.to_string(),
            count.to_string(),
            sum.to_string(),
        ]
    };
    for options in [&[][..], ONE_PARTITION] {
        let options: Vec<_> = options
            .iter()
            .copied()
            .chain([("datafusion.execution.batch_size", "512")])
            .collect();

        let sql = "SELECT g, k, count(*), sum(v) FROM t GROUP BY g, k ORDER BY g LIMIT 100";
        let (on, off) = on_and_off(tied_parts, &options, sql).await;
        assert_eq!(on.bounds, vec![100], "{sql}\n{}", on.plan);
        let rows = on.rows();
        assert_eq!(rows.len(), 100, "{sql}");
        assert_complete(&rows, want, sql);
        assert_eq!(sort_values(&rows), sort_values(&off.rows()), "{sql}");
        let mut g = vec!["0".to_string(); 64];
        g.extend(vec!["1".to_string(); 36]);
        assert_eq!(sort_values(&rows), g, "{sql}");
        assert!(on.rebuilds > 0, "{options:?}: no rebuild ran: {sql}");

        let sql = "SELECT g, k, count(*), sum(v) FROM t GROUP BY g, k ORDER BY g, k LIMIT 100";
        let (on, off) = on_and_off(tied_parts, &options, sql).await;
        assert_eq!(on.bounds, vec![100], "{sql}\n{}", on.plan);
        let want_rows: Vec<_> = (0..100).map(want).collect();
        assert_eq!(on.rows(), want_rows, "{options:?}: {sql}");
        assert_eq!(off.rows(), want_rows, "{options:?}: rule off: {sql}");
    }
}

/// 100,000 keys, two rows each over four partitions, arriving in descending
/// order, under a 1 MiB per-query pool. With the rule each final partition
/// cuts back to 10 groups whenever a batch takes it to 20 or more, and
/// answers; with the rule off it holds every group and the pool refuses the
/// query.
#[tokio::test]
async fn eviction_drops_the_groups_state() {
    let sql = "SELECT k, count(*), sum(v) FROM t GROUP BY k ORDER BY k LIMIT 10";
    let parts = || keyed_parts(100_000, 4, 2, Arrival::Descending);
    let config = SqlConfig {
        max_query_bytes: MIB,
        ..SqlConfig::default()
    };
    let on = probed(&config, parts(), TIGHT, sql).await;
    assert_eq!(on.bounds, vec![10], "{}", on.plan);
    assert_eq!(on.rows(), keyed_rows(0..10, 2));
    assert!(on.evictions > 0, "no group was evicted");
    assert!(on.rebuilds > 0, "no rebuild ran");
    assert!(on.dropped > 0, "no row was dropped at the threshold");
    assert!(on.charged < MIB as u64, "charged peak {}", on.charged);

    let off = probed(&rule_off(&config), parts(), TIGHT, sql).await;
    let err = off.result.expect_err("the rule-off plan exceeds 1 MiB");
    eprintln!(
        "top_keys 1 MiB: charged peak {} with {} evictions and {} rebuilds; rule off: {err}",
        on.charged, on.evictions, on.rebuilds
    );
    assert!(
        err.to_string().contains("Resources exhausted"),
        "rule off: {err}"
    );
}

/// A pool too small for the exec's own table refuses its consumer, and the
/// query fails with `ResourcesExhausted` naming it rather than panicking. On
/// one partition the plan is a `Single` aggregate over the source, so the
/// exec is the first consumer to reserve.
#[tokio::test]
async fn a_refused_reservation_fails_the_query_with_the_consumer_named() {
    let sql = "SELECT k, count(*), sum(v) FROM t GROUP BY k ORDER BY k LIMIT 1000";
    let config = SqlConfig {
        max_query_bytes: 64 * 1024,
        ..SqlConfig::default()
    };
    let on = probed(
        &config,
        keyed_parts(100_000, 1, 1, Arrival::Descending),
        ONE_PARTITION,
        sql,
    )
    .await;
    assert_eq!(on.bounds, vec![1000], "{}", on.plan);
    assert_eq!(on.modes, vec![AggregateMode::Single], "{}", on.plan);
    let err = on.result.expect_err("the exec's table exceeds 64 KiB");
    let message = err.to_string();
    assert!(
        matches!(err.find_root(), DataFusionError::ResourcesExhausted(_)),
        "{err:?}"
    );
    assert!(
        message.contains(&format!("{TOP_KEYS_CONSUMER_PREFIX}0] refused")),
        "{message}"
    );
    assert!(!message.to_lowercase().contains("panic"), "{message}");
}

/// The integer `avg` keeps an exact `Decimal128` sum and an `Int64` count,
/// no float state, so it is admitted, beside the other integer aggregates the
/// session registers. Over [`tied_parts`] with `ORDER BY g, k` the answer is
/// determined, through at least one rebuild, and equals the rule-off run row
/// for row with every aggregate complete.
#[tokio::test]
async fn integer_avg_is_admitted_and_exact_through_rebuilds() {
    let sql = "SELECT g, k, avg(v), count(*), count(v), sum(v), min(v), max(v) \
               FROM t GROUP BY g, k ORDER BY g, k LIMIT 100";
    for options in [&[][..], ONE_PARTITION] {
        let options: Vec<_> = options
            .iter()
            .copied()
            .chain([("datafusion.execution.batch_size", "512")])
            .collect();
        let (on, off) = on_and_off(tied_parts, &options, sql).await;
        assert_eq!(on.bounds, vec![100], "{options:?}\n{}", on.plan);
        assert!(on.rebuilds > 0, "{options:?}: no rebuild ran");
        let rows = on.rows();
        assert_eq!(rows, off.rows(), "{options:?}");
        for (k, row) in rows.iter().enumerate() {
            let [g, k, count, sum, max] = tied_group(k as i64);
            assert_eq!(row[0], g.to_string());
            assert_eq!(row[1], k.to_string());
            assert_eq!(
                row[2],
                format!("{:?}", sum as f64 / count as f64),
                "avg of {k}"
            );
            assert_eq!(row[3], count.to_string());
            assert_eq!(row[4], count.to_string());
            assert_eq!(row[5], sum.to_string());
            assert_eq!(row[6], (10 * k).to_string(), "min of {k}");
            assert_eq!(row[7], max.to_string());
        }
    }
}

/// The `Single` modes update from rows instead of merging states. One input
/// partition on one target partition plans a `Single` aggregate over the
/// source; a `GROUP BY k` over a subquery already grouped and hash
/// partitioned on `k` plans a `SinglePartitioned` one. Both run through
/// rebuilds over [`tied_parts`] and equal the complete figures row for row.
#[tokio::test]
async fn the_single_modes_are_admitted_and_exact() {
    let batch = [("datafusion.execution.batch_size", "512")];
    let one = [ONE_PARTITION, &batch[..]].concat();
    let sql = "SELECT g, k, count(*), sum(v) FROM t GROUP BY g, k ORDER BY g, k LIMIT 100";
    let (on, off) = on_and_off(|| vec![tied_parts().concat()], &one, sql).await;
    assert_eq!(on.modes, vec![AggregateMode::Single], "{}", on.plan);
    assert!(on.rebuilds > 0, "Single: no rebuild ran");
    let want: Vec<Vec<String>> = (0..100)
        .map(|k| tied_group(k)[..4].iter().map(i64::to_string).collect())
        .collect();
    assert_eq!(on.rows(), want, "Single");
    assert_eq!(off.rows(), want, "Single: rule off");

    let sql = "SELECT k, count(*), sum(s) FROM (SELECT k, sum(v) AS s FROM t GROUP BY k) \
               GROUP BY k ORDER BY k LIMIT 100";
    let (on, off) = on_and_off(tied_parts, &batch, sql).await;
    assert_eq!(
        on.modes,
        vec![AggregateMode::SinglePartitioned],
        "{}",
        on.plan
    );
    assert!(on.rebuilds > 0, "SinglePartitioned: no rebuild ran");
    let want: Vec<Vec<String>> = (0..100)
        .map(|k| vec![k.to_string(), "1".to_string(), tied_group(k)[3].to_string()])
        .collect();
    assert_eq!(on.rows(), want, "SinglePartitioned");
    assert_eq!(off.rows(), want, "SinglePartitioned: rule off");
}

/// `ORDER BY g LIMIT 40` over [`tied_parts`] cuts through the 64 keys that
/// tie on `g = 0`. The session runs without the executor's tie-order
/// rewrite, so the sort stays on `g` alone and the ties are real. Every group
/// returned carries its complete count, sum and max.
///
/// FLIP (the mask keeps rows strictly before the threshold): once a rebuild
/// sets the threshold to `g = 0`, every later row of the kept `g = 0` keys is
/// dropped, and they come back short.
#[tokio::test]
async fn ties_at_the_nth_key_return_complete_groups() {
    let sql = "SELECT g, k, count(*), sum(v), max(v) FROM t GROUP BY g, k ORDER BY g LIMIT 40";
    let want = |k: i64| tied_group(k).iter().map(i64::to_string).collect();
    for options in [&[][..], ONE_PARTITION] {
        let options: Vec<_> = options
            .iter()
            .copied()
            .chain([("datafusion.execution.batch_size", "512")])
            .collect();
        let (on, off) = on_and_off(tied_parts, &options, sql).await;
        assert_eq!(on.bounds, vec![40], "{}", on.plan);
        let rows = on.rows();
        assert_complete(&rows, want, sql);
        assert_eq!(sort_values(&rows), vec!["0".to_string(); 40]);
        assert_eq!(sort_values(&off.rows()), vec!["0".to_string(); 40]);
    }
}

/// `(g, k, v)` in one partition, two batches. The first, 80 rows: tied keys
/// `k` 100 to 169 with `g = 0`, then keys 0 to 9 with `g = k - 10`. The
/// second, 20 rows: one more `g = 0` row for each of keys 130 to 139, then
/// keys 200 to 209 with `g = -800 - k`. Row `j` of key `k` has `v = 10k + j`.
fn reentry_parts() -> Vec<Vec<RecordBatch>> {
    let schema = int64_schema(&["g", "k", "v"], false);
    let mut rows = Vec::new();
    let mut push = |g: i64, k: i64, j: i64| rows.push(vec![Some(g), Some(k), Some(10 * k + j)]);
    for k in 100..170 {
        push(0, k, 0);
    }
    for k in 0..10 {
        push(k - 10, k, 0);
    }
    for k in 130..140 {
        push(0, k, 1);
    }
    for k in 200..210 {
        push(-800 - k, k, 0);
    }
    vec![int64_batches(&schema, &rows, 80)]
}

/// Over [`reentry_parts`] with `LIMIT 40` on one final partition, the first
/// batch takes the exec to 80 groups and the rebuild keeps keys 0 to 9 and
/// the first 30 tied keys, 100 to 129, and evicts 130 to 169 at the
/// threshold `g = 0`. The second batch's rows for 130 to 139 tie the
/// threshold, so they pass the mask and those keys re-enter holding one of
/// their two rows; ten better keys follow them. The end-of-input cut must
/// pick the 20 tied keys it keeps from the ones the rebuild kept.
///
/// FLIP (the rebuild interns the kept keys after the next batch's new keys):
/// the re-entered keys get indexes 0 to 9, the cut picks all ten, and each
/// comes back with a count of 1 where the complete group has 2.
/// FLIP (the end-of-input cut ranks by sort row alone, with
/// `sort_unstable_by`): the unstable sort orders the tied keys by neither
/// index nor arrival, and re-entered keys come back with a count of 1.
#[tokio::test]
async fn a_key_evicted_at_the_threshold_and_seen_again_is_cut() {
    let sql = "SELECT g, k, count(*), sum(v) FROM t GROUP BY g, k ORDER BY g LIMIT 40";
    let (on, off) = on_and_off(reentry_parts, ONE_PARTITION, sql).await;
    assert_eq!(on.bounds, vec![40], "{}", on.plan);
    assert_eq!(on.modes, vec![AggregateMode::Single], "{}", on.plan);
    assert_eq!(on.rebuilds, 1, "{}", on.plan);
    assert_eq!(on.interned_at_threshold, 10, "{}", on.plan);
    let all = probed(
        &rule_off(&SqlConfig::default()),
        reentry_parts(),
        ONE_PARTITION,
        "SELECT g, k, count(*), sum(v) FROM t GROUP BY g, k",
    )
    .await
    .rows();
    let complete: BTreeMap<String, Vec<String>> =
        all.into_iter().map(|row| (row[1].clone(), row)).collect();
    let rows = on.rows();
    assert_eq!(rows.len(), 40, "{sql}");
    let mut seen = BTreeSet::new();
    for row in &rows {
        assert_eq!(
            Some(row),
            complete.get(&row[1]),
            "group {} is partial",
            row[1]
        );
        assert!(seen.insert(row[1].clone()), "key {} returned twice", row[1]);
    }
    assert_eq!(sort_values(&rows), sort_values(&off.rows()), "{sql}");
}

/// `(k, v)`, `k` nullable: keys 1 to 1,000, one row each, over four
/// partitions in descending order, and eight NULL-key rows, one first and
/// one last in every partition, each with `v = 7`.
fn null_parts() -> Vec<Vec<RecordBatch>> {
    let schema = int64_schema(&["k", "v"], true);
    (0..4)
        .map(|p| {
            let mut rows = vec![vec![None, Some(7)]];
            rows.extend(
                (1..=1000)
                    .rev()
                    .filter(|k| k % 4 == p)
                    .map(|k| vec![Some(k), Some(k)]),
            );
            rows.push(vec![None, Some(7)]);
            int64_batches(&schema, &rows, 64)
        })
        .collect()
}

/// A NULL group whose rows sit in every partition, at both ends of each.
/// `n = 1`: `NULLS FIRST` returns it with all eight rows in either
/// direction, `NULLS LAST` the smallest or largest key.
#[tokio::test]
async fn null_keys_sort_first_or_last_as_asked() {
    for (order, want) in [
        ("ASC NULLS FIRST", ["NULL", "8", "56"]),
        ("ASC NULLS LAST", ["1", "1", "1"]),
        ("DESC NULLS FIRST", ["NULL", "8", "56"]),
        ("DESC NULLS LAST", ["1000", "1", "1000"]),
    ] {
        let sql =
            format!("SELECT k, count(*), sum(v) FROM t GROUP BY k ORDER BY k {order} LIMIT 1");
        for options in [&[][..], ONE_PARTITION] {
            let (on, off) = on_and_off(null_parts, options, &sql).await;
            assert_eq!(on.bounds, vec![1], "{sql}\n{}", on.plan);
            let want = vec![want.map(str::to_string).to_vec()];
            assert_eq!(on.rows(), want, "{options:?}: {sql}");
            assert_eq!(off.rows(), want, "{options:?}: rule off: {sql}");
        }
    }
}

/// `ORDER BY k DESC LIMIT 10` over keys that arrive in ascending order, the
/// adversarial order for a descending sort, three rows per key.
///
/// FLIP (the threshold mask compares ascending rows, whatever the sort's
/// direction): after the first rebuild every later, larger key is dropped,
/// and the answer is the largest keys of the first batches.
#[tokio::test]
async fn desc_keeps_the_largest_keys() {
    let sql = "SELECT k, count(*), sum(v) FROM t GROUP BY k ORDER BY k DESC LIMIT 10";
    for options in [&[][..], ONE_PARTITION] {
        let options: Vec<_> = options
            .iter()
            .copied()
            .chain([("datafusion.execution.batch_size", "512")])
            .collect();
        let (on, off) = on_and_off(
            || keyed_parts(20_000, 4, 3, Arrival::Ascending),
            &options,
            sql,
        )
        .await;
        assert_eq!(on.bounds, vec![10], "{}", on.plan);
        let want = keyed_rows((19_990..20_000).rev(), 3);
        assert_eq!(on.rows(), want, "{options:?}");
        assert_eq!(off.rows(), want, "{options:?}: rule off");
        assert!(on.rebuilds > 0, "{options:?}: no rebuild ran");
    }
}

/// ClickBench row 43's `LIMIT 10 OFFSET 1000`: the sort fetches 1,010 rows,
/// so the exec keeps 1,010 keys, and the page is keys 1,000 to 1,009.
#[tokio::test]
async fn offset_bounds_by_skip_plus_fetch() {
    let sql = "SELECT k, count(*), sum(v) FROM t GROUP BY k ORDER BY k LIMIT 10 OFFSET 1000";
    let (on, off) = on_and_off(|| keyed_parts(5_000, 4, 2, Arrival::Descending), &[], sql).await;
    assert_eq!(on.bounds, vec![1010], "{}", on.plan);
    assert!(
        on.plan.contains("TopKeysAggregateExec: n=1010"),
        "{}",
        on.plan
    );
    let want = keyed_rows(1000..1010, 2);
    assert_eq!(on.rows(), want);
    assert_eq!(off.rows(), want);
}

/// A subquery's `OFFSET 5` plans a `GlobalLimitExec` between the sort and
/// the aggregate, which may skip any five groups: the rule leaves the plan as
/// it was. One partition in ascending order makes the skipped groups keys 0
/// to 4.
///
/// FLIP (the walk passes `GlobalLimitExec`): the exec keeps keys 0 to 9, the
/// skip eats five of them, and five rows come back.
#[tokio::test]
async fn a_subquery_limit_between_sort_and_aggregate_leaves_the_plan_unchanged() {
    let sql = "SELECT * FROM (SELECT k, count(*) FROM t GROUP BY k OFFSET 5) ORDER BY k LIMIT 10";
    let (on, off) = on_and_off(
        || keyed_parts(100, 1, 1, Arrival::Ascending),
        ONE_PARTITION,
        sql,
    )
    .await;
    assert!(on.bounds.is_empty(), "{}", on.plan);
    assert!(on.plan.contains("GlobalLimitExec"), "{}", on.plan);
    let want: Vec<Vec<String>> = (5..15)
        .map(|k| vec![k.to_string(), "1".to_string()])
        .collect();
    assert_eq!(on.rows(), want);
    assert_eq!(off.rows(), want);
}

/// `count(*) FILTER (WHERE ...)` carries a filter expression the exec does
/// not evaluate.
#[tokio::test]
async fn an_aggregate_filter_leaves_the_plan_unchanged() {
    let sql = "SELECT k, count(*) FILTER (WHERE v > 3), count(*) FROM t \
               GROUP BY k ORDER BY k LIMIT 10";
    let (on, off) = on_and_off(|| keyed_parts(1_000, 4, 2, Arrival::Descending), &[], sql).await;
    assert!(on.bounds.is_empty(), "{}", on.plan);
    let want: Vec<Vec<String>> = (0..10)
        .map(|k| {
            let filtered = if k > 3 { 2 } else { 0 };
            vec![k.to_string(), filtered.to_string(), "2".to_string()]
        })
        .collect();
    assert_eq!(on.rows(), want);
    assert_eq!(off.rows(), want);
}

/// `(k, v)` over four partitions in descending order: keys 0 to 1,999, `v =
/// k`, keys below 100 with one row and the rest with two.
fn having_parts() -> Vec<Vec<RecordBatch>> {
    let schema = int64_schema(&["k", "v"], false);
    let mut rows: Vec<Vec<Vec<Option<i64>>>> = vec![Vec::new(); 4];
    for k in 0..2_000 {
        for j in 0..if k < 100 { 1 } else { 2 } {
            rows[((k + j) % 4) as usize].push(vec![Some(k), Some(k)]);
        }
    }
    rows.into_iter()
        .map(|mut part| {
            part.reverse();
            int64_batches(&schema, &part, 1024)
        })
        .collect()
}

/// `HAVING` puts a `FilterExec` between the sort and the aggregate, and the
/// walk refuses it: the filter drops groups after the aggregate, so the n
/// best groups the exec would keep are not the n best that survive it. Over
/// [`having_parts`] the answer is keys 100 to 109.
///
/// FLIP (the walk passes through a `FilterExec`): each final partition keeps
/// its ten smallest keys, all below 100, the filter drops every one, and the
/// query returns no rows.
#[tokio::test]
async fn having_leaves_the_plan_unchanged() {
    let sql = "SELECT k, count(*) FROM t GROUP BY k HAVING count(*) > 1 ORDER BY k LIMIT 10";
    let want: Vec<Vec<String>> = (100..110)
        .map(|k| vec![k.to_string(), "2".to_string()])
        .collect();
    for options in [&[][..], ONE_PARTITION] {
        let (on, off) = on_and_off(having_parts, options, sql).await;
        assert_eq!(on.rows(), want, "{options:?}");
        assert_eq!(off.rows(), want, "{options:?}: rule off");
        assert!(on.bounds.is_empty(), "{options:?}\n{}", on.plan);
        assert!(on.plan.contains("FilterExec"), "{}", on.plan);
    }
}

/// An aggregate with its own `ORDER BY` is refused: the exec merges states
/// without the ordering they carry. `first_value` is the one ordered
/// aggregate the session registers, for the `DISTINCT ON` rewrite, which is
/// the spelling that reaches it through the executor.
#[tokio::test]
async fn an_aggregate_order_by_leaves_the_plan_unchanged() {
    for sql in [
        "SELECT k, first_value(v ORDER BY v) FROM t GROUP BY k ORDER BY k LIMIT 10",
        "SELECT DISTINCT ON (k) k, v FROM t ORDER BY k, v LIMIT 10",
    ] {
        let (on, off) =
            on_and_off(|| keyed_parts(1_000, 4, 2, Arrival::Descending), &[], sql).await;
        assert!(on.bounds.is_empty(), "{sql}\n{}", on.plan);
        assert_eq!(on.rows().len(), 10, "{sql}");
        assert_eq!(on.rows(), off.rows(), "{sql}");
    }
}

/// A fetch above `bounded_top_keys_max_limit` is refused: 1,025 under the
/// default ceiling of 1,024, and 10 under a ceiling of 5. The fetch at each
/// ceiling is admitted.
#[tokio::test]
async fn a_limit_above_the_ceiling_leaves_the_plan_unchanged() {
    let parts = || keyed_parts(3_000, 4, 2, Arrival::Descending);
    let low = SqlConfig {
        bounded_top_keys_max_limit: 5,
        ..SqlConfig::default()
    };
    for (config, limit, bounds) in [
        (SqlConfig::default(), 1_025, vec![]),
        (SqlConfig::default(), 1_024, vec![1_024]),
        (low.clone(), 10, vec![]),
        (low, 5, vec![5]),
    ] {
        let sql = format!("SELECT k, count(*), sum(v) FROM t GROUP BY k ORDER BY k LIMIT {limit}");
        let (on, off) = on_and_off_with(&config, parts, &[], &sql).await;
        assert_eq!(on.bounds, bounds, "{sql}\n{}", on.plan);
        assert_eq!(on.rows(), keyed_rows(0..limit, 2), "{sql}");
        assert_eq!(off.rows(), keyed_rows(0..limit, 2), "{sql}: rule off");
    }
}

/// `(k, f)` over four partitions in descending order: keys 0 to 1,999, two
/// rows each. Key 0 holds three `-0.0` rows and nothing else; key `k` holds
/// `f = k + 0.25`.
fn float_state_parts() -> Vec<Vec<RecordBatch>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("f", DataType::Float64, false),
    ]));
    let mut rows: Vec<Vec<(i64, f64)>> = vec![Vec::new(); 4];
    for k in 0..2_000i64 {
        let copies = if k == 0 { 3 } else { 2 };
        let f = if k == 0 { -0.0 } else { k as f64 + 0.25 };
        for j in 0..copies {
            rows[((k + j) % 4) as usize].push((k, f));
        }
    }
    rows.into_iter()
        .map(|mut part| {
            part.reverse();
            part.chunks(1024)
                .map(|chunk| {
                    let k = Int64Array::from_iter_values(chunk.iter().map(|row| row.0));
                    let f = Float64Array::from_iter_values(chunk.iter().map(|row| row.1));
                    RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(k), Arc::new(f)])
                        .expect("batch builds")
                })
                .collect()
        })
        .collect()
}

/// A float state whose merge into a fresh accumulator is not an identity is
/// refused even when the key is an integer. A float `sum` runs DataFusion's
/// built-in accumulator, whose fresh group is seeded at `0.0`, and
/// `0.0 + -0.0` is `+0.0`, so a `-0.0` state does not survive that merge.
/// The same seed already makes the stock plan's sum of the `-0.0` group of
/// [`float_state_parts`] `+0.0`, so the refusal is pinned by the plan, and
/// the values by the rule-off run. One such aggregate beside an admitted
/// `avg` refuses the whole statement.
///
/// FLIP (admit every float state): the plan holds `TopKeysAggregateExec:
/// n=10` and the first assertion fails.
#[tokio::test]
async fn a_float_state_that_is_not_an_identity_stays_refused() {
    for sql in [
        "SELECT k, sum(f) FROM t GROUP BY k ORDER BY k LIMIT 10",
        "SELECT k, sum(f), avg(f), count(*) FROM t GROUP BY k ORDER BY k LIMIT 10",
    ] {
        for options in [&[][..], ONE_PARTITION] {
            let (on, off) = on_and_off(float_state_parts, options, sql).await;
            assert!(on.bounds.is_empty(), "{sql}\n{}", on.plan);
            let rows = on.rows();
            assert_eq!(rows.len(), 10, "{sql}");
            assert_eq!(rows[0][0], "0", "{sql}");
            assert_eq!(rows[0][1], "0.0", "{sql}: the -0.0 group's sum");
            assert_eq!(rows[1][..2], ["1", "2.5"], "{sql}");
            assert_eq!(rows, off.rows(), "{sql}");
        }
    }
}

/// DataFusion's built-in `avg`, registered under the name `avg` over Ravel's,
/// is refused by the state layout: its state is a `UInt64` count and then a
/// `Float64` sum, not `[Float64, Int64]`. The implementation check refuses it
/// too, so this test does not pin that check on its own;
/// [`a_same_named_avg_with_a_matching_state_layout_stays_refused`] does.
///
/// FLIP (key the allowlist on the name `avg`, dropping the implementation
/// and state-layout checks): the built-in is admitted and the plan holds
/// `TopKeysAggregateExec: n=10`.
#[tokio::test]
async fn a_same_named_avg_with_another_accumulator_stays_refused() {
    let sql = "SELECT k, avg(f) FROM t GROUP BY k ORDER BY k LIMIT 10";
    let (pool, _breach) = SqlConfig::default()
        .query_pool(TenantMemoryAccountant::new(1 << 30), QueryAccounting::new());
    let ctx = session(&SqlConfig::default(), pool, float_state_parts(), &[]);
    let builtin = datafusion::functions_aggregate::average::avg_udaf();
    assert_eq!(builtin.name(), "avg");
    ctx.register_udaf(builtin.as_ref().clone());
    let plan = plan_of(&ctx, sql).await;
    assert!(top_keys_bounds(&plan).is_empty(), "{}", render(&plan));
    let mut names = Vec::new();
    aggregate_function_names(&plan, &mut names);
    assert!(!names.is_empty(), "{}", render(&plan));
    assert!(names.iter().all(|name| name == "avg"), "{names:?}");
    let mut states = Vec::new();
    aggregate_state_types(&plan, &mut states);
    assert!(!states.is_empty(), "{}", render(&plan));
    assert!(
        states
            .iter()
            .all(|types| types[..] == [DataType::UInt64, DataType::Float64]),
        "{states:?}"
    );
}

/// A test-local aggregate named `avg` with Ravel's float state layout,
/// `[Float64 sum, Int64 count]`, and a `GroupsAccumulator` whose fresh group's
/// sum is seeded at `0.0`, so merging a `-0.0` state into it gives `+0.0`:
/// its rebuild is not an identity.
#[derive(Debug, PartialEq, Eq, Hash)]
struct SeededAvg {
    signature: Signature,
}

impl AggregateUDFImpl for SeededAvg {
    fn name(&self) -> &str {
        "avg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float64)
    }

    fn accumulator(&self, _args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        Err(DataFusionError::NotImplemented(
            "SeededAvg runs only as a GroupsAccumulator".to_string(),
        ))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> DFResult<Vec<FieldRef>> {
        Ok(vec![
            Arc::new(Field::new(
                format!("{}[sum]", args.name),
                DataType::Float64,
                true,
            )),
            Arc::new(Field::new(
                format!("{}[count]", args.name),
                DataType::Int64,
                true,
            )),
        ])
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> DFResult<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(SeededAvgGroups::default()))
    }
}

#[derive(Debug, Default)]
struct SeededAvgGroups {
    sums: Vec<f64>,
    counts: Vec<i64>,
}

impl SeededAvgGroups {
    fn resize(&mut self, total_num_groups: usize) {
        self.sums.resize(total_num_groups, 0.0);
        self.counts.resize(total_num_groups, 0);
    }
}

impl GroupsAccumulator for SeededAvgGroups {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        _opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> DFResult<()> {
        self.resize(total_num_groups);
        let values = values[0]
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("a Float64 argument");
        for (value, &group) in values.iter().zip(group_indices) {
            if let Some(value) = value {
                self.sums[group] += value;
                self.counts[group] += 1;
            }
        }
        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> DFResult<()> {
        self.resize(total_num_groups);
        let sums = values[0]
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("a Float64 sum");
        let counts = values[1]
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("an Int64 count");
        for (row, &group) in group_indices.iter().enumerate() {
            self.sums[group] += sums.value(row);
            self.counts[group] += counts.value(row);
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> DFResult<ArrayRef> {
        let sums = emit_to.take_needed(&mut self.sums);
        let counts = emit_to.take_needed(&mut self.counts);
        Ok(Arc::new(Float64Array::from_iter(
            sums.into_iter()
                .zip(counts)
                .map(|(sum, count)| (count > 0).then(|| sum / count as f64)),
        )))
    }

    fn state(&mut self, emit_to: EmitTo) -> DFResult<Vec<ArrayRef>> {
        let sums = emit_to.take_needed(&mut self.sums);
        let counts = emit_to.take_needed(&mut self.counts);
        Ok(vec![
            Arc::new(Float64Array::from(sums)),
            Arc::new(Int64Array::from(counts)),
        ])
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        _opt_filter: Option<&BooleanArray>,
    ) -> DFResult<Vec<ArrayRef>> {
        let values = values[0]
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("a Float64 argument");
        Ok(vec![
            Arc::new(Float64Array::from_iter_values(
                values.iter().map(|value| value.unwrap_or(0.0)),
            )),
            Arc::new(Int64Array::from_iter_values(
                values.iter().map(|value| i64::from(value.is_some())),
            )),
        ])
    }

    fn size(&self) -> usize {
        self.sums.capacity() * size_of::<f64>() + self.counts.capacity() * size_of::<i64>()
    }
}

/// An `avg` that matches the allowlist on every point but the implementation
/// (the name, one `Float64` argument, not distinct, state `[Float64, Int64]`)
/// is refused: [`SeededAvg`] seeds a fresh sum at `0.0`, so its rebuild would
/// turn a `-0.0` state into `+0.0`. The plan is checked to run that aggregate,
/// with that state layout, so only the implementation check refuses it.
///
/// FLIP (delete the `aggr.fun() == &*SEQUENTIAL_AVG` conjunct from
/// `float_state_is_identity`): the plan holds `TopKeysAggregateExec: n=10`
/// and the first assertion fails.
#[tokio::test]
async fn a_same_named_avg_with_a_matching_state_layout_stays_refused() {
    let sql = "SELECT k, avg(f) FROM t GROUP BY k ORDER BY k LIMIT 10";
    let (pool, _breach) = SqlConfig::default()
        .query_pool(TenantMemoryAccountant::new(1 << 30), QueryAccounting::new());
    let ctx = session_typed(&SqlConfig::default(), pool, float_state_parts(), &[], false);
    ctx.register_udaf(AggregateUDF::new_from_impl(SeededAvg {
        signature: Signature::exact(vec![DataType::Float64], Volatility::Immutable),
    }));
    let plan = plan_of(&ctx, sql).await;
    assert!(top_keys_bounds(&plan).is_empty(), "{}", render(&plan));
    let mut seeded = 0;
    let mut states = Vec::new();
    for_each_aggregate(&plan, &mut |aggregate| {
        for aggr in aggregate.aggr_expr() {
            assert_eq!(aggr.fun().name(), "avg");
            let implementation: &dyn Any = aggr.fun().inner().as_ref();
            assert!(implementation.is::<SeededAvg>(), "{:?}", aggr.fun());
            assert!(!aggr.is_distinct());
            seeded += 1;
        }
    });
    aggregate_state_types(&plan, &mut states);
    assert!(seeded > 0, "{}", render(&plan));
    assert!(
        states
            .iter()
            .all(|types| types[..] == [DataType::Float64, DataType::Int64]),
        "{states:?}"
    );
}

/// Call `f` on every `AggregateExec` in `plan`, in pre-order.
fn for_each_aggregate(plan: &Arc<dyn ExecutionPlan>, f: &mut dyn FnMut(&AggregateExec)) {
    if let Some(aggregate) = plan.downcast_ref::<AggregateExec>() {
        f(aggregate);
    }
    for child in plan.children() {
        for_each_aggregate(child, f);
    }
}

/// The state field types of every aggregate expression of every
/// `AggregateExec` in `plan`.
fn aggregate_state_types(plan: &Arc<dyn ExecutionPlan>, out: &mut Vec<Vec<DataType>>) {
    for_each_aggregate(plan, &mut |aggregate| {
        out.extend(aggregate.aggr_expr().iter().map(|aggr| {
            aggr.state_fields()
                .expect("state fields")
                .iter()
                .map(|field| field.data_type().clone())
                .collect::<Vec<_>>()
        }));
    });
}

/// The function name of every aggregate expression of every `AggregateExec`
/// in `plan`.
fn aggregate_function_names(plan: &Arc<dyn ExecutionPlan>, out: &mut Vec<String>) {
    if let Some(aggregate) = plan.downcast_ref::<AggregateExec>() {
        out.extend(
            aggregate
                .aggr_expr()
                .iter()
                .map(|aggr| aggr.fun().name().to_string()),
        );
    }
    for child in plan.children() {
        aggregate_function_names(child, out);
    }
}

/// A quiet NaN with payload 1, a quiet NaN with the sign bit and payload
/// `0xb0`, and a signalling NaN with payload 1.
const NAN_A: u64 = 0x7ff8_0000_0000_0001;
const NAN_B: u64 = 0xfff8_0000_0000_00b0;
const SNAN: u64 = 0x7ff0_0000_0000_0001;
/// The smallest positive subnormal, `5e-324`.
const SUBNORMAL: u64 = 0x0000_0000_0000_0001;

/// Each value of a `Float64` array as its bits; NULL as `None`.
fn float_bits(array: &ArrayRef) -> Vec<Option<u64>> {
    array
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("a Float64 array")
        .iter()
        .map(|value| value.map(f64::to_bits))
        .collect()
}

fn int_values(array: &ArrayRef) -> Vec<Option<i64>> {
    array
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("an Int64 array")
        .iter()
        .collect()
}

/// The aggregate expression `avg` of `sql`'s `TopKeysAggregateExec`, which
/// must be present: the statement is admitted.
async fn admitted_avg(parts: Vec<Vec<RecordBatch>>, sql: &str) -> Arc<AggregateFunctionExpr> {
    let (pool, _breach) = SqlConfig::default()
        .query_pool(TenantMemoryAccountant::new(1 << 30), QueryAccounting::new());
    let ctx = session(&SqlConfig::default(), pool, parts, &[]);
    let plan = plan_of(&ctx, sql).await;
    fn find(plan: &Arc<dyn ExecutionPlan>) -> Option<&TopKeysAggregateExec> {
        plan.downcast_ref::<TopKeysAggregateExec>()
            .or_else(|| plan.children().into_iter().find_map(find))
    }
    let node = find(&plan).unwrap_or_else(|| panic!("{sql}: not admitted\n{}", render(&plan)));
    let aggr = Arc::clone(&node.aggregate().aggr_expr()[0]);
    assert_eq!(aggr.fun().name(), "avg");
    aggr
}

/// [`float_state_parts`] with `f` as `Float32`.
fn float32_state_parts() -> Vec<Vec<RecordBatch>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("g", DataType::Float32, false),
    ]));
    float_state_parts()
        .into_iter()
        .map(|part| {
            part.into_iter()
                .map(|batch| {
                    let g = Float32Array::from_iter_values(
                        batch
                            .column(1)
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .expect("f is Float64")
                            .values()
                            .iter()
                            .map(|f| *f as f32),
                    );
                    RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![Arc::clone(batch.column(0)), Arc::new(g)],
                    )
                    .expect("batch builds")
                })
                .collect()
        })
        .collect()
}

/// The admitted `avg` state, `[Float64 sum, Int64 count]`, merged into a
/// fresh accumulator over the same group indexes is the same state bit for
/// bit, and so is the value evaluated from it. That merge is exactly what a
/// rebuild does to each kept group. Twelve groups cover `-0.0` (one and two
/// rows), `+0.0`, two quiet NaN payloads (one with the sign bit set, one
/// beside a finite row), a signalling NaN, `+inf`, `-inf`, the smallest
/// subnormal, a NULL-only group, a group with no rows (both are the null sum
/// with count 0), and a finite mixed group. The source accumulator is filled
/// by `update_batch`, as the partial stage fills it. An empty state merges
/// to an empty state. The same holds for the `avg` planned over a `Float32`
/// column, which coerces to `Float64` before the accumulator.
///
/// FLIP (seed a fresh group's sum at `Some(0.0)` in `avg.rs`): the `-0.0`
/// groups come back as `+0.0`, the signalling NaN comes back quieted and the
/// two empty groups as `+0.0`; the source sums already differ, since
/// `update_batch` starts from the same seed.
#[tokio::test]
async fn the_identity_merge_is_bit_exact_for_admitted_float_states() {
    let rows: [(usize, Option<u64>); 15] = [
        (0, Some((-0.0f64).to_bits())),
        (1, Some((-0.0f64).to_bits())),
        (1, Some((-0.0f64).to_bits())),
        (2, Some(0.0f64.to_bits())),
        (3, Some(NAN_A)),
        (4, Some(NAN_B)),
        (4, Some(1.0f64.to_bits())),
        (5, Some(SNAN)),
        (6, Some(f64::INFINITY.to_bits())),
        (7, Some(f64::NEG_INFINITY.to_bits())),
        (8, Some(SUBNORMAL)),
        (9, None),
        (11, Some(1.5f64.to_bits())),
        (11, Some((-0.5f64).to_bits())),
        (11, Some(0.0f64.to_bits())),
    ];
    const GROUPS: usize = 12;
    let want_sums: [Option<u64>; GROUPS] = [
        Some((-0.0f64).to_bits()),
        Some((-0.0f64).to_bits()),
        Some(0.0f64.to_bits()),
        Some(NAN_A),
        Some(NAN_B),
        Some(SNAN),
        Some(f64::INFINITY.to_bits()),
        Some(f64::NEG_INFINITY.to_bits()),
        Some(SUBNORMAL),
        None,
        None,
        Some(1.0f64.to_bits()),
    ];
    let want_counts: [Option<i64>; GROUPS] = [1, 2, 1, 1, 2, 1, 1, 1, 1, 0, 0, 3].map(Some);
    let values: ArrayRef = Arc::new(Float64Array::from_iter(
        rows.iter().map(|(_, value)| value.map(f64::from_bits)),
    ));
    let groups: Vec<usize> = rows.iter().map(|(group, _)| *group).collect();
    let indexes: Vec<usize> = (0..GROUPS).collect();

    for (parts, sql) in [
        (
            float_state_parts(),
            "SELECT k, avg(f) FROM t GROUP BY k ORDER BY k LIMIT 10",
        ),
        (
            float32_state_parts(),
            "SELECT k, avg(g) FROM t GROUP BY k ORDER BY k LIMIT 10",
        ),
    ] {
        let aggr = admitted_avg(parts, sql).await;
        let state_types: Vec<DataType> = aggr
            .state_fields()
            .expect("state fields")
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        assert_eq!(state_types, [DataType::Float64, DataType::Int64], "{sql}");
        let updated = || {
            let mut source = aggr.create_groups_accumulator().expect("accumulator");
            source
                .update_batch(&[Arc::clone(&values)], &groups, None, GROUPS)
                .expect("update");
            source
        };
        let state = updated().state(EmitTo::All).expect("state");
        assert_eq!(float_bits(&state[0]), want_sums, "{sql}: the source sums");
        assert_eq!(
            int_values(&state[1]),
            want_counts,
            "{sql}: the source counts"
        );
        let evaluated = updated().evaluate(EmitTo::All).expect("evaluate");

        let merged = || {
            let mut fresh = aggr.create_groups_accumulator().expect("accumulator");
            fresh.merge_batch(&state, &indexes, GROUPS).expect("merge");
            fresh
        };
        let rebuilt = merged().state(EmitTo::All).expect("state");
        assert_eq!(float_bits(&rebuilt[0]), want_sums, "{sql}: rebuilt sums");
        assert_eq!(
            int_values(&rebuilt[1]),
            want_counts,
            "{sql}: rebuilt counts"
        );
        let rebuilt_evaluated = merged().evaluate(EmitTo::All).expect("evaluate");
        assert_eq!(
            float_bits(&rebuilt_evaluated),
            float_bits(&evaluated),
            "{sql}: the evaluated averages"
        );
        let averages = float_bits(&evaluated);
        assert_eq!(averages[0], Some((-0.0f64).to_bits()), "{sql}");
        assert_eq!(averages[1], Some((-0.0f64).to_bits()), "{sql}");
        assert_eq!(averages[3], Some(NAN_A), "{sql}");
        assert_eq!(averages[4], Some(NAN_B), "{sql}");
        assert_eq!(averages[8], Some(SUBNORMAL), "{sql}");
        assert_eq!(averages[9..11], [None, None], "{sql}");
        assert_eq!(averages[11], Some((1.0f64 / 3.0).to_bits()), "{sql}");

        let mut empty = aggr.create_groups_accumulator().expect("accumulator");
        let none: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(Vec::<f64>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
        ];
        empty.merge_batch(&none, &[], 0).expect("merge");
        let emptied = empty.state(EmitTo::All).expect("state");
        assert_eq!(
            emptied
                .iter()
                .map(|column| column.len())
                .collect::<Vec<_>>(),
            [0, 0]
        );
    }
}

/// 2,000 minutes of `(ts, v)`, `ts` a nanosecond timestamp a few seconds
/// into its minute, over 8 partitions; row `j` of minute `m` is in partition
/// `(m + j) mod 8` and every partition holds its minutes in ascending order,
/// the adversarial order for `ORDER BY ... DESC`. A minute's rows follow its
/// class `m mod 8`, see [`minute_class_rows`].
fn float_minute_parts() -> Vec<Vec<RecordBatch>> {
    const PARTITIONS: usize = 8;
    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("v", DataType::Float64, true),
    ]));
    let mut rows: Vec<Vec<(i64, Option<f64>)>> = vec![Vec::new(); PARTITIONS];
    for minute in 0..FLOAT_MINUTES {
        for (j, value) in minute_class_rows(minute).into_iter().enumerate() {
            let ts = minute * MINUTE_NS + (j as i64 + 1) * 7_000_000_000;
            rows[(minute as usize + j) % PARTITIONS].push((ts, value));
        }
    }
    rows.into_iter()
        .map(|part| {
            part.chunks(512)
                .map(|chunk| {
                    let ts =
                        TimestampNanosecondArray::from_iter_values(chunk.iter().map(|row| row.0));
                    let v = Float64Array::from_iter(chunk.iter().map(|row| row.1));
                    RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(ts), Arc::new(v)])
                        .expect("batch builds")
                })
                .collect()
        })
        .collect()
}

const FLOAT_MINUTES: i64 = 2_000;

/// The rows of minute `minute`, by class `minute mod 8`; their average is
/// [`minute_class_average`]. Each class holds at most one NaN payload, never both
/// infinities, and only small dyadic finite values, so every fold order the
/// partitions can produce gives the same bits.
fn minute_class_rows(minute: i64) -> Vec<Option<f64>> {
    let nan_a = f64::from_bits(NAN_A);
    let nan_b = f64::from_bits(NAN_B);
    let subnormal = f64::from_bits(SUBNORMAL);
    match minute % 8 {
        0 => vec![Some(-0.0), Some(-0.0)],
        1 => vec![Some(nan_a), Some(1.0), Some(2.0)],
        2 => vec![Some(nan_b), Some(-3.0)],
        3 => vec![Some(-0.0), Some(0.0), Some(1.5), Some(-0.5)],
        4 => vec![Some(f64::INFINITY), Some(5.0)],
        5 => vec![Some(f64::NEG_INFINITY), Some(-0.0)],
        6 => vec![Some(subnormal), Some(subnormal), None],
        _ => vec![None, None],
    }
}

/// The bits of the average of [`minute_class_rows`] for `minute`; NULL as
/// `None`.
fn minute_class_average(minute: i64) -> Option<u64> {
    match minute % 8 {
        0 => Some((-0.0f64).to_bits()),
        1 => Some(NAN_A),
        2 => Some(NAN_B),
        3 => Some(0.25f64.to_bits()),
        4 => Some(f64::INFINITY.to_bits()),
        5 => Some(f64::NEG_INFINITY.to_bits()),
        6 => Some(SUBNORMAL),
        _ => None,
    }
}

/// Every `(minute, average bits)` row of `batches`, in order.
fn minute_averages(batches: &[RecordBatch]) -> Vec<(i64, Option<u64>)> {
    let mut out = Vec::new();
    for batch in batches {
        let minutes = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("a nanosecond timestamp");
        out.extend(
            minutes
                .values()
                .iter()
                .map(|ts| ts / MINUTE_NS)
                .zip(float_bits(batch.column(1))),
        );
    }
    out
}

/// `avg(v)` per minute bucket, newest 60 first, over [`float_minute_parts`]:
/// the admitted float state goes through the exec's rebuilds and every
/// average equals the rule-off run's bit for bit, `-0.0`, both NaN payloads,
/// both infinities and the subnormal included. Minutes 1,940 to 1,999 come
/// back, each with its class's average. Batches of 64 rows make each final
/// partition take its partial states over many batches, so a kept group's
/// rebuilt state is merged with partial states that arrive after the rebuild.
///
/// FLIP (merge a `(0.0, 0)` state into each fresh accumulator before the kept
/// states in `rebuild`): the `-0.0` minutes come back as `+0.0`.
#[tokio::test]
async fn an_admitted_float_aggregate_is_exact_through_rebuilds() {
    let sql = "SELECT date_trunc('minute', ts) AS m, avg(v) FROM t \
               GROUP BY m ORDER BY m DESC LIMIT 60";
    let options = [
        ("datafusion.execution.target_partitions", "8"),
        ("datafusion.execution.batch_size", "64"),
    ];
    let (on, off) = on_and_off(float_minute_parts, &options, sql).await;
    assert_eq!(on.bounds, vec![60], "{}", on.plan);
    assert!(on.rebuilds > 0, "rebuilds {}\n{}", on.rebuilds, on.plan);
    let got = minute_averages(on.result.as_ref().expect("query runs"));
    let want: Vec<(i64, Option<u64>)> = (FLOAT_MINUTES - 60..FLOAT_MINUTES)
        .rev()
        .map(|minute| (minute, minute_class_average(minute)))
        .collect();
    assert_eq!(got, want);
    assert_eq!(
        minute_averages(off.result.as_ref().expect("query runs")),
        want,
        "rule off"
    );
}

/// The largest key of [`order_sensitive_batches`].
const ORDER_SENSITIVE_KEYS: i64 = 200;

/// The 8-row batches of [`order_sensitive_batches`] wholly before the second
/// rows of keys 0 and 1, which follow 2 + 199 rows.
const BEFORE_SECOND_ROWS: usize = (2 + ORDER_SENSITIVE_KEYS as usize - 1) / 8;

/// `(k, f)` in one fixed arrival order, in batches of 8 rows. Key 0 holds
/// `1e16`, `1.0` and `-1e16`, whose sum is `+0.0` folded in that order and
/// `1.0` folded as `(1e16 + -1e16) + 1.0`. Key 1 holds three `-0.0` rows.
/// Keys 2 to 200 hold `k + 0.25` twice. The rows arrive as: the first rows
/// of keys 0 and 1; keys 200 down to 2, the adversarial order for
/// `ORDER BY k`; the second rows of keys 0 and 1; keys 200 down to 2 again;
/// the last rows of keys 0 and 1.
fn order_sensitive_batches() -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("f", DataType::Float64, false),
    ]));
    let filler = || {
        (2..=ORDER_SENSITIVE_KEYS)
            .rev()
            .map(|k| (k, k as f64 + 0.25))
    };
    let mut rows: Vec<(i64, f64)> = vec![(0, 1e16), (1, -0.0)];
    rows.extend(filler());
    rows.extend([(0, 1.0), (1, -0.0)]);
    rows.extend(filler());
    rows.extend([(0, -1e16), (1, -0.0)]);
    rows.chunks(8)
        .map(|chunk| {
            let k = Int64Array::from_iter_values(chunk.iter().map(|row| row.0));
            let f = Float64Array::from_iter_values(chunk.iter().map(|row| row.1));
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(k), Arc::new(f)])
                .expect("batch builds")
        })
        .collect()
}

/// Every `(k, avg bits, count)` row of `batches`, in order.
fn key_averages(batches: &[RecordBatch]) -> Vec<(i64, Option<u64>, i64)> {
    let mut out = Vec::new();
    for batch in batches {
        let keys = int_values(batch.column(0));
        let averages = float_bits(batch.column(1));
        let counts = int_values(batch.column(2));
        out.extend(
            keys.into_iter()
                .zip(averages)
                .zip(counts)
                .map(|((k, average), count)| {
                    (k.expect("k is not null"), average, count.expect("a count"))
                }),
        );
    }
    out
}

/// The summed value of the metric `name` over every `AggregateExec` in `plan`.
fn aggregate_metric(plan: &Arc<dyn ExecutionPlan>, name: &str) -> usize {
    let mut total = 0;
    for_each_aggregate(plan, &mut |aggregate| {
        total += aggregate
            .metrics()
            .and_then(|metrics| metrics.sum_by_name(name))
            .map_or(0, |value| value.as_usize());
    });
    total
}

/// `avg(f)` over a float column in the plan shapes production gives it. A
/// float `avg` is not exact-typed (ADR-0094), so its final aggregate is never
/// `FinalPartitioned`: at the session's own partition count it is `Final`
/// over a `CoalescePartitionsExec`, where the exact-typed session would plan
/// `FinalPartitioned`. Both runs then use the not-exact-typed session on one
/// target partition with 8-row batches over [`order_sensitive_batches`]:
///
/// - `Final`: the batches in one partition beside an empty one, so the
///   partial stage runs on two partitions under a `CoalescePartitionsExec`
///   whose only producer is the non-empty one, and the final stage sees the
///   partial output in arrival order. The partial stage passes rows on after
///   its first batch, so the later rows of keys 0 and 1 reach the exec as
///   their own states and are merged after a rebuild.
/// - `Single`: the batches in one partition, so the exec reads them directly
///   and the later rows of keys 0 and 1 go through `update_batch` after a
///   rebuild.
///
/// Keys 0 and 1 are held across a rebuild and kept, in both modes, by
/// construction, not by chance: their first rows are the exec's first two
/// keys, a rebuild runs only at the end of a batch once 20 groups are held,
/// so both are held at the first rebuild, and as the two smallest keys under
/// `ORDER BY k` with `n = 10` they rank first at every rebuild and are never
/// evicted. Their second rows follow 199 rows of distinct keys, so at least
/// one rebuild has run before them; a run over only the batches before them
/// pins that. Every returned average, count included,
/// equals the rule-off run of the same plan shape bit for bit: key 0's is the
/// in-order fold `(1e16 + 1.0) + -1e16` over 3, key 1's is `-0.0`.
///
/// FLIP (merge a `(0.0, 0)` state into each fresh accumulator before the kept
/// states in `rebuild`): key 1's average comes back `+0.0` in both modes.
#[tokio::test]
async fn a_float_avg_is_exact_through_rebuilds_in_the_final_and_single_modes() {
    let sql = "SELECT k, avg(f), count(*) FROM t GROUP BY k ORDER BY k LIMIT 10";
    let config = SqlConfig::default();
    for (exact_typed, mode) in [
        (false, AggregateMode::Final),
        (true, AggregateMode::FinalPartitioned),
    ] {
        let (pool, _breach) =
            config.query_pool(TenantMemoryAccountant::new(1 << 30), QueryAccounting::new());
        let ctx = session_typed(
            &config,
            pool,
            vec![order_sensitive_batches(), order_sensitive_batches()],
            &[],
            exact_typed,
        );
        let plan = plan_of(&ctx, sql).await;
        assert_eq!(top_keys_bounds(&plan), vec![10], "{}", render(&plan));
        assert_eq!(top_keys_modes(&plan), vec![mode], "{}", render(&plan));
    }

    let folded = (1e16f64 + 1.0) + -1e16;
    assert_ne!(folded.to_bits(), ((1e16f64 + -1e16) + 1.0).to_bits());
    let want: Vec<(i64, Option<u64>, i64)> = (0..10)
        .map(|k| match k {
            0 => (0, Some((folded / 3.0).to_bits()), 3),
            1 => (1, Some((-0.0f64).to_bits()), 3),
            k => (k, Some((k as f64 + 0.25).to_bits()), 2),
        })
        .collect();
    let batch = [("datafusion.execution.batch_size", "8")];
    let pass_rows = [
        (
            "datafusion.execution.skip_partial_aggregation_probe_rows_threshold",
            "1",
        ),
        (
            "datafusion.execution.skip_partial_aggregation_probe_ratio_threshold",
            "0.0",
        ),
    ];
    for (mode, parts, options) in [
        (
            AggregateMode::Final,
            vec![order_sensitive_batches(), Vec::new()],
            [ONE_PARTITION, &batch, &pass_rows].concat(),
        ),
        (
            AggregateMode::Single,
            vec![order_sensitive_batches()],
            [ONE_PARTITION, &batch].concat(),
        ),
    ] {
        let prefix: Vec<Vec<RecordBatch>> = parts
            .iter()
            .map(|part| part.iter().take(BEFORE_SECOND_ROWS).cloned().collect())
            .collect();
        let on = probed_typed(&config, parts.clone(), &options, sql, false).await;
        let off = probed_typed(&rule_off(&config), parts, &options, sql, false).await;
        assert_eq!(on.bounds, vec![10], "{mode:?}\n{}", on.plan);
        assert_eq!(on.modes, vec![mode], "{}", on.plan);
        assert!(off.bounds.is_empty(), "{mode:?}: rule off\n{}", off.plan);
        assert!(
            off.plan
                .contains(&format!("AggregateExec: mode={mode:?}, ")),
            "{}",
            off.plan
        );
        assert_eq!(
            on.plan.contains("CoalescePartitionsExec"),
            mode == AggregateMode::Final,
            "{}",
            on.plan
        );
        assert_eq!(on.rebuilds, 12, "{mode:?}");
        assert_eq!(on.evictions, 191, "{mode:?}: 201 keys less the 10 kept");
        assert!(on.dropped > 0, "{mode:?}: no row was dropped");
        let skipped = if mode == AggregateMode::Final { 396 } else { 0 };
        assert_eq!(
            on.skipped_aggregation_rows, skipped,
            "{mode:?}: every row after the first batch passes as its own state"
        );
        let got = key_averages(on.result.as_ref().expect("query runs"));
        assert_eq!(got, want, "{mode:?}");

        // The batches before the one holding the second rows of keys 0 and 1
        // already rebuild, and hold exactly the first row of each.
        let before = probed_typed(&config, prefix, &options, sql, false).await;
        assert!(
            before.rebuilds > 0,
            "{mode:?}: no rebuild before the second rows"
        );
        let early = key_averages(before.result.as_ref().expect("query runs"));
        assert_eq!(
            early[..2],
            [
                (0, Some(1e16f64.to_bits()), 1),
                (1, Some((-0.0f64).to_bits()), 1)
            ],
            "{mode:?}"
        );
        assert_eq!(
            key_averages(off.result.as_ref().expect("query runs")),
            want,
            "{mode:?}: rule off"
        );
    }
}

/// `(f, k)` in one partition: a `+0.0` row and a `-5.0` row, then two `-0.0`
/// rows, then `-7.0` and `-8.0`, in batches of two. `-0.0` and `+0.0` are one
/// group to the aggregate, which stores the key as `+0.0`, and two sort
/// values to arrow-row, where `-0.0` sorts after `+0.0` in descending order.
fn signed_zero_parts() -> Vec<Vec<RecordBatch>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("f", DataType::Float64, false),
        Field::new("k", DataType::Int64, false),
    ]));
    let rows = [
        (0.0, 1),
        (-5.0, 2),
        (-0.0, 3),
        (-0.0, 4),
        (-7.0, 5),
        (-8.0, 6),
    ];
    vec![
        rows.chunks(2)
            .map(|chunk| {
                let f = Float64Array::from_iter_values(chunk.iter().map(|row| row.0));
                let k = Int64Array::from_iter_values(chunk.iter().map(|row| row.1));
                RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(f), Arc::new(k)])
                    .expect("batch builds")
            })
            .collect(),
    ]
}

/// A float sort expression, or a non-float one that reads a float key, is
/// refused. Over [`signed_zero_parts`] the zero group holds three rows and
/// is the largest key.
///
/// FLIP (admit float keys): the first rebuild keeps the zero group, stored
/// as `+0.0`, and sets the threshold there; the `-0.0` rows sort after it
/// under `DESC` and are dropped, and the group comes back with one row.
#[tokio::test]
async fn float_sort_keys_leave_the_plan_unchanged() {
    let sql = "SELECT f, count(*) FROM t GROUP BY f ORDER BY f DESC LIMIT 1";
    let options = [ONE_PARTITION, TIGHT].concat();
    let (on, _) = on_and_off(signed_zero_parts, &options, sql).await;
    assert_eq!(
        on.rows(),
        vec![vec!["0.0".to_string(), "3".to_string()]],
        "the zero group holds every zero row"
    );
    for sql in [
        "SELECT f, count(*) FROM t GROUP BY f ORDER BY f DESC LIMIT 1",
        "SELECT f, count(*) FROM t GROUP BY f ORDER BY f LIMIT 1",
        "SELECT f, count(*) FROM t GROUP BY f ORDER BY CAST(f AS BIGINT) LIMIT 1",
        "SELECT k, count(*) FROM t GROUP BY k ORDER BY CAST(k AS DOUBLE) LIMIT 1",
    ] {
        let (on, off) = on_and_off(signed_zero_parts, ONE_PARTITION, sql).await;
        assert!(on.bounds.is_empty(), "{sql}\n{}", on.plan);
        assert_eq!(on.rows(), off.rows(), "{sql}");
    }
}

const HOURS: i64 = 100;
const MINUTE_NS: i64 = 60_000_000_000;

/// `(m, v)`, `m` a nanosecond timestamp on a whole minute: every minute of
/// [`HOURS`] hours, three rows each over four partitions, each partition in
/// descending order.
fn minute_parts() -> Vec<Vec<RecordBatch>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("m", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("v", DataType::Int64, false),
    ]));
    let mut rows: Vec<Vec<i64>> = vec![Vec::new(); 4];
    for minute in 0..HOURS * 60 {
        for j in 0..3 {
            rows[((minute + j) % 4) as usize].push(minute);
        }
    }
    rows.into_iter()
        .map(|mut part| {
            part.reverse();
            part.chunks(1024)
                .map(|chunk| {
                    let m = TimestampNanosecondArray::from_iter_values(
                        chunk.iter().map(|minute| minute * MINUTE_NS),
                    );
                    let v = Int64Array::from_iter_values(chunk.iter().copied());
                    RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(m), Arc::new(v)])
                        .expect("batch builds")
                })
                .collect()
        })
        .collect()
}

/// `ORDER BY date_trunc('hour', m)` over minute keys: 60 keys tie on each
/// hour, so `LIMIT 120` returns exactly the minutes of the first two hours,
/// in some order within each hour. The multiset equals the rule-off run.
#[tokio::test]
async fn a_sort_over_a_function_of_the_key_is_admitted() {
    let sql = "SELECT date_trunc('hour', m) AS h, m, count(*), sum(v) FROM t \
               GROUP BY m ORDER BY date_trunc('hour', m) LIMIT 120";
    let (on, off) = on_and_off(minute_parts, &[], sql).await;
    assert_eq!(on.bounds, vec![120], "{}", on.plan);
    let on_rows = on.rows();
    assert_eq!(sorted(on_rows.clone()), sorted(off.rows()));
    let minutes: BTreeSet<String> = on_rows.iter().map(|row| row[1].clone()).collect();
    assert_eq!(minutes.len(), 120);
    for row in &on_rows {
        assert_eq!(row[2], "3", "count of minute {}", row[1]);
    }
    let hours: Vec<String> = on_rows.iter().map(|row| row[0].clone()).collect();
    let mut want = vec!["1970-01-01T00:00:00".to_string(); 60];
    want.extend(vec!["1970-01-01T01:00:00".to_string(); 60]);
    assert_eq!(hours, want);
}

/// `(k, k2)` over four partitions: `k2 = j` for `j` in 0..200 holds
/// `200 - j` rows, so ordering by the count reverses the order of the key;
/// `k` is the row number, and row `k` is in partition `k mod 4`.
fn renamed_parts() -> Vec<Vec<RecordBatch>> {
    let schema = int64_schema(&["k", "k2"], false);
    let mut rows: Vec<Vec<Vec<Option<i64>>>> = vec![Vec::new(); 4];
    let mut k = 0;
    for j in 0..200 {
        for _ in 0..200 - j {
            rows[k as usize % 4].push(vec![Some(k), Some(j)]);
            k += 1;
        }
    }
    rows.iter()
        .map(|part| int64_batches(&schema, part, 1024))
        .collect()
}

/// A sort on an aggregate, or on a column that carries a key's name and
/// holds an aggregate, does not resolve to a group key. On the default
/// session the renaming projection sits between the sort and the final
/// aggregate, under the `CoalescePartitionsExec`, so the walk reads the sort
/// through it.
///
/// FLIP (the projection step resolves a column by its name in the
/// projection's input): `count(*) AS k2 ... ORDER BY k2` resolves to the key
/// `k2`, each final partition keeps its ten smallest keys, which hold the
/// largest counts, and the ten smallest counts are missing from the answer.
#[tokio::test]
async fn a_sort_over_an_aggregate_or_a_renamed_non_key_leaves_the_plan_unchanged() {
    let want: Vec<Vec<String>> = (190..200)
        .rev()
        .map(|j| vec![(200 - j).to_string(), j.to_string()])
        .collect();
    for options in [&[][..], ONE_PARTITION] {
        let sql = "SELECT count(*) AS k2, k2 AS c FROM t GROUP BY k2 ORDER BY k2 LIMIT 10";
        let (on, _) = on_and_off(renamed_parts, options, sql).await;
        assert_eq!(on.rows(), want, "{options:?}\n{}", on.plan);
    }
    for sql in [
        "SELECT k2, count(*) AS c FROM t GROUP BY k2 ORDER BY c LIMIT 10",
        "SELECT k2, sum(k) FROM t GROUP BY k2 ORDER BY sum(k) DESC LIMIT 10",
        "SELECT count(*) AS k2, k2 AS c FROM t GROUP BY k2 ORDER BY k2 LIMIT 10",
        "SELECT count(*) AS k, k2 AS c FROM t GROUP BY k2 ORDER BY k LIMIT 10",
    ] {
        for options in [&[][..], ONE_PARTITION] {
            let (on, off) = on_and_off(renamed_parts, options, sql).await;
            assert!(on.bounds.is_empty(), "{sql}\n{}", on.plan);
            assert_eq!(on.rows(), off.rows(), "{sql}");
        }
    }
    let sql = "SELECT count(*) AS k2, k2 AS c FROM t GROUP BY k2 ORDER BY k2 LIMIT 10";
    let (pool, _breach) = SqlConfig::default()
        .query_pool(TenantMemoryAccountant::new(1 << 30), QueryAccounting::new());
    let ctx = session(&SqlConfig::default(), pool, renamed_parts(), &[]);
    let plan = render(&plan_of(&ctx, sql).await);
    let projection = plan.find("ProjectionExec").expect("a projection");
    let aggregate = plan.find("AggregateExec").expect("an aggregate");
    let sort = plan.find("SortExec").expect("a sort");
    assert!(sort < projection && projection < aggregate, "{plan}");
}

/// Every key of [`keyed_parts`] arriving in the adversarial order for the
/// sort, and in a fixed shuffle, at 1,000,000 keys over 8 partitions,
/// `LIMIT 10`, in both directions. With the rule each final partition holds
/// at most 20 groups plus one batch's keys, and its `TopKeys[` reservation
/// stays under [`BYTES_PER_HELD_GROUP`] times that many groups; the charged
/// peak that remains is the hash exchange and the partial aggregates. With
/// the rule off the final stage holds every group.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn the_exec_bounds_the_final_stages_charged_peak() {
    const KEYS: i64 = 1_000_000;
    const N: usize = 10;
    // DataFusion's default, which the session keeps.
    const BATCH: usize = 8192;
    // About twice the 110 to 125 bytes per group the exec's peak measures
    // here: key, count and sum, hash table, and the mask and sort-row scratch.
    // Only this single `Int64` key is measured and asserted; a wider key
    // needs its own figure, not this constant.
    const BYTES_PER_HELD_GROUP: usize = 256;
    let exec_bound = (2 * N + BATCH) * BYTES_PER_HELD_GROUP;
    for (direction, adversarial, want) in [
        ("ASC", Arrival::Descending, keyed_rows(0..10, 1)),
        (
            "DESC",
            Arrival::Ascending,
            keyed_rows((KEYS - 10..KEYS).rev(), 1),
        ),
    ] {
        let sql =
            format!("SELECT k, count(*), sum(v) FROM t GROUP BY k ORDER BY k {direction} LIMIT 10");
        for arrival in [adversarial, Arrival::Shuffled(4004)] {
            let (on, off) = on_and_off(|| keyed_parts(KEYS, 8, 1, arrival), &[], &sql).await;
            eprintln!(
                "top_keys peaks, {direction} {arrival:?}: on charged={} envelope={} \
                 exec={} over {} consumers; off charged={} envelope={}",
                on.charged,
                on.envelope,
                on.top_keys_peak,
                on.top_keys_consumers,
                off.charged,
                off.envelope
            );
            assert_eq!(on.bounds, vec![10], "{}", on.plan);
            assert!(on.rebuilds > 0, "{direction} {arrival:?}: no rebuild ran");
            assert_eq!(on.rows(), want, "{direction} {arrival:?}");
            assert_eq!(off.rows(), want, "{direction} {arrival:?}: rule off");
            assert!(on.top_keys_consumers > 0, "{direction} {arrival:?}");
            assert!(
                on.top_keys_peak < exec_bound,
                "{direction} {arrival:?}: the exec's own peak {} against {exec_bound}",
                on.top_keys_peak
            );
            // The hash exchange's buffers vary with scheduling, so the
            // query-wide peak is bounded at about twice the largest measured
            // (8.9 MB); the exec's own peak above is the bounded figure.
            assert!(
                on.charged < 32 * MIB as u64,
                "{direction} {arrival:?}: charged peak {} with the rule",
                on.charged
            );
            // The partial aggregates' envelope, which the rule leaves as it
            // is: 4.7 to 6.2 MB measured.
            assert!(
                on.envelope < 16 * MIB as i64,
                "{direction} {arrival:?}: envelope {} with the rule",
                on.envelope
            );
            assert!(
                off.charged > 32 * MIB as u64,
                "{direction} {arrival:?}: charged peak {} with the rule off",
                off.charged
            );
            assert!(
                off.envelope > 32 * MIB as i64,
                "{direction} {arrival:?}: envelope {} with the rule off",
                off.envelope
            );
        }
    }
}

/// An ORDER BY on an aggregate stays with `BoundedTopKAggregate`, which sets
/// the aggregate's own limit; the top-keys rule does not touch it.
#[tokio::test]
async fn bounded_topk_wins_when_the_order_is_an_aggregate() {
    let sql = "SELECT k, min(v) FROM t GROUP BY k ORDER BY min(v) LIMIT 10";
    let parts = || keyed_parts(1_000, 4, 2, Arrival::Descending);
    let (on, off) = on_and_off(parts, &[], sql).await;
    assert!(on.bounds.is_empty(), "{}", on.plan);
    let config = SqlConfig::default();
    let (pool, _breach) =
        config.query_pool(TenantMemoryAccountant::new(1 << 30), QueryAccounting::new());
    let plan = plan_of(&session(&config, pool, parts(), &[]), sql).await;
    let limited = count_nodes(&plan, &|node| {
        node.downcast_ref::<AggregateExec>()
            .is_some_and(|aggregate| aggregate.limit_options().is_some())
    });
    assert!(limited > 0, "{}", render(&plan));
    let want: Vec<Vec<String>> = (0..10)
        .map(|k| vec![k.to_string(), k.to_string()])
        .collect();
    assert_eq!(on.rows(), want);
    assert_eq!(off.rows(), want);
}

/// The ordered shape gets the top-keys exec and no first-groups filter; the
/// unordered one the first-groups filter and no top-keys exec.
#[tokio::test]
async fn first_groups_limit_stays_out_of_ordered_plans() {
    let config = SqlConfig::default();
    for (sql, top_keys, first_groups) in [
        (
            "SELECT k, count(*) FROM t GROUP BY k ORDER BY k LIMIT 10",
            1,
            0,
        ),
        ("SELECT k, count(*) FROM t GROUP BY k LIMIT 10", 0, 1),
    ] {
        let (pool, _breach) =
            config.query_pool(TenantMemoryAccountant::new(1 << 30), QueryAccounting::new());
        let ctx = session(
            &config,
            pool,
            keyed_parts(1_000, 4, 2, Arrival::Descending),
            &[],
        );
        let plan = plan_of(&ctx, sql).await;
        let found = (
            count_nodes(&plan, &|node| node.is::<TopKeysAggregateExec>()),
            count_nodes(&plan, &|node| node.is::<FirstGroupsExec>()),
        );
        assert_eq!(found, (top_keys, first_groups), "{sql}\n{}", render(&plan));
    }
}

/// With bucketed final aggregation configured, the aggregate the top-keys
/// rule replaced is gone by the time the bucketed rule runs, so the bucketed
/// rule leaves the exec as it is; with the top-keys rule off the bucketed
/// rule takes the same aggregate. Both answer alike.
#[tokio::test]
async fn the_bucketed_rule_leaves_the_exec_alone() {
    let sql = "SELECT k, count(*), sum(v) FROM t GROUP BY k ORDER BY k LIMIT 10";
    let parts = || keyed_parts(10_000, 4, 2, Arrival::Descending);
    let config = SqlConfig {
        bucketed_final_aggregation: Some(BucketedFinalAggregation {
            engage_groups: 16,
            ..BucketedFinalAggregation::default()
        }),
        // Each bucketed final stream holds about 17 MB, so 16 of them exhaust
        // the default 256 MiB pool with the top-keys rule off.
        first_attempt_min_partitions: 0,
        ..SqlConfig::default()
    };
    let mut answers = Vec::new();
    for (config, top_keys, bucketed) in [(config.clone(), 1, 0), (rule_off(&config), 0, 1)] {
        let (pool, _breach) =
            config.query_pool(TenantMemoryAccountant::new(1 << 30), QueryAccounting::new());
        let ctx = session(&config, pool, parts(), &[]);
        let plan = plan_of(&ctx, sql).await;
        let found = (
            count_nodes(&plan, &|node| node.is::<TopKeysAggregateExec>()),
            count_nodes(&plan, &|node| node.is::<BucketedFinalAggregateExec>()),
        );
        assert_eq!(found, (top_keys, bucketed), "{}", render(&plan));
        let run = probed(&config, parts(), &[], sql).await;
        answers.push(run.rows());
    }
    assert_eq!(answers[0], keyed_rows(0..10, 2));
    assert_eq!(answers[1], answers[0]);
}

const A_COL: &str = "A";
const B_COL: &str = "B";

fn tie_order_tenant() -> TenantId {
    TenantId::new("top-keys-4004".to_string())
}

/// 2013-07-14 00:00 UTC, epoch day 15,900, in nanoseconds.
const DAY_15900_NS: i64 = 15_900 * 86_400_000_000_000;

fn log_record(ts_ns: i64, attrs: Vec<(String, AttrValue)>) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns,
        observed_ts_ns: ts_ns,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: "req".into(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs,
    }
}

/// Write one RLOG object holding `records` as writer sequence `seq` and
/// publish its commit record.
async fn publish_object(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    seq: u64,
    records: &[LogRecord],
) {
    let identity = ObjectIdentity {
        tenant_hash: tenant.hash().0,
        shard: 0,
        writer_id: [6u8; 16],
        writer_epoch: 1,
        writer_seq: seq,
    };
    let mut writer = RlogWriter::new(RlogConfig::default(), identity);
    for r in records {
        writer.push(r.clone()).expect("push record");
    }
    let bytes = writer.finish().expect("finish rlog");
    let min = records.iter().map(|r| r.ts_ns).min().expect("nonempty");
    let max = records.iter().map(|r| r.ts_ns).max().expect("nonempty");
    let rec = record::build(NewCommitRecord {
        tenant_hash: tenant.hash(),
        signal: Signal::Logs,
        shard: 0,
        writer_id: Uuid::from_u128(4004),
        writer_epoch: 1,
        writer_seq: seq,
        object_size: bytes.len() as u64,
        content_hash: [seq as u8; 32],
        sample_count: records.len() as u64,
        series_count: 1,
        min_event_ts_ns: min,
        max_event_ts_ns: max,
        min_ingest_ts_ns: min,
        max_ingest_ts_ns: max,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        created_unix_ns: 10,
        ingest_hour_bucket: 0,
    })
    .expect("valid logs commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("logs data key");
    store
        .put(&data_key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put rlog object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish logs commit record");
}

fn logs_request(sql: &str) -> SqlRequest {
    SqlRequest {
        sql: sql.to_string(),
        window: TimeRange {
            start_ns: 0,
            end_ns: i64::MAX,
        },
        min_tokens: Vec::new(),
        now_ns: DAY_15900_NS + 10 * 86_400_000_000_000,
        deadline: Duration::from_secs(120),
        row_window: false,
        max_rows: None,
        budgets: None,
    }
}

fn executor(
    store: &Arc<dyn ObjectStoreBackend>,
    config: SqlConfig,
    declared: Vec<DeclaredColumn>,
) -> SqlExecutor {
    let catalog =
        Arc::new(Catalog::new(Arc::clone(store), CatalogConfig::default()).expect("catalog"));
    SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(store)),
        LogSegmentFetcher::new(Arc::clone(store)),
        SpanSegmentFetcher::new(Arc::clone(store)),
        config,
        1 << 30,
    )
    .with_declared_column_source(Arc::new(StaticDeclaredColumns::new(declared)))
}

/// The physical plan `executor` builds for `sql` through `plan_pinned`, and
/// the statement's output.
async fn plan_and_run(
    executor: &SqlExecutor,
    tenant: &TenantId,
    sql: &str,
) -> (Arc<dyn ExecutionPlan>, Vec<Vec<String>>) {
    let request = logs_request(sql);
    let accounting = QueryAccounting::new();
    let declared = executor
        .resolve_declared_columns(tenant.hash(), request.now_ns)
        .await;
    let (snapshot, _) = executor
        .resolve_snapshot(tenant.hash(), &request, &accounting)
        .await
        .expect("snapshot resolves");
    let plan = executor
        .plan_pinned(tenant.hash(), snapshot, sql, &accounting, &declared)
        .await
        .expect("query plans")
        .create_physical_plan()
        .await
        .expect("physical plan builds");
    let outcome = executor
        .execute(tenant.hash(), &request)
        .await
        .expect("query runs");
    (plan, cells(outcome.output.batches()))
}

/// Through the executor, which makes a sort over an aggregate total on its
/// keys: `ORDER BY "A"` over `GROUP BY "A", "B"` gains the term `"B"`
/// (the two-term shape), and when the select list drops `"B"` the key is
/// projected through under a `__ravel_tiebreak_` name. The rule replaces the
/// final aggregate in both, and the answer equals the rule-off run row for
/// row.
#[tokio::test]
async fn the_tie_order_rewrites_two_term_and_project_through_shapes_are_admitted() {
    let tenant = tie_order_tenant();
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let mut reference: BTreeMap<(i64, i64), i64> = BTreeMap::new();
    for object in 0..2i64 {
        let records: Vec<LogRecord> = (0..10_000i64)
            .rev()
            .map(|i| {
                let (a, b) = (i % 50, (i * 7 + object) % 37);
                *reference.entry((a, b)).or_default() += 1;
                log_record(
                    DAY_15900_NS + object * 1_000_000 + i,
                    vec![
                        (A_COL.to_string(), AttrValue::I64(a)),
                        (B_COL.to_string(), AttrValue::I64(b)),
                    ],
                )
            })
            .collect();
        publish_object(store.as_ref(), &tenant, object as u64 + 1, &records).await;
    }
    let declared = vec![
        DeclaredColumn::new(A_COL, DeclaredType::I64),
        DeclaredColumn::new(B_COL, DeclaredType::I64),
    ];
    let on = executor(&store, SqlConfig::default(), declared.clone());
    let off = executor(&store, rule_off(&SqlConfig::default()), declared);
    let first: Vec<((i64, i64), i64)> = reference.iter().take(10).map(|(k, v)| (*k, *v)).collect();

    for (sql, marker, want) in [
        (
            "SELECT \"A\", \"B\", count(*) FROM logs GROUP BY \"A\", \"B\" ORDER BY \"A\" LIMIT 10",
            "B@1 ASC NULLS LAST",
            first
                .iter()
                .map(|((a, b), c)| vec![a.to_string(), b.to_string(), c.to_string()])
                .collect::<Vec<_>>(),
        ),
        (
            "SELECT \"A\", count(*) FROM logs GROUP BY \"A\", \"B\" ORDER BY \"A\" LIMIT 10",
            "__ravel_tiebreak_",
            first
                .iter()
                .map(|((a, _), c)| vec![a.to_string(), c.to_string()])
                .collect::<Vec<_>>(),
        ),
    ] {
        let (plan, rows) = plan_and_run(&on, &tenant, sql).await;
        let text = render(&plan);
        assert_eq!(top_keys_bounds(&plan), vec![10], "{sql}\n{text}");
        assert!(text.contains(marker), "{sql}\n{text}");
        let (plan_off, rows_off) = plan_and_run(&off, &tenant, sql).await;
        assert!(
            top_keys_bounds(&plan_off).is_empty(),
            "{sql}\n{}",
            render(&plan_off)
        );
        assert_eq!(rows, want, "{sql}");
        assert_eq!(rows_off, want, "rule off: {sql}");
    }
}

/// ClickBench row 43's statement over the `logs` table, through the
/// executor: two objects, 40,000 rows over 1,500 minutes, each minute
/// visited once per pass of 1,500 rows. Some passes carry another counter, a
/// refresh or a third day, which the `WHERE` drops. The page is minutes
/// 1,000 to 1,009 with their exact counts, as with the rule off. The rule-off
/// plan has a `CompactSlicesExec` between the sort and the final aggregate;
/// with the rule on, none stands between the sort and the exec.
#[tokio::test]
async fn clickbench_row_43_returns_exact_counts_through_the_logs_table() {
    let tenant = TenantId::new("top-keys-4004-q43".to_string());
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let mut reference: BTreeMap<i64, i64> = BTreeMap::new();
    for object in 0..2i64 {
        let records: Vec<LogRecord> = (0..20_000i64)
            .rev()
            .map(|i| {
                let minute = (i * 7 + object) % 1_500;
                let pass = i / 1_500 + object;
                let counter = if pass % 5 == 4 { 61 } else { 62 };
                let day = 15_900 + pass % 3;
                let refresh = i64::from(pass % 7 == 6);
                if counter == 62 && day <= 15_901 && refresh == 0 {
                    *reference.entry(minute).or_default() += 1;
                }
                log_record(
                    DAY_15900_NS + minute * MINUTE_NS + (i % 60) * 1_000_000_000,
                    vec![
                        ("CounterID".to_string(), AttrValue::I64(counter)),
                        ("EventDate".to_string(), AttrValue::I64(day)),
                        ("IsRefresh".to_string(), AttrValue::I64(refresh)),
                        ("DontCountHits".to_string(), AttrValue::I64(0)),
                    ],
                )
            })
            .collect();
        publish_object(store.as_ref(), &tenant, object as u64 + 1, &records).await;
    }
    assert_eq!(reference.len(), 1_500, "every minute keeps a row");
    let declared: Vec<DeclaredColumn> = ["CounterID", "EventDate", "IsRefresh", "DontCountHits"]
        .into_iter()
        .map(|key| DeclaredColumn::new(key, DeclaredType::I64))
        .collect();
    let on = executor(&store, SqlConfig::default(), declared.clone());
    let off = executor(&store, rule_off(&SqlConfig::default()), declared);

    let sql = "SELECT DATE_TRUNC('minute', ts) AS M, COUNT(*) AS PageViews FROM logs \
               WHERE \"CounterID\" = 62 AND \"EventDate\" >= 15900 AND \"EventDate\" <= 15901 \
               AND \"IsRefresh\" = 0 AND \"DontCountHits\" = 0 \
               GROUP BY DATE_TRUNC('minute', ts) ORDER BY DATE_TRUNC('minute', ts) \
               LIMIT 10 OFFSET 1000";
    let (plan, rows) = plan_and_run(&on, &tenant, sql).await;
    assert_eq!(top_keys_bounds(&plan), vec![1010], "{}", render(&plan));
    let text = render(&plan);
    let sort = text.find("SortExec").expect("a sort");
    let exec = text.find("TopKeysAggregateExec").expect("the exec");
    assert!(
        sort < exec && !text[sort..exec].contains("CompactSlicesExec"),
        "{text}"
    );
    let (plan_off, rows_off) = plan_and_run(&off, &tenant, sql).await;
    assert!(
        top_keys_bounds(&plan_off).is_empty(),
        "{}",
        render(&plan_off)
    );
    let text_off = render(&plan_off);
    let sort = text_off.find("SortExec").expect("a sort");
    let aggregate = text_off.find("AggregateExec").expect("the final aggregate");
    assert!(
        sort < aggregate && text_off[sort..aggregate].contains("CompactSlicesExec"),
        "{text_off}"
    );

    let minute_text = |minute: i64| {
        let ts = TimestampNanosecondArray::from(vec![DAY_15900_NS + minute * MINUTE_NS]);
        cells(&[RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "m",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            )])),
            vec![Arc::new(ts)],
        )
        .expect("batch builds")])[0][0]
            .clone()
    };
    let want: Vec<Vec<String>> = reference
        .iter()
        .skip(1000)
        .take(10)
        .map(|(minute, count)| vec![minute_text(*minute), count.to_string()])
        .collect();
    assert_eq!(want.len(), 10);
    assert_eq!(rows, want);
    assert_eq!(rows_off, want);
}

/// One proptest case: rows of `(g, k, v)`, `g` nullable, dealt round-robin
/// over `partitions` in batches of three.
#[derive(Clone, Debug)]
struct Case {
    rows: Vec<(Option<i64>, i64, i64)>,
    partitions: usize,
    one_final_partition: bool,
    n: usize,
    desc: bool,
    nulls_first: bool,
    full_order: bool,
}

fn case_parts(case: &Case) -> Vec<Vec<RecordBatch>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int64, true),
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let mut parts: Vec<Vec<Vec<Option<i64>>>> = vec![Vec::new(); case.partitions];
    for (i, (g, k, v)) in case.rows.iter().enumerate() {
        parts[i % case.partitions].push(vec![*g, Some(*k), Some(*v)]);
    }
    parts
        .iter()
        .map(|rows| int64_batches(&schema, rows, 3))
        .collect()
}

fn arb_case() -> impl Strategy<Value = Case> {
    (
        prop::collection::vec(
            (prop::option::weighted(0.85, 0i64..6), 0i64..12, -50i64..50),
            0..300,
        ),
        1usize..=5,
        any::<bool>(),
        1usize..=8,
        any::<bool>(),
        any::<bool>(),
        any::<bool>(),
    )
        .prop_map(
            |(rows, partitions, one_final_partition, n, desc, nulls_first, full_order)| Case {
                rows,
                partitions,
                one_final_partition,
                n,
                desc,
                nulls_first,
                full_order,
            },
        )
}

async fn check_case(case: Case) {
    let direction = if case.desc { "DESC" } else { "ASC" };
    let nulls = if case.nulls_first { "FIRST" } else { "LAST" };
    let order = if case.full_order {
        format!("g {direction} NULLS {nulls}, k {direction}")
    } else {
        format!("g {direction} NULLS {nulls}")
    };
    let sql = format!(
        "SELECT g, k, count(*), sum(v) FROM t GROUP BY g, k ORDER BY {order} LIMIT {}",
        case.n
    );
    let mut options = vec![("datafusion.execution.batch_size", "4")];
    if case.one_final_partition {
        options.extend_from_slice(ONE_PARTITION);
    }
    let parts = || case_parts(&case);
    let (on, off) = on_and_off(parts, &options, &sql).await;
    assert_eq!(on.bounds, vec![case.n], "{sql}\n{}", on.plan);
    let all = probed(
        &rule_off(&SqlConfig::default()),
        parts(),
        &options,
        "SELECT g, k, count(*), sum(v) FROM t GROUP BY g, k",
    )
    .await
    .rows();
    let complete: BTreeMap<(String, String), Vec<String>> = all
        .into_iter()
        .map(|row| ((row[0].clone(), row[1].clone()), row))
        .collect();
    let rows = on.rows();
    let off_rows = off.rows();
    if case.full_order {
        assert_eq!(rows, off_rows, "{sql}");
    } else {
        assert_eq!(sort_values(&rows), sort_values(&off_rows), "{sql}");
        let mut seen = BTreeSet::new();
        for row in &rows {
            let key = (row[0].clone(), row[1].clone());
            assert_eq!(Some(row), complete.get(&key), "{sql}: group {key:?}");
            assert!(seen.insert(key.clone()), "{sql}: {key:?} returned twice");
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Random streams, partition counts, `n`, direction, null placement, and
    /// a prefix or a full order: the rule's answer is the stock aggregate's
    /// sorted and limited answer, with each tie resolved to some complete
    /// group.
    #[test]
    fn the_exec_answers_as_the_stock_aggregate(case in arb_case()) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime builds")
            .block_on(check_case(case));
    }
}
