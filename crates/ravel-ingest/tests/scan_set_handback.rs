//! The scan-set check at flush open (issue #2410, ADR-1642 scan-set
//! amendment), on all three pipelines.
//!
//! A tenant reshards from 4 shards down to 3. A flush that opens on the
//! retiring 4-shard set pins the ingest hour it opens in, and once that hour is
//! `S` hours past the successor's activation the read side no longer scans
//! shard index 3 for it. Each case here drives a real router over a
//! `FaultStore`: the provisioning record is written with the catalog's own
//! functions, a hold gate parks one flush so the next one on the same shard is
//! deferred by the queued-flush cap, and an injected clock moves the deferred
//! flush into the hour under test. The read side is reproduced with the
//! catalog's rule itself: for every hour, list the commit records of shard
//! indices below `ravel_catalog::scan_count(h)` and decode every object they
//! name.
//!
//! The increase cases start at 3 shards and reshard up to 4, where a late
//! flush hands its rows up to a larger set (issue #2429).
//!
//! Every wait is a cooperative poll plus an injected-clock advance. No
//! wall-clock sleep, no `tokio::time::timeout`, no `Instant`.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{TestClock, make_point, tenant};
use ravel_catalog::{
    AbsentPolicy, DEFAULT_SCAN_SLACK_HOURS, ShardGeneration, append_generation, read_generations,
    scan_count, stable_generation_for_hour, validate_or_adopt,
};
use ravel_commit::{keys, record};
use ravel_ingest::{
    IngestByteBudget, IngestByteBudgetLimit, IngestConfig, IngestRouter, LogIngestRouter,
    SpanIngestRouter, WriteMode, shard_for_span,
};
use ravel_logseg::{Predicate, RlogConfig, RlogReader, stream_attrs_bytes};
use ravel_object_store::fault::{FaultPlan, FaultStore, GateHandle, Occurrence, Op};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_otlp::traces_normalize::NormalizedSpan;
use ravel_rspan::{RspanConfig, RspanReader, SpanQuery, StatusCode};
use ravel_segment::{ReaderLimits, SeriesEntryV4};
use ravel_types::logstream::{AttrValue, log_stream_id};
use ravel_types::{Signal, TenantId, shard_for, shard_for_log};
use tokio::task::JoinHandle;
use tracing_subscriber::layer::SubscriberExt;

const HOUR_NS: i64 = 3_600_000_000_000;
/// The hour every case starts in, and its first second.
const H0: u32 = 472_222;
const T0_NS: i64 = H0 as i64 * HOUR_NS + 1_000_000_000;
/// The successor generation (3 shards) activates two hours later, the
/// shortest lead the reshard CLI allows at the default refresh interval.
const ACTIVATION_HOUR: u32 = H0 + 2;
const OLD_COUNT: u32 = 4;
const NEW_COUNT: u32 = 3;
/// The one index the successor's range does not cover.
const RETIRED_SHARD: u32 = 3;
/// The flush tick; each driver step advances the clock by one.
const TICK_NS: i64 = 10_000_000;
/// Event time of row `i` is `ROW_TS_NS + i`, so a decoded row names its key.
const ROW_TS_NS: i64 = 1_000;

/// Four shards, one permit and one queue slot so one parked flush defers the
/// next flush on its shard, and both age clocks at 50 ms so every buffer is
/// due on the tick after it is written.
fn config() -> IngestConfig {
    IngestConfig {
        shard_count: OLD_COUNT,
        target_bytes: 8 * 1024 * 1024,
        max_flush_delay: Duration::from_millis(50),
        max_flush_delay_idle: Duration::from_millis(50),
        flush_tick: Duration::from_millis(10),
        max_inflight_flushes: 1,
        max_queued_flushes: 1,
        ..IngestConfig::default()
    }
}

/// Nothing flushes on its own: the age clocks are longer than any clock jump
/// a case makes, so only a drain the case asks for flushes.
fn drain_only_config() -> IngestConfig {
    IngestConfig {
        max_flush_delay: Duration::from_secs(24 * 3600),
        max_flush_delay_idle: Duration::from_secs(24 * 3600),
        ..config()
    }
}

fn start_of(hour: u32) -> i64 {
    i64::from(hour) * HOUR_NS
}

/// Counters every pipeline's metrics snapshot carries under its own names.
#[derive(Debug, Clone, Copy, Default)]
struct Counters {
    buffered: u64,
    rerouted: u64,
    rerouted_generation_mismatch: u64,
    stale: u64,
    deferred: u64,
    in_flight: u64,
    hand_back_failures: u64,
    unscanned: u64,
    mismatch_in_place: u64,
}

/// Reads a router's counters through a shared metrics handle, so a case can
/// read them after `shutdown` consumed the router.
type CounterSource = Box<dyn Fn() -> Counters + Send + Sync>;

/// What a case needs from one pipeline's router.
trait Pipe: Send + Sync + Sized + 'static {
    const SIGNAL: Signal;
    fn build(
        config: IngestConfig,
        store: Arc<dyn ObjectStoreBackend>,
        clock: Arc<TestClock>,
        budget: Arc<IngestByteBudget>,
    ) -> Self;
    /// Writes row `i`, a single item whose routing key is derived from `i`.
    fn send_row(
        self: &Arc<Self>,
        tenant: &TenantId,
        i: usize,
        mode: WriteMode,
    ) -> JoinHandle<String>;
    /// The shard row `i` of `tenant` routes to under `count` shards.
    fn shard_of(tenant: &TenantId, i: usize, count: u32) -> u32;
    fn counter_source(&self) -> CounterSource;
    fn counters(&self) -> Counters {
        (self.counter_source())()
    }
    fn flush(&self) -> impl Future<Output = ()> + Send;
    fn shutdown(self) -> impl Future<Output = ()> + Send + 'static;
    /// Event times of every row in one data object.
    fn rows(data: &[u8]) -> Vec<i64>;
}

/// A write's outcome as text: `"ok"`, or the error's `Debug` form, so the
/// three error types compare the same way.
fn outcome<T, E: std::fmt::Debug>(result: Result<T, E>) -> String {
    match result {
        Ok(_) => "ok".to_string(),
        Err(e) => format!("{e:?}"),
    }
}

impl Pipe for IngestRouter {
    const SIGNAL: Signal = Signal::Metrics;

    fn build(
        config: IngestConfig,
        store: Arc<dyn ObjectStoreBackend>,
        clock: Arc<TestClock>,
        budget: Arc<IngestByteBudget>,
    ) -> Self {
        IngestRouter::new(config, store, Signal::Metrics, clock).with_budget(budget)
    }

    fn send_row(
        self: &Arc<Self>,
        tenant: &TenantId,
        i: usize,
        mode: WriteMode,
    ) -> JoinHandle<String> {
        let router = Arc::clone(self);
        let point = metric_point(tenant, i);
        let tenant = tenant.clone();
        tokio::spawn(async move {
            outcome(
                router
                    .write(tenant, vec![point], mode, Duration::from_secs(60))
                    .await,
            )
        })
    }

    fn shard_of(tenant: &TenantId, i: usize, count: u32) -> u32 {
        shard_for(&metric_point(tenant, i).series_id, count)
    }

    fn counter_source(&self) -> CounterSource {
        let metrics = self.metrics_handle();
        Box::new(move || {
            let snap = metrics.snapshot();
            Counters {
                buffered: snap.buffered_points_total,
                rerouted: snap.rerouted_flushes,
                rerouted_generation_mismatch: snap.rerouted_flushes_generation_mismatch,
                stale: snap.stale_provisioning_flushes,
                deferred: metrics
                    .shard_skew_by_shard()
                    .into_iter()
                    .map(|(_, s)| s.flush_trigger_deferred)
                    .sum(),
                in_flight: metrics
                    .in_flight_flushes_by_shard()
                    .into_iter()
                    .map(|(_, n)| n)
                    .sum(),
                hand_back_failures: snap.hand_back_failures,
                unscanned: snap.teardown_unscanned_writes,
                mismatch_in_place: snap.generation_mismatch_written_in_place,
            }
        })
    }

    async fn flush(&self) {
        self.flush_all().await;
    }

    fn shutdown(self) -> impl Future<Output = ()> + Send + 'static {
        Self::shutdown(self)
    }

    fn rows(data: &[u8]) -> Vec<i64> {
        let limits = ReaderLimits::default();
        let loc = ravel_segment::open_from_full(data, limits).expect("opens segment");
        let entries =
            ravel_segment::decode_catalog_v5(&loc.footer, data, limits).expect("decodes catalog");
        let selected: Vec<&SeriesEntryV4> = entries.iter().collect();
        let ranges = ravel_segment::plan_ranges_v4(&loc.footer, &selected).expect("plans ranges");
        let mut out = Vec::new();
        for entry in &entries {
            for (run_index, run) in entry.runs.iter().enumerate() {
                let range = ranges
                    .iter()
                    .find(|r| r.series_id == entry.entry.series_id && r.run_index == run_index)
                    .expect("planned range for this run");
                let ts_start = range.ts_range.0 as usize;
                let ts_bytes = &data[ts_start..ts_start + range.ts_range.1 as usize];
                let val_start = range.val_range.0 as usize;
                let val_bytes = &data[val_start..val_start + range.val_range.1 as usize];
                let mut scratch = Vec::new();
                let mut timestamps = Vec::new();
                let mut values = Vec::new();
                ravel_segment::decode_run_pages_soa(
                    &entry.entry.series_id,
                    run,
                    ts_bytes,
                    val_bytes,
                    limits,
                    &mut scratch,
                    &mut timestamps,
                    &mut values,
                )
                .expect("decodes run");
                out.extend(timestamps);
            }
        }
        out
    }
}

impl Pipe for LogIngestRouter {
    const SIGNAL: Signal = Signal::Logs;

    fn build(
        config: IngestConfig,
        store: Arc<dyn ObjectStoreBackend>,
        clock: Arc<TestClock>,
        budget: Arc<IngestByteBudget>,
    ) -> Self {
        LogIngestRouter::new(config, store, clock).with_budget(budget)
    }

    fn send_row(
        self: &Arc<Self>,
        tenant: &TenantId,
        i: usize,
        mode: WriteMode,
    ) -> JoinHandle<String> {
        let router = Arc::clone(self);
        let tenant = tenant.clone();
        tokio::spawn(async move {
            outcome(
                router
                    .write(tenant, vec![log_record(i)], mode, Duration::from_secs(60))
                    .await,
            )
        })
    }

    fn shard_of(_tenant: &TenantId, i: usize, count: u32) -> u32 {
        shard_for_log(&log_record(i).stream_id, count)
    }

    fn counter_source(&self) -> CounterSource {
        let metrics = self.metrics_handle();
        Box::new(move || {
            let snap = metrics.snapshot();
            Counters {
                buffered: snap.buffered_records_total,
                rerouted: snap.rerouted_flushes,
                rerouted_generation_mismatch: snap.rerouted_flushes_generation_mismatch,
                stale: snap.stale_provisioning_flushes,
                deferred: metrics
                    .shard_skew_by_shard()
                    .into_iter()
                    .map(|(_, s)| s.flush_trigger_deferred)
                    .sum(),
                in_flight: metrics
                    .in_flight_flushes_by_shard()
                    .into_iter()
                    .map(|(_, n)| n)
                    .sum(),
                hand_back_failures: snap.hand_back_failures,
                unscanned: snap.teardown_unscanned_writes,
                mismatch_in_place: snap.generation_mismatch_written_in_place,
            }
        })
    }

    async fn flush(&self) {
        self.flush_all().await;
    }

    fn shutdown(self) -> impl Future<Output = ()> + Send + 'static {
        Self::shutdown(self)
    }

    fn rows(data: &[u8]) -> Vec<i64> {
        let reader = RlogReader::new(data, &RlogConfig::default()).expect("open rlog");
        let (records, _stats) = reader
            .scan(&Predicate::And(Vec::new()))
            .expect("unfiltered scan");
        records.into_iter().map(|r| r.ts_ns).collect()
    }
}

impl Pipe for SpanIngestRouter {
    const SIGNAL: Signal = Signal::Spans;

    fn build(
        config: IngestConfig,
        store: Arc<dyn ObjectStoreBackend>,
        clock: Arc<TestClock>,
        budget: Arc<IngestByteBudget>,
    ) -> Self {
        SpanIngestRouter::new(config, store, clock).with_budget(budget)
    }

    fn send_row(
        self: &Arc<Self>,
        tenant: &TenantId,
        i: usize,
        mode: WriteMode,
    ) -> JoinHandle<String> {
        let router = Arc::clone(self);
        let tenant = tenant.clone();
        tokio::spawn(async move {
            outcome(
                router
                    .write(tenant, vec![span(i)], mode, Duration::from_secs(60))
                    .await,
            )
        })
    }

    fn shard_of(_tenant: &TenantId, i: usize, count: u32) -> u32 {
        shard_for_span(&span(i).trace_id, count)
    }

    fn counter_source(&self) -> CounterSource {
        let metrics = self.metrics_handle();
        Box::new(move || {
            let snap = metrics.snapshot();
            Counters {
                buffered: snap.buffered_spans_total,
                rerouted: snap.rerouted_flushes,
                rerouted_generation_mismatch: snap.rerouted_flushes_generation_mismatch,
                stale: snap.stale_provisioning_flushes,
                deferred: metrics
                    .shard_skew_by_shard()
                    .into_iter()
                    .map(|(_, s)| s.flush_trigger_deferred)
                    .sum(),
                in_flight: metrics
                    .in_flight_flushes_by_shard()
                    .into_iter()
                    .map(|(_, n)| n)
                    .sum(),
                hand_back_failures: snap.hand_back_failures,
                unscanned: snap.teardown_unscanned_writes,
                mismatch_in_place: snap.generation_mismatch_written_in_place,
            }
        })
    }

    async fn flush(&self) {
        self.flush_all().await;
    }

    fn shutdown(self) -> impl Future<Output = ()> + Send + 'static {
        Self::shutdown(self)
    }

    fn rows(data: &[u8]) -> Vec<i64> {
        let reader = RspanReader::new(data, &RspanConfig::default()).expect("open rspan");
        let (spans, _stats) = reader
            .scan(&SpanQuery::ts_range(i64::MIN, i64::MAX))
            .expect("unfiltered scan");
        spans.into_iter().map(|s| s.start_ts_ns).collect()
    }
}

fn metric_point(tenant: &TenantId, i: usize) -> ravel_otlp::NormalizedPoint {
    let host = format!("h{i}");
    make_point(
        tenant,
        "cpu_usage",
        &[("host", &host)],
        ROW_TS_NS + i as i64,
        1.0,
    )
}

/// One record on its own log stream.
fn log_record(i: usize) -> NormalizedLogRecord {
    let resource: Vec<(String, AttrValue)> = vec![
        (
            "service.name".to_string(),
            AttrValue::Str("api".to_string()),
        ),
        ("host".to_string(), AttrValue::Str(format!("h{i}"))),
    ];
    let scope_attrs: Vec<(String, AttrValue)> = Vec::new();
    NormalizedLogRecord {
        stream_id: log_stream_id(&resource, "scope", "", &scope_attrs),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "", &scope_attrs),
        ts_ns: ROW_TS_NS + i as i64,
        observed_ts_ns: ROW_TS_NS + i as i64,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: format!("line {i}"),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: Vec::new(),
    }
}

/// One span with its own trace id.
fn span(i: usize) -> NormalizedSpan {
    let mut trace_id = [0u8; 16];
    trace_id[..4].copy_from_slice(&(i as u32 + 1).to_be_bytes());
    let mut span_id = [0u8; 8];
    span_id[..4].copy_from_slice(&(i as u32 + 1).to_be_bytes());
    NormalizedSpan {
        trace_id,
        span_id,
        parent_span_id: None,
        name: format!("handle-{i}"),
        start_ts_ns: ROW_TS_NS + i as i64,
        end_ts_ns: ROW_TS_NS + 100 + i as i64,
        status_code: StatusCode::Unset,
        status_message: None,
        attrs: vec![("service.name".to_string(), "checkout".to_string())],
    }
}

/// The first `n` row keys of `tenant` on shard `shard` of the 4-shard set,
/// skipping `skip` matches so two tenants of one case never share a key.
fn rows_on<P: Pipe>(tenant: &TenantId, shard: u32, skip: usize, n: usize) -> Vec<usize> {
    rows_on_count::<P>(tenant, OLD_COUNT, shard, skip, n)
}

/// [`rows_on`] for a set of `count` shards.
fn rows_on_count<P: Pipe>(
    tenant: &TenantId,
    count: u32,
    shard: u32,
    skip: usize,
    n: usize,
) -> Vec<usize> {
    (0..10_000)
        .filter(|&i| P::shard_of(tenant, i, count) == shard)
        .skip(skip)
        .take(n)
        .collect()
}

/// Yields until `probe` holds, advancing the injected clock by one flush tick
/// between rounds so actor ticks fire. Returns the probe's last answer, so a
/// claim that never comes true fails on the caller's assertion, not a hang.
async fn drive(clock: &TestClock, mut probe: impl FnMut() -> bool) -> bool {
    for _ in 0..400 {
        if probe() {
            return true;
        }
        clock.advance_ns(TICK_NS);
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
    }
    probe()
}

/// Advances the injected clock `n` flush ticks, yielding after each.
async fn ticks(clock: &TestClock, n: usize) {
    for _ in 0..n {
        clock.advance_ns(TICK_NS);
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
    }
}

/// Ids of the calls `gate`'s store holds right now for `op` on a key
/// containing `key_part`. A gate's registry is shared by the whole store and
/// keeps the entry of a call whose future was dropped (the abandoned parked
/// flush), so a case filters for the calls it is about.
fn held(gate: &GateHandle, op: Op, key_part: &str) -> Vec<u64> {
    gate.held_details()
        .into_iter()
        .filter(|(_, held_op, key)| *held_op == op && key.contains(key_part))
        .map(|(id, ..)| id)
        .collect()
}

/// Yields (no clock movement) until `probe` holds or the budget runs out.
async fn settle(mut probe: impl FnMut() -> bool) -> bool {
    for _ in 0..10_000 {
        if probe() {
            return true;
        }
        tokio::task::yield_now().await;
    }
    probe()
}

/// Every key under the tenant's prefix for `signal` that names shard `shard`,
/// data objects and commit records alike.
async fn keys_under_shard(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    signal: Signal,
    shard: u32,
) -> Vec<String> {
    let prefix = format!("t/{}/{}/", tenant.hash().to_hex(), signal.key_prefix());
    let l0 = format!("/l0/{shard:04}/");
    let c = format!("/c/{shard:04}/");
    list_all(store, &prefix)
        .await
        .expect("list tenant prefix")
        .into_iter()
        .map(|o| o.key)
        .filter(|k| k.contains(&l0) || k.contains(&c))
        .collect()
}

/// The commit records under `(shard, hour)`.
async fn commits_at(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    signal: Signal,
    shard: u32,
    hour: u32,
) -> Vec<String> {
    let prefix =
        keys::commit_shard_hour_prefix(&tenant.hash(), signal, shard, hour).expect("commit prefix");
    list_all(store, &prefix)
        .await
        .expect("list commit prefix")
        .into_iter()
        .map(|o| o.key)
        .collect()
}

/// The read side's view of the tenant: for every hour in `hours`, the commit
/// records of every shard index below `scan_count(h)` over the persisted
/// generation history, and every row of every object they name. Sorted event
/// times, duplicates kept, so "exactly once" is an equality on this vector.
async fn scanned_rows<P: Pipe>(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    hours: std::ops::RangeInclusive<u32>,
) -> Vec<i64> {
    let generations = persisted_generations(store, tenant, P::SIGNAL).await;
    let mut rows = Vec::new();
    for hour in hours {
        for shard in 0..scan_count(&generations, hour, DEFAULT_SCAN_SLACK_HOURS) {
            for commit in commits_at(store, tenant, P::SIGNAL, shard, hour).await {
                let bytes = store
                    .get(&commit, GetRange::Full)
                    .await
                    .expect("get commit record")
                    .data;
                let decoded = record::decode(&bytes).expect("decode commit record");
                let data = store
                    .get(&decoded.object_key, GetRange::Full)
                    .await
                    .expect("get data object")
                    .data;
                rows.extend(P::rows(&data));
            }
        }
    }
    rows.sort_unstable();
    rows
}

async fn persisted_generations(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    signal: Signal,
) -> Vec<ShardGeneration> {
    let key = ravel_catalog::provisioning_key(&tenant.hash(), signal);
    let bytes = store
        .get(&key, GetRange::Full)
        .await
        .expect("provisioning record")
        .data;
    let record =
        <ravel_proto::sys::v1::ProvisioningRecord as prost::Message>::decode(bytes.as_ref())
            .expect("decode provisioning record");
    read_generations(&record, &key).expect("valid generation history")
}

fn expected(keys: &[usize]) -> Vec<i64> {
    let mut rows: Vec<i64> = keys.iter().map(|&i| ROW_TS_NS + i as i64).collect();
    rows.sort_unstable();
    rows
}

/// One case's world: a `FaultStore` holding both tenants' provisioning
/// records at generation 0 (4 shards), and a router over it.
struct World<P> {
    fault: Arc<FaultStore<MemoryStore>>,
    store: Arc<dyn ObjectStoreBackend>,
    clock: Arc<TestClock>,
    budget: Arc<IngestByteBudget>,
    router: Arc<P>,
    parked: TenantId,
    deferred: TenantId,
    /// Generation 0's shard count, the router's configured `shard_count`.
    initial: u32,
}

impl<P: Pipe> World<P> {
    async fn new() -> Self {
        Self::with_config(config()).await
    }

    async fn with_config(config: IngestConfig) -> Self {
        let initial = config.shard_count;
        let fault = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
        let store: Arc<dyn ObjectStoreBackend> = fault.clone();
        let clock = TestClock::new(T0_NS);
        let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(1 << 30));
        let parked = tenant("acme");
        let deferred = tenant("globex");
        for t in [&parked, &deferred] {
            validate_or_adopt(
                store.as_ref(),
                &t.hash(),
                P::SIGNAL,
                initial,
                T0_NS,
                AbsentPolicy::CreateFromConfig,
            )
            .await
            .expect("create the provisioning record at generation 0");
        }
        let router = Arc::new(P::build(
            config,
            Arc::clone(&store),
            clock.clone(),
            Arc::clone(&budget),
        ));
        World {
            fault,
            store,
            clock,
            budget,
            router,
            parked,
            deferred,
            initial,
        }
    }

    /// Appends the 3-shard successor to both tenants' records. The router has
    /// already read generation 0 and is not told.
    async fn reshard_down(&self) {
        self.reshard_to(NEW_COUNT).await;
    }

    /// Appends a `count`-shard generation 1, activating at `ACTIVATION_HOUR`,
    /// to both tenants' records, without telling the router.
    async fn reshard_to(&self, count: u32) {
        for t in [&self.parked, &self.deferred] {
            append_generation(
                self.store.as_ref(),
                &t.hash(),
                P::SIGNAL,
                count,
                ACTIVATION_HOUR,
                self.clock.now(),
            )
            .await
            .expect("append generation 1");
        }
    }

    /// Parks one flush of the `parked` tenant on `shard` behind a held data
    /// PUT, then buffers `keys` for the `deferred` tenant on the same shard
    /// and lets its age trigger be refused at the queued-flush cap. The
    /// parked flush is abandoned at its lifetime once the clock jumps, which
    /// frees the slot. Returns the bytes the deferred rows are charged.
    async fn defer(&self, shard: u32, keys: &[usize]) -> u64 {
        // Scoped to the parked tenant's data objects: the first gate matching
        // a call governs it, so a later gate on other tenants' PUTs still
        // sees theirs.
        let parked_data = format!(
            "t/{}/{}/l0/",
            self.parked.hash().to_hex(),
            P::SIGNAL.key_prefix()
        );
        let gate = self
            .fault
            .hold(Op::Put, Some(parked_data), Occurrence::Nth(1));
        let parked_key = rows_on_count::<P>(&self.parked, self.initial, shard, 0, 1)[0];
        let parked = self
            .router
            .send_row(&self.parked, parked_key, WriteMode::Buffered);
        assert_eq!(parked.await.expect("parked write task"), "ok");
        assert!(
            drive(&self.clock, || self.router.counters().in_flight == 1).await,
            "the parked flush opens"
        );
        // Never released: the parked flush is abandoned at its lifetime when
        // a case jumps the clock, which frees the queue slot.
        gate.wait_until_held(1).await;

        let charged_before = self.budget.in_flight_bytes();
        for &i in keys {
            let write = self.router.send_row(&self.deferred, i, WriteMode::Buffered);
            assert_eq!(write.await.expect("deferred write task"), "ok");
        }
        let deferred_bytes = self.budget.in_flight_bytes() - charged_before;
        let deferred_before = self.router.counters().deferred;
        assert!(
            drive(&self.clock, || self.router.counters().deferred
                > deferred_before)
            .await,
            "the deferred tenant's trigger is refused at the queued-flush cap"
        );
        deferred_bytes
    }
}

/// A buffer on the retiring index, deferred until its flush would pin an hour
/// `S` hours past the successor's activation, writes nothing under that index
/// and every row is found exactly once by the read side's scan rule.
///
/// Guards: the `if shard < scan` test in `GenerationSwitch::scan_check`
/// (generation.rs), and the `self.scope.check(..)` call at flush open in
/// each actor (`flush_tenant` in shard.rs, log_shard.rs, span_shard.rs). With
/// the first made unconditional, or the second replaced by
/// `ScanCheck::InScanSet`, the flush writes under shard 3 in hour
/// `ACTIVATION_HOUR + S` and nothing is handed back. And the reason test in
/// each metrics registry's `record_rerouted_flush`: counting every hand-back
/// as a generation mismatch moves `rerouted_generation_mismatch` to 1.
async fn retired_index_past_the_window_hands_back<P: Pipe>() {
    let world = World::<P>::new().await;
    let keys = rows_on::<P>(&world.deferred, RETIRED_SHARD, 1, 6);
    let targets: std::collections::BTreeSet<u32> = keys
        .iter()
        .map(|&i| P::shard_of(&world.deferred, i, NEW_COUNT))
        .collect();
    assert!(
        targets.len() >= 2,
        "the fixture splits across successor shards, {targets:?}"
    );
    world.defer(RETIRED_SHARD, &keys).await;
    world.reshard_down().await;

    let pin_hour = ACTIVATION_HOUR + DEFAULT_SCAN_SLACK_HOURS;
    world.clock.set_ns(start_of(pin_hour) + 1_000_000_000);
    let generations = persisted_generations(world.store.as_ref(), &world.deferred, P::SIGNAL).await;
    assert_eq!(
        scan_count(&generations, pin_hour, DEFAULT_SCAN_SLACK_HOURS),
        NEW_COUNT
    );
    assert!(
        drive(&world.clock, || world.router.counters().rerouted == 1).await,
        "the flush is handed back, counters {:?}",
        world.router.counters()
    );
    assert_eq!(
        world.router.counters().rerouted_generation_mismatch,
        0,
        "a retired-index hand-back is not counted as a generation mismatch"
    );
    let store = world.store.as_ref();
    world.router.flush().await;

    assert_eq!(
        keys_under_shard(store, &world.deferred, P::SIGNAL, RETIRED_SHARD).await,
        Vec::<String>::new(),
        "nothing of the deferred tenant is written under the retired index"
    );
    assert_eq!(
        scanned_rows::<P>(store, &world.deferred, H0..=pin_hour + 1).await,
        expected(&keys),
        "the scan rule finds every row exactly once"
    );
    assert!(
        commits_at(store, &world.deferred, P::SIGNAL, RETIRED_SHARD, pin_hour)
            .await
            .is_empty()
    );
}

/// A flush from the retiring set that opens inside the window, one hour after
/// the activation, still writes under its own index: the check does not
/// over-trigger.
///
/// Guard: `scan_count(&view.generations, hour, DEFAULT_SCAN_SLACK_HOURS)` in
/// `GenerationSwitch::scan_check`. Checked against the active count instead,
/// the flush is handed back and `rerouted` reads 1.
async fn retired_index_inside_the_window_writes_in_place<P: Pipe>() {
    let world = World::<P>::new().await;
    let keys = rows_on::<P>(&world.deferred, RETIRED_SHARD, 1, 3);
    world.defer(RETIRED_SHARD, &keys).await;
    world.reshard_down().await;

    let pin_hour = ACTIVATION_HOUR + 1;
    world.clock.set_ns(start_of(pin_hour) + 1_000_000_000);
    let store = world.store.as_ref();
    let mut landed = false;
    for _ in 0..10 {
        landed = !commits_at(store, &world.deferred, P::SIGNAL, RETIRED_SHARD, pin_hour)
            .await
            .is_empty();
        if landed {
            break;
        }
        ticks(&world.clock, 20).await;
    }
    assert!(landed, "the flush writes under shard 3 in hour {pin_hour}");
    assert_eq!(
        world.router.counters().rerouted,
        0,
        "nothing is handed back"
    );
    assert_eq!(
        scanned_rows::<P>(store, &world.deferred, H0..=pin_hour + 1).await,
        expected(&keys)
    );
}

/// An index the successor also covers, and the row keys on it under the
/// 4-shard set that the 3-shard successor routes to a different index, so a
/// row written under `COVERED_SHARD` and the same row routed by the successor
/// can never be confused.
const COVERED_SHARD: u32 = 1;

fn covered_rows<P: Pipe>(tenant: &TenantId, n: usize) -> Vec<usize> {
    (0..10_000)
        .filter(|&i| {
            P::shard_of(tenant, i, OLD_COUNT) == COVERED_SHARD
                && P::shard_of(tenant, i, NEW_COUNT) != COVERED_SHARD
        })
        .skip(1)
        .take(n)
        .collect()
}

/// Every row of every object committed under `(shard, hour)`, sorted.
async fn rows_at<P: Pipe>(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    shard: u32,
    hour: u32,
) -> Vec<i64> {
    let mut rows = Vec::new();
    for commit in commits_at(store, tenant, P::SIGNAL, shard, hour).await {
        let bytes = store
            .get(&commit, GetRange::Full)
            .await
            .expect("get commit record")
            .data;
        let decoded = record::decode(&bytes).expect("decode commit record");
        let data = store
            .get(&decoded.object_key, GetRange::Full)
            .await
            .expect("get data object")
            .data;
        rows.extend(P::rows(&data));
    }
    rows.sort_unstable();
    rows
}

/// Ticks until any commit record of `tenant` exists under one of `shards` in
/// `hour`, at most 200 ticks.
async fn wait_for_commit<P: Pipe>(world: &World<P>, shards: &[u32], hour: u32) {
    let store = world.store.as_ref();
    for _ in 0..10 {
        for &shard in shards {
            if !commits_at(store, &world.deferred, P::SIGNAL, shard, hour)
                .await
                .is_empty()
            {
                return;
            }
        }
        ticks(&world.clock, 20).await;
    }
}

/// Issue #2429. A buffer routed by generation 0 (4 shards) on an index the
/// 3-shard successor also covers is deferred until its flush would pin an hour
/// generation 1 owns alone, `S + 5` hours past the activation. The index is
/// inside the scan set, so the retired-index check passes it; written in
/// place, its rows would sit under the index generation 0 routed them to, in
/// an hour `ravel_catalog::stable_generation_for_hour` attributes to
/// generation 1, whose routing puts every one of these rows at another index.
/// The read side scans both and still finds them, but
/// `ravel_query::distrib::pushdown::is_pushdown_eligible` passes such an hour
/// and `ravel_query::distrib::partition::partition_snapshot` splits its work
/// by shard index, so a series at two indices in it is counted by two slice
/// workers that each assume they hold all of it. That pushdown split is what
/// the hand-back protects: this test asserts the precondition it relies on,
/// every row of the hour at the index generation 1 routes it to.
///
/// So the flush writes nothing under its own index in that hour, the rows are
/// handed back and re-routed under generation 1, each lands at generation 1's
/// index for its key, and the scan rule finds every row exactly once.
///
/// Guards: the `owner != self_count` hand-back arm of
/// `GenerationSwitch::scan_check` (generation.rs). With it removed the flush
/// writes in place: rows appear under `COVERED_SHARD` in `pin_hour` and
/// `rerouted` stays 0.
async fn covered_index_in_a_successor_owned_hour_hands_back<P: Pipe>() {
    let world = World::<P>::new().await;
    let keys = covered_rows::<P>(&world.deferred, 6);
    let mut by_target: std::collections::BTreeMap<u32, Vec<usize>> = Default::default();
    for &i in &keys {
        by_target
            .entry(P::shard_of(&world.deferred, i, NEW_COUNT))
            .or_default()
            .push(i);
    }
    assert_eq!(keys.len(), 6);
    world.defer(COVERED_SHARD, &keys).await;
    world.reshard_down().await;

    let pin_hour = ACTIVATION_HOUR + DEFAULT_SCAN_SLACK_HOURS + 5;
    world.clock.set_ns(start_of(pin_hour) + 1_000_000_000);
    let generations = persisted_generations(world.store.as_ref(), &world.deferred, P::SIGNAL).await;
    assert_eq!(
        stable_generation_for_hour(&generations, pin_hour, DEFAULT_SCAN_SLACK_HOURS),
        Some(1),
        "generation 1 owns the pinned hour alone"
    );
    assert!(COVERED_SHARD < scan_count(&generations, pin_hour, DEFAULT_SCAN_SLACK_HOURS));

    let targets: Vec<u32> = std::iter::once(COVERED_SHARD)
        .chain(by_target.keys().copied())
        .collect();
    wait_for_commit(&world, &targets, pin_hour).await;
    world.router.flush().await;
    let store = world.store.as_ref();
    assert_eq!(
        rows_at::<P>(store, &world.deferred, COVERED_SHARD, pin_hour).await,
        Vec::<i64>::new(),
        "nothing routed by generation 0 is written under shard {COVERED_SHARD} in hour \
         {pin_hour}, which generation 1 owns; counters {:?}",
        world.router.counters()
    );
    let counters = world.router.counters();
    assert_eq!(counters.rerouted, 1, "the flush is handed back once");
    assert_eq!(
        counters.rerouted_generation_mismatch, 1,
        "and counted under the generation-mismatch reason"
    );
    for (&target, target_keys) in &by_target {
        assert_eq!(
            rows_at::<P>(store, &world.deferred, target, pin_hour).await,
            expected(target_keys),
            "the rows generation 1 routes to shard {target} land there, in hour {pin_hour}"
        );
    }
    assert_eq!(
        scanned_rows::<P>(store, &world.deferred, H0..=pin_hour + 1).await,
        expected(&keys),
        "the scan rule finds every row exactly once"
    );
}

/// The same buffer, opening one hour after the activation, inside the
/// overlap where both generations' shards can hold the hour's data and
/// `stable_generation_for_hour` names no owner: it writes in place under its
/// own index, as before issue #2429.
///
/// Guard: the `Some(owner)` match on `stable_generation_for_hour` in
/// `GenerationSwitch::scan_check`. Treating an unowned hour as the active
/// generation's hands this flush back, and nothing lands under
/// `COVERED_SHARD` in `pin_hour`.
async fn covered_index_inside_the_overlap_writes_in_place<P: Pipe>() {
    let world = World::<P>::new().await;
    let keys = covered_rows::<P>(&world.deferred, 3);
    world.defer(COVERED_SHARD, &keys).await;
    world.reshard_down().await;

    let pin_hour = ACTIVATION_HOUR + 1;
    world.clock.set_ns(start_of(pin_hour) + 1_000_000_000);
    let generations = persisted_generations(world.store.as_ref(), &world.deferred, P::SIGNAL).await;
    assert_eq!(
        stable_generation_for_hour(&generations, pin_hour, DEFAULT_SCAN_SLACK_HOURS),
        None,
        "no generation owns an hour inside the activation overlap"
    );
    wait_for_commit(&world, &[COVERED_SHARD], pin_hour).await;
    let store = world.store.as_ref();
    assert_eq!(
        rows_at::<P>(store, &world.deferred, COVERED_SHARD, pin_hour).await,
        expected(&keys),
        "the flush writes under shard {COVERED_SHARD} in hour {pin_hour}"
    );
    assert_eq!(
        world.router.counters().rerouted,
        0,
        "nothing is handed back"
    );
    assert_eq!(
        scanned_rows::<P>(store, &world.deferred, H0..=pin_hour + 1).await,
        expected(&keys)
    );
}

/// The generation view cannot be refreshed: the flush does not open, nothing
/// is written and nothing is lost, and the rows land once the re-read
/// completes. The router's own view says generation 0 only, which would put
/// shard 3 inside the scan set; it is past its trust horizon, so it is not
/// used.
///
/// Guards: the `hour < trust_horizon(..)` match guard in
/// `GenerationSwitch::scan_check`. Without it the stale generation-0 view
/// clears the flush, which writes under shard 3 while the re-read is held,
/// and the fail-closed assertion below never sees a refusal. And the
/// `stale_view_counted` test in each actor's `ScanCheck::Unknown` arm: without
/// it every tick that retries the closed flush counts again, and the
/// once-per-episode assertion reads one per tick.
async fn unrefreshable_view_keeps_the_flush_closed<P: Pipe>() {
    let world = World::<P>::new().await;
    let keys = rows_on::<P>(&world.deferred, RETIRED_SHARD, 1, 3);
    world.defer(RETIRED_SHARD, &keys).await;
    world.reshard_down().await;
    // Only the first re-read is held. A flush that started a re-read per tick
    // would get its second one through and hand back while this one waits.
    let reread = world
        .fault
        .hold(Op::Get, Some("/prov".to_string()), Occurrence::Nth(1));

    let pin_hour = ACTIVATION_HOUR + DEFAULT_SCAN_SLACK_HOURS;
    world.clock.set_ns(start_of(pin_hour) + 1_000_000_000);
    let stale_before = world.router.counters().stale;
    assert!(
        drive(&world.clock, || world.router.counters().stale
            > stale_before)
        .await,
        "the flush fails closed, counters {:?}",
        world.router.counters()
    );
    ticks(&world.clock, 10).await;
    assert_eq!(
        world.router.counters().stale,
        stale_before + 1,
        "the stale-view episode counts once for the buffer, not once per tick"
    );
    let store = world.store.as_ref();
    let held_gets = held(&reread, Op::Get, "/prov");
    assert_eq!(held_gets.len(), 1, "one re-read in flight");
    assert_eq!(
        world.router.counters().rerouted,
        0,
        "nothing is handed back"
    );
    assert_eq!(
        keys_under_shard(store, &world.deferred, P::SIGNAL, RETIRED_SHARD).await,
        Vec::<String>::new(),
        "nothing is written while the view is unknown"
    );
    assert!(
        world.budget.in_flight_bytes() > 0,
        "the rows are still buffered and charged"
    );

    assert!(reread.release(held_gets[0]));
    assert!(
        drive(&world.clock, || world.router.counters().rerouted == 1).await,
        "the flush hands back once the view is re-read"
    );
    world.router.flush().await;
    assert_eq!(
        scanned_rows::<P>(store, &world.deferred, H0..=pin_hour + 1).await,
        expected(&keys),
        "nothing was lost"
    );
}

/// The byte charges move with the rows, a clone to every target: while one
/// successor's flush of the handed-back rows has landed and another's is still
/// held, exactly the deferred rows' bytes are still charged, and the budget
/// returns to zero once the held one lands too.
///
/// Guard: `charges: charges.clone()` in each pipeline's `hand_back`. Sending
/// an empty charge list instead refunds the bytes at hand-back; sending every
/// charge to the first (lowest) target only and none to the rest refunds them
/// when that target's flush lands, while the highest target's PUT is held.
/// Either way the held-while-buffered assertion fails.
async fn charges_follow_the_rows<P: Pipe>() {
    let world = World::<P>::new().await;
    let keys = rows_on::<P>(&world.deferred, RETIRED_SHARD, 1, 6);
    let targets: std::collections::BTreeSet<u32> = keys
        .iter()
        .map(|&i| P::shard_of(&world.deferred, i, NEW_COUNT))
        .collect();
    assert!(
        targets.len() >= 2,
        "the fixture splits across successor shards, {targets:?}"
    );
    let first = *targets.first().expect("a first target");
    let last = *targets.last().expect("a last target");
    let deferred_bytes = world.defer(RETIRED_SHARD, &keys).await;
    assert!(deferred_bytes > 0);
    world.reshard_down().await;
    let pin_hour = ACTIVATION_HOUR + DEFAULT_SCAN_SLACK_HOURS;
    // Holds only the last successor target's data PUTs. The parked tenant's
    // flush is abandoned at the jump, which refunds its own charge.
    let last_put = world
        .fault
        .hold(Op::Put, Some(format!("/l0/{last:04}/")), Occurrence::Always);
    world.clock.set_ns(start_of(pin_hour) + 1_000_000_000);
    assert!(
        drive(&world.clock, || world.router.counters().rerouted == 1).await,
        "the flush is handed back"
    );
    let store = world.store.as_ref();
    let mut first_landed = false;
    for _ in 0..10 {
        first_landed = !commits_at(store, &world.deferred, P::SIGNAL, first, pin_hour)
            .await
            .is_empty();
        if first_landed {
            break;
        }
        ticks(&world.clock, 20).await;
    }
    assert!(first_landed, "the first target's flush lands");
    assert!(
        drive(&world.clock, || !held(
            &last_put,
            Op::Put,
            &world.deferred.hash().to_hex()
        )
        .is_empty())
        .await,
        "the last target's flush reaches its data PUT"
    );
    assert_eq!(
        world.budget.in_flight_bytes(),
        deferred_bytes,
        "exactly the deferred rows' bytes are held while one target still holds them: \
         not refunded at hand-back or by the first target's flush, and not charged twice"
    );
    assert!(
        drive(&world.clock, || {
            for id in held(&last_put, Op::Put, &world.deferred.hash().to_hex()) {
                last_put.release(id);
            }
            world.budget.in_flight_bytes() == 0
        })
        .await,
        "the budget returns to zero once the handed-back rows flush, {} bytes left",
        world.budget.in_flight_bytes()
    );
    assert_eq!(
        scanned_rows::<P>(store, &world.deferred, H0..=pin_hour + 1).await,
        expected(&keys)
    );
}

/// Buffers `keys` of the `deferred` tenant on the 4-shard set, appends the
/// 3-shard successor, and moves the clock `S` hours past its activation, so
/// a flush of those rows on the retired index must hand them back. The
/// router's view of the tenant is now past its trust horizon.
async fn retired_rows_past_the_window<P: Pipe>(world: &World<P>, keys: &[usize]) -> u32 {
    for &i in keys {
        let write = world
            .router
            .send_row(&world.deferred, i, WriteMode::Buffered);
        assert_eq!(write.await.expect("write task"), "ok");
    }
    world.reshard_down().await;
    let pin_hour = ACTIVATION_HOUR + DEFAULT_SCAN_SLACK_HOURS;
    world.clock.set_ns(start_of(pin_hour) + 1_000_000_000);
    pin_hour
}

/// One drain hands the rows back and flushes them: its first pass finds the
/// view past its horizon, re-reads it and hands the rows to the 3-shard set,
/// which that pass did not list because the hand-back constructed it, and the
/// second pass flushes them there.
///
/// Guards: the repeat in each router's `flush_all` (`if
/// self.metrics.rerouted_flushes() == handed_back { break; }` in a loop of
/// `HAND_BACK_DRAIN_PASSES`). With a single pass the rows stay buffered in
/// the successor and the scan rule finds none. And the `reread_and_check(..)`
/// call in each actor's `scan_check`: without it the drain only starts a
/// background re-read, hands nothing back, and nothing is written.
async fn one_drain_hands_back_and_flushes<P: Pipe>() {
    let world = World::<P>::with_config(drain_only_config()).await;
    let keys = rows_on::<P>(&world.deferred, RETIRED_SHARD, 0, 6);
    let pin_hour = retired_rows_past_the_window(&world, &keys).await;

    world.router.flush().await;

    assert_eq!(world.router.counters().rerouted, 1);
    let store = world.store.as_ref();
    assert_eq!(
        keys_under_shard(store, &world.deferred, P::SIGNAL, RETIRED_SHARD).await,
        Vec::<String>::new()
    );
    assert_eq!(
        scanned_rows::<P>(store, &world.deferred, H0..=pin_hour + 1).await,
        expected(&keys),
        "one drain wrote every row where the scan rule finds it"
    );
}

/// Shutdown drains the 4-shard set before the 3-shard set, so the rows the
/// retiring set hands back reach a set that is still running.
///
/// Guard: `.max_by_key(|(count, _)| **count)` in
/// `GenerationSwitch::largest_undrained_set`. Draining smallest first stops
/// the 3-shard set before the hand-back, which then finds no live target and
/// writes the rows in place on the retired index at teardown.
async fn shutdown_drains_the_largest_set_first<P: Pipe>() {
    let world = World::<P>::with_config(drain_only_config()).await;
    let keys = rows_on::<P>(&world.deferred, RETIRED_SHARD, 0, 6);
    let pin_hour = retired_rows_past_the_window(&world, &keys).await;
    // A write after the activation builds the 3-shard set before shutdown.
    let other = world.router.send_row(&world.parked, 0, WriteMode::Buffered);
    assert_eq!(other.await.expect("write task"), "ok");

    let World {
        router,
        store,
        deferred,
        ..
    } = world;
    let router = Arc::into_inner(router).expect("sole router owner");
    let counters = router.counter_source();
    router.shutdown().await;

    assert_eq!(
        counters().unscanned,
        0,
        "nothing was written outside the scan set"
    );
    assert_eq!(
        keys_under_shard(store.as_ref(), &deferred, P::SIGNAL, RETIRED_SHARD).await,
        Vec::<String>::new()
    );
    assert_eq!(
        scanned_rows::<P>(store.as_ref(), &deferred, H0..=pin_hour + 1).await,
        expected(&keys)
    );
}

/// A hand-back during shutdown can construct the 3-shard set, which no list
/// taken before the drain holds; shutdown still signals it and waits for its
/// flush before returning.
///
/// Guard: the `while let Some(..) = self.switch.largest_undrained_set(..)`
/// loop in each router's `shutdown`. Draining a list of sets taken once,
/// shutdown returns after the 4-shard set while the 3-shard set's flush is
/// still parked on its held PUT, left to an unawaited channel-close drain.
async fn shutdown_drains_a_set_a_hand_back_built<P: Pipe>() {
    let world = World::<P>::with_config(drain_only_config()).await;
    let keys = rows_on::<P>(&world.deferred, RETIRED_SHARD, 0, 6);
    let pin_hour = retired_rows_past_the_window(&world, &keys).await;
    let tenant_data = format!(
        "t/{}/{}/l0/",
        world.deferred.hash().to_hex(),
        P::SIGNAL.key_prefix()
    );
    let gate = world
        .fault
        .hold(Op::Put, Some(tenant_data.clone()), Occurrence::Always);

    let World {
        router,
        store,
        deferred,
        ..
    } = world;
    let router = Arc::into_inner(router).expect("sole router owner");
    let shutdown = tokio::spawn(router.shutdown());
    assert!(
        settle(|| shutdown.is_finished() || !held(&gate, Op::Put, &tenant_data).is_empty()).await
    );
    assert!(
        !shutdown.is_finished(),
        "shutdown waits for the flush of the set the hand-back built"
    );
    assert!(
        settle(|| {
            for id in held(&gate, Op::Put, &tenant_data) {
                gate.release(id);
            }
            shutdown.is_finished()
        })
        .await
    );
    shutdown.await.expect("shutdown task");
    assert_eq!(
        keys_under_shard(store.as_ref(), &deferred, P::SIGNAL, RETIRED_SHARD).await,
        Vec::<String>::new()
    );
    assert_eq!(
        scanned_rows::<P>(store.as_ref(), &deferred, H0..=pin_hour + 1).await,
        expected(&keys)
    );
}

/// A strict waiter on a buffer that is handed back is answered with the
/// outcome-unknown `Abandoned`, and its rows are still written, inside the scan
/// set. The writer's clock jumps past the window before the buffer's first
/// trigger, so no deferral is involved and the deferral cap answers nothing.
///
/// Guard: the `ack_waiters(.., SCAN_SET_HANDBACK_ABANDONED)` call in each
/// pipeline's `hand_back`. Without it the waiter's sender is dropped with the
/// buffer and the write reports a dead shard.
async fn strict_waiter_on_a_handed_back_buffer_is_abandoned<P: Pipe>() {
    let world = World::<P>::new().await;
    let keys = rows_on::<P>(&world.deferred, RETIRED_SHARD, 0, 1);
    let handle = world
        .router
        .send_row(&world.deferred, keys[0], WriteMode::Strict);
    assert!(
        settle(|| world.router.counters().buffered == 1).await,
        "the strict row is buffered on the 4-shard set before the reshard"
    );
    world.reshard_down().await;
    let pin_hour = ACTIVATION_HOUR + DEFAULT_SCAN_SLACK_HOURS;
    world.clock.set_ns(start_of(pin_hour) + 1_000_000_000);
    assert!(
        drive(&world.clock, || handle.is_finished()).await,
        "the strict write is answered"
    );
    let answer = handle.await.expect("strict write task");
    assert!(
        answer.contains("Abandoned") && answer.contains("outside the read-side scan set"),
        "the waiter gets the outcome-unknown Abandoned, got {answer}"
    );
    assert_eq!(world.router.counters().rerouted, 1);
    world.router.flush().await;
    assert_eq!(
        scanned_rows::<P>(world.store.as_ref(), &world.deferred, H0..=pin_hour + 1).await,
        expected(&keys),
        "the abandoned waiter's row is written once"
    );
}

/// The increase cases start at 3 shards and reshard up to 4.
const UP_FROM: u32 = NEW_COUNT;
const UP_TO: u32 = OLD_COUNT;

fn up_config() -> IngestConfig {
    IngestConfig {
        shard_count: UP_FROM,
        ..config()
    }
}

/// The 4-shard index whose teardown flush the race case holds.
const HELD_SHARD: u32 = 1;
/// The 3-shard index the race case's late buffer sits on. Its rows route to
/// `HELD_SHARD` under 4 shards.
const SOURCE_SHARD: u32 = 2;

/// A hand-back sent to a set that is shutting down is never lost. Shutdown
/// drains the 4-shard set first; one of its actors is held inside its teardown
/// flush by a held data PUT. Meanwhile a 3-shard buffer whose flush opens in an
/// hour the 4-shard generation owns hands its rows up to exactly that actor.
/// The actor closed its mailbox before the teardown flush, so the send fails as
/// closed, the rows stay with the source, and the source's own teardown writes
/// them. Every acknowledged row is stored exactly once.
///
/// Guard: the `self.rx.close()` call in each actor's `close_mailbox`. Without
/// it the send is accepted into a mailbox nothing reads again, the source
/// gives the rows up, and they are lost when the actor's receiver drops.
async fn a_hand_back_to_a_draining_set_is_never_lost<P: Pipe>() {
    let world = World::<P>::with_config(up_config()).await;
    let keys: Vec<usize> = (0..10_000)
        .filter(|&i| {
            P::shard_of(&world.deferred, i, UP_FROM) == SOURCE_SHARD
                && P::shard_of(&world.deferred, i, UP_TO) == HELD_SHARD
        })
        .take(3)
        .collect();
    assert_eq!(keys.len(), 3);
    let parked_key = rows_on_count::<P>(&world.parked, UP_TO, HELD_SHARD, 0, 1)[0];
    for &i in &keys {
        let write = world
            .router
            .send_row(&world.deferred, i, WriteMode::Buffered);
        assert_eq!(write.await.expect("write task"), "ok");
    }
    world.reshard_to(UP_TO).await;
    // The late buffer's flush finds its view past the trust horizon and stays
    // closed until this re-read is released, inside the drain window.
    let prov = ravel_catalog::provisioning_key(&world.deferred.hash(), P::SIGNAL);
    let reread = world
        .fault
        .hold(Op::Get, Some(prov.clone()), Occurrence::Nth(1));
    let pin_hour = ACTIVATION_HOUR + DEFAULT_SCAN_SLACK_HOURS + 5;
    world.clock.set_ns(start_of(pin_hour) + 1_000_000_000);
    assert!(
        drive(&world.clock, || world.router.counters().stale == 1).await,
        "the late flush stays closed on the held re-read, counters {:?}",
        world.router.counters()
    );
    let held_gets = held(&reread, Op::Get, &prov);
    assert_eq!(held_gets.len(), 1, "the re-read is held");

    let parked_data = format!(
        "t/{}/{}/l0/",
        world.parked.hash().to_hex(),
        P::SIGNAL.key_prefix()
    );
    let put = world
        .fault
        .hold(Op::Put, Some(parked_data.clone()), Occurrence::Nth(1));
    let write = world
        .router
        .send_row(&world.parked, parked_key, WriteMode::Buffered);
    assert_eq!(write.await.expect("write task"), "ok");

    let World {
        router,
        store,
        clock,
        parked,
        deferred,
        ..
    } = world;
    let router = Arc::into_inner(router).expect("sole router owner");
    let counters = router.counter_source();
    let shutdown = tokio::spawn(router.shutdown());
    assert!(
        settle(|| !held(&put, Op::Put, &parked_data).is_empty()).await,
        "the 4-shard set's teardown flush reaches its data PUT"
    );
    assert!(
        !shutdown.is_finished(),
        "the 4-shard set is held in its teardown flush"
    );

    assert!(reread.release(held_gets[0]));
    assert!(
        drive(&clock, || {
            let c = counters();
            c.rerouted + c.hand_back_failures > 0
        })
        .await,
        "the late flush hands its rows up while the 4-shard set drains, counters {:?}",
        counters()
    );
    let held_puts = held(&put, Op::Put, &parked_data);
    assert_eq!(held_puts.len(), 1, "the teardown flush is still held");
    assert!(put.release(held_puts[0]));
    shutdown.await.expect("shutdown task");

    let store = store.as_ref();
    assert_eq!(
        scanned_rows::<P>(store, &deferred, H0..=pin_hour + 1).await,
        expected(&keys),
        "every acknowledged row of the late buffer is stored exactly once, counters {:?}",
        counters()
    );
    assert_eq!(
        scanned_rows::<P>(store, &parked, H0..=pin_hour + 1).await,
        expected(&[parked_key]),
        "the held teardown flush lands"
    );
    let c = counters();
    assert_eq!(c.rerouted, 0, "the draining set accepted nothing");
    assert_eq!(
        c.hand_back_failures, 1,
        "the closed mailbox refused the send"
    );
    assert_eq!(c.unscanned, 0);
    assert_eq!(
        c.mismatch_in_place, 1,
        "the source's teardown counts its in-place write"
    );
    assert_eq!(
        rows_at::<P>(store, &deferred, SOURCE_SHARD, pin_hour).await,
        expected(&keys),
        "the source's teardown wrote the rows in place"
    );
}

/// Issue #2429 on an increase, 3 shards to 4: a buffer routed by generation 0
/// on the 3-shard set is deferred until its flush would pin an hour the
/// 4-shard generation owns alone. Its rows are handed up to the 4-shard set,
/// a send that may not wait and here finds room, each lands at generation
/// 1's index for its key, and the scan rule finds every row exactly once.
///
/// Guard: the `Ok(Some(owner)) if owner != count` hand-back arm of
/// `GenerationSwitch::scan_check` (generation.rs). With it disabled the flush
/// writes in place under `COVERED_SHARD` in `pin_hour`.
async fn an_increase_hands_a_late_buffer_up_to_the_owner<P: Pipe>() {
    let world = World::<P>::with_config(up_config()).await;
    let keys: Vec<usize> = (0..10_000)
        .filter(|&i| {
            P::shard_of(&world.deferred, i, UP_FROM) == COVERED_SHARD
                && P::shard_of(&world.deferred, i, UP_TO) != COVERED_SHARD
        })
        .skip(1)
        .take(6)
        .collect();
    assert_eq!(keys.len(), 6);
    let mut by_target: std::collections::BTreeMap<u32, Vec<usize>> = Default::default();
    for &i in &keys {
        by_target
            .entry(P::shard_of(&world.deferred, i, UP_TO))
            .or_default()
            .push(i);
    }
    world.defer(COVERED_SHARD, &keys).await;
    world.reshard_to(UP_TO).await;

    let pin_hour = ACTIVATION_HOUR + DEFAULT_SCAN_SLACK_HOURS + 5;
    world.clock.set_ns(start_of(pin_hour) + 1_000_000_000);
    let generations = persisted_generations(world.store.as_ref(), &world.deferred, P::SIGNAL).await;
    assert_eq!(
        stable_generation_for_hour(&generations, pin_hour, DEFAULT_SCAN_SLACK_HOURS),
        Some(1),
        "the 4-shard generation owns the pinned hour alone"
    );
    let targets: Vec<u32> = std::iter::once(COVERED_SHARD)
        .chain(by_target.keys().copied())
        .collect();
    wait_for_commit(&world, &targets, pin_hour).await;
    world.router.flush().await;

    let store = world.store.as_ref();
    assert_eq!(
        rows_at::<P>(store, &world.deferred, COVERED_SHARD, pin_hour).await,
        Vec::<i64>::new(),
        "nothing routed by the 3-shard generation is written under shard {COVERED_SHARD} \
         in hour {pin_hour}; counters {:?}",
        world.router.counters()
    );
    let counters = world.router.counters();
    assert_eq!(counters.rerouted, 1, "the flush is handed up once");
    assert_eq!(counters.rerouted_generation_mismatch, 1);
    assert_eq!(counters.mismatch_in_place, 0);
    for (&target, target_keys) in &by_target {
        assert_eq!(
            rows_at::<P>(store, &world.deferred, target, pin_hour).await,
            expected(target_keys),
            "the rows the 4-shard generation routes to shard {target} land there"
        );
    }
    assert_eq!(
        scanned_rows::<P>(store, &world.deferred, H0..=pin_hour + 1).await,
        expected(&keys),
        "the scan rule finds every row exactly once"
    );
}

/// A strict waiter whose buffer is handed back for a generation mismatch is
/// answered with the retryable outcome-unknown `Abandoned` carrying the
/// mismatch message, never an `Ok`, and its row is still written once.
///
/// Guard: the `Ok(Some(owner)) if owner != count` hand-back arm of
/// `GenerationSwitch::scan_check`. With it disabled the flush writes in place
/// and acknowledges the waiter `Ok`.
async fn strict_waiter_on_a_mismatched_buffer_is_abandoned<P: Pipe>() {
    let world = World::<P>::new().await;
    let keys = covered_rows::<P>(&world.deferred, 1);
    let handle = world
        .router
        .send_row(&world.deferred, keys[0], WriteMode::Strict);
    assert!(
        settle(|| world.router.counters().buffered == 1).await,
        "the strict row is buffered on the 4-shard set before the reshard"
    );
    world.reshard_down().await;
    let pin_hour = ACTIVATION_HOUR + DEFAULT_SCAN_SLACK_HOURS + 5;
    world.clock.set_ns(start_of(pin_hour) + 1_000_000_000);
    assert!(
        drive(&world.clock, || handle.is_finished()).await,
        "the strict write is answered"
    );
    let answer = handle.await.expect("strict write task");
    assert!(
        answer.contains("Abandoned") && answer.contains("another generation owns"),
        "the waiter gets the outcome-unknown Abandoned for a mismatch, got {answer}"
    );
    let counters = world.router.counters();
    assert_eq!(counters.rerouted, 1);
    assert_eq!(counters.rerouted_generation_mismatch, 1);
    world.router.flush().await;
    assert_eq!(
        scanned_rows::<P>(world.store.as_ref(), &world.deferred, H0..=pin_hour + 1).await,
        expected(&keys),
        "the abandoned waiter's row is written once"
    );
}

/// Collects the message of every WARN event on this thread.
#[derive(Clone, Default)]
struct WarnCapture {
    messages: Arc<Mutex<Vec<String>>>,
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for WarnCapture {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if *event.metadata().level() != tracing::Level::WARN {
            return;
        }
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);
        self.messages.lock().expect("lock").push(visitor.0);
    }
}

#[derive(Default)]
struct MessageVisitor(String);

impl tracing::field::Visit for MessageVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}

impl WarnCapture {
    fn count(&self, needle: &str) -> usize {
        self.messages
            .lock()
            .expect("lock")
            .iter()
            .filter(|m| m.contains(needle))
            .count()
    }
}

/// A teardown whose generation-mismatch hand-back finds its target set
/// already drained writes the rows in place, where readers find them, logs a
/// WARN and counts the write once on the in-place counter, not on the
/// unscanned one.
///
/// Guard: the `!enforced || self.mismatch_in_place` in-place branch of each
/// actor's hand-back arm in `flush_tenant`. Without it the teardown takes the
/// unscanned-write branch: no WARN, the in-place counter reads 0 and the
/// unscanned counter 1.
async fn a_teardown_writes_a_mismatch_in_place_when_its_target_is_gone<P: Pipe>() {
    let capture = WarnCapture::default();
    let _default =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(capture.clone()));
    // Not `drain_only_config`: its day-long age clocks spend the whole
    // read-side slack, so its deferral cap is 0 and the bounded retry, not
    // the teardown, would write the rows in place.
    let world = World::<P>::with_config(up_config()).await;
    let keys: Vec<usize> = (0..10_000)
        .filter(|&i| {
            P::shard_of(&world.deferred, i, UP_FROM) == SOURCE_SHARD
                && P::shard_of(&world.deferred, i, UP_TO) != SOURCE_SHARD
        })
        .take(3)
        .collect();
    for &i in &keys {
        let write = world
            .router
            .send_row(&world.deferred, i, WriteMode::Buffered);
        assert_eq!(write.await.expect("write task"), "ok");
    }
    world.reshard_to(UP_TO).await;
    // One clock move: the late buffer's age trigger fires once, finds its
    // view past the trust horizon and stays closed. Its first hand-back is
    // tried by the teardown, after the 4-shard set has closed.
    let pin_hour = ACTIVATION_HOUR + DEFAULT_SCAN_SLACK_HOURS + 5;
    world.clock.set_ns(start_of(pin_hour) + 1_000_000_000);
    assert!(
        settle(|| world.router.counters().stale == 1).await,
        "the late flush stays closed on an untrusted view"
    );
    // A write after the activation builds the 4-shard set, which shutdown
    // drains, and closes, before the 3-shard set.
    let other = world.router.send_row(&world.parked, 0, WriteMode::Buffered);
    assert_eq!(other.await.expect("write task"), "ok");

    let World {
        router,
        store,
        deferred,
        ..
    } = world;
    let router = Arc::into_inner(router).expect("sole router owner");
    let counters = router.counter_source();
    router.shutdown().await;

    let c = counters();
    assert_eq!(
        c.mismatch_in_place, 1,
        "one buffer written in place, counters {c:?}"
    );
    assert_eq!(c.unscanned, 0);
    assert_eq!(c.rerouted, 0);
    assert_eq!(
        capture.count("writing a flush in place in an ingest hour another shard generation owns"),
        1,
        "the in-place write is logged once, {:?}",
        capture.messages.lock().expect("lock")
    );
    let store = store.as_ref();
    assert_eq!(
        rows_at::<P>(store, &deferred, SOURCE_SHARD, pin_hour).await,
        expected(&keys),
        "written in place under the source index"
    );
    assert_eq!(
        scanned_rows::<P>(store, &deferred, H0..=pin_hour + 1).await,
        expected(&keys),
        "the scan rule finds every row exactly once"
    );
}

macro_rules! scan_set_cases {
    ($pipe:ty, $($name:ident => $case:ident),+ $(,)?) => {
        $(
            #[tokio::test]
            async fn $name() {
                $case::<$pipe>().await;
            }
        )+
    };
}

scan_set_cases!(
    IngestRouter,
    metrics_retired_index_past_the_window_hands_back => retired_index_past_the_window_hands_back,
    metrics_retired_index_inside_the_window_writes_in_place => retired_index_inside_the_window_writes_in_place,
    metrics_covered_index_in_a_successor_owned_hour_hands_back => covered_index_in_a_successor_owned_hour_hands_back,
    metrics_covered_index_inside_the_overlap_writes_in_place => covered_index_inside_the_overlap_writes_in_place,
    metrics_unrefreshable_view_keeps_the_flush_closed => unrefreshable_view_keeps_the_flush_closed,
    metrics_charges_follow_the_rows => charges_follow_the_rows,
    metrics_strict_waiter_on_a_handed_back_buffer_is_abandoned => strict_waiter_on_a_handed_back_buffer_is_abandoned,
    metrics_one_drain_hands_back_and_flushes => one_drain_hands_back_and_flushes,
    metrics_shutdown_drains_the_largest_set_first => shutdown_drains_the_largest_set_first,
    metrics_shutdown_drains_a_set_a_hand_back_built => shutdown_drains_a_set_a_hand_back_built,
    metrics_a_hand_back_to_a_draining_set_is_never_lost => a_hand_back_to_a_draining_set_is_never_lost,
    metrics_an_increase_hands_a_late_buffer_up_to_the_owner => an_increase_hands_a_late_buffer_up_to_the_owner,
    metrics_strict_waiter_on_a_mismatched_buffer_is_abandoned => strict_waiter_on_a_mismatched_buffer_is_abandoned,
    metrics_a_teardown_writes_a_mismatch_in_place_when_its_target_is_gone => a_teardown_writes_a_mismatch_in_place_when_its_target_is_gone,
);

scan_set_cases!(
    LogIngestRouter,
    logs_retired_index_past_the_window_hands_back => retired_index_past_the_window_hands_back,
    logs_retired_index_inside_the_window_writes_in_place => retired_index_inside_the_window_writes_in_place,
    logs_covered_index_in_a_successor_owned_hour_hands_back => covered_index_in_a_successor_owned_hour_hands_back,
    logs_covered_index_inside_the_overlap_writes_in_place => covered_index_inside_the_overlap_writes_in_place,
    logs_unrefreshable_view_keeps_the_flush_closed => unrefreshable_view_keeps_the_flush_closed,
    logs_charges_follow_the_rows => charges_follow_the_rows,
    logs_strict_waiter_on_a_handed_back_buffer_is_abandoned => strict_waiter_on_a_handed_back_buffer_is_abandoned,
    logs_one_drain_hands_back_and_flushes => one_drain_hands_back_and_flushes,
    logs_shutdown_drains_the_largest_set_first => shutdown_drains_the_largest_set_first,
    logs_shutdown_drains_a_set_a_hand_back_built => shutdown_drains_a_set_a_hand_back_built,
    logs_a_hand_back_to_a_draining_set_is_never_lost => a_hand_back_to_a_draining_set_is_never_lost,
    logs_an_increase_hands_a_late_buffer_up_to_the_owner => an_increase_hands_a_late_buffer_up_to_the_owner,
    logs_strict_waiter_on_a_mismatched_buffer_is_abandoned => strict_waiter_on_a_mismatched_buffer_is_abandoned,
    logs_a_teardown_writes_a_mismatch_in_place_when_its_target_is_gone => a_teardown_writes_a_mismatch_in_place_when_its_target_is_gone,
);

scan_set_cases!(
    SpanIngestRouter,
    spans_retired_index_past_the_window_hands_back => retired_index_past_the_window_hands_back,
    spans_retired_index_inside_the_window_writes_in_place => retired_index_inside_the_window_writes_in_place,
    spans_covered_index_in_a_successor_owned_hour_hands_back => covered_index_in_a_successor_owned_hour_hands_back,
    spans_covered_index_inside_the_overlap_writes_in_place => covered_index_inside_the_overlap_writes_in_place,
    spans_unrefreshable_view_keeps_the_flush_closed => unrefreshable_view_keeps_the_flush_closed,
    spans_charges_follow_the_rows => charges_follow_the_rows,
    spans_strict_waiter_on_a_handed_back_buffer_is_abandoned => strict_waiter_on_a_handed_back_buffer_is_abandoned,
    spans_one_drain_hands_back_and_flushes => one_drain_hands_back_and_flushes,
    spans_shutdown_drains_the_largest_set_first => shutdown_drains_the_largest_set_first,
    spans_shutdown_drains_a_set_a_hand_back_built => shutdown_drains_a_set_a_hand_back_built,
    spans_a_hand_back_to_a_draining_set_is_never_lost => a_hand_back_to_a_draining_set_is_never_lost,
    spans_an_increase_hands_a_late_buffer_up_to_the_owner => an_increase_hands_a_late_buffer_up_to_the_owner,
    spans_strict_waiter_on_a_mismatched_buffer_is_abandoned => strict_waiter_on_a_mismatched_buffer_is_abandoned,
    spans_a_teardown_writes_a_mismatch_in_place_when_its_target_is_gone => a_teardown_writes_a_mismatch_in_place_when_its_target_is_gone,
);
