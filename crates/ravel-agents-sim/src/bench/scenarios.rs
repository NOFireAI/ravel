//! The scenarios. Each one builds its own [`BenchStore`] and paused
//! runtime, sets up its agents with latency off, then measures its
//! operations with latency on.
//!
//! Temperatures: `warm` runs every operation on one coordinator that lives
//! for the whole scenario; `cold` builds a fresh coordinator (and agents
//! store client) for every iteration, as after a process restart. The
//! agents are always created by a separate setup coordinator, so neither
//! starts with them loaded.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use ravel_agents_core::head::RecoveryPolicy;
use ravel_agents_core::identity::{agent_id, step_input_hash};
use ravel_agents_core::kernel::DerivedIds;
use ravel_agents_core::keys::{AgentKey, AgentKind, tenant_prefix};
use ravel_agents_core::wire::{
    AgentRef, CreateAgentRequest, SendRequest, TimerRequest, TurnAssignment, TurnOutcome,
    WorkerCommitRequest, WorkerPollRequest,
};
use ravel_agents_runtime::{
    Coordinator, DEFAULT_SWEEP_MAX_HEAD_GETS, Dispatch, EffectDispatcher, Rng, RuntimeConfig,
    StepProgress, StepSpec, SweepConfig,
};
use ravel_agents_store::backoff::NoBackoff;
use ravel_agents_store::counters::{CounterSnapshot, PhaseCounts};
use ravel_agents_store::{AgentStore, StoreLimits};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions};
use serde_json::{Value, json};
use tokio::time::Instant;

use super::figures::{Figure, HEAP_LIVE_DELTA, Key, OBJECT_BODY};
use super::store::{BenchStore, Speed, TELEMETRY_PREFIX, Tally, TokioClock, heap};
use super::{Config, DISCOVERY, SCENARIOS, SEED, Size};
use crate::drive::{GRACE_MS, HANDLER, LEASE_MS, PAST_LEASE_MS, T0_NS, TENANT, pin, turn_values};

/// Agent kind of every benchmark agent.
const KIND: &str = "bench";
/// The storm's LIST page and sweep, as T22 (`schedules::storm`).
const STORM_PAGE: usize = 4;
const STORM_SWEEP: SweepConfig = SweepConfig {
    interval_ms: u64::MAX,
    max_list_pages: 4,
    max_head_gets: 8,
};
/// Directory HEAD reads one default sweep pass makes when no wake hint takes
/// any: the directory half also gets the reads the wake half left unused.
pub const DIRECTORY_GETS_PER_PASS: usize = DEFAULT_SWEEP_MAX_HEAD_GETS as usize;
/// Directory HEAD reads one default sweep pass makes however many wake hints
/// are due: the directory half's own share of the budget.
pub const GUARANTEED_DIRECTORY_GETS_PER_PASS: usize =
    (DEFAULT_SWEEP_MAX_HEAD_GETS - DEFAULT_SWEEP_MAX_HEAD_GETS / 2) as usize;

fn err(context: &str) -> impl Fn(ravel_agents_core::error::AgentError) -> String + '_ {
    move |e| format!("{context}: {:?} {}", e.kind, e.reason)
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn as_f64(n: u64) -> f64 {
    // Counts here stay far below 2^53.
    n as f64
}

fn agent_ref(scenario: &str, i: usize) -> AgentRef {
    AgentRef {
        kind: KIND.to_string(),
        key: format!("{scenario}-{SEED}-{i:05}"),
    }
}

fn agent_hex(agent: &AgentRef) -> Result<String, String> {
    let kind = AgentKind::new(agent.kind.clone()).map_err(|e| format!("kind: {e:?}"))?;
    let key = AgentKey::new(agent.key.clone()).map_err(|e| format!("key: {e:?}"))?;
    Ok(agent_id(&kind, &key).to_hex())
}

/// The coordinator settings for the measured coordinators: the production
/// defaults.
fn bench_config() -> RuntimeConfig {
    RuntimeConfig::default()
}

/// T22's recovering coordinator: the sim's short leases, the production
/// idle unload.
fn storm_config() -> RuntimeConfig {
    RuntimeConfig {
        lease_ms: LEASE_MS,
        heartbeat_grace_ms: GRACE_MS,
        ..RuntimeConfig::default()
    }
}

/// Counts up from its seed, so worker ids are fixed per coordinator.
#[derive(Debug)]
struct SeededRng(AtomicU64);

impl Rng for SeededRng {
    fn next_u64(&self) -> u64 {
        self.0.fetch_add(1, Ordering::SeqCst)
    }
}

/// One process: an agents store client and a coordinator.
struct Node {
    store: Arc<AgentStore>,
    co: Arc<Coordinator>,
    worker: String,
}

struct World {
    bench: Arc<BenchStore>,
    clock: Arc<TokioClock>,
    limits: StoreLimits,
    seeds: u64,
}

impl World {
    /// Call inside the paused runtime: the clock starts at its instant.
    fn new(memory: MemoryStore, speed: Speed, limits: StoreLimits) -> Result<World, String> {
        let prefix = tenant_prefix(&TENANT).map_err(|e| format!("tenant prefix: {e:?}"))?;
        let agents_prefix = format!("{}ag/", prefix.trim_end_matches('/').to_string() + "/");
        Ok(World {
            bench: Arc::new(BenchStore::new(memory, speed, agents_prefix)),
            clock: Arc::new(TokioClock::new()),
            limits,
            seeds: 0,
        })
    }

    fn node(&mut self, config: RuntimeConfig, sweep: Option<SweepConfig>) -> Node {
        self.seeds += 1;
        let seed = (SEED << 32) + (self.seeds << 20);
        let backend: Arc<dyn ObjectStoreBackend> = self.bench.clone();
        let store = Arc::new(
            AgentStore::new(
                backend,
                TENANT,
                self.limits,
                self.clock.clone(),
                Arc::new(DerivedIds),
            )
            .with_backoff(Arc::new(NoBackoff)),
        );
        let co = Coordinator::new(
            store.clone(),
            config,
            self.clock.clone(),
            Arc::new(SeededRng(AtomicU64::new(seed))),
        );
        let co = Arc::new(match sweep {
            Some(sweep) => co.with_sweep(sweep),
            None => co,
        });
        let worker = co
            .register_worker(&[pin()])
            .unwrap_or_else(|_| format!("w-{seed}"));
        Node { store, co, worker }
    }
}

/// What moved over one stretch of a run.
#[derive(Debug, Clone, Copy, Default)]
struct Totals {
    counters: CounterSnapshot,
    tally: Tally,
}

fn add_phase(a: PhaseCounts, b: PhaseCounts) -> PhaseCounts {
    PhaseCounts {
        get: a.get + b.get,
        put: a.put + b.put,
        cas: a.cas + b.cas,
        list: a.list + b.list,
        conflict: a.conflict + b.conflict,
        bytes_in: a.bytes_in + b.bytes_in,
        bytes_out: a.bytes_out + b.bytes_out,
    }
}

fn add_tally(a: Tally, b: Tally) -> Tally {
    Tally {
        head_gets: a.head_gets + b.head_gets,
        deletes: a.deletes + b.deletes,
        telemetry_puts: a.telemetry_puts + b.telemetry_puts,
        telemetry_gets: a.telemetry_gets + b.telemetry_gets,
        telemetry_out: a.telemetry_out + b.telemetry_out,
        telemetry_in: a.telemetry_in + b.telemetry_in,
        agents_requests: a.agents_requests + b.agents_requests,
        watched_requests: a.watched_requests + b.watched_requests,
    }
}

impl Totals {
    fn add(self, other: Totals) -> Totals {
        let (a, b) = (self.counters, other.counters);
        Totals {
            counters: CounterSnapshot {
                resolve: add_phase(a.resolve, b.resolve),
                build: add_phase(a.build, b.build),
                store: add_phase(a.store, b.store),
                publish: add_phase(a.publish, b.publish),
                planner_misses: a.planner_misses + b.planner_misses,
            },
            tally: add_tally(self.tally, other.tally),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Mark {
    counters: CounterSnapshot,
    tally: Tally,
    at: Instant,
    loaded: u64,
}

/// One measured stretch.
#[derive(Debug, Clone, Copy)]
struct Part {
    totals: Totals,
    ms: f64,
    activated: bool,
}

#[derive(Debug, Default)]
struct OpAcc {
    count: u64,
    totals: Totals,
    latencies: Vec<f64>,
}

/// Accumulates one scenario-temperature's operations.
struct Meter {
    bench: Arc<BenchStore>,
    /// Record an `activation` for every operation during which the
    /// coordinator loaded an agent.
    auto_activation: bool,
    ops: BTreeMap<&'static str, OpAcc>,
}

impl Meter {
    fn new(bench: Arc<BenchStore>, auto_activation: bool) -> Meter {
        Meter {
            bench,
            auto_activation,
            ops: BTreeMap::new(),
        }
    }

    fn start(&self, node: Option<&Node>) -> Mark {
        heap::sample();
        Mark {
            counters: node.map(|n| n.store.counters()).unwrap_or_default(),
            tally: self.bench.tally(),
            at: Instant::now(),
            loaded: node.map(|n| n.co.metrics().agents_loaded).unwrap_or(0),
        }
    }

    fn part(&self, node: Option<&Node>, before: Mark) -> Part {
        let after = self.start(node);
        Part {
            totals: Totals {
                counters: after.counters.since(&before.counters),
                tally: after.tally.since(&before.tally),
            },
            ms: after.at.duration_since(before.at).as_secs_f64() * 1000.0,
            activated: after.loaded > before.loaded,
        }
    }

    /// Record one operation made of `parts`.
    fn record(&mut self, op: &'static str, parts: &[Part]) {
        let totals = parts
            .iter()
            .fold(Totals::default(), |acc, p| acc.add(p.totals));
        let ms: f64 = parts.iter().map(|p| p.ms).sum();
        let acc = self.ops.entry(op).or_default();
        acc.count += 1;
        acc.totals = acc.totals.add(totals);
        acc.latencies.push(ms);
        if self.auto_activation && parts.iter().any(|p| p.activated) {
            let acc = self.ops.entry("activation").or_default();
            acc.count += 1;
            acc.totals = acc.totals.add(totals);
            acc.latencies.push(ms);
        }
    }

    /// Record `latencies.len()` concurrent operations whose combined cost
    /// is `part`; each carries an equal share.
    fn record_group(
        &mut self,
        op: &'static str,
        part: Part,
        latencies: &[f64],
    ) -> Result<(), String> {
        if part.activated {
            return Err(format!("a concurrent {op} group loaded an agent"));
        }
        let acc = self.ops.entry(op).or_default();
        acc.count += latencies.len() as u64;
        acc.totals = acc.totals.add(part.totals);
        acc.latencies.extend_from_slice(latencies);
        Ok(())
    }
}

/// Nearest-rank percentile of `sorted`.
pub fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = (p * sorted.len() as f64).ceil().max(1.0) as usize;
    sorted[rank.min(sorted.len()) - 1]
}

fn phase_figures(
    out: &mut Vec<Figure>,
    key: impl Fn(&str, &str) -> Key,
    name: &str,
    p: PhaseCounts,
    n: f64,
) {
    let per = |v: u64| as_f64(v) / n;
    for (metric, value) in [
        ("get", p.get),
        ("put", p.put),
        ("cas", p.cas),
        ("list", p.list),
        ("conflict", p.conflict),
    ] {
        out.push(Figure::new(
            key(name, metric),
            per(value),
            "requests/op",
            None,
        ));
    }
    out.push(Figure::new(
        key(name, "body_in_bytes"),
        per(p.bytes_in),
        "bytes/op",
        Some(OBJECT_BODY),
    ));
    out.push(Figure::new(
        key(name, "body_out_bytes"),
        per(p.bytes_out),
        "bytes/op",
        Some(OBJECT_BODY),
    ));
}

impl Meter {
    /// The figures of every recorded operation, under `scenario.temp`.
    fn figures(&self, scenario: &str, temp: &str) -> Result<Vec<Figure>, String> {
        let mut out = Vec::new();
        for (op, acc) in &self.ops {
            if acc.count == 0 {
                continue;
            }
            let key = |phase: &str, metric: &str| Key::new(scenario, temp, op, phase, metric);
            let n = as_f64(acc.count);
            let c = acc.totals.counters;
            let t = acc.totals.tally;
            if c.build.requests() > 0 || c.build.conflict > 0 {
                return Err(format!(
                    "{scenario}.{temp}.{op}: the build phase issued requests"
                ));
            }
            out.push(Figure::new(key("ops", "count"), n, "ops", None));
            if *op == "ingest" {
                for (metric, value) in [("put", t.telemetry_puts), ("get", t.telemetry_gets)] {
                    out.push(Figure::new(
                        key("telemetry", metric),
                        as_f64(value) / n,
                        "requests/op",
                        None,
                    ));
                }
                out.push(Figure::new(
                    key("telemetry", "body_in_bytes"),
                    as_f64(t.telemetry_in) / n,
                    "bytes/op",
                    Some(OBJECT_BODY),
                ));
                out.push(Figure::new(
                    key("telemetry", "body_out_bytes"),
                    as_f64(t.telemetry_out) / n,
                    "bytes/op",
                    Some(OBJECT_BODY),
                ));
            } else {
                phase_figures(&mut out, key, "resolve", c.resolve, n);
                phase_figures(&mut out, key, "store", c.store, n);
                phase_figures(&mut out, key, "publish", c.publish, n);
                let page_gets = c.resolve.get.saturating_sub(t.head_gets);
                out.push(Figure::new(
                    key("index", "page_gets"),
                    as_f64(page_gets) / n,
                    "requests/op",
                    None,
                ));
                out.push(Figure::new(
                    key("index", "planner_misses"),
                    as_f64(c.planner_misses) / n,
                    "misses/op",
                    None,
                ));
                out.push(Figure::new(
                    key("other", "delete"),
                    as_f64(t.deletes) / n,
                    "requests/op",
                    None,
                ));
            }
            let mut sorted = acc.latencies.clone();
            sorted.sort_by(f64::total_cmp);
            if *op == "assign" {
                out.push(Figure::new(
                    key("latency", "delay_ms"),
                    sorted.last().copied().unwrap_or(0.0),
                    "ms",
                    None,
                ));
            } else {
                for (metric, p) in [("p50_ms", 0.50), ("p99_ms", 0.99), ("max_ms", 1.0)] {
                    out.push(Figure::new(
                        key("latency", metric),
                        percentile(&sorted, p),
                        "ms",
                        None,
                    ));
                }
            }
        }
        Ok(out)
    }
}

fn heap_figure(scenario: &str, temp: &str) -> Option<Figure> {
    heap::peak_rise().map(|rise| {
        Figure::new(
            Key::new(scenario, temp, "run", "heap", "peak_bytes"),
            as_f64(rise),
            "bytes",
            Some(HEAP_LIVE_DELTA),
        )
    })
}

fn paused_runtime() -> Result<tokio::runtime::Runtime, String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .map_err(|e| format!("runtime: {e}"))
}

/// Remembers the attempt `run_step` dispatched.
#[derive(Default)]
struct Catch(Mutex<Option<Dispatch>>);

impl EffectDispatcher for Catch {
    fn dispatch(&self, dispatch: &Dispatch) {
        *lock(&self.0) = Some(dispatch.clone());
    }
}

fn step_spec() -> StepSpec {
    let input = serde_json::to_vec(&json!({ "amount": 5 })).unwrap_or_default();
    StepSpec {
        step_key: "pay".to_string(),
        tool_name: "pay".to_string(),
        tool_version: "1".to_string(),
        policy: RecoveryPolicy::AtLeastOnce,
        input_hash: step_input_hash("pay", "1", &input),
    }
}

async fn create(node: &Node, agent: &AgentRef) -> Result<(), String> {
    node.co
        .create(&CreateAgentRequest {
            agent: agent.clone(),
        })
        .await
        .map(|_| ())
        .map_err(err("create"))
}

/// Objects are content-addressed per agent, so the payload carries the
/// request id: two sends of one body would share an object and the second
/// would write nothing.
fn send_request(agent: &AgentRef, request_id: String) -> SendRequest {
    SendRequest {
        agent: agent.clone(),
        handler: HANDLER.to_string(),
        payload: json!({ "request": request_id }),
        request_id,
    }
}

fn poll_request(node: &Node) -> WorkerPollRequest {
    WorkerPollRequest {
        worker_id: node.worker.clone(),
        handlers: vec![pin()],
        wait_ms: 0,
    }
}

fn completion(a: &TurnAssignment) -> WorkerCommitRequest {
    let (state, output) = turn_values(a);
    WorkerCommitRequest {
        lease: a.lease.clone(),
        outcome: TurnOutcome::Completed {
            state,
            output,
            events: Vec::new(),
            timers: Vec::new(),
            intents: Vec::new(),
        },
    }
}

/// Unmeasured: send, then poll and commit through `node`.
async fn finished_turn(node: &Node, agent: &AgentRef) -> Result<(), String> {
    node.co
        .send(&send_request(agent, "setup".to_string()))
        .await
        .map_err(err("setup send"))?;
    let a = node
        .co
        .poll(&poll_request(node), Instant::now())
        .await
        .map_err(err("setup poll"))?
        .ok_or("setup poll found no turn")?;
    node.co
        .commit(&completion(&a))
        .await
        .map_err(err("setup commit"))?;
    Ok(())
}

/// A measured `message`.
async fn send(
    meter: &mut Meter,
    node: &Node,
    agent: &AgentRef,
    request_id: String,
) -> Result<(), String> {
    let m = meter.start(Some(node));
    node.co
        .send(&send_request(agent, request_id))
        .await
        .map_err(err("send"))?;
    let part = meter.part(Some(node), m);
    meter.record("message", &[part]);
    Ok(())
}

/// Measured concurrent `message`s on one agent.
async fn send_group(
    meter: &mut Meter,
    node: &Node,
    agent: &AgentRef,
    request_ids: Vec<String>,
) -> Result<(), String> {
    let m = meter.start(Some(node));
    let sends = request_ids.into_iter().map(|id| {
        let req = send_request(agent, id);
        let co = node.co.clone();
        async move {
            let at = Instant::now();
            let out = co.send(&req).await;
            (out, at.elapsed().as_secs_f64() * 1000.0)
        }
    });
    let results = futures::future::join_all(sends).await;
    let part = meter.part(Some(node), m);
    let mut latencies = Vec::new();
    for (out, ms) in results {
        out.map_err(err("concurrent send"))?;
        latencies.push(ms);
    }
    meter.record_group("message", part, &latencies)
}

/// A measured `step`: dispatch, the tool's result, the replay.
async fn step(meter: &mut Meter, node: &Node, a: &TurnAssignment) -> Result<(), String> {
    let spec = step_spec();
    let catch = Catch::default();
    let m = meter.start(Some(node));
    let progress = node
        .co
        .run_step(&a.lease, &spec, &catch)
        .await
        .map_err(err("run_step"))?;
    let StepProgress::Dispatched(d) = progress else {
        return Err(format!("first run_step answered {progress:?}"));
    };
    node.co
        .complete_effect(&d.effect, d.attempt, &json!({ "paid": 5 }))
        .await
        .map_err(err("complete_effect"))?;
    let replay = node
        .co
        .run_step(&a.lease, &spec, &catch)
        .await
        .map_err(err("replay run_step"))?;
    if !matches!(replay, StepProgress::Completed { .. }) {
        return Err(format!("replay answered {replay:?}"));
    }
    let part = meter.part(Some(node), m);
    meter.record("step", &[part]);
    Ok(())
}

/// A measured `turn` (the assigning poll plus the commit), with a measured
/// `step` between them when `with_step`.
async fn turn(meter: &mut Meter, node: &Node, with_step: bool) -> Result<(), String> {
    let m = meter.start(Some(node));
    let a = node
        .co
        .poll(&poll_request(node), Instant::now())
        .await
        .map_err(err("poll"))?
        .ok_or("poll found no turn")?;
    let poll = meter.part(Some(node), m);
    if with_step {
        step(meter, node, &a).await?;
    }
    let m = meter.start(Some(node));
    node.co
        .commit(&completion(&a))
        .await
        .map_err(err("commit"))?;
    let commit = meter.part(Some(node), m);
    meter.record("turn", &[poll, commit]);
    Ok(())
}

/// A measured telemetry `ingest`: one batch PUT and its read-back.
async fn ingest(meter: &mut Meter, bench: &BenchStore, n: usize) -> Result<(), String> {
    let key = format!("{TELEMETRY_PREFIX}ingest/{n:06}");
    let batch = Bytes::from(format!("batch {n}"));
    let m = meter.start(None);
    bench
        .put(&key, batch.clone(), PutOptions::default())
        .await
        .map_err(|e| format!("ingest put: {e}"))?;
    let read = bench
        .get(&key, GetRange::Full)
        .await
        .map_err(|e| format!("ingest get: {e}"))?;
    if read.data != batch {
        return Err("ingest read back other bytes".to_string());
    }
    let part = meter.part(None, m);
    meter.record("ingest", &[part]);
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Temp {
    Warm,
    Cold,
}

impl Temp {
    fn name(self) -> &'static str {
        match self {
            Temp::Warm => "warm",
            Temp::Cold => "cold",
        }
    }
}

const TEMPS: [Temp; 2] = [Temp::Warm, Temp::Cold];

/// The coordinator an iteration runs on: the long-lived one when warm, a
/// fresh one when cold.
fn iteration_node<'a>(
    world: &mut World,
    temp: Temp,
    warm: &'a Node,
    fresh: &'a mut Option<Node>,
) -> &'a Node {
    match temp {
        Temp::Warm => warm,
        Temp::Cold => fresh.insert(world.node(bench_config(), None)),
    }
}

/// One hot agent: rounds of a lead send, three concurrent sends, then four
/// turns with a step each.
fn hot(scenario: &str, rounds: usize, speed: Speed, temp: Temp) -> Result<Vec<Figure>, String> {
    heap::reset();
    let runtime = paused_runtime()?;
    runtime.block_on(async {
        let mut world = World::new(MemoryStore::new(), speed, StoreLimits::default())?;
        let setup = world.node(bench_config(), None);
        let agent = agent_ref(scenario, 0);
        create(&setup, &agent).await?;
        let warm = world.node(bench_config(), None);
        let mut meter = Meter::new(world.bench.clone(), true);
        world.bench.set_latency(true);
        let mut fresh = None;
        for round in 0..rounds {
            let node = iteration_node(&mut world, temp, &warm, &mut fresh);
            send(&mut meter, node, &agent, format!("r{round}-lead")).await?;
            let ids = (0..3).map(|i| format!("r{round}-c{i}")).collect();
            send_group(&mut meter, node, &agent, ids).await?;
            for _ in 0..4 {
                turn(&mut meter, node, true).await?;
            }
        }
        world.bench.set_latency(false);
        let mut figures = meter.figures(scenario, temp.name())?;
        figures.extend(heap_figure(scenario, temp.name()));
        Ok(figures)
    })
}

/// `agents` agents, each sent two messages, each turned with a step. With
/// `dormant` more agents set up first, each with one finished turn and
/// never touched again; the requests to their keys are reported.
fn population(
    scenario: &str,
    agents: usize,
    dormant: usize,
    temp: Temp,
) -> Result<Vec<Figure>, String> {
    heap::reset();
    let runtime = paused_runtime()?;
    runtime.block_on(async {
        let mut world = World::new(MemoryStore::new(), Speed::Nominal, StoreLimits::default())?;
        let setup = world.node(bench_config(), None);
        let mut sleepers = Vec::new();
        for i in 0..dormant {
            let agent = agent_ref(&format!("{scenario}-z"), i);
            create(&setup, &agent).await?;
            finished_turn(&setup, &agent).await?;
            sleepers.push(agent_hex(&agent)?);
        }
        let active: Vec<AgentRef> = (0..agents).map(|i| agent_ref(scenario, i)).collect();
        for agent in &active {
            create(&setup, agent).await?;
        }
        world.bench.watch(sleepers);
        let warm = world.node(bench_config(), None);
        let mut meter = Meter::new(world.bench.clone(), true);
        let before = world.bench.tally();
        world.bench.set_latency(true);
        let mut fresh = None;
        for round in 0..2 {
            for agent in &active {
                let node = iteration_node(&mut world, temp, &warm, &mut fresh);
                send(&mut meter, node, agent, format!("m{round}")).await?;
                turn(&mut meter, node, true).await?;
            }
        }
        world.bench.set_latency(false);
        let moved = world.bench.tally().since(&before);
        let mut figures = meter.figures(scenario, temp.name())?;
        if dormant > 0 {
            figures.push(Figure::new(
                Key::new(
                    scenario,
                    temp.name(),
                    "run",
                    "isolation",
                    "dormant_requests",
                ),
                as_f64(moved.watched_requests),
                "requests",
                None,
            ));
        }
        figures.extend(heap_figure(scenario, temp.name()));
        Ok(figures)
    })
}

/// One agent's index growing over `messages` sends and turns, sealing a
/// page every `page_entries` entries.
fn index(
    scenario: &str,
    messages: usize,
    page_entries: usize,
    temp: Temp,
) -> Result<Vec<Figure>, String> {
    heap::reset();
    let runtime = paused_runtime()?;
    runtime.block_on(async {
        let mut limits = StoreLimits::default();
        limits.kernel.max_page_entries = page_entries;
        let mut world = World::new(MemoryStore::new(), Speed::Nominal, limits)?;
        let setup = world.node(bench_config(), None);
        let agent = agent_ref(scenario, 0);
        create(&setup, &agent).await?;
        let warm = world.node(bench_config(), None);
        let mut meter = Meter::new(world.bench.clone(), true);
        world.bench.set_latency(true);
        let mut fresh = None;
        for i in 0..messages {
            let node = iteration_node(&mut world, temp, &warm, &mut fresh);
            send(&mut meter, node, &agent, format!("i{i}")).await?;
            turn(&mut meter, node, false).await?;
        }
        world.bench.set_latency(false);
        let mut figures = meter.figures(scenario, temp.name())?;
        figures.extend(heap_figure(scenario, temp.name()));
        Ok(figures)
    })
}

/// T22's recovery storm: `agents` agents mid-turn under a dead worker,
/// recovered by one coordinator through `sweep_once` passes and lease
/// takeovers, an ingest iteration after every pass and every takeover.
fn storm(agents: usize) -> Result<Vec<Figure>, String> {
    const SCENARIO: &str = "storm";
    heap::reset();
    let runtime = paused_runtime()?;
    runtime.block_on(async {
        let mut world = World::new(
            MemoryStore::with_page_size(STORM_PAGE),
            Speed::Nominal,
            StoreLimits::default(),
        )?;
        let old = world.node(storm_config(), None);
        let new = world.node(storm_config(), Some(STORM_SWEEP));
        if new
            .co
            .poll(&poll_request(&new), Instant::now())
            .await
            .map_err(err("start poll"))?
            .is_some()
        {
            return Err("work before the storm".to_string());
        }
        for i in 0..agents {
            let agent = agent_ref(SCENARIO, i);
            create(&old, &agent).await?;
            old.co
                .send(&send_request(&agent, "r1".to_string()))
                .await
                .map_err(err("storm send"))?;
        }
        for _ in 0..agents {
            let worker = old.co.register_worker(&[pin()]).map_err(err("register"))?;
            let req = WorkerPollRequest {
                worker_id: worker,
                handlers: vec![pin()],
                wait_ms: 0,
            };
            old.co
                .poll(&req, Instant::now())
                .await
                .map_err(err("dying worker poll"))?
                .ok_or("a dying worker found no turn")?;
        }
        tokio::time::sleep(Duration::from_millis(PAST_LEASE_MS)).await;

        let mut meter = Meter::new(world.bench.clone(), false);
        world.bench.set_latency(true);
        let mut iterations = 0;
        ingest(&mut meter, &world.bench, iterations).await?;
        iterations += 1;
        let mut recovered = 0;
        let mut passes = 0;
        while recovered < agents {
            if passes >= 4 * agents {
                return Err(format!(
                    "recovered {recovered} of {agents} agents in {passes} passes"
                ));
            }
            passes += 1;
            let m = meter.start(Some(&new));
            let report = new.co.sweep_once(&TENANT).await.map_err(err("sweep"))?;
            let pass = meter.part(Some(&new), m);
            meter.record("pass", &[pass]);
            ingest(&mut meter, &world.bench, iterations).await?;
            iterations += 1;
            for _ in 0..report.agents_loaded {
                let m = meter.start(Some(&new));
                let a = new
                    .co
                    .poll(&poll_request(&new), Instant::now())
                    .await
                    .map_err(err("takeover poll"))?
                    .ok_or("a loaded agent's expired lease was not taken over")?;
                let poll = meter.part(Some(&new), m);
                meter.record("activation", &[poll]);
                let m = meter.start(Some(&new));
                new.co
                    .commit(&completion(&a))
                    .await
                    .map_err(err("recovered commit"))?;
                let commit = meter.part(Some(&new), m);
                meter.record("turn", &[poll, commit]);
                recovered += 1;
                ingest(&mut meter, &world.bench, iterations).await?;
                iterations += 1;
            }
        }
        world.bench.set_latency(false);
        let mut figures = meter.figures(SCENARIO, "cold")?;
        figures.extend(heap_figure(SCENARIO, "cold"));
        Ok(figures)
    })
}

/// Where in the directory the discovery target sits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Position {
    Start,
    Middle,
    End,
}

impl Position {
    pub fn name(self) -> &'static str {
        match self {
            Position::Start => "start",
            Position::Middle => "middle",
            Position::End => "end",
        }
    }

    /// The target's index in the sorted directory of `agents`.
    pub fn index(self, agents: usize) -> usize {
        match self {
            Position::Start => 0,
            Position::Middle => agents / 2,
            Position::End => agents.saturating_sub(1),
        }
    }
}

pub const POSITIONS: [Position; 3] = [Position::Start, Position::Middle, Position::End];

/// Passes to read every directory entry when no wake hint is due.
pub fn worst_case_passes(agents: usize) -> usize {
    agents.div_ceil(DIRECTORY_GETS_PER_PASS)
}

/// Passes to read every directory entry however many wake hints are due:
/// the bound a population can rely on.
pub fn guaranteed_passes(agents: usize) -> usize {
    agents.div_ceil(GUARANTEED_DIRECTORY_GETS_PER_PASS)
}

/// Handler of the saturated variant's timers, which no worker serves.
const UNSERVED_TIMER_HANDLER: &str = "bench-timer";

/// Which of the two discovery cases a run measures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Wake {
    /// No wake hint is due: the directory half gets the whole read budget.
    Quiet,
    /// Every agent but the target has a due timer, so the wake half spends
    /// its own reads every pass and the directory half keeps only its share.
    Saturated,
}

/// Cross-process discovery: a client accepts a message for one of `agents`
/// idle agents; a fresh worker coordinator, which never loaded it, polls
/// once per sweep interval from the accept until it is assigned the turn.
fn discovery(agents: usize, position: Position, wake: Wake) -> Result<Vec<Figure>, String> {
    let scenario = match wake {
        Wake::Quiet => format!("{DISCOVERY}-n{agents}-{}", position.name()),
        Wake::Saturated => format!("{DISCOVERY}-saturated-n{agents}-{}", position.name()),
    };
    let bound = match wake {
        Wake::Quiet => worst_case_passes(agents),
        Wake::Saturated => guaranteed_passes(agents),
    };
    heap::reset();
    let runtime = paused_runtime()?;
    runtime.block_on(async {
        let mut world = World::new(MemoryStore::new(), Speed::Nominal, StoreLimits::default())?;
        let setup = world.node(bench_config(), None);
        let mut population = Vec::new();
        for i in 0..agents {
            let agent = agent_ref(DISCOVERY, i);
            create(&setup, &agent).await?;
            population.push((agent_hex(&agent)?, agent));
        }
        population.sort_by(|a, b| a.0.cmp(&b.0));
        let target = population
            .get(position.index(agents))
            .map(|(_, agent)| agent.clone())
            .ok_or("empty population")?;
        if wake == Wake::Saturated {
            for (_, agent) in population.iter().filter(|(_, a)| *a != target) {
                let timer = TimerRequest {
                    agent: agent.clone(),
                    timer_key: "due".to_string(),
                    due_unix_ns: T0_NS,
                    handler: UNSERVED_TIMER_HANDLER.to_string(),
                    payload: Value::Null,
                };
                setup
                    .co
                    .schedule_timer(&timer)
                    .await
                    .map_err(err("schedule timer"))?;
            }
        }
        let client = world.node(bench_config(), None);
        let sweep = SweepConfig::default();
        let worker = world.node(bench_config(), Some(sweep));
        let mut meter = Meter::new(world.bench.clone(), false);
        world.bench.set_latency(true);
        client
            .co
            .send(&send_request(&target, "find-me".to_string()))
            .await
            .map_err(err("client send"))?;
        let accepted = Instant::now();
        let m = meter.start(Some(&worker));
        let limit = 2 * bound + 4;
        let mut assigned = false;
        for k in 0..limit {
            let k = u64::try_from(k).unwrap_or(u64::MAX);
            tokio::time::sleep_until(accepted + Duration::from_millis(k * sweep.interval_ms)).await;
            if worker
                .co
                .poll(&poll_request(&worker), Instant::now())
                .await
                .map_err(err("worker poll"))?
                .is_some()
            {
                assigned = true;
                break;
            }
        }
        if !assigned {
            return Err(format!("{scenario}: no assignment after {limit} polls"));
        }
        let part = meter.part(Some(&worker), m);
        meter.record("assign", &[part]);
        world.bench.set_latency(false);
        let sweep_metrics = worker.co.sweep_metrics();
        let passes = sweep_metrics.passes;
        let mut figures = meter.figures(&scenario, "cold")?;
        figures.push(Figure::new(
            Key::new(&scenario, "cold", "assign", "sweep", "passes"),
            as_f64(passes),
            "passes",
            None,
        ));
        if wake == Wake::Saturated {
            let hints = sweep_metrics.hints_read;
            let wake_gets = u64::from(DEFAULT_SWEEP_MAX_HEAD_GETS / 2);
            if hints != wake_gets * passes {
                return Err(format!(
                    "{scenario}: the wake half read {hints} hints in {passes} passes, not {wake_gets} a pass"
                ));
            }
            figures.push(Figure::new(
                Key::new(&scenario, "cold", "assign", "sweep", "hints_read"),
                as_f64(hints),
                "hints",
                None,
            ));
        }
        let bound_ms = as_f64(u64::try_from(bound).unwrap_or(u64::MAX) * sweep.interval_ms);
        if usize::try_from(passes).unwrap_or(usize::MAX) > bound || part.ms > bound_ms {
            return Err(format!(
                "{scenario}: {passes} passes and {} ms exceed the bound of {bound} passes, {bound_ms} ms",
                part.ms
            ));
        }
        figures.extend(heap_figure(&scenario, "cold"));
        Ok(figures)
    })
}

/// The telemetry loop alone, with no agents store or coordinator: the
/// baseline the agents scenarios are compared with.
fn baseline(iterations: usize) -> Result<Vec<Figure>, String> {
    const SCENARIO: &str = "baseline";
    heap::reset();
    let runtime = paused_runtime()?;
    runtime.block_on(async {
        let world = World::new(MemoryStore::new(), Speed::Nominal, StoreLimits::default())?;
        let mut meter = Meter::new(world.bench.clone(), false);
        let before = world.bench.tally();
        world.bench.set_latency(true);
        for n in 0..iterations {
            ingest(&mut meter, &world.bench, n).await?;
        }
        world.bench.set_latency(false);
        let moved = world.bench.tally().since(&before);
        let mut figures = meter.figures(SCENARIO, "na")?;
        figures.push(Figure::new(
            Key::new(SCENARIO, "na", "run", "isolation", "agents_requests"),
            as_f64(moved.agents_requests),
            "requests",
            None,
        ));
        figures.extend(heap_figure(SCENARIO, "na"));
        Ok(figures)
    })
}

/// Run one named scenario (from [`SCENARIOS`], or `discovery`) at `config`.
pub fn run_scenario(name: &str, config: &Config) -> Result<Vec<Figure>, String> {
    let mut figures = Vec::new();
    match name {
        "hot" | "slow" => {
            let speed = if name == "slow" {
                Speed::Slow { seed: SEED }
            } else {
                Speed::Nominal
            };
            for temp in TEMPS {
                figures.extend(hot(name, config.hot_rounds, speed, temp)?);
            }
        }
        "many" => {
            for temp in TEMPS {
                figures.extend(population(name, config.many_agents, 0, temp)?);
            }
        }
        "dormant" => {
            for temp in TEMPS {
                figures.extend(population(
                    name,
                    config.active_agents,
                    config.dormant_agents,
                    temp,
                )?);
            }
        }
        "index-p64" | "index-p2" => {
            let entries = if name == "index-p2" { 2 } else { 64 };
            for temp in TEMPS {
                figures.extend(index(name, config.index_messages, entries, temp)?);
            }
        }
        "storm" => figures.extend(storm(config.storm_agents)?),
        "baseline" => figures.extend(baseline(config.baseline_iterations)?),
        DISCOVERY => {
            for &agents in &config.discovery_agents {
                for position in POSITIONS {
                    figures.extend(discovery(agents, position, Wake::Quiet)?);
                }
                figures.extend(discovery(agents, Position::End, Wake::Saturated)?);
            }
        }
        other => return Err(format!("no scenario {other}")),
    }
    Ok(figures)
}

/// Every scenario at `size`, in run order.
pub fn run_all(size: Size) -> Result<Vec<Figure>, String> {
    let config = size.config();
    let mut figures = Vec::new();
    for name in SCENARIOS.iter().copied().chain([DISCOVERY]) {
        figures.extend(run_scenario(name, &config)?);
    }
    Ok(figures)
}
