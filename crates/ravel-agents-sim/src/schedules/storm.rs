//! A recovery storm: `AGENTS` agents whose worker died mid-turn, recovered
//! by one coordinator through bounded sweep passes and lease takeovers,
//! while a telemetry ingest and query loop shares the same memory store.
//!
//! Nothing in the telemetry crates is reachable from here without a
//! cross-crate harness, so the ingest side is a minimal counting loop: each
//! iteration PUTs one batch object and reads it back. Every request any
//! party makes is logged with the phase that issued it, in the one order
//! the single-threaded runtime gives them, and both budgets are read from
//! that log, never from a wall clock.

use std::collections::HashSet;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use ravel_agents_core::wire::{AgentRef, CreateAgentRequest, SendRequest, WorkerPollRequest};
use ravel_agents_runtime::{RuntimeConfig, SweepConfig, SweepReport};
use ravel_object_store::fault::{FaultPlan, Op};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
    ObjectStoreBackend, PageToken, Pin, PinnedRead, PutOptions, PutOutcome, StoreError,
};

use super::*;
use rand::RngExt;

use crate::drive::{HANDLER, PAST_LEASE_MS, TENANT, pin, runtime_config, sim_limits, turn_values};
use crate::seeds::AgentSeeds;

/// Agents in the storm.
const AGENTS: usize = 24;
/// Keys per LIST page of the memory store.
const PAGE: usize = 4;
/// The recovering coordinator's sweep: passes only from `sweep_once`, four
/// LISTs and eight HEAD reads each.
const STORM_SWEEP: SweepConfig = SweepConfig {
    interval_ms: u64::MAX,
    max_list_pages: 4,
    max_head_gets: 8,
};

/// The recovering coordinator keeps the production idle unload and rescan
/// intervals. The sim's zero intervals unload an agent on its commit, and an
/// unload restarts the directory scan, so every takeover after the first
/// would list the whole directory and the sweep would recover nothing.
fn storm_config() -> RuntimeConfig {
    let defaults = RuntimeConfig::default();
    RuntimeConfig {
        idle_unload_after_ms: defaults.idle_unload_after_ms,
        rescan_interval_ms: defaults.rescan_interval_ms,
        ..runtime_config()
    }
}

/// Who issued a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Creating the agents and leasing their turns to the worker that dies.
    Setup,
    /// The telemetry loop.
    Ingest,
    /// A sweep pass of the recovering coordinator.
    Sweep,
    /// A poll of the recovering coordinator taking over one lease.
    Takeover,
}

#[derive(Debug)]
struct Ledger {
    phase: Phase,
    log: Vec<(Phase, Op)>,
}

/// Logs every request with the phase current when it was issued.
struct Phased {
    inner: Arc<dyn ObjectStoreBackend>,
    ledger: Arc<Mutex<Ledger>>,
}

impl Phased {
    fn note(&self, op: Op) {
        let mut ledger = self.ledger.lock().expect("ledger");
        let phase = ledger.phase;
        ledger.log.push((phase, op));
    }
}

#[async_trait]
impl ObjectStoreBackend for Phased {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.note(Op::Put);
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.note(Op::Get);
        self.inner.get(key, range).await
    }

    async fn get_pinned(
        &self,
        key: &str,
        range: GetRange,
        pin: &Pin,
    ) -> Result<PinnedRead, StoreError> {
        self.note(Op::Get);
        self.inner.get_pinned(key, range, pin).await
    }

    async fn get_with_pin(&self, key: &str, range: GetRange) -> Result<PinnedRead, StoreError> {
        self.note(Op::Get);
        self.inner.get_with_pin(key, range).await
    }

    async fn pin_of(&self, key: &str) -> Result<(ObjectMeta, Pin), StoreError> {
        self.note(Op::Head);
        self.inner.pin_of(key).await
    }

    async fn put_multipart<'a>(
        &'a self,
        key: &str,
    ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
        self.note(Op::Put);
        self.inner.put_multipart(key).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.note(Op::Head);
        self.inner.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.note(Op::List);
        self.inner.list(prefix, page).await
    }

    async fn list_after(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        page: Option<PageToken>,
    ) -> Result<ListPage, StoreError> {
        self.note(Op::List);
        self.inner.list_after(prefix, start_after, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.note(Op::List);
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.note(Op::Delete);
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    fn observed_store_time_ns(&self) -> Option<i64> {
        self.inner.observed_store_time_ns()
    }
}

/// What the storm and the ingest loop beside it did.
#[derive(Debug)]
struct Storm {
    /// Every request after setup, in issue order.
    log: Vec<(Phase, Op)>,
    /// Each pass's report and the LISTs and GETs it issued.
    passes: Vec<(SweepReport, u64, u64)>,
    /// Ingest iterations run.
    iterations: u64,
}

impl Storm {
    fn count(&self, phase: Phase, op: Op) -> u64 {
        self.log.iter().filter(|e| **e == (phase, op)).count() as u64
    }

    /// The most storm requests issued between two consecutive ingest
    /// requests: on a store that serves requests in order, how many an
    /// ingest request waits behind.
    fn longest_wait(&self) -> u64 {
        let mut longest = 0;
        let mut run = 0;
        for (phase, _) in &self.log {
            if *phase == Phase::Ingest {
                longest = longest.max(run);
                run = 0;
            } else {
                run += 1;
            }
        }
        longest
    }
}

fn agent_ref(i: usize) -> AgentRef {
    AgentRef {
        kind: "storm".to_string(),
        key: format!("agent-{i:03}"),
    }
}

/// Run the storm with the recovering coordinator sweeping per `sweep`. With
/// no seed every agent a pass loads is taken over before the next pass; a
/// seed draws how many of the loaded agents are taken over after each pass,
/// so later passes also read agents already loaded.
fn storm(sweep: SweepConfig, seed: Option<u64>) -> Storm {
    let rig = Rig::over(
        MemoryStore::with_page_size(PAGE),
        FaultPlan::empty(),
        sim_limits(),
    );
    let ledger = Arc::new(Mutex::new(Ledger {
        phase: Phase::Setup,
        log: Vec::new(),
    }));
    let phased: Arc<dyn ObjectStoreBackend> = Arc::new(Phased {
        inner: rig.memory.clone(),
        ledger: Arc::clone(&ledger),
    });
    let set = |phase| ledger.lock().expect("ledger").phase = phase;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("runtime");
    let old = rig.proc("old", Arc::clone(&phased), 1);
    // Ids count up from the seed, so the recovering process starts far past
    // the ids the dying workers draw.
    let new = rig.proc_with(
        "new",
        Arc::clone(&phased),
        1 << 32,
        storm_config(),
        Some(sweep),
    );
    let poll = |worker_id: String| WorkerPollRequest {
        worker_id,
        handlers: vec![pin()],
        wait_ms: 0,
    };

    runtime.block_on(async {
        // The recovering coordinator was running before the storm: its start
        // scan saw an empty directory, so it learns of the agents only
        // through its sweep.
        let none = new
            .co
            .poll(&poll(new.worker_id.clone()), tokio::time::Instant::now())
            .await
            .expect("start poll");
        assert!(none.is_none(), "nothing to do before the storm");
        for i in 0..AGENTS {
            old.co
                .create(&CreateAgentRequest {
                    agent: agent_ref(i),
                })
                .await
                .expect("create");
            old.co
                .send(&SendRequest {
                    agent: agent_ref(i),
                    handler: HANDLER.to_string(),
                    request_id: "r1".to_string(),
                    payload: payload(1),
                })
                .await
                .expect("send");
        }
        // One dying worker per agent: a worker that polls while it holds a
        // turn is handed that turn again, so one worker leases one agent.
        let mut leased = HashSet::new();
        for _ in 0..AGENTS {
            let worker = old.co.register_worker(&[pin()]).expect("register");
            let a = old
                .co
                .poll(&poll(worker), tokio::time::Instant::now())
                .await
                .expect("poll")
                .expect("a dying worker leases a turn");
            leased.insert(a.lease.agent_id);
        }
        assert_eq!(leased.len(), AGENTS, "every agent is mid-turn");
    });
    // The old worker is gone; every lease it held is past its grace.
    rig.clock.advance_ms(PAST_LEASE_MS);
    ledger.lock().expect("ledger").log.clear();

    let mut iterations = 0u64;
    let ingest = |iterations: &mut u64| {
        set(Phase::Ingest);
        let key = format!("telemetry/ingest/{iterations:06}");
        let batch = Bytes::from(format!("batch {iterations}"));
        runtime.block_on(async {
            phased
                .put(&key, batch.clone(), PutOptions::default())
                .await
                .expect("ingest put");
            let read = phased.get(&key, GetRange::Full).await.expect("query get");
            assert_eq!(read.data, batch);
        });
        *iterations += 1;
    };
    let mut rng = seed.map(|seed| AgentSeeds::new(seed).schedule_rng());
    let mut passes = Vec::new();
    let mut recovered = HashSet::new();
    let mut pending = 0usize;
    ingest(&mut iterations);
    while recovered.len() < AGENTS {
        assert!(
            passes.len() < 4 * AGENTS,
            "the storm recovered {} of {AGENTS} agents in {} passes",
            recovered.len(),
            passes.len()
        );
        set(Phase::Sweep);
        let before = ledger.lock().expect("ledger").log.len();
        let report = runtime
            .block_on(new.co.sweep_once(&TENANT))
            .expect("sweep pass");
        let issued: Vec<Op> = ledger.lock().expect("ledger").log[before..]
            .iter()
            .map(|(_, op)| *op)
            .collect();
        let lists = issued.iter().filter(|op| **op == Op::List).count() as u64;
        let gets = issued.iter().filter(|op| **op == Op::Get).count() as u64;
        passes.push((report, lists, gets));
        ingest(&mut iterations);
        pending += report.agents_loaded as usize;
        let take = match rng.as_mut() {
            Some(rng) => rng.random_range(0..=pending),
            None => pending,
        };
        for _ in 0..take {
            set(Phase::Takeover);
            let taken = runtime
                .block_on(
                    new.co
                        .poll(&poll(new.worker_id.clone()), tokio::time::Instant::now()),
                )
                .expect("takeover poll")
                .expect("a loaded agent's expired lease is taken over");
            assert!(
                recovered.insert(taken.lease.agent_id.clone()),
                "each takeover is a different agent"
            );
            let (state, output) = turn_values(&taken);
            runtime
                .block_on(new.commit(&taken, &state, &output))
                .expect("commit the recovered turn");
            ingest(&mut iterations);
        }
        pending -= take;
    }
    let log = ledger.lock().expect("ledger").log.clone();
    Storm {
        log,
        passes,
        iterations,
    }
}

/// HEAD reads and PUTs one takeover issues: the poll reads HEAD and takes
/// the lease and begins the turn in one CAS; the commit reads HEAD, the
/// input object and HEAD again, then writes two objects and the HEAD CAS.
const TAKEOVER_GETS: u64 = 4;
const TAKEOVER_PUTS: u64 = 4;

/// The longest an ingest request may queue behind storm requests. The loop
/// runs one ingest iteration after every pass and every takeover, so an
/// ingest request waits behind at most one of them: a pass issues at most
/// `max_list_pages + max_head_gets` requests (the sweep's per-pass bound;
/// nothing fires and nothing needs a directory repair here), and a takeover
/// `TAKEOVER_GETS + TAKEOVER_PUTS`, which is less.
const INGEST_WAIT_BAND: u64 = STORM_SWEEP.max_list_pages as u64 + STORM_SWEEP.max_head_gets as u64;

/// Check both budgets on one storm. The storm band holds each pass to
/// `STORM_SWEEP`'s LIST and HEAD bounds whatever config ran it, and the
/// takeovers to exactly their own requests with no directory listing. The
/// ingest band holds the ingest loop to exactly its own two requests per
/// iteration and to `INGEST_WAIT_BAND`.
fn assert_within_budget(s: &Storm) {
    for (n, (report, lists, gets)) in s.passes.iter().enumerate() {
        assert_eq!(
            (report.list_pages, report.head_gets),
            (*lists, *gets),
            "pass {n} reports what it issued"
        );
        assert!(
            *lists <= u64::from(STORM_SWEEP.max_list_pages),
            "pass {n} issued {lists} LISTs, over the budget of {}",
            STORM_SWEEP.max_list_pages
        );
        assert!(
            *gets <= u64::from(STORM_SWEEP.max_head_gets),
            "pass {n} issued {gets} HEAD reads, over the budget of {}",
            STORM_SWEEP.max_head_gets
        );
    }
    let loaded: u64 = s.passes.iter().map(|(r, _, _)| r.agents_loaded).sum();
    assert_eq!(loaded, AGENTS as u64, "the passes load every agent once");
    assert_eq!(
        s.count(Phase::Takeover, Op::List),
        0,
        "no takeover lists the directory"
    );
    assert_eq!(
        s.count(Phase::Takeover, Op::Get),
        AGENTS as u64 * TAKEOVER_GETS
    );
    assert_eq!(
        s.count(Phase::Takeover, Op::Put),
        AGENTS as u64 * TAKEOVER_PUTS
    );
    let passes = s.passes.len() as u64;
    assert_eq!(
        s.iterations,
        1 + passes + AGENTS as u64,
        "one iteration first, then one per pass and per takeover"
    );
    assert_eq!(s.count(Phase::Ingest, Op::Put), s.iterations);
    assert_eq!(s.count(Phase::Ingest, Op::Get), s.iterations);
    let other: u64 = [Op::List, Op::Head, Op::Delete]
        .iter()
        .map(|op| s.count(Phase::Ingest, *op))
        .sum();
    assert_eq!(other, 0, "ingest issues only its PUT and GET");
    let wait = s.longest_wait();
    assert!(
        wait <= INGEST_WAIT_BAND,
        "an ingest request waited behind {wait} storm requests, over the band of {INGEST_WAIT_BAND}"
    );
}

/// The fixed storm: every agent a pass loads is taken over before the next.
#[test]
fn recovery_storm_leaves_telemetry_ingest_within_its_own_budget() {
    let s = storm(STORM_SWEEP, None);
    assert_within_budget(&s);
    // Each pass reads eight distinct HEADs and loads every one of them, so
    // the 24 agents take ceil(24 / 8) = 3 passes. Each spends its whole LIST
    // budget: one wake listing that finds no hint, then three directory
    // pages of four, the third listed after the HEAD reads ran out.
    let per_pass: Vec<(u64, u64, u64)> = s
        .passes
        .iter()
        .map(|(r, lists, gets)| (*lists, *gets, r.agents_loaded))
        .collect();
    assert_eq!(per_pass, vec![(4, 8, 8); 3]);
    assert_eq!(s.count(Phase::Sweep, Op::List), 12);
    assert_eq!(s.count(Phase::Sweep, Op::Get), AGENTS as u64);
    assert_eq!(s.iterations, 28);
    // A whole pass sits between two ingest iterations, so the band is met
    // exactly.
    assert_eq!(s.longest_wait(), INGEST_WAIT_BAND);
}

/// Seeded storms: the seed draws how many loaded agents are taken over
/// after each pass, so passes also re-read agents loaded earlier.
#[test]
fn recovery_storms_across_seeds() {
    let mut runs = 0;
    let mut totals = StormTotals::default();
    for seed in seeds() {
        let s = storm(STORM_SWEEP, Some(seed));
        assert_within_budget(&s);
        totals.passes += s.passes.len() as u64;
        totals.sweep_lists += s.count(Phase::Sweep, Op::List);
        totals.sweep_gets += s.count(Phase::Sweep, Op::Get);
        totals.longest_wait = totals.longest_wait.max(s.longest_wait());
        runs += 1;
    }
    pinned("recovery_storms", runs, totals, STORM_TOTALS);
}

/// The batch's totals. Every pass spends its whole budget, four LISTs and
/// eight HEAD reads, since a directory of 24 never fits one pass; a seed that
/// defers takeovers needs more than the fixed storm's three passes, because
/// a pass that reads an agent already loaded loads nothing new.
const STORM_TOTALS: StormTotals = StormTotals {
    passes: 393,
    sweep_lists: 393 * 4,
    sweep_gets: 393 * 8,
    longest_wait: INGEST_WAIT_BAND,
};

/// Sums over the batch, and the longest ingest wait in any storm.
#[derive(Debug, Default, PartialEq, Eq)]
struct StormTotals {
    passes: u64,
    sweep_lists: u64,
    sweep_gets: u64,
    longest_wait: u64,
}
