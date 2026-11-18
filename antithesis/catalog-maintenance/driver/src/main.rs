//! Antithesis driver for the catalog-maintenance workload (ADR-4326).
//!
//! Setup seeds a fresh tenant with whole past hours through `ravel-cli load
//! --ingest-hour-stride`, records every seeded record in the model, and then
//! writes the seed marker the ravel-server entrypoints wait on. Rules write
//! strict OTLP logs into the same tenant, run `ravel-cli` fold, compaction and
//! sweep on demand, jump every container's clock forward to seal the live
//! hours and later past the sweep's protection horizon, overwrite HEAD with
//! garbage through raw S3, audit the store against what HEAD names and for
//! swept compaction inputs, and check that no scan exceeds the stored
//! multiset of seeded and live records. The eventually stops faults, sweeps
//! every shard and checks the whole multiset. See
//! antithesis/catalog-maintenance/README.md for the properties.

mod audit;
mod cli;
mod clock;
mod multiset;
mod race;
mod s3;
mod seed;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use antithesis_sdk::{assert_always, assert_reachable, assert_sometimes, assert_unreachable};
use hegel::TestCase;
use hegel::generators as gs;
use hegel::stateful::machine;
use serde_json::json;

use antithesis_instrumentation as _;

use ravel_antithesis_driver::model::{self, Attempt, Mode, Model, Signal, Write};
use ravel_antithesis_driver::otlp::{self, PartialSuccess};
use ravel_antithesis_driver::ravel::{self, Ravel, Reply, SqlFailure};

use audit::{InputKeys, JsonPart};
use cli::Cli;
use s3::Store;
use seed::Plan;

/// The S3 object the ravel-server entrypoints wait for. It sits outside the
/// `t/` and `sys/` prefixes, so no ravel process reads or deletes it.
const SEED_MARKER: &str = "antithesis/catalog-maintenance/seeded";
const MAX_RETRIES: u32 = 2;
const WRITE_TIMEOUT: Duration = Duration::from_secs(15);
const READ_TIMEOUT: Duration = Duration::from_secs(30);
const WINDOW_SLACK_NS: u64 = 600 * 1_000_000_000;
const NS_PER_HOUR: u64 = 3_600_000_000_000;
const CLI_TIMEOUT_S: u32 = 300;
/// The shard count the servers and every `ravel-cli` call use.
const SHARDS: u32 = 4;
/// Compaction records one audit decodes for their inputs, at most.
const DECODES_PER_AUDIT: usize = 16;
/// Everything setup waits on, the seeded load included, finishes within
/// this or the driver exits non-zero without reporting setup complete. It
/// is below the local loop's and `launch.sh`'s 900-second setup timeouts.
const SETUP_BUDGET: Duration = Duration::from_secs(600);
/// Acked writes verified by one token read in the eventually.
const TOKEN_BATCH: usize = 20;
/// Fold races the eventually runs until a server fold is overtaken.
const FOLD_RACES_EVENTUALLY: u32 = 6;

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn env_u32(key: &str, default: u32) -> u32 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn node_list(key: &str, default: &str) -> Vec<String> {
    std::env::var(key)
        .unwrap_or_else(|_| default.to_string())
        .split(',')
        .map(|s| s.trim().trim_end_matches('/').to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// How and when the driver jumps the clocks.
struct ClockPlan {
    /// The libfaketime timestamp file every container reads.
    file: PathBuf,
    jump_s: u64,
    /// The first sweep jump, past the protection horizon.
    horizon_jump_s: u64,
    /// The second sweep jump, past the pinned-query window.
    window_jump_s: u64,
    /// libfaketime's reread interval (`FAKETIME_CACHE_DURATION`).
    reread_s: u64,
    /// Acked live writes the jump rule waits for, so the jump has live hours
    /// to seal.
    min_acked: usize,
}

struct Config {
    nodes: Vec<String>,
    maint_nodes: Vec<String>,
    token: String,
    tenant: String,
    seed_hours: u32,
    seed_stride: u32,
    seed_per_hour: u32,
    max_writes: usize,
    inject_swap: bool,
    eventually_on: bool,
    eventually_after: Option<u64>,
    clock: ClockPlan,
}

impl Config {
    fn from_env() -> Config {
        Config {
            nodes: node_list("RAVEL_NODES", "http://ravel1:4318,http://ravel2:4318"),
            maint_nodes: node_list(
                "RAVEL_MAINTAIN_NODES",
                "http://maint1:4318,http://maint2:4318",
            ),
            token: std::env::var("RAVEL_TOKEN").unwrap_or_else(|_| "cm-token".to_string()),
            tenant: std::env::var("RAVEL_TENANT").unwrap_or_else(|_| "cm-tenant".to_string()),
            seed_hours: env_u32("SEED_HOURS", 6),
            seed_stride: env_u32("SEED_HOUR_STRIDE", 2),
            seed_per_hour: env_u32("SEED_RECORDS_PER_HOUR", 50),
            max_writes: std::env::var("WORKLOAD_MAX_WRITES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(3000),
            inject_swap: std::env::var("WORKLOAD_INJECT_SWAPPED_RECORD").is_ok_and(|v| v == "1"),
            eventually_on: std::env::var("WORKLOAD_EVENTUALLY").map_or(true, |v| v != "off"),
            eventually_after: std::env::var("WORKLOAD_EVENTUALLY_AFTER_SECS")
                .ok()
                .and_then(|v| v.parse().ok()),
            clock: ClockPlan {
                file: PathBuf::from(
                    std::env::var("CLOCK_JUMP_FILE")
                        .unwrap_or_else(|_| "/faketime/offset".to_string()),
                ),
                jump_s: env_u64("CLOCK_JUMP_SECS", clock::DEFAULT_JUMP_S),
                horizon_jump_s: env_u64(
                    "CLOCK_SWEEP_HORIZON_JUMP_SECS",
                    clock::DEFAULT_HORIZON_JUMP_S,
                ),
                window_jump_s: env_u64(
                    "CLOCK_SWEEP_WINDOW_JUMP_SECS",
                    clock::DEFAULT_WINDOW_JUMP_S,
                ),
                reread_s: env_u64("FAKETIME_CACHE_DURATION", 2),
                min_acked: usize::try_from(env_u32("CLOCK_JUMP_MIN_ACKED", 20)).unwrap_or(20),
            },
        }
    }
}

/// One move of every clock.
#[derive(Clone, Copy, Debug)]
struct Jump {
    before_ns: u64,
    after_ns: u64,
    /// The total offset after this move.
    offset_s: u64,
    reached_driver: bool,
}

struct Shared {
    ravel: Ravel,
    /// The `--mode maintain` servers, polled on `/readyz` only.
    maint: Ravel,
    cli: Cli,
    store: Store,
    tenant: String,
    plan: Plan,
    /// The seeded records the multiset check expects, each exactly once.
    seeded: BTreeSet<(u64, u32)>,
    /// The live strict writes.
    model: Mutex<Model>,
    /// The hours, by the driver's clock, in which a live write was
    /// acknowledged. A write's ingest hour is no later than its
    /// acknowledgement's.
    acked_hours: Mutex<BTreeSet<u32>>,
    max_writes: usize,
    /// The HEAD key, learned from the first `catalog inspect --json`.
    head_key: Mutex<Option<String>>,
    /// Whether an audit has seen a compaction record, for the multiset
    /// check's details.
    compacted: AtomicBool,
    clock: ClockPlan,
    /// The jump that seals the live hours.
    jump: Mutex<Option<Jump>>,
    /// Set when the sweep jumps start; once per timeline.
    sweep_jump_started: AtomicBool,
    /// The horizon and window jumps, once both reached the driver.
    sweep_jump: Mutex<Option<(Jump, Jump)>>,
    /// The offset the file holds: every jump and nudge so far.
    offset_s: Mutex<u64>,
    /// The client and token of the on-demand fold route.
    admin: reqwest::blocking::Client,
    token: String,
    /// The last fold race's skewed-fold and mean server-fold seconds.
    fold_race_timings: Mutex<(f64, f64)>,
    /// Fold races run; numbers each one's HEAD garbage.
    fold_races: AtomicU64,
    /// Whether a server fold lost its HEAD CAS to the skewed fold.
    fold_overtaken: AtomicBool,
    /// Every key an audit or the sweep jump has listed, and the swept
    /// inputs confirmed gone. Keys are never reused, so one listed here and
    /// absent from a later LIST was deleted.
    sweeps: Mutex<audit::SweepLedger>,
    /// The inputs of each compaction record decoded so far; records are
    /// immutable, so each is decoded once.
    inputs_of: Mutex<BTreeMap<String, Vec<InputKeys>>>,
    /// Every rule holds a read guard while it acts, except for the audit
    /// rule's sweep check; the clock jump and the eventually take the write
    /// guard.
    gate: RwLock<()>,
    stopping: AtomicBool,
    eventually_lock: Mutex<()>,
    eventually_on: bool,
}

struct Workload(Arc<Shared>);

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn draw_node(tc: &TestCase, n: usize) -> usize {
    let max = u64::try_from(n.saturating_sub(1)).unwrap_or(0);
    tc.draw(gs::integers::<u64>().min_value(0).max_value(max)) as usize
}

fn first(lines: &[String]) -> serde_json::Value {
    json!({ "count": lines.len(), "first": lines.iter().take(5).collect::<Vec<_>>() })
}

/// The client's view of one strict log write attempt.
fn classify(reply: &Reply) -> Attempt {
    match reply {
        Reply::NoResponse(_) => Attempt::NoResponse,
        Reply::Status {
            status,
            commit_token,
            body,
        } if (200..300).contains(status) => match (otlp::partial_success(true, body), commit_token)
        {
            (PartialSuccess::None, Some(h)) if !h.is_empty() => {
                Attempt::Acked { header: h.clone() }
            }
            _ => Attempt::Partial,
        },
        Reply::Status { status, .. } => Attempt::Refused { status: *status },
    }
}

const LOGS_QUERY: &str = "SELECT attrs['wl.write'] AS w, attrs['wl.rec'] AS r, count(*) AS n \
     FROM logs WHERE attrs['wl.write'] IS NOT NULL GROUP BY attrs['wl.write'], attrs['wl.rec']";

/// The logs count query over the given live writes only.
fn writes_query(ids: &[u64]) -> String {
    let list = ids
        .iter()
        .map(|i| format!("'{i}'"))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "SELECT attrs['wl.write'] AS w, attrs['wl.rec'] AS r, count(*) AS n FROM logs \
         WHERE attrs['wl.write'] IN ({list}) GROUP BY attrs['wl.write'], attrs['wl.rec']"
    )
}

/// What one `catalog inspect --json` showed.
struct HeadView {
    head_key: String,
    /// `None` when there is no HEAD yet.
    created_unix_ns: Option<u64>,
    /// `None` when there is no HEAD, or its parts lack the expected fields.
    parts: Option<Vec<JsonPart>>,
}

impl Shared {
    fn args<'a>(&'a self, words: &[&'a str]) -> Vec<&'a str> {
        let mut v = words.to_vec();
        v.extend(["--tenant", self.tenant.as_str(), "--signal", "logs"]);
        v
    }

    /// A read guard for one rule, or `None` once the eventually started.
    fn enter(&self) -> Option<RwLockReadGuard<'_, ()>> {
        if self.stopping.load(Ordering::SeqCst) {
            return None;
        }
        let guard = self.gate.read().unwrap_or_else(|e| e.into_inner());
        if self.stopping.load(Ordering::SeqCst) {
            return None;
        }
        Some(guard)
    }

    fn register(&self, records: u32) -> Option<Write> {
        let mut m = lock(&self.model);
        if m.writes.len() >= self.max_writes {
            return None;
        }
        let id = m.next_id;
        m.next_id += 1;
        let w = Write {
            id,
            signal: Signal::Logs,
            mode: Mode::Strict,
            key: None,
            records,
            samples: Vec::new(),
            ts_ns: now_ns(),
            attempts: Vec::new(),
        };
        m.writes.insert(id, w.clone());
        Some(w)
    }

    fn begin_attempt(&self, id: u64) -> usize {
        let mut m = lock(&self.model);
        match m.writes.get_mut(&id) {
            Some(w) => {
                w.attempts.push(Attempt::InFlight);
                w.attempts.len() - 1
            }
            None => 0,
        }
    }

    fn finish_attempt(&self, id: u64, k: usize, attempt: Attempt) {
        let acked = matches!(attempt, Attempt::Acked { .. });
        {
            let mut m = lock(&self.model);
            if let Some(slot) = m.writes.get_mut(&id).and_then(|w| w.attempts.get_mut(k)) {
                *slot = attempt;
            }
            if acked {
                m.mark_acked(id);
            }
        }
        if acked && let Ok(hour) = u32::try_from(now_ns() / NS_PER_HOUR) {
            lock(&self.acked_hours).insert(hour);
        }
    }

    /// `catalog inspect --json`; `None` when it fails, which a corrupt HEAD
    /// does.
    fn inspect_json(&self) -> Option<HeadView> {
        let out = self
            .cli
            .run(&self.args(&["catalog", "inspect", "--json"]), CLI_TIMEOUT_S);
        if !out.ok() {
            println!("catalog inspect --json: {}", out.summary());
            if out.stderr.contains("is corrupt") {
                assert_reachable!(
                    "catalog: inspect reported a corrupt HEAD",
                    &json!({ "detail": out.summary() })
                );
            }
            return None;
        }
        let v: serde_json::Value = match serde_json::from_str(&out.stdout) {
            Ok(v) => v,
            Err(e) => {
                println!("catalog inspect --json printed no JSON: {e}");
                return None;
            }
        };
        let head_key = v.get("head_key")?.as_str()?.to_string();
        *lock(&self.head_key) = Some(head_key.clone());
        let head = v.get("head").filter(|h| !h.is_null());
        let parts = head.and_then(audit::json_parts);
        if head.is_some() && parts.is_none() {
            println!("catalog inspect --json: a part lacks the expected fields");
        }
        let view = HeadView {
            head_key,
            created_unix_ns: head
                .and_then(|h| h.get("created_unix_ns"))
                .and_then(serde_json::Value::as_u64),
            parts,
        };
        let watermark = head
            .and_then(|h| h.get("watermark_hour"))
            .and_then(serde_json::Value::as_u64);
        if let (Some(wm), Some(parts)) = (watermark, &view.parts) {
            self.observe_head(wm, parts);
        }
        Some(view)
    }

    /// The fold-progress properties, on every HEAD the driver reads: an hour
    /// counts as folded when a part with entries covers it.
    fn observe_head(&self, wm: u64, parts: &[JsonPart]) {
        let seeded_folded = audit::covered_hours(parts, self.plan.hours_list());
        assert_sometimes!(
            !seeded_folded.is_empty(),
            "catalog: a sealed seeded hour was folded into HEAD",
            &json!({ "watermark_hour": wm, "seeded_hours_folded": seeded_folded })
        );
        let acked_hours: Vec<u32> = lock(&self.acked_hours).iter().copied().collect();
        let live_sealed = audit::covered_hours(parts, acked_hours.iter().copied());
        let jumped = lock(&self.jump).is_some();
        assert_sometimes!(
            !live_sealed.is_empty(),
            "clock: an hour ingested live was sealed by a fold",
            &json!({
                "watermark_hour": wm,
                "acked_hours": acked_hours.len(),
                "sealed": live_sealed,
                "clock_jumped": jumped,
            })
        );
    }

    /// The SQL time window covering every seeded and live record.
    fn window_s(&self) -> (f64, f64) {
        let start = u64::from(self.plan.start_hour) * NS_PER_HOUR;
        let live_max = lock(&self.model).ts_bounds().map_or(0, |(_, max)| max);
        let end = now_ns().max(live_max);
        (
            start.saturating_sub(WINDOW_SLACK_NS) as f64 / 1e9,
            end.saturating_add(WINDOW_SLACK_NS) as f64 / 1e9,
        )
    }

    /// A no-token scan of every workload record on `node`, judged with no
    /// live write expected: its freshness is bounded by listing, so it
    /// proves upper bounds on live writes and, once faults stop, the seeded
    /// records' presence.
    fn scan(&self, node: usize) -> Result<multiset::Verdict, SqlFailure> {
        let (start, end) = self.window_s();
        let value = self
            .ravel
            .sql(node, LOGS_QUERY, start, end, &[], READ_TIMEOUT)?;
        let counts = model::log_counts(ravel::rows(&value)).map_err(SqlFailure::BadBody)?;
        let m = lock(&self.model);
        Ok(multiset::judge(&self.seeded, &m, &counts, &[]))
    }

    /// Read `writes` with their commit tokens, which makes every acked
    /// record visible whatever hour it landed in.
    fn token_read(&self, node: usize, writes: &[Write]) -> Result<model::LogJudgement, SqlFailure> {
        let ids: Vec<u64> = writes.iter().map(|w| w.id).collect();
        let tokens: Vec<String> = writes.iter().flat_map(|w| w.tokens("logs")).collect();
        let min = writes.iter().map(|w| w.ts_ns).min().unwrap_or(0);
        let max = writes.iter().map(|w| w.ts_ns).max().unwrap_or(0);
        let start = min.saturating_sub(WINDOW_SLACK_NS) as f64 / 1e9;
        let end = max.saturating_add(WINDOW_SLACK_NS) as f64 / 1e9;
        let value = self
            .ravel
            .sql(node, &writes_query(&ids), start, end, &tokens, READ_TIMEOUT)?;
        let counts = model::log_counts(ravel::rows(&value)).map_err(SqlFailure::BadBody)?;
        let m = lock(&self.model);
        Ok(model::judge_logs(&m, &counts, &ids))
    }

    /// Scan one node while faults may run and assert the upper bounds only.
    fn check_bounds(&self, node: usize, after: &str) {
        let verdict = match self.scan(node) {
            Ok(v) => v,
            Err(e) => {
                println!("multiset scan on node {node} ({after}): {e}");
                return;
            }
        };
        assert_always!(
            verdict.upper_bounds_hold(),
            "catalog: no scan shows a phantom or an excess copy",
            &json!({
                "node": node,
                "after": after,
                "compaction_record_seen": self.compacted.load(Ordering::SeqCst),
                "rows": verdict.rows,
                "seeded_over": first(&verdict.seeded_over),
                "phantoms": first(&verdict.phantoms),
                "over": first(&verdict.over),
            })
        );
    }

    /// Whether every ravel node is ready and answers SQL and every
    /// maintenance node is ready.
    fn all_nodes_serve(&self) -> Result<(), String> {
        let (start, end) = self.window_s();
        for node in 0..self.ravel.nodes.len() {
            self.ravel
                .ready(node)
                .map_err(|e| format!("{} not ready: {e}", self.ravel.nodes[node]))?;
            self.ravel
                .sql(
                    node,
                    "SELECT count(*) FROM logs",
                    start,
                    end,
                    &[],
                    READ_TIMEOUT,
                )
                .map_err(|e| format!("{} does not answer SQL: {e}", self.ravel.nodes[node]))?;
        }
        for node in 0..self.maint.nodes.len() {
            self.maint
                .ready(node)
                .map_err(|e| format!("{} not ready: {e}", self.maint.nodes[node]))?;
        }
        Ok(())
    }

    /// Wait up to `limit` for `all_nodes_serve`, returning the last error.
    fn wait_serving(&self, limit: Duration, what: &str) -> Result<(), String> {
        let deadline = Instant::now() + limit;
        let mut last = String::new();
        loop {
            match self.all_nodes_serve() {
                Ok(()) => return Ok(()),
                Err(e) if Instant::now() >= deadline => return Err(e),
                Err(e) => {
                    if e != last {
                        println!("{what}: waiting for every node: {e}");
                        last = e;
                    }
                    std::thread::sleep(Duration::from_secs(2));
                }
            }
        }
    }

    /// Move every clock forward by the planned jump, once per timeline. The
    /// caller holds the gate's write guard, so no write is between its
    /// registration and its acknowledgement.
    fn jump_clock(&self, reason: &str) {
        let mut slot = lock(&self.jump);
        if slot.is_some() {
            return;
        }
        let acked = lock(&self.model).acked_logs.len();
        let jump = self.shift_clock(self.clock.jump_s, reason);
        *slot = Some(jump);
        drop(slot);
        if !jump.reached_driver {
            return;
        }
        assert_reachable!(
            "clock: the driver's clock jumped forward",
            &json!({
                "reason": reason,
                "jump_s": self.clock.jump_s,
                "offset_s": jump.offset_s,
                "min_jump_s": clock::min_jump_s(),
                "before_ns": jump.before_ns,
                "after_ns": jump.after_ns,
                "acked_log_writes": acked,
            })
        );
        self.settle("clock jump");
    }

    /// Once a compaction record was seen after the sealing jump: move every
    /// clock past the record's protection horizon, fold so HEAD stops naming
    /// its inputs, sweep every shard so each input gets its unnamed-since
    /// marker, then move every clock past the marker's pinned-query window.
    /// The next sweep deletes the inputs. The caller holds the gate's write
    /// guard for all of it, as for the sealing jump.
    fn sweep_jump(&self, reason: &str) {
        if lock(&self.jump).is_none_or(|j| !j.reached_driver)
            || !self.compacted.load(Ordering::SeqCst)
            || self.sweep_jump_started.swap(true, Ordering::SeqCst)
        {
            return;
        }
        // A deleted input counts only if a LIST held it before the delete.
        match self.tenant_prefix() {
            Some(prefix) => match self.store.list(&prefix) {
                Ok(keys) => lock(&self.sweeps).observe(keys),
                Err(e) => println!("sweep jump: the listing before the jump failed: {e}"),
            },
            None => println!("sweep jump: no HEAD key known, so nothing listed before the jump"),
        }
        let horizon = self.shift_clock(self.clock.horizon_jump_s, reason);
        if !horizon.reached_driver {
            return;
        }
        self.settle("protection-horizon jump");
        if !self.fold() {
            println!("sweep jump: the fold after the horizon jump failed");
        }
        let marked = (0..SHARDS).filter(|&shard| self.sweep(shard)).count();
        let window = self.shift_clock(self.clock.window_jump_s, reason);
        if !window.reached_driver {
            return;
        }
        *lock(&self.sweep_jump) = Some((horizon, window));
        assert_reachable!(
            "clock: the clocks jumped past the sweep's protection horizon and pinned-query window",
            &json!({
                "reason": reason,
                "offset_s": window.offset_s,
                "horizon_jump_s": self.clock.horizon_jump_s,
                "protection_horizon_s": clock::protection_horizon_s(),
                "window_jump_s": self.clock.window_jump_s,
                "pinned_window_s": clock::pinned_window_s(),
                "shards_swept_between": marked,
            })
        );
        self.settle("pinned-window jump");
    }

    /// Raise the total offset by `by_s` and wait for the driver's own clock
    /// to move by it. libfaketime rereads the file every `reread_s` seconds;
    /// the driver runs under it like every other container.
    fn shift_clock(&self, by_s: u64, reason: &str) -> Jump {
        let before_ns = now_ns();
        let offset_s = {
            let mut total = lock(&self.offset_s);
            if let Err(e) = clock::write_offset(&self.clock.file, *total + by_s) {
                assert_unreachable!(
                    "clock: the driver could not write the clock offset",
                    &json!({ "file": self.clock.file.display().to_string(), "detail": e })
                );
                return Jump {
                    before_ns,
                    after_ns: before_ns,
                    offset_s: *total,
                    reached_driver: false,
                };
            }
            *total += by_s;
            *total
        };
        let deadline = Instant::now() + Duration::from_secs(self.clock.reread_s + 30);
        let mut after_ns = now_ns();
        while !clock::moved(before_ns, after_ns, by_s) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(500));
            after_ns = now_ns();
        }
        let reached_driver = clock::moved(before_ns, after_ns, by_s);
        let details = json!({
            "reason": reason,
            "offset_s": offset_s,
            "by_s": by_s,
            "before_ns": before_ns,
            "after_ns": after_ns,
        });
        println!("clock: jumped ({details})");
        if !reached_driver {
            assert_unreachable!("clock: the clock jump did not reach the driver", &details);
        }
        Jump {
            before_ns,
            after_ns,
            offset_s,
            reached_driver,
        }
    }

    /// Move every clock forward by `by_s` outside the three planned jumps,
    /// and settle. The local loop adds every such move, from the line this
    /// prints, to the offset it expects.
    fn nudge(&self, by_s: u64, reason: &str) -> bool {
        if by_s == 0 {
            return true;
        }
        let jump = self.shift_clock(by_s, reason);
        if !jump.reached_driver {
            return false;
        }
        println!("clock: moved forward by {by_s} s outside the planned jumps ({reason})");
        self.settle(reason);
        true
    }

    /// After a jump reached the driver: wait one more reread, within which
    /// every other process picks the offset up too, then for every node.
    fn settle(&self, what: &str) {
        std::thread::sleep(Duration::from_secs(self.clock.reread_s + 1));
        if let Err(e) = self.wait_serving(Duration::from_secs(120), what) {
            println!("clock: a node is not serving two minutes after the {what}: {e}");
        }
    }

    /// `t/<tenant hash>/`, once an inspect has shown the HEAD key.
    fn tenant_prefix(&self) -> Option<String> {
        lock(&self.head_key)
            .as_deref()
            .and_then(audit::tenant_prefix)
    }

    /// `ravel-cli maintain sweep` over one shard: every GC rule, deleting.
    fn sweep(&self, shard: u32) -> bool {
        let shard = shard.to_string();
        let out = self.cli.run(
            &self.args(&["maintain", "sweep", "--shard", &shard]),
            CLI_TIMEOUT_S,
        );
        if !out.ok() {
            println!("maintain sweep --shard {shard}: {}", out.summary());
            return false;
        }
        assert_reachable!(
            "catalog: an on-demand sweep succeeded",
            &json!({ "shard": shard })
        );
        true
    }

    /// The sweep property, over one LIST of the tenant prefix: an input of a
    /// listed compaction record that an earlier LIST held is gone, and a GET
    /// of it finds nothing. Each such key counts once across audits. Then
    /// the LIST joins the keys seen.
    fn check_swept(&self, prefix: &str, listed: &BTreeSet<String>) {
        let records = audit::compaction_records(prefix, listed);
        let todo: Vec<String> = {
            let known = lock(&self.inputs_of);
            records
                .iter()
                .filter(|k| !known.contains_key(*k))
                .take(DECODES_PER_AUDIT)
                .cloned()
                .collect()
        };
        for key in todo {
            let out = self
                .cli
                .run(&["commit", "decode-compaction", &key], CLI_TIMEOUT_S);
            if !out.ok() {
                println!("audit: commit decode-compaction {key}: {}", out.summary());
                continue;
            }
            match audit::compaction_inputs(prefix, &key, &out.stdout) {
                Some(inputs) => {
                    lock(&self.inputs_of).insert(key, inputs);
                }
                None => println!("audit: commit decode-compaction {key} printed no input list"),
            }
        }
        let mut gone = Vec::new();
        let mut decoded = 0;
        {
            let known = lock(&self.inputs_of);
            let sweeps = lock(&self.sweeps);
            for record in &records {
                if let Some(inputs) = known.get(record) {
                    decoded += 1;
                    gone.extend(
                        sweeps
                            .unconfirmed(inputs, listed)
                            .into_iter()
                            .map(|key| (record.clone(), key)),
                    );
                }
            }
        }
        // A LIST can miss a key; only a GET that finds nothing confirms it.
        let confirmed: Vec<String> = gone
            .iter()
            .filter(|(_, key)| matches!(self.store.get(key), Ok(None)))
            .filter(|(_, key)| lock(&self.sweeps).confirm(key))
            .take(5)
            .map(|(record, key)| format!("{key} (input of {record})"))
            .collect();
        lock(&self.sweeps).observe(listed.iter().cloned());
        if decoded > 0 {
            assert_sometimes!(
                !confirmed.is_empty(),
                "catalog: sweep deleted a superseded compaction input",
                &json!({
                    "compaction_records_decoded": decoded,
                    "absent_from_list": gone.len(),
                    "confirmed_by_get": confirmed,
                    "sweep_jumped": lock(&self.sweep_jump).is_some(),
                })
            );
        }
    }

    fn fold(&self) -> bool {
        let out = self.cli.run(
            &self.args(&["catalog", "fold", "--shards", "4"]),
            CLI_TIMEOUT_S,
        );
        if !out.ok() {
            println!("catalog fold: {}", out.summary());
            return false;
        }
        assert_reachable!("catalog: an on-demand fold succeeded", &json!({}));
        let _ = self.inspect_json();
        true
    }

    fn compact(&self) -> bool {
        let out = self.cli.run(
            &self.args(&["maintain", "compact-tenant", "--shards", "4"]),
            CLI_TIMEOUT_S * 2,
        );
        if !out.ok() {
            println!("maintain compact-tenant: {}", out.summary());
        }
        out.ok()
    }

    /// `POST /api/v1/admin/fold` on one `--mode all` server: the body on a
    /// 200, else why not.
    fn admin_fold(&self, node: usize) -> Result<String, String> {
        let url = format!("{}/api/v1/admin/fold", self.ravel.nodes[node]);
        let resp = self
            .admin
            .post(&url)
            .bearer_auth(&self.token)
            .json(&json!({ "signal": "logs", "tenant": self.tenant }))
            .timeout(Duration::from_secs(u64::from(CLI_TIMEOUT_S)))
            .send()
            .map_err(|e| format!("{url}: {e}"))?;
        let status = resp.status();
        let body = resp.text().map_err(|e| format!("{url}: {e}"))?;
        if status.is_success() {
            Ok(body)
        } else {
            Err(format!("{url}: {status}: {body}"))
        }
    }

    /// One fold race (README.md, "The forced races"): nudge every clock to
    /// just before an hour seals, overwrite HEAD with garbage, start a
    /// `ravel-cli` fold whose clock runs `race::FOLD_SKEW_S` ahead, and
    /// `delay_frac` into the window the last race's timings give, post an
    /// on-demand fold to each `--mode all` server. The garbage HEAD makes
    /// every fold rebuild and lets the on-demand route past its rate gate.
    /// The caller holds the gate's write guard: nothing else moves the clock
    /// or HEAD's bytes meanwhile. Returns whether a server fold lost its HEAD
    /// CAS to the skewed fold.
    fn fold_race(&self, delay_frac: f64, reason: &str) -> bool {
        let Some(head_key) = lock(&self.head_key).clone() else {
            println!("fold race: no HEAD key known yet");
            return false;
        };
        if !self.nudge(
            clock::fold_race_nudge_s(now_ns() / 1_000_000_000),
            "fold race",
        ) {
            return false;
        }
        let now_s = now_ns() / 1_000_000_000;
        let Some((seal, ahead_hour)) = clock::fold_race_seal(now_s)
            .and_then(|seal| Some((seal, clock::sealed_watermark_hour(seal)?)))
        else {
            println!("fold race: {now_s} is outside the race's window after the nudge");
            return false;
        };
        let n = self.fold_races.fetch_add(1, Ordering::SeqCst);
        // Bytes no earlier race wrote, so HEAD's version is new on a store
        // that derives it from content.
        let garbage = format!("antithesis: not a catalog HEAD (fold race {n})");
        if let Err(e) = self.store.put(&head_key, garbage.as_bytes()) {
            println!("fold race: overwriting HEAD: {e}");
            return false;
        }
        let skewed = format!("+{}", *lock(&self.offset_s) + race::FOLD_SKEW_S);
        let (cli_s, server_s) = *lock(&self.fold_race_timings);
        let delay_s = race::server_delay_s(cli_s, server_s, delay_frac);
        let started = Instant::now();
        let ((cli_out, cli_took), servers) = std::thread::scope(|scope| {
            let cli = scope.spawn(|| {
                let out = self.cli.run_env(
                    &self.args(&["catalog", "fold", "--shards", "4", "--json"]),
                    CLI_TIMEOUT_S,
                    &[("FAKETIME", skewed.as_str())],
                );
                (out, started.elapsed())
            });
            std::thread::sleep(Duration::from_secs_f64(delay_s));
            let posts: Vec<_> = (0..self.ravel.nodes.len())
                .map(|node| {
                    scope.spawn(move || {
                        let t = Instant::now();
                        (self.admin_fold(node), t.elapsed())
                    })
                })
                .collect();
            let servers: Vec<_> = posts
                .into_iter()
                .map(|h| {
                    h.join().unwrap_or_else(|_| {
                        (Err("the post thread panicked".to_string()), Duration::ZERO)
                    })
                })
                .collect();
            let cli = cli.join().unwrap_or_else(|_| {
                (
                    cli::Output {
                        code: None,
                        stdout: String::new(),
                        stderr: "the fold thread panicked".to_string(),
                    },
                    Duration::ZERO,
                )
            });
            (cli, servers)
        });
        let cli_fold = race::parse_cli_fold(&cli_out.stdout);
        let answered: Vec<f64> = servers
            .iter()
            .filter(|(r, _)| r.is_ok())
            .map(|(_, took)| took.as_secs_f64())
            .collect();
        if cli_out.ok() && !answered.is_empty() {
            let mean = answered.iter().sum::<f64>() / answered.len() as f64;
            *lock(&self.fold_race_timings) = (cli_took.as_secs_f64(), mean);
        }
        let folds: Vec<Option<race::ServerFold>> = servers
            .iter()
            .map(|(r, _)| r.as_ref().ok().and_then(|b| race::parse_server_fold(b)))
            .collect();
        let won = cli_fold.as_ref().is_some_and(|c| {
            folds
                .iter()
                .flatten()
                .any(|s| race::overtaken(c, s, ahead_hour))
        });
        let details = json!({
            "reason": reason,
            "race": n,
            "seal_s": seal,
            "ahead_hour": ahead_hour,
            "skew_s": race::FOLD_SKEW_S,
            "server_delay_s": delay_s,
            "cli_s": cli_took.as_secs_f64(),
            "cli": cli_fold.as_ref().map(|c| format!("{c:?}")).unwrap_or_else(|| cli_out.summary()),
            "servers": servers.iter().map(|(r, took)| match r {
                Ok(body) => format!("{body} ({:.1} s)", took.as_secs_f64()),
                Err(e) => e.clone(),
            }).collect::<Vec<_>>(),
        });
        println!("fold race: {details}");
        assert_sometimes!(
            won,
            "catalog: a server fold lost its HEAD CAS to a fold one sealed hour ahead",
            &details
        );
        if won {
            self.fold_overtaken.store(true, Ordering::SeqCst);
        }
        let _ = self.inspect_json();
        won
    }

    /// The audit, then the sweep check over its LIST.
    fn audit(&self, choose: &dyn Fn(usize) -> usize) {
        if let Some((prefix, listed)) = self.audit_head(choose) {
            self.check_swept(&prefix, &listed);
        }
    }

    /// Inspect, list, inspect again: the audit judges only a HEAD that did
    /// not change while the bucket was listed. `choose(n)` picks which of
    /// `n` listed commit records to decode. Returns the tenant prefix and
    /// the LIST for [`Shared::check_swept`], which runs after the second
    /// inspect so that it does not widen the window HEAD must hold still in.
    fn audit_head(&self, choose: &dyn Fn(usize) -> usize) -> Option<(String, BTreeSet<String>)> {
        let before = self.inspect_json()?;
        if before.created_unix_ns.is_none() {
            println!("audit: no HEAD yet");
            return None;
        }
        let text = self
            .cli
            .run(&self.args(&["catalog", "inspect"]), CLI_TIMEOUT_S);
        let inspected = audit::parse_inspect(&text.stdout);
        let unfetched = if text.ok() {
            None
        } else {
            audit::unfetched_part(&text.stderr)
        };
        if !text.ok() && unfetched.is_none() {
            println!("audit: catalog inspect: {}", text.summary());
            return None;
        }
        let Some(prefix) = audit::tenant_prefix(&before.head_key) else {
            println!("audit: unexpected HEAD key {}", before.head_key);
            return None;
        };
        let footprint = self.cli.run(
            &[
                "rlog",
                "footprint",
                "--tenant",
                self.tenant.as_str(),
                "--shards",
                "4",
                "--json",
            ],
            CLI_TIMEOUT_S,
        );
        let resolved: Option<Vec<String>> = if footprint.ok() {
            let keys = audit::footprint_keys(&footprint.stdout);
            if keys.is_none() {
                println!("audit: rlog footprint printed no report");
            }
            keys
        } else {
            // Only a not-found on a data object under the tenant is a
            // verdict; any other failure leaves this pass without one.
            match audit::unfetched_object(&footprint.stderr, &prefix) {
                Some(key) => Some(vec![key]),
                None => {
                    println!(
                        "audit: no verdict on resolved data objects, rlog footprint: {}",
                        footprint.summary()
                    );
                    None
                }
            }
        };
        let listed: BTreeSet<String> = match self.store.list(&prefix) {
            Ok(keys) => keys.into_iter().collect(),
            Err(e) => {
                println!("audit: no verdict, the listing failed: {e}");
                return None;
            }
        };
        let Some(after) = self.inspect_json() else {
            return Some((prefix, listed));
        };
        if after.created_unix_ns != before.created_unix_ns
            || inspected.created_unix_ns != before.created_unix_ns
        {
            println!("audit: HEAD moved while listing; skipped");
            return Some((prefix, listed));
        }
        // A text parse that found nothing, or other parts than the JSON
        // form lists, would check nothing and pass.
        let parse = match (&before.parts, text.ok()) {
            (_, false) => Ok(()),
            (Some(parts), true) => audit::cross_check(&inspected, parts),
            (None, true) => Err("catalog inspect --json lists no usable parts".to_string()),
        };
        let mut named = inspected.named.clone();
        if let Some(k) = unfetched
            && !named.contains(&k)
        {
            named.push(k);
        }
        let missing = audit::missing_named(&named, &listed);
        assert_always!(
            missing.is_empty() && parse.is_ok(),
            "catalog: every object HEAD names exists",
            &json!({
                "head_key": before.head_key,
                "created_unix_ns": before.created_unix_ns,
                "named": named.len(),
                "listed": listed.len(),
                "missing": first(&missing),
                "parse": parse.err(),
            })
        );

        if let Some(resolved) = resolved {
            let missing = audit::missing_named(&resolved, &listed);
            assert_always!(
                missing.is_empty(),
                "catalog: every data object the catalog resolves exists",
                &json!({
                    "resolved": resolved.len(),
                    "listed": listed.len(),
                    "missing": first(&missing),
                })
            );
            assert_sometimes!(
                !resolved.is_empty(),
                "catalog: the audit checked the data objects the catalog resolves",
                &json!({ "resolved": resolved.len() })
            );
        }

        let compactions = audit::compaction_records(&prefix, &listed);
        let decoded = match pick(choose, &compactions) {
            Some(key) => {
                let out = self
                    .cli
                    .run(&["commit", "decode-compaction", key], CLI_TIMEOUT_S);
                if !out.ok() {
                    println!("audit: commit decode-compaction {key}: {}", out.summary());
                }
                out.ok()
            }
            None => false,
        };
        if decoded {
            self.compacted.store(true, Ordering::SeqCst);
        }
        assert_sometimes!(
            decoded,
            "catalog: a compaction record was published",
            &json!({ "compaction_records": compactions.len() })
        );

        let l0 = audit::l0_commit_records(&prefix, &listed);
        if let Some(key) = pick(choose, &l0) {
            let out = self.cli.run(&["commit", "decode", key], CLI_TIMEOUT_S);
            if !out.ok() {
                println!("audit: commit decode {key}: {}", out.summary());
            }
            assert_sometimes!(
                out.ok(),
                "catalog: a listed commit record decoded through ravel-cli",
                &json!({ "commit_records": l0.len() })
            );
        }
        Some((prefix, listed))
    }

    /// A no-token scan of `node` after faults stopped, re-read for up to two
    /// minutes while its only finding is absent seeded records.
    fn settled_scan(&self, node: usize) -> Result<multiset::Verdict, SqlFailure> {
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            match self.scan(node) {
                Ok(v) if v.holds() || !v.upper_bounds_hold() || Instant::now() >= deadline => {
                    return Ok(v);
                }
                Err(e) if Instant::now() >= deadline => return Err(e),
                _ => std::thread::sleep(Duration::from_secs(2)),
            }
        }
    }
}

fn pick<'a>(choose: &dyn Fn(usize) -> usize, keys: &'a [String]) -> Option<&'a str> {
    if keys.is_empty() {
        return None;
    }
    keys.get(choose(keys.len()).min(keys.len() - 1))
        .map(String::as_str)
}

#[hegel::concurrent_state_machine]
impl Workload {
    #[rule(groups = ["main"], weight = 6)]
    fn write_logs(&self, tc: TestCase) {
        let s = &self.0;
        let Some(_g) = s.enter() else {
            return;
        };
        let records = tc.draw(gs::integers::<u32>().min_value(1).max_value(8));
        let streams = tc.draw(gs::integers::<u32>().min_value(1).max_value(4));
        let Some(w) = s.register(records) else {
            return;
        };
        let body = otlp::logs_request(w.id, records, streams, w.ts_ns);
        for attempt_no in 0..=MAX_RETRIES {
            let node = draw_node(&tc, s.ravel.nodes.len());
            let k = s.begin_attempt(w.id);
            let reply = s.ravel.export(
                node,
                "/v1/logs",
                &body,
                w.mode.header(),
                None,
                false,
                WRITE_TIMEOUT,
            );
            let attempt = classify(&reply);
            s.finish_attempt(w.id, k, attempt.clone());
            let retryable = match attempt {
                Attempt::NoResponse => true,
                Attempt::Refused { status } => status == 503 || status == 429,
                _ => false,
            };
            if !retryable || attempt_no == MAX_RETRIES {
                break;
            }
        }
    }

    #[rule(groups = ["main"], weight = 1)]
    fn fold(&self, _tc: TestCase) {
        let s = &self.0;
        let Some(_g) = s.enter() else {
            return;
        };
        s.fold();
    }

    #[rule(groups = ["main"], weight = 1)]
    fn compact(&self, tc: TestCase) {
        let s = &self.0;
        let Some(_g) = s.enter() else {
            return;
        };
        if s.compact() {
            let node = draw_node(&tc, s.ravel.nodes.len());
            s.check_bounds(node, "on-demand compaction");
        }
    }

    #[rule(groups = ["main"], weight = 1)]
    fn sweep(&self, tc: TestCase) {
        let s = &self.0;
        let Some(_g) = s.enter() else {
            return;
        };
        let shard = tc.draw(gs::integers::<u32>().min_value(0).max_value(SHARDS - 1));
        s.sweep(shard);
    }

    #[rule(groups = ["main"], weight = 1)]
    fn jump_clock(&self, tc: TestCase) {
        let s = &self.0;
        if s.stopping.load(Ordering::SeqCst) {
            return;
        }
        let sealed = lock(&s.jump).is_some();
        if sealed
            && (!s.compacted.load(Ordering::SeqCst) || s.sweep_jump_started.load(Ordering::SeqCst))
        {
            return;
        }
        if !sealed && lock(&s.model).acked_logs.len() < s.clock.min_acked {
            return;
        }
        if !tc.draw(gs::weighted_booleans(0.05)) {
            return;
        }
        let _quiet = s.gate.write().unwrap_or_else(|e| e.into_inner());
        if s.stopping.load(Ordering::SeqCst) {
            return;
        }
        if sealed {
            s.sweep_jump("drawn");
        } else {
            s.jump_clock("drawn");
        }
    }

    #[rule(groups = ["main"], weight = 1)]
    fn race_folds(&self, tc: TestCase) {
        let s = &self.0;
        if s.stopping.load(Ordering::SeqCst) || lock(&s.head_key).is_none() {
            return;
        }
        if !tc.draw(gs::weighted_booleans(0.03)) {
            return;
        }
        let frac = f64::from(tc.draw(gs::integers::<u32>().min_value(1).max_value(9))) / 10.0;
        let _quiet = s.gate.write().unwrap_or_else(|e| e.into_inner());
        if s.stopping.load(Ordering::SeqCst) {
            return;
        }
        s.fold_race(frac, "drawn");
    }

    #[rule(groups = ["main"], weight = 1)]
    fn corrupt_head(&self, tc: TestCase) {
        let s = &self.0;
        let Some(_g) = s.enter() else {
            return;
        };
        if !tc.draw(gs::weighted_booleans(0.1)) {
            return;
        }
        let Some(key) = lock(&s.head_key).clone() else {
            return;
        };
        match s.store.put(&key, b"antithesis: not a catalog HEAD") {
            Ok(()) => assert_reachable!(
                "catalog: HEAD was overwritten with garbage through raw S3",
                &json!({ "head_key": key })
            ),
            Err(e) => println!("corrupt HEAD: {e}"),
        }
    }

    #[rule(groups = ["main"], weight = 2)]
    fn audit(&self, tc: TestCase) {
        let s = &self.0;
        let listing = {
            let Some(_g) = s.enter() else {
                return;
            };
            s.audit_head(&|n| draw_node(&tc, n))
        };
        // The sweep check reads only the shared ledger and the store, so it
        // runs without the gate and does not hold off a clock jump.
        if let Some((prefix, listed)) = listing {
            s.check_swept(&prefix, &listed);
        }
    }

    #[rule(groups = ["main"], weight = 2)]
    fn check_bounds(&self, tc: TestCase) {
        let s = &self.0;
        let Some(_g) = s.enter() else {
            return;
        };
        let node = draw_node(&tc, s.ravel.nodes.len());
        s.check_bounds(node, "scan");
    }

    #[rule(groups = ["main"])]
    fn eventually(&self, tc: TestCase) {
        let s = &self.0;
        if !s.eventually_on || s.stopping.load(Ordering::SeqCst) {
            return;
        }
        if !tc.draw(gs::weighted_booleans(0.003)) {
            return;
        }
        run_eventually(s, "drawn");
    }
}

/// Stop faults for good, wait for every node, jump the clocks if no rule
/// did, fold, compact and audit, take the sweep jumps if no rule did, sweep
/// every shard, fold and audit again, then check the whole multiset and end
/// the timeline.
fn run_eventually(s: &Shared, reason: &str) -> ! {
    s.stopping.store(true, Ordering::SeqCst);
    let _only = lock(&s.eventually_lock);
    let _quiet = s.gate.write().unwrap_or_else(|e| e.into_inner());
    println!("eventually ({reason}): stopping faults");
    // The Antithesis platform sets ANTITHESIS_STOP_FAULTS in every container
    // of a test run; the local loop injects no faults and leaves it unset.
    if let Ok(cmd) = std::env::var("ANTITHESIS_STOP_FAULTS") {
        match std::process::Command::new(&cmd).arg("3600").status() {
            Ok(st) => println!("eventually: {cmd} 3600 exited {st}"),
            Err(e) => println!("eventually: running {cmd} failed: {e}"),
        }
    }
    let recovered = s.wait_serving(Duration::from_secs(300), "eventually");
    assert_always!(
        recovered.is_ok(),
        "catalog: the system recovers once faults stop",
        &json!({ "last_error": recovered.as_ref().err() })
    );
    if recovered.is_err() {
        println!("eventually: not recovered, ending the timeline");
        std::process::exit(0);
    }

    if lock(&s.jump).is_none() {
        if lock(&s.model).acked_logs.is_empty() {
            println!("eventually: no live write was acknowledged, so the clocks stay put");
        } else {
            s.jump_clock("eventually");
        }
    }
    if lock(&s.head_key).is_none() {
        let _ = s.inspect_json();
    }
    for attempt in 0..FOLD_RACES_EVENTUALLY {
        if s.fold_overtaken.load(Ordering::SeqCst) {
            break;
        }
        s.fold_race(f64::from(attempt % 3 + 1) / 4.0, "eventually");
    }
    // With faults stopped the fold and the compaction run over every hour
    // the jump sealed, so the multiset below is checked after both.
    for (what, op) in [
        ("fold", Shared::fold as fn(&Shared) -> bool),
        ("compaction", Shared::compact),
    ] {
        let done = (0..3).any(|attempt| {
            if attempt > 0 {
                std::thread::sleep(Duration::from_secs(2));
            }
            op(s)
        });
        if !done {
            println!("eventually: the on-demand {what} failed three times");
        }
    }
    s.audit(&|_| 0);
    if s.compacted.load(Ordering::SeqCst) {
        s.sweep_jump("eventually");
    } else {
        println!("eventually: no compaction record seen, so no sweep jump");
    }
    // Past the pinned-query window this sweep deletes the superseded inputs;
    // the audit and the multiset below then run after the deletion.
    for shard in 0..SHARDS {
        s.sweep(shard);
    }
    if !s.fold() {
        println!("eventually: the fold after the sweep failed");
    }
    s.audit(&|_| 0);

    let acked: Vec<Write> = {
        let m = lock(&s.model);
        m.acked_logs
            .iter()
            .filter_map(|id| m.writes.get(id).cloned())
            .collect()
    };
    println!(
        "eventually: multiset check; {} seeded records, {} acked live writes",
        s.seeded.len(),
        acked.len()
    );
    let n = s.ravel.nodes.len();
    let mut seeded_missing = Vec::new();
    let mut seeded_over = Vec::new();
    let mut phantoms = Vec::new();
    let mut over = Vec::new();
    let mut missing = Vec::new();
    let mut unreadable = Vec::new();
    let mut rows = Vec::new();
    for node in 0..n {
        match s.settled_scan(node) {
            Ok(v) => {
                let tag = |l: &String| format!("node {node} scan: {l}");
                seeded_missing.extend(v.seeded_missing.iter().map(tag));
                seeded_over.extend(v.seeded_over.iter().map(tag));
                phantoms.extend(v.phantoms.iter().map(tag));
                over.extend(v.over.iter().map(tag));
                rows.push(v.rows);
            }
            Err(e) => unreadable.push(format!("node {node} scan: {e}")),
        }
    }
    for (i, chunk) in acked.chunks(TOKEN_BATCH).enumerate() {
        match retry_read(|attempt| s.token_read((i + attempt) % n, chunk)) {
            Ok(j) => {
                phantoms.extend(j.phantoms);
                over.extend(j.over);
                over.extend(j.over_after_replay);
                missing.extend(j.missing);
            }
            Err(e) => unreadable.push(format!(
                "token read of writes {:?}: {e}",
                chunk.iter().map(|w| w.id).collect::<Vec<_>>()
            )),
        }
    }
    let jump = *lock(&s.jump);
    let sweep_jump = *lock(&s.sweep_jump);
    assert_always!(
        seeded_missing.is_empty()
            && seeded_over.is_empty()
            && phantoms.is_empty()
            && over.is_empty()
            && missing.is_empty()
            && unreadable.is_empty(),
        "catalog: compaction preserves the acked multiset",
        &json!({
            "compaction_record_seen": s.compacted.load(Ordering::SeqCst),
            "clock_jump": jump.map(|j| json!({
                "before_ns": j.before_ns,
                "after_ns": j.after_ns,
                "reached_driver": j.reached_driver,
            })),
            "sweep_jump_after_ns": sweep_jump.map(|(_, w)| w.after_ns),
            "sweep_deleted_inputs": lock(&s.sweeps).confirmed(),
            "rows_per_node": rows,
            "acked_live_writes": acked.len(),
            "seeded_missing": first(&seeded_missing),
            "seeded_over": first(&seeded_over),
            "phantoms": first(&phantoms),
            "over": first(&over),
            "missing": first(&missing),
            "unreadable": first(&unreadable),
        })
    );
    println!(
        "eventually: ending the timeline (seeded missing {}, seeded over {}, phantoms {}, \
         over {}, missing {}, unreadable {})",
        seeded_missing.len(),
        seeded_over.len(),
        phantoms.len(),
        over.len(),
        missing.len(),
        unreadable.len()
    );
    std::process::exit(0);
}

/// Retry a read for up to two minutes: nodes that just restarted can answer
/// 503 for a while after reporting ready.
fn retry_read<T>(mut read: impl FnMut(usize) -> Result<T, SqlFailure>) -> Result<T, SqlFailure> {
    let deadline = Instant::now() + Duration::from_secs(120);
    let mut attempt = 0;
    loop {
        match read(attempt) {
            Ok(v) => return Ok(v),
            Err(e) if Instant::now() >= deadline => return Err(e),
            Err(_) => {
                attempt += 1;
                std::thread::sleep(Duration::from_secs(2));
            }
        }
    }
}

/// Wait until `check` passes, printing the reason whenever it changes. Past
/// `deadline` the driver exits non-zero, so setup never reports complete
/// over a container that did not come up.
fn wait_for(what: &str, deadline: Instant, mut check: impl FnMut() -> Result<(), String>) {
    let mut last = String::new();
    loop {
        match check() {
            Ok(()) => {
                println!("startup: {what}");
                return;
            }
            Err(e) if Instant::now() >= deadline => {
                die(&format!("setup gave up waiting until {what}: {e}"));
            }
            Err(e) => {
                if e != last {
                    println!("startup: waiting until {what}: {e}");
                    last = e;
                }
                std::thread::sleep(Duration::from_millis(500));
            }
        }
    }
}

fn die(msg: &str) -> ! {
    eprintln!("driver: {msg}");
    std::process::exit(1);
}

/// The seconds left before `deadline`, at least one.
fn seconds_left(deadline: Instant) -> u32 {
    let left = deadline.saturating_duration_since(Instant::now()).as_secs();
    u32::try_from(left).unwrap_or(u32::MAX).max(1)
}

/// Seed the tenant through `ravel-cli load` and return the plan. Runs before
/// any ravel-server starts: every server waits for the marker this writes.
fn seed(cfg: &Config, store: &Store, cli: &Cli, deadline: Instant) -> Plan {
    match store.get(SEED_MARKER) {
        Ok(None) => {}
        Ok(Some(_)) => die(
            "the seed marker already exists: the seeded expectation and the live \
             model of an earlier driver are gone, so this run cannot judge the store",
        ),
        Err(e) => die(&e),
    }
    let plan = Plan::new(now_ns(), cfg.seed_hours, cfg.seed_stride, cfg.seed_per_hour)
        .unwrap_or_else(|e| die(&e));
    let dir = std::env::temp_dir().join("catalog-maintenance-seed");
    let parquet = dir.join("seed.parquet");
    let mapping = dir.join("mapping.toml");
    if let Err(e) = std::fs::create_dir_all(&dir) {
        die(&format!("{}: {e}", dir.display()));
    }
    if let Err(e) = seed::write_parquet(&plan.rows(), &parquet) {
        die(&e);
    }
    if let Err(e) = std::fs::write(&mapping, seed::mapping()) {
        die(&format!("{}: {e}", mapping.display()));
    }
    let args = plan.load_args(&cfg.tenant, &parquet, &mapping);
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    let out = cli.run(&args, seconds_left(deadline));
    println!(
        "seed: ravel-cli load: {}\n{}",
        out.summary(),
        out.stdout.trim()
    );
    if !out.ok() {
        assert_unreachable!(
            "catalog: the seeded load was refused",
            &json!({ "detail": out.summary() })
        );
        die("the seeded load failed");
    }
    if let Err(e) = seed::check_load_report(&out.stdout, &plan) {
        assert_unreachable!(
            "catalog: the seeded load reported other rows than planned",
            &json!({ "detail": e })
        );
        die(&e);
    }
    let marker = json!({
        "start_hour": plan.start_hour,
        "stride": plan.stride,
        "hours": plan.hours,
        "per_hour": plan.per_hour,
    })
    .to_string();
    if let Err(e) = store.put(SEED_MARKER, marker.as_bytes()) {
        die(&e);
    }
    println!("seed: {marker}");
    plan
}

/// Refuse to start unless the offset file every container reads holds the
/// starting offset and the planned jump seals the live hours.
fn check_clock(plan: &ClockPlan) {
    if let Err(e) = clock::check_sweep_jumps(plan.horizon_jump_s, plan.window_jump_s) {
        die(&e);
    }
    if plan.jump_s < clock::min_jump_s() {
        die(&format!(
            "CLOCK_JUMP_SECS={} is below {}, the jump that seals an hour acknowledged \
             at its first second",
            plan.jump_s,
            clock::min_jump_s()
        ));
    }
    match std::fs::read_to_string(&plan.file) {
        Ok(text) if clock::is_start_offset(&text) => {}
        Ok(text) => die(&format!(
            "{} holds {:?}, not the starting offset +0",
            plan.file.display(),
            text.trim()
        )),
        Err(e) => die(&format!(
            "{}: {e}; every container runs under libfaketime reading this file",
            plan.file.display()
        )),
    }
}

#[hegel::main(phases = [hegel::Phase::Generate])]
fn main(tc: TestCase) {
    antithesis_sdk::antithesis_init();
    let setup_deadline = Instant::now() + SETUP_BUDGET;
    let cfg = Config::from_env();
    check_clock(&cfg.clock);
    let store = Store::from_env().unwrap_or_else(|e| die(&e));
    let cli = Cli::from_env();
    wait_for("the bucket answers LIST", setup_deadline, || {
        store.list("antithesis/").map(|_| ())
    });
    let plan = seed(&cfg, &store, &cli, setup_deadline);
    let seeded = plan.expected(cfg.inject_swap);
    let ravel = Ravel::new(cfg.nodes.clone(), cfg.token.clone()).unwrap_or_else(|e| die(&e));
    let maint = Ravel::new(cfg.maint_nodes.clone(), cfg.token.clone()).unwrap_or_else(|e| die(&e));
    let admin = reqwest::blocking::Client::builder()
        .connect_timeout(Duration::from_secs(2))
        .build()
        .unwrap_or_else(|e| die(&format!("building the HTTP client: {e}")));
    let Config {
        clock,
        token,
        tenant,
        max_writes,
        eventually_on,
        eventually_after,
        nodes,
        maint_nodes,
        ..
    } = cfg;
    let shared = Arc::new(Shared {
        ravel,
        maint,
        cli,
        store,
        tenant,
        plan,
        seeded,
        model: Mutex::new(Model::default()),
        acked_hours: Mutex::new(BTreeSet::new()),
        max_writes,
        head_key: Mutex::new(None),
        compacted: AtomicBool::new(false),
        clock,
        jump: Mutex::new(None),
        sweep_jump_started: AtomicBool::new(false),
        sweep_jump: Mutex::new(None),
        offset_s: Mutex::new(0),
        admin,
        token,
        fold_race_timings: Mutex::new((0.0, 0.0)),
        fold_races: AtomicU64::new(0),
        fold_overtaken: AtomicBool::new(false),
        sweeps: Mutex::new(audit::SweepLedger::default()),
        inputs_of: Mutex::new(BTreeMap::new()),
        gate: RwLock::new(()),
        stopping: AtomicBool::new(false),
        eventually_lock: Mutex::new(()),
        eventually_on,
    });

    for node in 0..shared.ravel.nodes.len() {
        wait_for(
            &format!("{} is ready", shared.ravel.nodes[node]),
            setup_deadline,
            || shared.ravel.ready(node),
        );
    }
    for node in 0..shared.maint.nodes.len() {
        wait_for(
            &format!("{} is ready", shared.maint.nodes[node]),
            setup_deadline,
            || shared.maint.ready(node),
        );
    }
    wait_for("every node serves", setup_deadline, || {
        shared.all_nodes_serve()
    });
    antithesis_sdk::lifecycle::setup_complete(&json!({
        "nodes": nodes,
        "maintain_nodes": maint_nodes,
        "seed_start_hour": shared.plan.start_hour,
        "seeded_records": shared.seeded.len(),
        "clock_jump_s": shared.clock.jump_s,
        "sweep_horizon_jump_s": shared.clock.horizon_jump_s,
        "sweep_window_jump_s": shared.clock.window_jump_s,
    }));
    println!("startup: setup complete; waiting for /workload-active");
    while !Path::new("/workload-active").exists() {
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_reachable!("catalog: the workload starts", &json!({}));

    if let Some(secs) = eventually_after {
        let s = Arc::clone(&shared);
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_secs(secs));
            run_eventually(&s, "forced by WORKLOAD_EVENTUALLY_AFTER_SECS");
        });
    }

    machine(Workload(shared))
        .steps(i64::MAX)
        .max_concurrency(4)
        .run_concurrent(tc);
}
