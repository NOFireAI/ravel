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
//! multiset of seeded and live records. Four forced situations, run as drawn
//! rules and again in the eventually, put the servers where a fault schedule
//! alone rarely reaches: a fold losing its HEAD CAS, a compaction record PUT
//! losing to a concurrent compaction, a sweep past a compaction record that
//! HEAD has not folded, and a sweep past the protection horizon while HEAD
//! is unreadable. Token reads read an acked write back with its commit
//! tokens before and after its input is compacted, folded and swept, and
//! judge every answer exactly against the model. The eventually stops
//! faults, sweeps every shard and checks the whole multiset. See
//! antithesis/catalog-maintenance/README.md for the properties.

mod audit;
mod cli;
mod clock;
mod forced;
mod multiset;
mod race;
mod s3;
mod seed;
mod stop;
mod token;

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

use ravel_antithesis_catalog_driver::{keys, proxy};

use audit::{InputKeys, JsonPart};
use cli::Cli;
use forced::Held;
use s3::Store;
use seed::Plan;
use stop::Stop;

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
/// How long a fold race waits for a server HEAD PUT to be held after the
/// on-demand fold is posted: well under the proxy's hold cap, so the hold does
/// not lapse before the skewed fold and the release.
const FOLD_HELD_WAIT: Duration = Duration::from_secs(10);
const _: () = assert!(FOLD_HELD_WAIT.as_secs() < proxy::HOLD_CAP.as_secs());
/// Fold races the eventually runs until a server fold is overtaken.
const FOLD_RACES_EVENTUALLY: u32 = 6;
/// Record races the eventually runs until a server's record PUT loses: room
/// for a race the store refused for another reason and for races in which
/// no server sent a record PUT in time.
const RECORD_RACES_EVENTUALLY: u32 = 5;
/// How long a record race waits, after the release, for the proxy to report
/// the store's answer to the held PUT.
const HELD_RESULT_WAIT: Duration = Duration::from_secs(10);
/// How long a record race waits for a server to send a record PUT after the
/// nudge: several maintenance intervals.
const HELD_WAIT: Duration = Duration::from_secs(120);
/// Live writes a forced race adds, at most, to give the current hour a
/// compactable bucket.
const FILL_WRITES: u32 = 16;
/// How long the unfolded-record sweep waits after each of its jumps for the
/// maintenance servers' scheduled sweeps: several maintenance intervals.
const UNFOLDED_PASS_WAIT: Duration = Duration::from_secs(150);
const NS_PER_SECOND: u64 = 1_000_000_000;
const FORCED_REASON: &str = "forced by WORKLOAD_EVENTUALLY_AFTER_SECS";
/// How long the timer waits, once it requested the forced eventually, for a
/// rule to start it before running it itself. Longer than any rule takes, so
/// it only fires when no rule runs any more.
const FORCED_BACKSTOP: Duration = Duration::from_secs(600);
/// Writes the eventually reads with their tokens at each lifecycle point.
const EVENTUALLY_TOKEN_READS: usize = 6;
/// How long a token read refused as unsatisfiable waits before its one
/// retry; the refusal's own message asks for a retry.
const UNSATISFIABLE_RETRY: Duration = Duration::from_secs(2);
/// The file `local-run.sh` creates in the driver container once it restarted
/// the node the driver asked it to (`WORKLOAD_LOCAL_RESTART_NODE`).
const LOCAL_RESTART_DONE: &str = "/tmp/local-restart-done";
/// How long the eventually waits for that restart, and then for every node.
const LOCAL_RESTART_WAIT: Duration = Duration::from_secs(300);
/// A clock move waits at most this many quiet spans (`clock::head_quiet_s`)
/// for HEAD to hold still, then gives the move up.
const QUIET_HEAD_TRIES: u32 = 4;
/// How often the quiet wait reads HEAD and renews the proxy's freeze of it.
const QUIET_HEAD_POLL: Duration = Duration::from_secs(1);

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
    /// The servers' HEAD cache TTL as the compose states it
    /// (`HEAD_CACHE_TTL_SECS`), checked against `sys/gc` at startup.
    head_cache_ttl_s: u64,
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
    inject_stale_token: bool,
    /// The local loop only: the host name of the node the eventually asks
    /// `local-run.sh` to restart before its reads after the sweep.
    local_restart: Option<String>,
    eventually_on: bool,
    eventually_after: Option<u64>,
    clock: ClockPlan,
    /// The `s3proxy` container's control port.
    proxy: String,
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
            inject_stale_token: std::env::var("WORKLOAD_INJECT_STALE_TOKEN_RESULT")
                .is_ok_and(|v| v == "1"),
            local_restart: std::env::var("WORKLOAD_LOCAL_RESTART_NODE")
                .ok()
                .filter(|v| !v.is_empty()),
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
                reread_s: env_u64("FAKETIME_CACHE_DURATION", clock::FAKETIME_CACHE_DURATION_S),
                head_cache_ttl_s: env_u64("HEAD_CACHE_TTL_SECS", clock::HEAD_CACHE_TTL_S),
                min_acked: usize::try_from(env_u32("CLOCK_JUMP_MIN_ACKED", 20)).unwrap_or(20),
            },
            proxy: std::env::var("S3_PROXY_CONTROL").unwrap_or_else(|_| "s3proxy:9001".to_string()),
        }
    }
}

/// One move of every clock.
#[derive(Clone, Copy, Debug)]
struct Jump {
    before_ns: u64,
    after_ns: u64,
    /// How far this move took the clocks; 0 when there was nothing to move.
    by_s: u64,
    /// The total offset after this move.
    offset_s: u64,
    reached_driver: bool,
}

/// HEAD held still by the proxy for a clock move, thawed on drop.
struct Frozen<'a> {
    shared: &'a Shared,
    key: String,
    /// HEAD as read at the end of the quiet wait.
    head: clock::HeadRead,
    /// How long HEAD had held still when the wait ended.
    quiet: Duration,
}

impl Drop for Frozen<'_> {
    fn drop(&mut self) {
        let _ = self.shared.proxy_cmd(&format!("thaw-key {}", self.key));
    }
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
    /// Where the two sweep jumps stand; each is written once per timeline.
    sweep_jumps: Mutex<clock::SweepJumps>,
    /// The horizon jump once it reached the driver, with the shards swept
    /// after it, for the window jump's details.
    sweep_horizon: Mutex<Option<(Jump, usize)>>,
    /// The horizon and window jumps, once both reached the driver.
    sweep_jump: Mutex<Option<(Jump, Jump)>>,
    /// The offset the file holds: every jump and nudge so far.
    offset_s: Mutex<u64>,
    /// The client and token of the on-demand fold route.
    admin: reqwest::blocking::Client,
    token: String,
    /// Fold races run; numbers each one's HEAD garbage.
    fold_races: AtomicU64,
    /// Whether a server fold lost its HEAD CAS to the skewed fold.
    fold_overtaken: AtomicBool,
    /// The `s3proxy` control port.
    proxy: String,
    /// Record races run.
    record_races: AtomicU64,
    /// Whether a server's record PUT lost to the `ravel-cli` compaction.
    record_lost: AtomicBool,
    /// Set once the unfolded-record sweep took its first sweep jump; the rule
    /// runs it once per timeline.
    unfolded_started: AtomicBool,
    /// Whether an unfolded-record sweep reached its situation; the
    /// eventually runs one until it has.
    unfolded_reached: AtomicBool,
    /// Set once the unreadable-HEAD sweep started; the rule runs it once per
    /// timeline.
    unreadable_started: AtomicBool,
    /// Unreadable-HEAD sweeps run; numbers each one's HEAD garbage.
    unreadable_sweeps: AtomicU64,
    /// Every key an audit or the sweep jump has listed, and the swept
    /// inputs confirmed gone. Keys are never reused, so one listed here and
    /// absent from a later LIST was deleted.
    sweeps: Mutex<audit::SweepLedger>,
    /// The inputs of each compaction record decoded so far; records are
    /// immutable, so each is decoded once.
    inputs_of: Mutex<BTreeMap<String, Vec<InputKeys>>>,
    /// Every acknowledgement, by node and by write, for the restart
    /// evidence of a token read.
    acks: Mutex<token::AckLedger>,
    /// Orders attempts and their answers in real time.
    ticks: AtomicU64,
    /// The last HEAD an audit saw hold still, with what the catalog
    /// resolved under it.
    head_evidence: Mutex<Option<token::HeadEvidence>>,
    /// The token-read self-check: one returned record counted twice.
    inject_stale_token: bool,
    /// Token reads by outcome class.
    token_tally: Mutex<BTreeMap<&'static str, u64>>,
    /// The local loop only: the index of the node to restart.
    restart_node: Option<usize>,
    /// Every rule holds a read guard while it acts, except for the audit
    /// rule's sweep check; the clock jump and the eventually take the write
    /// guard.
    gate: RwLock<()>,
    stop: Stop,
    eventually_on: bool,
}

struct Workload(Arc<Shared>);

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Halts a rule that saw faults stop while it held the gate, releasing the
/// gate first so the eventually can take it.
fn release_and_halt<G>(guard: G) -> ! {
    drop(guard);
    stop::halt()
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
    watermark_hour: Option<u64>,
}

impl Shared {
    fn args<'a>(&'a self, words: &[&'a str]) -> Vec<&'a str> {
        let mut v = words.to_vec();
        v.extend(["--tenant", self.tenant.as_str(), "--signal", "logs"]);
        v
    }

    /// Every rule's first statement. Runs the forced eventually if it was
    /// requested, and parks the thread for good once faults stopped: a rule
    /// that returned then would let the engine schedule rounds with nothing
    /// to do. Never call it while holding the gate.
    fn halt_if_stopping(&self) {
        if self.stop.forced() {
            run_eventually(self, FORCED_REASON);
        }
        self.stop.halt_if_stopping();
    }

    /// A read guard for one rule; halts once the eventually started.
    fn enter(&self) -> RwLockReadGuard<'_, ()> {
        self.halt_if_stopping();
        let guard = self.gate.read().unwrap_or_else(|e| e.into_inner());
        if self.stop.stopping() {
            release_and_halt(guard);
        }
        guard
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

    /// One strict log write of `records` records over `streams` streams,
    /// retried on a 503, a 429 or no response; `node` picks each attempt's
    /// node. Returns whether it was acknowledged.
    fn write(&self, records: u32, streams: u32, node: &mut dyn FnMut() -> usize) -> bool {
        let Some(w) = self.register(records) else {
            return false;
        };
        let body = otlp::logs_request(w.id, records, streams, w.ts_ns);
        for attempt_no in 0..=MAX_RETRIES {
            let k = self.begin_attempt(w.id);
            let n = node();
            let sent = self.ticks.fetch_add(1, Ordering::SeqCst);
            let reply = self.ravel.export(
                n,
                "/v1/logs",
                &body,
                w.mode.header(),
                None,
                false,
                WRITE_TIMEOUT,
            );
            let attempt = classify(&reply);
            self.record_ack(w.id, n, sent, &attempt);
            self.finish_attempt(w.id, k, attempt.clone());
            let retryable = match attempt {
                Attempt::NoResponse => true,
                Attempt::Refused { status } => status == 503 || status == 429,
                Attempt::Acked { .. } => return true,
                _ => false,
            };
            if !retryable || attempt_no == MAX_RETRIES {
                break;
            }
        }
        false
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

    /// Record which node acknowledged an attempt, under which writer
    /// identity, and when, for a token read's restart evidence.
    fn record_ack(&self, id: u64, node: usize, sent: u64, attempt: &Attempt) {
        let Attempt::Acked { header } = attempt else {
            return;
        };
        let answered = self.ticks.fetch_add(1, Ordering::SeqCst);
        match token::decode_header(header).map(|t| t.into_iter().next()) {
            Ok(Some((_, first))) => lock(&self.acks).record(
                id,
                token::Ack {
                    node,
                    writer: first.writer,
                    sent,
                    answered,
                },
            ),
            Ok(None) => {}
            Err(e) => println!("write {id}: the commit-token header does not decode: {e}"),
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
            watermark_hour: head
                .and_then(|h| h.get("watermark_hour"))
                .and_then(serde_json::Value::as_u64),
        };
        if let (Some(wm), Some(parts)) = (view.watermark_hour, &view.parts) {
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
        // A move given up before its offset write leaves the slot empty, so a
        // later rule or the eventually takes the jump instead.
        let Ok(jump) = self.shift_clock(reason, &|_| self.clock.jump_s) else {
            return;
        };
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
    /// The next sweep deletes the inputs. Like every move, the window jump
    /// first waits for HEAD to hold still for `clock::head_quiet_s` in real
    /// time, so no server still serves the HEAD that named the inputs when
    /// the delete gate opens. The caller holds the gate's write guard for all
    /// of it, as for the sealing jump.
    fn sweep_jump(&self, reason: &str) {
        if lock(&self.jump).is_none_or(|j| !j.reached_driver)
            || !self.compacted.load(Ordering::SeqCst)
        {
            return;
        }
        // The caller holds the gate's write guard, so no other call runs
        // between reading the state and recording the step's outcome. A jump
        // the quiet wait gave up leaves the state as it was, so the next call
        // (a later rule or the eventually) takes that jump.
        let state = *lock(&self.sweep_jumps);
        if state.next() == clock::SweepStep::Horizon {
            // A deleted input counts only if a LIST held it before the delete.
            match self.tenant_prefix() {
                Some(prefix) => match self.store.list(&prefix) {
                    Ok(keys) => lock(&self.sweeps).observe(keys),
                    Err(e) => println!("sweep jump: the listing before the jump failed: {e}"),
                },
                None => {
                    println!("sweep jump: no HEAD key known, so nothing listed before the jump")
                }
            }
            let horizon = self.shift_clock(reason, &|_| self.clock.horizon_jump_s);
            let state = state.after(
                clock::SweepStep::Horizon,
                horizon.as_ref().map(|j| j.reached_driver).map_err(|_| ()),
            );
            *lock(&self.sweep_jumps) = state;
            let Ok(horizon) = horizon else {
                return;
            };
            if !horizon.reached_driver {
                return;
            }
            self.settle("protection-horizon jump");
            if !self.fold() {
                println!("sweep jump: the fold after the horizon jump failed");
            }
            let marked = (0..SHARDS).filter(|&shard| self.sweep(shard)).count();
            *lock(&self.sweep_horizon) = Some((horizon, marked));
        }
        let state = *lock(&self.sweep_jumps);
        if state.next() != clock::SweepStep::Window {
            return;
        }
        let Some((horizon, marked)) = *lock(&self.sweep_horizon) else {
            return;
        };
        let window = self.shift_clock(reason, &|_| self.clock.window_jump_s);
        *lock(&self.sweep_jumps) = state.after(
            clock::SweepStep::Window,
            window.as_ref().map(|j| j.reached_driver).map_err(|_| ()),
        );
        let Ok(window) = window else {
            return;
        };
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
        self.wait_for_server_sweep();
    }

    /// After the window jump: the servers' first sweep is a full pass that
    /// deletes the superseded inputs (`clock::window_jump_floor_s`). Give it
    /// the wait the unfolded sweep gives the servers' sweeps before any sweep
    /// of the driver's own can delete them first, since the driver's reports
    /// no SDK assertions and the SUT's `catalog: a sweep never deletes an
    /// object the current HEAD names` is evaluated only on a server's. The
    /// caller holds the gate's write guard, so no driver sweep runs
    /// meanwhile. The first input a LIST shows gone ends the wait; returns
    /// whether one did.
    fn wait_for_server_sweep(&self) -> bool {
        let Some(prefix) = self.tenant_prefix() else {
            return false;
        };
        let started = Instant::now();
        while started.elapsed() < UNFOLDED_PASS_WAIT {
            std::thread::sleep(Duration::from_secs(10));
            let Ok(keys) = self.store.list(&prefix) else {
                continue;
            };
            let listed: BTreeSet<String> = keys.into_iter().collect();
            let inputs: Vec<InputKeys> =
                lock(&self.inputs_of).values().flatten().cloned().collect();
            if !lock(&self.sweeps).unconfirmed(&inputs, &listed).is_empty() {
                println!(
                    "sweep jump: a superseded input was gone {} s after the window jump",
                    started.elapsed().as_secs()
                );
                return true;
            }
        }
        println!(
            "sweep jump: no superseded input gone within {} s of the window jump",
            UNFOLDED_PASS_WAIT.as_secs()
        );
        false
    }

    /// One raw read of the object at `key`, for the quiet wait.
    fn head_read(&self, key: &str) -> clock::HeadRead {
        match self.store.get(key) {
            Ok(Some(bytes)) => clock::HeadRead::Bytes(bytes),
            Ok(None) => clock::HeadRead::Absent,
            Err(_) => clock::HeadRead::Failed,
        }
    }

    /// Hold HEAD still and wait until it has held still for
    /// `clock::head_quiet_s` in real time (README.md, "The clock jump"). The
    /// proxy refuses every server's PUT of HEAD until the returned freeze is
    /// dropped; the freeze is renewed with every read, so a restarted proxy
    /// gets it back, and a changed or unreadable HEAD, or a freeze the proxy
    /// did not confirm, starts the wait again (`clock::wait_quiet`). `Err`
    /// when no inspect has shown the HEAD key, since HEAD can then be neither
    /// frozen nor read, and after [`QUIET_HEAD_TRIES`] quiet spans without
    /// one.
    fn quiet_head(&self, reason: &str) -> Result<Frozen<'_>, String> {
        let w = clock::QuietWait {
            need: Duration::from_secs(clock::head_quiet_s(self.clock.head_cache_ttl_s)),
            tries: QUIET_HEAD_TRIES,
            poll: QUIET_HEAD_POLL,
        };
        if lock(&self.head_key).is_none() {
            let _ = self.inspect_json();
        }
        let key = lock(&self.head_key).clone();
        // Built before the first freeze, so dropping it thaws on every path.
        let mut frozen = key.as_ref().map(|key| Frozen {
            shared: self,
            key: key.clone(),
            head: clock::HeadRead::Failed,
            quiet: Duration::ZERO,
        });
        let key = key.unwrap_or_default();
        let start = Instant::now();
        let waited = clock::wait_quiet(
            frozen.is_some(),
            w,
            &mut || start.elapsed(),
            &mut std::thread::sleep,
            &mut || self.proxy_cmd(&format!("freeze-key {key}")).is_ok(),
            &mut || self.head_read(&key),
        );
        match (waited, frozen.take()) {
            (Ok((head, quiet)), Some(mut f)) => {
                f.head = head;
                f.quiet = quiet;
                Ok(f)
            }
            (Ok(_), None) => Err("the quiet wait ended with no HEAD key".to_string()),
            (Err(e), _) => {
                println!("clock: giving the move up ({reason}): {e}");
                Err(e)
            }
        }
    }

    /// Raise the total offset by `by_s` of the driver's clock, and wait for
    /// the driver's own clock to move by it. HEAD is held still for the
    /// quiet span first, and `by_s` reads the clock after that wait; it stays
    /// frozen until every other process rereads the offset too, so no fold
    /// moves HEAD while the move lands. libfaketime rereads the file every
    /// `reread_s` seconds; the driver runs under it like every other
    /// container. A move of zero writes nothing. `Err` when the quiet wait
    /// gave the move up, before any write.
    fn shift_clock(&self, reason: &str, by_s: &dyn Fn(u64) -> u64) -> Result<Jump, String> {
        let frozen = self.quiet_head(reason)?;
        let before_ns = now_ns();
        let by_s = by_s(before_ns / NS_PER_SECOND);
        if by_s == 0 {
            return Ok(Jump {
                before_ns,
                after_ns: before_ns,
                by_s: 0,
                offset_s: *lock(&self.offset_s),
                reached_driver: true,
            });
        }
        let offset_s = {
            let mut total = lock(&self.offset_s);
            if let Err(e) = clock::write_offset(&self.clock.file, *total + by_s) {
                assert_unreachable!(
                    "clock: the driver could not write the clock offset",
                    &json!({ "file": self.clock.file.display().to_string(), "detail": e })
                );
                return Ok(Jump {
                    before_ns,
                    after_ns: before_ns,
                    by_s: 0,
                    offset_s: *total,
                    reached_driver: false,
                });
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
        std::thread::sleep(Duration::from_secs(clock::reach_wait_s(
            self.clock.reread_s,
        )));
        let head_moved_while_landing =
            clock::moved_while_landing(&frozen.head, &self.head_read(&frozen.key));
        let details = json!({
            "reason": reason,
            "offset_s": offset_s,
            "by_s": by_s,
            "before_ns": before_ns,
            "after_ns": after_ns,
            "head_quiet_s": frozen.quiet.as_secs_f64(),
            "head_quiet_needed_s": clock::head_quiet_s(self.clock.head_cache_ttl_s),
            "head_moved_while_landing": head_moved_while_landing,
        });
        drop(frozen);
        println!("clock: jumped ({details})");
        if head_moved_while_landing == Some(true) {
            println!(
                "clock: HEAD changed while the move landed, so a server may serve a HEAD older \
                 than the delete gate assumes ({reason})"
            );
            assert_unreachable!("clock: HEAD changed while a clock move landed", &details);
        }
        if !reached_driver {
            assert_unreachable!("clock: the clock jump did not reach the driver", &details);
        }
        Ok(Jump {
            before_ns,
            after_ns,
            by_s,
            offset_s,
            reached_driver,
        })
    }

    /// Move every clock forward by `by_s` of the driver's clock outside the
    /// three planned jumps, and settle; `by_s` is read again after the quiet
    /// wait, so a move aimed at a window still lands in it. The local loop
    /// adds every such move, from the line this prints, to the offset it
    /// expects.
    fn nudge(&self, reason: &str, by_s: &dyn Fn(u64) -> u64) -> bool {
        if by_s(now_ns() / NS_PER_SECOND) == 0 {
            return true;
        }
        let Ok(jump) = self.shift_clock(reason, by_s) else {
            return false;
        };
        if !jump.reached_driver {
            return false;
        }
        if jump.by_s == 0 {
            return true;
        }
        println!(
            "clock: moved forward by {} s outside the planned jumps ({reason})",
            jump.by_s
        );
        self.settle(reason);
        true
    }

    /// After a jump reached the driver: wait one more reread, within which
    /// every other process picks the offset up too, then for every node.
    fn settle(&self, what: &str) {
        std::thread::sleep(Duration::from_secs(clock::reach_wait_s(
            self.clock.reread_s,
        )));
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
    /// just before an hour seals, overwrite HEAD with garbage, hold the next
    /// PUT of the HEAD key at the s3proxy, and post an on-demand fold to one
    /// `--mode all` server so it rebuilds and tries to publish HEAD, the held
    /// PUT. Once that PUT is held, run a `ravel-cli` fold whose clock runs
    /// `race::FOLD_SKEW_S` ahead to completion: it rebuilds with the hour ahead
    /// sealed and lands its HEAD CAS directly on rustfs, replacing the garbage
    /// version. Releasing the held PUT then makes rustfs refuse it (412/409),
    /// since its CAS was conditioned on the garbage version. The garbage HEAD
    /// makes every fold rebuild and lets the on-demand route past its rate
    /// gate. The caller holds the gate's write guard: nothing else moves the
    /// clock or HEAD's bytes meanwhile. Returns whether a server fold lost its
    /// HEAD CAS to the skewed fold.
    fn fold_race(&self, reason: &str) -> bool {
        let Some(head_key) = lock(&self.head_key).clone() else {
            println!("fold race: no HEAD key known yet");
            return false;
        };
        if !self.nudge("fold race", &clock::fold_race_nudge_s) {
            return false;
        }
        let now_s = now_ns() / NS_PER_SECOND;
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
        if self
            .proxy_cmd("pass")
            .and_then(|_| self.proxy_cmd(&format!("hold-key {head_key}")))
            .is_err()
        {
            return false;
        }
        let node = (n as usize) % self.ravel.nodes.len();
        let skewed = format!("+{}", *lock(&self.offset_s) + race::FOLD_SKEW_S);
        let (cli, head_result, (server_result, server_took), reached) =
            std::thread::scope(|scope| {
                let server = scope.spawn(move || {
                    let t = Instant::now();
                    (self.admin_fold(node), t.elapsed())
                });
                let deadline = Instant::now() + FOLD_HELD_WAIT;
                let mut held = false;
                while Instant::now() < deadline {
                    if self
                        .proxy_cmd("held")
                        .ok()
                        .and_then(|r| proxy::parse_held(&r))
                        .is_some_and(|(k, _)| k == head_key)
                    {
                        held = true;
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
                let join_server = |h: std::thread::ScopedJoinHandle<'_, _>| {
                    h.join().unwrap_or_else(|_| {
                        (Err("the post thread panicked".to_string()), Duration::ZERO)
                    })
                };
                if !held {
                    // The server answered first: nothing eligible, or the
                    // on-demand rate gate; no HEAD PUT was held in time.
                    let _ = self.proxy_cmd("pass");
                    return (None, None, join_server(server), false);
                }
                // The skewed fold reaches rustfs directly, not through the
                // proxy, so its CAS lands while the server PUT is held.
                let started = Instant::now();
                let cli_out = self.cli.run_env(
                    &self.args(&["catalog", "fold", "--shards", "4", "--json"]),
                    CLI_TIMEOUT_S,
                    &[("FAKETIME", skewed.as_str())],
                );
                let cli_took = started.elapsed();
                let _ = self.proxy_cmd("release");
                let server = join_server(server);
                let head_result = self
                    .proxy_cmd("held-result")
                    .ok()
                    .and_then(|r| proxy::parse_held_result(&r));
                let _ = self.proxy_cmd("pass");
                (Some((cli_out, cli_took)), head_result, server, true)
            });
        let cli_fold = cli
            .as_ref()
            .and_then(|(out, _)| race::parse_cli_fold(&out.stdout));
        // Only a held PUT of the HEAD key, refused by the store, counts.
        let head_cas_status = head_result
            .as_ref()
            .filter(|(k, _)| *k == head_key)
            .map(|(_, status)| *status);
        let server_fold = server_result
            .as_ref()
            .ok()
            .and_then(|b| race::parse_server_fold(b));
        let won = cli_fold
            .as_ref()
            .is_some_and(|c| race::overtaken(c, head_cas_status, ahead_hour));
        let details = json!({
            "reason": reason,
            "race": n,
            "seal_s": seal,
            "ahead_hour": ahead_hour,
            "skew_s": race::FOLD_SKEW_S,
            "reached": reached,
            "cli_s": cli.as_ref().map(|(_, took)| took.as_secs_f64()),
            "cli": cli_fold.as_ref().map(|c| format!("{c:?}"))
                .or_else(|| cli.as_ref().map(|(out, _)| out.summary())),
            "head_cas_status": head_cas_status,
            "server_s": server_took.as_secs_f64(),
            "server": match &server_result {
                Ok(body) => server_fold
                    .as_ref()
                    .map(|f| format!("{f:?}"))
                    .unwrap_or_else(|| body.clone()),
                Err(e) => e.clone(),
            },
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

    /// The tenant prefix, inspecting HEAD first if no inspect has shown it.
    fn known_prefix(&self) -> Option<String> {
        if self.tenant_prefix().is_none() {
            let _ = self.inspect_json();
        }
        self.tenant_prefix()
    }

    /// One command to the `s3proxy` control port.
    fn proxy_cmd(&self, command: &str) -> Result<String, String> {
        let reply = proxy::control(&self.proxy, command);
        if let Err(e) = &reply {
            println!("s3 proxy: {e}");
        }
        reply
    }

    /// The proxy's `held-result` once it names `key`, polled for up to
    /// [`HELD_RESULT_WAIT`] after the release; else the last reply seen.
    fn held_result_of(&self, key: &str) -> Option<(String, u16)> {
        let deadline = Instant::now() + HELD_RESULT_WAIT;
        loop {
            let result = self
                .proxy_cmd("held-result")
                .ok()
                .and_then(|r| proxy::parse_held_result(&r));
            if result.as_ref().is_some_and(|(k, _)| k == key) || Instant::now() >= deadline {
                return result;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// A `(hour, shard)` bucket of the current or the previous hour that a
    /// compaction would publish a record for, after up to [`FILL_WRITES`]
    /// live writes if none has one yet. The caller holds the gate's write
    /// guard, so no other write lands meanwhile.
    fn compactable_bucket(&self, prefix: &str, what: &str) -> Option<(u64, u32)> {
        let mut next_node = 0usize;
        for writes in 0..=FILL_WRITES {
            let hour = now_ns() / NS_PER_HOUR;
            let hours = [hour.saturating_sub(1), hour];
            let mut listed = BTreeSet::new();
            for h in hours {
                for shard in 0..SHARDS {
                    match self.store.list(&keys::bucket_prefix(prefix, shard, h)) {
                        Ok(keys) => listed.extend(keys),
                        Err(e) => {
                            println!("{what}: listing the buckets of hour {h}: {e}");
                            return None;
                        }
                    }
                }
            }
            if let Some(found) = forced::compactable_bucket(prefix, hours, SHARDS, &listed) {
                return Some(found);
            }
            if writes == FILL_WRITES {
                println!("{what}: no compactable bucket in hour {hour} or the one before");
                break;
            }
            let n = self.ravel.nodes.len().max(1);
            self.write(4, 4, &mut || {
                next_node += 1;
                next_node % n
            });
        }
        None
    }

    /// One record race (README.md, "The forced races"): nudge every clock
    /// until a compactable bucket seals for compaction, and once the move
    /// reached every clock hold the next compaction record PUT a server
    /// sends. While the PUT is held run a `ravel-cli` compaction of the held
    /// record's bucket, which publishes the same record key first. The
    /// released create-only PUT then finds the record in place, and the store
    /// answers it 412. The caller holds the gate's write guard. Returns
    /// whether the server's PUT lost.
    fn record_race(&self, reason: &str) -> bool {
        let Some(prefix) = self.known_prefix() else {
            println!("record race: no HEAD key known yet");
            return false;
        };
        let Some((hour, shard)) = self.compactable_bucket(&prefix, "record race") else {
            return false;
        };
        if self.proxy_cmd("pass").is_err() {
            return false;
        }
        let n = self.record_races.fetch_add(1, Ordering::SeqCst);
        let lost = self.record_race_held(n, &prefix, (hour, shard), reason);
        let _ = self.proxy_cmd("pass");
        if lost {
            self.record_lost.store(true, Ordering::SeqCst);
        }
        lost
    }

    /// [`Shared::record_race`] once the proxy runs with no rules.
    fn record_race_held(&self, n: u64, prefix: &str, bucket: (u64, u32), reason: &str) -> bool {
        let nudge_s =
            |now_s| clock::seal_nudge_s(now_s, bucket.0, clock::compaction_seal_margin_s());
        // The nudge's settle waits `clock::reach_wait_s` after the move
        // reached the driver, so every record PUT held below was signed on
        // the moved clock, which rustfs shares: none is refused for skew.
        if !self.nudge("record race", &nudge_s) || self.proxy_cmd("hold-record").is_err() {
            return false;
        }
        let deadline = Instant::now() + HELD_WAIT;
        let (key, record, age) = loop {
            let Ok(reply) = self.proxy_cmd("held") else {
                return false;
            };
            match forced::judge_held(&reply, prefix) {
                Held::Race { key, record, age } => break (key, record, age),
                Held::Skip(key) => {
                    println!("record race: released {key}, not a PUT to race");
                    if self.proxy_cmd("release").is_err() {
                        return false;
                    }
                    std::thread::sleep(Duration::from_millis(500));
                    if self.proxy_cmd("hold-record").is_err() {
                        return false;
                    }
                }
                Held::Nothing => {}
            }
            if Instant::now() >= deadline {
                println!(
                    "record race: no server sent a compaction record PUT within {} s of the nudge",
                    HELD_WAIT.as_secs()
                );
                return false;
            }
            std::thread::sleep(Duration::from_millis(250));
        };
        let shard_arg = record.shard.to_string();
        let hour_arg = record.hour.to_string();
        let args = self.args(&[
            "maintain",
            "compact-bucket",
            "--shard",
            &shard_arg,
            "--hour",
            &hour_arg,
            "--no-claim",
        ]);
        let budget = forced::cli_budget(age);
        let started = Instant::now();
        let ((cli_out, cli_took), before, released_at_age, held_result, after) =
            std::thread::scope(|scope| {
                let cli = scope.spawn(|| {
                    let out = self.cli.run(&args, CLI_TIMEOUT_S);
                    (out, started.elapsed())
                });
                while !cli.is_finished() && started.elapsed() < budget {
                    std::thread::sleep(Duration::from_millis(100));
                }
                let before = self.store.get(&key);
                let released_at_age = age + started.elapsed();
                let released = self.proxy_cmd("release").ok().map(|_| released_at_age);
                let cli = cli.join().unwrap_or_else(|_| {
                    (
                        cli::Output {
                            code: None,
                            stdout: String::new(),
                            stderr: "the compaction thread panicked".to_string(),
                        },
                        Duration::ZERO,
                    )
                });
                // The store's answer to the released PUT, once the proxy read
                // it; then time for the server's handling of that answer.
                let held_result = self.held_result_of(&key);
                std::thread::sleep(Duration::from_secs(2));
                (cli, before, released, held_result, self.store.get(&key))
            });
        let held_status = forced::held_status(&key, held_result.as_ref());
        let published_before_release = matches!(before, Ok(Some(_)));
        let lost = forced::record_lost(published_before_release, released_at_age, held_status);
        let changed = match (&before, &after) {
            (Ok(Some(b)), Ok(Some(a))) => Some(a != b),
            _ => None,
        };
        let details = json!({
            "reason": reason,
            "race": n,
            "bucket_hour": bucket.0,
            "bucket_shard": bucket.1,
            "held_key": key,
            "held_age_ms": age.as_millis(),
            "cli_budget_s": budget.as_secs_f64(),
            "cli_s": cli_took.as_secs_f64(),
            "cli": cli_out.summary(),
            "published_before_release": before.as_ref().map(Option::is_some),
            "released": released_at_age.is_some(),
            "released_at_age_ms": released_at_age.map(|a| a.as_millis()),
            "held_status": held_status,
            "held_result": held_result.as_ref().map(|(k, s)| format!("{k} {s}")),
            "record_bytes_changed_after_release": changed,
        });
        println!("record race: {details}");
        assert_sometimes!(
            lost,
            "catalog: a server's compaction record PUT lost to a concurrent compaction",
            &details
        );
        lost
    }

    /// The unfolded-record sweep (README.md, "The forced races"): with every
    /// server's compaction record PUT refused, nudge every clock until a
    /// compactable bucket's hour seals for the fold and fold it into HEAD,
    /// refuse every server's PUT of HEAD, publish the bucket's compaction
    /// record through `ravel-cli`, then jump every clock past the record's
    /// protection horizon and, once the servers' sweeps ran, past the
    /// pinned-query window and the interior re-verify interval. HEAD names
    /// the record's inputs throughout, so the servers' sweeps must keep
    /// them. The caller holds the gate's write guard. Returns whether the
    /// sequence reached that situation.
    fn unfolded_sweep(&self, reason: &str) -> bool {
        let Some(prefix) = self.known_prefix() else {
            println!("unfolded sweep: no HEAD key known yet");
            return false;
        };
        let Some(bucket) = self.compactable_bucket(&prefix, "unfolded sweep") else {
            return false;
        };
        if self
            .proxy_cmd("pass")
            .and_then(|_| self.proxy_cmd("refuse-records"))
            .is_err()
        {
            return false;
        }
        let reached = self.unfolded_sweep_refused(&prefix, bucket, reason);
        // The driver's own check of what the sweep's HEAD gate protects,
        // before any fold can move HEAD on.
        self.audit(&|_| 0);
        let _ = self.proxy_cmd("pass");
        if reached {
            self.unfolded_reached.store(true, Ordering::SeqCst);
        }
        reached
    }

    /// [`Shared::unfolded_sweep`] once the proxy refuses record PUTs.
    fn unfolded_sweep_refused(
        &self,
        prefix: &str,
        (hour, shard): (u64, u32),
        reason: &str,
    ) -> bool {
        let nudge_s = |now_s| clock::seal_nudge_s(now_s, hour, clock::seal_margin_s());
        if !self.nudge("unfolded sweep: fold seal", &nudge_s) {
            return false;
        }
        let folded = (0..3).find_map(|attempt| {
            if attempt > 0 {
                std::thread::sleep(Duration::from_secs(2));
            }
            if !self.fold() {
                return None;
            }
            self.inspect_json()
                .filter(|v| v.watermark_hour.is_some_and(|w| w >= hour))
        });
        let Some(view) = folded else {
            println!("unfolded sweep: no fold folded hour {hour}");
            return false;
        };
        let bucket_prefix = keys::bucket_prefix(prefix, shard, hour);
        match self.store.list(&bucket_prefix) {
            Ok(listed) if keys::bucket(&bucket_prefix, &listed).compactable() => {}
            Ok(listed) => {
                println!(
                    "unfolded sweep: {bucket_prefix} is no longer compactable: {:?}",
                    keys::bucket(&bucket_prefix, &listed)
                );
                return false;
            }
            Err(e) => {
                println!("unfolded sweep: listing {bucket_prefix}: {e}");
                return false;
            }
        }
        if self
            .proxy_cmd(&format!("refuse-key {}", view.head_key))
            .is_err()
        {
            return false;
        }
        let head = match self.store.get(&view.head_key) {
            Ok(Some(bytes)) => bytes,
            other => {
                println!("unfolded sweep: reading HEAD: {other:?}");
                return false;
            }
        };
        let shard_arg = shard.to_string();
        let hour_arg = hour.to_string();
        let out = self.cli.run(
            &self.args(&[
                "maintain",
                "compact-bucket",
                "--shard",
                &shard_arg,
                "--hour",
                &hour_arg,
                "--no-claim",
            ]),
            CLI_TIMEOUT_S,
        );
        if !out.ok() {
            println!("unfolded sweep: maintain compact-bucket: {}", out.summary());
            return false;
        }
        let records = match self.store.list(&bucket_prefix) {
            Ok(listed) => keys::bucket(&bucket_prefix, &listed).records,
            Err(e) => {
                println!("unfolded sweep: listing {bucket_prefix}: {e}");
                return false;
            }
        };
        let [record] = records.as_slice() else {
            println!(
                "unfolded sweep: {bucket_prefix} holds {} compaction records after the compaction",
                records.len()
            );
            return false;
        };
        let decoded = self
            .cli
            .run(&["commit", "decode-compaction", record], CLI_TIMEOUT_S);
        let inputs = if decoded.ok() {
            audit::compaction_inputs(prefix, record, &decoded.stdout)
        } else {
            None
        };
        let Some(inputs) = inputs else {
            println!(
                "unfolded sweep: commit decode-compaction {record}: {}",
                decoded.summary()
            );
            return false;
        };
        lock(&self.inputs_of).insert(record.clone(), inputs.clone());
        let before: BTreeSet<String> = match self.store.list(prefix) {
            Ok(keys) => keys.into_iter().collect(),
            Err(e) => {
                println!("unfolded sweep: listing {prefix}: {e}");
                return false;
            }
        };
        lock(&self.sweeps).observe(before.iter().cloned());
        let present = forced::present_inputs(&inputs, &before);
        self.unfolded_started.store(true, Ordering::SeqCst);
        if !self.nudge("unfolded sweep: protection horizon", &|_| {
            self.clock.horizon_jump_s
        }) {
            return false;
        }
        std::thread::sleep(UNFOLDED_PASS_WAIT);
        if !self.nudge("unfolded sweep: pinned-query window", &|_| {
            clock::unfolded_window_jump_s()
        }) {
            return false;
        }
        // The servers' next full sweep pass would delete a marked input; a
        // deletion ends the wait early.
        let deadline = Instant::now() + UNFOLDED_PASS_WAIT;
        let mut after: Option<BTreeSet<String>> = None;
        while Instant::now() < deadline {
            std::thread::sleep(Duration::from_secs(10));
            if let Ok(keys) = self.store.list(prefix) {
                let listed: BTreeSet<String> = keys.into_iter().collect();
                let any_gone = present.iter().any(|k| !listed.contains(k));
                after = Some(listed);
                if any_gone {
                    break;
                }
            }
        }
        let gone: Vec<String> = after.as_ref().map_or_else(Vec::new, |listed| {
            present
                .iter()
                .filter(|k| !listed.contains(*k))
                .filter(|k| matches!(self.store.get(k), Ok(None)))
                .cloned()
                .collect()
        });
        let head_after = self.store.get(&view.head_key).ok().flatten();
        let record_present = matches!(self.store.get(record), Ok(Some(_)));
        let reached =
            forced::unfolded_reached(&head, head_after.as_deref(), record_present, present.len());
        let details = json!({
            "reason": reason,
            "bucket_hour": hour,
            "bucket_shard": shard,
            "record": record,
            "head_key": view.head_key,
            "head_unchanged": head_after.as_deref() == Some(head.as_slice()),
            "record_present": record_present,
            "inputs_before": present.len(),
            "listed_after": after.is_some(),
            "inputs_deleted": first(&gone),
            "horizon_jump_s": self.clock.horizon_jump_s,
            "window_jump_s": clock::unfolded_window_jump_s(),
            "offset_s": *lock(&self.offset_s),
        });
        println!("unfolded sweep: {details}");
        assert_sometimes!(
            reached,
            "catalog: the clocks passed a compaction record's protection horizon and \
             pinned-query window while HEAD had not folded it",
            &details
        );
        reached
    }

    /// The unreadable-HEAD sweep (README.md, "The forced races"): refuse every
    /// server's PUT of HEAD, PUT garbage over HEAD through raw S3, move every
    /// clock past the sweep's protection horizon and wait for the servers'
    /// sweep passes, which read the undecodable HEAD and must delete nothing
    /// on its account. Then lift the refusal and rebuild HEAD with a
    /// `ravel-cli` fold. The caller holds the gate's write guard. Returns
    /// whether HEAD stayed unreadable while the clocks passed the horizon;
    /// that a server's sweep read it is the SUT's own "delete pass refused on
    /// an unreadable HEAD", which local-run.sh requires.
    fn unreadable_head_sweep(&self, reason: &str) -> bool {
        let (Some(prefix), Some(head_key)) = (self.known_prefix(), lock(&self.head_key).clone())
        else {
            println!("unreadable-HEAD sweep: no HEAD key known yet");
            return false;
        };
        let Some(snap) = forced::snap_prefix(&head_key) else {
            println!("unreadable-HEAD sweep: {head_key} is not a catalog HEAD key");
            return false;
        };
        self.unreadable_started.store(true, Ordering::SeqCst);
        let reached = self
            .proxy_cmd("pass")
            .and_then(|_| self.proxy_cmd(&format!("refuse-key {head_key}")))
            .is_ok()
            && self.unreadable_head_sweep_refused(&head_key, &prefix, &snap, reason);
        let _ = self.proxy_cmd("pass");
        let rebuilt = (0..3).any(|attempt| {
            if attempt > 0 {
                std::thread::sleep(Duration::from_secs(2));
            }
            self.fold()
        });
        if !rebuilt {
            println!("unreadable-HEAD sweep: the fold that rebuilds HEAD failed three times");
        }
        reached
    }

    /// [`Shared::unreadable_head_sweep`] once the proxy refuses HEAD PUTs.
    fn unreadable_head_sweep_refused(
        &self,
        head_key: &str,
        prefix: &str,
        snap: &str,
        reason: &str,
    ) -> bool {
        let n = self.unreadable_sweeps.fetch_add(1, Ordering::SeqCst);
        let garbage = format!("antithesis: not a catalog HEAD (unreadable-HEAD sweep {n})");
        if let Err(e) = self.store.put(head_key, garbage.as_bytes()) {
            println!("unreadable-HEAD sweep: overwriting HEAD: {e}");
            return false;
        }
        let before: BTreeSet<String> = match self.store.list(prefix) {
            Ok(keys) => keys.into_iter().collect(),
            Err(e) => {
                println!("unreadable-HEAD sweep: listing {prefix}: {e}");
                return false;
            }
        };
        lock(&self.sweeps).observe(before.iter().cloned());
        let inputs: Vec<InputKeys> = lock(&self.inputs_of).values().flatten().cloned().collect();
        let inputs_before = forced::present_inputs(&inputs, &before);
        if !self.nudge("unreadable-HEAD sweep: protection horizon", &|_| {
            self.clock.horizon_jump_s
        }) {
            return false;
        }
        std::thread::sleep(UNFOLDED_PASS_WAIT);
        let head_after = self.store.get(head_key).ok().flatten();
        let after: Option<BTreeSet<String>> = match self.store.list(prefix) {
            Ok(keys) => Some(keys.into_iter().collect()),
            Err(e) => {
                println!("unreadable-HEAD sweep: listing {prefix} after the wait: {e}");
                None
            }
        };
        let reached = forced::unreadable_head_reached(head_after.as_deref(), garbage.as_bytes());
        let snaps_before = forced::deleted_under(snap, &before, &BTreeSet::new());
        let (snaps_deleted, inputs_deleted) =
            after.as_ref().map_or((Vec::new(), Vec::new()), |a| {
                (
                    forced::deleted_under(snap, &before, a),
                    inputs_before
                        .iter()
                        .filter(|k| !a.contains(*k))
                        .cloned()
                        .collect(),
                )
            });
        let details = json!({
            "reason": reason,
            "sweep": n,
            "head_key": head_key,
            "head_still_garbage": reached,
            "head_after_len": head_after.as_ref().map(Vec::len),
            "snapshots_before": snaps_before.len(),
            "snapshots_after": after.as_ref().map(|a| forced::deleted_under(snap, a, &BTreeSet::new()).len()),
            "snapshots_deleted": first(&snaps_deleted),
            "superseded_inputs_before": inputs_before.len(),
            "superseded_inputs_deleted": first(&inputs_deleted),
            "listed_after": after.is_some(),
            "horizon_jump_s": self.clock.horizon_jump_s,
            "protection_horizon_s": clock::protection_horizon_s(),
            "wait_s": UNFOLDED_PASS_WAIT.as_secs(),
            "offset_s": *lock(&self.offset_s),
        });
        println!("unreadable-HEAD sweep: {details}");
        assert_sometimes!(
            reached,
            "catalog: HEAD stayed unreadable while the clocks passed the protection horizon",
            &details
        );
        reached
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
        // Only a footprint that resolved everything lists HEAD's objects in
        // full; a failed one names just the object it could not fetch.
        if let (true, Some(resolved), Some(created), Some(wm)) = (
            footprint.ok(),
            &resolved,
            before.created_unix_ns,
            before.watermark_hour,
        ) {
            *lock(&self.head_evidence) = Some(token::HeadEvidence {
                created_unix_ns: created,
                watermark_hour: wm,
                resolved: resolved.iter().cloned().collect(),
            });
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

    /// The acked live writes a token read can judge exactly: one attempt's
    /// worth of copies at most (`max_copies() == 1`), so every acked record
    /// must come back exactly once. `pool` 0 is all of them, 1 those with an
    /// input a decoded compaction record names, 2 those with an input
    /// commit record the sweep check confirmed deleted.
    fn token_candidates(&self, prefix: &str, pool: u64) -> Vec<Write> {
        let writes: Vec<Write> = {
            let m = lock(&self.model);
            m.acked_logs
                .iter()
                .filter_map(|id| m.writes.get(id))
                .filter(|w| w.max_copies() == 1)
                .cloned()
                .collect()
        };
        if pool == 0 {
            return writes;
        }
        let compacted: BTreeSet<String> = lock(&self.inputs_of)
            .values()
            .flatten()
            .map(|i| i.commit.clone())
            .collect();
        let sweeps = lock(&self.sweeps);
        writes
            .into_iter()
            .filter(|w| {
                let Some(Ok(tokens)) = w.acked_header().map(token::decode_header) else {
                    return false;
                };
                tokens.iter().any(|(_, t)| {
                    let key = t.commit_key(prefix);
                    if pool == 1 {
                        compacted.contains(&key)
                    } else {
                        sweeps.is_confirmed(&key)
                    }
                })
            })
            .collect()
    }

    /// Whether `key` is in the store: from `listed` when it holds the key,
    /// else from a GET. `None` when the GET failed.
    fn present(&self, key: &str, listed: Option<&BTreeSet<String>>) -> Option<bool> {
        if listed.is_some_and(|l| l.contains(key)) {
            return Some(true);
        }
        match self.store.get(key) {
            Ok(found) => Some(found.is_some()),
            Err(e) => {
                println!("token read: GET {key}: {e}");
                None
            }
        }
    }

    /// LIST each token's bucket, decode its compaction records not decoded
    /// yet, and return every bucket's listing and the decoded records.
    #[allow(clippy::type_complexity)]
    fn token_buckets(
        &self,
        prefix: &str,
        tokens: &[(String, token::Token)],
    ) -> (
        BTreeMap<String, BTreeSet<String>>,
        BTreeMap<String, Vec<InputKeys>>,
    ) {
        let mut listings = BTreeMap::new();
        for (_, t) in tokens {
            let bucket = keys::bucket_prefix(prefix, t.shard, u64::from(t.hour));
            if listings.contains_key(&bucket) {
                continue;
            }
            match self.store.list(&bucket) {
                Ok(listed) => {
                    let listed: BTreeSet<String> = listed.into_iter().collect();
                    for record in keys::bucket(&bucket, &listed).records {
                        if lock(&self.inputs_of).contains_key(&record) {
                            continue;
                        }
                        let out = self
                            .cli
                            .run(&["commit", "decode-compaction", &record], CLI_TIMEOUT_S);
                        match audit::compaction_inputs(prefix, &record, &out.stdout) {
                            Some(inputs) if out.ok() => {
                                lock(&self.inputs_of).insert(record, inputs);
                            }
                            _ => println!(
                                "token read: commit decode-compaction {record}: {}",
                                out.summary()
                            ),
                        }
                    }
                    listings.insert(bucket, listed);
                }
                Err(e) => println!("token read: listing {bucket}: {e}"),
            }
        }
        (listings, lock(&self.inputs_of).clone())
    }

    /// One token read of write `w` on `node`, retried once if refused as
    /// unsatisfiable.
    fn token_read_once(&self, node: usize, w: &Write, tokens: &[String]) -> token::Outcome {
        let start = w.ts_ns.saturating_sub(WINDOW_SLACK_NS) as f64 / 1e9;
        let end = w.ts_ns.saturating_add(WINDOW_SLACK_NS) as f64 / 1e9;
        let read = || {
            let value = self.ravel.sql(
                node,
                &writes_query(&[w.id]),
                start,
                end,
                tokens,
                READ_TIMEOUT,
            )?;
            model::log_counts(ravel::rows(&value)).map_err(SqlFailure::BadBody)
        };
        let mut result = read();
        if let Err(e) = &result
            && token::failure_class(e) == "unsatisfiable"
        {
            std::thread::sleep(UNSATISFIABLE_RETRY);
            result = read();
        }
        match result {
            Ok(counts) => {
                let m = lock(&self.model);
                token::Outcome::Conclusive(token::judge(&m, counts, w.id, self.inject_stale_token))
            }
            Err(e) => match token::failure_class(&e) {
                "unsatisfiable" => token::Outcome::Unsatisfiable(e.to_string()),
                class => token::Outcome::Inconclusive {
                    class,
                    detail: e.to_string(),
                },
            },
        }
    }

    /// Read write `w` back with its commit tokens on `node` (twice, the
    /// second on a warm cache, with `twice`), judge each answer against the
    /// model, and record which lifecycle state each token's input was in:
    /// decided before the read from the bucket's LIST, the decoded
    /// compaction records, a GET of the commit record and the last stable
    /// HEAD the audit saw, and after the read from the commit record's
    /// presence. Returns whether every read that gave a verdict held.
    fn lifecycle_read(&self, w: &Write, node: usize, twice: bool, reason: &str) -> bool {
        let Some(prefix) = self.tenant_prefix() else {
            println!("token read: no HEAD key known yet");
            return true;
        };
        let Some(header) = w.acked_header() else {
            return true;
        };
        let tokens = match token::decode_header(header) {
            Ok(t) => t,
            Err(e) => {
                println!("token read: write {}: {e}", w.id);
                return true;
            }
        };
        let (listings, records) = self.token_buckets(&prefix, &tokens);
        let head = lock(&self.head_evidence).clone();
        let restart = lock(&self.acks).restarted_since(w.id, node);
        let mut states: Vec<token::InputState> = tokens
            .iter()
            .map(|(_, t)| {
                let key = t.commit_key(&prefix);
                let bucket = keys::bucket_prefix(&prefix, t.shard, u64::from(t.hour));
                let cover = token::covering(&key, &records);
                token::InputState {
                    commit_before: self.present(&key, listings.get(&bucket)),
                    head_named_output: match (cover, &head) {
                        (Some((record, input)), Some(h)) => h.names_output(t.hour, record, input),
                        _ => false,
                    },
                    covering_record: cover.map(|(r, _)| r.to_string()),
                    commit_key: key,
                    commit_after: None,
                }
            })
            .collect();
        let qualified: Vec<String> = w.tokens("logs");
        let mut outcomes = vec![self.token_read_once(node, w, &qualified)];
        if twice {
            outcomes.push(self.token_read_once(node, w, &qualified));
        }
        for s in &mut states {
            if s.covering_record.is_some() {
                s.commit_after = self.present(&s.commit_key, None);
            }
        }
        let tally: BTreeMap<&'static str, u64> = {
            let mut t = lock(&self.token_tally);
            for o in &outcomes {
                *t.entry(o.class()).or_insert(0) += 1;
            }
            t.clone()
        };
        let state_json: Vec<serde_json::Value> = states.iter().map(|s| s.to_json()).collect();
        let restart_json = restart.as_ref().map(|(x, y)| {
            json!({
                "write_acked_by_node": lock(&self.acks).of_write(w.id).map(|a| a.node),
                "writer_before": x.writer,
                "writer_after": y.writer,
                "ticks": [x.sent, x.answered, y.sent, y.answered],
            })
        });
        let base = json!({
            "reason": reason,
            "write": w.id,
            "records": w.records,
            "node": self.ravel.nodes[node],
            "tokens": tokens.len(),
            "inputs": state_json,
            "head": head.as_ref().map(|h| json!({
                "created_unix_ns": h.created_unix_ns,
                "watermark_hour": h.watermark_hour,
                "resolved": h.resolved.len(),
            })),
            "restart": restart_json,
            "tally": tally,
        });
        let mut all_hold = true;
        for (i, o) in outcomes.iter().enumerate() {
            let mut details = base.clone();
            details["read"] = json!(i);
            details["outcome"] = json!(o.class());
            match o {
                token::Outcome::Conclusive(j) => {
                    details["rows"] = json!(j.rows);
                    details["injected"] = json!(j.injected);
                    details["phantoms"] = first(&j.phantoms);
                    details["over"] = first(&j.over);
                    details["missing"] = first(&j.missing);
                }
                token::Outcome::Unsatisfiable(e) => details["refusal"] = json!(e),
                token::Outcome::Inconclusive { detail, .. } => details["detail"] = json!(detail),
            }
            println!("token read: {details}");
            if o.conclusive() {
                assert_always!(
                    o.holds(),
                    "catalog: a token read returns each acked record of its write exactly once",
                    &details
                );
                all_hold &= o.holds();
            }
        }
        let first_verdict = outcomes[0].conclusive();
        assert_sometimes!(
            first_verdict
                && states
                    .iter()
                    .any(token::InputState::compacted_record_present),
            "catalog: a token read ran after its input was compacted",
            &base
        );
        assert_sometimes!(
            first_verdict && states.iter().any(|s| s.head_named_output),
            "catalog: a token read ran after HEAD named the compaction output",
            &base
        );
        assert_sometimes!(
            first_verdict && states.iter().any(token::InputState::swept),
            "catalog: a token read ran after its input commit record was swept",
            &base
        );
        assert_sometimes!(
            outcomes.len() == 2 && outcomes.iter().all(token::Outcome::conclusive),
            "catalog: a token read ran twice on one node",
            &base
        );
        assert_sometimes!(
            first_verdict && restart.is_some(),
            "catalog: a token read ran on a node restarted since the write",
            &base
        );
        all_hold
    }

    /// The eventually's token reads: up to [`EVENTUALLY_TOKEN_READS`] writes
    /// of `pool` (of pool 1 when `pool` is empty), spread over the acked
    /// writes, each read twice on one node.
    /// With `on`, every read goes to that node; otherwise they alternate.
    fn eventually_token_reads(&self, pool: u64, reason: &str, on: Option<usize>) {
        let Some(prefix) = self.tenant_prefix() else {
            println!("{reason}: no HEAD key known, so no token reads");
            return;
        };
        let mut writes = self.token_candidates(&prefix, pool);
        if writes.is_empty() && pool > 1 {
            writes = self.token_candidates(&prefix, 1);
        }
        let step = (writes.len() / EVENTUALLY_TOKEN_READS).max(1);
        let n = self.ravel.nodes.len().max(1);
        for (i, w) in writes
            .iter()
            .step_by(step)
            .take(EVENTUALLY_TOKEN_READS)
            .enumerate()
        {
            self.lifecycle_read(w, on.unwrap_or(i % n), true, reason);
        }
    }

    /// The local loop's stand-in for a node-restart fault (README.md, "Token
    /// reads across the lifecycle"): ask `local-run.sh` to restart `node`,
    /// wait for its confirmation and for every node to serve, then write once
    /// to the restarted node so its new writer identity is on record. Returns
    /// whether the restart was confirmed.
    fn local_restart(&self, node: usize) -> bool {
        let url = &self.ravel.nodes[node];
        println!("eventually: local loop: restart {url} now");
        let deadline = Instant::now() + LOCAL_RESTART_WAIT;
        while !Path::new(LOCAL_RESTART_DONE).exists() {
            if Instant::now() >= deadline {
                println!("eventually: local loop: no restart of {url} confirmed");
                return false;
            }
            std::thread::sleep(Duration::from_secs(1));
        }
        if let Err(e) = self.wait_serving(LOCAL_RESTART_WAIT, "local restart") {
            println!("eventually: local loop: not every node serves after the restart: {e}");
        }
        let acked = (0..3).any(|_| self.write(1, 1, &mut || node));
        println!("eventually: local loop: {url} restarted; a write to it acked: {acked}");
        true
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
        self.0.halt_if_stopping();
        let s = &self.0;
        let _g = s.enter();
        let records = tc.draw(gs::integers::<u32>().min_value(1).max_value(8));
        let streams = tc.draw(gs::integers::<u32>().min_value(1).max_value(4));
        s.write(records, streams, &mut || {
            draw_node(&tc, s.ravel.nodes.len())
        });
    }

    #[rule(groups = ["main"], weight = 1)]
    fn fold(&self, _tc: TestCase) {
        self.0.halt_if_stopping();
        let s = &self.0;
        let _g = s.enter();
        s.fold();
    }

    #[rule(groups = ["main"], weight = 1)]
    fn compact(&self, tc: TestCase) {
        self.0.halt_if_stopping();
        let s = &self.0;
        let _g = s.enter();
        if s.compact() {
            let node = draw_node(&tc, s.ravel.nodes.len());
            s.check_bounds(node, "on-demand compaction");
        }
    }

    #[rule(groups = ["main"], weight = 1)]
    fn sweep(&self, tc: TestCase) {
        self.0.halt_if_stopping();
        let s = &self.0;
        let _g = s.enter();
        let shard = tc.draw(gs::integers::<u32>().min_value(0).max_value(SHARDS - 1));
        s.sweep(shard);
    }

    #[rule(groups = ["main"], weight = 1)]
    fn jump_clock(&self, tc: TestCase) {
        self.0.halt_if_stopping();
        let s = &self.0;
        let sealed = lock(&s.jump).is_some();
        if sealed
            && (!s.compacted.load(Ordering::SeqCst)
                || lock(&s.sweep_jumps).next() == clock::SweepStep::Nothing)
        {
            return;
        }
        if !sealed && lock(&s.model).acked_logs.len() < s.clock.min_acked {
            return;
        }
        if !tc.draw(gs::weighted_booleans(0.05)) {
            return;
        }
        let quiet = s.gate.write().unwrap_or_else(|e| e.into_inner());
        if s.stop.stopping() {
            release_and_halt(quiet);
        }
        if sealed {
            s.sweep_jump("drawn");
        } else {
            s.jump_clock("drawn");
        }
    }

    #[rule(groups = ["main"], weight = 1)]
    fn race_folds(&self, tc: TestCase) {
        self.0.halt_if_stopping();
        let s = &self.0;
        if lock(&s.head_key).is_none() {
            return;
        }
        if !tc.draw(gs::weighted_booleans(0.03)) {
            return;
        }
        let quiet = s.gate.write().unwrap_or_else(|e| e.into_inner());
        if s.stop.stopping() {
            release_and_halt(quiet);
        }
        s.fold_race("drawn");
    }

    #[rule(groups = ["main"], weight = 1)]
    fn race_records(&self, tc: TestCase) {
        self.0.halt_if_stopping();
        let s = &self.0;
        if lock(&s.head_key).is_none() {
            return;
        }
        if !tc.draw(gs::weighted_booleans(0.03)) {
            return;
        }
        let quiet = s.gate.write().unwrap_or_else(|e| e.into_inner());
        if s.stop.stopping() {
            release_and_halt(quiet);
        }
        s.record_race("drawn");
    }

    #[rule(groups = ["main"], weight = 1)]
    fn sweep_unfolded(&self, tc: TestCase) {
        self.0.halt_if_stopping();
        let s = &self.0;
        if s.unfolded_started.load(Ordering::SeqCst) || lock(&s.head_key).is_none() {
            return;
        }
        if !tc.draw(gs::weighted_booleans(0.02)) {
            return;
        }
        let quiet = s.gate.write().unwrap_or_else(|e| e.into_inner());
        if s.stop.stopping() {
            release_and_halt(quiet);
        }
        if s.unfolded_started.load(Ordering::SeqCst) {
            return;
        }
        s.unfolded_sweep("drawn");
    }

    #[rule(groups = ["main"], weight = 1)]
    fn sweep_unreadable_head(&self, tc: TestCase) {
        self.0.halt_if_stopping();
        let s = &self.0;
        if s.unreadable_started.load(Ordering::SeqCst) || lock(&s.head_key).is_none() {
            return;
        }
        if !tc.draw(gs::weighted_booleans(0.02)) {
            return;
        }
        let quiet = s.gate.write().unwrap_or_else(|e| e.into_inner());
        if s.stop.stopping() {
            release_and_halt(quiet);
        }
        if s.unreadable_started.load(Ordering::SeqCst) {
            return;
        }
        s.unreadable_head_sweep("drawn");
    }

    #[rule(groups = ["main"], weight = 1)]
    fn corrupt_head(&self, tc: TestCase) {
        self.0.halt_if_stopping();
        let s = &self.0;
        let _g = s.enter();
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
        self.0.halt_if_stopping();
        let s = &self.0;
        let listing = {
            let _g = s.enter();
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
        self.0.halt_if_stopping();
        let s = &self.0;
        let _g = s.enter();
        let node = draw_node(&tc, s.ravel.nodes.len());
        s.check_bounds(node, "scan");
    }

    /// Read one acked write back with its commit tokens on a drawn node,
    /// while faults may run, drawing the write from all of them, from those
    /// with a compacted input, or from those with a swept input commit
    /// record, and drawing whether to read it twice on that node.
    #[rule(groups = ["main"], weight = 2)]
    fn token_lifecycle_read(&self, tc: TestCase) {
        self.0.halt_if_stopping();
        let s = &self.0;
        let _g = s.enter();
        let Some(prefix) = s.tenant_prefix() else {
            return;
        };
        let pool = tc.draw(gs::integers::<u64>().min_value(0).max_value(2));
        let mut writes = s.token_candidates(&prefix, pool);
        if writes.is_empty() {
            writes = s.token_candidates(&prefix, 0);
        }
        if writes.is_empty() {
            return;
        }
        let w = &writes[draw_node(&tc, writes.len())];
        let node = draw_node(&tc, s.ravel.nodes.len());
        let twice = tc.draw(gs::weighted_booleans(0.5));
        s.lifecycle_read(w, node, twice, "drawn");
    }

    #[rule(groups = ["main"])]
    fn eventually(&self, tc: TestCase) {
        // The helper runs no eventually of its own unless one was forced, so
        // a drawn one below still reaches run_eventually.
        self.0.halt_if_stopping();
        let s = &self.0;
        if !s.eventually_on {
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
    // A drawn and a forced eventually can race here: the loser halts, with
    // faults already stopping.
    if !s.stop.begin_eventually() {
        stop::halt();
    }
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
    for _ in 0..FOLD_RACES_EVENTUALLY {
        if s.fold_overtaken.load(Ordering::SeqCst) {
            break;
        }
        s.fold_race("eventually");
    }
    for _ in 0..RECORD_RACES_EVENTUALLY {
        if s.record_lost.load(Ordering::SeqCst) {
            break;
        }
        s.record_race("eventually");
    }
    if !s.unfolded_reached.load(Ordering::SeqCst) {
        s.unfolded_sweep("eventually");
    }
    s.unreadable_head_sweep("eventually");
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
    // A fold after the compaction lets HEAD name its output, and the audit
    // records what that HEAD resolves; the token reads then run while the
    // compacted inputs' commit records are still there.
    if !s.fold() {
        println!("eventually: the fold after the compaction failed");
    }
    s.audit(&|_| 0);
    s.eventually_token_reads(1, "eventually: compacted", None);
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
    // Every node read every live record while its hour was unsealed and
    // keeps it cached, so only a restarted node takes the token fallback.
    let restarted = s.restart_node.filter(|&node| s.local_restart(node));
    s.eventually_token_reads(2, "eventually: swept", restarted);
    println!("token reads by outcome: {:?}", lock(&s.token_tally));

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
    if let Err(e) = clock::check_head_cache_ttl(plan.head_cache_ttl_s, None) {
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
        proxy,
        inject_stale_token,
        local_restart,
        ..
    } = cfg;
    // A name that matches no node is a broken local config: refuse it rather
    // than run a loop that never restarts anything.
    let restart_node = local_restart.map(|name| {
        let host = format!("//{name}:");
        ravel
            .nodes
            .iter()
            .position(|url| url.contains(&host))
            .unwrap_or_else(|| die(&format!("WORKLOAD_LOCAL_RESTART_NODE={name} names no node")))
    });
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
        sweep_jumps: Mutex::new(clock::SweepJumps::default()),
        sweep_horizon: Mutex::new(None),
        sweep_jump: Mutex::new(None),
        offset_s: Mutex::new(0),
        admin,
        token,
        fold_races: AtomicU64::new(0),
        fold_overtaken: AtomicBool::new(false),
        proxy,
        record_races: AtomicU64::new(0),
        record_lost: AtomicBool::new(false),
        unfolded_started: AtomicBool::new(false),
        unfolded_reached: AtomicBool::new(false),
        unreadable_started: AtomicBool::new(false),
        unreadable_sweeps: AtomicU64::new(0),
        sweeps: Mutex::new(audit::SweepLedger::default()),
        inputs_of: Mutex::new(BTreeMap::new()),
        acks: Mutex::new(token::AckLedger::default()),
        ticks: AtomicU64::new(0),
        head_evidence: Mutex::new(None),
        inject_stale_token,
        token_tally: Mutex::new(BTreeMap::new()),
        restart_node,
        gate: RwLock::new(()),
        stop: Stop::default(),
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
    let ttl_s = shared.clock.head_cache_ttl_s;
    wait_for(
        "sys/gc records the HEAD cache TTL the compose states",
        setup_deadline,
        || {
            let out = shared
                .cli
                .run(&["gc-config", "show"], seconds_left(setup_deadline));
            if !out.ok() {
                return Err(format!("gc-config show: {}", out.summary()));
            }
            let Some(recorded_ns) = clock::recorded_head_cache_ttl_ns(&out.stdout) else {
                return Err(format!(
                    "gc-config show printed no TTL: {}",
                    out.stdout.trim()
                ));
            };
            if let Err(e) = clock::check_head_cache_ttl(ttl_s, Some(recorded_ns)) {
                die(&e);
            }
            Ok(())
        },
    );
    antithesis_sdk::lifecycle::setup_complete(&json!({
        "nodes": nodes,
        "maintain_nodes": maint_nodes,
        "seed_start_hour": shared.plan.start_hour,
        "seeded_records": shared.seeded.len(),
        "clock_jump_s": shared.clock.jump_s,
        "sweep_horizon_jump_s": shared.clock.horizon_jump_s,
        "sweep_window_jump_s": shared.clock.window_jump_s,
        "head_cache_ttl_s": ttl_s,
        "head_quiet_s": clock::head_quiet_s(ttl_s),
    }));
    println!("startup: setup complete; waiting for /workload-active");
    while !Path::new("/workload-active").exists() {
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_reachable!("catalog: the workload starts", &json!({}));

    if let Some(secs) = eventually_after {
        let s = Arc::clone(&shared);
        // The next rule runs the forced eventually, inside the machine; the
        // timer runs it only if no rule does, once the machine has ended.
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_secs(secs));
            s.stop.request_forced();
            if !s.stop.wait_started(FORCED_BACKSTOP, Duration::from_secs(1)) {
                println!("eventually: no rule ran it within {FORCED_BACKSTOP:?}");
                run_eventually(&s, FORCED_REASON);
            }
        });
    }

    machine(Workload(shared))
        .steps(i64::MAX)
        .max_concurrency(4)
        .run_concurrent(tc);
}
