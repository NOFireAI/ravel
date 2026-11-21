//! Antithesis driver for the lifecycle-gc workload: selective subject
//! erasure, legal holds and the GC that removes superseded objects, judged
//! by an oracle that never calls a Ravel matcher. See
//! antithesis/lifecycle-gc/README.md for the properties.

mod cli;
mod client;
mod clock;
mod model;
mod stop;
mod store;

use std::collections::{BTreeMap, HashMap};
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

use cli::Cli;
use client::{Ravel, Reply, SqlFailure};
use model::{ERASABLE, Model, Outcome, Placement, Request, SUBJECTS, Seq, Signal};
use stop::{Stop, halt};
use store::Store;

const P1: &str = "erasure: an erased subject is never returned after its request";
const P3: &str = "lifecycle: every acked survivor is read exactly once";
const P4: &str = "lifecycle: no read returns a record more often than it was written";
const P5: &str = "gc: an object under an acked hold is not deleted";

const WRITE_TIMEOUT: Duration = Duration::from_secs(10);
const READ_TIMEOUT: Duration = Duration::from_secs(20);
const CLI_TIMEOUT_S: u32 = 60;
const SHARDS: u64 = 4;
const MAX_REQUESTS: usize = 4;
const MAX_HOLDS: usize = 2;
const MAX_RECORDS_PER_WRITE: u64 = 6;
const WINDOW_SLACK_NS: u64 = 600 * 1_000_000_000;
/// Writes whose survivors one token read in the eventually checks.
const TOKEN_BATCH: usize = 20;
/// How long a final scan, and a final token read, retries before it is
/// counted as no verdict.
const SCAN_RETRY: Duration = Duration::from_secs(120);
const TOKEN_READ_RETRY: Duration = Duration::from_secs(30);
/// Horizon jumps per timeline, rules and eventually together.
const MAX_HORIZON_JUMPS: u32 = 7;
const SIGNALS: [Signal; 2] = [Signal::Logs, Signal::Metrics];
/// How far behind ingest time a retention-tenant record's event time sits:
/// inside `max_ingest_lag` (7200 s) by ten minutes.
const RETIRE_EVENT_AGE_S: u64 = 6600;
/// Retention-tenant record ids start here, apart from the model's.
const RETIRE_ID_BASE: u64 = 1 << 40;
/// `keep-0`, a subject no request erases.
const KEEP_SUBJECT: u8 = 6;
/// The object the hold-failure step puts into the legal-hold shard: no
/// bucket-entry shape a hold read knows, so every hold read fails closed.
const HOLD_POISON: &str = "antithesis-hold-poison";

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
        .unwrap_or(0)
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

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn die(msg: &str) -> ! {
    eprintln!("driver: {msg}");
    std::process::exit(1);
}

struct Config {
    nodes: Vec<String>,
    maint_nodes: Vec<String>,
    token: String,
    tenant: String,
    ready_marker: String,
    max_writes: usize,
    placements: Vec<Placement>,
    inject: bool,
    eventually_on: bool,
    eventually_after: Option<u64>,
    clock_file: PathBuf,
    seal_jump_s: u64,
    horizon_jump_s: u64,
    reread_s: u64,
    min_acked: usize,
    /// One maintain tick plus margin.
    tick_window: Duration,
    /// How long after a hold's ack a delete of a held key is still allowed:
    /// a delete batch that re-read the holds before the ack may still be
    /// deleting (ADR-0042, the delete-batch re-read amendment's residual).
    hold_grace: Duration,
    /// The token of the retention tenant, whose writes the maintain nodes
    /// retire after `--retention-tenant`'s window.
    retire_token: String,
    /// The jump that ages a retention tombstone's unnamed-since marker past
    /// the pinned-query window (4830 s) but leaves its protection horizon
    /// (90300 s) ahead.
    retention_jump_s: u64,
}

impl Config {
    fn from_env() -> Config {
        let placements: Vec<Placement> = std::env::var("WORKLOAD_PLACEMENTS")
            .unwrap_or_else(|_| "record,resource,scope".to_string())
            .split(',')
            .filter_map(Placement::parse)
            .collect();
        if placements.is_empty() {
            die("WORKLOAD_PLACEMENTS names no placement (record, resource, scope)");
        }
        Config {
            nodes: node_list("RAVEL_NODES", "http://ravel1:4318,http://ravel2:4318"),
            maint_nodes: node_list(
                "RAVEL_MAINTAIN_NODES",
                "http://maint1:4318,http://maint2:4318",
            ),
            token: std::env::var("RAVEL_TOKEN").unwrap_or_else(|_| "lc-token".to_string()),
            tenant: std::env::var("RAVEL_TENANT").unwrap_or_else(|_| "lc-tenant".to_string()),
            ready_marker: std::env::var("READY_MARKER")
                .unwrap_or_else(|_| "antithesis/lifecycle-gc/ready".to_string()),
            max_writes: usize::try_from(env_u64("WORKLOAD_MAX_WRITES", 500)).unwrap_or(500),
            placements,
            inject: std::env::var("WORKLOAD_INJECT_SERVED_ERASED").is_ok_and(|v| v == "1"),
            eventually_on: std::env::var("WORKLOAD_EVENTUALLY").map_or(true, |v| v != "off"),
            eventually_after: std::env::var("WORKLOAD_EVENTUALLY_AFTER_SECS")
                .ok()
                .and_then(|v| v.parse().ok()),
            clock_file: PathBuf::from(
                std::env::var("CLOCK_JUMP_FILE").unwrap_or_else(|_| "/faketime/offset".to_string()),
            ),
            seal_jump_s: env_u64("CLOCK_SEAL_JUMP_SECS", 9000),
            horizon_jump_s: env_u64("CLOCK_HORIZON_JUMP_SECS", 91_200),
            reread_s: env_u64("FAKETIME_CACHE_DURATION", 2),
            min_acked: usize::try_from(env_u64("CLOCK_JUMP_MIN_ACKED", 20)).unwrap_or(20),
            tick_window: Duration::from_secs(env_u64("MAINTAIN_INTERVAL_SECS", 20) + 20),
            hold_grace: Duration::from_secs(env_u64("HOLD_GRACE_SECS", 120)),
            retire_token: std::env::var("RAVEL_RETIRE_TOKEN")
                .unwrap_or_else(|_| "lc-retire-token".to_string()),
            retention_jump_s: env_u64("CLOCK_RETENTION_JUMP_SECS", 9000),
        }
    }
}

/// One acknowledged legal hold on a shard of one signal.
struct Hold {
    signal: Signal,
    shard: u64,
    scopes: Vec<String>,
    acked_at: Instant,
    /// Every key the baseline listing held, with when it was last seen
    /// present. A key leaves this map once it is confirmed deleted.
    keys: BTreeMap<String, Instant>,
    baselined: bool,
    clear_requested: bool,
}

#[derive(Default)]
struct ClockState {
    offset_s: u64,
    sealed: bool,
    horizon_jumps: u32,
    last_horizon: Option<Instant>,
}

struct Shared {
    cfg: Config,
    ravel: Ravel,
    /// The same nodes under the retention tenant's token.
    retire: Ravel,
    maint: Ravel,
    cli: Cli,
    store: Store,
    seq: AtomicU64,
    model: Mutex<Model>,
    tenant_prefix: Mutex<Option<String>>,
    holds: Mutex<Vec<Hold>>,
    /// `(signal, shard)` holds whose set or clear had an unknown outcome:
    /// the eventually clears them, since a hold blocks `.done`.
    unsure_holds: Mutex<Vec<(Signal, u64)>>,
    clock: Mutex<ClockState>,
    /// Rules hold a read guard while they act; clock jumps and the
    /// eventually take the write guard.
    gate: RwLock<()>,
    stop: Stop,
    reads_ok: AtomicU64,
    reads_inconclusive: AtomicU64,
    injected: AtomicU64,
    rewrite_seen: AtomicBool,
    /// Strictly acknowledged writes to the retention tenant.
    retired_acked: AtomicU64,
}

struct Workload(Arc<Shared>);

fn draw_below(tc: &TestCase, n: u64) -> u64 {
    tc.draw(
        gs::integers::<u64>()
            .min_value(0)
            .max_value(n.saturating_sub(1)),
    )
}

fn first<T: std::fmt::Debug>(items: &[T]) -> serde_json::Value {
    json!({
        "count": items.len(),
        "first": items.iter().take(5).map(|i| format!("{i:?}")).collect::<Vec<_>>(),
    })
}

impl Shared {
    fn next_seq(&self) -> Seq {
        self.seq.fetch_add(1, Ordering::SeqCst)
    }

    /// Every rule's first statement: run the forced eventually when its
    /// deadline passed, and park for good once faults stopped.
    fn halt_if_stopping(&self) {
        if self.stop.force_due() {
            run_eventually(self, "forced by WORKLOAD_EVENTUALLY_AFTER_SECS");
        }
        if self.stop.stopping() {
            halt();
        }
    }

    fn enter(&self) -> RwLockReadGuard<'_, ()> {
        let guard = self.gate.read().unwrap_or_else(|e| e.into_inner());
        if self.stop.stopping() {
            drop(guard);
            halt();
        }
        guard
    }

    fn window_s(&self) -> (f64, f64) {
        let m = lock(&self.model);
        let min = m
            .writes
            .iter()
            .map(|w| w.ts_ns)
            .min()
            .unwrap_or_else(now_ns);
        let max = m
            .writes
            .iter()
            .map(|w| w.ts_ns)
            .max()
            .unwrap_or(0)
            .max(now_ns());
        (
            min.saturating_sub(WINDOW_SLACK_NS) as f64 / 1e9,
            max.saturating_add(WINDOW_SLACK_NS) as f64 / 1e9,
        )
    }

    fn write(&self, tc: &TestCase, signal: Signal) {
        let n = 1 + draw_below(tc, MAX_RECORDS_PER_WRITE);
        let records: Vec<(u8, Placement)> = (0..n)
            .map(|_| {
                let subject = u8::try_from(draw_below(tc, SUBJECTS.len() as u64)).unwrap_or(0);
                let placement = match signal {
                    Signal::Metrics => Placement::Series,
                    Signal::Logs => {
                        let k = draw_below(tc, self.cfg.placements.len() as u64);
                        self.cfg.placements[usize::try_from(k).unwrap_or(0)]
                    }
                };
                (subject, placement)
            })
            .collect();
        let buffered = tc.draw(gs::weighted_booleans(0.15));
        let node = usize::try_from(draw_below(tc, self.ravel.nodes.len() as u64)).unwrap_or(0);
        let ts = now_ns();
        let (idx, first_id) = {
            let mut m = lock(&self.model);
            if m.writes.len() >= self.cfg.max_writes {
                return;
            }
            m.register(signal, buffered, ts, records.clone())
        };
        let (path, body) = match signal {
            Signal::Logs => ("/v1/logs", client::logs_request(ts, first_id, &records)),
            Signal::Metrics => (
                "/v1/metrics",
                client::metrics_request(ts, first_id, &records),
            ),
        };
        let reply = self.ravel.export(node, path, body, buffered, WRITE_TIMEOUT);
        let outcome = match &reply {
            Reply::Status {
                status,
                commit_token: Some(h),
                body,
            } if !buffered
                && (200..300).contains(status)
                && !h.is_empty()
                && client::clean_success(signal == Signal::Logs, body) =>
            {
                Outcome::Acked {
                    header: h.clone(),
                    seq: self.next_seq(),
                }
            }
            _ => Outcome::Indefinite,
        };
        lock(&self.model).finish(idx, outcome);
    }

    /// One strict write of one log record to the retention tenant. Its event
    /// time is as old as ingest admits (`max_ingest_lag`, 2 h, less a
    /// margin), so its hour expires under the tenant's retention window soon
    /// after the sealing jump. Nothing reads these records back.
    fn write_retired(&self, node: usize) -> bool {
        let ts = now_ns().saturating_sub(RETIRE_EVENT_AGE_S * 1_000_000_000);
        let id = RETIRE_ID_BASE + self.next_seq();
        let body = client::logs_request(ts, id, &[(KEEP_SUBJECT, Placement::Record)]);
        let acked = matches!(
            self.retire.export(node, "/v1/logs", body, false, WRITE_TIMEOUT),
            Reply::Status { status, commit_token: Some(h), body }
                if (200..300).contains(&status) && !h.is_empty() && client::clean_success(true, &body)
        );
        if acked {
            self.retired_acked.fetch_add(1, Ordering::SeqCst);
        }
        acked
    }

    /// Judge one successful read and assert properties 1, 2 and 4 on it.
    fn judge(&self, signal: Signal, issued: Seq, mut counts: HashMap<u64, u64>, how: &str) {
        let m = lock(&self.model);
        let injected = if self.cfg.inject {
            m.inject_victim(signal, issued, &mut counts)
        } else {
            None
        };
        let v = m.judge_read(signal, issued, &counts);
        drop(m);
        if injected.is_some() {
            self.injected.fetch_add(1, Ordering::SeqCst);
        }
        self.reads_ok.fetch_add(1, Ordering::SeqCst);
        let base = json!({
            "signal": signal.name(),
            "read": how,
            "rows": counts.len(),
            "injected_victim": injected,
        });
        assert_always!(
            v.over.is_empty(),
            P4,
            &json!({ "read": base, "over_id_read_written": first(&v.over) })
        );
        // One property for every read issued after a request's ack. The
        // details split the hits by whether the driver had already observed
        // the request's `.done` or `.dreq` removal when the read was issued:
        // hits only after completion are the #4503 shape.
        if !v.pending_checked.is_empty() || !v.done_checked.is_empty() {
            assert_always!(
                v.pending_hits.is_empty() && v.done_hits.is_empty(),
                P1,
                &json!({
                    "read": base,
                    "requests_pending": v.pending_checked,
                    "requests_completed_or_swept": v.done_checked,
                    "requests_swept": v.gone_checked,
                    "hits_while_pending_by_placement": v.pending_hits_by_placement(),
                    "hits_after_completion_by_placement": v.done_hits_by_placement(),
                    "hits_while_pending": first(&v.pending_hits),
                    "hits_after_completion": first(&v.done_hits),
                })
            );
        }
        if !v.gone_checked.is_empty() {
            assert_sometimes!(
                v.survivors_before_gone > 0,
                "erasure: a read after a .dreq removal ran",
                &json!({
                    "read": base,
                    "requests_swept": v.gone_checked,
                    "survivor_rows_from_before_the_requests": v.survivors_before_gone,
                })
            );
        }
        if !v.done_hits.is_empty() || !v.pending_hits.is_empty() || !v.over.is_empty() {
            println!(
                "read {how} {}: over {:?} pending_hits {:?} done_hits {:?} (by placement {:?})",
                signal.name(),
                v.over,
                v.pending_hits,
                v.done_hits,
                v.done_hits_by_placement()
            );
        }
    }

    /// One SQL read of every workload record of `signal`, or of the ids in
    /// `ids`, bounded below by `tokens`.
    fn read_counts(
        &self,
        signal: Signal,
        node: usize,
        ids: Option<(u64, u64)>,
        tokens: &[String],
    ) -> Result<HashMap<u64, u64>, SqlFailure> {
        let logs = signal == Signal::Logs;
        let (start, end) = self.window_s();
        let rows = self.ravel.sql(
            node,
            &client::read_query(logs, ids),
            start,
            end,
            tokens,
            READ_TIMEOUT,
        )?;
        client::read_counts(logs, &rows).map_err(SqlFailure::BadBody)
    }

    fn read(&self, signal: Signal, node: usize, with_tokens: bool) {
        let tokens: Vec<String> = if with_tokens {
            let m = lock(&self.model);
            m.writes
                .iter()
                .rev()
                .filter(|w| w.signal == signal)
                .flat_map(|w| w.tokens())
                .take(8)
                .collect()
        } else {
            Vec::new()
        };
        let issued = self.next_seq();
        let how = if tokens.is_empty() {
            "scan"
        } else {
            "scan with tokens"
        };
        match self.read_counts(signal, node, None, &tokens) {
            Ok(counts) => self.judge(signal, issued, counts, how),
            Err(e) => {
                self.reads_inconclusive.fetch_add(1, Ordering::SeqCst);
                println!(
                    "read {} on node {node} ({how}): inconclusive: {e}",
                    signal.name()
                );
            }
        }
    }

    fn set_prefix_from(&self, key: &str) {
        if let Some(p) = cli::tenant_prefix(key) {
            let mut slot = lock(&self.tenant_prefix);
            if slot.is_none() {
                *slot = Some(p);
            }
        }
    }

    /// Submit an erasure of `subject` on `signal` with a drawn request id,
    /// retried with the same id while the outcome is unknown.
    fn erase(&self, signal: Signal, subject: u8, request_id: String) {
        let started = self.next_seq();
        let idx = {
            let mut m = lock(&self.model);
            m.requests.push(Request {
                request_id: request_id.clone(),
                signal,
                subject,
                started,
                acked: None,
                done: None,
                gone: None,
                dreq_key: None,
            });
            m.requests.len() - 1
        };
        let matcher = format!("{}={}", client::SUBJECT_KEY, SUBJECTS[usize::from(subject)]);
        for attempt in 0..3 {
            let out = self.cli.run(
                &[
                    "erase",
                    "submit",
                    "--tenant",
                    &self.cfg.tenant,
                    "--signal",
                    signal.name(),
                    "--matcher",
                    &matcher,
                    "--request-id",
                    &request_id,
                    "--reason",
                    "antithesis lifecycle-gc",
                ],
                CLI_TIMEOUT_S,
            );
            match (out.ok(), cli::submitted_key(&out.stdout)) {
                (true, Some(key)) => {
                    let acked = self.next_seq();
                    self.set_prefix_from(&key);
                    let victims = {
                        let mut m = lock(&self.model);
                        m.requests[idx].acked = Some(acked);
                        m.requests[idx].dreq_key = Some(key.clone());
                        let r = m.requests[idx].clone();
                        m.victims(&r).len()
                    };
                    println!(
                        "erase: request {request_id} {} {matcher} acked (attempt {attempt}, {victims} victims): {key}",
                        signal.name()
                    );
                    assert_reachable!(
                        "erasure: an erasure request was acked",
                        &json!({ "signal": signal.name(), "victims": victims })
                    );
                    return;
                }
                _ => println!(
                    "erase: submit {request_id} {} {matcher} attempt {attempt}: {}",
                    signal.name(),
                    out.summary()
                ),
            }
        }
        println!("erase: request {request_id} outcome unknown; its subject stays unconstrained");
    }

    /// List each signal's `del/` prefix and record `.done` and `.dreq`
    /// removal for every acked request; with `rewrites`, list `c/` for
    /// rewrite records too.
    fn observe(&self, rewrites: bool) {
        let Some(prefix) = lock(&self.tenant_prefix).clone() else {
            return;
        };
        for signal in SIGNALS {
            let acked: Vec<Request> = lock(&self.model)
                .requests
                .iter()
                .filter(|r| r.signal == signal && r.acked.is_some())
                .cloned()
                .collect();
            if acked.is_empty() {
                continue;
            }
            let del = format!("{prefix}{}/del/", signal.key_segment());
            let listed = match self.store.list(&del) {
                Ok(keys) => keys,
                Err(e) => {
                    println!("observe: {e}");
                    continue;
                }
            };
            let after_list = self.next_seq();
            for r in &acked {
                let done_key = format!("{del}{}.done", r.request_id);
                let dreq_key = format!("{del}{}.dreq", r.request_id);
                let mut done = None;
                let mut gone = None;
                if r.done.is_none() && listed.contains(&done_key) {
                    done = Some(after_list);
                    println!("observe: {done_key} present");
                    assert_reachable!(
                        "erasure: a .done was observed",
                        &json!({ "signal": signal.name(), "request": r.request_id })
                    );
                }
                if r.gone.is_none()
                    && !listed.contains(&dreq_key)
                    && matches!(self.store.exists(&dreq_key), Ok(false))
                {
                    gone = Some(self.next_seq());
                    println!("observe: {dreq_key} removed");
                    assert_reachable!(
                        "erasure: a .dreq removal was observed",
                        &json!({
                            "signal": signal.name(),
                            "request": r.request_id,
                            "done_listed": listed.contains(&done_key) || r.done.is_some(),
                        })
                    );
                }
                if done.is_some() || gone.is_some() {
                    let mut m = lock(&self.model);
                    if let Some(req) = m.requests.iter_mut().find(|q| q.request_id == r.request_id)
                    {
                        req.done = req.done.or(done);
                        req.gone = req.gone.or(gone);
                    }
                }
            }
            if rewrites {
                let c = format!("{prefix}{}/c/", signal.key_segment());
                match self.store.list(&c) {
                    Ok(keys) => {
                        let found = keys
                            .iter()
                            .filter(|k| {
                                k.rsplit('/')
                                    .next()
                                    .is_some_and(|f| f.starts_with("rw.") && f.ends_with(".cmt"))
                            })
                            .count();
                        if found > 0 {
                            self.rewrite_seen.store(true, Ordering::SeqCst);
                        }
                        assert_sometimes!(
                            found > 0,
                            "erasure: a rewrite record was observed",
                            &json!({ "signal": signal.name(), "rewrite_records": found })
                        );
                    }
                    Err(e) => println!("observe: {e}"),
                }
            }
        }
    }

    /// Whether every acked request has reached `want` (done or gone),
    /// ignoring signals in `skip`.
    fn all_requests(&self, gone: bool, skip: &[Signal]) -> bool {
        lock(&self.model)
            .requests
            .iter()
            .filter(|r| r.acked.is_some() && !skip.contains(&r.signal))
            .all(|r| {
                if gone {
                    r.gone.is_some()
                } else {
                    r.completed().is_some()
                }
            })
    }

    fn hold_scopes(&self, signal: Signal, shard: u64, printed: Vec<String>) -> Vec<String> {
        if !printed.is_empty() {
            return printed;
        }
        match lock(&self.tenant_prefix).as_deref() {
            Some(p) => ["l0", "c", "l1"]
                .iter()
                .map(|d| format!("{p}{}/{d}/{shard:04}/", signal.key_segment()))
                .collect(),
            None => Vec::new(),
        }
    }

    fn hold_set(&self, signal: Signal, shard: u64) {
        let shard_s = shard.to_string();
        let out = self.cli.run(
            &[
                "hold",
                "set",
                "--tenant",
                &self.cfg.tenant,
                "--signal",
                signal.name(),
                "--shard",
                &shard_s,
                "--reason",
                "antithesis lifecycle-gc",
            ],
            CLI_TIMEOUT_S,
        );
        if !out.ok() {
            println!(
                "hold set {} shard {shard}: {}",
                signal.name(),
                out.summary()
            );
            lock(&self.unsure_holds).push((signal, shard));
            return;
        }
        let acked_at = Instant::now();
        let printed = cli::hold_scopes(&out.stdout);
        if let Some(p) = printed.first() {
            self.set_prefix_from(p);
        }
        let scopes = self.hold_scopes(signal, shard, printed);
        println!("hold set {} shard {shard}: {scopes:?}", signal.name());
        assert_reachable!(
            "gc: a hold was placed",
            &json!({ "signal": signal.name(), "shard": shard })
        );
        let mut hold = Hold {
            signal,
            shard,
            scopes,
            acked_at,
            keys: BTreeMap::new(),
            baselined: false,
            clear_requested: false,
        };
        self.baseline(&mut hold);
        lock(&self.holds).push(hold);
    }

    /// The listing every later check of `hold` compares with.
    fn baseline(&self, hold: &mut Hold) {
        if hold.scopes.is_empty() {
            return;
        }
        let mut keys = BTreeMap::new();
        for scope in &hold.scopes {
            match self.store.list(scope) {
                Ok(listed) => {
                    let at = Instant::now();
                    keys.extend(listed.into_iter().map(|k| (k, at)));
                }
                Err(e) => {
                    println!("hold baseline: {e}");
                    return;
                }
            }
        }
        hold.keys = keys;
        hold.baselined = true;
    }

    /// Property 5 over every active hold: each baseline key still listed, or
    /// confirmed absent and judged by when it was last seen.
    ///
    /// Judged in place under the lock: `hold_set` and `hold_clear` read the
    /// same vector, so it must never look empty while a check runs.
    fn check_holds(&self) {
        let mut holds = lock(&self.holds);
        for hold in holds.iter_mut().filter(|h| !h.clear_requested) {
            if !hold.baselined {
                self.baseline(hold);
                continue;
            }
            let mut listed = std::collections::BTreeSet::new();
            let mut complete = true;
            for scope in &hold.scopes {
                match self.store.list(scope) {
                    Ok(keys) => listed.extend(keys),
                    Err(e) => {
                        println!("hold check: {e}");
                        complete = false;
                    }
                }
            }
            if !complete {
                continue;
            }
            let at = Instant::now();
            let window_end = hold.acked_at + self.cfg.hold_grace;
            let mut deleted_after_grace = Vec::new();
            let mut deleted_in_grace = Vec::new();
            let mut gone = Vec::new();
            for (key, last_seen) in hold.keys.iter_mut() {
                if listed.contains(key) {
                    *last_seen = at;
                    continue;
                }
                if !matches!(self.store.exists(key), Ok(false)) {
                    continue;
                }
                gone.push(key.clone());
                if *last_seen >= window_end {
                    deleted_after_grace.push(key.clone());
                } else {
                    deleted_in_grace.push(key.clone());
                }
            }
            for k in &gone {
                hold.keys.remove(k);
            }
            let details = json!({
                "signal": hold.signal.name(),
                "shard": hold.shard,
                "scopes": hold.scopes,
                "held_for_s": hold.acked_at.elapsed().as_secs(),
                "grace_s": self.cfg.hold_grace.as_secs(),
                "keys_tracked": hold.keys.len(),
                "deleted_after_grace": first(&deleted_after_grace),
                "deleted_inside_grace": first(&deleted_in_grace),
            });
            if !gone.is_empty() {
                println!("hold check: {details}");
            }
            assert_always!(deleted_after_grace.is_empty(), P5, &details);
        }
    }

    fn hold_clear(&self, signal: Signal, shard: u64) -> bool {
        let shard_s = shard.to_string();
        let out = self.cli.run(
            &[
                "hold",
                "clear",
                "--tenant",
                &self.cfg.tenant,
                "--signal",
                signal.name(),
                "--shard",
                &shard_s,
            ],
            CLI_TIMEOUT_S,
        );
        if !out.ok() {
            println!(
                "hold clear {} shard {shard}: {}",
                signal.name(),
                out.summary()
            );
        }
        out.ok()
    }

    fn all_nodes_serve(&self) -> Result<(), String> {
        let (start, end) = self.window_s();
        for node in 0..self.ravel.nodes.len() {
            self.ravel.ready(node)?;
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

    /// Move every clock forward by `by_s`. The caller holds the gate's
    /// write guard. Returns whether the jump reached the driver's clock.
    fn shift(&self, by_s: u64, reason: &str) -> bool {
        let mut c = lock(&self.clock);
        let offset = c.offset_s + by_s;
        let before = now_ns();
        if let Err(e) = clock::write_offset(&self.cfg.clock_file, offset) {
            assert_unreachable!(
                "clock: the driver could not write the clock offset",
                &json!({ "detail": e })
            );
            return false;
        }
        let deadline = Instant::now() + Duration::from_secs(self.cfg.reread_s + 30);
        let mut after = now_ns();
        while !clock::moved(before, after, by_s) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(500));
            after = now_ns();
        }
        let reached = clock::moved(before, after, by_s);
        println!("clock: jumped by {by_s}s to +{offset} ({reason}); reached driver: {reached}");
        if !reached {
            assert_unreachable!(
                "clock: the clock jump did not reach the driver",
                &json!({ "offset_s": offset, "by_s": by_s, "before_ns": before, "after_ns": after })
            );
            return false;
        }
        c.offset_s = offset;
        drop(c);
        std::thread::sleep(Duration::from_secs(self.cfg.reread_s + 1));
        if let Err(e) = self.wait_serving(Duration::from_secs(120), "clock jump") {
            println!("clock: a node is not serving two minutes after the jump: {e}");
        }
        true
    }

    fn seal_jump(&self, reason: &str) {
        if lock(&self.clock).sealed {
            return;
        }
        if self.shift(self.cfg.seal_jump_s, reason) {
            lock(&self.clock).sealed = true;
        }
    }

    fn horizon_jump(&self, reason: &str) -> bool {
        if lock(&self.clock).horizon_jumps >= MAX_HORIZON_JUMPS {
            return false;
        }
        if !self.shift(self.cfg.horizon_jump_s, reason) {
            return false;
        }
        let mut c = lock(&self.clock);
        c.sealed = true;
        c.horizon_jumps += 1;
        c.last_horizon = Some(Instant::now());
        let jumps = c.horizon_jumps;
        let offset = c.offset_s;
        drop(c);
        assert_reachable!(
            "clock: the clocks passed the protection horizon",
            &json!({ "reason": reason, "horizon_jumps": jumps, "offset_s": offset })
        );
        true
    }

    /// Observe every 10 s until `done` holds or `limit` passes.
    fn wait_until(&self, limit: Duration, what: &str, done: impl Fn(&Shared) -> bool) -> bool {
        let deadline = Instant::now() + limit;
        loop {
            self.observe(true);
            if done(self) {
                println!("eventually: {what}");
                return true;
            }
            if Instant::now() >= deadline {
                println!("eventually: gave up after {limit:?} waiting until {what}");
                return false;
            }
            std::thread::sleep(Duration::from_secs(10));
        }
    }

    fn held_signals(&self) -> Vec<Signal> {
        let mut v: Vec<Signal> = lock(&self.holds)
            .iter()
            .filter(|h| !h.clear_requested)
            .map(|h| h.signal)
            .collect();
        v.extend(lock(&self.unsure_holds).iter().map(|(s, _)| *s));
        v.sort();
        v.dedup();
        v
    }
}

#[hegel::concurrent_state_machine]
impl Workload {
    #[rule(groups = ["main"], weight = 5)]
    fn write_logs(&self, tc: TestCase) {
        self.0.halt_if_stopping();
        let s = &self.0;
        let _g = s.enter();
        s.write(&tc, Signal::Logs);
    }

    #[rule(groups = ["main"], weight = 3)]
    fn write_metrics(&self, tc: TestCase) {
        self.0.halt_if_stopping();
        let s = &self.0;
        let _g = s.enter();
        s.write(&tc, Signal::Metrics);
    }

    #[rule(groups = ["main"], weight = 1)]
    fn write_retired(&self, tc: TestCase) {
        self.0.halt_if_stopping();
        let s = &self.0;
        let _g = s.enter();
        let node = usize::try_from(draw_below(&tc, s.retire.nodes.len() as u64)).unwrap_or(0);
        s.write_retired(node);
    }

    #[rule(groups = ["main"], weight = 5)]
    fn read(&self, tc: TestCase) {
        self.0.halt_if_stopping();
        let s = &self.0;
        let _g = s.enter();
        let signal = SIGNALS[usize::try_from(draw_below(&tc, 2)).unwrap_or(0)];
        let node = usize::try_from(draw_below(&tc, s.ravel.nodes.len() as u64)).unwrap_or(0);
        let with_tokens = tc.draw(gs::weighted_booleans(0.5));
        s.read(signal, node, with_tokens);
    }

    #[rule(groups = ["main"], weight = 1)]
    fn erase(&self, tc: TestCase) {
        self.0.halt_if_stopping();
        let s = &self.0;
        let _g = s.enter();
        if !tc.draw(gs::weighted_booleans(0.15)) {
            return;
        }
        let signal = SIGNALS[usize::try_from(draw_below(&tc, 2)).unwrap_or(0)];
        let subject = u8::try_from(draw_below(&tc, ERASABLE as u64)).unwrap_or(0);
        {
            let m = lock(&s.model);
            if m.requests.len() >= MAX_REQUESTS || !m.is_survivor_subject(signal, subject) {
                return;
            }
        }
        // The low half carries the sequence number, so two requests of one
        // timeline never share an id whatever hegel draws.
        let id = cli::uuid_from(tc.draw(gs::integers::<u64>()), s.next_seq());
        s.erase(signal, subject, id);
    }

    #[rule(groups = ["main"], weight = 2)]
    fn observe(&self, tc: TestCase) {
        self.0.halt_if_stopping();
        let s = &self.0;
        let _g = s.enter();
        let rewrites = tc.draw(gs::weighted_booleans(0.3));
        s.observe(rewrites);
    }

    #[rule(groups = ["main"], weight = 1)]
    fn hold_set(&self, tc: TestCase) {
        self.0.halt_if_stopping();
        let s = &self.0;
        let _g = s.enter();
        if !tc.draw(gs::weighted_booleans(0.2)) {
            return;
        }
        let signal = SIGNALS[usize::try_from(draw_below(&tc, 2)).unwrap_or(0)];
        let shard = draw_below(&tc, SHARDS);
        {
            let holds = lock(&s.holds);
            let active = holds.iter().filter(|h| !h.clear_requested).count();
            if active >= MAX_HOLDS
                || holds
                    .iter()
                    .any(|h| !h.clear_requested && h.signal == signal && h.shard == shard)
            {
                return;
            }
        }
        s.hold_set(signal, shard);
    }

    #[rule(groups = ["main"], weight = 2)]
    fn check_holds(&self, _tc: TestCase) {
        self.0.halt_if_stopping();
        let s = &self.0;
        let _g = s.enter();
        s.check_holds();
    }

    #[rule(groups = ["main"], weight = 1)]
    fn hold_clear(&self, tc: TestCase) {
        self.0.halt_if_stopping();
        let s = &self.0;
        let _g = s.enter();
        if !tc.draw(gs::weighted_booleans(0.1)) {
            return;
        }
        // Judge the hold one last time, then stop judging before the clear
        // is sent: from then on a delete is allowed.
        s.check_holds();
        let target = {
            let mut holds = lock(&s.holds);
            let min_age = s.cfg.tick_window * 3;
            match holds
                .iter_mut()
                .find(|h| !h.clear_requested && h.acked_at.elapsed() >= min_age)
            {
                Some(h) => {
                    h.clear_requested = true;
                    Some((h.signal, h.shard))
                }
                None => None,
            }
        };
        if let Some((signal, shard)) = target
            && !s.hold_clear(signal, shard)
        {
            lock(&s.unsure_holds).push((signal, shard));
        }
    }

    #[rule(groups = ["main"], weight = 1)]
    fn jump_clock(&self, tc: TestCase) {
        self.0.halt_if_stopping();
        let s = &self.0;
        let (sealed, jumps, last) = {
            let c = lock(&s.clock);
            (c.sealed, c.horizon_jumps, c.last_horizon)
        };
        let want_seal = !sealed && {
            let m = lock(&s.model);
            m.requests.iter().any(|r| r.acked.is_some())
                && m.writes.iter().filter(|w| w.acked_seq().is_some()).count() >= s.cfg.min_acked
        };
        let want_horizon = sealed
            && jumps < 2
            && last.is_none_or(|t| t.elapsed() >= Duration::from_secs(90))
            && lock(&s.model).requests.iter().any(|r| r.done.is_some())
            && s.held_signals().is_empty();
        if !(want_seal || want_horizon) || !tc.draw(gs::weighted_booleans(0.05)) {
            return;
        }
        let quiet = s.gate.write().unwrap_or_else(|e| e.into_inner());
        if s.stop.stopping() {
            drop(quiet);
            halt();
        }
        if want_seal {
            s.seal_jump("drawn");
        } else {
            s.horizon_jump("drawn");
        }
    }

    #[rule(groups = ["main"])]
    fn eventually(&self, tc: TestCase) {
        self.0.halt_if_stopping();
        let s = &self.0;
        if !s.cfg.eventually_on || !tc.draw(gs::weighted_booleans(0.003)) {
            return;
        }
        run_eventually(s, "drawn");
    }
}

/// Make sure the eventually has an erasure to judge: for each signal with no
/// acked request, erase the erasable subject with the most acked records.
fn ensure_requests(s: &Shared) {
    for signal in SIGNALS {
        let pick = {
            let m = lock(&s.model);
            if m.requests
                .iter()
                .any(|r| r.signal == signal && r.acked.is_some())
                || m.requests.len() >= MAX_REQUESTS + 2
            {
                continue;
            }
            let mut per_subject = [0usize; ERASABLE];
            for w in m
                .writes
                .iter()
                .filter(|w| w.signal == signal && !w.buffered && w.acked_seq().is_some())
            {
                for (subject, _) in &w.records {
                    if let Some(n) = per_subject.get_mut(usize::from(*subject)) {
                        *n += 1;
                    }
                }
            }
            (0..ERASABLE)
                .filter(|&i| m.is_survivor_subject(signal, u8::try_from(i).unwrap_or(0)))
                .max_by_key(|&i| per_subject[i])
                .filter(|&i| per_subject[i] > 0)
        };
        if let Some(subject) = pick {
            let n = s.next_seq();
            let id = cli::uuid_from(0x1c00_0000_0000_0000 | n, now_ns());
            s.erase(signal, u8::try_from(subject).unwrap_or(0), id);
        }
    }
}

/// Make sure the retention tenant has an acknowledged write to retire.
fn ensure_retired(s: &Shared) {
    for attempt in 0..10 {
        if s.retired_acked.load(Ordering::SeqCst) > 0 {
            return;
        }
        if !s.write_retired(attempt % s.retire.nodes.len()) {
            std::thread::sleep(Duration::from_secs(3));
        }
    }
    println!("eventually: no write to the retention tenant was acknowledged");
}

/// Two retention jumps, before any horizon jump. After the first every
/// retention-tenant hour has expired and the maintain nodes tombstone it.
/// A sweep that skips the protection-horizon gate (the
/// `delete-before-horizon` mutant) then writes each bucket's unnamed-since
/// marker once the fold drops it from HEAD, and the second jump ages those
/// markers past the pinned-query window while every tombstone's horizon
/// stays more than a day ahead, so that sweep deletes inside the protection
/// window. The shipped sweep does nothing with the bucket until the horizon
/// jumps.
fn retention_phase(s: &Shared) {
    let acked = s.retired_acked.load(Ordering::SeqCst);
    for step in ["expire", "age the markers"] {
        if !s.shift(s.cfg.retention_jump_s, &format!("retention: {step}")) {
            return;
        }
        std::thread::sleep(s.cfg.tick_window * 3);
    }
    assert_reachable!(
        "clock: the retention jumps ran",
        &json!({ "retention_writes_acked": acked })
    );
}

/// Put an object of no known bucket-entry shape into the tenant's legal-hold
/// shard, beside a hold record, so every hold read until it is removed fails
/// closed. Returns its key, or `None` when there is no hold record to put it
/// beside or the PUT failed.
fn poison_holds(s: &Shared) -> Option<String> {
    let prefix = lock(&s.tenant_prefix).clone()?;
    let shard = format!("{prefix}u/c/0000/");
    let dir = match s.store.list(&shard) {
        Ok(keys) => keys
            .iter()
            .find_map(|k| k.rsplit_once('/').map(|(d, _)| d.to_string()))?,
        Err(e) => {
            println!("eventually: hold poison: {e}");
            return None;
        }
    };
    let key = format!("{dir}/{HOLD_POISON}");
    match s.store.put(&key, b"not a commit record") {
        Ok(()) => {
            println!("eventually: hold reads fail from now: {key}");
            assert_reachable!(
                "gc: the hold reads were made to fail",
                &json!({ "key": key })
            );
            Some(key)
        }
        Err(e) => {
            println!("eventually: hold poison: {e}");
            None
        }
    }
}

fn unpoison_holds(s: &Shared, key: &str) {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        match s.store.delete(key) {
            Ok(()) => {
                println!("eventually: hold reads work again: {key} removed");
                return;
            }
            Err(e) if Instant::now() >= deadline => {
                println!("eventually: could not remove {key}: {e}");
                return;
            }
            Err(_) => std::thread::sleep(Duration::from_secs(2)),
        }
    }
}

/// Clear every hold, acked or of unknown outcome, retrying each clear.
fn clear_all_holds(s: &Shared) {
    let to_clear: Vec<(Signal, u64)> = {
        let mut holds = lock(&s.holds);
        let mut v: Vec<(Signal, u64)> = holds
            .iter_mut()
            .filter(|h| !h.clear_requested)
            .map(|h| {
                h.clear_requested = true;
                (h.signal, h.shard)
            })
            .collect();
        v.extend(lock(&s.unsure_holds).drain(..));
        v.sort();
        v.dedup();
        v
    };
    for (signal, shard) in to_clear {
        let cleared = (0..3).any(|_| s.hold_clear(signal, shard));
        println!(
            "eventually: hold clear {} shard {shard}: {cleared}",
            signal.name()
        );
    }
}

/// Stop faults, take the erasures through completion and `.dreq` removal
/// (jumping the clocks as each step needs), judge the holds before and
/// after clearing them, then read everything back and end the timeline.
fn run_eventually(s: &Shared, reason: &str) -> ! {
    if !s.stop.begin() {
        halt();
    }
    let _quiet = s.gate.write().unwrap_or_else(|e| e.into_inner());
    println!("eventually ({reason}): stopping faults");
    if let Ok(cmd) = std::env::var("ANTITHESIS_STOP_FAULTS") {
        match std::process::Command::new(&cmd).arg("3600").status() {
            Ok(st) => println!("eventually: {cmd} 3600 exited {st}"),
            Err(e) => println!("eventually: running {cmd} failed: {e}"),
        }
    }
    if let Err(e) = s.wait_serving(Duration::from_secs(300), "eventually") {
        println!("eventually: not every node serves after 300 s: {e}");
    }
    ensure_requests(s);
    ensure_retired(s);
    s.check_holds();
    let any_acked = lock(&s.model)
        .writes
        .iter()
        .any(|w| w.acked_seq().is_some());
    if any_acked {
        s.seal_jump("eventually");
    }
    let held = s.held_signals();
    s.wait_until(
        Duration::from_secs(240),
        "every unheld request completed",
        |s| s.all_requests(false, &held),
    );
    // A hold defers every `.done` of its signal, and a rewrite published
    // more than the fold's reconcile window (26 h) after its hour sealed is
    // left to the sweep's targeted re-fold request, which a quiet system may
    // never act on, holding its `.dreq` for good. So the holds are judged and
    // cleared, and every request completes, before any horizon jump.
    s.check_holds();
    clear_all_holds(s);
    s.wait_until(Duration::from_secs(240), "every request completed", |s| {
        s.all_requests(false, &[])
    });
    retention_phase(s);
    // Then one hold, placed after completion, is judged across two horizon
    // jumps: the first lets a sweep mark the superseded inputs, the second
    // lets it delete them, except under the hold. Across the second, the
    // hold reads are made to fail for a few ticks first, while there is
    // something to delete: a tick whose hold read failed must delete nothing.
    if lock(&s.tenant_prefix).is_some() {
        s.hold_set(Signal::Logs, 0);
        // A key is judged by the last LIST that saw it, so list once after
        // the grace: a held key gone after that was deleted after the grace.
        std::thread::sleep(s.cfg.hold_grace + Duration::from_secs(5));
        s.check_holds();
        for round in 0..2 {
            let poison = if round == 1 { poison_holds(s) } else { None };
            let jumped = s.horizon_jump("eventually, hold active");
            if jumped {
                std::thread::sleep(s.cfg.tick_window + Duration::from_secs(20));
            }
            if let Some(key) = poison {
                std::thread::sleep(s.cfg.tick_window);
                unpoison_holds(s, &key);
                std::thread::sleep(s.cfg.tick_window);
            }
            if !jumped {
                break;
            }
            s.observe(true);
            s.check_holds();
        }
        clear_all_holds(s);
    }
    loop {
        if s.all_requests(true, &[]) {
            println!("eventually: every acked request's .dreq is gone");
            break;
        }
        s.wait_until(Duration::from_secs(240), "every request completed", |s| {
            s.all_requests(false, &[])
        });
        if !s.horizon_jump("eventually") {
            println!("eventually: no horizon jump left");
            break;
        }
        s.wait_until(
            Duration::from_secs(240),
            "every acked request's .dreq is gone",
            |s| s.all_requests(true, &[]),
        );
    }
    s.observe(true);
    final_reads(s);
    std::process::exit(0);
}

/// Retry a read for up to `limit`: a restarted node can answer 503 for a
/// while after reporting ready.
fn retry_read<T>(
    limit: Duration,
    mut read: impl FnMut(usize) -> Result<T, SqlFailure>,
) -> Result<T, SqlFailure> {
    let deadline = Instant::now() + limit;
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

/// The survivor ids of a batch of writes, their commit tokens, and the id
/// range one token read covers.
type SurvivorBatch = (Vec<u64>, Vec<String>, (u64, u64));

/// Every node's full scan, judged for the erasure and over-count properties
/// and for every survivor exactly once; then every survivor read with its
/// writes' commit tokens, judged the same way. A read that gives no answer
/// within its retry bound is no verdict: it is counted and printed, never
/// passed, as the catalog-maintenance driver counts its token reads. Only a
/// signal that no node could scan at all fails the survivor property.
fn final_reads(s: &Shared) {
    let n = s.ravel.nodes.len();
    let mut missing = Vec::new();
    let mut extra = Vec::new();
    let mut unscanned = Vec::new();
    let mut scans_without_verdict = Vec::new();
    let mut token_reads_without_verdict = Vec::new();
    let mut token_verdicts = 0usize;
    let mut expected = 0;
    for signal in SIGNALS {
        let survivors = lock(&s.model).survivors(signal);
        expected += survivors.len();
        let mut verdicts = 0;
        for node in 0..n {
            let issued = s.next_seq();
            match retry_read(SCAN_RETRY, |_| s.read_counts(signal, node, None, &[])) {
                Ok(counts) => {
                    verdicts += 1;
                    let v = model::judge_survivors(&survivors, &counts);
                    let at = format!("{} scan on node {node}", signal.name());
                    missing.extend(v.missing.iter().map(|id| (at.clone(), *id)));
                    extra.extend(v.extra.iter().map(|(id, c)| (at.clone(), *id, *c)));
                    s.judge(signal, issued, counts, "final scan");
                }
                Err(e) => {
                    s.reads_inconclusive.fetch_add(1, Ordering::SeqCst);
                    scans_without_verdict
                        .push(format!("{} scan on node {node}: {e}", signal.name()));
                }
            }
        }
        if verdicts == 0 {
            unscanned.push(signal.name());
        }
    }
    for signal in SIGNALS {
        let batches: Vec<SurvivorBatch> = {
            let m = lock(&s.model);
            let survivors: std::collections::BTreeSet<u64> =
                m.survivors(signal).into_iter().collect();
            let writes: Vec<&model::Write> = m
                .writes
                .iter()
                .filter(|w| w.signal == signal && w.ids().any(|id| survivors.contains(&id)))
                .collect();
            writes
                .chunks(TOKEN_BATCH)
                .map(|chunk| {
                    let ids: Vec<u64> = chunk
                        .iter()
                        .flat_map(|w| w.ids())
                        .filter(|id| survivors.contains(id))
                        .collect();
                    let tokens: Vec<String> = chunk.iter().flat_map(|w| w.tokens()).collect();
                    let lo = chunk.first().map_or(0, |w| w.first_rec);
                    let hi = chunk.last().map_or(0, |w| w.ids().end.saturating_sub(1));
                    (ids, tokens, (lo, hi))
                })
                .collect()
        };
        for (i, (ids, tokens, range)) in batches.into_iter().enumerate() {
            let issued = s.next_seq();
            match retry_read(TOKEN_READ_RETRY, |attempt| {
                s.read_counts(signal, (i + attempt) % n, Some(range), &tokens)
            }) {
                Ok(counts) => {
                    token_verdicts += 1;
                    let v = model::judge_survivors(&ids, &counts);
                    let at = format!("{} token read of {range:?}", signal.name());
                    missing.extend(v.missing.iter().map(|id| (at.clone(), *id)));
                    extra.extend(v.extra.iter().map(|(id, c)| (at.clone(), *id, *c)));
                    s.judge(signal, issued, counts, "final token read");
                }
                Err(e) => {
                    s.reads_inconclusive.fetch_add(1, Ordering::SeqCst);
                    token_reads_without_verdict
                        .push(format!("{} token read of {range:?}: {e}", signal.name()));
                }
            }
        }
    }
    for r in scans_without_verdict
        .iter()
        .chain(&token_reads_without_verdict)
    {
        println!("eventually: no verdict: {r}");
    }
    let requests: Vec<serde_json::Value> = lock(&s.model)
        .requests
        .iter()
        .map(|r| {
            json!({
                "id": r.request_id, "signal": r.signal.name(),
                "subject": SUBJECTS[usize::from(r.subject)],
                "acked": r.acked.is_some(), "done": r.done.is_some(), "gone": r.gone.is_some(),
            })
        })
        .collect();
    let clock_offset = lock(&s.clock).offset_s;
    assert_always!(
        missing.is_empty() && extra.is_empty() && unscanned.is_empty(),
        P3,
        &json!({
            "survivors_expected": expected,
            "missing": first(&missing),
            "extra_copies": first(&extra),
            "signals_no_node_scanned": unscanned,
            "scans_without_verdict": first(&scans_without_verdict),
            "token_reads_with_verdict": token_verdicts,
            "token_reads_without_verdict": first(&token_reads_without_verdict),
            "requests": requests.clone(),
            "clock_offset_s": clock_offset,
        })
    );
    println!(
        "eventually: ending the timeline (survivors {expected}, missing {}, extra {}, \
         scans without verdict {}, token reads with verdict {token_verdicts}, without {}, \
         reads ok {}, inconclusive {}, injected {}, rewrite seen {}, clock offset +{clock_offset}, requests {})",
        missing.len(),
        extra.len(),
        scans_without_verdict.len(),
        token_reads_without_verdict.len(),
        s.reads_ok.load(Ordering::SeqCst),
        s.reads_inconclusive.load(Ordering::SeqCst),
        s.injected.load(Ordering::SeqCst),
        s.rewrite_seen.load(Ordering::SeqCst),
        serde_json::Value::Array(requests)
    );
}

fn wait_for(what: &str, mut check: impl FnMut() -> Result<(), String>) {
    let mut last = String::new();
    loop {
        match check() {
            Ok(()) => {
                println!("startup: {what}");
                return;
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

#[hegel::main(phases = [hegel::Phase::Generate])]
fn main(tc: TestCase) {
    antithesis_sdk::antithesis_init();
    let cfg = Config::from_env();
    if let Err(e) = clock::check_jumps(cfg.seal_jump_s, cfg.horizon_jump_s, cfg.retention_jump_s) {
        die(&e);
    }
    match std::fs::read_to_string(&cfg.clock_file) {
        Ok(t) if clock::is_start_offset(&t) => {}
        Ok(t) => die(&format!(
            "{} holds {:?}, not the starting offset +0",
            cfg.clock_file.display(),
            t.trim()
        )),
        Err(e) => die(&format!("{}: {e}", cfg.clock_file.display())),
    }
    let store = Store::from_env().unwrap_or_else(|e| die(&e));
    wait_for("the bucket answers LIST", || {
        store.list("antithesis/").map(|_| ())
    });
    match store.get(&cfg.ready_marker) {
        Ok(None) => {}
        Ok(Some(_)) => die("the ready marker already exists: an earlier driver's model is gone"),
        Err(e) => die(&e),
    }
    wait_for("the ready marker is written", || {
        store.put(&cfg.ready_marker, b"ready")
    });
    let ravel = Ravel::new(cfg.nodes.clone(), cfg.token.clone()).unwrap_or_else(|e| die(&e));
    let retire =
        Ravel::new(cfg.nodes.clone(), cfg.retire_token.clone()).unwrap_or_else(|e| die(&e));
    let maint = Ravel::new(cfg.maint_nodes.clone(), cfg.token.clone()).unwrap_or_else(|e| die(&e));
    let force_after = cfg.eventually_after.map(Duration::from_secs);
    let mut shared = Shared {
        cfg,
        ravel,
        retire,
        maint,
        cli: Cli::from_env(),
        store,
        seq: AtomicU64::new(1),
        model: Mutex::new(Model::default()),
        tenant_prefix: Mutex::new(None),
        holds: Mutex::new(Vec::new()),
        unsure_holds: Mutex::new(Vec::new()),
        clock: Mutex::new(ClockState::default()),
        gate: RwLock::new(()),
        stop: Stop::new(None),
        reads_ok: AtomicU64::new(0),
        reads_inconclusive: AtomicU64::new(0),
        injected: AtomicU64::new(0),
        rewrite_seen: AtomicBool::new(false),
        retired_acked: AtomicU64::new(0),
    };
    wait_for("every node serves", || shared.all_nodes_serve());
    antithesis_sdk::lifecycle::setup_complete(&json!({
        "nodes": shared.cfg.nodes,
        "maintain_nodes": shared.cfg.maint_nodes,
        "placements": shared.cfg.placements.iter().map(|p| p.name()).collect::<Vec<_>>(),
        "inject_served_erased": shared.cfg.inject,
    }));
    println!("startup: setup complete; waiting for /workload-active");
    while !Path::new("/workload-active").exists() {
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_reachable!("lifecycle: the workload starts", &json!({}));
    shared.stop = Stop::new(force_after);
    machine(Workload(Arc::new(shared)))
        .steps(i64::MAX)
        .max_concurrency(4)
        .run_concurrent(tc);
}
