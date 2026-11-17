//! Antithesis driver for the catalog-maintenance workload (ADR-4326).
//!
//! Setup seeds a fresh tenant with whole past hours through `ravel-cli load
//! --ingest-hour-stride`, records every seeded record in the model, and then
//! writes the seed marker the ravel-server entrypoints wait on. Rules write
//! strict OTLP logs into the same tenant, run `ravel-cli` fold, compaction and
//! sweep on demand, overwrite HEAD with garbage through raw S3, audit the
//! store against what HEAD names, and check the stored multiset of seeded and
//! live records. See antithesis/catalog-maintenance/README.md for the
//! properties.

mod audit;
mod cli;
mod multiset;
mod s3;
mod seed;

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
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

struct Config {
    nodes: Vec<String>,
    token: String,
    tenant: String,
    seed_hours: u32,
    seed_stride: u32,
    seed_per_hour: u32,
    max_writes: usize,
    inject_swap: bool,
}

impl Config {
    fn from_env() -> Config {
        let nodes = std::env::var("RAVEL_NODES")
            .unwrap_or_else(|_| "http://ravel1:4318,http://ravel2:4318".to_string())
            .split(',')
            .map(|s| s.trim().trim_end_matches('/').to_string())
            .filter(|s| !s.is_empty())
            .collect();
        Config {
            nodes,
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
        }
    }
}

struct Shared {
    ravel: Ravel,
    cli: Cli,
    store: Store,
    tenant: String,
    plan: Plan,
    /// The seeded records the multiset check expects, each exactly once.
    seeded: BTreeSet<(u64, u32)>,
    /// The live strict writes.
    model: Mutex<Model>,
    max_writes: usize,
    /// The HEAD key, learned from the first `catalog inspect --json`.
    head_key: Mutex<Option<String>>,
    /// Whether an audit has seen a compaction record, for the multiset
    /// check's details.
    compacted: AtomicBool,
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

/// What one `catalog inspect --json` showed.
struct HeadView {
    head_key: String,
    /// `None` when there is no HEAD yet.
    created_unix_ns: Option<u64>,
    watermark_hour: Option<u32>,
}

impl Shared {
    fn args<'a>(&'a self, words: &[&'a str]) -> Vec<&'a str> {
        let mut v = words.to_vec();
        v.extend(["--tenant", self.tenant.as_str(), "--signal", "logs"]);
        v
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
        let mut m = lock(&self.model);
        let acked = matches!(attempt, Attempt::Acked { .. });
        if let Some(slot) = m.writes.get_mut(&id).and_then(|w| w.attempts.get_mut(k)) {
            *slot = attempt;
        }
        if acked {
            m.mark_acked(id);
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
        let view = HeadView {
            head_key,
            created_unix_ns: head
                .and_then(|h| h.get("created_unix_ns"))
                .and_then(serde_json::Value::as_u64),
            watermark_hour: head
                .and_then(|h| h.get("watermark_hour"))
                .and_then(serde_json::Value::as_u64)
                .and_then(|w| u32::try_from(w).ok()),
        };
        if let Some(wm) = view.watermark_hour {
            self.observe_watermark(wm);
        }
        Some(view)
    }

    /// The fold-progress properties, on every HEAD the driver reads.
    fn observe_watermark(&self, wm: u32) {
        let seeded_folded: Vec<u32> = self
            .plan
            .hours_list()
            .into_iter()
            .filter(|h| *h <= wm)
            .collect();
        assert_sometimes!(
            !seeded_folded.is_empty(),
            "catalog: a sealed seeded hour was folded into HEAD",
            &json!({ "watermark_hour": wm, "seeded_hours_folded": seeded_folded.len() })
        );
        let live_hours: BTreeSet<u64> = {
            let m = lock(&self.model);
            m.acked_logs
                .iter()
                .filter_map(|id| m.writes.get(id))
                .map(|w| w.ts_ns / NS_PER_HOUR)
                .collect()
        };
        let live_sealed = live_hours.iter().filter(|h| **h <= u64::from(wm)).count();
        assert_sometimes!(
            live_sealed > 0,
            "clock: an hour ingested live was sealed by a fold",
            &json!({ "watermark_hour": wm, "live_hours": live_hours.len(), "sealed": live_sealed })
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

    fn scan(&self, node: usize) -> Result<multiset::Verdict, SqlFailure> {
        let (start, end) = self.window_s();
        let expect = lock(&self.model).acked_logs.clone();
        let value = self
            .ravel
            .sql(node, LOGS_QUERY, start, end, &[], READ_TIMEOUT)?;
        let counts = model::log_counts(ravel::rows(&value)).map_err(SqlFailure::BadBody)?;
        let m = lock(&self.model);
        Ok(multiset::judge(&self.seeded, &m, &counts, &expect))
    }

    /// Scan one node and assert the multiset. A verdict whose only finding
    /// is absences is re-read for up to two minutes first: a node can lag
    /// the newest commit it has not listed yet.
    fn check_multiset(&self, node: usize, after: &str) {
        let deadline = Instant::now() + Duration::from_secs(120);
        let verdict = loop {
            match self.scan(node) {
                Ok(v) if v.holds() || !v.only_missing() || Instant::now() >= deadline => break v,
                Ok(_) => std::thread::sleep(Duration::from_secs(3)),
                Err(e) => {
                    println!("multiset scan on node {node} ({after}): {e}");
                    return;
                }
            }
        };
        assert_always!(
            verdict.holds(),
            "catalog: compaction preserves the acked multiset",
            &json!({
                "node": node,
                "after": after,
                "compaction_record_seen": self.compacted.load(Ordering::SeqCst),
                "rows": verdict.rows,
                "seeded_missing": first(&verdict.seeded_missing),
                "seeded_over": first(&verdict.seeded_over),
                "phantoms": first(&verdict.phantoms),
                "over": first(&verdict.over),
                "missing": first(&verdict.missing),
            })
        );
    }

    /// Inspect, list, inspect again: the audit judges only a HEAD that did
    /// not change while the bucket was listed.
    fn audit(&self, tc: &TestCase) {
        let Some(before) = self.inspect_json() else {
            return;
        };
        if before.created_unix_ns.is_none() {
            println!("audit: no HEAD yet");
            return;
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
            return;
        }
        let Some(prefix) = audit::tenant_prefix(&before.head_key) else {
            println!("audit: unexpected HEAD key {}", before.head_key);
            return;
        };
        let listed: BTreeSet<String> = match self.store.list(&prefix) {
            Ok(keys) => keys.into_iter().collect(),
            Err(e) => {
                println!("audit: {e}");
                return;
            }
        };
        let Some(after) = self.inspect_json() else {
            return;
        };
        if after.created_unix_ns != before.created_unix_ns
            || inspected.created_unix_ns != before.created_unix_ns
        {
            println!("audit: HEAD moved while listing; skipped");
            return;
        }
        let mut named = inspected.named.clone();
        if let Some(k) = unfetched
            && !named.contains(&k)
        {
            named.push(k);
        }
        let missing = audit::missing_named(&named, &listed);
        assert_always!(
            missing.is_empty(),
            "catalog: every object HEAD names exists",
            &json!({
                "head_key": before.head_key,
                "created_unix_ns": before.created_unix_ns,
                "named": named.len(),
                "listed": listed.len(),
                "missing": first(&missing),
            })
        );

        let compactions = audit::compaction_records(&prefix, &listed);
        let decoded = match pick(tc, &compactions) {
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
        if let Some(key) = pick(tc, &l0) {
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
    }
}

fn pick<'a>(tc: &TestCase, keys: &'a [String]) -> Option<&'a str> {
    if keys.is_empty() {
        return None;
    }
    let i = draw_node(tc, keys.len());
    keys.get(i).map(String::as_str)
}

#[hegel::concurrent_state_machine]
impl Workload {
    #[rule(groups = ["main"], weight = 6)]
    fn write_logs(&self, tc: TestCase) {
        let s = &self.0;
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
        let out = s.cli.run(
            &s.args(&["catalog", "fold", "--shards", "4"]),
            CLI_TIMEOUT_S,
        );
        if !out.ok() {
            println!("catalog fold: {}", out.summary());
            return;
        }
        assert_reachable!("catalog: an on-demand fold succeeded", &json!({}));
        let _ = s.inspect_json();
    }

    #[rule(groups = ["main"], weight = 1)]
    fn compact(&self, tc: TestCase) {
        let s = &self.0;
        let out = s.cli.run(
            &s.args(&["maintain", "compact-tenant", "--shards", "4"]),
            CLI_TIMEOUT_S * 2,
        );
        if !out.ok() {
            println!("maintain compact-tenant: {}", out.summary());
            return;
        }
        let node = draw_node(&tc, s.ravel.nodes.len());
        s.check_multiset(node, "on-demand compaction");
    }

    #[rule(groups = ["main"], weight = 1)]
    fn sweep(&self, tc: TestCase) {
        let s = &self.0;
        let shard = tc
            .draw(gs::integers::<u32>().min_value(0).max_value(3))
            .to_string();
        let out = s.cli.run(
            &s.args(&["maintain", "sweep", "--shard", &shard]),
            CLI_TIMEOUT_S,
        );
        if !out.ok() {
            println!("maintain sweep --shard {shard}: {}", out.summary());
        }
    }

    #[rule(groups = ["main"], weight = 1)]
    fn corrupt_head(&self, tc: TestCase) {
        let s = &self.0;
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
        self.0.audit(&tc);
    }

    #[rule(groups = ["main"], weight = 2)]
    fn check_multiset(&self, tc: TestCase) {
        let s = &self.0;
        let node = draw_node(&tc, s.ravel.nodes.len());
        s.check_multiset(node, "scan");
    }
}

/// Wait, with no deadline of its own, until `check` passes, printing the
/// reason whenever it changes.
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

fn die(msg: &str) -> ! {
    eprintln!("driver: {msg}");
    std::process::exit(1);
}

/// Seed the tenant through `ravel-cli load` and return the plan. Runs before
/// any ravel-server starts: every server waits for the marker this writes.
fn seed(cfg: &Config, store: &Store, cli: &Cli) -> Plan {
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
    let out = cli.run(&args, 900);
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

#[hegel::main(phases = [hegel::Phase::Generate])]
fn main(tc: TestCase) {
    antithesis_sdk::antithesis_init();
    let cfg = Config::from_env();
    let store = Store::from_env().unwrap_or_else(|e| die(&e));
    let cli = Cli::from_env();
    wait_for("the bucket answers LIST", || {
        store.list("antithesis/").map(|_| ())
    });
    let plan = seed(&cfg, &store, &cli);
    let seeded = plan.expected(cfg.inject_swap);
    let ravel = Ravel::new(cfg.nodes.clone(), cfg.token.clone()).unwrap_or_else(|e| die(&e));
    let shared = Arc::new(Shared {
        ravel,
        cli,
        store,
        tenant: cfg.tenant.clone(),
        plan,
        seeded,
        model: Mutex::new(Model::default()),
        max_writes: cfg.max_writes,
        head_key: Mutex::new(None),
        compacted: AtomicBool::new(false),
    });

    for node in 0..shared.ravel.nodes.len() {
        wait_for(&format!("node {node} is ready"), || {
            shared.ravel.ready(node)
        });
    }
    wait_for("every node answers SQL", || {
        let (start, end) = shared.window_s();
        for node in 0..shared.ravel.nodes.len() {
            shared
                .ravel
                .sql(
                    node,
                    "SELECT count(*) FROM logs",
                    start,
                    end,
                    &[],
                    READ_TIMEOUT,
                )
                .map_err(|e| format!("node {node} does not answer SQL: {e}"))?;
        }
        Ok(())
    });
    antithesis_sdk::lifecycle::setup_complete(&json!({
        "nodes": cfg.nodes,
        "seed_start_hour": shared.plan.start_hour,
        "seeded_records": shared.seeded.len(),
    }));
    println!("startup: setup complete; waiting for /workload-active");
    while !Path::new("/workload-active").exists() {
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_reachable!("catalog: the workload starts", &json!({}));

    machine(Workload(shared))
        .steps(i64::MAX)
        .max_concurrency(4)
        .run_concurrent(tc);
}
