//! The coverage variant (`WORKLOAD_COVERAGE=1`; README.md, "Coverage
//! variant"): poison records, graceful drains asked of the supervisor, and the
//! properties they make checkable.
//!
//! Every assertion here goes through `assert_raw` and is catalogued only when
//! the variant is on. An assertion macro would add a catalog entry to every
//! run of the binary, and the base variant would then report these
//! properties as never reached.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use antithesis_sdk::assert::{AssertType, assert_raw};
use hegel::TestCase;
use hegel::generators as gs;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric, number_data_point,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use serde_json::{Value, json};

use ravel_antithesis_driver::model::{Attempt, Mode, Model, Signal, Write};
use ravel_antithesis_driver::otlp::{self, PartialSuccess, VALUE_STRIDE};
use ravel_antithesis_driver::ravel::Reply;

use crate::{draw_node, lock};

/// A poison log record's body length: above ravel-server's default 64 KiB
/// log body limit, so normalization rejects that record alone.
const POISON_BODY_BYTES: usize = 70_000;
/// A poison data point's label value length: above the default 4096-byte
/// label value limit, so normalization rejects that point alone.
const POISON_LABEL_BYTES: usize = 5_000;
const POISON_LABEL: &str = "wl_poison";

/// The 429 bodies ravel-server gives a write it refuses at the request head,
/// before reading the body: no shard is touched and nothing is stored. The
/// flush deferral cap's 429 is not among them: a log write spanning several
/// shards can commit on one shard before another refuses it.
const HEAD_SHED_BODIES: [&str; 2] = [
    "process pending strict-acknowledgement limit reached",
    "process in-flight ingest-request limit reached",
];

/// The least time between two drain requests, so the nodes spend most of
/// the run serving.
const DRAIN_INTERVAL: Duration = Duration::from_secs(60);
/// The same under the self-check, so its broken drains come early in a run.
const SELF_CHECK_DRAIN_INTERVAL: Duration = Duration::from_secs(10);
/// How long the eventually waits, once faults stop, for the answer to a drain
/// the supervisor already took: above ravel-server's 25 s shutdown timeout
/// plus a container restart, after which the supervisor reports a drain that
/// a kill cut short.
const DRAIN_SETTLE: Duration = Duration::from_secs(180);
/// The exit status of a ravel-server that got SIGTERM before it installed its
/// handler (#4493): the supervisor's wait after readiness makes it rare, but
/// does not rule it out, so it is logged rather than judged.
const SIGTERM_BEFORE_HANDLER: i32 = 143;

#[derive(Clone, Copy)]
enum Kind {
    Always,
    Sometimes,
    Reachable,
}

struct Prop {
    kind: Kind,
    message: &'static str,
}

const HEAD_SHED_STORES_NOTHING: Prop = Prop {
    kind: Kind::Always,
    message: "a write shed at the request head on every attempt stores nothing",
};
const POISON_REJECTED: Prop = Prop {
    kind: Kind::Always,
    message: "a strict write's partial success rejects every poison record",
};
const POISON_ONLY_HAS_TOKEN: Prop = Prop {
    kind: Kind::Always,
    message: "a strict partial success that rejected only the poison records carries a commit token",
};
const POISON_ACKED: Prop = Prop {
    kind: Kind::Reachable,
    message: "a strict write with poison records was acknowledged with commit tokens",
};
const POISON_READ: Prop = Prop {
    kind: Kind::Reachable,
    message: "a token read checked an acknowledged write that carried poison records",
};
const ERROR_HAS_NO_TOKEN: Prop = Prop {
    kind: Kind::Always,
    message: "a write answered with an error status carries no commit token",
};
const DRAIN_EXIT: Prop = Prop {
    kind: Kind::Always,
    message: "a driver-requested drain exits with status 0 or 1",
};
const DRAIN_CLEAN: Prop = Prop {
    kind: Kind::Sometimes,
    message: "a driver-requested drain exited cleanly",
};
const DRAIN_ANSWERED: Prop = Prop {
    kind: Kind::Always,
    message: "a driver-requested drain is answered or reported interrupted",
};
const METRICS_429: Prop = Prop {
    kind: Kind::Reachable,
    message: "a metrics write was answered 429",
};

/// Fault-switch profile only (`WORKLOAD_FAULT_SWITCHES=1`): every node was
/// restarted after the driver wrote `faults-off`, so none runs with Ravel's
/// fault switches on while the eventually checks recovery.
const FAULTS_OFF: Prop = Prop {
    kind: Kind::Always,
    message: "every node restarted with its fault switches off before the eventually's checks",
};

/// How long the eventually waits for each node's switch-off restart.
const FAULTS_OFF_RESTART: Duration = Duration::from_secs(180);
/// How long the eventually waits for a node to answer `readyz` before it
/// asks for its switch-off restart: the recovery wait the eventually
/// allows the same nodes.
const FAULTS_OFF_READY: Duration = Duration::from_secs(300);

const PROPS: [&Prop; 10] = [
    &DRAIN_ANSWERED,
    &HEAD_SHED_STORES_NOTHING,
    &POISON_REJECTED,
    &POISON_ONLY_HAS_TOKEN,
    &POISON_ACKED,
    &POISON_READ,
    &ERROR_HAS_NO_TOKEN,
    &DRAIN_EXIT,
    &DRAIN_CLEAN,
    &METRICS_429,
];

fn emit(p: &Prop, condition: bool, hit: bool, details: &Value) {
    let (assert_type, display_type) = match p.kind {
        Kind::Always => (AssertType::Always, "Always"),
        Kind::Sometimes => (AssertType::Sometimes, "Sometimes"),
        Kind::Reachable => (AssertType::Reachability, "Reachable"),
    };
    assert_raw(
        condition,
        p.message.to_string(),
        details,
        module_path!().to_string(),
        "coverage".to_string(),
        file!().to_string(),
        0,
        0,
        hit,
        true,
        assert_type,
        display_type.to_string(),
        p.message.to_string(),
    );
}

fn check(p: &Prop, condition: bool, details: Value) {
    emit(p, condition, true, &details);
}

struct PendingDrain {
    host: String,
    seq: u64,
}

#[derive(Default)]
struct Drains {
    next_seq: u64,
    pending: Option<PendingDrain>,
    last_request: Option<Instant>,
}

pub struct Coverage {
    control: PathBuf,
    /// Per write: how many of its attempts were shed at the request head.
    /// Counted when the response arrives, before the attempt's outcome is
    /// recorded in the model, so the count never exceeds the attempts the
    /// model holds for the write.
    head_shed: Mutex<HashMap<u64, usize>>,
    /// Per write: how many poison records or points it carries.
    poison: Mutex<HashMap<u64, u32>>,
    drains: Mutex<Drains>,
    /// `WORKLOAD_FAULT_SWITCHES=1`: the servers run with
    /// `RAVEL_ANTITHESIS_FAULTS` set, which the eventually turns off.
    pub fault_switches: bool,
    /// `WORKLOAD_COVERAGE_SELF_CHECK=1`: the local loop's self-check. Each of
    /// the variant's always-properties is fed an input that breaks it, so
    /// each must fail; the supervisor breaks the drains to match.
    pub self_check: bool,
}

impl Coverage {
    /// The variant's state, with every assertion catalogued, when
    /// `WORKLOAD_COVERAGE=1`. Call after `antithesis_init`.
    pub fn from_env() -> Option<Coverage> {
        if std::env::var("WORKLOAD_COVERAGE").ok().as_deref() != Some("1") {
            return None;
        }
        for p in PROPS {
            emit(p, false, false, &Value::Null);
        }
        let control = std::env::var("RAVEL_CONTROL_DIR").unwrap_or_else(|_| "/control".to_string());
        let self_check = std::env::var("WORKLOAD_COVERAGE_SELF_CHECK")
            .ok()
            .as_deref()
            == Some("1");
        let fault_switches = std::env::var("WORKLOAD_FAULT_SWITCHES").ok().as_deref() == Some("1");
        if fault_switches {
            emit(&FAULTS_OFF, false, false, &Value::Null);
        }
        println!(
            "coverage variant: on, control directory {control}, self-check {self_check}, \
             fault switches {fault_switches}"
        );
        let mut cov = Coverage::new(PathBuf::from(control), self_check);
        cov.fault_switches = fault_switches;
        Some(cov)
    }

    fn new(control: PathBuf, self_check: bool) -> Coverage {
        Coverage {
            control,
            head_shed: Mutex::new(HashMap::new()),
            poison: Mutex::new(HashMap::new()),
            drains: Mutex::new(Drains::default()),
            fault_switches: false,
            self_check,
        }
    }

    /// The fault-switch profile's half of stopping faults: write `faults-off`
    /// to the control volume, which every later supervisor start reads, then
    /// drain each node in turn and wait for it to come back. Each server read
    /// `RAVEL_ANTITHESIS_FAULTS` once at startup, so only a restart turns its
    /// switches off. Call after `settle_drains`, with no drain pending.
    ///
    /// `ready(i)` says whether node `i` answers `readyz`. A node a fault
    /// stopped just before the eventually is waited for, up to
    /// [`FAULTS_OFF_READY`], before its drain is requested: a request no
    /// supervisor is alive to take is not a failed switch-off.
    pub fn switch_faults_off(&self, nodes: &[String], ready: &dyn Fn(usize) -> bool) {
        if !self.fault_switches {
            return;
        }
        let (restarted, failed) =
            self.switch_faults_off_within(nodes, ready, FAULTS_OFF_READY, FAULTS_OFF_RESTART);
        println!("fault switches: restarted {restarted:?}, failed {failed:?}");
        check(
            &FAULTS_OFF,
            failed.is_empty(),
            json!({ "restarted": restarted, "failed": failed }),
        );
    }

    /// [`Self::switch_faults_off`] with its two waits given, returning the
    /// nodes restarted and the failures.
    fn switch_faults_off_within(
        &self,
        nodes: &[String],
        ready: &dyn Fn(usize) -> bool,
        ready_wait: Duration,
        answer_wait: Duration,
    ) -> (Vec<String>, Vec<String>) {
        let mut restarted = Vec::new();
        let mut failed = Vec::new();
        let off = self.control.join("faults-off");
        if let Err(e) = self.put(&off, "1\n") {
            failed.push(format!("writing {}: {e}", off.display()));
            return (restarted, failed);
        }
        let mut d = lock(&self.drains);
        for (i, url) in nodes.iter().enumerate() {
            let Some(host) = host_of(url) else {
                failed.push(format!("no host in {url}"));
                continue;
            };
            let ready_by = Instant::now() + ready_wait;
            while !ready(i) {
                if Instant::now() >= ready_by {
                    break;
                }
                std::thread::sleep(Duration::from_millis(500));
            }
            if !ready(i) {
                failed.push(format!("{host}: not ready within {ready_wait:?}"));
                continue;
            }
            d.next_seq += 1;
            let seq = d.next_seq;
            let request = self.control.join(format!("drain-{host}"));
            if let Err(e) = self.put(&request, &format!("{seq}\n")) {
                failed.push(format!("{host}: writing the request: {e}"));
                continue;
            }
            println!("fault switches: restarting {host} with switches off (seq {seq})");
            let result = self.control.join(format!("drained-{host}"));
            let deadline = Instant::now() + answer_wait;
            let answer = loop {
                match std::fs::read_to_string(&result) {
                    Ok(text) if parse_drain_result(&text).seq == Some(seq) => {
                        let _ = std::fs::remove_file(&result);
                        break Some(text);
                    }
                    Ok(_) | Err(_) if Instant::now() >= deadline => break None,
                    _ => std::thread::sleep(Duration::from_millis(200)),
                }
            };
            match answer {
                // An interrupted drain means the container was stopped and
                // started again; its new supervisor also read `faults-off`.
                Some(text) => restarted.push(format!("{host}: {}", text.trim())),
                None => {
                    let _ = std::fs::remove_file(&request);
                    failed.push(format!("{host}: no answer in {answer_wait:?}"));
                }
            }
        }
        d.last_request = Some(Instant::now());
        (restarted, failed)
    }

    /// Poison records for a log write: only strict and unkeyed ones, since a
    /// keyed retry can replay the receipt with no partial success at all.
    pub fn draw_log_poison(&self, tc: &TestCase, mode: Mode, keyed: bool) -> u32 {
        if mode == Mode::Buffered || keyed || !tc.draw(gs::weighted_booleans(0.2)) {
            return 0;
        }
        tc.draw(gs::integers::<u32>().min_value(1).max_value(2))
    }

    /// Poison points for a metrics write: only strict ones.
    pub fn draw_metric_poison(&self, tc: &TestCase, mode: Mode) -> u32 {
        if mode == Mode::Buffered || !tc.draw(gs::weighted_booleans(0.2)) {
            return 0;
        }
        1
    }

    pub fn set_poison(&self, id: u64, poison: u32) {
        if poison > 0 {
            lock(&self.poison).insert(id, poison);
        }
    }

    fn poison(&self, id: u64) -> u32 {
        lock(&self.poison).get(&id).copied().unwrap_or(0)
    }

    /// A strict 2xx whose partial success rejected exactly the poison
    /// records, with a commit token, acknowledges every other record: the
    /// poison is rejected by size alone, so the rejection names exactly
    /// those records. Any other answer keeps the base classification.
    pub fn reclassify(&self, w: &Write, reply: &Reply, attempt: Attempt) -> Attempt {
        let poison = self.poison(w.id);
        if poison == 0 || w.mode == Mode::Buffered {
            return attempt;
        }
        match reply {
            Reply::Status {
                status,
                commit_token: Some(header),
                body,
            } if (200..300).contains(status)
                && !header.is_empty()
                && otlp::partial_success(w.signal == Signal::Logs, body)
                    == PartialSuccess::Rejected(i64::from(poison)) =>
            {
                Attempt::Acked {
                    header: header.clone(),
                }
            }
            _ => attempt,
        }
    }

    /// The variant's assertions on one write response. Call it before the
    /// attempt's outcome is recorded in the model.
    pub fn observe(&self, w: &Write, node: usize, reply: &Reply, attempt: &Attempt) {
        let logs = w.signal == Signal::Logs;
        let poison = self.poison(w.id);
        if let Reply::Status {
            status,
            commit_token,
            body,
        } = reply
        {
            let details = json!({
                "write": w.id,
                "signal": format!("{:?}", w.signal),
                "mode": format!("{:?}", w.mode),
                "node": node,
                "status": status,
                "poison": poison,
                "commit_token": commit_token,
                "body": String::from_utf8_lossy(&body[..body.len().min(400)]),
            });
            let ok = (200..300).contains(status);
            // The self-check reads every response as if the header were
            // flipped: present on an error, absent on a partial success.
            let token_seen = commit_token.as_deref().is_some_and(|t| !t.is_empty());
            let token_seen_on_error = token_seen || (self.self_check && *status == 429);
            let token_seen_on_partial = token_seen && !self.self_check;
            // The self-check also expects one poison record more than was sent.
            let expected_poison = i64::from(poison) + i64::from(self.self_check);
            if !ok {
                check(&ERROR_HAS_NO_TOKEN, !token_seen_on_error, details.clone());
            }
            if *status == 429 {
                if !logs {
                    check(&METRICS_429, true, details.clone());
                }
                let text = String::from_utf8_lossy(body);
                if HEAD_SHED_BODIES.iter().any(|b| *b == text.trim()) {
                    *lock(&self.head_shed).entry(w.id).or_insert(0) += 1;
                }
            }
            if ok && poison > 0 && w.mode != Mode::Buffered {
                let rejected = match otlp::partial_success(logs, body) {
                    PartialSuccess::Rejected(n) => Some(n),
                    _ => None,
                };
                check(
                    &POISON_REJECTED,
                    rejected.is_some_and(|n| n >= expected_poison),
                    details.clone(),
                );
                if rejected == Some(i64::from(poison)) {
                    check(&POISON_ONLY_HAS_TOKEN, token_seen_on_partial, details);
                }
            }
        }
        if poison > 0 && matches!(attempt, Attempt::Acked { .. }) {
            check(
                &POISON_ACKED,
                true,
                json!({ "write": w.id, "poison": poison }),
            );
        }
    }

    /// A token read of an acknowledged write came back; the presence
    /// assertions beside the call judge its rows.
    pub fn note_token_read(&self, w: &Write) {
        let poison = self.poison(w.id);
        if poison > 0 {
            check(
                &POISON_READ,
                true,
                json!({ "write": w.id, "poison": poison }),
            );
        }
    }

    /// Writes of `signal` whose every attempt so far was shed at the request
    /// head. Call with the model locked. The self-check also counts every
    /// acknowledged write whose id is a multiple of 7, which is stored.
    fn fully_shed(&self, m: &Model, signal: Signal) -> HashSet<u64> {
        let mut shed: HashSet<u64> = lock(&self.head_shed)
            .iter()
            .filter(|(id, shed)| {
                m.writes
                    .get(*id)
                    .is_some_and(|w| w.signal == signal && w.attempts.len() == **shed)
            })
            .map(|(id, _)| *id)
            .collect();
        if self.self_check {
            shed.extend(
                m.writes
                    .values()
                    .filter(|w| w.signal == signal && w.id % 7 == 0 && w.is_acked())
                    .map(|w| w.id),
            );
        }
        shed
    }

    /// Head-shed check on the rows of a log scan, `(write, record) -> count`.
    pub fn check_log_rows(&self, m: &Model, counts: &HashMap<(u64, u32), u64>, node: usize) {
        let shed = self.fully_shed(m, Signal::Logs);
        if shed.is_empty() {
            return;
        }
        let stored: Vec<String> = counts
            .iter()
            .filter(|((wid, _), _)| shed.contains(wid))
            .map(|((wid, rec), n)| format!("write {wid} rec {rec} x{n}"))
            .collect();
        check(
            &HEAD_SHED_STORES_NOTHING,
            stored.is_empty(),
            json!({
                "node": node,
                "signal": "logs",
                "shed_writes": shed.len(),
                "stored": crate::first(&stored),
            }),
        );
    }

    /// Head-shed check on the rows of a metrics scan, `(ts, value, series)`.
    pub fn check_metric_rows(&self, m: &Model, rows: &[(i64, f64, String)], node: usize) {
        let shed = self.fully_shed(m, Signal::Metrics);
        if shed.is_empty() {
            return;
        }
        let stored: Vec<String> = rows
            .iter()
            .filter(|(_, v, _)| {
                *v >= 0.0
                    && v.fract() == 0.0
                    && *v < 9.0e15
                    && shed.contains(&((*v as u64) / VALUE_STRIDE))
            })
            .map(|(ts, v, s)| format!("sample ts={ts} value={v} series={s}"))
            .collect();
        check(
            &HEAD_SHED_STORES_NOTHING,
            stored.is_empty(),
            json!({
                "node": node,
                "signal": "metrics",
                "shed_writes": shed.len(),
                "stored": crate::first(&stored),
            }),
        );
    }

    /// One step of the drain rule: collect the answer to a pending request,
    /// then, if none is pending and the interval has passed, ask one node's
    /// supervisor to drain it. One drain at a time keeps a node serving.
    pub fn drain_step(&self, tc: &TestCase, nodes: &[String]) {
        let mut d = lock(&self.drains);
        self.collect_drain(&mut d);
        let interval = if self.self_check {
            SELF_CHECK_DRAIN_INTERVAL
        } else {
            DRAIN_INTERVAL
        };
        if d.pending.is_some() || d.last_request.is_some_and(|t| t.elapsed() < interval) {
            return;
        }
        let node = draw_node(tc, nodes.len());
        let Some(host) = host_of(&nodes[node]) else {
            println!("coverage: no host name in node URL {}", nodes[node]);
            return;
        };
        d.next_seq += 1;
        let seq = d.next_seq;
        let request = self.control.join(format!("drain-{host}"));
        if let Err(e) = self.put(&request, &format!("{seq}\n")) {
            println!("coverage: writing drain request {}: {e}", request.display());
            return;
        }
        println!("coverage: asked {host} to drain (seq {seq})");
        d.pending = Some(PendingDrain { host, seq });
        d.last_request = Some(Instant::now());
    }

    /// Write `path` through a temporary file and a rename, synced, so the
    /// supervisor never reads a partial request and a crash keeps it.
    fn put(&self, path: &std::path::Path, content: &str) -> std::io::Result<()> {
        use std::io::Write as _;
        let tmp = path.with_extension("tmp");
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(content.as_bytes())?;
        f.sync_all()?;
        std::fs::rename(&tmp, path)?;
        std::fs::File::open(&self.control)?.sync_all()
    }

    /// Judge the supervisor's answer to the pending drain, if it has come.
    fn collect_drain(&self, d: &mut Drains) {
        let Some(p) = &d.pending else {
            return;
        };
        let result = self.control.join(format!("drained-{}", p.host));
        let text = match std::fs::read_to_string(&result) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
            Err(e) => {
                println!("coverage: reading {}: {e}", result.display());
                return;
            }
        };
        let _ = std::fs::remove_file(&result);
        let answer = parse_drain_result(&text);
        if answer.seq != Some(p.seq) {
            println!(
                "coverage: ignoring a stale drain answer from {}: {}",
                p.host,
                text.trim()
            );
            return;
        }
        let details = json!({ "host": p.host, "seq": p.seq, "answer": text.trim() });
        check(&DRAIN_ANSWERED, true, details.clone());
        match answer.code {
            Some(SIGTERM_BEFORE_HANDLER) => println!(
                "coverage: drain {} of {} exited 143: SIGTERM arrived before ravel-server \
                 installed its shutdown handler (#4493)",
                p.seq, p.host
            ),
            Some(code) => {
                println!("coverage: drain {} of {} exited {code}", p.seq, p.host);
                check(&DRAIN_EXIT, code == 0 || code == 1, details.clone());
                check(&DRAIN_CLEAN, code == 0, details);
            }
            None => println!("coverage: drain {} of {} was interrupted", p.seq, p.host),
        }
        d.pending = None;
    }

    /// Settle the pending drain once faults have stopped, before the
    /// eventually's checks: withdraw a request the supervisor has not taken,
    /// or wait for the answer to one it has. A drain the supervisor took and
    /// never answered, not even as interrupted after a restart, fails
    /// "a driver-requested drain is answered or reported interrupted".
    pub fn settle_drains(&self) {
        let mut d = lock(&self.drains);
        let deadline = Instant::now() + DRAIN_SETTLE;
        loop {
            self.collect_drain(&mut d);
            let Some(p) = &d.pending else {
                return;
            };
            let request = self.control.join(format!("drain-{}", p.host));
            if std::fs::remove_file(&request).is_ok() {
                println!(
                    "coverage: withdrew drain {} of {} before it was taken",
                    p.seq, p.host
                );
                d.pending = None;
                return;
            }
            if Instant::now() >= deadline {
                println!(
                    "coverage: drain {} of {} unanswered {:?} after faults stopped",
                    p.seq, p.host, DRAIN_SETTLE
                );
                check(
                    &DRAIN_ANSWERED,
                    false,
                    json!({ "host": p.host, "seq": p.seq, "waited_s": DRAIN_SETTLE.as_secs() }),
                );
                d.pending = None;
                return;
            }
            std::thread::sleep(Duration::from_secs(2));
        }
    }
}

struct DrainResult {
    seq: Option<u64>,
    /// The server's exit status, or `None` for an interrupted drain.
    code: Option<i32>,
}

/// Parse the supervisor's answer, `seq=<n> code=<status>` or
/// `seq=<n> interrupted`.
fn parse_drain_result(text: &str) -> DrainResult {
    let mut r = DrainResult {
        seq: None,
        code: None,
    };
    for field in text.split_whitespace() {
        if let Some(v) = field.strip_prefix("seq=") {
            r.seq = v.parse().ok();
        } else if let Some(v) = field.strip_prefix("code=") {
            r.code = v.parse().ok();
        }
    }
    r
}

/// The host part of a node URL such as `http://ravel1:4318`, which is the
/// node container's hostname.
fn host_of(url: &str) -> Option<String> {
    let rest = url.split_once("://").map_or(url, |(_, r)| r);
    let host = rest.split(['/', ':']).next()?;
    (!host.is_empty()).then(|| host.to_string())
}

fn kv(key: &str, value: String) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value)),
        }),
        ..Default::default()
    }
}

/// A log export of write `write_id`: its `records` good records as the base
/// variant sends them, plus `poison` records with indexes from `records` up.
/// The model holds only the good records, so a poison record that is ever
/// stored reads back as a phantom.
pub fn logs_request(write_id: u64, records: u32, streams: u32, ts_ns: u64, poison: u32) -> Vec<u8> {
    let mut body = otlp::logs_request(write_id, records, streams, ts_ns);
    if poison == 0 {
        return body;
    }
    let log_records = (records..records + poison)
        .map(|rec| LogRecord {
            time_unix_nano: ts_ns,
            observed_time_unix_nano: ts_ns,
            severity_number: 9,
            severity_text: "INFO".to_string(),
            body: Some(AnyValue {
                value: Some(any_value::Value::StringValue("p".repeat(POISON_BODY_BYTES))),
            }),
            attributes: vec![
                kv(otlp::LOG_WRITE_ATTR, write_id.to_string()),
                kv(otlp::LOG_REC_ATTR, rec.to_string()),
            ],
            ..Default::default()
        })
        .collect();
    let extra = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![kv("service.name", "wl-svc-poison".to_string())],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records,
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    // Concatenated protobuf messages decode as one message whose repeated
    // fields hold both parts: here, one more ResourceLogs entry.
    body.extend(extra.encode_to_vec());
    body
}

/// A metrics export of write `w`: its samples as the base variant sends them,
/// plus `poison` points whose value index lies past the write's samples, so a
/// poison point that is ever stored reads back as matching no sent sample.
pub fn metrics_request(w: &Write, poison: u32) -> Vec<u8> {
    let mut body = otlp::metrics_request(&w.samples);
    if poison == 0 {
        return body;
    }
    let ts_ns = w.samples.first().map_or(w.ts_ns, |s| s.ts_ns);
    let data_points = (0..u64::from(poison))
        .map(|i| NumberDataPoint {
            attributes: vec![
                kv(otlp::METRIC_SERIES_LABEL, "s0".to_string()),
                kv(POISON_LABEL, "p".repeat(POISON_LABEL_BYTES)),
            ],
            time_unix_nano: ts_ns,
            value: Some(number_data_point::Value::AsDouble(
                (w.id * VALUE_STRIDE + VALUE_STRIDE - 1 - i) as f64,
            )),
            ..Default::default()
        })
        .collect();
    let extra = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![kv("service.name", "wl-metrics".to_string())],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: otlp::METRIC_NAME.to_string(),
                    data: Some(metric::Data::Gauge(Gauge { data_points })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    body.extend(extra.encode_to_vec());
    body
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn host_of_takes_the_host_from_a_node_url() {
        assert_eq!(host_of("http://ravel1:4318").as_deref(), Some("ravel1"));
        assert_eq!(host_of("http://ravel2:4318/").as_deref(), Some("ravel2"));
        assert_eq!(host_of("ravel1").as_deref(), Some("ravel1"));
        assert_eq!(host_of("http://:4318"), None);
    }

    #[test]
    fn drain_results_parse() {
        let r = parse_drain_result("seq=3 code=0\n");
        assert_eq!((r.seq, r.code), (Some(3), Some(0)));
        let r = parse_drain_result("seq=4 interrupted\n");
        assert_eq!((r.seq, r.code), (Some(4), None));
        let r = parse_drain_result("seq= interrupted\n");
        assert_eq!((r.seq, r.code), (None, None));
    }

    use opentelemetry_proto::tonic::collector::logs::v1::{
        ExportLogsPartialSuccess, ExportLogsServiceResponse,
    };
    use ravel_antithesis_driver::model;

    fn write(id: u64, signal: Signal, mode: Mode, records: u32, attempts: Vec<Attempt>) -> Write {
        Write {
            id,
            signal,
            mode,
            key: None,
            records,
            samples: Vec::new(),
            ts_ns: 1_000,
            attempts,
        }
    }

    fn str_attr(attrs: &[KeyValue], key: &str) -> Option<String> {
        attrs
            .iter()
            .find(|kv| kv.key == key)
            .and_then(|kv| kv.value.as_ref())
            .and_then(|v| match &v.value {
                Some(any_value::Value::StringValue(s)) => Some(s.clone()),
                _ => None,
            })
    }

    /// The rows a store that kept every record of `body` would return to the
    /// driver's logs count query, `(write, record) -> count`.
    fn log_rows_if_all_stored(body: &[u8]) -> HashMap<(u64, u32), u64> {
        let req = ExportLogsServiceRequest::decode(body).expect("decodes");
        let mut counts = HashMap::new();
        for r in req
            .resource_logs
            .iter()
            .flat_map(|rl| rl.scope_logs.iter())
            .flat_map(|sl| sl.log_records.iter())
        {
            let w = str_attr(&r.attributes, otlp::LOG_WRITE_ATTR).expect("write id");
            let rec = str_attr(&r.attributes, otlp::LOG_REC_ATTR).expect("record index");
            *counts
                .entry((w.parse().expect("u64"), rec.parse().expect("u32")))
                .or_insert(0) += 1;
        }
        counts
    }

    /// The rows a store that kept every point of `body` would return to the
    /// driver's samples query, `(ts, value, series)`.
    fn metric_rows_if_all_stored(body: &[u8]) -> Vec<(i64, f64, String)> {
        let req = ExportMetricsServiceRequest::decode(body).expect("decodes");
        req.resource_metrics
            .iter()
            .flat_map(|rm| rm.scope_metrics.iter())
            .flat_map(|sm| sm.metrics.iter())
            .filter_map(|m| match &m.data {
                Some(metric::Data::Gauge(g)) => Some(g.data_points.iter()),
                _ => None,
            })
            .flatten()
            .map(|p| {
                let v = match p.value {
                    Some(number_data_point::Value::AsDouble(v)) => v,
                    _ => f64::NAN,
                };
                let series = str_attr(&p.attributes, otlp::METRIC_SERIES_LABEL).expect("series");
                (i64::try_from(p.time_unix_nano).expect("ts"), v, series)
            })
            .collect()
    }

    #[test]
    fn a_stored_poison_log_record_is_judged_a_phantom() {
        let mut m = Model::default();
        m.writes
            .insert(7, write(7, Signal::Logs, Mode::Strict, 3, Vec::new()));
        let counts = log_rows_if_all_stored(&logs_request(7, 3, 2, 1_000, 2));
        assert_eq!(counts.len(), 5);
        let j = model::judge_logs(&m, &counts, &[7]);
        let mut phantoms = j.phantoms.clone();
        phantoms.sort();
        assert_eq!(
            phantoms,
            vec![
                "write 7 rec 3 x1: write has 3 records".to_string(),
                "write 7 rec 4 x1: write has 3 records".to_string(),
            ]
        );
        assert!(j.missing.is_empty());
    }

    #[test]
    fn a_stored_poison_point_is_judged_foreign() {
        let mut m = Model::default();
        let mut w = write(5, Signal::Metrics, Mode::Strict, 0, Vec::new());
        w.samples = vec![otlp::Sample {
            series: 1,
            ts_ns: 2_000,
            value: 500.0,
        }];
        m.writes.insert(5, w.clone());
        let rows = metric_rows_if_all_stored(&metrics_request(&w, 1));
        assert_eq!(rows.len(), 2);
        let j = model::judge_metrics(&m, &rows, &[5]);
        assert_eq!(
            j.foreign,
            vec![format!(
                "sample ts=2000 value=599 {}=s0: matches no sent sample",
                otlp::METRIC_SERIES_LABEL
            )]
        );
        assert!(j.missing.is_empty());
    }

    fn logs_reply(rejected: Option<i64>, token: Option<&str>) -> Reply {
        let body = ExportLogsServiceResponse {
            partial_success: rejected.map(|n| ExportLogsPartialSuccess {
                rejected_log_records: n,
                error_message: "too long".to_string(),
            }),
        }
        .encode_to_vec();
        Reply::Status {
            status: 200,
            commit_token: token.map(str::to_string),
            body,
        }
    }

    #[test]
    fn reclassify_acks_only_a_rejection_of_exactly_the_poison_with_a_token() {
        let cov = Coverage::new(std::env::temp_dir(), false);
        let w = write(9, Signal::Logs, Mode::Strict, 3, Vec::new());
        cov.set_poison(9, 2);
        let acked = Attempt::Acked {
            header: "t1,t2".to_string(),
        };
        assert_eq!(
            cov.reclassify(&w, &logs_reply(Some(2), Some("t1,t2")), Attempt::Partial),
            acked
        );
        // More rejected than the poison: some good record may be lost too.
        assert_eq!(
            cov.reclassify(&w, &logs_reply(Some(3), Some("t1,t2")), Attempt::Partial),
            Attempt::Partial
        );
        // No token, or an empty one, is no acknowledgement.
        assert_eq!(
            cov.reclassify(&w, &logs_reply(Some(2), None), Attempt::Partial),
            Attempt::Partial
        );
        assert_eq!(
            cov.reclassify(&w, &logs_reply(Some(2), Some("")), Attempt::Partial),
            Attempt::Partial
        );
        // A write with no poison, and a buffered one, keep the base class.
        let clean = write(10, Signal::Logs, Mode::Strict, 3, Vec::new());
        assert_eq!(
            cov.reclassify(&clean, &logs_reply(Some(0), Some("t1")), Attempt::Partial),
            Attempt::Partial
        );
        let buffered = write(11, Signal::Logs, Mode::Buffered, 3, Vec::new());
        cov.set_poison(11, 2);
        assert_eq!(
            cov.reclassify(
                &buffered,
                &logs_reply(Some(2), Some("t1")),
                Attempt::Partial
            ),
            Attempt::Partial
        );
        let error = Reply::Status {
            status: 503,
            commit_token: Some("t1".to_string()),
            body: Vec::new(),
        };
        assert_eq!(
            cov.reclassify(&w, &error, Attempt::Refused { status: 503 }),
            Attempt::Refused { status: 503 }
        );
    }

    #[test]
    fn fully_shed_holds_only_writes_whose_every_attempt_was_shed() {
        let cov = Coverage::new(std::env::temp_dir(), false);
        let shed = Attempt::Refused { status: 429 };
        let mut m = Model::default();
        // Shed once, one attempt.
        m.writes.insert(
            1,
            write(1, Signal::Logs, Mode::Strict, 1, vec![shed.clone()]),
        );
        // Shed once, then retried: the retry may have stored it.
        m.writes.insert(
            2,
            write(
                2,
                Signal::Logs,
                Mode::Strict,
                1,
                vec![shed.clone(), Attempt::InFlight],
            ),
        );
        // Shed twice, two attempts.
        m.writes.insert(
            3,
            write(
                3,
                Signal::Logs,
                Mode::Strict,
                1,
                vec![shed.clone(), shed.clone()],
            ),
        );
        // A metrics write shed once.
        m.writes.insert(
            4,
            write(4, Signal::Metrics, Mode::Strict, 0, vec![shed.clone()]),
        );
        // Acked, never shed.
        m.writes.insert(
            7,
            write(
                7,
                Signal::Logs,
                Mode::Strict,
                1,
                vec![Attempt::Acked {
                    header: "t".to_string(),
                }],
            ),
        );
        {
            let mut h = lock(&cov.head_shed);
            h.insert(1, 1);
            h.insert(2, 1);
            h.insert(3, 2);
            h.insert(4, 1);
        }
        let logs = cov.fully_shed(&m, Signal::Logs);
        assert_eq!(logs, HashSet::from([1, 3]));
        assert_eq!(cov.fully_shed(&m, Signal::Metrics), HashSet::from([4]));
        // The self-check adds acked writes whose id is a multiple of 7.
        let broken = Coverage::new(std::env::temp_dir(), true);
        *lock(&broken.head_shed) = lock(&cov.head_shed).clone();
        assert_eq!(
            broken.fully_shed(&m, Signal::Logs),
            HashSet::from([1, 3, 7])
        );
    }

    #[test]
    fn settle_withdraws_a_request_the_supervisor_has_not_taken() {
        let dir = std::env::temp_dir().join(format!("a2cov-settle-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let cov = Coverage::new(dir.clone(), false);
        cov.put(&dir.join("drain-ravel1"), "1\n").expect("request");
        lock(&cov.drains).pending = Some(PendingDrain {
            host: "ravel1".to_string(),
            seq: 1,
        });
        cov.settle_drains();
        assert!(lock(&cov.drains).pending.is_none());
        assert!(!dir.join("drain-ravel1").exists());
        // An answer to the pending drain clears it too.
        cov.put(&dir.join("drained-ravel1"), "seq=2 code=0\n")
            .expect("answer");
        lock(&cov.drains).pending = Some(PendingDrain {
            host: "ravel1".to_string(),
            seq: 2,
        });
        cov.settle_drains();
        assert!(lock(&cov.drains).pending.is_none());
        assert!(!dir.join("drained-ravel1").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn switch_off_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("a2cov-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp dir");
        dir
    }

    /// A stand-in supervisor for `host`, alive from `up_at` on: answers each
    /// drain request it finds with `code=0`, until `stop` is set.
    fn answering_supervisor(
        dir: PathBuf,
        host: &'static str,
        up_at: Instant,
        stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            let request = dir.join(format!("drain-{host}"));
            while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                if Instant::now() < up_at {
                    std::thread::sleep(Duration::from_millis(20));
                    continue;
                }
                if let Ok(seq) = std::fs::read_to_string(&request) {
                    let _ = std::fs::remove_file(&request);
                    let _ = std::fs::write(
                        dir.join(format!("drained-{host}")),
                        format!("seq={} code=0\n", seq.trim()),
                    );
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        })
    }

    const TWO_NODES: [&str; 2] = ["http://ravel1:4318", "http://ravel2:4318"];

    /// A node that is still down when the switch-off starts is waited for,
    /// not counted as failed: ravel2 (container and supervisor) comes up
    /// only after the 1 s answer wait would have run out, and both nodes
    /// then restart. Requesting its drain before it is ready fails here.
    #[test]
    fn switch_off_waits_for_a_node_that_is_not_ready_yet() {
        let dir = switch_off_dir("switch-wait");
        let cov = Coverage::new(dir.clone(), false);
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let start = Instant::now();
        let ravel2_up = start + Duration::from_secs(2);
        let sups = [
            answering_supervisor(dir.clone(), "ravel1", start, stop.clone()),
            answering_supervisor(dir.clone(), "ravel2", ravel2_up, stop.clone()),
        ];
        let ready = |node: usize| node == 0 || Instant::now() >= ravel2_up;
        let nodes: Vec<String> = TWO_NODES.iter().map(|s| s.to_string()).collect();
        let (restarted, failed) = cov.switch_faults_off_within(
            &nodes,
            &ready,
            Duration::from_secs(10),
            Duration::from_secs(1),
        );
        stop.store(true, std::sync::atomic::Ordering::SeqCst);
        for s in sups {
            let _ = s.join();
        }
        assert!(dir.join("faults-off").exists());
        assert_eq!(failed, Vec::<String>::new());
        assert_eq!(restarted.len(), 2, "{restarted:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A node that never becomes ready fails the switch-off without a drain
    /// request left behind for it; a ready node that never answers fails
    /// with its request withdrawn.
    #[test]
    fn switch_off_reports_a_node_never_ready_and_one_never_answering() {
        let dir = switch_off_dir("switch-fail");
        let cov = Coverage::new(dir.clone(), false);
        let nodes: Vec<String> = TWO_NODES.iter().map(|s| s.to_string()).collect();
        let ready = |node: usize| node == 1;
        let (restarted, failed) = cov.switch_faults_off_within(
            &nodes,
            &ready,
            Duration::from_millis(600),
            Duration::from_millis(600),
        );
        assert!(restarted.is_empty(), "{restarted:?}");
        assert_eq!(failed.len(), 2, "{failed:?}");
        assert!(failed[0].starts_with("ravel1: not ready"), "{failed:?}");
        assert!(failed[1].starts_with("ravel2: no answer"), "{failed:?}");
        assert!(!dir.join("drain-ravel1").exists());
        assert!(!dir.join("drain-ravel2").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
