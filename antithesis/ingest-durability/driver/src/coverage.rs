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
/// How long a drain request may go unanswered before the driver drops it:
/// above ravel-server's 25 s shutdown timeout plus a restart. A container
/// killed during its drain never answers.
const DRAIN_GIVE_UP: Duration = Duration::from_secs(180);

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
const METRICS_429: Prop = Prop {
    kind: Kind::Reachable,
    message: "a metrics write was answered 429",
};

const PROPS: [&Prop; 9] = [
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
    at: Instant,
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
        println!("coverage variant: on, control directory {control}");
        Some(Coverage {
            control: PathBuf::from(control),
            head_shed: Mutex::new(HashMap::new()),
            poison: Mutex::new(HashMap::new()),
            drains: Mutex::new(Drains::default()),
        })
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
            if !ok {
                check(&ERROR_HAS_NO_TOKEN, commit_token.is_none(), details.clone());
            }
            if *status == 429 {
                if !logs {
                    check(&METRICS_429, true, details.clone());
                }
                let text = String::from_utf8_lossy(body);
                if HEAD_SHED_BODIES.contains(&text.trim()) {
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
                    rejected.is_some_and(|n| n >= i64::from(poison)),
                    details.clone(),
                );
                if rejected == Some(i64::from(poison)) {
                    check(
                        &POISON_ONLY_HAS_TOKEN,
                        commit_token.as_deref().is_some_and(|t| !t.is_empty()),
                        details,
                    );
                }
            }
        }
        if poison > 0 && matches!(attempt, Attempt::Acked { .. }) {
            check(&POISON_ACKED, true, json!({ "write": w.id, "poison": poison }));
        }
    }

    /// A token read of an acknowledged write came back; the presence
    /// assertions beside the call judge its rows.
    pub fn note_token_read(&self, w: &Write) {
        let poison = self.poison(w.id);
        if poison > 0 {
            check(&POISON_READ, true, json!({ "write": w.id, "poison": poison }));
        }
    }

    /// Writes of `signal` whose every attempt so far was shed at the request
    /// head. Call with the model locked.
    fn fully_shed(&self, m: &Model, signal: Signal) -> HashSet<u64> {
        lock(&self.head_shed)
            .iter()
            .filter(|(id, shed)| {
                m.writes
                    .get(id)
                    .is_some_and(|w| w.signal == signal && w.attempts.len() == **shed)
            })
            .map(|(id, _)| *id)
            .collect()
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
        if d.pending.is_some()
            || d
                .last_request
                .is_some_and(|t| t.elapsed() < DRAIN_INTERVAL)
        {
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
        let tmp = self.control.join(format!(".drain-{host}.tmp"));
        let written = std::fs::write(&tmp, format!("{seq}\n"))
            .and_then(|()| std::fs::rename(&tmp, &request));
        if let Err(e) = written {
            println!("coverage: writing drain request {}: {e}", request.display());
            return;
        }
        println!("coverage: asked {host} to drain (seq {seq})");
        d.pending = Some(PendingDrain {
            host,
            seq,
            at: Instant::now(),
        });
        d.last_request = Some(Instant::now());
    }

    fn collect_drain(&self, d: &mut Drains) {
        let Some(p) = &d.pending else {
            return;
        };
        let result = self.control.join(format!("drained-{}", p.host));
        let text = match std::fs::read_to_string(&result) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                if p.at.elapsed() > DRAIN_GIVE_UP {
                    println!(
                        "coverage: drain {} of {} unanswered after {:?}, dropped",
                        p.seq, p.host, DRAIN_GIVE_UP
                    );
                    let _ = std::fs::remove_file(self.control.join(format!("drain-{}", p.host)));
                    d.pending = None;
                }
                return;
            }
            Err(e) => {
                println!("coverage: reading {}: {e}", result.display());
                return;
            }
        };
        let _ = std::fs::remove_file(&result);
        let answer = parse_drain_result(&text);
        if answer.seq != Some(p.seq) {
            println!("coverage: ignoring a stale drain answer from {}: {}", p.host, text.trim());
            return;
        }
        let details = json!({ "host": p.host, "seq": p.seq, "answer": text.trim() });
        match answer.code {
            Some(code) => {
                println!("coverage: drain {} of {} exited {code}", p.seq, p.host);
                check(&DRAIN_EXIT, code == 0 || code == 1, details.clone());
                check(&DRAIN_CLEAN, code == 0, details);
            }
            None => println!("coverage: drain {} of {} was interrupted", p.seq, p.host),
        }
        d.pending = None;
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

    #[test]
    fn poison_log_records_follow_the_good_ones_in_one_request() {
        let body = logs_request(7, 3, 2, 1_000, 2);
        let req = ExportLogsServiceRequest::decode(body.as_slice()).expect("decodes");
        let mut recs: Vec<(u32, usize)> = req
            .resource_logs
            .iter()
            .flat_map(|rl| rl.scope_logs.iter())
            .flat_map(|sl| sl.log_records.iter())
            .map(|r| {
                let rec = r
                    .attributes
                    .iter()
                    .find(|kv| kv.key == otlp::LOG_REC_ATTR)
                    .and_then(|kv| kv.value.as_ref())
                    .and_then(|v| match &v.value {
                        Some(any_value::Value::StringValue(s)) => s.parse().ok(),
                        _ => None,
                    })
                    .expect("record index");
                let len = match r.body.as_ref().and_then(|b| b.value.as_ref()) {
                    Some(any_value::Value::StringValue(s)) => s.len(),
                    _ => 0,
                };
                (rec, len)
            })
            .collect();
        recs.sort_unstable();
        assert_eq!(recs.len(), 5);
        assert!(recs[..3].iter().all(|(_, len)| *len < 1_000));
        assert_eq!(recs[3], (3, POISON_BODY_BYTES));
        assert_eq!(recs[4], (4, POISON_BODY_BYTES));
    }

    #[test]
    fn a_poison_point_is_attributed_to_no_sample() {
        let w = Write {
            id: 5,
            signal: Signal::Metrics,
            mode: Mode::Strict,
            key: None,
            records: 0,
            samples: vec![otlp::Sample {
                series: 1,
                ts_ns: 2_000,
                value: 500.0,
            }],
            ts_ns: 2_000,
            attempts: Vec::new(),
        };
        let body = metrics_request(&w, 1);
        let req = ExportMetricsServiceRequest::decode(body.as_slice()).expect("decodes");
        let values: Vec<f64> = req
            .resource_metrics
            .iter()
            .flat_map(|rm| rm.scope_metrics.iter())
            .flat_map(|sm| sm.metrics.iter())
            .filter_map(|m| match &m.data {
                Some(metric::Data::Gauge(g)) => Some(g.data_points.iter()),
                _ => None,
            })
            .flatten()
            .filter_map(|p| match p.value {
                Some(number_data_point::Value::AsDouble(v)) => Some(v),
                _ => None,
            })
            .collect();
        assert_eq!(values, vec![500.0, 599.0]);
    }
}
