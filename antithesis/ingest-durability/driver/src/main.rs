//! Antithesis driver for Ravel's ingest durability contract
//! (docs/consistency-model.md): a strictly acknowledged write is never lost
//! and is readable with its commit tokens; no stored record is a phantom or
//! stored more often than the attempts that could have written it; and once
//! faults stop, every acknowledged write is readable and visible.
//!
//! The driver is a hegel concurrent state machine. Each rule sends one write
//! or one query to a ravel-server node and records the outcome in the model
//! (`model.rs`). See antithesis/ingest-durability/README.md for the properties.
//! `WORKLOAD_COVERAGE=1` turns on the coverage variant (`coverage.rs`).

mod coverage;

use ravel_antithesis_driver::{model, otlp, ravel};

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use antithesis_sdk::{assert_always, assert_reachable, assert_sometimes, assert_unreachable};
use hegel::TestCase;
use hegel::generators as gs;
use hegel::stateful::machine;
use serde_json::json;

use antithesis_instrumentation as _;

use coverage::Coverage;
use model::{Attempt, Mode, Model, Signal, Write};
use otlp::{METRIC_SERIES, PartialSuccess, Sample, VALUE_STRIDE};
use ravel::{Ravel, Reply, SqlFailure};

/// Retries after the first attempt of a write with no definite outcome.
const MAX_RETRIES: u32 = 2;
/// Client timeout for a write: above the server's 10 s strict ack deadline,
/// so the server's own `AckTimeout` answer is observable.
const WRITE_TIMEOUT: Duration = Duration::from_secs(15);
/// A short client timeout some writes draw, which abandons the request while
/// the server is still working on it.
const SHORT_WRITE_TIMEOUT: Duration = Duration::from_secs(2);
const READ_TIMEOUT: Duration = Duration::from_secs(10);
/// Query windows reach this far past the writes' event times on each side.
const WINDOW_SLACK_NS: u64 = 600 * 1_000_000_000;
/// Writes verified by one token read in the eventually.
const TOKEN_BATCH: usize = 20;

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

struct Config {
    nodes: Vec<String>,
    token: String,
    eventually_on: bool,
    eventually_after: Option<u64>,
    max_writes: usize,
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
            token: std::env::var("RAVEL_TOKEN").unwrap_or_else(|_| "wl-token".to_string()),
            eventually_on: std::env::var("WORKLOAD_EVENTUALLY").map_or(true, |v| v != "off"),
            eventually_after: std::env::var("WORKLOAD_EVENTUALLY_AFTER_SECS")
                .ok()
                .and_then(|v| v.parse().ok()),
            max_writes: std::env::var("WORKLOAD_MAX_WRITES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(5000),
        }
    }
}

struct Shared {
    ravel: Ravel,
    model: Mutex<Model>,
    /// Every rule holds a read guard while it acts; the eventually takes the
    /// write guard so it checks a quiet system.
    gate: RwLock<()>,
    stopping: AtomicBool,
    eventually_lock: Mutex<()>,
    eventually_on: bool,
    max_writes: usize,
    /// `WORKLOAD_INJECT_MISSING_RECORD=1`: the local loop's self-check.
    inject_missing: bool,
    /// The coverage variant's state; `None` in the base variant.
    cov: Option<Coverage>,
}

struct Workload(Arc<Shared>);

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn draw_node(tc: &TestCase, n: usize) -> usize {
    let max = u64::try_from(n.saturating_sub(1)).unwrap_or(0);
    tc.draw(gs::integers::<u64>().min_value(0).max_value(max)) as usize
}

fn draw_mode(tc: &TestCase) -> Mode {
    match tc.draw(gs::integers::<u32>().min_value(0).max_value(9)) {
        0..=5 => Mode::Strict,
        6..=7 => Mode::StrictArrival,
        _ => Mode::Buffered,
    }
}

fn window_s(min_ns: u64, max_ns: u64) -> (f64, f64) {
    let start = min_ns.saturating_sub(WINDOW_SLACK_NS) as f64 / 1e9;
    let end = max_ns.saturating_add(WINDOW_SLACK_NS) as f64 / 1e9;
    (start, end)
}

fn ids_list(ids: &[u64]) -> String {
    ids.iter()
        .map(|i| format!("'{i}'"))
        .collect::<Vec<_>>()
        .join(",")
}

fn logs_query(ids: Option<&[u64]>) -> String {
    let filter = match ids {
        Some(ids) => format!("attrs['wl.write'] IN ({})", ids_list(ids)),
        None => "attrs['wl.write'] IS NOT NULL".to_string(),
    };
    format!(
        "SELECT attrs['wl.write'] AS w, attrs['wl.rec'] AS r, count(*) AS n FROM logs \
         WHERE {filter} GROUP BY attrs['wl.write'], attrs['wl.rec']"
    )
}

fn metrics_query(value_range: Option<(u64, u64)>) -> String {
    let filter = match value_range {
        Some((lo, hi)) => format!(" AND value >= {lo} AND value < {hi}"),
        None => String::new(),
    };
    format!(
        "SELECT ts, value, label(labels, 'wl_series') AS s FROM samples \
         WHERE label(labels, '__name__') = 'wl_gauge'{filter}"
    )
}

/// The client's view of one write attempt.
fn classify(signal: Signal, mode: Mode, reply: &Reply) -> Attempt {
    match reply {
        Reply::NoResponse(_) => Attempt::NoResponse,
        Reply::Status {
            status,
            commit_token,
            body,
        } if (200..300).contains(status) => {
            let partial = otlp::partial_success(signal == Signal::Logs, body);
            match (mode, partial, commit_token) {
                (Mode::Buffered, PartialSuccess::None, _) => Attempt::BufferedAck,
                (Mode::Buffered, _, _) => Attempt::Partial,
                (_, PartialSuccess::None, Some(h)) if !h.is_empty() => {
                    Attempt::Acked { header: h.clone() }
                }
                _ => Attempt::Partial,
            }
        }
        Reply::Status { status, .. } => Attempt::Refused { status: *status },
    }
}

impl Shared {
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

    /// Register a write before sending it.
    fn register(
        &self,
        signal: Signal,
        mode: Mode,
        records: u32,
        samples: u32,
        series_offset: u64,
        keyed: bool,
    ) -> Option<Write> {
        let mut m = lock(&self.model);
        if m.writes.len() >= self.max_writes {
            return None;
        }
        let id = m.next_id;
        m.next_id += 1;
        let ts_ns = now_ns();
        let ms = ts_ns / 1_000_000 * 1_000_000;
        let mut sample_list = Vec::new();
        for i in 0..u64::from(samples) {
            let seq = m.next_sample;
            m.next_sample += 1;
            sample_list.push(Sample {
                series: (series_offset + i) % METRIC_SERIES,
                ts_ns: ms + seq % 1_000_000,
                value: (id * VALUE_STRIDE + i) as f64,
            });
        }
        let w = Write {
            id,
            signal,
            mode,
            key: keyed.then(|| format!("wl-key-{id}")),
            records,
            samples: sample_list,
            ts_ns,
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

    /// Assertions on one write response, whatever the write was.
    fn observe_write(&self, w: &Write, node: usize, reply: &Reply, attempt: &Attempt) {
        let logs = w.signal == Signal::Logs;
        match reply {
            Reply::NoResponse(err) => {
                if logs {
                    assert_reachable!(
                        "a log write got no response",
                        &json!({ "write": w.id, "node": node, "error": err })
                    );
                } else {
                    assert_reachable!(
                        "a metrics write got no response",
                        &json!({ "write": w.id, "node": node, "error": err })
                    );
                }
            }
            Reply::Status {
                status,
                commit_token,
                body,
            } => {
                let details = json!({
                    "write": w.id,
                    "signal": format!("{:?}", w.signal),
                    "mode": format!("{:?}", w.mode),
                    "node": node,
                    "status": status,
                    "commit_token": commit_token,
                    "body": String::from_utf8_lossy(&body[..body.len().min(400)]),
                });
                let ok = (200..300).contains(status);
                if ok && w.mode != Mode::Buffered {
                    if otlp::partial_success(logs, body) == PartialSuccess::None {
                        assert_always!(
                            commit_token.as_deref().is_some_and(|t| !t.is_empty()),
                            "a clean strict 200 response carries a commit token",
                            &details
                        );
                    } else {
                        assert_reachable!(
                            "a strict write was answered with a partial success",
                            &details
                        );
                    }
                }
                if ok && w.mode == Mode::Buffered {
                    assert_always!(
                        commit_token.is_none(),
                        "a buffered-mode acknowledgement carries no commit token",
                        &details
                    );
                }
                if *status == 400 {
                    assert_unreachable!(
                        "a well-formed write was refused as non-retryable (HTTP 400)",
                        &details
                    );
                }
                match (logs, *status) {
                    (true, 503) => assert_reachable!("a log write was answered 503", &details),
                    (false, 503) => assert_reachable!("a metrics write was answered 503", &details),
                    (true, 429) => assert_reachable!("a log write was answered 429", &details),
                    (true, s) if (200..300).contains(&s) && w.mode == Mode::Buffered => {
                        assert_reachable!("a buffered log write was acknowledged", &details)
                    }
                    _ => {}
                }
                if !ok && *status != 503 && *status != 429 {
                    println!(
                        "write {} node {node}: unexpected status {status}: {details}",
                        w.id
                    );
                }
            }
        }
        if let Attempt::Acked { header } = attempt {
            if logs {
                assert_reachable!(
                    "a strict log write was acknowledged with commit tokens",
                    &json!({ "write": w.id })
                );
            } else {
                assert_reachable!(
                    "a strict metrics write was acknowledged with commit tokens",
                    &json!({ "write": w.id })
                );
            }
            assert_sometimes!(
                header.contains(','),
                "a strict write committed through more than one shard",
                &json!({ "write": w.id, "header": header })
            );
        }
    }

    /// Classify one write response and run every assertion on it, before its
    /// outcome is recorded in the model.
    fn judge_reply(&self, w: &Write, node: usize, reply: &Reply) -> Attempt {
        let mut attempt = classify(w.signal, w.mode, reply);
        if let Some(cov) = &self.cov {
            attempt = cov.reclassify(w, reply, attempt);
        }
        self.observe_write(w, node, reply, &attempt);
        if let Some(cov) = &self.cov {
            cov.observe(w, node, reply, &attempt);
        }
        attempt
    }

    /// Send a registered write, retrying an attempt with no definite outcome
    /// the way an OTLP exporter does, then sometimes replay an acknowledged
    /// keyed write with the same key.
    fn send(&self, tc: &TestCase, w: &Write, body: &[u8], gzip: bool) {
        let path = match w.signal {
            Signal::Logs => "/v1/logs",
            Signal::Metrics => "/v1/metrics",
        };
        let n = self.ravel.nodes.len();
        let mut first_indefinite = false;
        let mut acked_header: Option<String> = None;
        for attempt_no in 0..=MAX_RETRIES {
            let node = draw_node(tc, n);
            let timeout = if tc.draw(gs::weighted_booleans(0.15)) {
                SHORT_WRITE_TIMEOUT
            } else {
                WRITE_TIMEOUT
            };
            let k = self.begin_attempt(w.id);
            let reply = self.ravel.export(
                node,
                path,
                body,
                w.mode.header(),
                w.key.as_deref(),
                gzip,
                timeout,
            );
            let attempt = self.judge_reply(w, node, &reply);
            self.finish_attempt(w.id, k, attempt.clone());
            let retryable = match &attempt {
                Attempt::NoResponse => true,
                Attempt::Refused { status } => *status == 503 || *status == 429,
                _ => false,
            };
            if let Attempt::Acked { header } = &attempt {
                if first_indefinite {
                    assert_reachable!(
                        "a write with no definite outcome was acknowledged on retry",
                        &json!({ "write": w.id })
                    );
                }
                acked_header = Some(header.clone());
            }
            if !retryable {
                break;
            }
            if attempt_no == 0 {
                first_indefinite = true;
            }
            if attempt_no == MAX_RETRIES || !tc.draw(gs::weighted_booleans(0.6)) {
                break;
            }
        }
        let (Some(original), Some(key)) = (acked_header, w.key.as_deref()) else {
            return;
        };
        if !tc.draw(gs::weighted_booleans(0.3)) {
            return;
        }
        let node = draw_node(tc, n);
        let k = self.begin_attempt(w.id);
        let reply = self.ravel.export(
            node,
            path,
            body,
            w.mode.header(),
            Some(key),
            gzip,
            WRITE_TIMEOUT,
        );
        let attempt = self.judge_reply(w, node, &reply);
        self.finish_attempt(w.id, k, attempt.clone());
        if let Attempt::Acked { header } = &attempt {
            if *header == original {
                assert_reachable!(
                    "a keyed retry after an ack replayed the original commit token",
                    &json!({ "write": w.id })
                );
            } else {
                assert_reachable!(
                    "a keyed retry after an ack stored a new copy under a new commit token",
                    &json!({ "write": w.id, "original": original, "retry": header })
                );
            }
        }
    }

    fn token_read_logs(
        &self,
        node: usize,
        writes: &[Write],
    ) -> Result<model::LogJudgement, SqlFailure> {
        let ids: Vec<u64> = writes.iter().map(|w| w.id).collect();
        let tokens: Vec<String> = writes.iter().flat_map(|w| w.tokens("logs")).collect();
        let min = writes.iter().map(|w| w.ts_ns).min().unwrap_or(0);
        let max = writes.iter().map(|w| w.ts_ns).max().unwrap_or(0);
        let (start, end) = window_s(min, max);
        let value = self.ravel.sql(
            node,
            &logs_query(Some(&ids)),
            start,
            end,
            &tokens,
            READ_TIMEOUT,
        )?;
        let counts = model::log_counts(ravel::rows(&value)).map_err(SqlFailure::BadBody)?;
        let m = lock(&self.model);
        Ok(model::judge_logs(&m, &counts, &ids))
    }

    fn token_read_metrics(
        &self,
        node: usize,
        writes: &[Write],
    ) -> Result<model::MetricJudgement, SqlFailure> {
        let ids: Vec<u64> = writes.iter().map(|w| w.id).collect();
        let tokens: Vec<String> = writes.iter().flat_map(|w| w.tokens("samples")).collect();
        let min = writes.iter().map(|w| w.ts_ns).min().unwrap_or(0);
        let max = writes.iter().map(|w| w.ts_ns).max().unwrap_or(0);
        let (start, end) = window_s(min, max);
        let lo = ids.iter().min().copied().unwrap_or(0) * VALUE_STRIDE;
        let hi = (ids.iter().max().copied().unwrap_or(0) + 1) * VALUE_STRIDE;
        let value = self.ravel.sql(
            node,
            &metrics_query(Some((lo, hi))),
            start,
            end,
            &tokens,
            READ_TIMEOUT,
        )?;
        let rows = model::metric_rows(ravel::rows(&value)).map_err(SqlFailure::BadBody)?;
        let m = lock(&self.model);
        Ok(model::judge_metrics(&m, &rows, &ids))
    }

    fn scan_logs(&self, node: usize, expect: &[u64]) -> Result<model::LogJudgement, SqlFailure> {
        let bounds = lock(&self.model).ts_bounds();
        let Some((min, max)) = bounds else {
            return Ok(model::LogJudgement::default());
        };
        let (start, end) = window_s(min, max);
        let value = self
            .ravel
            .sql(node, &logs_query(None), start, end, &[], READ_TIMEOUT)?;
        let counts = model::log_counts(ravel::rows(&value)).map_err(SqlFailure::BadBody)?;
        let m = lock(&self.model);
        if let Some(cov) = &self.cov {
            cov.check_log_rows(&m, &counts, node);
        }
        Ok(model::judge_logs(&m, &counts, expect))
    }

    fn scan_metrics(
        &self,
        node: usize,
        expect: &[u64],
    ) -> Result<model::MetricJudgement, SqlFailure> {
        let bounds = lock(&self.model).ts_bounds();
        let Some((min, max)) = bounds else {
            return Ok(model::MetricJudgement::default());
        };
        let (start, end) = window_s(min, max);
        let value = self
            .ravel
            .sql(node, &metrics_query(None), start, end, &[], READ_TIMEOUT)?;
        let rows = model::metric_rows(ravel::rows(&value)).map_err(SqlFailure::BadBody)?;
        let m = lock(&self.model);
        if let Some(cov) = &self.cov {
            cov.check_metric_rows(&m, &rows, node);
        }
        Ok(model::judge_metrics(&m, &rows, expect))
    }

    /// The guarantees every log read checks, whatever it was for.
    fn check_log_rows(j: &model::LogJudgement, node: usize, how: &str) {
        assert_always!(
            j.phantoms.is_empty(),
            "every stored workload log record belongs to a write the workload sent",
            &json!({ "node": node, "read": how, "phantoms": first(&j.phantoms) })
        );
        assert_always!(
            j.over.is_empty(),
            "no log record is stored more often than the attempts that could have written it",
            &json!({ "node": node, "read": how, "over": first(&j.over) })
        );
        if j.replay_rows_checked {
            assert_always!(
                j.over_after_replay.is_empty(),
                "a keyed log retry that replays the original commit token stores no new copy",
                &json!({ "node": node, "read": how, "over": first(&j.over_after_replay) })
            );
        }
    }

    fn check_metric_rows(j: &model::MetricJudgement, node: usize, how: &str) {
        assert_always!(
            j.foreign.is_empty(),
            "every stored workload sample matches a sample the workload sent",
            &json!({ "node": node, "read": how, "foreign": first(&j.foreign) })
        );
    }

    /// Whether every node is ready and answers a query.
    fn all_nodes_serve(&self) -> Result<(), String> {
        let now = now_ns();
        let (start, end) = window_s(now, now);
        for node in 0..self.ravel.nodes.len() {
            self.ravel
                .ready(node)
                .map_err(|e| format!("node {node} not ready: {e}"))?;
            self.ravel
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
    }
}

fn first(lines: &[String]) -> serde_json::Value {
    json!({ "count": lines.len(), "first": lines.iter().take(5).collect::<Vec<_>>() })
}

/// The rules, shared by the base and the coverage machines.
impl Shared {
    fn write_logs(&self, tc: &TestCase) {
        let s = self;
        let Some(_guard) = s.enter() else {
            return;
        };
        let records = tc.draw(gs::integers::<u32>().min_value(1).max_value(8));
        let streams = tc.draw(gs::integers::<u32>().min_value(1).max_value(4));
        let mode = draw_mode(tc);
        let keyed = tc.draw(gs::weighted_booleans(0.5));
        let gzip = tc.draw(gs::weighted_booleans(0.3));
        let poison = s
            .cov
            .as_ref()
            .map_or(0, |c| c.draw_log_poison(tc, mode, keyed));
        let Some(w) = s.register(Signal::Logs, mode, records, 0, 0, keyed) else {
            return;
        };
        if let Some(cov) = &s.cov {
            cov.set_poison(w.id, poison);
        }
        // The local loop's self-check: the model expects one record more than
        // is sent, so the presence assertions must fail.
        if s.inject_missing
            && w.id % 50 == 3
            && let Some(m) = lock(&s.model).writes.get_mut(&w.id)
        {
            m.records += 1;
        }
        let body = if poison > 0 {
            coverage::logs_request(w.id, records, streams, w.ts_ns, poison)
        } else {
            otlp::logs_request(w.id, records, streams, w.ts_ns)
        };
        s.send(tc, &w, &body, gzip);
    }

    fn write_metrics(&self, tc: &TestCase) {
        let s = self;
        let Some(_guard) = s.enter() else {
            return;
        };
        let samples = tc.draw(gs::integers::<u32>().min_value(1).max_value(6));
        let offset = tc.draw(
            gs::integers::<u64>()
                .min_value(0)
                .max_value(METRIC_SERIES - 1),
        );
        let mode = draw_mode(tc);
        let gzip = tc.draw(gs::weighted_booleans(0.3));
        let poison = s.cov.as_ref().map_or(0, |c| c.draw_metric_poison(tc, mode));
        let Some(w) = s.register(Signal::Metrics, mode, 0, samples, offset, false) else {
            return;
        };
        let body = match &s.cov {
            Some(cov) if poison > 0 => {
                cov.set_poison(w.id, poison);
                coverage::metrics_request(&w, poison)
            }
            _ => otlp::metrics_request(&w.samples),
        };
        s.send(tc, &w, &body, gzip);
    }

    fn read_with_tokens(&self, tc: &TestCase) {
        let s = self;
        let Some(_guard) = s.enter() else {
            return;
        };
        let logs = tc.draw(gs::weighted_booleans(0.6));
        let w = {
            let m = lock(&s.model);
            let list = if logs {
                &m.acked_logs
            } else {
                &m.acked_metrics
            };
            if list.is_empty() {
                return;
            }
            let max = u64::try_from(list.len() - 1).unwrap_or(0);
            let idx = tc.draw(gs::integers::<u64>().min_value(0).max_value(max)) as usize;
            match list.get(idx).and_then(|id| m.writes.get(id)) {
                Some(w) => w.clone(),
                None => return,
            }
        };
        let node = draw_node(tc, s.ravel.nodes.len());
        let details = |e: &dyn std::fmt::Display| json!({ "write": w.id, "node": node, "error": e.to_string() });
        if logs {
            match s.token_read_logs(node, std::slice::from_ref(&w)) {
                Ok(j) => {
                    assert_always!(
                        j.missing.is_empty(),
                        "a read with an acked log write's commit tokens returns every record of that write",
                        &json!({ "write": w.id, "node": node, "missing": first(&j.missing), "attempts": format!("{:?}", w.attempts) })
                    );
                    Shared::check_log_rows(&j, node, "token");
                    if let Some(cov) = &s.cov {
                        cov.note_token_read(&w);
                    }
                }
                Err(SqlFailure::Status(503, e)) => {
                    assert_reachable!(
                        "a read with commit tokens was refused as unavailable",
                        &details(&e)
                    );
                }
                Err(e) => println!("token read of log write {} on node {node}: {e}", w.id),
            }
        } else {
            match s.token_read_metrics(node, std::slice::from_ref(&w)) {
                Ok(j) => {
                    assert_always!(
                        j.missing.is_empty(),
                        "a read with an acked metrics write's commit tokens returns every sample of that write",
                        &json!({ "write": w.id, "node": node, "missing": first(&j.missing), "attempts": format!("{:?}", w.attempts) })
                    );
                    Shared::check_metric_rows(&j, node, "token");
                    if let Some(cov) = &s.cov {
                        cov.note_token_read(&w);
                    }
                }
                Err(SqlFailure::Status(503, e)) => {
                    assert_reachable!(
                        "a read with commit tokens was refused as unavailable",
                        &details(&e)
                    );
                }
                Err(e) => println!("token read of metrics write {} on node {node}: {e}", w.id),
            }
        }
    }

    fn scan_without_token(&self, tc: &TestCase) {
        let s = self;
        let Some(_guard) = s.enter() else {
            return;
        };
        let node = draw_node(tc, s.ravel.nodes.len());
        if tc.draw(gs::weighted_booleans(0.6)) {
            match s.scan_logs(node, &[]) {
                Ok(j) => {
                    Shared::check_log_rows(&j, node, "scan");
                    assert_sometimes!(
                        j.saw_acked,
                        "a read without a token saw an acked write",
                        &json!({ "node": node })
                    );
                    assert_sometimes!(
                        j.saw_duplicate,
                        "a stored log record was found more than once",
                        &json!({ "node": node, "over": j.over.len() })
                    );
                    assert_sometimes!(
                        j.saw_unknown_stored,
                        "a write with no definite outcome was later found stored",
                        &json!({ "node": node })
                    );
                }
                Err(e) => println!("log scan on node {node}: {e}"),
            }
        } else {
            match s.scan_metrics(node, &[]) {
                Ok(j) => {
                    Shared::check_metric_rows(&j, node, "scan");
                    // An empty result set satisfies check_metric_rows, so a
                    // scan that never sees an acked sample has to show up.
                    assert_sometimes!(
                        j.saw_acked,
                        "a metrics read without a token saw an acked write",
                        &json!({ "node": node })
                    );
                }
                Err(e) => println!("metrics scan on node {node}: {e}"),
            }
        }
    }

    fn eventually(&self, tc: &TestCase) {
        let s = self;
        if !s.eventually_on || s.stopping.load(Ordering::SeqCst) {
            return;
        }
        // The coverage self-check runs its eventually only when forced, so
        // its broken drains always come first.
        if s.cov.as_ref().is_some_and(|c| c.self_check) {
            return;
        }
        if !tc.draw(gs::weighted_booleans(0.003)) {
            return;
        }
        run_eventually(s, "drawn");
    }

    /// The coverage variant's drain rule.
    fn drain(&self, tc: &TestCase) {
        let Some(cov) = &self.cov else {
            return;
        };
        let Some(_guard) = self.enter() else {
            return;
        };
        cov.drain_step(tc, &self.ravel.nodes);
    }
}

#[hegel::concurrent_state_machine]
impl Workload {
    #[rule(groups = ["main"], weight = 6)]
    fn write_logs(&self, tc: TestCase) {
        self.0.write_logs(&tc);
    }

    #[rule(groups = ["main"], weight = 3)]
    fn write_metrics(&self, tc: TestCase) {
        self.0.write_metrics(&tc);
    }

    #[rule(groups = ["main"], weight = 3)]
    fn read_with_tokens(&self, tc: TestCase) {
        self.0.read_with_tokens(&tc);
    }

    #[rule(groups = ["main"], weight = 1)]
    fn scan_without_token(&self, tc: TestCase) {
        self.0.scan_without_token(&tc);
    }

    #[rule(groups = ["main"])]
    fn eventually(&self, tc: TestCase) {
        self.0.eventually(&tc);
    }
}

/// The coverage variant's machine: the base rules, plus graceful drains.
struct CoverageWorkload(Arc<Shared>);

#[hegel::concurrent_state_machine]
impl CoverageWorkload {
    #[rule(groups = ["main"], weight = 6)]
    fn write_logs(&self, tc: TestCase) {
        self.0.write_logs(&tc);
    }

    #[rule(groups = ["main"], weight = 3)]
    fn write_metrics(&self, tc: TestCase) {
        self.0.write_metrics(&tc);
    }

    #[rule(groups = ["main"], weight = 3)]
    fn read_with_tokens(&self, tc: TestCase) {
        self.0.read_with_tokens(&tc);
    }

    #[rule(groups = ["main"], weight = 1)]
    fn scan_without_token(&self, tc: TestCase) {
        self.0.scan_without_token(&tc);
    }

    #[rule(groups = ["main"], weight = 1)]
    fn drain(&self, tc: TestCase) {
        self.0.drain(&tc);
    }

    #[rule(groups = ["main"])]
    fn eventually(&self, tc: TestCase) {
        self.0.eventually(&tc);
    }
}

/// Stop faults for good, wait for the system to recover, assert every
/// guarantee that must hold once faults stop, then end the timeline.
fn run_eventually(s: &Shared, reason: &str) -> ! {
    s.stopping.store(true, Ordering::SeqCst);
    let _only = lock(&s.eventually_lock);
    let _quiet = s.gate.write().unwrap_or_else(|e| e.into_inner());
    println!("eventually ({reason}): stopping faults");
    // The Antithesis platform sets ANTITHESIS_STOP_FAULTS in every container
    // of a test run (snouty docs:
    // /docs/product/writing_tests/controlling_faults/pause_faults/). The local
    // loop injects no faults and leaves it unset, so this is a no-op there.
    if let Ok(cmd) = std::env::var("ANTITHESIS_STOP_FAULTS") {
        match std::process::Command::new(&cmd).arg("3600").status() {
            Ok(st) => println!("eventually: {cmd} 3600 exited {st}"),
            Err(e) => println!("eventually: running {cmd} failed: {e}"),
        }
    }
    // No drain may start once the checks below begin.
    if let Some(cov) = &s.cov {
        cov.settle_drains();
    }

    let deadline = Instant::now() + Duration::from_secs(300);
    let mut last = String::new();
    let mut recovered = false;
    while Instant::now() < deadline {
        match s.all_nodes_serve() {
            Ok(()) => {
                recovered = true;
                break;
            }
            Err(e) => {
                if e != last {
                    println!("eventually: waiting for recovery: {e}");
                    last = e;
                }
                std::thread::sleep(Duration::from_secs(2));
            }
        }
    }
    assert_always!(
        recovered,
        "the system recovers once faults stop",
        &json!({ "last_error": last })
    );
    if !recovered {
        println!("eventually: not recovered, ending the timeline");
        std::process::exit(0);
    }
    std::thread::sleep(Duration::from_secs(5));

    let (acked_logs, acked_metrics) = {
        let m = lock(&s.model);
        let logs: Vec<Write> = m
            .acked_logs
            .iter()
            .filter_map(|id| m.writes.get(id).cloned())
            .collect();
        let metrics: Vec<Write> = m
            .acked_metrics
            .iter()
            .filter_map(|id| m.writes.get(id).cloned())
            .collect();
        println!(
            "eventually: assertions start; {} writes, {} acked log writes, {} acked metrics writes",
            m.writes.len(),
            logs.len(),
            metrics.len()
        );
        (logs, metrics)
    };
    let n = s.ravel.nodes.len();

    // Durability: every acked write, read with its commit tokens.
    let mut unreadable: Vec<String> = Vec::new();
    let mut missing: Vec<String> = Vec::new();
    for (i, chunk) in acked_logs.chunks(TOKEN_BATCH).enumerate() {
        let read = retry_read(|attempt| s.token_read_logs((i + attempt) % n, chunk));
        match read {
            Ok(j) => {
                missing.extend(j.missing.iter().cloned());
                Shared::check_log_rows(&j, i % n, "eventually-token");
            }
            Err(e) => unreadable.push(format!(
                "log writes {:?}: {e}",
                chunk.iter().map(|w| w.id).collect::<Vec<_>>()
            )),
        }
    }
    for (i, chunk) in acked_metrics.chunks(TOKEN_BATCH).enumerate() {
        let read = retry_read(|attempt| s.token_read_metrics((i + attempt) % n, chunk));
        match read {
            Ok(j) => {
                missing.extend(j.missing.iter().cloned());
                Shared::check_metric_rows(&j, i % n, "eventually-token");
            }
            Err(e) => unreadable.push(format!(
                "metrics writes {:?}: {e}",
                chunk.iter().map(|w| w.id).collect::<Vec<_>>()
            )),
        }
    }
    assert_always!(
        unreadable.is_empty() && missing.is_empty(),
        "acked writes are readable with their commit tokens once faults stop",
        &json!({ "unreadable": first(&unreadable), "missing": first(&missing) })
    );

    // Visibility: every acked write, on every node, without a token.
    let log_ids: Vec<u64> = acked_logs.iter().map(|w| w.id).collect();
    let metric_ids: Vec<u64> = acked_metrics.iter().map(|w| w.id).collect();
    let mut invisible: Vec<String> = Vec::new();
    let mut scan_failures: Vec<String> = Vec::new();
    for node in 0..n {
        match retry_read(|_| s.scan_logs(node, &log_ids)) {
            Ok(j) => {
                invisible.extend(j.missing.iter().map(|l| format!("node {node}: {l}")));
                Shared::check_log_rows(&j, node, "eventually-scan");
            }
            Err(e) => scan_failures.push(format!("node {node} logs: {e}")),
        }
        match retry_read(|_| s.scan_metrics(node, &metric_ids)) {
            Ok(j) => {
                invisible.extend(j.missing.iter().map(|l| format!("node {node}: {l}")));
                Shared::check_metric_rows(&j, node, "eventually-scan");
            }
            Err(e) => scan_failures.push(format!("node {node} metrics: {e}")),
        }
    }
    assert_always!(
        invisible.is_empty() && scan_failures.is_empty(),
        "acked writes are visible without a token once faults stop",
        &json!({ "invisible": first(&invisible), "failures": first(&scan_failures) })
    );
    println!(
        "eventually: ending the timeline (unreadable {}, missing {}, invisible {}, scan failures {})",
        unreadable.len(),
        missing.len(),
        invisible.len(),
        scan_failures.len()
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

#[hegel::main(phases = [hegel::Phase::Generate])]
fn main(tc: TestCase) {
    antithesis_sdk::antithesis_init();
    let cfg = Config::from_env();
    let ravel = match Ravel::new(cfg.nodes.clone(), cfg.token.clone()) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("driver: {e}");
            std::process::exit(1);
        }
    };
    let shared = Arc::new(Shared {
        ravel,
        model: Mutex::new(Model::default()),
        gate: RwLock::new(()),
        stopping: AtomicBool::new(false),
        eventually_lock: Mutex::new(()),
        eventually_on: cfg.eventually_on,
        max_writes: cfg.max_writes,
        inject_missing: std::env::var("WORKLOAD_INJECT_MISSING_RECORD").is_ok_and(|v| v == "1"),
        cov: Coverage::from_env(),
    });

    for node in 0..shared.ravel.nodes.len() {
        wait_for(&format!("node {node} is ready"), || {
            shared.ravel.ready(node)
        });
    }
    wait_for("every node answers SQL", || shared.all_nodes_serve());
    antithesis_sdk::lifecycle::setup_complete(&json!({ "nodes": cfg.nodes }));
    println!("startup: setup complete; waiting for /workload-active");
    while !Path::new("/workload-active").exists() {
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_reachable!("the workload starts", &json!({}));

    if let Some(secs) = cfg.eventually_after {
        let s = Arc::clone(&shared);
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_secs(secs));
            run_eventually(&s, "forced by WORKLOAD_EVENTUALLY_AFTER_SECS");
        });
    }

    if shared.cov.is_some() {
        machine(CoverageWorkload(shared))
            .steps(i64::MAX)
            .max_concurrency(4)
            .run_concurrent(tc);
    } else {
        machine(Workload(shared))
            .steps(i64::MAX)
            .max_concurrency(4)
            .run_concurrent(tc);
    }
}
