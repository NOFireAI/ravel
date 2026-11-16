//! What the driver remembers about every write, and how a query result is
//! judged against it.
//!
//! A write is registered, with an in-flight attempt, before its request is
//! sent, so a query that runs while the request is in flight never sees rows
//! the model cannot attribute. Each attempt then records what the client saw.
//! Only a strict 200 with no partial success and a commit-token header is an
//! acknowledgement; every other outcome may or may not have stored the
//! records, now or later, for the rest of the timeline (an `Abandoned` or
//! timed-out flush can still commit up to `max_flush_lifetime` later).

use std::collections::{BTreeMap, HashMap};

use crate::otlp::{METRIC_SERIES_LABEL, Sample, VALUE_STRIDE};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Signal {
    Logs,
    Metrics,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// Strict with the process default flush policy.
    Strict,
    /// Strict with the `arrival` flush policy (`strict-arrival` header).
    StrictArrival,
    /// Acknowledged at enqueue; never durable.
    Buffered,
}

impl Mode {
    pub fn header(self) -> Option<&'static str> {
        match self {
            Mode::Strict => None,
            Mode::StrictArrival => Some("strict-arrival"),
            Mode::Buffered => Some("buffered"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Attempt {
    /// Sent, no outcome recorded yet.
    InFlight,
    /// A strict 200 with no partial success, carrying this commit-token
    /// header value.
    Acked { header: String },
    /// A buffered-mode 200: admitted, not durable.
    BufferedAck,
    /// A 200 that is not a clean strict acknowledgement: a partial success, a
    /// missing token header, or an undecodable body.
    Partial,
    /// An HTTP error status.
    Refused { status: u16 },
    /// No response at all.
    NoResponse,
}

#[derive(Clone, Debug)]
pub struct Write {
    pub id: u64,
    pub signal: Signal,
    pub mode: Mode,
    pub key: Option<String>,
    /// Log records in the write (logs only).
    pub records: u32,
    /// The write's samples (metrics only).
    pub samples: Vec<Sample>,
    /// Event time of the write's records.
    pub ts_ns: u64,
    pub attempts: Vec<Attempt>,
}

impl Write {
    /// The first acknowledgement's commit-token header, if any attempt was
    /// acknowledged.
    pub fn acked_header(&self) -> Option<&str> {
        self.attempts.iter().find_map(|a| match a {
            Attempt::Acked { header } => Some(header.as_str()),
            _ => None,
        })
    }

    pub fn is_acked(&self) -> bool {
        self.acked_header().is_some()
    }

    /// Commit tokens of the first acknowledgement, qualified with `table`.
    pub fn tokens(&self, table: &str) -> Vec<String> {
        self.acked_header()
            .map(|h| {
                h.split(',')
                    .map(str::trim)
                    .filter(|t| !t.is_empty())
                    .map(|t| format!("{table}:{t}"))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The most copies of any one record the attempts so far can have
    /// stored. An acknowledgement whose header repeats an earlier
    /// acknowledgement's is an idempotency replay and stores nothing; every
    /// other attempt, whatever its outcome, may have stored one copy.
    pub fn max_copies(&self) -> u64 {
        let mut seen: Vec<&str> = Vec::new();
        let mut n = 0;
        for a in &self.attempts {
            match a {
                Attempt::Acked { header } => {
                    if !seen.contains(&header.as_str()) {
                        seen.push(header.as_str());
                        n += 1;
                    }
                }
                _ => n += 1,
            }
        }
        n
    }

    /// Whether some acknowledgement repeated an earlier one's header.
    pub fn has_replay(&self) -> bool {
        let mut seen: Vec<&str> = Vec::new();
        for a in &self.attempts {
            if let Attempt::Acked { header } = a {
                if seen.contains(&header.as_str()) {
                    return true;
                }
                seen.push(header.as_str());
            }
        }
        false
    }

    /// Whether some attempt had no definite outcome.
    pub fn had_unknown(&self) -> bool {
        self.attempts
            .iter()
            .any(|a| matches!(a, Attempt::NoResponse | Attempt::Refused { .. }))
    }
}

#[derive(Default)]
pub struct Model {
    pub writes: BTreeMap<u64, Write>,
    pub next_id: u64,
    pub next_sample: u64,
    pub acked_logs: Vec<u64>,
    pub acked_metrics: Vec<u64>,
}

impl Model {
    /// Event-time bounds over every write, in nanoseconds.
    pub fn ts_bounds(&self) -> Option<(u64, u64)> {
        let min = self.writes.values().map(|w| w.ts_ns).min()?;
        let max = self.writes.values().map(|w| w.ts_ns).max()?;
        Some((min, max))
    }

    pub fn mark_acked(&mut self, id: u64) {
        let Some(w) = self.writes.get(&id) else {
            return;
        };
        let list = match w.signal {
            Signal::Logs => &mut self.acked_logs,
            Signal::Metrics => &mut self.acked_metrics,
        };
        if !list.contains(&id) {
            list.push(id);
        }
    }
}

/// Everything wrong, and everything worth noting, in one set of log rows.
#[derive(Default, Debug)]
pub struct LogJudgement {
    /// Rows naming a write or record the workload never sent.
    pub phantoms: Vec<String>,
    /// Records stored more often than the attempts allow, on writes with no
    /// replayed acknowledgement.
    pub over: Vec<String>,
    /// The same, on writes where some acknowledgement was a replay.
    pub over_after_replay: Vec<String>,
    /// Records of the expected writes that the rows do not contain.
    pub missing: Vec<String>,
    /// Whether any counted row belongs to a write with a replayed
    /// acknowledgement, so the replay bound was actually checked.
    pub replay_rows_checked: bool,
    pub saw_duplicate: bool,
    pub saw_unknown_stored: bool,
    pub saw_acked: bool,
}

/// Judge log rows `(write, record) -> count` against the model. `expect`
/// lists the writes every record of which must be present.
pub fn judge_logs(model: &Model, counts: &HashMap<(u64, u32), u64>, expect: &[u64]) -> LogJudgement {
    let mut j = LogJudgement::default();
    for (&(wid, rec), &n) in counts {
        let Some(w) = model.writes.get(&wid).filter(|w| w.signal == Signal::Logs) else {
            j.phantoms.push(format!("write {wid} rec {rec} x{n}: no such log write"));
            continue;
        };
        if rec >= w.records {
            j.phantoms.push(format!("write {wid} rec {rec} x{n}: write has {} records", w.records));
            continue;
        }
        let max = w.max_copies();
        if w.has_replay() {
            j.replay_rows_checked = true;
        }
        if n > max {
            let line = format!(
                "write {wid} rec {rec}: stored {n} times, at most {max} allowed; attempts {:?}",
                w.attempts
            );
            if w.has_replay() {
                j.over_after_replay.push(line);
            } else {
                j.over.push(line);
            }
        }
        if n > 1 {
            j.saw_duplicate = true;
        }
        if w.is_acked() {
            j.saw_acked = true;
        } else if w.had_unknown() {
            j.saw_unknown_stored = true;
        }
    }
    for wid in expect {
        let Some(w) = model.writes.get(wid) else {
            continue;
        };
        for rec in 0..w.records {
            if !counts.contains_key(&(*wid, rec)) {
                j.missing.push(format!(
                    "write {wid} rec {rec} missing; acked header {:?}",
                    w.acked_header()
                ));
            }
        }
    }
    j
}

/// Parse the rows of the logs count query (`w`, `r`, `n` columns).
pub fn log_counts(rows: &[serde_json::Value]) -> Result<HashMap<(u64, u32), u64>, String> {
    let mut out = HashMap::new();
    for row in rows {
        let cells = row.as_array().ok_or_else(|| format!("row is not an array: {row}"))?;
        let (Some(w), Some(r), Some(n)) = (
            cells.first().and_then(crate::ravel::cell_u64),
            cells.get(1).and_then(crate::ravel::cell_u64),
            cells.get(2).and_then(crate::ravel::cell_u64),
        ) else {
            return Err(format!("unparseable logs row: {row}"));
        };
        let r = u32::try_from(r).map_err(|_| format!("record index out of range: {row}"))?;
        *out.entry((w, r)).or_insert(0) += n;
    }
    Ok(out)
}

/// Everything wrong in one set of metric rows.
#[derive(Default, Debug)]
pub struct MetricJudgement {
    /// Rows whose value, timestamp or series no sent sample matches.
    pub foreign: Vec<String>,
    /// Samples of the expected writes that the rows do not contain.
    pub missing: Vec<String>,
    pub saw_acked: bool,
}

/// One metric row: timestamp, value, series label.
pub type MetricRow = (i64, f64, String);

pub fn metric_rows(rows: &[serde_json::Value]) -> Result<Vec<MetricRow>, String> {
    rows.iter()
        .map(|row| {
            let cells = row.as_array().ok_or_else(|| format!("row is not an array: {row}"))?;
            let ts = cells.first().and_then(crate::ravel::cell_i64);
            let v = cells.get(1).and_then(crate::ravel::cell_f64);
            let s = cells.get(2).and_then(|c| c.as_str()).map(str::to_string);
            match (ts, v, s) {
                (Some(ts), Some(v), Some(s)) => Ok((ts, v, s)),
                _ => Err(format!("unparseable samples row: {row}")),
            }
        })
        .collect()
}

pub fn judge_metrics(model: &Model, rows: &[MetricRow], expect: &[u64]) -> MetricJudgement {
    let mut j = MetricJudgement::default();
    let mut present: HashMap<(u64, usize), ()> = HashMap::new();
    for (ts, v, series) in rows {
        let attributed = attribute_sample(model, *v).filter(|(w, i)| {
            let s = &w.samples[*i];
            i64::try_from(s.ts_ns).ok() == Some(*ts)
                && *series == format!("s{}", s.series)
                && s.value.to_bits() == v.to_bits()
        });
        match attributed {
            Some((w, i)) => {
                if w.is_acked() {
                    j.saw_acked = true;
                }
                present.insert((w.id, i), ());
            }
            None => j.foreign.push(format!(
                "sample ts={ts} value={v} {METRIC_SERIES_LABEL}={series}: matches no sent sample"
            )),
        }
    }
    for wid in expect {
        let Some(w) = model.writes.get(wid) else {
            continue;
        };
        for (i, s) in w.samples.iter().enumerate() {
            if !present.contains_key(&(*wid, i)) {
                j.missing.push(format!(
                    "write {wid} sample {i} (ts={} s{}) missing; acked header {:?}",
                    s.ts_ns,
                    s.series,
                    w.acked_header()
                ));
            }
        }
    }
    j
}

fn attribute_sample(model: &Model, v: f64) -> Option<(&Write, usize)> {
    if !(v >= 0.0 && v.fract() == 0.0 && v < 9.0e15) {
        return None;
    }
    let n = v as u64;
    let w = model.writes.get(&(n / VALUE_STRIDE))?;
    if w.signal != Signal::Metrics {
        return None;
    }
    let i = usize::try_from(n % VALUE_STRIDE).ok()?;
    if i < w.samples.len() { Some((w, i)) } else { None }
}
