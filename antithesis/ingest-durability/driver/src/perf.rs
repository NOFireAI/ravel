//! The perf variant (`WORKLOAD_PERF=1`; README.md, "Perf variant"): once
//! faults stop, how long each node takes to its first strict
//! acknowledgement, and how long strict writes take once the system has
//! recovered. It measures and reports, and once the system recovers asserts
//! three always-bounds (#4559): P1, every node's first strict ack within 60 s
//! of faults stopping; P2, every post-recovery strict write answered within
//! the client timeout; P3, every recovered node's median strict-ack latency
//! within 2 s.
//!
//! Every assertion here goes through `assert_raw` and is catalogued only when
//! the variant is on, so the other variants' property lists do not change.
//!
//! No probe thread takes the gate or calls a stop helper: the eventually
//! holds the gate's write guard and `stopping` is set, so either would block
//! or park the thread and the eventually's join would never return.

use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime};

use antithesis_sdk::assert::{AssertType, assert_raw};
use serde_json::{Value, json};

use ravel_antithesis_driver::model::{Attempt, Signal};

use crate::lock;

/// How often a node's TTFA prober sends a probe write.
pub const TTFA_CADENCE: Duration = Duration::from_secs(1);
/// The client timeout of one TTFA probe write.
pub const TTFA_TIMEOUT: Duration = Duration::from_secs(5);
/// How long after faults stop a TTFA prober keeps sending.
pub const TTFA_DEADLINE: Duration = Duration::from_secs(300);
/// Ack probes per node after recovery.
pub const ACK_PROBES: u32 = 10;
/// P1's band, seconds: a recovered node's first strict ack must come within
/// this of faults stopping. The 60 s rung of the TTFA ladder.
pub const P1_RECOVERY_SECS: u64 = 60;
/// P3's band, seconds: a recovered node's median strict-ack latency must be
/// within this. The 2 s rung of the median ladder.
pub const P3_MEDIAN_SECS: u64 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Always,
    Sometimes,
    Reachable,
    Unreachable,
}

#[derive(Debug)]
pub struct Prop {
    kind: Kind,
    message: &'static str,
}

const TTFA_5: Prop = Prop {
    kind: Kind::Sometimes,
    message: "a node's first strict ack came within 5 s of faults stopping",
};
const TTFA_15: Prop = Prop {
    kind: Kind::Sometimes,
    message: "a node's first strict ack came within 15 s of faults stopping",
};
const TTFA_30: Prop = Prop {
    kind: Kind::Sometimes,
    message: "a node's first strict ack came within 30 s of faults stopping",
};
const TTFA_60: Prop = Prop {
    kind: Kind::Sometimes,
    message: "a node's first strict ack came within 60 s of faults stopping",
};
const TTFA_120: Prop = Prop {
    kind: Kind::Sometimes,
    message: "a node's first strict ack came within 120 s of faults stopping",
};
const TTFA_300: Prop = Prop {
    kind: Kind::Sometimes,
    message: "a node's first strict ack came within 300 s of faults stopping",
};
const ACK_1: Prop = Prop {
    kind: Kind::Sometimes,
    message: "a strict write after recovery was acknowledged within 1 s",
};
const ACK_2: Prop = Prop {
    kind: Kind::Sometimes,
    message: "a strict write after recovery was acknowledged within 2 s",
};
const ACK_5: Prop = Prop {
    kind: Kind::Sometimes,
    message: "a strict write after recovery was acknowledged within 5 s",
};
const ACK_10: Prop = Prop {
    kind: Kind::Sometimes,
    message: "a strict write after recovery was acknowledged within 10 s",
};
const ANSWERED: Prop = Prop {
    kind: Kind::Sometimes,
    message: "a strict write after recovery was answered within the client timeout",
};
const MEDIAN_1: Prop = Prop {
    kind: Kind::Sometimes,
    message: "a node's median strict-ack latency after recovery is within 1 s",
};
const MEDIAN_2: Prop = Prop {
    kind: Kind::Sometimes,
    message: "a node's median strict-ack latency after recovery is within 2 s",
};
const MEDIAN_5: Prop = Prop {
    kind: Kind::Sometimes,
    message: "a node's median strict-ack latency after recovery is within 5 s",
};
const MEDIAN_8: Prop = Prop {
    kind: Kind::Sometimes,
    message: "a node's median strict-ack latency after recovery is within 8 s",
};
const CLOCKS_DISAGREE: Prop = Prop {
    kind: Kind::Sometimes,
    message: "a perf interval saw the wall and monotonic clocks disagree by more than 1 s",
};
const STOP_FAULTS_FAILED: Prop = Prop {
    kind: Kind::Unreachable,
    message: "the driver could not stop faults before measuring",
};
const PROBES_RAN: Prop = Prop {
    kind: Kind::Reachable,
    message: "the perf probes ran in an eventually",
};

/// P1: the 60 s band is #4559 comment 7088441883 (perf-measure-1): over 952
/// TTFA samples the smallest rung with zero false was 30 s, and the band is
/// the next rung up. t0 is not always fault-free: #4559 comment 7088652995
/// found that in about one history in four the platform stops and restarts one
/// container, in all but one case a ravel node, 0.0012 to 8.54 s after the
/// fault pause and after t0 (largest TTFA on such a history 14570 ms); the
/// 60 s band was derived over all histories, those included.
const P1_RECOVERY: Prop = Prop {
    kind: Kind::Always,
    message: "every node's first strict ack comes within 60 s of faults stopping",
};
/// P2: a deadline, not a measured band. The client abandons a probe write at
/// its client timeout, 15 s except for probes 2, 5 and 8 under the self-check,
/// which use 1 ms (`ack_timeout`). A post-recovery strict write that gets no
/// response, by that timeout or through a transport error, including a failed
/// body read (`Reply::NoResponse` in ravel.rs), makes this fire.
/// The expectation that it holds rests on perf-measure-1 (0 false over 9506
/// probes, #4559 comment 7088441883), whose post-pause restarts all landed
/// before the ack probes (#4559 comment 7088652995).
const P2_ANSWERED: Prop = Prop {
    kind: Kind::Always,
    message: "every strict write after recovery is answered within the client timeout",
};
/// P3: the 2 s band is measured (#4559 comment 7088441883, perf-measure-1:
/// over 938 medians the smallest rung with zero false was 1 s, and the band is
/// the next rung up), not a guarantee documented in docs/consistency-model.md.
/// The band was derived over all histories, including those whose post-pause
/// restarts landed before the ack probes (#4559 comment 7088652995).
const P3_MEDIAN: Prop = Prop {
    kind: Kind::Always,
    message: "every recovered node's median strict-ack latency is within 2 s",
};

const TTFA_LADDER: [(u64, &Prop); 6] = [
    (5, &TTFA_5),
    (15, &TTFA_15),
    (30, &TTFA_30),
    (60, &TTFA_60),
    (120, &TTFA_120),
    (300, &TTFA_300),
];
const ACK_LADDER: [(u64, &Prop); 4] = [(1, &ACK_1), (2, &ACK_2), (5, &ACK_5), (10, &ACK_10)];
const MEDIAN_LADDER: [(u64, &Prop); 4] = [
    (1, &MEDIAN_1),
    (2, &MEDIAN_2),
    (5, &MEDIAN_5),
    (8, &MEDIAN_8),
];

const PROPS: [&Prop; 21] = [
    &TTFA_5,
    &TTFA_15,
    &TTFA_30,
    &TTFA_60,
    &TTFA_120,
    &TTFA_300,
    &ACK_1,
    &ACK_2,
    &ACK_5,
    &ACK_10,
    &ANSWERED,
    &MEDIAN_1,
    &MEDIAN_2,
    &MEDIAN_5,
    &MEDIAN_8,
    &CLOCKS_DISAGREE,
    &STOP_FAULTS_FAILED,
    &PROBES_RAN,
    &P1_RECOVERY,
    &P2_ANSWERED,
    &P3_MEDIAN,
];

/// The arguments the SDK's assertion macros pass to `assert_raw` for each
/// kind: the assert type, the display type and must-hit. An always matches the
/// `assert_always!` macro (`AssertType::Always`, "Always", must-hit on); an
/// unreachable is a reachability assertion with must-hit off.
fn kind_args(kind: Kind) -> (AssertType, &'static str, bool) {
    match kind {
        Kind::Always => (AssertType::Always, "Always", true),
        Kind::Sometimes => (AssertType::Sometimes, "Sometimes", true),
        Kind::Reachable => (AssertType::Reachability, "Reachable", true),
        Kind::Unreachable => (AssertType::Reachability, "Unreachable", false),
    }
}

fn emit(p: &Prop, condition: bool, hit: bool, details: &Value) {
    let (assert_type, display_type, must_hit) = kind_args(p.kind);
    assert_raw(
        condition,
        p.message.to_string(),
        details,
        module_path!().to_string(),
        "perf".to_string(),
        file!().to_string(),
        0,
        0,
        hit,
        must_hit,
        assert_type,
        display_type.to_string(),
        p.message.to_string(),
    );
}

/// Whether a `WORKLOAD_PERF` value turns the variant on.
pub fn enabled(value: Option<&str>) -> bool {
    value == Some("1")
}

/// Whether the perf and coverage variants were both asked for.
pub fn conflicts(perf: Option<&str>, coverage: Option<&str>) -> bool {
    enabled(perf) && coverage == Some("1")
}

/// Whether a `WORKLOAD_PERF_SELF_CHECK` value turns the self-check on.
pub fn self_check_enabled(value: Option<&str>) -> bool {
    value == Some("1")
}

/// Whether the self-check was asked for without the perf variant, which main
/// refuses: the self-check only shortens the perf variant's ack probes.
pub fn self_check_needs_perf(perf: Option<&str>, self_check: Option<&str>) -> bool {
    self_check_enabled(self_check) && !enabled(perf)
}

/// The client timeout for ack probe `i`: 1 ms for probes 2, 5 and 8 of each
/// node (i modulo 3 equal to 2) under the self-check, so those three answer
/// no-response and make P2 false while the other seven keep the median finite;
/// `normal` otherwise.
pub fn ack_timeout(i: u32, self_check: bool, normal: Duration) -> Duration {
    if self_check && i % 3 == 2 {
        Duration::from_millis(1)
    } else {
        normal
    }
}

/// What running `ANTITHESIS_STOP_FAULTS` came to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StopFaults {
    /// The variable is absent.
    Unset,
    /// The command ran and exited 0.
    Ok,
    /// The command could not be spawned or exited non-zero.
    Failed,
}

/// What `ANTITHESIS_STOP_FAULTS` came to: `var` is the variable's lookup and
/// `run` runs the command it names. A value that is not UTF-8 is never run.
pub fn stop_faults<R>(var: Result<String, std::env::VarError>, run: R) -> StopFaults
where
    R: FnOnce(&str) -> std::io::Result<std::process::ExitStatus>,
{
    match var {
        Ok(cmd) => match run(&cmd) {
            Ok(st) => {
                println!("eventually: {cmd} 3600 exited {st}");
                if st.success() {
                    StopFaults::Ok
                } else {
                    StopFaults::Failed
                }
            }
            Err(e) => {
                println!("eventually: running {cmd} failed: {e}");
                StopFaults::Failed
            }
        },
        Err(std::env::VarError::NotPresent) => StopFaults::Unset,
        Err(std::env::VarError::NotUnicode(_)) => StopFaults::Failed,
    }
}

/// The line the variant prints at t0, and whether it measures.
pub fn start_line(stop: StopFaults) -> (String, bool) {
    match stop {
        StopFaults::Unset => ("perf t0 stop_faults=unset".to_string(), true),
        StopFaults::Ok => ("perf t0 stop_faults=ok".to_string(), true),
        StopFaults::Failed => ("perf skipped reason=stop-faults-failed".to_string(), false),
    }
}

/// One instant on both clocks.
#[derive(Clone, Copy, Debug)]
pub struct Stamp {
    pub mono: Instant,
    pub wall: SystemTime,
}

impl Stamp {
    pub fn now() -> Stamp {
        Stamp {
            mono: Instant::now(),
            wall: SystemTime::now(),
        }
    }
}

/// The interval from `from` to `to` in milliseconds: monotonic, and the
/// signed wall-clock difference, negative when the wall clock went back.
fn interval(from: Stamp, to: Stamp) -> (u64, i64) {
    let mono =
        u64::try_from(to.mono.saturating_duration_since(from.mono).as_millis()).unwrap_or(u64::MAX);
    let wall = match to.wall.duration_since(from.wall) {
        Ok(d) => i64::try_from(d.as_millis()).unwrap_or(i64::MAX),
        Err(e) => i64::try_from(e.duration().as_millis()).map_or(i64::MIN, |ms| -ms),
    };
    (mono, wall)
}

/// What the client saw for one probe write, in `classify`'s terms.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    Acked,
    Refused(u16),
    Partial,
    NoResponse,
}

impl Outcome {
    pub fn from_attempt(a: &Attempt) -> Outcome {
        match a {
            Attempt::Acked { .. } => Outcome::Acked,
            Attempt::Refused { status } => Outcome::Refused(*status),
            Attempt::NoResponse => Outcome::NoResponse,
            Attempt::Partial | Attempt::BufferedAck | Attempt::InFlight => Outcome::Partial,
        }
    }

    fn render(&self) -> String {
        match self {
            Outcome::Acked => "acked".to_string(),
            Outcome::Refused(s) => format!("refused-{s}"),
            Outcome::Partial => "partial".to_string(),
            Outcome::NoResponse => "no-response".to_string(),
        }
    }
}

/// One probe write's answer, with the clocks read immediately before the
/// HTTP call and when it returned.
#[derive(Clone, Debug)]
pub struct Answer {
    pub outcome: Outcome,
    pub sent: Stamp,
    pub received: Stamp,
}

/// Figure (a) for one node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TtfaSample {
    /// From t0 to the receipt of the first ack; `None` when none came within
    /// the deadline.
    pub mono_ms: Option<u64>,
    pub wall_ms: Option<i64>,
    pub probes: u32,
    /// The latest non-ack answer before the first ack.
    pub last: Option<Outcome>,
}

impl TtfaSample {
    pub fn none(probes: u32) -> TtfaSample {
        TtfaSample {
            mono_ms: None,
            wall_ms: None,
            probes,
            last: None,
        }
    }
}

/// Figure (b) for one probe.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AckSample {
    pub i: u32,
    pub signal: Signal,
    pub mono_ms: u64,
    pub wall_ms: i64,
    pub outcome: Outcome,
}

fn signal_name(s: Signal) -> &'static str {
    match s {
        Signal::Logs => "logs",
        Signal::Metrics => "metrics",
    }
}

/// Even probes are logs, odd ones metrics.
pub fn ack_signal(i: u32) -> Signal {
    if i.is_multiple_of(2) {
        Signal::Logs
    } else {
        Signal::Metrics
    }
}

fn opt<T: std::fmt::Display>(v: Option<T>, absent: &str) -> String {
    v.map_or_else(|| absent.to_string(), |v| v.to_string())
}

pub fn ttfa_line(node: usize, s: &TtfaSample) -> String {
    let outcome = if s.mono_ms.is_some() { "acked" } else { "none" };
    let last = s.last.as_ref().map_or_else(
        || "-".to_string(),
        |o| o.render().split_whitespace().collect::<Vec<_>>().join("_"),
    );
    format!(
        "perf ttfa node={node} mono_ms={} wall_ms={} probes={} outcome={outcome} last={last}",
        opt(s.mono_ms, "none"),
        opt(s.wall_ms, "none"),
        s.probes
    )
}

pub fn ack_start_line(node: usize, i: u32) -> String {
    format!("perf ack-start node={node} i={i}")
}

pub fn ack_line(node: usize, s: &AckSample) -> String {
    format!(
        "perf ack node={node} i={} signal={} mono_ms={} wall_ms={} outcome={}",
        s.i,
        signal_name(s.signal),
        s.mono_ms,
        s.wall_ms,
        s.outcome.render()
    )
}

pub fn median_line(node: usize, median: Option<u64>) -> String {
    format!("perf median node={node} mono_ms={}", opt(median, "inf"))
}

pub fn summary_line(ttfa: usize, ack: usize, recovered: bool) -> String {
    format!("perf summary ttfa={ttfa} ack={ack} recovered={recovered}")
}

/// The element at index len/2 of the latencies sorted ascending, with every
/// non-acked probe counted as infinity; `None` is infinity.
pub fn median_ms(latencies: &[Option<u64>]) -> Option<u64> {
    let mut sorted: Vec<u64> = latencies.iter().map(|l| l.unwrap_or(u64::MAX)).collect();
    sorted.sort_unstable();
    sorted
        .get(latencies.len() / 2)
        .copied()
        .filter(|v| *v != u64::MAX)
}

fn within(ms: Option<u64>, secs: u64) -> bool {
    ms.is_some_and(|m| m <= secs * 1000)
}

fn clocks_disagree(mono_ms: u64, wall_ms: i64) -> bool {
    (i128::from(wall_ms) - i128::from(mono_ms)).unsigned_abs() > 1000
}

/// One assertion evaluation.
#[derive(Debug)]
pub struct Eval {
    pub prop: &'static Prop,
    pub condition: bool,
    pub details: Value,
}

/// The TTFA ladder for one node, plus P1 when the timeline recovered and the
/// clock check when it acked. P1 is evaluated once per node, only when
/// `recovered`: a `none` sample is false, not skipped. When not recovered P1 is
/// left out (the "the system recovers once faults stop" assertion already
/// fails there); the ladder is evaluated either way.
pub fn ttfa_evals(node: usize, s: &TtfaSample, recovered: bool) -> Vec<Eval> {
    let outcome = if s.mono_ms.is_some() { "acked" } else { "none" };
    let details = json!({
        "node": node,
        "mono_ms": s.mono_ms,
        "wall_ms": s.wall_ms,
        "outcome": outcome,
        "probes": s.probes,
    });
    let mut out: Vec<Eval> = TTFA_LADDER
        .iter()
        .map(|(secs, prop)| Eval {
            prop,
            condition: within(s.mono_ms, *secs),
            details: details.clone(),
        })
        .collect();
    if recovered {
        out.push(Eval {
            prop: &P1_RECOVERY,
            condition: within(s.mono_ms, P1_RECOVERY_SECS),
            details: details.clone(),
        });
    }
    if let (Some(mono), Some(wall)) = (s.mono_ms, s.wall_ms) {
        out.push(Eval {
            prop: &CLOCKS_DISAGREE,
            condition: clocks_disagree(mono, wall),
            details,
        });
    }
    out
}

/// The per-probe ladder for one ack probe, plus the clock check.
pub fn ack_evals(node: usize, s: &AckSample) -> Vec<Eval> {
    let details = json!({
        "node": node,
        "mono_ms": s.mono_ms,
        "wall_ms": s.wall_ms,
        "outcome": s.outcome.render(),
        "i": s.i,
        "signal": signal_name(s.signal),
    });
    let acked_ms = (s.outcome == Outcome::Acked).then_some(s.mono_ms);
    let mut out: Vec<Eval> = ACK_LADDER
        .iter()
        .map(|(secs, prop)| Eval {
            prop,
            condition: within(acked_ms, *secs),
            details: details.clone(),
        })
        .collect();
    out.push(Eval {
        prop: &ANSWERED,
        condition: s.outcome != Outcome::NoResponse,
        details: details.clone(),
    });
    out.push(Eval {
        prop: &P2_ANSWERED,
        condition: s.outcome != Outcome::NoResponse,
        details: details.clone(),
    });
    out.push(Eval {
        prop: &CLOCKS_DISAGREE,
        condition: clocks_disagree(s.mono_ms, s.wall_ms),
        details,
    });
    out
}

/// The median ladder for one node.
pub fn median_evals(node: usize, median: Option<u64>, acked: usize) -> Vec<Eval> {
    let details = json!({
        "node": node,
        "mono_ms": median,
        "wall_ms": Value::Null,
        "outcome": if median.is_some() { "finite" } else { "inf" },
        "acked": acked,
    });
    MEDIAN_LADDER
        .iter()
        .map(|(secs, prop)| Eval {
            prop,
            condition: within(median, *secs),
            details: details.clone(),
        })
        .collect()
}

/// One node's median strict-ack latency and its evaluations: the median ladder
/// plus P3. The latency of a probe that was not acked is infinity (`median_ms`
/// counts it so), so the median is finite only when at least six of the ten
/// probes acked. P3 holds when the median is within the 2 s band.
pub fn median_and_evals(node: usize, samples: &[AckSample]) -> (Option<u64>, Vec<Eval>) {
    let lat: Vec<Option<u64>> = samples
        .iter()
        .map(|s| (s.outcome == Outcome::Acked).then_some(s.mono_ms))
        .collect();
    let median = median_ms(&lat);
    let acked = lat.iter().filter(|l| l.is_some()).count();
    let mut evals = median_evals(node, median, acked);
    evals.push(Eval {
        prop: &P3_MEDIAN,
        condition: within(median, P3_MEDIAN_SECS),
        details: json!({
            "node": node,
            "mono_ms": median,
            "wall_ms": Value::Null,
            "outcome": if median.is_some() { "finite" } else { "inf" },
            "acked": acked,
        }),
    });
    (median, evals)
}

fn apply(evals: Vec<Eval>) {
    for e in evals {
        emit(e.prop, e.condition, true, &e.details);
    }
}

/// Figure (a) for one node: sends one probe every `cadence`, each with
/// `timeout`, until the first ack or until `deadline` after t0, with at most
/// `timeout / cadence` probes in flight. Joins every probe before it returns.
/// The sample is the earliest receipt among the acks recorded before the
/// loop sees the first one.
pub fn run_prober<F>(
    send: F,
    t0: Stamp,
    cadence: Duration,
    timeout: Duration,
    deadline: Duration,
) -> TtfaSample
where
    F: Fn(Duration) -> Answer + Sync,
{
    struct State {
        first: Option<Stamp>,
        /// Set once the loop has seen `first`; later acks are not compared.
        observed: bool,
        last: Option<Outcome>,
        in_flight: u128,
    }
    let max_in_flight = (timeout.as_millis() / cadence.as_millis().max(1)).max(1);
    let end = t0.mono + deadline;
    let state = Mutex::new(State {
        first: None,
        observed: false,
        last: None,
        in_flight: 0,
    });
    let wake = Condvar::new();
    let mut probes: u32 = 0;
    std::thread::scope(|sc| {
        let (state, wake, send) = (&state, &wake, &send);
        let mut next = t0.mono;
        let mut st = lock(state);
        loop {
            if st.first.is_some() {
                st.observed = true;
                break;
            }
            let now = Instant::now();
            if now >= end {
                break;
            }
            if now >= next && st.in_flight < max_in_flight {
                st.in_flight += 1;
                probes += 1;
                next = now + cadence;
                sc.spawn(move || {
                    let a = send(timeout);
                    let mut st = lock(state);
                    st.in_flight -= 1;
                    if a.outcome == Outcome::Acked {
                        if !st.observed && a.received.mono <= end {
                            st.first = match st.first {
                                Some(f) if f.mono <= a.received.mono => Some(f),
                                _ => Some(a.received),
                            };
                        }
                    } else if st.first.is_none() {
                        st.last = Some(a.outcome);
                    }
                    wake.notify_all();
                });
                continue;
            }
            let until = if now >= next { end } else { next.min(end) };
            st = wake
                .wait_timeout(st, until.saturating_duration_since(now))
                .map(|(g, _)| g)
                .unwrap_or_else(|e| e.into_inner().0);
        }
        drop(st);
    });
    let st = lock(&state);
    match st.first {
        Some(received) => {
            let (mono, wall) = interval(t0, received);
            TtfaSample {
                mono_ms: Some(mono),
                wall_ms: Some(wall),
                probes,
                last: st.last.clone(),
            }
        }
        None => TtfaSample {
            last: st.last.clone(),
            ..TtfaSample::none(probes)
        },
    }
}

/// Figure (b) for one node: `count` probe writes, serially, each with the
/// timeout `ack_timeout` gives it (the self-check shortens probes 2, 5 and 8 to
/// 1 ms, every other probe uses `timeout`), passing every line to `line` as it
/// happens.
pub fn run_ack_probes<F, L>(
    node: usize,
    count: u32,
    timeout: Duration,
    self_check: bool,
    mut send: F,
    mut line: L,
) -> Vec<AckSample>
where
    F: FnMut(Signal, Duration) -> Answer,
    L: FnMut(String),
{
    let mut out = Vec::new();
    for i in 0..count {
        let signal = ack_signal(i);
        line(ack_start_line(node, i));
        let a = send(signal, ack_timeout(i, self_check, timeout));
        let (mono_ms, wall_ms) = interval(a.sent, a.received);
        let s = AckSample {
            i,
            signal,
            mono_ms,
            wall_ms,
            outcome: a.outcome,
        };
        line(ack_line(node, &s));
        out.push(s);
    }
    out
}

/// The variant's switch: catalogues its assertions when on and carries the
/// self-check flag.
pub struct Perf {
    self_check: bool,
}

impl Perf {
    /// The variant, with every assertion catalogued, when `WORKLOAD_PERF=1`.
    /// Reads `WORKLOAD_PERF_SELF_CHECK` once here. Call after `antithesis_init`.
    pub fn from_env() -> Option<Perf> {
        if !enabled(std::env::var("WORKLOAD_PERF").ok().as_deref()) {
            return None;
        }
        for p in PROPS {
            emit(p, false, false, &Value::Null);
        }
        println!("driver: WORKLOAD_PERF=1, the perf variant is on");
        let self_check =
            self_check_enabled(std::env::var("WORKLOAD_PERF_SELF_CHECK").ok().as_deref());
        if self_check {
            println!(
                "driver: WORKLOAD_PERF_SELF_CHECK=1, ack probes 2, 5 and 8 of each node use a 1 ms client timeout"
            );
        }
        Some(Perf { self_check })
    }

    /// Prints the t0 line. On a failed stop-faults hits the unreachable and
    /// returns `None`: nothing is measured.
    pub fn start(&self, stop: StopFaults, t0: Stamp) -> Option<Session> {
        let (line, measure) = start_line(stop);
        println!("{line}");
        if !measure {
            emit(
                &STOP_FAULTS_FAILED,
                false,
                true,
                &json!({ "stop_faults": "failed" }),
            );
            return None;
        }
        Some(Session {
            t0,
            ttfa_lines: 0,
            ack_lines: 0,
            all_acked: false,
            self_check: self.self_check,
        })
    }
}

/// One eventually's measurements.
pub struct Session {
    t0: Stamp,
    ttfa_lines: usize,
    ack_lines: usize,
    all_acked: bool,
    self_check: bool,
}

impl Session {
    /// Figure (a) for one node; runs on its own thread.
    pub fn probe_ttfa<F>(&self, send: F) -> TtfaSample
    where
        F: Fn(Duration) -> Answer + Sync,
    {
        run_prober(send, self.t0, TTFA_CADENCE, TTFA_TIMEOUT, TTFA_DEADLINE)
    }

    /// Prints one line per node, in node order, and evaluates the TTFA ladder,
    /// with P1 when `recovered`.
    pub fn record_ttfa(&mut self, samples: &[TtfaSample], recovered: bool) {
        for (node, s) in samples.iter().enumerate() {
            println!("{}", ttfa_line(node, s));
            self.ttfa_lines += 1;
            apply(ttfa_evals(node, s, recovered));
        }
        self.all_acked = !samples.is_empty() && samples.iter().all(|s| s.mono_ms.is_some());
    }

    /// Whether the ack probes run: recovered, and every node acked a TTFA probe.
    pub fn ack_probes_due(&self, recovered: bool) -> bool {
        recovered && self.all_acked
    }

    /// Figure (b) for one node; runs on its own thread.
    pub fn probe_acks<F>(&self, node: usize, timeout: Duration, send: F) -> Vec<AckSample>
    where
        F: FnMut(Signal, Duration) -> Answer,
    {
        let samples = run_ack_probes(node, ACK_PROBES, timeout, self.self_check, send, |l| {
            println!("{l}")
        });
        for s in &samples {
            apply(ack_evals(node, s));
        }
        samples
    }

    /// Prints each node's median and evaluates the median ladder and P3.
    pub fn record_acks(&mut self, per_node: &[Vec<AckSample>]) {
        for (node, samples) in per_node.iter().enumerate() {
            self.ack_lines += samples.len();
            let (median, evals) = median_and_evals(node, samples);
            println!("{}", median_line(node, median));
            apply(evals);
        }
    }

    /// The summary line, once per eventually.
    pub fn finish(&self, recovered: bool) {
        println!(
            "{}",
            summary_line(self.ttfa_lines, self.ack_lines, recovered)
        );
        emit(
            &PROBES_RAN,
            true,
            true,
            &json!({ "ttfa": self.ttfa_lines, "ack": self.ack_lines, "recovered": recovered }),
        );
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

    fn cond(evals: &[Eval], p: &Prop) -> Option<bool> {
        let hits: Vec<bool> = evals
            .iter()
            .filter(|e| e.prop.message == p.message)
            .map(|e| e.condition)
            .collect();
        assert!(
            hits.len() <= 1,
            "{} evaluated {} times",
            p.message,
            hits.len()
        );
        hits.first().copied()
    }

    fn acked_ttfa(ms: u64) -> TtfaSample {
        TtfaSample {
            mono_ms: Some(ms),
            wall_ms: Some(i64::try_from(ms).expect("small")),
            probes: 1,
            last: None,
        }
    }

    fn ack(ms: u64, outcome: Outcome) -> AckSample {
        AckSample {
            i: 0,
            signal: Signal::Logs,
            mono_ms: ms,
            wall_ms: i64::try_from(ms).expect("small"),
            outcome,
        }
    }

    fn perf(self_check: bool) -> Perf {
        Perf { self_check }
    }

    fn ackn(i: u32, ms: u64, outcome: Outcome) -> AckSample {
        AckSample {
            i,
            signal: ack_signal(i),
            mono_ms: ms,
            wall_ms: i64::try_from(ms).expect("small"),
            outcome,
        }
    }

    fn ten_acked(ms: u64) -> Vec<AckSample> {
        (0..ACK_PROBES)
            .map(|i| ackn(i, ms, Outcome::Acked))
            .collect()
    }

    fn p3(samples: &[AckSample]) -> Option<bool> {
        let (_, evals) = median_and_evals(0, samples);
        cond(&evals, &P3_MEDIAN)
    }

    // Test a: P1 is evaluated once per node, only when the timeline recovered.
    #[test]
    fn p1_is_evaluated_only_when_recovered_and_counts_none_false() {
        // Inclusive to the millisecond at the 60 s band.
        assert_eq!(
            cond(&ttfa_evals(0, &acked_ttfa(60_000), true), &P1_RECOVERY),
            Some(true)
        );
        assert_eq!(
            cond(&ttfa_evals(0, &acked_ttfa(60_001), true), &P1_RECOVERY),
            Some(false)
        );
        // A none sample in a recovered timeline is false, not skipped.
        assert_eq!(
            cond(&ttfa_evals(0, &TtfaSample::none(300), true), &P1_RECOVERY),
            Some(false)
        );
        // Not recovered: no P1 for either sample, but the six rungs remain.
        for s in [acked_ttfa(500), TtfaSample::none(300)] {
            let evals = ttfa_evals(0, &s, false);
            assert_eq!(cond(&evals, &P1_RECOVERY), None);
            for (_, prop) in TTFA_LADDER {
                assert!(cond(&evals, prop).is_some(), "{}", prop.message);
            }
        }
        // Recovered: the six rungs are still there beside P1.
        for s in [acked_ttfa(500), TtfaSample::none(300)] {
            let evals = ttfa_evals(0, &s, true);
            for (_, prop) in TTFA_LADDER {
                assert!(cond(&evals, prop).is_some(), "{}", prop.message);
            }
        }
    }

    // Test b: P2 is true unless the probe got no response.
    #[test]
    fn p2_is_true_unless_the_probe_got_no_response() {
        assert_eq!(
            cond(&ack_evals(0, &ack(10, Outcome::Acked)), &P2_ANSWERED),
            Some(true)
        );
        assert_eq!(
            cond(&ack_evals(0, &ack(10, Outcome::Refused(503))), &P2_ANSWERED),
            Some(true)
        );
        assert_eq!(
            cond(&ack_evals(0, &ack(10, Outcome::Partial)), &P2_ANSWERED),
            Some(true)
        );
        assert_eq!(
            cond(
                &ack_evals(0, &ack(15_000, Outcome::NoResponse)),
                &P2_ANSWERED
            ),
            Some(false)
        );
    }

    // Test c: P3 tracks the median, non-acked counted infinite.
    #[test]
    fn p3_tracks_the_median_with_non_acked_counted_infinite() {
        assert_eq!(p3(&ten_acked(2000)), Some(true));
        assert_eq!(p3(&ten_acked(2001)), Some(false));
        let none: Vec<AckSample> = (0..ACK_PROBES)
            .map(|i| ackn(i, 10, Outcome::NoResponse))
            .collect();
        assert_eq!(p3(&none), Some(false)); // median is inf
        // Five acked at 600 ms, five not acked: the median is inf.
        let five_five: Vec<AckSample> = (0..ACK_PROBES)
            .map(|i| {
                if i < 5 {
                    ackn(i, 600, Outcome::Acked)
                } else {
                    ackn(i, 600, Outcome::NoResponse)
                }
            })
            .collect();
        assert_eq!(p3(&five_five), Some(false));
        // Nine acked at 600 ms, one at 9000 ms: the median is 600 ms.
        let nine_one: Vec<AckSample> = (0..ACK_PROBES)
            .map(|i| ackn(i, if i == 9 { 9000 } else { 600 }, Outcome::Acked))
            .collect();
        assert_eq!(p3(&nine_one), Some(true));
    }

    // Test d: each Always carries the same condition as its ladder rung.
    #[test]
    fn each_always_matches_its_ladder_rung() {
        for ms in [0u64, 1_000, 60_000, 60_001, 120_000, 300_000] {
            let evals = ttfa_evals(0, &acked_ttfa(ms), true);
            assert_eq!(
                cond(&evals, &P1_RECOVERY),
                cond(&evals, &TTFA_60),
                "ttfa {ms}"
            );
        }
        let evals = ttfa_evals(0, &TtfaSample::none(5), true);
        assert_eq!(
            cond(&evals, &P1_RECOVERY),
            cond(&evals, &TTFA_60),
            "ttfa none"
        );
        for outcome in [
            Outcome::Acked,
            Outcome::Refused(503),
            Outcome::Partial,
            Outcome::NoResponse,
        ] {
            let evals = ack_evals(0, &ack(10, outcome.clone()));
            assert_eq!(
                cond(&evals, &P2_ANSWERED),
                cond(&evals, &ANSWERED),
                "{outcome:?}"
            );
        }
        for ms in [0u64, 2_000, 2_001, 8_000] {
            let (_, evals) = median_and_evals(0, &ten_acked(ms));
            assert_eq!(
                cond(&evals, &P3_MEDIAN),
                cond(&evals, &MEDIAN_2),
                "median {ms}"
            );
        }
    }

    // Test e covers the catalogue and the required tsv (above).

    // Test f: the self-check parsing and the per-probe timeout choice.
    #[test]
    fn the_self_check_is_on_only_for_exactly_1() {
        assert!(self_check_enabled(Some("1")));
        for v in [
            None,
            Some(""),
            Some("0"),
            Some("true"),
            Some(" 1"),
            Some("1 "),
            Some("01"),
        ] {
            assert!(!self_check_enabled(v), "{v:?}");
        }
    }

    #[test]
    fn self_check_needs_perf_only_when_asked_without_it() {
        assert!(self_check_needs_perf(None, Some("1")));
        assert!(!self_check_needs_perf(Some("1"), Some("1")));
        assert!(!self_check_needs_perf(Some("1"), None));
        assert!(!self_check_needs_perf(None, None));
    }

    #[test]
    fn the_self_check_shortens_only_probes_two_five_and_eight() {
        let normal = Duration::from_secs(15);
        for i in 0..ACK_PROBES {
            assert_eq!(ack_timeout(i, false, normal), normal, "off i={i}");
        }
        let short = Duration::from_millis(1);
        for i in 0..ACK_PROBES {
            let want = if i == 2 || i == 5 || i == 8 {
                short
            } else {
                normal
            };
            assert_eq!(ack_timeout(i, true, normal), want, "on i={i}");
        }
    }

    // Mode shapes: all three Always together over hand-built samples.
    #[test]
    fn the_self_check_shape_fails_only_p2() {
        // P1: one node, TTFA 600 ms, recovered.
        assert_eq!(
            cond(&ttfa_evals(0, &acked_ttfa(600), true), &P1_RECOVERY),
            Some(true)
        );
        // The no-response set comes from run_ack_probes calling ack_timeout:
        // the fake send answers no-response exactly for the 1 ms timeout.
        let samples = run_ack_probes(
            0,
            ACK_PROBES,
            Duration::from_secs(15),
            true,
            |_, t| {
                let outcome = if t == Duration::from_millis(1) {
                    Outcome::NoResponse
                } else {
                    Outcome::Acked
                };
                answer_in(outcome, 600)
            },
            |_| {},
        );
        let mut p2_true = 0;
        let mut p2_false = 0;
        for s in &samples {
            match cond(&ack_evals(0, s), &P2_ANSWERED) {
                Some(true) => p2_true += 1,
                Some(false) => p2_false += 1,
                None => panic!("P2 not evaluated"),
            }
        }
        assert_eq!(p2_false, 3);
        assert_eq!(p2_true, 7);
        assert_eq!(p3(&samples), Some(true)); // seven acked at 600 ms
    }

    #[test]
    fn the_slow_ack_shape_fails_only_p3() {
        assert_eq!(
            cond(&ttfa_evals(0, &acked_ttfa(2600), true), &P1_RECOVERY),
            Some(true)
        );
        let samples: Vec<AckSample> = (0..ACK_PROBES)
            .map(|i| ackn(i, 2412, Outcome::Acked))
            .collect();
        for s in &samples {
            assert_eq!(cond(&ack_evals(0, s), &P2_ANSWERED), Some(true));
        }
        assert_eq!(p3(&samples), Some(false)); // median 2412 ms > 2 s
    }

    #[test]
    fn the_no_ack_shape_fails_only_p1() {
        for node in 0..2 {
            assert_eq!(
                cond(
                    &ttfa_evals(node, &TtfaSample::none(300), true),
                    &P1_RECOVERY
                ),
                Some(false)
            );
        }
        // On a Session, two none samples leave the ack probes not due, so no
        // P2 or P3 evaluation can exist.
        let mut s = perf(false)
            .start(StopFaults::Unset, Stamp::now())
            .expect("measures");
        s.record_ttfa(&[TtfaSample::none(300), TtfaSample::none(300)], true);
        assert!(!s.ack_probes_due(true));
    }

    #[test]
    fn ttfa_rungs_are_inclusive_to_the_millisecond() {
        for (secs, prop) in TTFA_LADDER {
            let at = ttfa_evals(0, &acked_ttfa(secs * 1000), false);
            assert_eq!(cond(&at, prop), Some(true), "{}", prop.message);
            let above = ttfa_evals(0, &acked_ttfa(secs * 1000 + 1), false);
            assert_eq!(cond(&above, prop), Some(false), "{}", prop.message);
        }
    }

    #[test]
    fn ack_rungs_are_inclusive_to_the_millisecond() {
        for (secs, prop) in ACK_LADDER {
            let at = ack_evals(0, &ack(secs * 1000, Outcome::Acked));
            assert_eq!(cond(&at, prop), Some(true), "{}", prop.message);
            let above = ack_evals(0, &ack(secs * 1000 + 1, Outcome::Acked));
            assert_eq!(cond(&above, prop), Some(false), "{}", prop.message);
        }
    }

    #[test]
    fn median_rungs_are_inclusive_to_the_millisecond() {
        for (secs, prop) in MEDIAN_LADDER {
            let at = median_evals(0, Some(secs * 1000), 10);
            assert_eq!(cond(&at, prop), Some(true), "{}", prop.message);
            let above = median_evals(0, Some(secs * 1000 + 1), 10);
            assert_eq!(cond(&above, prop), Some(false), "{}", prop.message);
        }
    }

    #[test]
    fn a_none_ttfa_sample_evaluates_every_rung_false() {
        let evals = ttfa_evals(1, &TtfaSample::none(300), false);
        for (_, prop) in TTFA_LADDER {
            assert_eq!(cond(&evals, prop), Some(false), "{}", prop.message);
        }
        assert_eq!(cond(&evals, &CLOCKS_DISAGREE), None);
        assert_eq!(evals.len(), 6);
    }

    #[test]
    fn a_non_acked_probe_evaluates_every_ack_rung_false() {
        for outcome in [Outcome::Refused(503), Outcome::Partial, Outcome::NoResponse] {
            let evals = ack_evals(0, &ack(10, outcome.clone()));
            for (_, prop) in ACK_LADDER {
                assert_eq!(
                    cond(&evals, prop),
                    Some(false),
                    "{outcome:?} {}",
                    prop.message
                );
            }
            assert_eq!(evals.len(), 7);
        }
    }

    #[test]
    fn an_inf_median_evaluates_every_rung_false() {
        let evals = median_evals(0, None, 5);
        for (_, prop) in MEDIAN_LADDER {
            assert_eq!(cond(&evals, prop), Some(false), "{}", prop.message);
        }
    }

    #[test]
    fn a_refused_probe_is_answered_and_a_no_response_is_not() {
        let refused = ack_evals(0, &ack(10, Outcome::Refused(503)));
        assert_eq!(cond(&refused, &ANSWERED), Some(true));
        let partial = ack_evals(0, &ack(10, Outcome::Partial));
        assert_eq!(cond(&partial, &ANSWERED), Some(true));
        let acked = ack_evals(0, &ack(10, Outcome::Acked));
        assert_eq!(cond(&acked, &ANSWERED), Some(true));
        let none = ack_evals(0, &ack(15_000, Outcome::NoResponse));
        assert_eq!(cond(&none, &ANSWERED), Some(false));
    }

    #[test]
    fn the_clock_check_is_true_only_past_one_second() {
        let mut s = ack(2000, Outcome::Acked);
        s.wall_ms = 3000;
        assert_eq!(cond(&ack_evals(0, &s), &CLOCKS_DISAGREE), Some(false));
        s.wall_ms = 3001;
        assert_eq!(cond(&ack_evals(0, &s), &CLOCKS_DISAGREE), Some(true));
        s.wall_ms = -1000;
        assert_eq!(cond(&ack_evals(0, &s), &CLOCKS_DISAGREE), Some(true));
        let mut t = acked_ttfa(500);
        t.wall_ms = Some(1600);
        assert_eq!(
            cond(&ttfa_evals(0, &t, false), &CLOCKS_DISAGREE),
            Some(true)
        );
    }

    #[test]
    fn median_of_ten_acked_is_index_five_of_the_sorted_list() {
        let lat: Vec<Option<u64>> = [900, 100, 800, 200, 700, 300, 600, 400, 500, 1000]
            .into_iter()
            .map(Some)
            .collect();
        assert_eq!(median_ms(&lat), Some(600));
    }

    #[test]
    fn median_with_five_non_acked_is_inf() {
        let lat = [
            Some(100),
            None,
            Some(200),
            None,
            Some(300),
            None,
            Some(400),
            None,
            Some(500),
            None,
        ];
        assert_eq!(median_ms(&lat), None);
    }

    #[test]
    fn median_with_four_non_acked_is_the_largest_acked() {
        let lat = [
            Some(600),
            None,
            Some(100),
            None,
            Some(300),
            None,
            Some(200),
            None,
            Some(500),
            Some(400),
        ];
        assert_eq!(median_ms(&lat), Some(600));
    }

    #[test]
    fn lines_render_exactly() {
        let acked = TtfaSample {
            mono_ms: Some(812),
            wall_ms: Some(-4200),
            probes: 2,
            last: Some(Outcome::Refused(503)),
        };
        assert_eq!(
            ttfa_line(0, &acked),
            "perf ttfa node=0 mono_ms=812 wall_ms=-4200 probes=2 outcome=acked last=refused-503"
        );
        assert_eq!(
            ttfa_line(1, &TtfaSample::none(300)),
            "perf ttfa node=1 mono_ms=none wall_ms=none probes=300 outcome=none last=-"
        );
        let none_after_timeouts = TtfaSample {
            last: Some(Outcome::NoResponse),
            ..TtfaSample::none(7)
        };
        assert_eq!(
            ttfa_line(1, &none_after_timeouts),
            "perf ttfa node=1 mono_ms=none wall_ms=none probes=7 outcome=none last=no-response"
        );
        assert_eq!(ack_start_line(1, 9), "perf ack-start node=1 i=9");
        let refused = AckSample {
            i: 3,
            signal: Signal::Metrics,
            mono_ms: 41,
            wall_ms: -12,
            outcome: Outcome::Refused(503),
        };
        assert_eq!(
            ack_line(0, &refused),
            "perf ack node=0 i=3 signal=metrics mono_ms=41 wall_ms=-12 outcome=refused-503"
        );
        let ok = AckSample {
            i: 4,
            signal: Signal::Logs,
            mono_ms: 731,
            wall_ms: 733,
            outcome: Outcome::Acked,
        };
        assert_eq!(
            ack_line(1, &ok),
            "perf ack node=1 i=4 signal=logs mono_ms=731 wall_ms=733 outcome=acked"
        );
        let partial = AckSample {
            outcome: Outcome::Partial,
            ..ok.clone()
        };
        assert!(ack_line(1, &partial).ends_with(" outcome=partial"));
        let lost = AckSample {
            outcome: Outcome::NoResponse,
            ..ok
        };
        assert!(ack_line(1, &lost).ends_with(" outcome=no-response"));
        assert_eq!(median_line(0, Some(640)), "perf median node=0 mono_ms=640");
        assert_eq!(median_line(1, None), "perf median node=1 mono_ms=inf");
        assert_eq!(
            summary_line(2, 20, true),
            "perf summary ttfa=2 ack=20 recovered=true"
        );
        assert_eq!(
            summary_line(2, 0, false),
            "perf summary ttfa=2 ack=0 recovered=false"
        );
    }

    #[test]
    fn a_backwards_wall_clock_gives_a_negative_wall_ms() {
        let from = Stamp::now();
        let to = Stamp {
            mono: from.mono + Duration::from_millis(250),
            wall: from.wall - Duration::from_millis(1750),
        };
        assert_eq!(interval(from, to), (250, -1750));
    }

    #[test]
    fn stop_faults_states() {
        assert_eq!(
            start_line(StopFaults::Failed),
            ("perf skipped reason=stop-faults-failed".to_string(), false)
        );
        assert_eq!(
            start_line(StopFaults::Unset),
            ("perf t0 stop_faults=unset".to_string(), true)
        );
        assert_eq!(
            start_line(StopFaults::Ok),
            ("perf t0 stop_faults=ok".to_string(), true)
        );
        assert!(
            perf(false)
                .start(StopFaults::Failed, Stamp::now())
                .is_none()
        );
        let s = perf(false)
            .start(StopFaults::Unset, Stamp::now())
            .expect("unset measures");
        assert_eq!((s.ttfa_lines, s.ack_lines), (0, 0));
        assert!(perf(false).start(StopFaults::Ok, Stamp::now()).is_some());
    }

    #[test]
    fn the_variant_is_on_only_for_exactly_1() {
        assert!(enabled(Some("1")));
        for v in [
            None,
            Some(""),
            Some("0"),
            Some("true"),
            Some(" 1"),
            Some("1 "),
            Some("01"),
        ] {
            assert!(!enabled(v), "{v:?}");
        }
        assert!(conflicts(Some("1"), Some("1")));
        assert!(!conflicts(Some("1"), None));
        assert!(!conflicts(Some("1"), Some("0")));
        assert!(!conflicts(None, Some("1")));
        assert!(!conflicts(Some("yes"), Some("1")));
    }

    #[test]
    fn the_required_tsv_names_only_perf_messages() {
        let tsv = include_str!("../../local/perf-required.tsv");
        let always: Vec<&str> = PROPS
            .iter()
            .filter(|p| p.kind == Kind::Always)
            .map(|p| p.message)
            .collect();
        // T1 classes name no Always message; the T2 classes name only Always.
        let t1 = [
            "required",
            "reported",
            "slow-ack-never",
            "slow-ack-required",
            "no-ack-never",
        ];
        let t2 = [
            "always",
            "slow-ack-violated",
            "no-ack-violated",
            "no-ack-unhit",
            "self-check-violated",
        ];
        let mut lines = 0;
        let mut new_lines = 0;
        for line in tsv.lines() {
            if line.starts_with('#') || line.is_empty() {
                continue;
            }
            let (class, msg) = line.split_once('\t').expect("class<TAB>message");
            assert!(t1.contains(&class) || t2.contains(&class), "{class}");
            assert!(
                PROPS.iter().any(|p| p.message == msg),
                "not a perf message: {msg}"
            );
            if t2.contains(&class) {
                assert!(
                    always.contains(&msg),
                    "new line not an Always message: {msg}"
                );
                new_lines += 1;
            } else {
                assert!(
                    !always.contains(&msg),
                    "T1 line names an Always message: {msg}"
                );
            }
            lines += 1;
        }
        assert_eq!(lines, 32);
        assert_eq!(new_lines, 8);
    }

    #[test]
    fn the_catalogue_holds_twenty_one_distinct_messages() {
        let mut m: Vec<&str> = PROPS.iter().map(|p| p.message).collect();
        m.sort_unstable();
        m.dedup();
        assert_eq!(m.len(), 21);
        let count = |k: Kind| PROPS.iter().filter(|p| p.kind == k).count();
        assert_eq!(count(Kind::Sometimes), 16);
        assert_eq!(count(Kind::Reachable), 1);
        assert_eq!(count(Kind::Unreachable), 1);
        assert_eq!(count(Kind::Always), 3);
        // Each kind maps to its macro's arguments: assert type, display type
        // and must-hit.
        assert_eq!(
            kind_args(Kind::Always),
            (AssertType::Always, "Always", true)
        );
        assert_eq!(
            kind_args(Kind::Sometimes),
            (AssertType::Sometimes, "Sometimes", true)
        );
        assert_eq!(
            kind_args(Kind::Reachable),
            (AssertType::Reachability, "Reachable", true)
        );
        assert_eq!(
            kind_args(Kind::Unreachable),
            (AssertType::Reachability, "Unreachable", false)
        );
    }

    fn answer_after(outcome: Outcome, took: Duration) -> Answer {
        let sent = Stamp::now();
        std::thread::sleep(took);
        Answer {
            outcome,
            sent,
            received: Stamp::now(),
        }
    }

    /// An answer whose clocks read `ms` apart on both clocks, from now.
    fn answer_in(outcome: Outcome, ms: u64) -> Answer {
        let sent = Stamp::now();
        Answer {
            outcome,
            sent,
            received: Stamp {
                mono: sent.mono + Duration::from_millis(ms),
                wall: sent.wall + Duration::from_millis(ms),
            },
        }
    }

    #[test]
    fn a_prober_that_fails_twice_then_acks_measures_from_t0() {
        let calls = AtomicU32::new(0);
        let slow = Duration::from_millis(100);
        let t0 = Stamp::now();
        let ack_at = Stamp {
            mono: t0.mono + Duration::from_millis(777),
            wall: t0.wall + Duration::from_millis(777),
        };
        let s = run_prober(
            |_| {
                if calls.fetch_add(1, Ordering::SeqCst) < 2 {
                    answer_after(Outcome::Refused(503), slow)
                } else {
                    Answer {
                        outcome: Outcome::Acked,
                        sent: Stamp::now(),
                        received: ack_at,
                    }
                }
            },
            t0,
            Duration::from_millis(150),
            Duration::from_millis(500),
            Duration::from_secs(10),
        );
        assert_eq!(s.probes, 3);
        assert_eq!(s.last, Some(Outcome::Refused(503)));
        assert_eq!(
            s.mono_ms,
            Some(777),
            "measured from t0 to the ack's receipt"
        );
        assert_eq!(s.wall_ms, Some(777));
        assert!(ttfa_line(0, &s).contains(" outcome=acked last=refused-503"));
    }

    #[test]
    fn a_prober_that_never_acks_ends_at_the_deadline_with_nothing_running() {
        let in_flight = AtomicUsize::new(0);
        let t0 = Stamp::now();
        let timeout = Duration::from_millis(100);
        let s = run_prober(
            |t| {
                in_flight.fetch_add(1, Ordering::SeqCst);
                let a = answer_after(Outcome::NoResponse, t);
                in_flight.fetch_sub(1, Ordering::SeqCst);
                a
            },
            t0,
            Duration::from_millis(50),
            timeout,
            Duration::from_millis(300),
        );
        let took = t0.mono.elapsed();
        assert_eq!(s.mono_ms, None);
        assert_eq!(s.wall_ms, None);
        assert_eq!(s.last, Some(Outcome::NoResponse));
        assert!(s.probes >= 3, "probes {}", s.probes);
        assert_eq!(
            in_flight.load(Ordering::SeqCst),
            0,
            "a probe outlived the prober"
        );
        assert!(took >= Duration::from_millis(300), "{took:?}");
    }

    #[test]
    fn a_prober_never_has_more_than_timeout_over_cadence_in_flight() {
        let in_flight = AtomicUsize::new(0);
        let max_seen = AtomicUsize::new(0);
        let cadence = Duration::from_millis(20);
        let timeout = Duration::from_millis(100);
        let s = run_prober(
            |_| {
                let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                max_seen.fetch_max(now, Ordering::SeqCst);
                // Every send outlives three timeouts, so an uncapped prober
                // would reach about 15 in flight.
                let a = answer_after(Outcome::NoResponse, timeout * 3);
                in_flight.fetch_sub(1, Ordering::SeqCst);
                a
            },
            Stamp::now(),
            cadence,
            timeout,
            Duration::from_millis(600),
        );
        let max = max_seen.load(Ordering::SeqCst);
        assert!(max <= 5, "{max} probes in flight");
        assert!(max >= 2, "probes never overlapped: {max}");
        assert!(s.probes > 5, "probes {}", s.probes);
        assert_eq!(in_flight.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn ack_probes_alternate_signals_and_time_the_call_alone() {
        let mut lines = Vec::new();
        let mut n = 0;
        let samples = run_ack_probes(
            1,
            ACK_PROBES,
            Duration::from_secs(15),
            false,
            |_, _| {
                n += 1;
                // Time spent before the call is not part of the interval.
                std::thread::sleep(Duration::from_millis(5));
                let outcome = if n == 4 {
                    Outcome::Refused(503)
                } else {
                    Outcome::Acked
                };
                answer_in(outcome, 3)
            },
            |l| lines.push(l),
        );
        assert_eq!(samples.len(), 10);
        assert_eq!(lines.len(), 20);
        assert_eq!(lines[0], "perf ack-start node=1 i=0");
        assert!(lines[1].starts_with("perf ack node=1 i=0 signal=logs mono_ms="));
        assert!(lines[3].starts_with("perf ack node=1 i=1 signal=metrics "));
        assert!(lines[7].ends_with(" outcome=refused-503"));
        for (i, s) in samples.iter().enumerate() {
            assert_eq!(s.i as usize, i);
            assert_eq!(s.signal, ack_signal(s.i));
            assert_eq!(s.mono_ms, 3, "the interval is sent to received alone");
            assert_eq!(s.wall_ms, 3);
        }
    }

    #[cfg(unix)]
    mod stop_faults_mapping {
        use super::super::{StopFaults, stop_faults};
        use std::cell::Cell;
        use std::os::unix::ffi::OsStringExt;
        use std::os::unix::process::ExitStatusExt;
        use std::process::ExitStatus;

        #[test]
        fn absent_is_unset_and_runs_nothing() {
            let ran = Cell::new(false);
            let got = stop_faults(Err(std::env::VarError::NotPresent), |_| {
                ran.set(true);
                Ok(ExitStatus::from_raw(0))
            });
            assert_eq!(got, StopFaults::Unset);
            assert!(!ran.get());
        }

        #[test]
        fn not_unicode_is_failed_and_runs_nothing() {
            let ran = Cell::new(false);
            let raw = std::ffi::OsString::from_vec(vec![0xff, 0xfe]);
            let got = stop_faults(Err(std::env::VarError::NotUnicode(raw)), |_| {
                ran.set(true);
                Ok(ExitStatus::from_raw(0))
            });
            assert_eq!(got, StopFaults::Failed);
            assert!(!ran.get(), "a non-UTF-8 command was run");
        }

        #[test]
        fn a_spawn_error_is_failed() {
            let got = stop_faults(Ok("/no/such/cmd".to_string()), |cmd| {
                assert_eq!(cmd, "/no/such/cmd");
                Err(std::io::Error::from(std::io::ErrorKind::NotFound))
            });
            assert_eq!(got, StopFaults::Failed);
        }

        #[test]
        fn a_non_zero_exit_is_failed() {
            // A wait status of 1 << 8 is exit code 1.
            let got = stop_faults(Ok("stop".to_string()), |_| Ok(ExitStatus::from_raw(1 << 8)));
            assert_eq!(got, StopFaults::Failed);
        }

        #[test]
        fn a_zero_exit_is_ok() {
            let got = stop_faults(Ok("stop".to_string()), |_| Ok(ExitStatus::from_raw(0)));
            assert_eq!(got, StopFaults::Ok);
        }
    }
}
