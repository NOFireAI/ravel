//! Shared machinery for the two report-only T13 experiments (issue #3711,
//! ADR-3692 work package 4, Plan row T13): [`crate::structural_fetch`]
//! (Lance-style structural decode for a journey row fetch) and
//! [`crate::fulltext_sidecar`] (a LogCloud-style full-text sidecar for cold
//! logs).
//!
//! Both follow the T12 pattern ([`crate::string_fingerprint`]): one
//! pre-registration file (`fixtures/t13/subjects.toml`) names every subject,
//! its arm, its corpus by sha256, every figure-changing parameter, and for
//! every figure the report prints a prediction with its miss band, the reason
//! it is absent, or the reason it cannot be predicted. A parameter given on
//! the command line is a check against the registered value and is refused
//! before the corpus is read. Nothing here ships: no reader or writer outside
//! this crate changes.

use std::collections::{BTreeMap, HashSet};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::Parser;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{InstrumentedStore, ObjectStoreBackend, StoreMetrics};
use serde::{Deserialize, Serialize};

pub use crate::string_fingerprint::spec::Expect;
pub use crate::string_fingerprint::{Checked, Figure, check_figures};
use crate::string_fingerprint_report::{command_out, file_digest, git_head, mem_total_kib};

/// Measure: written, no structural problem, every figure in its band.
/// `--decide`: overall GO.
pub const EXIT_OK: u8 = 0;
/// An error, or a report with a structural problem; with `--decide`, a report
/// that could not be read or a set of reports that is not a decision.
pub const EXIT_ERROR: u8 = 1;
/// Measure: written with no structural problem and at least one MISS.
pub const EXIT_MISS: u8 = 2;
/// `--decide`: a complete set of reports whose overall decision is NO-GO.
pub const EXIT_NO_GO: u8 = 3;

pub const EXIT_CODES: &str = "Exit codes:
  0  measure: the report was written, no structural problem, every figure in its band
     --decide: overall GO
  1  an error, or a report with a structural problem (a missing, duplicate,
     unexpected or not-pre-registered figure; the report is still written);
     --decide: a report could not be read, or the reports are not a decision
     (a subject missing or reported twice, a report with a problem, or a
     subject that is not pre-registered for this arm)
  2  measure: the report was written with no structural problem and at least
     one figure outside its band (MISS)
  3  --decide: every pre-registered subject of the arm reported once without a
     problem, and the overall decision is NO-GO";

pub const CPU_CLOCK: &str = "CLOCK_THREAD_CPUTIME_ID of the report thread; \
the measurement runs on a current-thread tokio runtime, so every decode the \
production fetcher does lands on that thread";

/// The two experiments.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Arm {
    StructuralFetch,
    FulltextSidecar,
}

/// One GO condition: the figure is reported exactly once and is `<= max`.
/// An infinite `max` is "reported at all".
#[derive(Clone, Copy, Debug)]
pub struct Rule {
    pub key: &'static str,
    pub max: f64,
}

impl Arm {
    pub fn name(self) -> &'static str {
        match self {
            Arm::StructuralFetch => "structural_fetch",
            Arm::FulltextSidecar => "fulltext_sidecar",
        }
    }

    pub fn params(self) -> &'static [&'static str] {
        match self {
            Arm::StructuralFetch => crate::structural_fetch::PARAMS,
            Arm::FulltextSidecar => crate::fulltext_sidecar::PARAMS,
        }
    }

    pub fn figures(self) -> &'static [&'static str] {
        match self {
            Arm::StructuralFetch => crate::structural_fetch::FIGURES,
            Arm::FulltextSidecar => crate::fulltext_sidecar::FIGURES,
        }
    }

    pub fn rules(self) -> &'static [Rule] {
        match self {
            Arm::StructuralFetch => crate::structural_fetch::RULES,
            Arm::FulltextSidecar => crate::fulltext_sidecar::RULES,
        }
    }

    pub fn decision_rule(self) -> String {
        let parts: Vec<String> = self
            .rules()
            .iter()
            .map(|r| {
                if r.max.is_infinite() {
                    format!("{} reported", r.key)
                } else {
                    format!("{} <= {}", r.key, r.max)
                }
            })
            .collect();
        format!(
            "Per subject: GO when {}, each reported exactly once. Overall: GO when \
             every pre-registered subject of the arm is GO.",
            parts.join(" and ")
        )
    }
}

/// The pre-registration file.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct T13Spec {
    pub schema: u32,
    pub subject: Vec<SubjectSpec>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubjectSpec {
    pub name: String,
    pub arm: Arm,
    /// The corpus file name, for the reader; the sha256 is what is checked.
    pub corpus: String,
    /// Where the corpus comes from.
    pub source: String,
    pub corpus_sha256: String,
    pub params: BTreeMap<String, f64>,
    pub figures: BTreeMap<String, Expect>,
}

fn exact_set(what: &str, got: Vec<&str>, want: &[&str]) -> Result<(), String> {
    let got_set: HashSet<&str> = got.iter().copied().collect();
    let want_set: HashSet<&str> = want.iter().copied().collect();
    let mut missing: Vec<&str> = want_set.difference(&got_set).copied().collect();
    let mut unknown: Vec<&str> = got_set.difference(&want_set).copied().collect();
    if missing.is_empty() && unknown.is_empty() {
        return Ok(());
    }
    missing.sort_unstable();
    unknown.sort_unstable();
    Err(format!("{what}: missing {missing:?}, unknown {unknown:?}"))
}

pub fn parse(text: &str) -> Result<T13Spec, String> {
    let spec: T13Spec = toml::from_str(text).map_err(|e| format!("spec: {e}"))?;
    if spec.schema != 1 {
        return Err(format!("spec: schema {} is not 1", spec.schema));
    }
    if spec.subject.is_empty() {
        return Err("spec: no subject".into());
    }
    let mut names = HashSet::new();
    for s in &spec.subject {
        if !names.insert(s.name.as_str()) {
            return Err(format!("spec: subject {} listed twice", s.name));
        }
        if s.corpus_sha256.len() != 64
            || !s
                .corpus_sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(format!("{}: corpus_sha256 is not 64 lowercase hex", s.name));
        }
        exact_set(
            &format!("{} params", s.name),
            s.params.keys().map(String::as_str).collect(),
            s.arm.params(),
        )?;
        for (k, v) in &s.params {
            if !v.is_finite() {
                return Err(format!("{}: param {k} is not finite", s.name));
            }
        }
        exact_set(
            &format!("{} figures", s.name),
            s.figures.keys().map(String::as_str).collect(),
            s.arm.figures(),
        )?;
    }
    Ok(spec)
}

pub fn read_spec(path: &Path) -> Result<T13Spec, String> {
    let text =
        std::fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    parse(&text)
}

impl T13Spec {
    pub fn subject(&self, name: &str) -> Option<&SubjectSpec> {
        self.subject.iter().find(|s| s.name == name)
    }

    pub fn arm_subjects(&self, arm: Arm) -> Vec<String> {
        self.subject
            .iter()
            .filter(|s| s.arm == arm)
            .map(|s| s.name.clone())
            .collect()
    }
}

impl SubjectSpec {
    pub fn param(&self, key: &str) -> Result<f64, String> {
        self.params
            .get(key)
            .copied()
            .ok_or_else(|| format!("{}: no param {key}", self.name))
    }

    /// A parameter that must be a non-negative integer.
    pub fn param_u64(&self, key: &str) -> Result<u64, String> {
        let v = self.param(key)?;
        if v < 0.0 || v.fract() != 0.0 || v > 2f64.powi(53) {
            return Err(format!(
                "{}: param {key}={v} is not a whole count",
                self.name
            ));
        }
        Ok(v as u64)
    }

    pub fn param_usize(&self, key: &str) -> Result<usize, String> {
        usize::try_from(self.param_u64(key)?).map_err(|e| format!("{}: {key}: {e}", self.name))
    }
}

/// `KEY=VALUE` from the command line.
pub fn parse_param(s: &str) -> Result<(String, f64), String> {
    let (k, v) = s
        .split_once('=')
        .ok_or_else(|| format!("--param {s}: expected KEY=VALUE"))?;
    let v: f64 = v.trim().parse().map_err(|e| format!("--param {s}: {e}"))?;
    Ok((k.trim().to_string(), v))
}

/// Refuses a given parameter that is not registered or differs from the
/// registered value (compared bit for bit).
pub fn refuse_param_drift(spec: &SubjectSpec, given: &[(String, f64)]) -> Result<(), String> {
    for (k, v) in given {
        let want = spec
            .params
            .get(k)
            .ok_or_else(|| format!("--param {k}: not a parameter of {}", spec.name))?;
        if want.to_bits() != v.to_bits() {
            return Err(format!(
                "--param {k}={v} differs from the pre-registered {want} for {}; refusing before \
                 the corpus is read",
                spec.name
            ));
        }
    }
    Ok(())
}

/// Up to `cap` lines of a text file, read lossily as UTF-8 (a byte sequence
/// that is not UTF-8 becomes U+FFFD), with the trailing newline (and a CR
/// before it) removed.
pub fn read_lines(path: &Path, cap: usize) -> Result<Vec<String>, String> {
    let file = std::fs::File::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
    let mut reader = BufReader::with_capacity(1 << 20, file);
    let mut out = Vec::new();
    let mut buf = Vec::new();
    while out.len() < cap {
        buf.clear();
        let n = reader
            .read_until(b'\n', &mut buf)
            .map_err(|e| format!("read {}: {e}", path.display()))?;
        if n == 0 {
            break;
        }
        if buf.last() == Some(&b'\n') {
            buf.pop();
        }
        if buf.last() == Some(&b'\r') {
            buf.pop();
        }
        out.push(String::from_utf8_lossy(&buf).into_owned());
    }
    Ok(out)
}

/// What every report records about the run.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Stamp {
    pub git: String,
    pub arch: String,
    pub nproc: usize,
    pub mem_total_kib: u64,
    pub corpus_file: String,
    pub corpus_sha256: String,
    pub corpus_bytes: u64,
    pub cpu_clock: String,
}

pub fn stamp(corpus: &Path, corpus_sha256: String, corpus_bytes: u64) -> Stamp {
    Stamp {
        git: git_head(),
        arch: command_out("uname", &["-m"]),
        nproc: std::thread::available_parallelism().map_or(1, usize::from),
        mem_total_kib: mem_total_kib(),
        corpus_file: corpus
            .file_name()
            .map_or_else(String::new, |n| n.to_string_lossy().into_owned()),
        corpus_sha256,
        corpus_bytes,
        cpu_clock: CPU_CLOCK.to_string(),
    }
}

/// One subject's verdict.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Decision {
    pub subject: String,
    pub go: bool,
    pub line: String,
}

fn one(figures: &[Figure], key: &str) -> Option<f64> {
    let vs: Vec<f64> = figures
        .iter()
        .filter(|f| f.key == key)
        .map(|f| f.value)
        .collect();
    match vs.as_slice() {
        [v] => Some(*v),
        _ => None,
    }
}

/// The per-subject rule of [`Arm::decision_rule`].
pub fn decide(arm: Arm, subject: &str, figures: &[Figure]) -> Decision {
    let mut go = true;
    let mut parts = Vec::new();
    for r in arm.rules() {
        let (ok, shown) = match one(figures, r.key) {
            Some(v) if r.max.is_infinite() => (v.is_finite(), format!("{v:.4}")),
            Some(v) => (v <= r.max, format!("{v:.4}<={}", r.max)),
            None => (false, "missing".to_string()),
        };
        go &= ok;
        parts.push(format!(
            "{}={shown} {}",
            r.key,
            if ok { "ok" } else { "FAIL" }
        ));
    }
    Decision {
        subject: subject.to_string(),
        go,
        line: format!(
            "{} decision {subject}: {} ({})",
            arm.name(),
            if go { "GO" } else { "NO-GO" },
            parts.join(", ")
        ),
    }
}

/// One subject's written report.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Report {
    pub arm: Arm,
    pub subject: String,
    pub stamp: Stamp,
    pub params: BTreeMap<String, f64>,
    pub decision_rule: String,
    pub figures: Vec<Figure>,
    pub expect: BTreeMap<String, Expect>,
    pub checked: Checked,
    pub decision: Decision,
    pub notes: Vec<String>,
}

impl Report {
    pub fn new(spec: &SubjectSpec, stamp: Stamp, figures: Vec<Figure>, notes: Vec<String>) -> Self {
        let checked = check_figures(&figures, &spec.figures);
        let decision = decide(spec.arm, &spec.name, &figures);
        Report {
            arm: spec.arm,
            subject: spec.name.clone(),
            stamp,
            params: spec.params.clone(),
            decision_rule: spec.arm.decision_rule(),
            figures,
            expect: spec.figures.clone(),
            checked,
            decision,
            notes,
        }
    }

    pub fn lines(&self) -> Vec<String> {
        let p = self.arm.name();
        let s = &self.stamp;
        let mut out = vec![format!(
            "{p} stamp git={} arch={} nproc={} mem_total_kib={} corpus={} sha256={} bytes={}",
            s.git, s.arch, s.nproc, s.mem_total_kib, s.corpus_file, s.corpus_sha256, s.corpus_bytes
        )];
        out.push(format!("{p} cpu_clock {}", s.cpu_clock));
        let params: Vec<String> = self
            .params
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect();
        out.push(format!("{p} params {}", params.join(" ")));
        for (key, e) in &self.expect {
            let got: Vec<f64> = self
                .figures
                .iter()
                .filter(|f| &f.key == key)
                .map(|f| f.value)
                .collect();
            let value = match got.as_slice() {
                [] => "-".to_string(),
                [v] => format!("{v}"),
                many => format!("{many:?}"),
            };
            let verdict = match (e, got.as_slice()) {
                (Expect::Predicted { predict, band }, [v]) => format!(
                    "predict={predict} band=[{}, {}] {}",
                    band[0],
                    band[1],
                    if band[0] <= *v && *v <= band[1] {
                        "IN"
                    } else {
                        "MISS"
                    }
                ),
                (Expect::Predicted { predict, band }, _) => {
                    format!("predict={predict} band=[{}, {}] PROBLEM", band[0], band[1])
                }
                (Expect::Unpredicted(why), _) => format!("unpredicted ({why})"),
                (Expect::Absent(why), _) => format!("absent ({why})"),
            };
            out.push(format!("{p} {} {key}={value} {verdict}", self.subject));
        }
        for n in &self.notes {
            out.push(format!("{p} note {n}"));
        }
        out.push(self.decision.line.clone());
        out
    }
}

/// The verdict and figure-check counts read back from a written report.
#[derive(Clone, Debug, PartialEq)]
pub struct SavedDecision {
    pub arm: Arm,
    pub decision: Decision,
    pub problems: usize,
    pub misses: usize,
}

pub fn saved_decision(json: &str) -> Result<SavedDecision, String> {
    #[derive(Deserialize)]
    struct Saved {
        arm: Arm,
        decision: Decision,
        checked: Checked,
    }
    let saved: Saved = serde_json::from_str(json).map_err(|e| format!("report: {e}"))?;
    Ok(SavedDecision {
        arm: saved.arm,
        decision: saved.decision,
        problems: saved.checked.problems.len(),
        misses: saved.checked.misses.len(),
    })
}

/// Whether `saved` is a decision at all: every subject of `subjects` reported
/// exactly once, for `arm`, without a figure problem, and nothing else.
pub fn complete(arm: Arm, subjects: &[String], saved: &[SavedDecision]) -> bool {
    !subjects.is_empty()
        && subjects.iter().all(|s| {
            let mine: Vec<&SavedDecision> =
                saved.iter().filter(|d| &d.decision.subject == s).collect();
            matches!(mine.as_slice(), [d] if d.problems == 0 && d.arm == arm)
        })
        && saved
            .iter()
            .all(|d| d.arm == arm && subjects.contains(&d.decision.subject))
}

/// GO when every subject of `subjects` is GO.
pub fn overall(arm: Arm, subjects: &[String], saved: &[SavedDecision]) -> Decision {
    let mut go = !subjects.is_empty();
    let mut parts = Vec::new();
    for s in subjects {
        let mine: Vec<&SavedDecision> = saved.iter().filter(|d| &d.decision.subject == s).collect();
        let part = match mine.as_slice() {
            [] => "missing".to_string(),
            [d] if d.arm != arm => format!("a {} report", d.arm.name()),
            [d] if d.problems > 0 => format!(
                "{} but {} figure problems (not a result)",
                if d.decision.go { "GO" } else { "NO-GO" },
                d.problems
            ),
            [d] => (if d.decision.go { "GO" } else { "NO-GO" }).to_string(),
            many => format!("{} reports", many.len()),
        };
        go &= part == "GO";
        parts.push(format!("{s}={part}"));
    }
    for d in saved {
        if !subjects.contains(&d.decision.subject) {
            go = false;
            parts.push(format!("{}=not pre-registered", d.decision.subject));
        }
    }
    Decision {
        subject: "overall".to_string(),
        go,
        line: format!(
            "{} decision overall: {} ({})",
            arm.name(),
            if go { "GO" } else { "NO-GO" },
            parts.join(", ")
        ),
    }
}

pub fn report_code(checked: &Checked) -> u8 {
    if !checked.problems.is_empty() {
        EXIT_ERROR
    } else if !checked.misses.is_empty() {
        EXIT_MISS
    } else {
        EXIT_OK
    }
}

/// The `--decide` output and its exit code: a stamp line naming the git
/// commit and every report read with its sha256, each subject's decision line
/// and its miss and problem counts, then the overall line.
pub fn decide_overall(
    arm: Arm,
    spec_path: &Path,
    reports: &[PathBuf],
) -> Result<(Vec<String>, u8), String> {
    let spec = read_spec(spec_path)?;
    let mut read = Vec::new();
    let mut saved = Vec::new();
    for path in reports {
        let json =
            std::fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
        let (sha256, _) = file_digest(path)?;
        let name = path
            .file_name()
            .map_or_else(String::new, |n| n.to_string_lossy().into_owned());
        read.push(format!("{name}:{sha256}"));
        saved.push(saved_decision(&json).map_err(|e| format!("{}: {e}", path.display()))?);
    }
    let mut lines = vec![format!(
        "{} decide stamp git={} reports={}",
        arm.name(),
        git_head(),
        read.join(",")
    )];
    for s in &saved {
        lines.push(s.decision.line.clone());
        lines.push(format!(
            "{} decide subject={} misses={} problems={}",
            arm.name(),
            s.decision.subject,
            s.misses,
            s.problems
        ));
    }
    let names = spec.arm_subjects(arm);
    let d = overall(arm, &names, &saved);
    lines.push(d.line);
    let code = if !complete(arm, &names, &saved) {
        EXIT_ERROR
    } else if d.go {
        EXIT_OK
    } else {
        EXIT_NO_GO
    };
    Ok((lines, code))
}

/// The command line both report bins share.
#[derive(Parser, Debug)]
#[command(after_help = EXIT_CODES)]
pub struct Args {
    /// The pre-registration file.
    #[arg(long, default_value = "crates/ravel-bench/fixtures/t13/subjects.toml")]
    pub spec: PathBuf,
    /// The subject name from the pre-registration file.
    #[arg(long)]
    pub subject: Option<String>,
    /// The corpus: one log line per line of this text file.
    #[arg(long, conflicts_with = "decide")]
    pub corpus_lines: Option<PathBuf>,
    /// Where to write the JSON report.
    #[arg(long)]
    pub out: Option<PathBuf>,
    /// `KEY=VALUE`: a check that the registered parameter has this value; a
    /// different value or an unknown key is refused before the corpus is read.
    #[arg(long = "param", value_name = "KEY=VALUE")]
    pub params: Vec<String>,
    /// Print the corpus facts a pre-registration needs (lines, bytes, sha256)
    /// and exit.
    #[arg(long, conflicts_with_all = ["decide", "subject", "out"])]
    pub describe: bool,
    /// Print the arm's overall decision over these written reports and exit.
    #[arg(long, num_args = 1..)]
    pub decide: Vec<PathBuf>,
}

/// What an arm's measurement returns.
pub struct Measured {
    pub figures: Vec<Figure>,
    pub notes: Vec<String>,
}

/// An arm's measurement over the subject's corpus file, whose sha256 has
/// already been checked.
pub type MeasureFn = fn(&SubjectSpec, &Path) -> Result<Measured, String>;

fn describe(path: &Path) -> Result<String, String> {
    let (sha256, bytes) = file_digest(path)?;
    let file = std::fs::File::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
    let mut reader = BufReader::with_capacity(1 << 20, file);
    let mut lines = 0u64;
    let mut buf = Vec::new();
    loop {
        buf.clear();
        let n = reader
            .read_until(b'\n', &mut buf)
            .map_err(|e| format!("read {}: {e}", path.display()))?;
        if n == 0 {
            break;
        }
        lines += 1;
    }
    Ok(format!(
        "corpus={} lines={lines} bytes={bytes} sha256={sha256}",
        path.display()
    ))
}

fn measure(arm: Arm, args: &Args, run: MeasureFn) -> Result<u8, String> {
    let (Some(subject), Some(out), Some(corpus)) = (
        args.subject.clone(),
        args.out.clone(),
        args.corpus_lines.clone(),
    ) else {
        return Err("--subject, --corpus-lines and --out are required".into());
    };
    let spec = read_spec(&args.spec)?;
    let sub = spec
        .subject(&subject)
        .ok_or_else(|| format!("subject {subject} is not pre-registered"))?;
    if sub.arm != arm {
        return Err(format!(
            "subject {subject} is a {} subject, not {}",
            sub.arm.name(),
            arm.name()
        ));
    }
    let given: Vec<(String, f64)> = args
        .params
        .iter()
        .map(|p| parse_param(p))
        .collect::<Result<_, _>>()?;
    refuse_param_drift(sub, &given)?;
    let (sha256, bytes) = file_digest(&corpus)?;
    if sha256 != sub.corpus_sha256 {
        return Err(format!(
            "{}: sha256 {sha256} is not the pre-registered {} for {subject}",
            corpus.display(),
            sub.corpus_sha256
        ));
    }
    let st = stamp(&corpus, sha256, bytes);
    let m = run(sub, &corpus)?;
    let report = Report::new(sub, st, m.figures, m.notes);
    let json = serde_json::to_string_pretty(&report).map_err(|e| format!("serialize: {e}"))?;
    std::fs::write(&out, json + "\n").map_err(|e| format!("write {}: {e}", out.display()))?;
    for line in report.lines() {
        println!("{line}");
    }
    for p in &report.checked.problems {
        eprintln!("{}: problem: {p}", arm.name());
    }
    for m in &report.checked.misses {
        println!("{} miss {m}", arm.name());
    }
    Ok(report_code(&report.checked))
}

/// One bin invocation.
pub fn exec(arm: Arm, args: &Args, run: MeasureFn) -> Result<u8, String> {
    if !args.decide.is_empty() {
        let (lines, code) = decide_overall(arm, &args.spec, &args.decide)?;
        for line in lines {
            println!("{line}");
        }
        Ok(code)
    } else if args.describe {
        let corpus = args
            .corpus_lines
            .as_ref()
            .ok_or("--describe needs --corpus-lines")?;
        println!("{}", describe(corpus)?);
        Ok(EXIT_OK)
    } else {
        measure(arm, args, run)
    }
}

/// Thread CPU seconds.
pub fn thread_cpu_s() -> f64 {
    let ts = rustix::time::clock_gettime(rustix::time::ClockId::ThreadCPUTime);
    ts.tv_sec as f64 + ts.tv_nsec as f64 / 1e9
}

/// The median of `values` (the lower middle for an even count); `None` when
/// empty.
pub fn median(values: &[f64]) -> Option<f64> {
    let mut v = values.to_vec();
    v.sort_by(f64::total_cmp);
    v.get(v.len().checked_sub(1)? / 2).copied()
}

/// splitmix64: the deterministic generator both arms draw from.
pub fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// A remote store for one measurement: `MemoryStore` behind the production
/// `InstrumentedStore` (which counts every GET, its returned bytes, and every
/// HEAD), behind `DelayedGetStore`, which sleeps `latency` before each GET. The
/// sleep is outside the counter, so every counted GET pays it.
pub fn remote_store(
    latency: std::time::Duration,
) -> (Arc<dyn ObjectStoreBackend>, Arc<StoreMetrics>) {
    let instrumented = InstrumentedStore::new(MemoryStore::new());
    let metrics = instrumented.metrics();
    let store: Arc<dyn ObjectStoreBackend> =
        Arc::new(crate::harness::DelayedGetStore::new(instrumented, latency));
    (store, metrics)
}

/// `(gets, bytes the gets returned, heads)` so far.
pub fn store_counts(metrics: &StoreMetrics) -> (u64, u64, u64) {
    let s = metrics.snapshot();
    (s.get.calls, s.get.bytes, s.head.calls)
}

/// A `SegmentRef` for one bench object, built as the fetcher's own tests
/// build one.
pub fn segment_ref(
    key: &str,
    bytes: &[u8],
    rows: u64,
    ts: (i64, i64),
) -> ravel_catalog::SegmentRef {
    ravel_catalog::SegmentRef {
        data_object_key: key.to_string(),
        object_size: bytes.len() as u64,
        min_event_ts_ns: ts.0,
        max_event_ts_ns: ts.1,
        ingest_hour_bucket: 0,
        sample_count: rows,
        series_count: 0,
        shard: 0,
        content_hash: *blake3::hash(bytes).as_bytes(),
        writer_id: uuid::Uuid::from_u128(1),
        writer_epoch: 1,
        writer_seq: 1,
        created_unix_ns: 0,
        level: ravel_catalog::SegmentLevel::L0,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        declared_column_stats: Default::default(),
    }
}

/// The tenant every bench object is written under.
pub const TENANT: [u8; 16] = [0x54; 16];

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    fn fig(key: &str, value: f64) -> Figure {
        Figure {
            key: key.to_string(),
            value,
        }
    }

    #[test]
    fn structural_decision_is_go_only_when_every_bar_holds() {
        let at_bars = vec![
            fig("ratio/bytes", 0.25),
            fig("ratio/gets", 1.5),
            fig("ratio/wall", 1.0),
        ];
        let d = decide(Arm::StructuralFetch, "s", &at_bars);
        assert!(d.go, "{}", d.line);
        assert_eq!(
            d.line,
            "structural_fetch decision s: GO (ratio/bytes=0.2500<=0.25 ok, \
             ratio/gets=1.5000<=1.5 ok, ratio/wall=1.0000<=1 ok)"
        );
        for (i, over) in [0.2501, 1.5001, 1.0001].iter().enumerate() {
            let mut f = at_bars.clone();
            f[i].value = *over;
            assert!(!decide(Arm::StructuralFetch, "s", &f).go, "{i}");
        }
        // A figure reported twice is no verdict at all.
        let mut twice = at_bars.clone();
        twice.push(fig("ratio/gets", 1.0));
        let d = decide(Arm::StructuralFetch, "s", &twice);
        assert!(!d.go);
        assert!(d.line.contains("ratio/gets=missing FAIL"), "{}", d.line);
    }

    #[test]
    fn sidecar_decision_needs_the_deletion_path_costed() {
        let all = vec![
            fig("rare/bytes_ratio_100gb", 0.1),
            fig("sidecar/over_postings", 1.2),
            fig("build/cpu_ratio", 1.5),
            fig("delete/rebuild_cpu_over_compaction", 0.4),
            fig("delete/bytes_over_object", 0.3),
            fig("delete/residual_terms", 0.0),
        ];
        assert!(decide(Arm::FulltextSidecar, "s", &all).go);
        for missing in 0..all.len() {
            let mut f = all.clone();
            f.remove(missing);
            assert!(!decide(Arm::FulltextSidecar, "s", &f).go, "{missing}");
        }
        let mut residual = all.clone();
        residual[5].value = 1.0;
        assert!(!decide(Arm::FulltextSidecar, "s", &residual).go);
        let mut over = all.clone();
        over[0].value = 0.1001;
        assert!(!decide(Arm::FulltextSidecar, "s", &over).go);
    }

    fn saved(arm: Arm, subject: &str, go: bool, problems: usize) -> SavedDecision {
        SavedDecision {
            arm,
            decision: Decision {
                subject: subject.into(),
                go,
                line: String::new(),
            },
            problems,
            misses: 0,
        }
    }

    #[test]
    fn overall_and_complete_follow_the_arm() {
        let names = vec!["a".to_string(), "b".to_string()];
        let arm = Arm::FulltextSidecar;
        let both = vec![saved(arm, "a", true, 0), saved(arm, "b", true, 0)];
        assert!(complete(arm, &names, &both));
        assert!(overall(arm, &names, &both).go);
        let one_no = vec![saved(arm, "a", true, 0), saved(arm, "b", false, 0)];
        assert!(complete(arm, &names, &one_no));
        let d = overall(arm, &names, &one_no);
        assert!(!d.go);
        assert_eq!(
            d.line,
            "fulltext_sidecar decision overall: NO-GO (a=GO, b=NO-GO)"
        );
        // A report from the other arm under a registered name is not this
        // arm's result.
        let wrong_arm = vec![
            saved(arm, "a", true, 0),
            saved(Arm::StructuralFetch, "b", true, 0),
        ];
        assert!(!complete(arm, &names, &wrong_arm));
        assert!(!overall(arm, &names, &wrong_arm).go);
        let problem = vec![saved(arm, "a", true, 0), saved(arm, "b", true, 1)];
        assert!(!complete(arm, &names, &problem));
        assert!(!overall(arm, &names, &problem).go);
        assert!(!complete(arm, &names, &both[..1]));
    }

    const SPEC: &str = r#"
schema = 1
[[subject]]
name = "s"
arm = "structural_fetch"
corpus = "c.log"
source = "test"
corpus_sha256 = "0000000000000000000000000000000000000000000000000000000000000000"
"#;

    fn full_spec(drop_param: Option<&str>, extra_figure: Option<&str>) -> String {
        let mut text = SPEC.to_string();
        text.push_str("[subject.params]\n");
        for p in crate::structural_fetch::PARAMS {
            if Some(*p) != drop_param {
                text.push_str(&format!("{p} = 1\n"));
            }
        }
        text.push_str("[subject.figures]\n");
        for f in crate::structural_fetch::FIGURES {
            text.push_str(&format!("\"{f}\" = {{ unpredicted = \"test\" }}\n"));
        }
        if let Some(f) = extra_figure {
            text.push_str(&format!("\"{f}\" = {{ predict = 1, band = [0, 2] }}\n"));
        }
        text
    }

    #[test]
    fn the_spec_must_name_exactly_the_arms_params_and_figures() {
        let spec = parse(&full_spec(None, None)).expect("spec");
        assert_eq!(spec.arm_subjects(Arm::StructuralFetch), vec!["s"]);
        let e = parse(&full_spec(Some("runs"), None)).expect_err("missing param");
        assert!(e.contains("missing [\"runs\"]"), "{e}");
        let e = parse(&full_spec(None, Some("ratio/extra"))).expect_err("unknown figure");
        assert!(e.contains("unknown [\"ratio/extra\"]"), "{e}");
    }

    #[test]
    fn a_drifted_or_unknown_param_is_refused() {
        let spec = parse(&full_spec(None, None)).expect("spec");
        let sub = &spec.subject[0];
        assert!(refuse_param_drift(sub, &[("runs".into(), 1.0)]).is_ok());
        let e = refuse_param_drift(sub, &[("runs".into(), 2.0)]).expect_err("drift");
        assert!(e.contains("differs from the pre-registered 1"), "{e}");
        assert!(refuse_param_drift(sub, &[("nope".into(), 1.0)]).is_err());
        assert_eq!(parse_param("runs=5").expect("param"), ("runs".into(), 5.0));
        assert!(parse_param("runs").is_err());
    }

    #[test]
    fn each_arms_report_is_in_band_at_the_edges_and_misses_past_them() {
        for arm in [Arm::StructuralFetch, Arm::FulltextSidecar] {
            let banded = arm.figures()[0];
            let expect: BTreeMap<String, Expect> = arm
                .figures()
                .iter()
                .map(|f| {
                    let e = if *f == banded {
                        Expect::Predicted {
                            predict: 10.0,
                            band: [8.0, 12.0],
                        }
                    } else {
                        Expect::Unpredicted("test".into())
                    };
                    (f.to_string(), e)
                })
                .collect();
            let with = |v: f64| -> Vec<Figure> {
                arm.figures()
                    .iter()
                    .map(|f| fig(f, if *f == banded { v } else { 1.0 }))
                    .collect()
            };
            for edge in [8.0, 12.0] {
                let c = check_figures(&with(edge), &expect);
                assert_eq!(report_code(&c), EXIT_OK, "{arm:?} {edge} {c:?}");
            }
            for past in [7.999, 12.001] {
                let c = check_figures(&with(past), &expect);
                assert_eq!(report_code(&c), EXIT_MISS, "{arm:?} {past}");
                assert_eq!(
                    c.misses,
                    vec![format!("{banded}: {past} outside band [8.0, 12.0]")]
                );
            }
            // A figure not reported, or reported twice, is a problem, not a
            // miss: the run is not a result.
            let mut short = with(10.0);
            short.pop();
            assert_eq!(report_code(&check_figures(&short, &expect)), EXIT_ERROR);
            let mut twice = with(10.0);
            twice.push(fig(banded, 10.0));
            assert_eq!(report_code(&check_figures(&twice, &expect)), EXIT_ERROR);
        }
    }

    #[test]
    fn median_takes_the_lower_middle() {
        assert_eq!(median(&[3.0, 1.0, 2.0]), Some(2.0));
        assert_eq!(median(&[4.0, 1.0, 3.0, 2.0]), Some(2.0));
        assert_eq!(median(&[]), None);
    }
}
