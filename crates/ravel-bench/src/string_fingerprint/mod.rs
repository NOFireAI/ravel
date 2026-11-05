//! `string_fingerprint_report` (issue #3710, ADR-3692 Plan row T12): a
//! report-only experiment measuring a learned, conservative per-block string
//! summary for equality and `LIKE '%literal%'` predicates on a declared `Str`
//! column, against the production bloom and `Contains` baselines, with a
//! pre-registered go/no-go. Nothing outside the report reads or writes the
//! summary; no production format changes.
//!
//! Hypothesis (arXiv:2507.10391): a learned, conservative per-block string
//! summary is cheaper than bloom and postings at equal false-positive rate,
//! including on predicates unseen at build time, and saturates on long
//! bodies.
//!
//! The summary ([`summary`]) learns from a TRAINING workload and is judged on
//! a disjoint EVALUATION workload ([`workload`]), both drawn from the
//! subject's values by a pre-registered seed before the build. `seen/*`
//! figures evaluate the training literals (in-sample), `unseen/*` the
//! evaluation literals; the two are never pooled.
//!
//! A `*_false_positive_share` is the standard filter rate over (literal,
//! block) pairs: pairs whose block does not hold the literal and that the
//! summary (or production code) keeps, over all pairs whose block does not
//! hold it, pooled over the literals of the group. A block holding the
//! literal is never counted; the summary dropping one is an error, never a
//! figure.
//!
//! Baselines are production code over the same RLOG objects ([`Production`]):
//! - **equality**: `RlogReader::scan_blocks` with `Predicate::Equals` on the
//!   column, reading `ScanStats::blocks_after_bloom`, which is after the skip
//!   index, POSTINGS and the bloom. The bloom holds a value's exact bytes only
//!   when it is at most [`BLOOM_EXACT_MAX`] bytes, so `bloom/*` covers the
//!   evaluation literals of that length and `production/*` covers all of them.
//! - **contains**: `Predicate::Contains` on the column, scanning with that
//!   column selected and every block drained, reading
//!   `ScanStats::contains_cells_tested` (the `containsCellsTested` figure)
//!   and each block's surviving rows, which must agree with the in-module
//!   truth block for block. The same scan restricted to the blocks the
//!   summary keeps gives the cells production would test with the summary
//!   in front of it.
//!
//! The saturation arm rebuilds blocks from the values over
//! [`SATURATION_BYTES`] alone, with the production block cut, summarizes them
//! with the learned layout, and evaluates the `LIKE` evaluation literals on
//! them. It is a separate verdict ([`Saturation`]) and not a go/no-go leg.
//!
//! CPU is the calling thread's CPU clock ([`CPU_CLOCK`]).

pub mod spec;
pub mod summary;
pub mod workload;

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use self::spec::{Expect, SubjectSpec};
use self::summary::{BlockSummary, Builder, Params, survives};
use self::workload::{Kind, Literal, Workload};

/// A block's real column values, read back through the production block
/// cuts.
pub type BlockValues = Vec<Vec<u8>>;

/// A value longer than this is a long body, for the saturation arm.
pub const SATURATION_BYTES: usize = 1024;
/// The longest value the RLOG writer puts into the bloom as exact bytes
/// (`EXACT_BLOOM_MAX`, crates/ravel-logseg/src/writer.rs:238).
pub const BLOOM_EXACT_MAX: usize = 64;
/// The production block's byte cut (`RlogConfig::block_max_bytes` default).
pub const BLOCK_MAX_BYTES: usize = 8 << 20;

pub const GO_FALSE_POSITIVE_BAR: f64 = 0.10;
pub const GO_BYTES_BAR: f64 = 0.01;
pub const GO_CPU_BAR: f64 = 2.0;
pub const SATURATION_BAR: f64 = 0.30;

pub const DECISION_RULE: &str = "Per subject: GO when unseen/equals/false_positive_share and \
unseen/like/false_positive_share are both at most 0.10, summary_bytes_share is at most 0.01, and \
build_cpu_over_zstd is at most 2; otherwise NO-GO, and a missing figure fails its leg. \
Saturation is a separate verdict: with values over 1024 bytes, \
saturation/unseen/like/false_positive_share at most 0.30 is body-eligible and above it \
body-ineligible; with none, untested. Overall: GO when every pre-registered subject has exactly \
one report, it is GO, and it has no structural problem (a missing, duplicate or unexpected \
figure); a figure outside its prediction band is reported as a miss and does not change the \
decision.";

pub const CPU_CLOCK: &str = "CLOCK_THREAD_CPUTIME_ID of the report thread, one run each. \
zstd_cpu_s compresses every block's values joined with a 0x00 byte at the pre-registered zstd \
level. The build (summary::build: the training grams' block counts, the learned set, and every \
block's bitset) runs once on the same thread.";

/// The suffix of every figure the report can print, in print order.
pub const FIGURES: &[&str] = &[
    "lines",
    "blocks",
    "column_bytes",
    "compressed_bytes",
    "zstd_cpu_s",
    "build_cpu_over_zstd",
    "candidate_grams",
    "learned_bits",
    "fallback_bits",
    "bits_per_block",
    "summary_bytes_share",
    "fallback_density",
    "eval_equals_bloom_testable",
    "seen/equals/false_positive_share",
    "seen/like/false_positive_share",
    "unseen/equals/false_positive_share",
    "unseen/like/false_positive_share",
    "unseen/equals_absent/false_positive_share",
    "unseen/like_absent/false_positive_share",
    "production/equals/false_positive_share",
    "bloom/equals/false_positive_share",
    "summary/equals_bloom_testable/false_positive_share",
    "contains/like/cells_tested_per_row",
    "summary/like/cells_tested_share",
    "saturation/long_values",
    "saturation/blocks",
    "saturation/fallback_density",
    "saturation/unseen/like/false_positive_share",
];

/// One reported figure.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Figure {
    pub key: String,
    pub value: f64,
}

pub(crate) fn threads() -> usize {
    std::thread::available_parallelism().map_or(4, |n| n.get())
}

/// `f` over `items` on every core, in order.
pub(crate) fn par_map<T: Sync, R: Send>(
    items: &[T],
    f: impl Fn(&T) -> R + Sync,
) -> Result<Vec<R>, String> {
    let chunk = items.len().div_ceil(threads()).max(1);
    std::thread::scope(|s| {
        let f = &f;
        let handles: Vec<_> = items
            .chunks(chunk)
            .map(|c| s.spawn(move || c.iter().map(f).collect::<Vec<R>>()))
            .collect();
        let mut out = Vec::with_capacity(items.len());
        for h in handles {
            out.extend(h.join().map_err(|_| "worker thread panicked".to_string())?);
        }
        Ok(out)
    })
}

/// Whether `f` holds for any of `items`, on every core. A panicked worker
/// counts as a match, so a caller treating `false` as proof of absence
/// cannot be misled by it.
pub(crate) fn par_any<T: Sync>(items: &[T], f: impl Fn(&T) -> bool + Sync) -> bool {
    par_map(items, |t| f(t)).map_or(true, |v| v.into_iter().any(|b| b))
}

/// Each block's values joined by a 0x00 byte, as text. Literals hold no 0x00,
/// so a literal occurs in a block's text exactly when some value contains it.
pub fn block_texts(blocks: &[BlockValues]) -> Result<Vec<String>, String> {
    blocks
        .iter()
        .enumerate()
        .map(|(i, b)| String::from_utf8(b.join(&0u8)).map_err(|e| format!("block {i}: {e}")))
        .collect()
}

fn thread_cpu_s() -> f64 {
    let ts = rustix::time::clock_gettime(rustix::time::ClockId::ThreadCPUTime);
    ts.tv_sec as f64 + ts.tv_nsec as f64 / 1e9
}

fn cpu_time<T>(f: impl FnOnce() -> T) -> (T, f64) {
    let t = thread_cpu_s();
    let out = std::hint::black_box(f());
    (out, thread_cpu_s() - t)
}

/// What [`check_figures`] found: `problems` make a report not a result (a
/// figure missing, present twice, present though pre-registered absent, or
/// not pre-registered at all); `misses` are figures outside their band.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Checked {
    pub problems: Vec<String>,
    pub misses: Vec<String>,
}

pub fn check_figures(figures: &[Figure], expect: &BTreeMap<String, Expect>) -> Checked {
    let mut seen: BTreeMap<&str, Vec<f64>> = BTreeMap::new();
    for f in figures {
        seen.entry(f.key.as_str()).or_default().push(f.value);
    }
    let mut out = Checked::default();
    for (key, e) in expect {
        let got = seen.get(key.as_str()).map(Vec::as_slice);
        match (e, got) {
            (Expect::Absent(_), None) => {}
            (Expect::Absent(why), Some(_)) => out
                .problems
                .push(format!("{key}: reported, pre-registered absent ({why})")),
            (_, None) => out.problems.push(format!("{key}: missing")),
            (_, Some(vs)) if vs.len() > 1 => out
                .problems
                .push(format!("{key}: present {} times", vs.len())),
            (Expect::Predicted { band, .. }, Some([v])) if !(band[0] <= *v && *v <= band[1]) => {
                out.misses.push(format!("{key}: {v} outside band {band:?}"));
            }
            _ => {}
        }
    }
    for key in seen.keys() {
        if !expect.contains_key(*key) {
            out.problems.push(format!("{key}: not pre-registered"));
        }
    }
    out
}

/// Production's answer for one `Contains` literal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContainsRun {
    /// `ScanStats::contains_cells_tested`, summed over objects.
    pub cells_tested: u64,
    /// Surviving rows per block, over every block of the subject in order
    /// (zero for a block the scan did not read).
    pub rows: Vec<u64>,
}

/// The production baselines, run over the subject's real RLOG objects.
pub trait Production: Sync {
    /// Blocks `Predicate::Equals` keeps after skip, POSTINGS and bloom.
    fn equals_blocks_kept(&self, literal: &str) -> Result<u64, String>;
    /// A drained `Predicate::Contains` scan, over every block or only the
    /// blocks `keep` marks.
    fn contains(&self, literal: &str, keep: Option<&[bool]>) -> Result<ContainsRun, String>;
}

/// What the report hands the measurement for one subject.
pub struct SubjectInput<'a> {
    pub lines: u64,
    pub blocks: &'a [BlockValues],
    pub texts: &'a [String],
    pub workload: &'a Workload,
}

/// Which blocks hold `literal`.
fn holders(lit: &Literal, blocks: &[BlockValues], texts: &[String]) -> Result<Vec<bool>, String> {
    match lit.kind {
        Kind::Equals => par_map(blocks, |b| {
            b.iter().any(|v| v.as_slice() == lit.text.as_bytes())
        }),
        Kind::Like => par_map(texts, |t| t.contains(lit.text.as_str())),
    }
}

/// False-positive pairs over non-holder pairs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Tally {
    false_positive: u64,
    non_holder: u64,
}

impl Tally {
    fn add(&mut self, held: &[bool], kept: &[bool]) {
        for (&h, &k) in held.iter().zip(kept) {
            if !h {
                self.non_holder += 1;
                if k {
                    self.false_positive += 1;
                }
            }
        }
    }

    fn rate(self) -> f64 {
        if self.non_holder == 0 {
            f64::NAN
        } else {
            self.false_positive as f64 / self.non_holder as f64
        }
    }
}

/// The summary's verdict on every block for `lit`; an error when it drops a
/// block that holds the literal.
fn summary_verdicts(
    lit: &Literal,
    held: &[bool],
    summaries: &[BlockSummary],
    layout: &summary::Layout,
) -> Result<Vec<bool>, String> {
    let bits = layout.literal_bits(lit.text.as_bytes());
    let kept: Vec<bool> = summaries.iter().map(|s| survives(s, &bits)).collect();
    if let Some(i) = held.iter().zip(&kept).position(|(&h, &k)| h && !k) {
        return Err(format!(
            "false negative: the summary drops block {i}, which holds {:?} literal {:?}",
            lit.kind.name(),
            lit.text
        ));
    }
    Ok(kept)
}

/// The values over [`SATURATION_BYTES`], in order, cut into blocks the way
/// the writer cuts them: `block_records` values or [`BLOCK_MAX_BYTES`].
pub fn long_value_blocks(blocks: &[BlockValues], block_records: usize) -> Vec<BlockValues> {
    let mut out = Vec::new();
    let mut cur: BlockValues = Vec::new();
    let mut bytes = 0usize;
    for v in blocks
        .iter()
        .flatten()
        .filter(|v| v.len() > SATURATION_BYTES)
    {
        if !cur.is_empty() && (cur.len() >= block_records || bytes + v.len() > BLOCK_MAX_BYTES) {
            out.push(std::mem::take(&mut cur));
            bytes = 0;
        }
        bytes += v.len();
        cur.push(v.clone());
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParamsOut {
    pub column: String,
    pub gram_len: usize,
    pub budget_share: f64,
    pub fallback_min_bits: usize,
    pub zstd_level: i32,
    pub block_records: usize,
    pub object_records: usize,
    pub max_lines: u64,
    pub seed: u64,
    pub literals_per_kind: usize,
    pub eq_max_len: usize,
    pub like_min_len: usize,
    pub like_max_len: usize,
}

impl ParamsOut {
    pub fn of(spec: &SubjectSpec) -> ParamsOut {
        ParamsOut {
            column: spec.column.clone(),
            gram_len: spec.gram_len,
            budget_share: spec.budget_share,
            fallback_min_bits: spec.fallback_min_bits,
            zstd_level: spec.zstd_level,
            block_records: spec.block_records,
            object_records: spec.object_records,
            max_lines: spec.max_lines,
            seed: spec.seed,
            literals_per_kind: spec.literals_per_kind,
            eq_max_len: spec.eq_max_len,
            like_min_len: spec.like_min_len,
            like_max_len: spec.like_max_len,
        }
    }

    pub fn line(&self) -> String {
        format!(
            "string_fingerprint params column={} gram_len={} budget_share={} fallback_min_bits={} \
             zstd_level={} block_records={} object_records={} max_lines={} seed={} \
             literals_per_kind={} eq_max_len={} like_min_len={} like_max_len={}",
            self.column,
            self.gram_len,
            self.budget_share,
            self.fallback_min_bits,
            self.zstd_level,
            self.block_records,
            self.object_records,
            self.max_lines,
            self.seed,
            self.literals_per_kind,
            self.eq_max_len,
            self.like_min_len,
            self.like_max_len
        )
    }
}

/// Provenance printed first and kept in the report.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Stamp {
    /// `git rev-parse HEAD`, with `-dirty` when tracked files differ from it.
    pub git: String,
    /// `uname -m`.
    pub arch: String,
    pub nproc: usize,
    pub mem_total_kib: u64,
    /// The corpus file's name (no directory).
    pub corpus_file: String,
    pub corpus_sha256: String,
    pub corpus_bytes: u64,
    pub zstd_level: i32,
    pub cpu_clock: String,
}

impl Stamp {
    pub fn line(&self) -> String {
        format!(
            "string_fingerprint stamp git={} arch={} nproc={} mem_total_kib={} corpus_file={} \
             corpus_sha256={} corpus_bytes={} zstd_level={} cpu_clock=CLOCK_THREAD_CPUTIME_ID",
            self.git,
            self.arch,
            self.nproc,
            self.mem_total_kib,
            self.corpus_file,
            self.corpus_sha256,
            self.corpus_bytes,
            self.zstd_level
        )
    }
}

/// Serialized only: a NaN figure (a rate with no non-holder) is written as
/// JSON `null`. [`saved_decision`] reads a written report back.
#[derive(Debug, Serialize)]
pub struct SubjectReport {
    /// Set by the report bin; `None` for a measurement with no corpus file.
    pub stamp: Option<Stamp>,
    pub subject: String,
    pub params: ParamsOut,
    pub decision_rule: String,
    pub summary_bytes_rule: String,
    pub cpu_clock: String,
    pub workload: Workload,
    pub figures: Vec<Figure>,
    pub expect: BTreeMap<String, Expect>,
    pub checked: Checked,
    pub decision: Decision,
}

fn fmt_expect(e: &Expect, v: f64) -> String {
    match e {
        Expect::Predicted { predict, band } => {
            let ok = if band[0] <= v && v <= band[1] {
                "ok"
            } else {
                "MISS"
            };
            format!("predict {predict} band [{}, {}] {ok}", band[0], band[1])
        }
        Expect::Unpredicted(why) => format!("unpredicted ({why})"),
        Expect::Absent(why) => format!("pre-registered absent ({why}) PROBLEM"),
    }
}

impl SubjectReport {
    /// The params line, one line per figure (and per pre-registered absent
    /// figure), then the decision line.
    pub fn lines(&self) -> Vec<String> {
        let mut out: Vec<String> = self.stamp.iter().map(Stamp::line).collect();
        out.push(self.params.line());
        let by_kind = |present: bool, set: &[Literal]| {
            [Kind::Equals, Kind::Like]
                .iter()
                .map(|&k| {
                    let n = set
                        .iter()
                        .filter(|l| l.kind == k && l.present == present)
                        .count();
                    format!("{}={n}", k.name())
                })
                .collect::<Vec<_>>()
                .join(" ")
        };
        out.push(format!(
            "string_fingerprint workload training present {} absent {}; evaluation present {} absent {}",
            by_kind(true, &self.workload.training),
            by_kind(false, &self.workload.training),
            by_kind(true, &self.workload.evaluation),
            by_kind(false, &self.workload.evaluation),
        ));
        for f in &self.figures {
            let tail = match self.expect.get(&f.key) {
                Some(e) => fmt_expect(e, f.value),
                None => "not pre-registered PROBLEM".to_string(),
            };
            out.push(format!(
                "string_fingerprint figure {} = {} {tail}",
                f.key, f.value
            ));
        }
        for (key, e) in &self.expect {
            if let Expect::Absent(why) = e
                && !self.figures.iter().any(|f| &f.key == key)
            {
                out.push(format!(
                    "string_fingerprint figure {key} absent as pre-registered ({why})"
                ));
            }
        }
        out.push(self.decision.line.clone());
        out
    }
}

/// Builds the summary from the training literals, evaluates both sets
/// against it and against `production`, and decides. A summary dropping a
/// block that holds a literal, or production disagreeing with the in-module
/// truth, is an error rather than a figure.
pub fn measure(
    spec: &SubjectSpec,
    input: &SubjectInput<'_>,
    production: &dyn Production,
) -> Result<SubjectReport, String> {
    let blocks = input.blocks;
    let n = blocks.len();
    if input.texts.len() != n {
        return Err("texts and blocks differ in length".into());
    }
    let (sizes, zstd_cpu) = cpu_time(|| {
        blocks
            .iter()
            .map(|b| zstd::bulk::compress(&b.join(&0u8), spec.zstd_level).map(|c| c.len() as u64))
            .collect::<Result<Vec<u64>, _>>()
    });
    let compressed_bytes: u64 = sizes.map_err(|e| format!("zstd: {e}"))?.iter().sum();
    let column_bytes: u64 = blocks.iter().flatten().map(|v| v.len() as u64).sum();

    let training: Vec<Vec<u8>> = input
        .workload
        .training
        .iter()
        .map(|l| l.text.as_bytes().to_vec())
        .collect();
    let params = Params {
        gram_len: spec.gram_len,
        budget_share: spec.budget_share,
        fallback_min_bits: spec.fallback_min_bits,
    };
    let (built, build_cpu) =
        cpu_time(|| summary::build(&training, blocks, &params, compressed_bytes));
    let built = built?;
    let layout = &built.layout;

    let mut seen = [Tally::default(); 2];
    let mut unseen = [Tally::default(); 2];
    let mut unseen_absent = [Tally::default(); 2];
    let slot = |k: Kind| usize::from(k == Kind::Like);
    for lit in &input.workload.training {
        let held = holders(lit, blocks, input.texts)?;
        let kept = summary_verdicts(lit, &held, &built.summaries, layout)?;
        seen[slot(lit.kind)].add(&held, &kept);
    }

    let mut production_eq = Tally::default();
    let mut bloom_eq = Tally::default();
    let mut summary_bloom_eq = Tally::default();
    let mut bloom_testable = 0u64;
    let mut eval_like: Vec<(&Literal, Vec<bool>, Vec<bool>)> = Vec::new();
    for lit in &input.workload.evaluation {
        let held = holders(lit, blocks, input.texts)?;
        if held.iter().any(|&h| h) != lit.present {
            return Err(format!(
                "workload literal {:?} is marked present={} but holds in {} blocks",
                lit.text,
                lit.present,
                held.iter().filter(|&&h| h).count()
            ));
        }
        let kept = summary_verdicts(lit, &held, &built.summaries, layout)?;
        unseen[slot(lit.kind)].add(&held, &kept);
        if !lit.present {
            unseen_absent[slot(lit.kind)].add(&held, &kept);
        }
        match lit.kind {
            Kind::Equals => {
                let holding = held.iter().filter(|&&h| h).count() as u64;
                let prod = production.equals_blocks_kept(&lit.text)?;
                if prod < holding {
                    return Err(format!(
                        "production equality keeps {prod} blocks for {:?}, fewer than the {holding} holding it",
                        lit.text
                    ));
                }
                let pair = Tally {
                    false_positive: prod - holding,
                    non_holder: n as u64 - holding,
                };
                production_eq.false_positive += pair.false_positive;
                production_eq.non_holder += pair.non_holder;
                if lit.text.len() <= BLOOM_EXACT_MAX {
                    bloom_testable += 1;
                    bloom_eq.false_positive += pair.false_positive;
                    bloom_eq.non_holder += pair.non_holder;
                    summary_bloom_eq.add(&held, &kept);
                }
            }
            Kind::Like => eval_like.push((lit, held, kept)),
        }
    }

    let contains = par_map(&eval_like, |(lit, held, kept)| {
        let full = production.contains(&lit.text, None)?;
        let sub = production.contains(&lit.text, Some(kept))?;
        for (run, what) in [(&full, "full"), (&sub, "summary-kept")] {
            let prod_held: Vec<bool> = run.rows.iter().map(|&r| r > 0).collect();
            if &prod_held != held {
                return Err(format!(
                    "production Contains ({what} scan) disagrees with the in-module truth for {:?}",
                    lit.text
                ));
            }
        }
        Ok::<(u64, u64), String>((full.cells_tested, sub.cells_tested))
    })?
    .into_iter()
    .collect::<Result<Vec<(u64, u64)>, String>>()?;
    let cells_full: u64 = contains.iter().map(|c| c.0).sum();
    let cells_sub: u64 = contains.iter().map(|c| c.1).sum();

    let long_blocks = long_value_blocks(blocks, spec.block_records);
    let long_values: usize = long_blocks.iter().map(Vec::len).sum();

    let s = &spec.name;
    let mut figures = Vec::new();
    let mut fig = |k: &str, v: f64| {
        figures.push(Figure {
            key: format!("{s}/{k}"),
            value: v,
        })
    };
    fig("lines", input.lines as f64);
    fig("blocks", n as f64);
    fig("column_bytes", column_bytes as f64);
    fig("compressed_bytes", compressed_bytes as f64);
    fig("zstd_cpu_s", zstd_cpu);
    fig(
        "build_cpu_over_zstd",
        if zstd_cpu > 0.0 {
            build_cpu / zstd_cpu
        } else {
            f64::INFINITY
        },
    );
    fig("candidate_grams", built.candidates as f64);
    fig("learned_bits", layout.learned.len() as f64);
    fig("fallback_bits", layout.fallback_bits as f64);
    fig("bits_per_block", layout.bits_per_block() as f64);
    fig(
        "summary_bytes_share",
        built.summary_bytes() as f64 / compressed_bytes.max(1) as f64,
    );
    fig("fallback_density", built.fallback_density());
    fig("eval_equals_bloom_testable", bloom_testable as f64);
    for (group, tallies) in [
        ("seen", &seen),
        ("unseen", &unseen),
        ("unseen_absent", &unseen_absent),
    ] {
        for kind in [Kind::Equals, Kind::Like] {
            let key = match group {
                "unseen_absent" => format!("unseen/{}_absent/false_positive_share", kind.name()),
                g => format!("{g}/{}/false_positive_share", kind.name()),
            };
            fig(&key, tallies[slot(kind)].rate());
        }
    }
    fig(
        "production/equals/false_positive_share",
        production_eq.rate(),
    );
    if bloom_testable > 0 {
        fig("bloom/equals/false_positive_share", bloom_eq.rate());
        fig(
            "summary/equals_bloom_testable/false_positive_share",
            summary_bloom_eq.rate(),
        );
    }
    let rows_total: u64 = blocks.iter().map(|b| b.len() as u64).sum();
    fig(
        "contains/like/cells_tested_per_row",
        cells_full as f64 / (rows_total * eval_like.len() as u64).max(1) as f64,
    );
    fig(
        "summary/like/cells_tested_share",
        if cells_full == 0 {
            f64::NAN
        } else {
            cells_sub as f64 / cells_full as f64
        },
    );
    fig("saturation/long_values", long_values as f64);
    let mut saturation_rate = None;
    if long_values > 0 {
        let builder = Builder::new(layout);
        let sums: Vec<BlockSummary> = long_blocks.iter().map(|b| builder.block(b)).collect();
        let texts = block_texts(&long_blocks)?;
        let mut tally = Tally::default();
        for (lit, _, _) in &eval_like {
            let held = holders(lit, &long_blocks, &texts)?;
            let kept = summary_verdicts(lit, &held, &sums, layout)?;
            tally.add(&held, &kept);
        }
        fig("saturation/blocks", long_blocks.len() as f64);
        fig(
            "saturation/fallback_density",
            summary::mean_density(&sums, layout),
        );
        fig("saturation/unseen/like/false_positive_share", tally.rate());
        saturation_rate = Some(tally.rate());
    }

    let expect = spec.expectations();
    let checked = check_figures(&figures, &expect);
    let decision = decide(&decision_input(s, &figures, saturation_rate));
    Ok(SubjectReport {
        stamp: None,
        subject: s.clone(),
        params: ParamsOut::of(spec),
        decision_rule: DECISION_RULE.to_string(),
        summary_bytes_rule: summary::LAYOUT_BYTES.to_string(),
        cpu_clock: CPU_CLOCK.to_string(),
        workload: input.workload.clone(),
        figures,
        expect,
        checked,
        decision,
    })
}

/// The figures a subject decision rests on; `None` for one not reported
/// exactly once.
#[derive(Clone, Debug)]
pub struct DecisionInput {
    pub subject: String,
    pub unseen_equals_fp: Option<f64>,
    pub unseen_like_fp: Option<f64>,
    pub summary_bytes_share: Option<f64>,
    pub build_cpu_over_zstd: Option<f64>,
    /// `None` when the subject has no value over [`SATURATION_BYTES`].
    pub saturation_fp: Option<f64>,
}

pub fn decision_input(
    subject: &str,
    figures: &[Figure],
    saturation_fp: Option<f64>,
) -> DecisionInput {
    let one = |suffix: &str| {
        let key = format!("{subject}/{suffix}");
        let vs: Vec<f64> = figures
            .iter()
            .filter(|f| f.key == key)
            .map(|f| f.value)
            .collect();
        match vs.as_slice() {
            [v] => Some(*v),
            _ => None,
        }
    };
    DecisionInput {
        subject: subject.to_string(),
        unseen_equals_fp: one("unseen/equals/false_positive_share"),
        unseen_like_fp: one("unseen/like/false_positive_share"),
        summary_bytes_share: one("summary_bytes_share"),
        build_cpu_over_zstd: one("build_cpu_over_zstd"),
        saturation_fp,
    }
}

/// The saturation arm's verdict.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Saturation {
    Untested,
    BodyEligible,
    BodyIneligible,
}

/// A go/no-go, the saturation verdict, and the line that states both.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Decision {
    pub subject: String,
    pub go: bool,
    pub saturation: Saturation,
    pub line: String,
}

fn shown(v: Option<f64>) -> String {
    v.map_or_else(|| "missing".to_string(), |v| format!("{v:.4}"))
}

/// The per-subject rule of [`DECISION_RULE`].
pub fn decide(d: &DecisionInput) -> Decision {
    let equals_ok = d
        .unseen_equals_fp
        .is_some_and(|v| v <= GO_FALSE_POSITIVE_BAR);
    let like_ok = d.unseen_like_fp.is_some_and(|v| v <= GO_FALSE_POSITIVE_BAR);
    let bytes_ok = d.summary_bytes_share.is_some_and(|v| v <= GO_BYTES_BAR);
    let cpu_ok = d.build_cpu_over_zstd.is_some_and(|v| v <= GO_CPU_BAR);
    let go = equals_ok && like_ok && bytes_ok && cpu_ok;
    let saturation = match d.saturation_fp {
        None => Saturation::Untested,
        Some(v) if v <= SATURATION_BAR => Saturation::BodyEligible,
        Some(_) => Saturation::BodyIneligible,
    };
    let fails: Vec<&str> = [
        (equals_ok, "unseen_equals"),
        (like_ok, "unseen_like"),
        (bytes_ok, "bytes"),
        (cpu_ok, "cpu"),
    ]
    .iter()
    .filter(|(ok, _)| !ok)
    .map(|(_, name)| *name)
    .collect();
    let line = format!(
        "string_fingerprint decision {}: {}; unseen_equals_fp={} unseen_like_fp={} (bar {GO_FALSE_POSITIVE_BAR}); \
         summary_bytes_share={} (bar {GO_BYTES_BAR}); build_cpu_over_zstd={} (bar {GO_CPU_BAR}); \
         fails: {}; saturation {} (saturation_fp={}, bar {SATURATION_BAR})",
        d.subject,
        if go { "GO" } else { "NO-GO" },
        shown(d.unseen_equals_fp),
        shown(d.unseen_like_fp),
        shown(d.summary_bytes_share),
        shown(d.build_cpu_over_zstd),
        if fails.is_empty() {
            "none".to_string()
        } else {
            fails.join(", ")
        },
        match saturation {
            Saturation::Untested => "untested (no value over 1024 bytes)",
            Saturation::BodyEligible => "body-eligible",
            Saturation::BodyIneligible => "body-ineligible",
        },
        shown(d.saturation_fp),
    );
    Decision {
        subject: d.subject.clone(),
        go,
        saturation,
        line,
    }
}

/// A subject decision read back from a written report, with its structural
/// problem count.
#[derive(Clone, Debug)]
pub struct SavedDecision {
    pub decision: Decision,
    pub problems: usize,
}

pub fn saved_decision(json: &str) -> Result<SavedDecision, String> {
    #[derive(Deserialize)]
    struct Saved {
        decision: Decision,
        checked: Checked,
    }
    let saved: Saved = serde_json::from_str(json).map_err(|e| format!("report: {e}"))?;
    Ok(SavedDecision {
        decision: saved.decision,
        problems: saved.checked.problems.len(),
    })
}

/// The overall rule of [`DECISION_RULE`] over `subjects`, the names the
/// pre-registration file lists.
pub fn overall(subjects: &[String], saved: &[SavedDecision]) -> Decision {
    let mut go = !subjects.is_empty();
    let mut parts = Vec::new();
    for s in subjects {
        let mine: Vec<&SavedDecision> = saved.iter().filter(|d| &d.decision.subject == s).collect();
        let part = match mine.as_slice() {
            [] => "missing".to_string(),
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
        saturation: Saturation::Untested,
        line: format!(
            "string_fingerprint decision overall: {}; {}",
            if go { "GO" } else { "NO-GO" },
            parts.join(", ")
        ),
    }
}

#[cfg(test)]
mod tests;
