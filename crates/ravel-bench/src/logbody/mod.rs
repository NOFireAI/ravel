//! The REI arm of `logbody_report` (issue #3709, ADR-3692 Plan row T11): a
//! report-only experiment that measures a selected-gram block index against
//! the exact per-block trigram set on templated log corpora, with a
//! pre-registered go/no-go. Nothing outside the report reads or writes
//! either index.
//!
//! The hypothesis (REI, arXiv:2510.10348): for substring and regex needles
//! that hold a literal run of at least 3 bytes on templated logs, an n-gram
//! index whose grams are selected, not every trigram, prunes blocks the
//! trigram set does not, at a fraction of its bytes.
//!
//! Both arms run over the blocks the report already writes and reads back
//! (its block geometry, one index over all of a corpus's blocks) and answer
//! a needle with the blocks that may hold it:
//!
//! - `trigram`, the control: each block's exact set of distinct byte
//!   trigrams. A block survives when its set holds every trigram of every
//!   literal run. Its bytes are 3 per distinct trigram per block, the plain
//!   sorted array ([`TRIGRAM_LAYOUT`]).
//! - `selected`, the REI-style index ([`grams::GramIndex`]). The candidate
//!   grams are every substring of 3 to `max_gram` bytes (default 6) of the
//!   literal runs of the corpus's workload: its template needles in the
//!   pre-registration file. Id needles are not in the workload, so they are
//!   a negative control: the selected arm prunes an id only through a gram
//!   some template needle shares with it. A candidate
//!   is kept when (frequency rule) the number of blocks holding it is at
//!   most `floor(df_threshold * blocks)`, `df_threshold` defaulting to 0.10,
//!   and (redundancy rule, applied shortest first) no kept shorter substring
//!   of it occurs in the same number of blocks, since such a substring
//!   already occurs in exactly the same blocks. The kept grams then enter
//!   the index rarest first, longer first at equal frequency, each only if
//!   the encoded index stays within `budget_share` (default 0.05) of the
//!   corpus's compressed object bytes; one that does not fit is skipped. A
//!   block survives when it holds every indexed gram that is a substring of
//!   some literal run; a needle with no indexed gram keeps every block. The
//!   bytes are the encoded index ([`grams::INDEX_LAYOUT`]).
//!
//! The workload the grams are selected from is the measured template needle
//! set, so the selected arm's template figures are in-sample: what selection
//! buys for a workload known in advance, not for an unseen needle.
//!
//! A pattern is literal runs separated by `.*` (any bytes within one line);
//! every other byte is literal. The exact residual scans the bodies of every
//! block for a line that holds the runs in order; its blocks are the true
//! holders. A block an arm drops that holds the needle fails the run.
//!
//! Figures ([`Figure`]) are keyed `corpus/figure`, `corpus/arm/figure`,
//! `corpus/needle/figure` and `corpus/needle/arm/figure`. Every one must be
//! present exactly once and inside its band from the pre-registration file
//! ([`spec`]), and every figure must have a band; [`check_figures`] lists
//! each breach and the binary exits 1 on any. `survival_share` is surviving
//! blocks over all blocks; `false_positive_share` is false positives over
//! surviving blocks, 0 when none survive.
//!
//! CPU is the calling thread's CPU clock ([`CPU_CLOCK`]): zstd encoding of
//! every block (bodies joined with 0x0A, the writer's level), each arm's
//! build from those blocks, one run each, on the same thread.
//!
//! The decision ([`decide`], [`DECISION_RULE`]) is per corpus, and overall
//! ([`overall`]) across every corpus the pre-registration file names.

pub mod grams;
pub mod spec;

use std::collections::{BTreeMap, HashMap};
use std::hint::black_box;

use serde::{Deserialize, Serialize};

use self::grams::{BlockText, GramIndex, Params, Pattern, TrigramSets};
use self::spec::{Band, CorpusSpec, Kind};

pub const ARM_TRIGRAM: &str = "trigram";
pub const ARM_SELECTED: &str = "selected";
/// The arms, control first.
pub const ARMS: [&str; 2] = [ARM_TRIGRAM, ARM_SELECTED];

/// Non-id needles each corpus pre-registers.
pub const TEMPLATE_NEEDLES: usize = 5;
/// Of those, how many must clear [`SURVIVAL_BAR`].
pub const NEEDLES_TO_CLEAR: usize = 3;
pub const SURVIVAL_BAR: f64 = 0.10;
pub const BYTES_BAR: f64 = 0.05;
pub const CPU_BAR: f64 = 2.0;

pub const DECISION_RULE: &str = "Per corpus, on the selected arm: GO when survival_share is at \
most 0.10 on at least 3 of the 5 template needles, index_bytes_share is at most 0.05, and \
build_cpu_over_zstd is at most 2; otherwise NO-GO, and a missing figure fails its leg. Overall: \
GO when every corpus the pre-registration names has exactly one report, that report is GO, and \
it has no problems; otherwise NO-GO.";

pub const TRIGRAM_LAYOUT: &str = "per block, the sorted distinct byte trigrams of its lines, 3 \
bytes each; index_bytes is 3 times the distinct trigram count summed over blocks.";

pub const CPU_CLOCK: &str = "CLOCK_THREAD_CPUTIME_ID of the report thread, one run per figure. \
zstd_cpu_s compresses every block's bodies joined with 0x0A at the writer's zstd level. \
build_cpu_s for trigram computes every block's distinct trigram set; for selected it counts the \
candidates over every block, selects, applies the byte cap and encodes. build_cpu_over_zstd is \
build_cpu_s over zstd_cpu_s.";

/// One reported figure.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Figure {
    pub key: String,
    pub value: f64,
}

fn fig(out: &mut Vec<Figure>, key: String, value: f64) {
    out.push(Figure { key, value });
}

/// Every breach of the pre-registration: a band whose figure is missing or
/// present more than once, a figure outside its band, a figure with no band.
pub fn check_figures(figures: &[Figure], bands: &BTreeMap<String, Band>) -> Vec<String> {
    let mut seen: BTreeMap<&str, Vec<f64>> = BTreeMap::new();
    for f in figures {
        seen.entry(f.key.as_str()).or_default().push(f.value);
    }
    let mut out = Vec::new();
    for (key, band) in bands {
        match seen.get(key.as_str()).map(Vec::as_slice) {
            None => out.push(format!("{key}: missing (band {band:?})")),
            Some([v]) => {
                if !(band[0] <= *v && *v <= band[1]) {
                    out.push(format!("{key}: {v} outside band {band:?}"));
                }
            }
            Some(vs) => out.push(format!("{key}: present {} times", vs.len())),
        }
    }
    for key in seen.keys() {
        if !bands.contains_key(*key) {
            out.push(format!("{key}: no band"));
        }
    }
    out
}

/// Surviving blocks over every block.
pub fn survival_share(surviving: u64, blocks: u64) -> f64 {
    if blocks == 0 {
        f64::INFINITY
    } else {
        surviving as f64 / blocks as f64
    }
}

fn share(a: u64, b: u64) -> f64 {
    if b == 0 {
        f64::INFINITY
    } else {
        a as f64 / b as f64
    }
}

fn thread_cpu_s() -> f64 {
    let ts = rustix::time::clock_gettime(rustix::time::ClockId::ThreadCPUTime);
    ts.tv_sec as f64 + ts.tv_nsec as f64 / 1e9
}

fn cpu_time<T>(f: impl FnOnce() -> T) -> (T, f64) {
    let t = thread_cpu_s();
    let out = black_box(f());
    (out, thread_cpu_s() - t)
}

/// What the report hands the arm.
pub struct ReiInput<'a> {
    pub blocks: &'a [BlockText],
    pub lines: u64,
    pub compressed_bytes: u64,
    pub zstd_level: i32,
}

/// A corpus to measure and how.
#[derive(Clone, Debug)]
pub struct ReiRun {
    pub spec: CorpusSpec,
    pub params: Params,
}

#[derive(Debug, Serialize)]
pub struct GramStat {
    pub gram: String,
    pub blocks: u64,
}

#[derive(Debug, Serialize)]
pub struct ReiReport {
    pub corpus: String,
    pub params: Params,
    pub decision_rule: &'static str,
    pub trigram_layout: &'static str,
    pub index_layout: &'static str,
    pub cpu_clock: &'static str,
    pub figures: Vec<Figure>,
    /// The bands the figures were checked against.
    pub bands: BTreeMap<String, Band>,
    /// The selected index's grams, for reading why a needle pruned.
    pub selected_grams: Vec<GramStat>,
    pub problems: Vec<String>,
    pub decision: Decision,
}

impl ReiReport {
    /// The selection parameters the run used, one line per figure, in key
    /// order, then the decision line.
    pub fn lines(&self) -> Vec<String> {
        let mut out = vec![format!(
            "rei params df_threshold={} budget_share={} max_gram={}",
            self.params.df_threshold, self.params.budget_share, self.params.max_gram
        )];
        out.extend(self.figures.iter().map(|f| match self.bands.get(&f.key) {
            Some(b) => {
                let ok = if b[0] <= f.value && f.value <= b[1] {
                    "ok"
                } else {
                    "MISS"
                };
                format!(
                    "rei figure {} = {} band [{}, {}] {ok}",
                    f.key, f.value, b[0], b[1]
                )
            }
            None => format!("rei figure {} = {} band none MISS", f.key, f.value),
        }));
        out.push(self.decision.line.clone());
        out
    }
}

/// Builds both arms over `input.blocks`, answers every needle of `spec`, and
/// checks the figures against its bands. An arm that drops a block holding
/// its needle is an error.
pub fn measure(
    spec: &CorpusSpec,
    input: &ReiInput<'_>,
    params: &Params,
) -> Result<ReiReport, String> {
    if !(grams::MIN_GRAM..=7).contains(&params.max_gram) {
        return Err(format!("max_gram {} is not in 3..=7", params.max_gram));
    }
    let blocks = input.blocks;
    let n_blocks = blocks.len() as u64;
    let patterns: Vec<Pattern> = spec
        .needle
        .iter()
        .map(|n| {
            Pattern::parse(&n.pattern)
                .ok_or_else(|| format!("needle {}: no literal run of 3 bytes", n.id))
        })
        .collect::<Result<_, _>>()?;

    let (zstd, zstd_cpu) = cpu_time(|| {
        blocks
            .iter()
            .map(|b| zstd::bulk::compress(b.bytes(), input.zstd_level).map(|c| c.len()))
            .collect::<Result<Vec<usize>, _>>()
    });
    zstd.map_err(|e| format!("zstd: {e}"))?;
    let (tri, tri_cpu) = cpu_time(|| TrigramSets::build(blocks));
    let workload: Vec<Pattern> = spec
        .needle
        .iter()
        .zip(&patterns)
        .filter(|(n, _)| n.kind == Kind::Template)
        .map(|(_, p)| p.clone())
        .collect();
    let (sel, sel_cpu) =
        cpu_time(|| GramIndex::build(blocks, &workload, input.compressed_bytes, params));

    let c = &spec.name;
    let mut figures = Vec::new();
    fig(&mut figures, format!("{c}/lines"), input.lines as f64);
    fig(&mut figures, format!("{c}/blocks"), n_blocks as f64);
    fig(
        &mut figures,
        format!("{c}/compressed_bytes"),
        input.compressed_bytes as f64,
    );
    fig(&mut figures, format!("{c}/zstd_cpu_s"), zstd_cpu);
    for (arm, bytes, cpu) in [
        (ARM_TRIGRAM, tri.bytes(), tri_cpu),
        (ARM_SELECTED, sel.bytes(), sel_cpu),
    ] {
        fig(&mut figures, format!("{c}/{arm}/index_bytes"), bytes as f64);
        fig(
            &mut figures,
            format!("{c}/{arm}/index_bytes_share"),
            share(bytes, input.compressed_bytes),
        );
        fig(&mut figures, format!("{c}/{arm}/build_cpu_s"), cpu);
        fig(
            &mut figures,
            format!("{c}/{arm}/build_cpu_over_zstd"),
            if zstd_cpu > 0.0 {
                cpu / zstd_cpu
            } else {
                f64::INFINITY
            },
        );
    }
    fig(
        &mut figures,
        format!("{c}/{ARM_SELECTED}/grams_selected"),
        sel.grams.len() as f64,
    );

    for (n, p) in spec.needle.iter().zip(&patterns) {
        let id = &n.id;
        let holders: Vec<bool> = blocks.iter().map(|b| p.matches_block(b)).collect();
        let held = holders.iter().filter(|&&h| h).count() as u64;
        fig(
            &mut figures,
            format!("{c}/{id}/literal_run_bytes"),
            p.literal_run_bytes() as f64,
        );
        fig(&mut figures, format!("{c}/{id}/true_holders"), held as f64);
        for (arm, keep) in [
            (ARM_TRIGRAM, tri.survivors(p)),
            (ARM_SELECTED, sel.survivors(p)),
        ] {
            if let Some(b) = holders.iter().zip(&keep).position(|(&h, &k)| h && !k) {
                return Err(format!("{arm} arm dropped block {b} holding needle {id}"));
            }
            let surviving = keep.iter().filter(|&&k| k).count() as u64;
            let fp = surviving - held;
            fig(
                &mut figures,
                format!("{c}/{id}/{arm}/blocks_surviving"),
                surviving as f64,
            );
            fig(
                &mut figures,
                format!("{c}/{id}/{arm}/survival_share"),
                survival_share(surviving, n_blocks),
            );
            fig(
                &mut figures,
                format!("{c}/{id}/{arm}/false_positives"),
                fp as f64,
            );
            fig(
                &mut figures,
                format!("{c}/{id}/{arm}/false_positive_share"),
                if surviving == 0 {
                    0.0
                } else {
                    fp as f64 / surviving as f64
                },
            );
        }
    }

    let bands = spec.bands();
    let problems = check_figures(&figures, &bands);
    let decision = decide(&decision_input(spec, &figures));
    Ok(ReiReport {
        corpus: spec.name.clone(),
        params: *params,
        decision_rule: DECISION_RULE,
        trigram_layout: TRIGRAM_LAYOUT,
        index_layout: grams::INDEX_LAYOUT,
        cpu_clock: CPU_CLOCK,
        figures,
        bands,
        selected_grams: sel
            .grams
            .iter()
            .map(|(g, list)| GramStat {
                gram: String::from_utf8_lossy(g).into_owned(),
                blocks: list.len() as u64,
            })
            .collect(),
        problems,
        decision,
    })
}

/// The figures a corpus decision rests on; `None` for one not reported
/// exactly once.
#[derive(Clone, Debug)]
pub struct DecisionInput {
    pub corpus: String,
    /// `(needle id, selected survival_share)` per template needle.
    pub template_survival: Vec<(String, Option<f64>)>,
    pub index_bytes_share: Option<f64>,
    pub build_cpu_over_zstd: Option<f64>,
}

/// Reads the decision's figures for `spec` out of `figures`.
pub fn decision_input(spec: &CorpusSpec, figures: &[Figure]) -> DecisionInput {
    let mut by_key: HashMap<&str, Vec<f64>> = HashMap::new();
    for f in figures {
        by_key.entry(f.key.as_str()).or_default().push(f.value);
    }
    let one = |key: String| match by_key.get(key.as_str()).map(Vec::as_slice) {
        Some([v]) => Some(*v),
        _ => None,
    };
    let c = &spec.name;
    DecisionInput {
        corpus: c.clone(),
        template_survival: spec
            .needle
            .iter()
            .filter(|n| n.kind == Kind::Template)
            .map(|n| {
                (
                    n.id.clone(),
                    one(format!("{c}/{}/{ARM_SELECTED}/survival_share", n.id)),
                )
            })
            .collect(),
        index_bytes_share: one(format!("{c}/{ARM_SELECTED}/index_bytes_share")),
        build_cpu_over_zstd: one(format!("{c}/{ARM_SELECTED}/build_cpu_over_zstd")),
    }
}

/// A go/no-go and the one line that states it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Decision {
    pub corpus: String,
    pub go: bool,
    pub line: String,
}

fn shown(v: Option<f64>) -> String {
    v.map_or_else(|| "missing".to_string(), |v| format!("{v:.4}"))
}

/// The per-corpus rule of [`DECISION_RULE`].
pub fn decide(d: &DecisionInput) -> Decision {
    let cleared = d
        .template_survival
        .iter()
        .filter(|(_, s)| s.is_some_and(|s| s <= SURVIVAL_BAR))
        .count();
    let survival_ok = d.template_survival.len() == TEMPLATE_NEEDLES && cleared >= NEEDLES_TO_CLEAR;
    let bytes_ok = d.index_bytes_share.is_some_and(|v| v <= BYTES_BAR);
    let cpu_ok = d.build_cpu_over_zstd.is_some_and(|v| v <= CPU_BAR);
    let go = survival_ok && bytes_ok && cpu_ok;
    let fails: Vec<&str> = [
        (survival_ok, "survival"),
        (bytes_ok, "bytes"),
        (cpu_ok, "cpu"),
    ]
    .iter()
    .filter(|(ok, _)| !ok)
    .map(|(_, name)| *name)
    .collect();
    let needles: Vec<String> = d
        .template_survival
        .iter()
        .map(|(id, s)| format!("{id}={}", shown(*s)))
        .collect();
    let line = format!(
        "rei decision {}: {}; selected survival_share <= {SURVIVAL_BAR} on {cleared} of {} \
         template needles, need {NEEDLES_TO_CLEAR} ({}); index_bytes_share={} (bar {BYTES_BAR}); \
         build_cpu_over_zstd={} (bar {CPU_BAR}); fails: {}",
        d.corpus,
        if go { "GO" } else { "NO-GO" },
        d.template_survival.len(),
        needles.join(" "),
        shown(d.index_bytes_share),
        shown(d.build_cpu_over_zstd),
        if fails.is_empty() {
            "none".to_string()
        } else {
            fails.join(", ")
        },
    );
    Decision {
        corpus: d.corpus.clone(),
        go,
        line,
    }
}

/// A corpus decision read back from a written report, with its problem
/// count.
#[derive(Clone, Debug)]
pub struct SavedDecision {
    pub decision: Decision,
    pub problems: usize,
}

/// The `rei` section's decision from a report the binary wrote.
pub fn saved_decision(json: &str) -> Result<SavedDecision, String> {
    #[derive(Deserialize)]
    struct Saved {
        rei: Option<SavedRei>,
    }
    #[derive(Deserialize)]
    struct SavedRei {
        problems: Vec<String>,
        decision: Decision,
    }
    let saved: Saved = serde_json::from_str(json).map_err(|e| format!("report: {e}"))?;
    let rei = saved.rei.ok_or("report has no rei section")?;
    Ok(SavedDecision {
        decision: rei.decision,
        problems: rei.problems.len(),
    })
}

/// The overall rule of [`DECISION_RULE`] over `corpora`, the names the
/// pre-registration file lists.
pub fn overall(corpora: &[String], saved: &[SavedDecision]) -> Decision {
    let mut go = !corpora.is_empty();
    let mut parts = Vec::new();
    for c in corpora {
        let mine: Vec<&SavedDecision> = saved.iter().filter(|s| &s.decision.corpus == c).collect();
        let part = match mine.as_slice() {
            [] => "missing".to_string(),
            [s] if s.problems > 0 => format!(
                "{} but {} figure problems (not a result)",
                if s.decision.go { "GO" } else { "NO-GO" },
                s.problems
            ),
            [s] => (if s.decision.go { "GO" } else { "NO-GO" }).to_string(),
            many => format!("{} reports", many.len()),
        };
        go &= part == "GO";
        parts.push(format!("{c}={part}"));
    }
    for s in saved {
        if !corpora.contains(&s.decision.corpus) {
            go = false;
            parts.push(format!("{}=not pre-registered", s.decision.corpus));
        }
    }
    Decision {
        corpus: "overall".to_string(),
        go,
        line: format!(
            "rei decision overall: {}; {}",
            if go { "GO" } else { "NO-GO" },
            parts.join(", ")
        ),
    }
}

#[cfg(test)]
mod tests;
