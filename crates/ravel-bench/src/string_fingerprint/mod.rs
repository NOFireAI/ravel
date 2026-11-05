//! `string_fingerprint_report` (issue #3710, ADR-3692 Plan row T12): a
//! report-only experiment measuring a learned, conservative per-block string
//! summary for equality and `LIKE '%literal%'` predicates on a declared
//! `Str` column, against the production bloom and `Contains` baselines, with
//! a pre-registered go/no-go. Nothing outside the report reads or writes the
//! summary; no production format changes.
//!
//! Hypothesis (arXiv:2507.10391, "Instance-Optimized String Fingerprints"): a
//! lightweight, per-workload secondary index can approximate equality and
//! substring predicates soundly (never a false negative) and generalize past
//! the workload it was built from.
//!
//! The summary itself ([`summary`]) is a shared vocabulary of features
//! selected from a *seen* predicate workload (equality literals verbatim;
//! `LIKE` literals decomposed into fixed-length grams), with each block
//! storing only its own bitset over that vocabulary -- see `summary`'s
//! module doc for why that is a different shape than the REI arm's shared
//! postings list ([`crate::logbody::grams::GramIndex`], issue #3709), and for
//! the conservativeness argument that holds regardless of the vocabulary.
//!
//! Every subject (a corpus and a column) is written as a declared `Str`
//! attribute under one fixed column name, `value`, with `RlogConfig`
//! defaults (`BloomScope::All` covers it, so the production bloom baseline
//! applies with no extra schema step). This holds for the four LogHub body
//! subjects too: `Predicate::Contains` resolves its `key` through a named
//! `FIELD_DIR` column (`crates/ravel-logseg/src/reader.rs`, the `Contains`
//! arm build), never through `FieldSel::Body`, so a body-text subject that
//! was written as the record body would have no production `Contains`
//! baseline to compare against at all. Writing every subject's values as the
//! same kind of attribute column keeps the two baselines, and the summary
//! itself, identical in mechanism across all eight subjects.
//!
//! Baselines are real production code, run over the same blocks:
//! - **bloom**: `RlogReader::scan_blocks` with `Predicate::Equals{field:
//!   Attr(column), value}`, reading `ScanStats::blocks_after_bloom` (checked
//!   against `bloom_degraded == false`).
//! - **contains**: `RlogReader::scan_blocks` with `Predicate::Contains{key:
//!   column, needle, negated: false}`, fully drained, reading
//!   `ScanStats::contains_cells_tested`
//!   (`crates/ravel-sql/src/stats_json.rs` names this the production
//!   `containsCellsTested` figure).
//!
//! Figures are keyed `subject/figure`. Every one must be present exactly
//! once and inside its band from the pre-registration file ([`spec`]), and
//! every figure must have a band; [`check_figures`] lists each breach.
//! `*_false_positive_share` is false-positive blocks over surviving blocks
//! (0 when none survive), matching `crate::logbody`'s convention: a block
//! the production exact-residual scan (recomputed here directly over the
//! real per-block values the report already read back, so no second
//! production code path is needed to get a true-holder oracle) says holds
//! the predicate and the summary does not keep is a hard error, never a
//! softly-reported figure -- the summary must never produce a false
//! negative.
//!
//! `seen/*` figures are in-sample: the vocabulary was built from exactly
//! those literals. `unseen/*` figures are the out-of-sample test the
//! hypothesis lives or dies on. An equality literal outside the vocabulary
//! always survives unconditionally (summary module doc): since the
//! vocabulary's equality features are drawn only from seen literals, the
//! `unseen/equals/false_positive_share` figure measures exactly that
//! ceiling, not a defect in one run's selection. A `LIKE` literal can still
//! be pruned when its grams happen to already be vocabulary entries built
//! from other, seen literals, which is the generalization the hypothesis
//! predicts and `unseen/like/false_positive_share` tests directly.
//!
//! `saturation/false_positive_share` restricts the unseen `LIKE` measurement
//! to blocks holding a value over 1024 bytes. A subject with no such block
//! reports `0.0` (vacuously conservative: there is nothing to falsely keep)
//! and is noted "body-ineligible" in the decision line rather than silently
//! passing a bar nothing was measured against.
//!
//! CPU is the calling thread's CPU clock ([`CPU_CLOCK`]).
//!
//! The decision ([`decide`], [`DECISION_RULE`]) is per subject, and overall
//! ([`overall`]) across every subject the pre-registration file names.

pub mod spec;
pub mod summary;

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use self::spec::{Band, Kind, SubjectSpec};
use self::summary::{BlockSummary, Params, Vocabulary, survives_equals, survives_like};

/// A block's real column values, read back through the production block
/// cuts (never a naive re-chunking of the corpus).
pub type BlockValues = Vec<Vec<u8>>;

/// A byte threshold a block is "long" at, for the saturation arm.
pub const SATURATION_BYTES: usize = 1024;

pub const GO_FALSE_POSITIVE_BAR: f64 = 0.10;
pub const GO_BYTES_BAR: f64 = 0.01;
pub const GO_CPU_BAR: f64 = 2.0;
pub const GO_SATURATION_BAR: f64 = 0.30;

pub const DECISION_RULE: &str = "Per subject: GO when unseen/equals/false_positive_share and \
unseen/like/false_positive_share are both at most 0.10, summary_bytes_share is at most 0.01, \
build_cpu_over_zstd is at most 2, and saturation/false_positive_share is at most 0.30 (or the \
subject is body-ineligible: zero blocks over 1024 bytes); otherwise NO-GO, and a missing figure \
fails its leg. Overall: GO when every subject the pre-registration names has exactly one report, \
that report is GO, and it has no problems; otherwise NO-GO.";

pub const CPU_CLOCK: &str = "CLOCK_THREAD_CPUTIME_ID of the report thread, one run per figure. \
zstd_cpu_s compresses every block's values joined with a 0x00 byte at the writer's zstd level. \
build_cpu_s (folded into build_cpu_over_zstd) builds the vocabulary from the seen workload and \
every block's bitset over it, once, on the same thread.";

/// One reported figure.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Figure {
    pub key: String,
    pub value: f64,
}

fn fig(out: &mut Vec<Figure>, key: String, value: f64) {
    out.push(Figure { key, value });
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
    let out = std::hint::black_box(f());
    (out, thread_cpu_s() - t)
}

/// Every breach of the pre-registration: a band whose figure is missing or
/// present more than once, a figure outside its band, a figure with no band.
/// A missing figure is always a breach, never defaulted to a passing value:
/// [`tests::a_missing_figure_is_never_silently_in_band`] pins this.
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

/// One predicate the report evaluates, with the real baseline the
/// orchestration module (`string_fingerprint_report`) already measured
/// against production code over the same blocks.
#[derive(Clone, Debug)]
pub struct PredicateInput {
    pub id: String,
    pub kind: Kind,
    pub literal: Vec<u8>,
    /// `ScanStats::blocks_after_bloom` for an `Equals` predicate on this
    /// literal; `None` for a `Like` predicate.
    pub bloom_blocks_kept: Option<u64>,
    /// `ScanStats::contains_cells_tested` for a `Like` predicate on this
    /// literal; `None` for an `Equals` predicate.
    pub contains_cells_tested: Option<u64>,
}

/// What the report hands the measurement for one subject.
pub struct SubjectInput<'a> {
    pub lines: u64,
    pub blocks: &'a [BlockValues],
    pub zstd_level: i32,
    pub seen: &'a [PredicateInput],
    pub unseen: &'a [PredicateInput],
}

#[derive(Debug, Serialize)]
pub struct SubjectReport {
    pub subject: String,
    pub params: ParamsOut,
    pub decision_rule: &'static str,
    pub feature_layout: &'static str,
    pub cpu_clock: &'static str,
    pub figures: Vec<Figure>,
    pub bands: BTreeMap<String, Band>,
    pub problems: Vec<String>,
    pub decision: Decision,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct ParamsOut {
    pub block_records: usize,
    pub gram_len: usize,
    pub df_threshold: f64,
    pub budget_share: f64,
}

impl SubjectReport {
    /// The parameters the run used, then one line per figure, then the
    /// decision line.
    pub fn lines(&self) -> Vec<String> {
        let mut out = vec![format!(
            "string_fingerprint params block_records={} gram_len={} df_threshold={} budget_share={}",
            self.params.block_records,
            self.params.gram_len,
            self.params.df_threshold,
            self.params.budget_share
        )];
        out.extend(self.figures.iter().map(|f| match self.bands.get(&f.key) {
            Some(b) => {
                let ok = if b[0] <= f.value && f.value <= b[1] {
                    "ok"
                } else {
                    "MISS"
                };
                format!(
                    "string_fingerprint figure {} = {} band [{}, {}] {ok}",
                    f.key, f.value, b[0], b[1]
                )
            }
            None => format!(
                "string_fingerprint figure {} = {} band none MISS",
                f.key, f.value
            ),
        }));
        out.push(self.decision.line.clone());
        out
    }
}

/// The built summary a subject's predicates are evaluated against.
struct Model<'a> {
    vocab: &'a Vocabulary,
    summaries: &'a [BlockSummary],
    gram_len: usize,
}

/// True holders and the summary's survivors, per block, for one predicate
/// restricted to `which` block indices. Errors if the summary ever drops a
/// true holder.
fn evaluate(
    model: &Model<'_>,
    kind: Kind,
    literal: &[u8],
    blocks: &[BlockValues],
    which: &[usize],
    id: &str,
) -> Result<(u64, u64), String> {
    let mut surviving = 0u64;
    let mut held = 0u64;
    for &i in which {
        let holds = match kind {
            Kind::Equals => blocks[i].iter().any(|v| v.as_slice() == literal),
            Kind::Like => blocks[i].iter().any(|v| {
                v.windows(literal.len().max(1)).any(|w| w == literal) || literal.is_empty()
            }),
        };
        let survives = match kind {
            Kind::Equals => survives_equals(&model.summaries[i], model.vocab, literal),
            Kind::Like => survives_like(&model.summaries[i], model.vocab, literal, model.gram_len),
        };
        if holds {
            held += 1;
            if !survives {
                return Err(format!(
                    "predicate {id} dropped block {i}, which truly holds it"
                ));
            }
        }
        if survives {
            surviving += 1;
        }
    }
    Ok((surviving, held))
}

fn pooled_false_positive_share(
    model: &Model<'_>,
    preds: &[&PredicateInput],
    blocks: &[BlockValues],
    which: &[usize],
) -> Result<f64, String> {
    let mut surviving_total = 0u64;
    let mut fp_total = 0u64;
    for p in preds {
        let (surviving, held) = evaluate(model, p.kind, &p.literal, blocks, which, &p.id)?;
        surviving_total += surviving;
        fp_total += surviving - held;
    }
    Ok(if surviving_total == 0 {
        0.0
    } else {
        fp_total as f64 / surviving_total as f64
    })
}

/// Builds the summary over `input.blocks` from `spec`'s seen workload,
/// answers every seen and unseen predicate, checks the figures against
/// their bands, and returns the decision. A predicate the summary drops a
/// true holder of is an error, not a figure.
pub fn measure(spec: &SubjectSpec, input: &SubjectInput<'_>) -> Result<SubjectReport, String> {
    let params = Params {
        gram_len: spec.gram_len,
        df_threshold: spec.df_threshold,
        budget_share: spec.budget_share,
    };
    let blocks = input.blocks;
    let n_blocks = blocks.len() as u64;
    let all_blocks: Vec<usize> = (0..blocks.len()).collect();

    let (zstd_sizes, zstd_cpu) = cpu_time(|| {
        blocks
            .iter()
            .map(|b| zstd::bulk::compress(&b.join(&0u8), input.zstd_level).map(|c| c.len()))
            .collect::<Result<Vec<usize>, _>>()
    });
    let zstd_sizes = zstd_sizes.map_err(|e| format!("zstd: {e}"))?;
    let compressed_bytes: u64 = zstd_sizes.iter().map(|&n| n as u64).sum();

    let seen_equals: Vec<Vec<u8>> = input
        .seen
        .iter()
        .filter(|p| p.kind == Kind::Equals)
        .map(|p| p.literal.clone())
        .collect();
    let seen_like: Vec<Vec<u8>> = input
        .seen
        .iter()
        .filter(|p| p.kind == Kind::Like)
        .map(|p| p.literal.clone())
        .collect();

    let ((vocab, summaries), build_cpu) = cpu_time(|| {
        let vocab = Vocabulary::build(&seen_equals, &seen_like, blocks, &params, compressed_bytes);
        let summaries: Vec<BlockSummary> = blocks
            .iter()
            .map(|b| BlockSummary::build(b, &vocab))
            .collect();
        (vocab, summaries)
    });

    let summary_bytes: u64 =
        vocab.bytes() as u64 + summaries.iter().map(|s| s.bytes() as u64).sum::<u64>();

    let model = Model {
        vocab: &vocab,
        summaries: &summaries,
        gram_len: spec.gram_len,
    };
    let s = &spec.name;
    let mut figures = Vec::new();
    fig(&mut figures, format!("{s}/lines"), input.lines as f64);
    fig(&mut figures, format!("{s}/blocks"), n_blocks as f64);
    fig(
        &mut figures,
        format!("{s}/compressed_bytes"),
        compressed_bytes as f64,
    );
    fig(&mut figures, format!("{s}/zstd_cpu_s"), zstd_cpu);
    fig(
        &mut figures,
        format!("{s}/build_cpu_over_zstd"),
        if zstd_cpu > 0.0 {
            build_cpu / zstd_cpu
        } else {
            f64::INFINITY
        },
    );
    fig(
        &mut figures,
        format!("{s}/summary_bytes_share"),
        share(summary_bytes, compressed_bytes),
    );

    for (group_name, preds) in [("seen", input.seen), ("unseen", input.unseen)] {
        for kind in [Kind::Equals, Kind::Like] {
            let kind_name = if kind == Kind::Equals {
                "equals"
            } else {
                "like"
            };
            let subset: Vec<&PredicateInput> = preds.iter().filter(|p| p.kind == kind).collect();
            let value = pooled_false_positive_share(&model, &subset, blocks, &all_blocks)?;
            fig(
                &mut figures,
                format!("{s}/{group_name}/{kind_name}/false_positive_share"),
                value,
            );
        }
    }

    let equals_preds: Vec<&PredicateInput> = input
        .seen
        .iter()
        .chain(input.unseen)
        .filter(|p| p.kind == Kind::Equals)
        .collect();
    let bloom_share = {
        let shares: Vec<f64> = equals_preds
            .iter()
            .filter_map(|p| p.bloom_blocks_kept.map(|k| share(k, n_blocks)))
            .collect();
        if shares.is_empty() {
            return Err(format!("{s}: no equals predicate carries a bloom baseline"));
        }
        shares.iter().sum::<f64>() / shares.len() as f64
    };
    fig(
        &mut figures,
        format!("{s}/bloom/equals/blocks_kept_share"),
        bloom_share,
    );

    let like_preds: Vec<&PredicateInput> = input
        .seen
        .iter()
        .chain(input.unseen)
        .filter(|p| p.kind == Kind::Like)
        .collect();
    let total_cells: u64 = blocks.iter().map(|b| b.len() as u64).sum();
    let contains_share = {
        let shares: Vec<f64> = like_preds
            .iter()
            .filter_map(|p| p.contains_cells_tested.map(|c| share(c, total_cells)))
            .collect();
        if shares.is_empty() {
            return Err(format!(
                "{s}: no like predicate carries a contains baseline"
            ));
        }
        shares.iter().sum::<f64>() / shares.len() as f64
    };
    fig(
        &mut figures,
        format!("{s}/contains/like/cells_tested_share"),
        contains_share,
    );

    let long_blocks: Vec<usize> = (0..blocks.len())
        .filter(|&i| blocks[i].iter().any(|v| v.len() > SATURATION_BYTES))
        .collect();
    let saturation_eligible = !long_blocks.is_empty();
    let saturation_value = if saturation_eligible {
        let unseen_like: Vec<&PredicateInput> = input
            .unseen
            .iter()
            .filter(|p| p.kind == Kind::Like)
            .collect();
        pooled_false_positive_share(&model, &unseen_like, blocks, &long_blocks)?
    } else {
        0.0
    };
    fig(
        &mut figures,
        format!("{s}/saturation/false_positive_share"),
        saturation_value,
    );

    let bands = spec.bands();
    let problems = check_figures(&figures, &bands);
    let decision = decide(&decision_input(spec, &figures), saturation_eligible);
    Ok(SubjectReport {
        subject: spec.name.clone(),
        params: ParamsOut {
            block_records: spec.block_records,
            gram_len: spec.gram_len,
            df_threshold: spec.df_threshold,
            budget_share: spec.budget_share,
        },
        decision_rule: DECISION_RULE,
        feature_layout: summary::FEATURE_LAYOUT,
        cpu_clock: CPU_CLOCK,
        figures,
        bands,
        problems,
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
    pub saturation_fp: Option<f64>,
}

/// Reads the decision's figures for `spec` out of `figures`.
pub fn decision_input(spec: &SubjectSpec, figures: &[Figure]) -> DecisionInput {
    use std::collections::HashMap;
    let mut by_key: HashMap<&str, Vec<f64>> = HashMap::new();
    for f in figures {
        by_key.entry(f.key.as_str()).or_default().push(f.value);
    }
    let one = |key: String| match by_key.get(key.as_str()).map(Vec::as_slice) {
        Some([v]) => Some(*v),
        _ => None,
    };
    let s = &spec.name;
    DecisionInput {
        subject: s.clone(),
        unseen_equals_fp: one(format!("{s}/unseen/equals/false_positive_share")),
        unseen_like_fp: one(format!("{s}/unseen/like/false_positive_share")),
        summary_bytes_share: one(format!("{s}/summary_bytes_share")),
        build_cpu_over_zstd: one(format!("{s}/build_cpu_over_zstd")),
        saturation_fp: one(format!("{s}/saturation/false_positive_share")),
    }
}

/// A go/no-go and the one line that states it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Decision {
    pub subject: String,
    pub go: bool,
    pub line: String,
}

fn shown(v: Option<f64>) -> String {
    v.map_or_else(|| "missing".to_string(), |v| format!("{v:.4}"))
}

/// The per-subject rule of [`DECISION_RULE`]. `saturation_eligible` is
/// `false` when the subject had no block over [`SATURATION_BYTES`]; the
/// saturation leg then always passes (there was nothing to test) and the
/// line says so.
pub fn decide(d: &DecisionInput, saturation_eligible: bool) -> Decision {
    let equals_ok = d
        .unseen_equals_fp
        .is_some_and(|v| v <= GO_FALSE_POSITIVE_BAR);
    let like_ok = d.unseen_like_fp.is_some_and(|v| v <= GO_FALSE_POSITIVE_BAR);
    let bytes_ok = d.summary_bytes_share.is_some_and(|v| v <= GO_BYTES_BAR);
    let cpu_ok = d.build_cpu_over_zstd.is_some_and(|v| v <= GO_CPU_BAR);
    let saturation_ok =
        !saturation_eligible || d.saturation_fp.is_some_and(|v| v <= GO_SATURATION_BAR);
    let go = equals_ok && like_ok && bytes_ok && cpu_ok && saturation_ok;
    let fails: Vec<&str> = [
        (equals_ok, "unseen_equals"),
        (like_ok, "unseen_like"),
        (bytes_ok, "bytes"),
        (cpu_ok, "cpu"),
        (saturation_ok, "saturation"),
    ]
    .iter()
    .filter(|(ok, _)| !ok)
    .map(|(_, name)| *name)
    .collect();
    let line = format!(
        "string_fingerprint decision {}: {}; unseen_equals_fp={} unseen_like_fp={} (bar {GO_FALSE_POSITIVE_BAR}); \
         summary_bytes_share={} (bar {GO_BYTES_BAR}); build_cpu_over_zstd={} (bar {GO_CPU_BAR}); \
         saturation_fp={} (bar {GO_SATURATION_BAR}{}); fails: {}",
        d.subject,
        if go { "GO" } else { "NO-GO" },
        shown(d.unseen_equals_fp),
        shown(d.unseen_like_fp),
        shown(d.summary_bytes_share),
        shown(d.build_cpu_over_zstd),
        shown(d.saturation_fp),
        if saturation_eligible {
            ""
        } else {
            ", body-ineligible: vacuous pass"
        },
        if fails.is_empty() {
            "none".to_string()
        } else {
            fails.join(", ")
        },
    );
    Decision {
        subject: d.subject.clone(),
        go,
        line,
    }
}

/// A subject decision read back from a written report, with its problem
/// count.
#[derive(Clone, Debug)]
pub struct SavedDecision {
    pub decision: Decision,
    pub problems: usize,
}

/// The `string_fingerprint` section's decision from a report the binary
/// wrote.
pub fn saved_decision(json: &str) -> Result<SavedDecision, String> {
    #[derive(Deserialize)]
    struct Saved {
        decision: Decision,
        problems: Vec<String>,
    }
    let saved: Saved = serde_json::from_str(json).map_err(|e| format!("report: {e}"))?;
    Ok(SavedDecision {
        decision: saved.decision,
        problems: saved.problems.len(),
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
        line: format!(
            "string_fingerprint decision overall: {}; {}",
            if go { "GO" } else { "NO-GO" },
            parts.join(", ")
        ),
    }
}

#[cfg(test)]
mod tests;
