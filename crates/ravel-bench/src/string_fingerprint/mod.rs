//! A report-only experiment (issue #3710, ADR-3692 Plan row T12): measures a
//! learned, conservative per-block string summary for equality and
//! `LIKE '%literal%'` predicates on a declared Str column, against the
//! production per-block word-token Bloom and a modeled proxy for the
//! production `containsCellsTested` baseline, with a pre-registered
//! go/no-go. Nothing outside the report reads or writes the summary.
//!
//! The hypothesis (arXiv:2507.10391): a block summary trained on a bounded,
//! known workload (the `seen` predicates) generalizes to predicates it never
//! saw (`unseen`), at a fraction of a bloom filter's or a full scan's cost.
//! This reuses [`crate::logbody::grams::GramIndex`] unmodified: its
//! `workload` parameter already restricts candidate-gram generation to a
//! given predicate set, which is exactly the training mechanic this
//! hypothesis needs. One index is built per column, trained on
//! `sample_size` of its `seen` predicates (clamped to however many there
//! are), and answers every predicate -- `seen` figures are in-sample,
//! `unseen` figures are the out-of-sample result the decision rests on.
//!
//! Three things are measured per predicate:
//!
//! - **fingerprint**, the summary under test. A block survives when it
//!   holds every kept gram that is a substring of the predicate's pattern;
//!   a pattern with no kept gram keeps every block ([`grams::GramIndex::survivors`]).
//!   This arm is conservative by construction and [`measure`] treats a
//!   dropped true holder as a hard error, pinned by a proptest in `tests`.
//! - **bloom**, the production per-block word-token filter
//!   (`ravel_codec::bloom`, built exactly as `ravel-logseg`'s writer builds
//!   it: [`ravel_logseg::writer::bloom_probe::insert_text`] per row,
//!   [`ravel_codec::bloom::BloomBuilder::finish_exact`] to close it,
//!   [`ravel_codec::bloom::BloomView::parse_exact`] to query it). It only
//!   indexes whole tokenizer tokens, so it can only answer a predicate whose
//!   pattern is itself a single ASCII alphanumeric token; such a predicate's
//!   pre-registration carries `bloom` bands, every other predicate's does
//!   not, and [`measure`] refuses a run where the two disagree. Unlike the
//!   fingerprint arm, the bloom arm can miss a true holder (a literal that
//!   occurs only inside a longer token is not itself a token), so its
//!   `blocks_missed` is reported, not asserted zero.
//! - **contains**, a modeled proxy for the production `containsCellsTested`
//!   counter on `LIKE` predicates (ADR-2773). This is NOT a replay of the
//!   real per-row counter (ravel-logseg's reader content-arm evaluation);
//!   it reuses the same bloom signal as the one real pruning mechanism
//!   production has for a single-token literal (`cells_tested` is every row
//!   of only the blocks the bloom keeps) and otherwise assumes no pruning
//!   at all (`cells_tested` is every row of every block). Treat its figures
//!   as an honestly-labeled estimate of the baseline's cost, not a
//!   byte-faithful figure.
//!
//! The **saturation** arm pools the fingerprint arm's false positives over
//! `unseen` predicates, restricted to blocks holding a value longer than
//! `long_value_bytes` ("long" blocks): [`SATURATION_BAR`] is reported as an
//! informational `body_eligible` flag on the decision line, not a gating
//! input, per the task's "marked body-ineligible" phrasing.
//!
//! Figures ([`Figure`]) are keyed `corpus/column/figure`,
//! `corpus/column/arm/figure`, `corpus/column/predicate/figure` and
//! `corpus/column/predicate/arm/figure`. Every one must be present exactly
//! once and inside its band from the pre-registration file ([`spec`]), and
//! every figure must have a band; [`check_figures`] lists each breach and
//! the binary exits 1 on any.
//!
//! CPU is the calling thread's CPU clock ([`CPU_CLOCK`]): zstd encoding of
//! every block, and the fingerprint index's build, one run each, on the
//! same thread.
//!
//! The decision ([`decide`], [`DECISION_RULE`]) is per corpus/column, and
//! overall ([`overall`]) across every corpus/column the pre-registration
//! file names. Two interpretation choices the task text leaves open, made
//! explicit here: the unseen false-positive bar is a mean across every
//! unseen predicate (the task gives no per-predicate or N-of-M rule, unlike
//! T11's "3 of 5"), and `sample_size` above the number of `seen` predicates
//! clamps rather than refuses.

pub mod spec;

use std::collections::{BTreeMap, HashMap};
use std::hint::black_box;

use ravel_codec::bloom::{BloomBuilder, BloomView};
use ravel_codec::tokenizer::tokens;
use ravel_logseg::writer::bloom_probe;
use serde::{Deserialize, Serialize};

use self::spec::{Band, ColumnSpec, Kind, Split};
use crate::logbody::grams::{self, BlockText, GramIndex, MIN_GRAM, Params, Pattern};

/// The bloom's field scope. One column is measured per run, so one id
/// suffices; it only has to be consistent between build and query here.
const COLUMN_ID: u32 = 1;
/// Arbitrary and fixed: this report compares the bloom's own false-negative
/// and false-positive behavior, not its sensitivity to a seed choice.
const BLOOM_SEED: u64 = 0xF17E_5EED;

/// Out-of-sample false positives: the mean, over every `unseen` predicate,
/// of the fingerprint arm's `false_positive_share` must be at most this.
pub const SURVIVAL_BAR: f64 = 0.10;
/// The fingerprint index's bytes must be at most this share of the column's
/// compressed bytes.
pub const BYTES_BAR: f64 = 0.01;
/// The fingerprint index's build CPU must be at most this multiple of zstd's.
pub const CPU_BAR: f64 = 2.0;
/// Below this, the saturation arm's pooled false-positive share keeps the
/// column `body_eligible`; at or above it, the column is reported
/// "body-ineligible" (informational, not gating).
pub const SATURATION_BAR: f64 = 0.30;

pub const DECISION_RULE: &str = "Per corpus/column, on the fingerprint arm: GO when the mean of \
false_positive_share over every unseen predicate is at most 0.10, index_bytes_share is at most \
0.01, and build_cpu_over_zstd is at most 2; a missing figure fails its leg. body_eligible (the \
saturation arm's pooled false_positive_share on long blocks is under 0.30) is reported but does \
not gate the decision. Overall: GO when every corpus/column the pre-registration names has \
exactly one report, that report is GO, and it has no problems; otherwise NO-GO.";

pub const CPU_CLOCK: &str = "CLOCK_THREAD_CPUTIME_ID of the report thread, one run per figure. \
zstd_cpu_s compresses every block's values joined with 0x0A at the writer's zstd level. \
fingerprint/build_cpu_s counts the candidates of the training workload over every block, \
selects, applies the byte cap and encodes. build_cpu_over_zstd is build_cpu_s over zstd_cpu_s. \
The bloom arm has no CPU figure: it is not a learned structure, and its build cost is production's \
existing per-block write-path cost, not a new one this report is pricing.";

pub const BLOOM_RULE: &str = "Built exactly as ravel-logseg's writer builds it: one \
BloomBuilder per block, every row's bytes inserted via bloom_probe::insert_text (word tokens, \
plus the exact value when at most 64 bytes), finished with finish_exact and queried with \
parse_exact. Applicable only to a predicate whose pattern is a single ASCII alphanumeric \
tokenizer token (tokens(pattern) has exactly one element equal to pattern.to_lowercase()); such \
a predicate's pre-registration carries bloom bands and every other predicate's does not, checked \
before any block is read.";

pub const CONTAINS_RULE: &str = "A modeled proxy for the production containsCellsTested counter, \
not a replay of it: cells_tested is every row of the blocks the bloom keeps when the pattern is a \
single token (the one real pruning signal available), or every row of every block otherwise (no \
pruning assumed). Reported only for contains-kind predicates.";

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

/// The single ASCII-alphanumeric tokenizer token `pattern` folds to, when it
/// is exactly one: `None` if tokenizing drops or splits any of it, which is
/// exactly when production's word-token bloom cannot answer it.
fn single_token_bytes(pattern: &str) -> Option<Vec<u8>> {
    match tokens(pattern).as_slice() {
        [t] if *t == pattern.to_lowercase().into_bytes() => Some(t.clone()),
        _ => None,
    }
}

/// What the report hands the column measurement.
pub struct ColumnInput<'a> {
    pub blocks: &'a [BlockText],
    pub compressed_bytes: u64,
    pub zstd_level: i32,
}

#[derive(Debug, Serialize)]
pub struct GramStat {
    pub gram: String,
    pub blocks: u64,
}

/// The parameters a run used, pinned by the pre-registration file.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct RunParams {
    pub gram: Params,
    pub sample_size: usize,
    pub long_value_bytes: u64,
}

#[derive(Debug, Serialize)]
pub struct ColumnReport {
    pub corpus: String,
    pub column: String,
    pub params: RunParams,
    pub decision_rule: &'static str,
    pub bloom_rule: &'static str,
    pub contains_rule: &'static str,
    pub index_layout: &'static str,
    pub cpu_clock: &'static str,
    pub figures: Vec<Figure>,
    /// The bands the figures were checked against.
    pub bands: BTreeMap<String, Band>,
    /// The fingerprint index's grams, for reading why a predicate pruned.
    pub selected_grams: Vec<GramStat>,
    pub problems: Vec<String>,
    pub decision: Decision,
}

impl ColumnReport {
    /// The parameters the run used, one line per figure, in key order, then
    /// the decision line.
    pub fn lines(&self) -> Vec<String> {
        let mut out = vec![format!(
            "string_fingerprint params df_threshold={} budget_share={} max_gram={} \
             sample_size={} long_value_bytes={}",
            self.params.gram.df_threshold,
            self.params.gram.budget_share,
            self.params.gram.max_gram,
            self.params.sample_size,
            self.params.long_value_bytes,
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

/// Whether a block of `column` holds a true match for `predicate`: exact
/// equality for `Equals`, a byte substring anywhere in some row for
/// `Contains`.
fn holds_block(kind: Kind, pattern: &str, parsed: &Pattern, block: &BlockText) -> bool {
    match kind {
        Kind::Equals => block.lines().any(|l| l == pattern.as_bytes()),
        Kind::Contains => parsed.matches_block(block),
    }
}

/// Builds the fingerprint index trained on `column`'s sampled `seen`
/// predicates, the production bloom per block, and answers every predicate,
/// checking the figures against `column`'s bands. A dropped true holder on
/// the fingerprint arm is an error, as is an applicability mismatch between
/// a predicate's pattern and its pre-registered bloom bands.
pub fn measure(
    corpus: &str,
    column: &ColumnSpec,
    input: &ColumnInput<'_>,
) -> Result<ColumnReport, String> {
    let params = Params {
        max_gram: column.max_gram,
        df_threshold: column.df_threshold,
        budget_share: column.budget_share,
    };
    if !(MIN_GRAM..=7).contains(&params.max_gram) {
        return Err(format!("max_gram {} is not in 3..=7", params.max_gram));
    }
    let blocks = input.blocks;
    let n_blocks = blocks.len() as u64;

    let parsed: Vec<Pattern> = column
        .predicate
        .iter()
        .map(|p| {
            Pattern::parse(&p.pattern)
                .ok_or_else(|| format!("predicate {}: no literal run of {MIN_GRAM} bytes", p.id))
        })
        .collect::<Result<_, _>>()?;

    for p in &column.predicate {
        let applicable = single_token_bytes(&p.pattern).is_some();
        if applicable != p.bands.bloom.is_some() {
            return Err(format!(
                "predicate {}: pattern is{} a single token but pre-registration bloom bands are{}",
                p.id,
                if applicable { "" } else { " not" },
                if p.bands.bloom.is_some() {
                    " present"
                } else {
                    " absent"
                }
            ));
        }
        if (p.kind == Kind::Contains) != p.bands.contains.is_some() {
            return Err(format!(
                "predicate {}: kind {:?} disagrees with presence of contains bands",
                p.id, p.kind
            ));
        }
    }

    let (zstd, zstd_cpu) = cpu_time(|| {
        blocks
            .iter()
            .map(|b| zstd::bulk::compress(b.bytes(), input.zstd_level).map(|c| c.len()))
            .collect::<Result<Vec<usize>, _>>()
    });
    zstd.map_err(|e| format!("zstd: {e}"))?;

    let seen_patterns: Vec<&Pattern> = column
        .predicate
        .iter()
        .zip(&parsed)
        .filter(|(p, _)| p.set == Split::Seen)
        .map(|(_, pat)| pat)
        .collect();
    let take = column.sample_size.min(seen_patterns.len());
    let train_patterns: Vec<Pattern> = seen_patterns[..take].iter().map(|p| (*p).clone()).collect();
    let (fingerprint, fp_cpu) =
        cpu_time(|| GramIndex::build(blocks, &train_patterns, input.compressed_bytes, &params));

    let bloom_bytes: Vec<Vec<u8>> = blocks
        .iter()
        .map(|b| {
            let mut builder = BloomBuilder::new(BLOOM_SEED);
            for line in b.lines() {
                bloom_probe::insert_text(&mut builder, COLUMN_ID, line);
            }
            builder.finish_exact()
        })
        .collect();
    let bloom_views: Vec<BloomView<'_>> = bloom_bytes
        .iter()
        .map(|b| BloomView::parse_exact(b).map_err(|e| format!("bloom: {e}")))
        .collect::<Result<_, _>>()?;

    let long_blocks: Vec<bool> = blocks
        .iter()
        .map(|b| b.lines().any(|l| l.len() as u64 > column.long_value_bytes))
        .collect();
    let blocks_long = long_blocks.iter().filter(|&&x| x).count() as u64;

    let prefix = format!("{corpus}/{}", column.name);
    let mut figures = Vec::new();
    fig(&mut figures, format!("{prefix}/blocks"), n_blocks as f64);
    fig(
        &mut figures,
        format!("{prefix}/compressed_bytes"),
        input.compressed_bytes as f64,
    );
    fig(&mut figures, format!("{prefix}/zstd_cpu_s"), zstd_cpu);
    fig(
        &mut figures,
        format!("{prefix}/fingerprint/index_bytes"),
        fingerprint.bytes() as f64,
    );
    fig(
        &mut figures,
        format!("{prefix}/fingerprint/index_bytes_share"),
        share(fingerprint.bytes(), input.compressed_bytes),
    );
    fig(
        &mut figures,
        format!("{prefix}/fingerprint/build_cpu_s"),
        fp_cpu,
    );
    fig(
        &mut figures,
        format!("{prefix}/fingerprint/build_cpu_over_zstd"),
        if zstd_cpu > 0.0 {
            fp_cpu / zstd_cpu
        } else {
            f64::INFINITY
        },
    );
    fig(
        &mut figures,
        format!("{prefix}/fingerprint/grams_selected"),
        fingerprint.grams.len() as f64,
    );
    fig(
        &mut figures,
        format!("{prefix}/saturation/blocks_long"),
        blocks_long as f64,
    );

    let mut sat_fp: u64 = 0;
    let mut sat_surv: u64 = 0;

    for (p, pat) in column.predicate.iter().zip(&parsed) {
        let id = &p.id;
        let pred_prefix = format!("{prefix}/{id}");
        let holders: Vec<bool> = blocks
            .iter()
            .map(|b| holds_block(p.kind, &p.pattern, pat, b))
            .collect();
        let held = holders.iter().filter(|&&h| h).count() as u64;
        fig(
            &mut figures,
            format!("{pred_prefix}/true_holders"),
            held as f64,
        );

        let fp_survivors = fingerprint.survivors(pat);
        if let Some(b) = holders
            .iter()
            .zip(&fp_survivors)
            .position(|(&h, &k)| h && !k)
        {
            return Err(format!(
                "fingerprint arm dropped block {b} holding predicate {id}"
            ));
        }
        let fp_surviving = fp_survivors.iter().filter(|&&k| k).count() as u64;
        let fp_fp = fp_surviving - held;
        fig(
            &mut figures,
            format!("{pred_prefix}/fingerprint/blocks_surviving"),
            fp_surviving as f64,
        );
        fig(
            &mut figures,
            format!("{pred_prefix}/fingerprint/survival_share"),
            survival_share(fp_surviving, n_blocks),
        );
        fig(
            &mut figures,
            format!("{pred_prefix}/fingerprint/blocks_missed"),
            0.0,
        );
        fig(
            &mut figures,
            format!("{pred_prefix}/fingerprint/false_positive_share"),
            if fp_surviving == 0 {
                0.0
            } else {
                fp_fp as f64 / fp_surviving as f64
            },
        );

        if p.set == Split::Unseen {
            for ((&h, &k), &long) in holders.iter().zip(&fp_survivors).zip(&long_blocks) {
                if !long {
                    continue;
                }
                if k {
                    sat_surv += 1;
                    if !h {
                        sat_fp += 1;
                    }
                }
            }
        }

        let bloom_survivors: Option<Vec<bool>> = single_token_bytes(&p.pattern).map(|t| {
            bloom_views
                .iter()
                .map(|v| v.may_contain(COLUMN_ID, &t))
                .collect()
        });
        if let Some(bs) = &bloom_survivors {
            let mut surv = 0u64;
            let mut tp = 0u64;
            let mut missed = 0u64;
            for (&h, &k) in holders.iter().zip(bs) {
                if k {
                    surv += 1;
                    if h {
                        tp += 1;
                    }
                } else if h {
                    missed += 1;
                }
            }
            let fp = surv - tp;
            fig(
                &mut figures,
                format!("{pred_prefix}/bloom/blocks_surviving"),
                surv as f64,
            );
            fig(
                &mut figures,
                format!("{pred_prefix}/bloom/survival_share"),
                survival_share(surv, n_blocks),
            );
            fig(
                &mut figures,
                format!("{pred_prefix}/bloom/blocks_missed"),
                missed as f64,
            );
            fig(
                &mut figures,
                format!("{pred_prefix}/bloom/false_positive_share"),
                if surv == 0 {
                    0.0
                } else {
                    fp as f64 / surv as f64
                },
            );
        }

        if p.kind == Kind::Contains {
            let cells_total: u64 = blocks.iter().map(|b| b.lines().count() as u64).sum();
            let cells_tested: u64 = match &bloom_survivors {
                Some(bs) => blocks
                    .iter()
                    .zip(bs)
                    .filter(|&(_, &k)| k)
                    .map(|(b, _)| b.lines().count() as u64)
                    .sum(),
                None => cells_total,
            };
            fig(
                &mut figures,
                format!("{pred_prefix}/contains/cells_tested"),
                cells_tested as f64,
            );
            fig(
                &mut figures,
                format!("{pred_prefix}/contains/cells_total"),
                cells_total as f64,
            );
            fig(
                &mut figures,
                format!("{pred_prefix}/contains/cells_tested_share"),
                share(cells_tested, cells_total),
            );
        }
    }

    fig(
        &mut figures,
        format!("{prefix}/saturation/false_positive_share"),
        if sat_surv == 0 {
            0.0
        } else {
            sat_fp as f64 / sat_surv as f64
        },
    );

    let bands: BTreeMap<String, Band> = column
        .bands()
        .into_iter()
        .map(|(k, v)| (format!("{prefix}/{k}"), v))
        .collect();
    let problems = check_figures(&figures, &bands);
    let run_params = RunParams {
        gram: params,
        sample_size: column.sample_size,
        long_value_bytes: column.long_value_bytes,
    };
    let decision = decide(&decision_input(corpus, column, &figures));
    Ok(ColumnReport {
        corpus: corpus.to_string(),
        column: column.name.clone(),
        params: run_params,
        decision_rule: DECISION_RULE,
        bloom_rule: BLOOM_RULE,
        contains_rule: CONTAINS_RULE,
        index_layout: grams::INDEX_LAYOUT,
        cpu_clock: CPU_CLOCK,
        figures,
        bands,
        selected_grams: fingerprint
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

/// The figures a corpus/column decision rests on; `None` for one not
/// reported exactly once.
#[derive(Clone, Debug)]
pub struct DecisionInput {
    pub corpus: String,
    pub column: String,
    /// `(predicate id, fingerprint false_positive_share)` per unseen predicate.
    pub unseen_false_positive_share: Vec<(String, Option<f64>)>,
    pub index_bytes_share: Option<f64>,
    pub build_cpu_over_zstd: Option<f64>,
    pub saturation_false_positive_share: Option<f64>,
}

/// Reads the decision's figures for `column` out of `figures`.
pub fn decision_input(corpus: &str, column: &ColumnSpec, figures: &[Figure]) -> DecisionInput {
    let mut by_key: HashMap<&str, Vec<f64>> = HashMap::new();
    for f in figures {
        by_key.entry(f.key.as_str()).or_default().push(f.value);
    }
    let one = |key: String| match by_key.get(key.as_str()).map(Vec::as_slice) {
        Some([v]) => Some(*v),
        _ => None,
    };
    let prefix = format!("{corpus}/{}", column.name);
    DecisionInput {
        corpus: corpus.to_string(),
        column: column.name.clone(),
        unseen_false_positive_share: column
            .predicate
            .iter()
            .filter(|p| p.set == Split::Unseen)
            .map(|p| {
                (
                    p.id.clone(),
                    one(format!(
                        "{prefix}/{}/fingerprint/false_positive_share",
                        p.id
                    )),
                )
            })
            .collect(),
        index_bytes_share: one(format!("{prefix}/fingerprint/index_bytes_share")),
        build_cpu_over_zstd: one(format!("{prefix}/fingerprint/build_cpu_over_zstd")),
        saturation_false_positive_share: one(format!("{prefix}/saturation/false_positive_share")),
    }
}

/// A go/no-go and the one line that states it. `corpus` holds the qualified
/// `corpus/column` name.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Decision {
    pub corpus: String,
    pub go: bool,
    pub line: String,
}

fn shown(v: Option<f64>) -> String {
    v.map_or_else(|| "missing".to_string(), |v| format!("{v:.4}"))
}

/// The per corpus/column rule of [`DECISION_RULE`].
pub fn decide(d: &DecisionInput) -> Decision {
    let qualified = format!("{}/{}", d.corpus, d.column);
    let have_all = !d.unseen_false_positive_share.is_empty()
        && d.unseen_false_positive_share
            .iter()
            .all(|(_, s)| s.is_some());
    let mean = have_all.then(|| {
        let sum: f64 = d
            .unseen_false_positive_share
            .iter()
            .filter_map(|(_, s)| *s)
            .sum();
        sum / d.unseen_false_positive_share.len() as f64
    });
    let survival_ok = mean.is_some_and(|m| m <= SURVIVAL_BAR);
    let bytes_ok = d.index_bytes_share.is_some_and(|v| v <= BYTES_BAR);
    let cpu_ok = d.build_cpu_over_zstd.is_some_and(|v| v <= CPU_BAR);
    let go = survival_ok && bytes_ok && cpu_ok;
    let body_eligible = d
        .saturation_false_positive_share
        .is_some_and(|v| v < SATURATION_BAR);
    let fails: Vec<&str> = [
        (survival_ok, "survival"),
        (bytes_ok, "bytes"),
        (cpu_ok, "cpu"),
    ]
    .iter()
    .filter(|(ok, _)| !ok)
    .map(|(_, name)| *name)
    .collect();
    let per_predicate: Vec<String> = d
        .unseen_false_positive_share
        .iter()
        .map(|(id, s)| format!("{id}={}", shown(*s)))
        .collect();
    let line = format!(
        "string_fingerprint decision {qualified}: {}; unseen fingerprint false_positive_share \
         mean={} (bar {SURVIVAL_BAR}) over [{}]; index_bytes_share={} (bar {BYTES_BAR}); \
         build_cpu_over_zstd={} (bar {CPU_BAR}); body_eligible={body_eligible} \
         (saturation false_positive_share={}, bar < {SATURATION_BAR}, informational); fails: {}",
        if go { "GO" } else { "NO-GO" },
        shown(mean),
        per_predicate.join(" "),
        shown(d.index_bytes_share),
        shown(d.build_cpu_over_zstd),
        shown(d.saturation_false_positive_share),
        if fails.is_empty() {
            "none".to_string()
        } else {
            fails.join(", ")
        },
    );
    Decision {
        corpus: qualified,
        go,
        line,
    }
}

/// A corpus/column decision read back from a written report, with its
/// problem count.
#[derive(Clone, Debug)]
pub struct SavedDecision {
    pub decision: Decision,
    pub problems: usize,
}

/// The `string_fingerprint` section's decision from a report the binary wrote.
pub fn saved_decision(json: &str) -> Result<SavedDecision, String> {
    #[derive(Deserialize)]
    struct Saved {
        string_fingerprint: Option<SavedColumn>,
    }
    #[derive(Deserialize)]
    struct SavedColumn {
        problems: Vec<String>,
        decision: Decision,
    }
    let saved: Saved = serde_json::from_str(json).map_err(|e| format!("report: {e}"))?;
    let sf = saved
        .string_fingerprint
        .ok_or("report has no string_fingerprint section")?;
    Ok(SavedDecision {
        decision: sf.decision,
        problems: sf.problems.len(),
    })
}

/// The overall rule of [`DECISION_RULE`] over `columns`, the `corpus/column`
/// names the pre-registration file lists.
pub fn overall(columns: &[String], saved: &[SavedDecision]) -> Decision {
    let mut go = !columns.is_empty();
    let mut parts = Vec::new();
    for c in columns {
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
        if !columns.contains(&s.decision.corpus) {
            go = false;
            parts.push(format!("{}=not pre-registered", s.decision.corpus));
        }
    }
    Decision {
        corpus: "overall".to_string(),
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
