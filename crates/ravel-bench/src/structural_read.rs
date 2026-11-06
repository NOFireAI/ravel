//! T13 arm 1 (issue #3711, ADR-3692 work package 4): the journey's row fetch
//! (a handful of matched rows from one ~10 MiB RLOG object whose block set is
//! already known) through today's ranged plan against a structural read in
//! the sense of Lance (arXiv:2504.15247), which reads only the needed columns
//! and the pages, or mini-block chunks, that hold the needed rows.
//!
//! Three plans run against the same object in a store that sleeps a
//! registered latency before every GET:
//!
//! - `today`: the production [`ravel_query::LogSegmentFetcher`] subset path,
//!   called through its public API with the wanted block set. Its GETs and
//!   bytes are what that code issues.
//! - `page_exact`: a real decode through the public `ravel-logseg` reader API.
//!   A registered-size suffix GET, one coalesced round for the directory
//!   sections the suffix missed, then exactly the projected pages of the
//!   wanted blocks ([`ravel_logseg::page_dir::PageDir::projected_page_ranges`]),
//!   coalesced by [`crate::t13::coalesce`] with no run cap and no whole-object
//!   fallback, decoded by `scan_blocks_raw_subset`.
//! - `miniblock`: a faithful simulation of Lance's mini-block layout. Each
//!   (block, projected column) page's values are cut into power-of-two row
//!   chunks of at most a registered compressed size ([`mini_chunks`]); each
//!   chunk is placed inside the real page's byte extent in proportion to its
//!   compressed size ([`place_chunks`]), so byte and GET counts come from the
//!   object's real layout. The GETs are issued against the real object at
//!   those positions; decode CPU is the decompression of the simulated
//!   chunks. Chunk metadata is charged at a registered size per chunk over
//!   every projected column of the object, on every fetch.
//!
//! The wall figure is [`wall_ms`]: measured elapsed time (injected latency
//! included) plus the bytes over a registered bandwidth.

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::t13::{Decision, Expect, Figure, SavedDecision, validate_figure_names};

pub const ARM: &str = "structural_read";
pub const SHAPES: [&str; 2] = ["scattered", "clustered"];
pub const VARIANTS: [&str; 2] = ["page_exact", "miniblock"];

/// GO thresholds (issue #3711): against today's plan, bytes at most 0.25x,
/// GETs at most 1.5x, wall not worse, on every subject and shape.
pub const MAX_BYTES_RATIO: f64 = 0.25;
pub const MAX_GETS_RATIO: f64 = 1.5;
pub const MAX_WALL_RATIO: f64 = 1.0;

pub const DECISION_RULE: &str = "A variant (page_exact or miniblock) is GO on a subject when, \
for both row shapes, bytes_ratio <= 0.25, gets_ratio <= 1.5 and wall_ratio <= 1.0 against \
today's plan. The arm is GO when some variant is GO on every pre-registered subject.";

/// Every figure a subject reports, keyed without the subject prefix.
pub fn figure_names() -> Vec<String> {
    let mut out = vec![
        "object/lines".to_string(),
        "object/bytes".to_string(),
        "object/blocks".to_string(),
        "miniblock/meta_bytes".to_string(),
    ];
    for shape in SHAPES {
        out.push(format!("{shape}/blocks_touched"));
        for f in ["gets", "bytes", "wall_ms", "decode_cpu_ms"] {
            out.push(format!("{shape}/today/{f}"));
        }
        for v in VARIANTS {
            for f in [
                "gets",
                "bytes",
                "decode_cpu_ms",
                "bytes_ratio",
                "gets_ratio",
                "wall_ratio",
            ] {
                out.push(format!("{shape}/{v}/{f}"));
            }
        }
    }
    out
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Spec {
    pub schema: u32,
    pub arm: String,
    pub subject: Vec<SubjectSpec>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubjectSpec {
    pub name: String,
    pub corpus: String,
    pub source: String,
    /// The corpus file's sha256; any other file is refused before it is read.
    pub corpus_sha256: String,
    /// Lines read from the corpus at most.
    pub max_lines: u64,
    /// The object is written from the first N lines, N calibrated so the
    /// object lands within `object_tolerance` of this size.
    pub object_target_bytes: u64,
    pub object_tolerance: f64,
    pub block_records: usize,
    pub zstd_level: i32,
    /// Matched rows per fetch.
    pub rows: usize,
    pub seed: u64,
    pub get_latency_ms: u64,
    pub bandwidth_mib_s: f64,
    pub structural_suffix_bytes: u64,
    pub coalesce_gap: u64,
    pub concurrency: usize,
    pub reps: usize,
    pub mini_max_rows: usize,
    pub mini_chunk_bytes: usize,
    pub mini_meta_bytes_per_chunk: u64,
    pub figures: BTreeMap<String, Expect>,
}

pub fn parse(text: &str) -> Result<Spec, String> {
    let spec: Spec = toml::from_str(text).map_err(|e| format!("{ARM} spec: {e}"))?;
    if spec.schema != 1 || spec.arm != ARM {
        return Err(format!(
            "{ARM} spec: schema {} arm {:?} is not schema 1 arm {ARM:?}",
            spec.schema, spec.arm
        ));
    }
    let mut names = std::collections::HashSet::new();
    for s in &spec.subject {
        if !names.insert(s.name.as_str()) {
            return Err(format!("{ARM} spec: subject {} twice", s.name));
        }
        s.validate()?;
    }
    if spec.subject.is_empty() {
        return Err(format!("{ARM} spec: no subject"));
    }
    Ok(spec)
}

impl Spec {
    pub fn subject(&self, name: &str) -> Option<&SubjectSpec> {
        self.subject.iter().find(|s| s.name == name)
    }
}

impl SubjectSpec {
    fn validate(&self) -> Result<(), String> {
        let at = |m: &str| format!("{ARM} spec: subject {}: {m}", self.name);
        if self.max_lines == 0
            || self.object_target_bytes == 0
            || self.block_records == 0
            || self.rows == 0
            || self.concurrency == 0
            || self.reps == 0
            || self.mini_max_rows == 0
            || self.mini_chunk_bytes == 0
            || self.structural_suffix_bytes < 16
        {
            return Err(at(
                "max_lines, object_target_bytes, block_records, rows, concurrency, reps, \
                 mini_max_rows and mini_chunk_bytes must be nonzero and structural_suffix_bytes \
                 at least the 16-byte trailer",
            ));
        }
        if !(self.object_tolerance > 0.0 && self.object_tolerance < 1.0) {
            return Err(at("object_tolerance must be in (0, 1)"));
        }
        if !(self.bandwidth_mib_s > 0.0 && self.bandwidth_mib_s.is_finite()) {
            return Err(at("bandwidth_mib_s must be positive"));
        }
        if self.corpus_sha256.len() != 64 {
            return Err(at("corpus_sha256 must be 64 hex digits"));
        }
        validate_figure_names(
            &format!("{ARM} spec: subject {}", self.name),
            &self.figures,
            &figure_names(),
        )
    }

    pub fn expectations(&self) -> BTreeMap<String, Expect> {
        self.figures
            .iter()
            .map(|(k, v)| (format!("{}/{k}", self.name), v.clone()))
            .collect()
    }
}

/// One mini-block chunk of a (block, column) page: `rows` rows from
/// `first_row`, `stored` bytes once compressed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MiniChunk {
    pub first_row: usize,
    pub rows: usize,
    pub stored: usize,
}

/// Lance's mini-block rule as simulated here: the largest power-of-two row
/// count, at most `max_rows`, for which every chunk's compressed size is at
/// most `max_chunk` bytes; one row per chunk when even that does not fit.
/// `values` are the rows' serialized values; `compress` gives the stored size
/// of a chunk's concatenated values.
pub fn mini_chunks(
    values: &[Vec<u8>],
    max_rows: usize,
    max_chunk: usize,
    compress: &dyn Fn(&[u8]) -> Result<usize, String>,
) -> Result<Vec<MiniChunk>, String> {
    if values.is_empty() {
        return Ok(Vec::new());
    }
    let mut r = 1usize;
    while r * 2 <= max_rows.max(1) {
        r *= 2;
    }
    loop {
        let mut out = Vec::with_capacity(values.len().div_ceil(r));
        let mut fits = true;
        for (i, rows) in values.chunks(r).enumerate() {
            let stored = compress(&rows.concat())?;
            fits &= stored <= max_chunk;
            out.push(MiniChunk {
                first_row: i * r,
                rows: rows.len(),
                stored,
            });
        }
        if fits || r == 1 {
            return Ok(out);
        }
        r /= 2;
    }
}

/// The chunks (indices into `chunks`) holding `rows`, ascending, each once.
pub fn chunks_for_rows(chunks: &[MiniChunk], rows: &[u32]) -> Vec<usize> {
    let mut out: Vec<usize> = rows
        .iter()
        .filter_map(|r| {
            let r = *r as usize;
            chunks
                .iter()
                .position(|c| c.first_row <= r && r < c.first_row + c.rows)
        })
        .collect();
    out.sort_unstable();
    out.dedup();
    out
}

/// Each chunk's `(offset, len)` inside the real page extent
/// `[page_start, page_start + page_len)`, in proportion to its stored size:
/// chunk `i` starts at `page_start + floor(cum_i * page_len / total)`, where
/// `cum_i` is the stored size of the chunks before it. A chunk is at least
/// one byte.
pub fn place_chunks(page_start: u64, page_len: u64, chunks: &[MiniChunk]) -> Vec<(u64, u64)> {
    let total: u128 = chunks.iter().map(|c| c.stored as u128).sum();
    if total == 0 {
        return chunks.iter().map(|_| (page_start, 1)).collect();
    }
    let at = |cum: u128| page_start + (cum * u128::from(page_len) / total) as u64;
    let mut cum = 0u128;
    chunks
        .iter()
        .map(|c| {
            let start = at(cum);
            cum += c.stored as u128;
            (start, (at(cum) - start).max(1))
        })
        .collect()
}

/// Measured elapsed seconds (injected latency included) plus `bytes` at
/// `bandwidth_mib_s` MiB/s, in milliseconds.
pub fn wall_ms(elapsed_s: f64, bytes: u64, bandwidth_mib_s: f64) -> f64 {
    elapsed_s * 1000.0 + bytes as f64 / (bandwidth_mib_s * 1024.0 * 1024.0) * 1000.0
}

fn value(figures: &[Figure], key: &str) -> Option<f64> {
    let mut it = figures.iter().filter(|f| f.key == key);
    match (it.next(), it.next()) {
        (Some(f), None) => Some(f.value),
        _ => None,
    }
}

/// The per-subject rule of [`DECISION_RULE`].
pub fn decide(subject: &str, figures: &[Figure]) -> Decision {
    let mut variants = Vec::new();
    let mut parts = Vec::new();
    for v in VARIANTS {
        let mut pass = true;
        let mut shown = Vec::new();
        for shape in SHAPES {
            let get = |f: &str| value(figures, &format!("{subject}/{shape}/{v}/{f}"));
            let (b, g, w) = (get("bytes_ratio"), get("gets_ratio"), get("wall_ratio"));
            pass &= b.is_some_and(|x| x <= MAX_BYTES_RATIO)
                && g.is_some_and(|x| x <= MAX_GETS_RATIO)
                && w.is_some_and(|x| x <= MAX_WALL_RATIO);
            let s = |x: Option<f64>| x.map_or_else(|| "missing".to_string(), |x| format!("{x:.3}"));
            shown.push(format!(
                "{shape} bytes {} gets {} wall {}",
                s(b),
                s(g),
                s(w)
            ));
        }
        if pass {
            variants.push(v.to_string());
        }
        parts.push(format!(
            "{v} {} ({})",
            if pass { "GO" } else { "NO-GO" },
            shown.join("; ")
        ));
    }
    let go = !variants.is_empty();
    Decision {
        subject: subject.to_string(),
        go,
        line: format!(
            "{ARM} decision {subject}: {} [{}]",
            if go { "GO" } else { "NO-GO" },
            parts.join(" | ")
        ),
        variants,
    }
}

/// The overall rule of [`DECISION_RULE`] over the pre-registered `subjects`.
pub fn overall(subjects: &[String], saved: &[SavedDecision]) -> Decision {
    let mut passing: Vec<String> = VARIANTS.iter().map(|v| v.to_string()).collect();
    let mut parts = Vec::new();
    for s in subjects {
        let mine: Vec<&SavedDecision> = saved.iter().filter(|d| &d.decision.subject == s).collect();
        match mine.as_slice() {
            [d] if d.problems == 0 => {
                passing.retain(|v| d.decision.variants.contains(v));
                parts.push(format!("{s}=[{}]", d.decision.variants.join(",")));
            }
            [d] => {
                passing.clear();
                parts.push(format!("{s}=not a result ({} problems)", d.problems));
            }
            [] => {
                passing.clear();
                parts.push(format!("{s}=missing"));
            }
            _ => {
                passing.clear();
                parts.push(format!("{s}=reported {} times", mine.len()));
            }
        }
    }
    if subjects.is_empty() {
        passing.clear();
    }
    let go = !passing.is_empty();
    Decision {
        subject: "overall".into(),
        go,
        line: format!(
            "{ARM} decision overall: {} (variants GO on every subject: [{}]; {})",
            if go { "GO" } else { "NO-GO" },
            passing.join(","),
            parts.join(" ")
        ),
        variants: passing,
    }
}

#[cfg(test)]
pub(crate) mod tests_support {
    use super::figure_names;

    /// A one-subject spec (`s1`) with every figure unpredicted and no
    /// injected latency.
    pub(crate) fn spec_toml(block_records: usize, object_target_bytes: u64, tolerance: f64) -> String {
        let mut figures = String::new();
        for f in figure_names() {
            figures.push_str(&format!("\"{f}\" = {{ unpredicted = \"test\" }}\n"));
        }
        format!(
            r#"schema = 1
arm = "structural_read"

[[subject]]
name = "s1"
corpus = "test"
source = "inline"
corpus_sha256 = "{sha}"
max_lines = 100
object_target_bytes = {object_target_bytes}
object_tolerance = {tolerance}
block_records = {block_records}
zstd_level = 3
rows = 4
seed = 1
get_latency_ms = 0
bandwidth_mib_s = 100.0
structural_suffix_bytes = 4096
coalesce_gap = 65536
concurrency = 4
reps = 1
mini_max_rows = 4096
mini_chunk_bytes = 8192
mini_meta_bytes_per_chunk = 2

[subject.figures]
{figures}"#,
            sha = "0".repeat(64)
        )
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    fn identity(b: &[u8]) -> Result<usize, String> {
        Ok(b.len())
    }

    /// A chunker that accepts a chunk only when it is strictly smaller than
    /// the cap.
    fn mini_chunks_strict(values: &[Vec<u8>], max_rows: usize, max_chunk: usize) -> usize {
        let mut r = max_rows;
        loop {
            if values.chunks(r).all(|c| c.concat().len() < max_chunk) || r == 1 {
                return r;
            }
            r /= 2;
        }
    }

    #[test]
    fn mini_chunks_take_the_largest_fitting_power_of_two() {
        let values: Vec<Vec<u8>> = [2, 2, 2, 2, 3, 3].iter().map(|n| vec![7u8; *n]).collect();
        let got = mini_chunks(&values, 4, 8, &identity).expect("chunks");
        assert_eq!(
            got,
            vec![
                MiniChunk {
                    first_row: 0,
                    rows: 4,
                    stored: 8
                },
                MiniChunk {
                    first_row: 4,
                    rows: 2,
                    stored: 6
                },
            ]
        );
        // max_rows that is not a power of two rounds down to one.
        assert_eq!(mini_chunks(&values, 5, 8, &identity).expect("chunks"), got);
        // Nothing fits: one row per chunk.
        let one = mini_chunks(&values, 4, 1, &identity).expect("chunks");
        assert_eq!(one.len(), 6);
        assert!(one.iter().all(|c| c.rows == 1));
        assert_eq!(mini_chunks_strict(&values, 4, 8), 2);
    }

    #[test]
    fn chunks_for_rows_dedups_and_sorts() {
        let chunks = [
            MiniChunk {
                first_row: 0,
                rows: 4,
                stored: 1,
            },
            MiniChunk {
                first_row: 4,
                rows: 4,
                stored: 1,
            },
            MiniChunk {
                first_row: 8,
                rows: 1,
                stored: 1,
            },
        ];
        assert_eq!(chunks_for_rows(&chunks, &[8, 1, 3, 9]), vec![0, 2]);
        assert_eq!(chunks_for_rows(&chunks, &[4]), vec![1]);
    }

    fn place_unscaled(page_start: u64, chunks: &[MiniChunk]) -> Vec<(u64, u64)> {
        let mut at = page_start;
        chunks
            .iter()
            .map(|c| {
                let s = at;
                at += c.stored as u64;
                (s, c.stored as u64)
            })
            .collect()
    }

    #[test]
    fn chunks_are_placed_in_proportion_inside_the_real_page() {
        let c = |stored| MiniChunk {
            first_row: 0,
            rows: 1,
            stored,
        };
        let chunks = [c(15), c(5), c(30)];
        let want = vec![(1000, 30), (1030, 10), (1040, 60)];
        assert_eq!(place_chunks(1000, 100, &chunks), want);
        assert_ne!(place_unscaled(1000, &chunks), want);
        // Floors at each boundary; the extents tile the page exactly.
        assert_eq!(
            place_chunks(0, 10, &[c(1), c(1), c(1)]),
            vec![(0, 3), (3, 3), (6, 4)]
        );
    }

    #[test]
    fn wall_is_elapsed_plus_bytes_over_mebibytes_per_second() {
        assert!((wall_ms(0.090, 1_048_576, 100.0) - 100.0).abs() < 1e-9);
        // Decimal megabytes would give 100.48576.
        let wrong = 0.090 * 1000.0 + 1_048_576.0 / (100.0 * 1e6) * 1000.0;
        assert!((wall_ms(0.090, 1_048_576, 100.0) - wrong).abs() > 0.4);
    }

    fn figs(subject: &str, ratios: &[(&str, &str, [f64; 3])]) -> Vec<Figure> {
        let mut out = Vec::new();
        for (shape, v, r) in ratios {
            for (f, x) in ["bytes_ratio", "gets_ratio", "wall_ratio"].iter().zip(r) {
                out.push(Figure {
                    key: format!("{subject}/{shape}/{v}/{f}"),
                    value: *x,
                });
            }
        }
        out
    }

    #[test]
    fn decide_needs_every_threshold_on_both_shapes_inclusive() {
        let at_edge = [0.25, 1.5, 1.0];
        let f = figs(
            "s",
            &[
                ("scattered", "page_exact", at_edge),
                ("clustered", "page_exact", at_edge),
                ("scattered", "miniblock", [0.1, 1.0, 0.5]),
                ("clustered", "miniblock", [0.1, 1.51, 0.5]),
            ],
        );
        let d = decide("s", &f);
        assert!(d.go);
        assert_eq!(d.variants, vec!["page_exact".to_string()]);
        assert!(d.line.starts_with("structural_read decision s: GO"), "{}", d.line);
        // Just over each edge is a NO-GO for page_exact.
        for i in 0..3 {
            let mut r = at_edge;
            r[i] += 0.001;
            let f = figs(
                "s",
                &[
                    ("scattered", "page_exact", at_edge),
                    ("clustered", "page_exact", r),
                ],
            );
            assert!(!decide("s", &f).go, "threshold {i}");
        }
        // A missing figure is never a pass.
        assert!(!decide("s", &figs("s", &[("scattered", "page_exact", at_edge)])).go);
    }

    #[test]
    fn overall_needs_one_variant_go_on_every_subject() {
        let d = |s: &str, v: &[&str], problems: usize| SavedDecision {
            decision: Decision {
                subject: s.into(),
                go: !v.is_empty(),
                variants: v.iter().map(|x| x.to_string()).collect(),
                line: String::new(),
            },
            problems,
            misses: 0,
        };
        let subjects = vec!["a".to_string(), "b".to_string()];
        let go = overall(
            &subjects,
            &[d("a", &["page_exact", "miniblock"], 0), d("b", &["miniblock"], 0)],
        );
        assert!(go.go);
        assert_eq!(go.variants, vec!["miniblock".to_string()]);
        let split = overall(
            &subjects,
            &[d("a", &["page_exact"], 0), d("b", &["miniblock"], 0)],
        );
        assert!(!split.go, "a different variant per subject is not GO");
        assert!(!overall(&subjects, &[d("a", &["miniblock"], 0)]).go);
        assert!(!overall(&subjects, &[d("a", &["miniblock"], 0), d("b", &["miniblock"], 1)]).go);
    }

    fn minimal_toml() -> String {
        tests_support::spec_toml(16, 1000, 0.5)
    }

    #[test]
    fn spec_parses_and_refuses_bad_input() {
        let spec = parse(&minimal_toml()).expect("valid");
        assert_eq!(
            spec.subject("s1").expect("s1").expectations().len(),
            figure_names().len()
        );
        assert!(parse(&minimal_toml().replace("schema = 1", "schema = 2")).is_err());
        assert!(parse(&minimal_toml().replace("reps = 1", "reps = 0")).is_err());
        let missing = minimal_toml().replace("\"object/lines\" = { unpredicted = \"test\" }\n", "");
        let err = parse(&missing).unwrap_err();
        assert!(err.contains("missing [\"object/lines\"]"), "{err}");
        let extra = minimal_toml().replace("reps = 1", "reps = 1\nsurprise = 1");
        assert!(parse(&extra).is_err());
    }
}
