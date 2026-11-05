//! The pre-registration file
//! (`fixtures/string_fingerprint/subjects.toml`): per subject (a corpus and
//! column), its block geometry, the summary's own parameters, its seen and
//! unseen predicates with the reason each was chosen, and a band for every
//! figure the report prints. Mirrors `crate::logbody::spec`'s shape and
//! validation style.

use std::collections::{BTreeMap, HashSet};

use serde::Deserialize;

use super::summary::MIN_GRAM;

/// An inclusive `[low, high]` band.
pub type Band = [f64; 2];

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FingerprintSpec {
    pub schema: u32,
    pub subject: Vec<SubjectSpec>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubjectSpec {
    pub name: String,
    /// Display name of the source corpus (e.g. "HDFS", "ClickBench hits").
    pub corpus: String,
    /// Where the corpus comes from (URL or recipe reference).
    pub source: String,
    /// The attribute column every subject's values are written and read
    /// back under. Every subject, log body included, is written as a
    /// declared `Str` attribute (never the record body) so the production
    /// `Equals` and `Contains` baselines, which both resolve through a
    /// named `FIELD_DIR` column, apply uniformly across every subject (see
    /// the module doc for why `Predicate::Contains` cannot target the
    /// body field directly).
    pub column: String,
    /// `--block-records` the run must use.
    pub block_records: usize,
    /// The summary's own `gram_len` the run must use.
    pub gram_len: usize,
    /// The summary's own `df_threshold` the run must use.
    pub df_threshold: f64,
    /// The summary's own `budget_share` the run must use.
    pub budget_share: f64,
    pub bands: SubjectBands,
    pub seen: Vec<PredicateSpec>,
    pub unseen: Vec<PredicateSpec>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Equals,
    Like,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PredicateSpec {
    pub id: String,
    pub kind: Kind,
    pub literal: String,
    pub reason: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubjectBands {
    pub lines: Band,
    pub blocks: Band,
    pub compressed_bytes: Band,
    pub zstd_cpu_s: Band,
    pub build_cpu_over_zstd: Band,
    pub summary_bytes_share: Band,
    pub seen_equals_false_positive_share: Band,
    pub seen_like_false_positive_share: Band,
    pub unseen_equals_false_positive_share: Band,
    pub unseen_like_false_positive_share: Band,
    pub bloom_equals_blocks_kept_share: Band,
    pub contains_like_cells_tested_share: Band,
    pub saturation_false_positive_share: Band,
}

/// Parses and validates a pre-registration file.
pub fn parse(text: &str) -> Result<FingerprintSpec, String> {
    let spec: FingerprintSpec =
        toml::from_str(text).map_err(|e| format!("string fingerprint subjects: {e}"))?;
    if spec.schema != 1 {
        return Err(format!(
            "string fingerprint subjects: schema {} is not 1",
            spec.schema
        ));
    }
    let mut names = HashSet::new();
    for s in &spec.subject {
        if !names.insert(s.name.as_str()) {
            return Err(format!(
                "string fingerprint subjects: subject {} twice",
                s.name
            ));
        }
        s.validate()?;
    }
    Ok(spec)
}

impl FingerprintSpec {
    pub fn subject(&self, name: &str) -> Option<&SubjectSpec> {
        self.subject.iter().find(|s| s.name == name)
    }
}

impl SubjectSpec {
    fn validate(&self) -> Result<(), String> {
        let at = |m: String| format!("string fingerprint subjects: subject {}: {m}", self.name);
        if self.block_records == 0 {
            return Err(at("block_records is 0".into()));
        }
        if !(MIN_GRAM..=32).contains(&self.gram_len) {
            return Err(at(format!("gram_len {} is not in 3..=32", self.gram_len)));
        }
        if !(self.df_threshold > 0.0 && self.df_threshold <= 1.0) {
            return Err(at(format!(
                "df_threshold {} is not in (0, 1]",
                self.df_threshold
            )));
        }
        if !(self.budget_share > 0.0 && self.budget_share <= 1.0) {
            return Err(at(format!(
                "budget_share {} is not in (0, 1]",
                self.budget_share
            )));
        }
        if self.column.trim().is_empty() {
            return Err(at("column is empty".into()));
        }
        for (group, preds) in [("seen", &self.seen), ("unseen", &self.unseen)] {
            let mut ids = HashSet::new();
            for p in preds {
                if !ids.insert(p.id.as_str()) {
                    return Err(at(format!("{group} predicate id {:?} is repeated", p.id)));
                }
                if p.reason.trim().is_empty() {
                    return Err(at(format!("{group} predicate {} has no reason", p.id)));
                }
                if p.literal.is_empty() {
                    return Err(at(format!(
                        "{group} predicate {} has an empty literal",
                        p.id
                    )));
                }
            }
            for kind in [Kind::Equals, Kind::Like] {
                if !preds.iter().any(|p| p.kind == kind) {
                    return Err(at(format!("no {group} predicate of kind {kind:?}")));
                }
            }
        }
        // Seen and unseen must be disjoint, within a kind: an unseen literal
        // that is also a seen one (of the same kind) is not actually unseen,
        // and the figures it feeds would silently stop measuring
        // generalization.
        for kind in [Kind::Equals, Kind::Like] {
            let seen_literals: HashSet<&str> = self
                .seen
                .iter()
                .filter(|p| p.kind == kind)
                .map(|p| p.literal.as_str())
                .collect();
            for p in self.unseen.iter().filter(|p| p.kind == kind) {
                if seen_literals.contains(p.literal.as_str()) {
                    return Err(at(format!(
                        "unseen predicate {} ({:?}) repeats a seen literal",
                        p.id, kind
                    )));
                }
            }
        }
        for (key, b) in self.bands() {
            if !(b[0].is_finite() && b[1].is_finite() && b[0] <= b[1]) {
                return Err(at(format!(
                    "band {key} = {b:?} is not a finite low <= high"
                )));
            }
        }
        Ok(())
    }

    /// Every band of this subject, keyed like the figures.
    pub fn bands(&self) -> BTreeMap<String, Band> {
        let s = &self.name;
        let b = &self.bands;
        let mut out = BTreeMap::new();
        for (k, v) in [
            ("lines", b.lines),
            ("blocks", b.blocks),
            ("compressed_bytes", b.compressed_bytes),
            ("zstd_cpu_s", b.zstd_cpu_s),
            ("build_cpu_over_zstd", b.build_cpu_over_zstd),
            ("summary_bytes_share", b.summary_bytes_share),
            (
                "seen/equals/false_positive_share",
                b.seen_equals_false_positive_share,
            ),
            (
                "seen/like/false_positive_share",
                b.seen_like_false_positive_share,
            ),
            (
                "unseen/equals/false_positive_share",
                b.unseen_equals_false_positive_share,
            ),
            (
                "unseen/like/false_positive_share",
                b.unseen_like_false_positive_share,
            ),
            (
                "bloom/equals/blocks_kept_share",
                b.bloom_equals_blocks_kept_share,
            ),
            (
                "contains/like/cells_tested_share",
                b.contains_like_cells_tested_share,
            ),
            (
                "saturation/false_positive_share",
                b.saturation_false_positive_share,
            ),
        ] {
            out.insert(format!("{s}/{k}"), v);
        }
        out
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    fn minimal_toml() -> String {
        r#"
schema = 1

[[subject]]
name = "s1"
corpus = "test"
source = "inline"
column = "value"
block_records = 4
gram_len = 3
df_threshold = 0.5
budget_share = 0.5

[subject.bands]
lines = [0, 1000]
blocks = [0, 100]
compressed_bytes = [0, 100000]
zstd_cpu_s = [0, 1]
build_cpu_over_zstd = [0, 10]
summary_bytes_share = [0, 0.01]
seen_equals_false_positive_share = [0, 0.1]
seen_like_false_positive_share = [0, 0.1]
unseen_equals_false_positive_share = [0, 1]
unseen_like_false_positive_share = [0, 0.1]
bloom_equals_blocks_kept_share = [0, 1]
contains_like_cells_tested_share = [0, 1]
saturation_false_positive_share = [0, 0.3]

[[subject.seen]]
id = "e1"
kind = "equals"
literal = "needle"
reason = "test"

[[subject.seen]]
id = "l1"
kind = "like"
literal = "stack"
reason = "test"

[[subject.unseen]]
id = "e2"
kind = "equals"
literal = "other"
reason = "test"

[[subject.unseen]]
id = "l2"
kind = "like"
literal = "trace"
reason = "test"
"#
        .to_string()
    }

    #[test]
    fn parses_a_minimal_valid_spec() {
        let spec = parse(&minimal_toml()).expect("valid");
        assert_eq!(spec.subject.len(), 1);
        assert!(spec.subject("s1").is_some());
    }

    #[test]
    fn refuses_wrong_schema() {
        let bad = minimal_toml().replace("schema = 1", "schema = 2");
        assert!(parse(&bad).is_err());
    }

    #[test]
    fn refuses_duplicate_subject_names() {
        let one = minimal_toml();
        let (_, second_subject) = one.split_once("[[subject]]").expect("one subject block");
        let doubled = format!("{one}\n[[subject]]{second_subject}");
        let err = parse(&doubled).unwrap_err();
        assert!(err.contains("twice"), "{err}");
    }

    #[test]
    fn refuses_unseen_literal_repeating_a_seen_one() {
        let bad = minimal_toml().replace("literal = \"other\"", "literal = \"needle\"");
        let err = parse(&bad).unwrap_err();
        assert!(err.contains("repeats a seen literal"), "{err}");
    }

    #[test]
    fn refuses_missing_predicate_kind() {
        let bad = minimal_toml().replace(
            r#"[[subject.unseen]]
id = "l2"
kind = "like"
literal = "trace"
reason = "test"
"#,
            "",
        );
        let err = parse(&bad).unwrap_err();
        assert!(err.contains("no unseen predicate of kind Like"), "{err}");
    }

    #[test]
    fn refuses_empty_reason() {
        let bad = minimal_toml().replacen(
            "id = \"e1\"\nkind = \"equals\"\nliteral = \"needle\"\nreason = \"test\"",
            "id = \"e1\"\nkind = \"equals\"\nliteral = \"needle\"\nreason = \"   \"",
            1,
        );
        let err = parse(&bad).unwrap_err();
        assert!(err.contains("has no reason"), "{err}");
    }

    #[test]
    fn refuses_out_of_range_band() {
        let bad = minimal_toml().replace("zstd_cpu_s = [0, 1]", "zstd_cpu_s = [1, 0]");
        let err = parse(&bad).unwrap_err();
        assert!(err.contains("is not a finite low <= high"), "{err}");
    }

    #[test]
    fn refuses_bad_gram_len() {
        let bad = minimal_toml().replace("gram_len = 3", "gram_len = 1");
        assert!(parse(&bad).is_err());
    }
}
