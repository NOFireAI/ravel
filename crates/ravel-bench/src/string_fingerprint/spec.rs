//! The pre-registration file (`fixtures/string_fingerprint/*.toml`): per
//! corpus, its declared Str columns, each column's summary parameters, and a
//! seen/unseen predicate split with a band for every figure the report
//! prints. One level deeper than `logbody::spec` (corpus -> column ->
//! predicate), since a corpus here can hold more than one declared Str
//! column (ClickBench's `URL`, `Title`, `SearchPhrase`, `Referer` all come
//! from one corpus).

use std::collections::{BTreeMap, HashSet};

use serde::Deserialize;

use crate::logbody::grams::{MIN_GRAM, Pattern};

/// An inclusive `[low, high]` band.
pub type Band = [f64; 2];

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FingerprintSpec {
    pub schema: u32,
    pub corpus: Vec<CorpusSpec>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CorpusSpec {
    pub name: String,
    pub source: String,
    /// `--block-records` the run must use.
    pub block_records: usize,
    pub column: Vec<ColumnSpec>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnSpec {
    pub name: String,
    /// `--max-gram` the run must use: the longest gram considered (3..=7,
    /// same rule as `logbody`'s selected-gram arm).
    pub max_gram: usize,
    /// `--df-threshold` the run must use: the summary keeps a gram held by
    /// at most this share of blocks.
    pub df_threshold: f64,
    /// `--budget-share` the run must use: the summary's byte cap as a share
    /// of the column's compressed bytes.
    pub budget_share: f64,
    /// `--sample-size` the run must use: how many of the `seen` predicates
    /// (in file order) train the summary. A value at or above the number of
    /// `seen` predicates trains on all of them.
    pub sample_size: usize,
    /// `--long-value-bytes` the run must use: a row longer than this makes
    /// its block "long" for the saturation arm.
    pub long_value_bytes: u64,
    pub bands: ColumnBands,
    pub predicate: Vec<PredicateSpec>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnBands {
    pub blocks: Band,
    pub compressed_bytes: Band,
    pub zstd_cpu_s: Band,
    pub fingerprint_index_bytes: Band,
    pub fingerprint_index_bytes_share: Band,
    pub fingerprint_build_cpu_s: Band,
    pub fingerprint_build_cpu_over_zstd: Band,
    pub fingerprint_grams_selected: Band,
    pub saturation_blocks_long: Band,
    pub saturation_false_positive_share: Band,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    /// The declared column equals the pattern exactly.
    Equals,
    /// `LIKE '%pattern%'`: the declared column holds the pattern as a byte
    /// substring.
    Contains,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Split {
    /// Trains the summary.
    Seen,
    /// Held out: the summary never sees it while training. Its figures are
    /// the out-of-sample result.
    Unseen,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PredicateSpec {
    pub id: String,
    pub kind: Kind,
    pub set: Split,
    pub pattern: String,
    pub reason: String,
    pub bands: PredicateBands,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArmBands {
    pub blocks_surviving: Band,
    pub survival_share: Band,
    /// True holders the arm's survivors do not cover. Pinned `[0, 0]` for
    /// the fingerprint arm (conservativeness) and asserted, not just
    /// banded; the bloom arm can miss a holder (it indexes word tokens, not
    /// substrings) and is only banded.
    pub blocks_missed: Band,
    pub false_positive_share: Band,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContainsBands {
    pub cells_tested: Band,
    pub cells_total: Band,
    pub cells_tested_share: Band,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PredicateBands {
    pub true_holders: Band,
    pub fingerprint: ArmBands,
    /// Present only when the pattern is a single ASCII alphanumeric
    /// tokenizer token: production's word-token Bloom cannot answer a
    /// mid-token substring, so such a predicate has no bloom baseline at
    /// all (the precedent is `logbody_report`'s `WordBloom::NotApplicable`).
    /// `measure` refuses a run whose patterns do not match this split.
    #[serde(default)]
    pub bloom: Option<ArmBands>,
    /// Required when `kind = "contains"`, forbidden when `kind = "equals"`:
    /// the production `containsCellsTested` baseline only prices a `LIKE`
    /// evaluation.
    #[serde(default)]
    pub contains: Option<ContainsBands>,
}

/// Parses and validates a pre-registration file.
pub fn parse(text: &str) -> Result<FingerprintSpec, String> {
    let spec: FingerprintSpec =
        toml::from_str(text).map_err(|e| format!("string_fingerprint needles: {e}"))?;
    if spec.schema != 1 {
        return Err(format!(
            "string_fingerprint needles: schema {} is not 1",
            spec.schema
        ));
    }
    let mut names = HashSet::new();
    for c in &spec.corpus {
        if !names.insert(c.name.as_str()) {
            return Err(format!(
                "string_fingerprint needles: corpus {} twice",
                c.name
            ));
        }
        c.validate()?;
    }
    Ok(spec)
}

impl FingerprintSpec {
    pub fn corpus(&self, name: &str) -> Option<&CorpusSpec> {
        self.corpus.iter().find(|c| c.name == name)
    }

    /// Every `corpus/column` name this file pre-registers, in file order.
    pub fn columns(&self) -> Vec<String> {
        self.corpus
            .iter()
            .flat_map(|c| {
                c.column
                    .iter()
                    .map(move |col| format!("{}/{}", c.name, col.name))
            })
            .collect()
    }
}

impl CorpusSpec {
    fn validate(&self) -> Result<(), String> {
        let at = |m: String| format!("string_fingerprint needles: corpus {}: {m}", self.name);
        if self.block_records == 0 {
            return Err(at("block_records is 0".into()));
        }
        if self.column.is_empty() {
            return Err(at("no columns".into()));
        }
        let mut names = HashSet::new();
        for col in &self.column {
            if !names.insert(col.name.as_str()) {
                return Err(at(format!("column {} twice", col.name)));
            }
            col.validate(&self.name)?;
        }
        Ok(())
    }

    pub fn column(&self, name: &str) -> Option<&ColumnSpec> {
        self.column.iter().find(|c| c.name == name)
    }
}

impl ColumnSpec {
    fn validate(&self, corpus: &str) -> Result<(), String> {
        let at = |m: String| {
            format!(
                "string_fingerprint needles: corpus {corpus}: column {}: {m}",
                self.name
            )
        };
        if !(MIN_GRAM..=7).contains(&self.max_gram) {
            return Err(at(format!("max_gram {} is not in 3..=7", self.max_gram)));
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
        if self.sample_size == 0 {
            return Err(at("sample_size is 0".into()));
        }
        if self.long_value_bytes == 0 {
            return Err(at("long_value_bytes is 0".into()));
        }
        let seen = self
            .predicate
            .iter()
            .filter(|p| p.set == Split::Seen)
            .count();
        let unseen = self
            .predicate
            .iter()
            .filter(|p| p.set == Split::Unseen)
            .count();
        if seen == 0 {
            return Err(at("no seen predicates (nothing to train on)".into()));
        }
        if unseen == 0 {
            return Err(at("no unseen predicates (nothing out-of-sample)".into()));
        }
        let mut ids = HashSet::new();
        for p in &self.predicate {
            if !ids.insert(p.id.as_str()) || p.id.contains('/') {
                return Err(at(format!(
                    "predicate id {:?} is repeated or reserved",
                    p.id
                )));
            }
            if p.reason.trim().is_empty() {
                return Err(at(format!("predicate {} has no reason", p.id)));
            }
            if Pattern::parse(&p.pattern).is_none() {
                return Err(at(format!(
                    "predicate {} has no literal run of {MIN_GRAM} bytes or more",
                    p.id
                )));
            }
            match (p.kind, &p.bands.contains) {
                (Kind::Contains, None) => {
                    return Err(at(format!(
                        "predicate {} is kind contains but has no contains bands",
                        p.id
                    )));
                }
                (Kind::Equals, Some(_)) => {
                    return Err(at(format!(
                        "predicate {} is kind equals but has contains bands",
                        p.id
                    )));
                }
                _ => {}
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

    /// Every band of this column, keyed like the figures (without the
    /// corpus/column prefix; the caller adds it).
    pub fn bands(&self) -> BTreeMap<String, Band> {
        let b = &self.bands;
        let mut out = BTreeMap::new();
        for (k, v) in [
            ("blocks", b.blocks),
            ("compressed_bytes", b.compressed_bytes),
            ("zstd_cpu_s", b.zstd_cpu_s),
            ("fingerprint/index_bytes", b.fingerprint_index_bytes),
            (
                "fingerprint/index_bytes_share",
                b.fingerprint_index_bytes_share,
            ),
            ("fingerprint/build_cpu_s", b.fingerprint_build_cpu_s),
            (
                "fingerprint/build_cpu_over_zstd",
                b.fingerprint_build_cpu_over_zstd,
            ),
            ("fingerprint/grams_selected", b.fingerprint_grams_selected),
            ("saturation/blocks_long", b.saturation_blocks_long),
            (
                "saturation/false_positive_share",
                b.saturation_false_positive_share,
            ),
        ] {
            out.insert(k.to_string(), v);
        }
        for p in &self.predicate {
            let id = &p.id;
            out.insert(format!("{id}/true_holders"), p.bands.true_holders);
            let arms: [(&str, Option<&ArmBands>); 2] = [
                ("fingerprint", Some(&p.bands.fingerprint)),
                ("bloom", p.bands.bloom.as_ref()),
            ];
            for (arm, ab) in arms.into_iter().filter_map(|(a, b)| b.map(|b| (a, b))) {
                out.insert(format!("{id}/{arm}/blocks_surviving"), ab.blocks_surviving);
                out.insert(format!("{id}/{arm}/survival_share"), ab.survival_share);
                out.insert(format!("{id}/{arm}/blocks_missed"), ab.blocks_missed);
                out.insert(
                    format!("{id}/{arm}/false_positive_share"),
                    ab.false_positive_share,
                );
            }
            if let Some(c) = &p.bands.contains {
                out.insert(format!("{id}/contains/cells_tested"), c.cells_tested);
                out.insert(format!("{id}/contains/cells_total"), c.cells_total);
                out.insert(
                    format!("{id}/contains/cells_tested_share"),
                    c.cells_tested_share,
                );
            }
        }
        out
    }
}
