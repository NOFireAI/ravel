//! The pre-registration file (`fixtures/string_fingerprint/subjects.toml`):
//! per subject (a corpus and column), every run parameter, the workload's
//! parameters and seed, and for every figure the report can print either a
//! prediction with its miss band, the reason it is expected absent, or the
//! reason it cannot be predicted.

use std::collections::{BTreeMap, HashSet};

use serde::{Deserialize, Serialize};

use super::FIGURES;
use super::summary::{MAX_GRAM, MIN_GRAM};
use super::workload::WorkloadParams;

/// An inclusive `[low, high]` band.
pub type Band = [f64; 2];

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FingerprintSpec {
    pub schema: u32,
    pub subject: Vec<SubjectSpec>,
}

/// What the pre-registration says about one figure.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "RawExpect", into = "RawExpect")]
pub enum Expect {
    /// Reported exactly once, and a miss when outside `band`.
    Predicted { predict: f64, band: Band },
    /// Not reported; the string says why.
    Absent(String),
    /// Reported exactly once with no band; the string says why no prediction
    /// could be made.
    Unpredicted(String),
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawExpect {
    #[serde(skip_serializing_if = "Option::is_none")]
    predict: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    band: Option<Band>,
    #[serde(skip_serializing_if = "Option::is_none")]
    absent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    unpredicted: Option<String>,
}

impl From<Expect> for RawExpect {
    fn from(e: Expect) -> RawExpect {
        let mut r = RawExpect {
            predict: None,
            band: None,
            absent: None,
            unpredicted: None,
        };
        match e {
            Expect::Predicted { predict, band } => {
                r.predict = Some(predict);
                r.band = Some(band);
            }
            Expect::Absent(why) => r.absent = Some(why),
            Expect::Unpredicted(why) => r.unpredicted = Some(why),
        }
        r
    }
}

impl TryFrom<RawExpect> for Expect {
    type Error = String;

    fn try_from(r: RawExpect) -> Result<Expect, String> {
        let reason = |s: String, what: &str| {
            if s.trim().is_empty() {
                Err(format!("{what} needs a reason"))
            } else {
                Ok(s)
            }
        };
        match (r.predict, r.band, r.absent, r.unpredicted) {
            (Some(predict), Some(band), None, None) => {
                if !(band[0].is_finite() && band[1].is_finite() && band[0] <= band[1]) {
                    return Err(format!("band {band:?} is not a finite low <= high"));
                }
                if !(band[0] <= predict && predict <= band[1]) {
                    return Err(format!("prediction {predict} is outside its band {band:?}"));
                }
                Ok(Expect::Predicted { predict, band })
            }
            (None, None, Some(a), None) => Ok(Expect::Absent(reason(a, "absent")?)),
            (None, None, None, Some(u)) => Ok(Expect::Unpredicted(reason(u, "unpredicted")?)),
            _ => {
                Err("a figure takes exactly one of {predict, band}, {absent}, {unpredicted}".into())
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubjectSpec {
    pub name: String,
    /// Display name of the source corpus (e.g. "LogHub HDFS").
    pub corpus: String,
    /// Where the corpus comes from (URL and file).
    pub source: String,
    /// The `Str` attribute every value is written and read back under.
    pub column: String,
    pub block_records: usize,
    pub object_records: usize,
    /// Values read from the corpus; the corpus must hold at least this many.
    pub max_lines: u64,
    pub zstd_level: i32,
    pub gram_len: usize,
    pub budget_share: f64,
    pub fallback_min_bits: usize,
    pub seed: u64,
    pub literals_per_kind: usize,
    pub eq_max_len: usize,
    pub like_min_len: usize,
    pub like_max_len: usize,
    /// Keyed by figure suffix (the key without `name/`).
    pub figures: BTreeMap<String, Expect>,
}

/// Parses and validates a pre-registration file.
pub fn parse(text: &str) -> Result<FingerprintSpec, String> {
    let spec: FingerprintSpec =
        toml::from_str(text).map_err(|e| format!("string fingerprint subjects: {e}"))?;
    if spec.schema != 2 {
        return Err(format!(
            "string fingerprint subjects: schema {} is not 2",
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
        if self.block_records == 0 || self.object_records == 0 || self.max_lines == 0 {
            return Err(at(
                "block_records, object_records and max_lines must be nonzero".into(),
            ));
        }
        if !(MIN_GRAM..=MAX_GRAM).contains(&self.gram_len) {
            return Err(at(format!(
                "gram_len {} is not in {MIN_GRAM}..={MAX_GRAM}",
                self.gram_len
            )));
        }
        if !(self.budget_share > 0.0 && self.budget_share <= 1.0) {
            return Err(at(format!(
                "budget_share {} is not in (0, 1]",
                self.budget_share
            )));
        }
        if self.fallback_min_bits == 0 {
            return Err(at("fallback_min_bits is 0".into()));
        }
        if self.literals_per_kind == 0 || !self.literals_per_kind.is_multiple_of(2) {
            return Err(at(format!(
                "literals_per_kind {} is not a positive even number",
                self.literals_per_kind
            )));
        }
        if !(self.gram_len <= self.like_min_len && self.like_min_len <= self.like_max_len)
            || self.eq_max_len < self.gram_len
        {
            return Err(at(
                "need gram_len <= like_min_len <= like_max_len and gram_len <= eq_max_len".into(),
            ));
        }
        if self.column.trim().is_empty() {
            return Err(at("column is empty".into()));
        }
        let want: HashSet<&str> = FIGURES.iter().copied().collect();
        let have: HashSet<&str> = self.figures.keys().map(String::as_str).collect();
        let mut missing: Vec<&str> = want.difference(&have).copied().collect();
        let mut unknown: Vec<&str> = have.difference(&want).copied().collect();
        if !missing.is_empty() || !unknown.is_empty() {
            missing.sort_unstable();
            unknown.sort_unstable();
            return Err(at(format!(
                "figures must name every reported figure exactly: missing {missing:?}, unknown {unknown:?}"
            )));
        }
        Ok(())
    }

    /// Every figure's expectation, keyed like the reported figures.
    pub fn expectations(&self) -> BTreeMap<String, Expect> {
        self.figures
            .iter()
            .map(|(k, v)| (format!("{}/{k}", self.name), v.clone()))
            .collect()
    }

    pub fn workload_params(&self) -> WorkloadParams {
        WorkloadParams {
            seed: self.seed,
            literals_per_kind: self.literals_per_kind,
            gram_len: self.gram_len,
            eq_max_len: self.eq_max_len,
            like_min_len: self.like_min_len,
            like_max_len: self.like_max_len,
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    fn minimal_toml() -> String {
        let mut figures = String::new();
        for f in FIGURES {
            figures.push_str(&format!("\"{f}\" = {{ unpredicted = \"test\" }}\n"));
        }
        format!(
            r#"
schema = 2

[[subject]]
name = "s1"
corpus = "test"
source = "inline"
column = "value"
block_records = 4
object_records = 8
max_lines = 16
zstd_level = 3
gram_len = 3
budget_share = 0.5
fallback_min_bits = 64
seed = 1
literals_per_kind = 4
eq_max_len = 64
like_min_len = 4
like_max_len = 10

[subject.figures]
{figures}"#
        )
    }

    #[test]
    fn parses_a_minimal_valid_spec() {
        let spec = parse(&minimal_toml()).expect("valid");
        assert_eq!(spec.subject.len(), 1);
        let s = spec.subject("s1").expect("s1");
        assert_eq!(s.expectations().len(), FIGURES.len());
    }

    #[test]
    fn refuses_wrong_schema() {
        let bad = minimal_toml().replace("schema = 2", "schema = 1");
        assert!(parse(&bad).is_err());
    }

    #[test]
    fn refuses_duplicate_subject_names() {
        let one = minimal_toml();
        let (_, second) = one.split_once("[[subject]]").expect("one subject");
        let err = parse(&format!("{one}\n[[subject]]{second}")).unwrap_err();
        assert!(err.contains("twice"), "{err}");
    }

    #[test]
    fn refuses_a_missing_or_unknown_figure() {
        let missing = minimal_toml().replace("\"lines\" = { unpredicted = \"test\" }\n", "");
        let err = parse(&missing).unwrap_err();
        assert!(err.contains("missing [\"lines\"]"), "{err}");
        let unknown = format!("{}\"nope\" = {{ unpredicted = \"x\" }}\n", minimal_toml());
        let err = parse(&unknown).unwrap_err();
        assert!(err.contains("unknown [\"nope\"]"), "{err}");
    }

    #[test]
    fn refuses_a_prediction_outside_its_band() {
        let bad = minimal_toml().replace(
            "\"lines\" = { unpredicted = \"test\" }",
            "\"lines\" = { predict = 20, band = [0, 16] }",
        );
        let err = parse(&bad).unwrap_err();
        assert!(err.contains("outside its band"), "{err}");
    }

    #[test]
    fn refuses_a_figure_with_two_forms_or_no_reason() {
        let two = minimal_toml().replace(
            "\"lines\" = { unpredicted = \"test\" }",
            "\"lines\" = { predict = 1, band = [0, 2], absent = \"x\" }",
        );
        assert!(parse(&two).unwrap_err().contains("exactly one"));
        let empty = minimal_toml().replace(
            "\"lines\" = { unpredicted = \"test\" }",
            "\"lines\" = { absent = \" \" }",
        );
        assert!(parse(&empty).unwrap_err().contains("needs a reason"));
    }

    #[test]
    fn refuses_bad_workload_parameters() {
        let odd = minimal_toml().replace("literals_per_kind = 4", "literals_per_kind = 3");
        assert!(parse(&odd).is_err());
        let short = minimal_toml().replace("like_min_len = 4", "like_min_len = 2");
        assert!(parse(&short).is_err());
        let gram = minimal_toml().replace("gram_len = 3", "gram_len = 2");
        assert!(parse(&gram).is_err());
    }
}
