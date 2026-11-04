//! The pre-registration file (`fixtures/logbody/rei_needles.toml`): per
//! corpus, its block geometry, its needles with the reason each was chosen,
//! and a band for every figure the REI arm reports.

use std::collections::{BTreeMap, HashSet};

use serde::{Deserialize, Serialize};

use super::grams::Pattern;
use super::{ARM_SELECTED, ARMS, TEMPLATE_NEEDLES};

/// An inclusive `[low, high]` band.
pub type Band = [f64; 2];

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReiSpec {
    pub schema: u32,
    pub corpus: Vec<CorpusSpec>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CorpusSpec {
    pub name: String,
    /// The file inside the source archive.
    pub file: String,
    pub source: String,
    /// `--block-records` the run must use.
    pub block_records: usize,
    pub bands: CorpusBands,
    pub needle: Vec<NeedleSpec>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CorpusBands {
    pub lines: Band,
    pub blocks: Band,
    pub compressed_bytes: Band,
    pub zstd_cpu_s: Band,
    pub trigram: ArmBands,
    pub selected: ArmBands,
    /// Needle-and-arm defaults for every needle of a kind.
    pub template: KindBands,
    pub id: KindBands,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArmBands {
    pub index_bytes: Band,
    pub index_bytes_share: Band,
    pub build_cpu_s: Band,
    pub build_cpu_over_zstd: Band,
    /// Only the selected arm reports it.
    pub grams_selected: Option<Band>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KindBands {
    pub trigram: NeedleArmBands,
    pub selected: NeedleArmBands,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NeedleArmBands {
    pub blocks_surviving: Option<Band>,
    pub survival_share: Option<Band>,
    pub false_positives: Option<Band>,
    pub false_positive_share: Option<Band>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    /// A templated text fragment.
    Template,
    /// An identifier: the negative control.
    Id,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NeedleSpec {
    pub id: String,
    pub kind: Kind,
    pub pattern: String,
    pub reason: String,
    pub bands: NeedleBands,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NeedleBands {
    pub literal_run_bytes: Band,
    pub true_holders: Band,
    /// Overrides of the kind's defaults, field by field.
    #[serde(default)]
    pub trigram: NeedleArmBands,
    #[serde(default)]
    pub selected: NeedleArmBands,
}

impl NeedleArmBands {
    fn over(&self, base: &NeedleArmBands) -> NeedleArmBands {
        NeedleArmBands {
            blocks_surviving: self.blocks_surviving.or(base.blocks_surviving),
            survival_share: self.survival_share.or(base.survival_share),
            false_positives: self.false_positives.or(base.false_positives),
            false_positive_share: self.false_positive_share.or(base.false_positive_share),
        }
    }

    fn entries(&self) -> [(&'static str, Option<Band>); 4] {
        [
            ("blocks_surviving", self.blocks_surviving),
            ("survival_share", self.survival_share),
            ("false_positives", self.false_positives),
            ("false_positive_share", self.false_positive_share),
        ]
    }
}

/// Parses and validates a pre-registration file.
pub fn parse(text: &str) -> Result<ReiSpec, String> {
    let spec: ReiSpec = toml::from_str(text).map_err(|e| format!("rei needles: {e}"))?;
    if spec.schema != 1 {
        return Err(format!("rei needles: schema {} is not 1", spec.schema));
    }
    let mut names = HashSet::new();
    for c in &spec.corpus {
        if !names.insert(c.name.as_str()) {
            return Err(format!("rei needles: corpus {} twice", c.name));
        }
        c.validate()?;
    }
    Ok(spec)
}

impl ReiSpec {
    pub fn corpus(&self, name: &str) -> Option<&CorpusSpec> {
        self.corpus.iter().find(|c| c.name == name)
    }
}

impl CorpusSpec {
    fn validate(&self) -> Result<(), String> {
        let at = |m: String| format!("rei needles: corpus {}: {m}", self.name);
        if self.block_records == 0 {
            return Err(at("block_records is 0".into()));
        }
        let templates = self
            .needle
            .iter()
            .filter(|n| n.kind == Kind::Template)
            .count();
        if templates != TEMPLATE_NEEDLES {
            return Err(at(format!(
                "{templates} template needles, the rule needs exactly {TEMPLATE_NEEDLES}"
            )));
        }
        if !self.needle.iter().any(|n| n.kind == Kind::Id) {
            return Err(at("no id needle (the negative control)".into()));
        }
        let mut ids = HashSet::new();
        for n in &self.needle {
            if !ids.insert(n.id.as_str()) || ARMS.contains(&n.id.as_str()) || n.id.contains('/') {
                return Err(at(format!("needle id {:?} is repeated or reserved", n.id)));
            }
            if n.reason.trim().is_empty() {
                return Err(at(format!("needle {} has no reason", n.id)));
            }
            if Pattern::parse(&n.pattern).is_none() {
                return Err(at(format!(
                    "needle {} has no literal run of 3 bytes or more",
                    n.id
                )));
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

    /// Every band of this corpus, keyed like the figures.
    pub fn bands(&self) -> BTreeMap<String, Band> {
        let c = &self.name;
        let b = &self.bands;
        let mut out = BTreeMap::new();
        for (k, v) in [
            ("lines", b.lines),
            ("blocks", b.blocks),
            ("compressed_bytes", b.compressed_bytes),
            ("zstd_cpu_s", b.zstd_cpu_s),
        ] {
            out.insert(format!("{c}/{k}"), v);
        }
        for (arm, ab) in [(ARMS[0], &b.trigram), (ARMS[1], &b.selected)] {
            for (k, v) in [
                ("index_bytes", Some(ab.index_bytes)),
                ("index_bytes_share", Some(ab.index_bytes_share)),
                ("build_cpu_s", Some(ab.build_cpu_s)),
                ("build_cpu_over_zstd", Some(ab.build_cpu_over_zstd)),
                ("grams_selected", ab.grams_selected),
            ] {
                if let Some(v) = v {
                    out.insert(format!("{c}/{arm}/{k}"), v);
                }
            }
        }
        for n in &self.needle {
            let id = &n.id;
            out.insert(
                format!("{c}/{id}/literal_run_bytes"),
                n.bands.literal_run_bytes,
            );
            out.insert(format!("{c}/{id}/true_holders"), n.bands.true_holders);
            let kind = match n.kind {
                Kind::Template => &b.template,
                Kind::Id => &b.id,
            };
            for arm in ARMS {
                let (own, base) = if arm == ARM_SELECTED {
                    (&n.bands.selected, &kind.selected)
                } else {
                    (&n.bands.trigram, &kind.trigram)
                };
                for (k, v) in own.over(base).entries() {
                    if let Some(v) = v {
                        out.insert(format!("{c}/{id}/{arm}/{k}"), v);
                    }
                }
            }
        }
        out
    }
}
