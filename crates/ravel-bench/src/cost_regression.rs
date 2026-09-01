//! Request-cost regression gate (ADR-0996 workstream F, task 996-7).
//!
//! A comparison TOOL, not a counter: it reads two machine-readable
//! [`CostReport`]s -- a baseline and a candidate -- and fails when any
//! workstream-F figure regresses past its per-figure band. The reports carry
//! the figures the ledger already produces (object count, write-class request
//! attempts, data GETs, range amplification, modeled request cost, wire bytes
//! by kind, p50/p95 latency, peak memory, ranged opens); this module consumes
//! them, it does not add counters.
//!
//! # The measurement discipline, made mechanical
//!
//! Every figure is PRESENT EXACTLY ONCE in each report and inside its band, or
//! the check fails. A figure absent from one report but present in the other is
//! a FAILURE naming it, never a silent skip. The one exception is the
//! absent-not-zero convention ([`crate::report::RequestCounts`] renders a
//! backend that cannot bill as `None`, never `0`): a figure whose VALUE is
//! absent in BOTH reports passes as equally-absent and says so. A figure CLASS
//! the bands declare expected but that no figure in a report carries fails
//! naming the class, for the same reason: a band nobody emitted a figure for is
//! a band that passed vacuously.
//!
//! # Absolutes
//!
//! Beside the per-figure bands sit the baseline-independent gates
//! ([`Absolutes`]): a physical amplification ceiling and floor, the
//! request-minimal plan-shape rule on ranged opens, and the optional
//! corpus-scale ceilings. These judge EACH report on its own, so a defect both
//! reports share (an amplification of 0.999, which no real scan can produce)
//! fails instead of comparing equal.
//!
//! # Profile guard
//!
//! Two reports priced under different EFFECTIVE cost profiles (the 996-4 stamp)
//! cannot be compared: their modeled-cost and request figures are denominated
//! differently, the wrong-basis error the epic documents. The comparison
//! refuses such a pair, naming both profiles.

use std::collections::BTreeMap;
use std::fmt;

use ravel_types::cost_profile::StoreCostProfile;
use serde::{Deserialize, Serialize};

/// One workstream-F figure class. The band a figure is judged against is keyed
/// by its class, not its free-form [`Figure::name`], so a report may carry the
/// same class many times under distinct names (one per statement) and each is
/// judged by the same band.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FigureClass {
    /// Data objects the resolved snapshot comprises (`DatasetInfo::object_count`).
    ObjectCount,
    /// PUT-class billed attempts (`RequestCounts::put_class_attempts`): the
    /// write class a read-policy change must never move.
    WriteClassRequests,
    /// GET-class requests the read path issued (data GETs).
    DataGets,
    /// Scan-phase wire bytes over stored decoded page bytes / objects touched
    /// (the amplification ratio); higher is worse.
    RangeAmplification,
    /// Modeled request cost in nanodollars (attempts x profile).
    ModeledRequestCost,
    /// Object-store bytes. The [`Figure::unit`] names the byte KIND (wire,
    /// charged, decompressed); two figures of different kinds are never
    /// compared.
    Bytes,
    /// Median latency, milliseconds.
    LatencyP50,
    /// 95th-percentile latency, milliseconds.
    LatencyP95,
    /// Peak resident / fetch-buffer bytes.
    PeakMemory,
    /// Fast-path ranged segment opens (`RunAccounting::logs_ranged_opens`). The
    /// plan-shape rule: a request-minimal full scan opens zero ranged segments,
    /// so this defaults to an exact band and any nonzero delta fails.
    RangedOpens,
}

impl FigureClass {
    /// Every class, so the defaults table and its round-trip test cannot drop
    /// one silently.
    pub const ALL: [FigureClass; 10] = [
        FigureClass::ObjectCount,
        FigureClass::WriteClassRequests,
        FigureClass::DataGets,
        FigureClass::RangeAmplification,
        FigureClass::ModeledRequestCost,
        FigureClass::Bytes,
        FigureClass::LatencyP50,
        FigureClass::LatencyP95,
        FigureClass::PeakMemory,
        FigureClass::RangedOpens,
    ];

    /// The class name as it appears in the band TOML and the report table.
    pub fn as_str(self) -> &'static str {
        match self {
            FigureClass::ObjectCount => "object_count",
            FigureClass::WriteClassRequests => "write_class_requests",
            FigureClass::DataGets => "data_gets",
            FigureClass::RangeAmplification => "range_amplification",
            FigureClass::ModeledRequestCost => "modeled_request_cost",
            FigureClass::Bytes => "bytes",
            FigureClass::LatencyP50 => "latency_p50",
            FigureClass::LatencyP95 => "latency_p95",
            FigureClass::PeakMemory => "peak_memory",
            FigureClass::RangedOpens => "ranged_opens",
        }
    }
}

impl fmt::Display for FigureClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One reported figure. [`Self::value`] is `None` for the absent-not-zero
/// convention (a figure whose backend cannot produce it); [`Self::unit`] names
/// a byte figure's kind so two kinds are never compared.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Figure {
    /// Free-form identity, unique within a report. A statement-scoped figure
    /// carries its statement id here (e.g. `"q07.data_gets"`); an aggregate
    /// figure carries the class name. Two figures sharing a name are the
    /// duplicated-figure failure the present-exactly-once discipline forbids.
    pub name: String,
    /// Which band judges this figure.
    pub class: FigureClass,
    /// The measured value, or `None` for the absent-not-zero convention. A
    /// value present in one report and absent in the other is the asymmetry
    /// failure; absent in both is the equally-absent pass.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<f64>,
    /// Byte-kind or unit label, e.g. `"wire"`. Two figures of the same name but
    /// different unit are the wrong-basis error and refuse to compare.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
}

/// The cost-profile provenance stamp (ADR-0996 decision 1 / task 996-4).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProfileStamp {
    /// The profile the run asked to price at.
    pub requested: StoreCostProfile,
    /// The profile that actually governed pricing, or `None` on a lane that
    /// cannot know it (Flight). The comparison keys the profile guard on this:
    /// two reports whose effective profiles differ cannot be compared.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective: Option<StoreCostProfile>,
}

impl ProfileStamp {
    /// The name that identifies the effective profile in a refusal message.
    /// A `None` effective profile (Flight lane) reads as `"<none>"`.
    fn effective_name(&self) -> &str {
        self.effective
            .as_ref()
            .map_or("<none>", |p| p.name.as_str())
    }
}

/// Render the two effective profiles for a refusal message.
///
/// Distinct names identify the profiles on their own. Two profiles that SHARE
/// a name but carry different prices are the trap the stamp exists to catch (a
/// profile edited in place under its old name), and there the name alone would
/// print twice and say nothing, so the prices are appended.
fn profile_labels(baseline: &ProfileStamp, candidate: &ProfileStamp) -> (String, String) {
    let (base_name, cand_name) = (baseline.effective_name(), candidate.effective_name());
    if base_name != cand_name {
        return (base_name.to_string(), cand_name.to_string());
    }
    (
        format!("{base_name} {}", price_digest(baseline.effective.as_ref())),
        format!("{cand_name} {}", price_digest(candidate.effective.as_ref())),
    )
}

/// Every priced field of a profile, for the same-name refusal above.
fn price_digest(profile: Option<&StoreCostProfile>) -> String {
    match profile {
        None => "(no prices)".to_string(),
        Some(p) => format!(
            "(put={} get={} delete={} transfer_per_gib={} retrieval_per_gib={})",
            p.put_class_nanodollars,
            p.get_class_nanodollars,
            p.delete_class_nanodollars,
            p.transfer_nanodollars_per_gib,
            p.retrieval_nanodollars_per_gib,
        ),
    }
}

/// The effective-policy stamp value the plan-shape absolutes are defined over:
/// the name [`ravel_query::LogsFetchPolicy::RequestMinimal`] renders itself as.
/// `as_str` is not `const`, so the string is spelled once here and pinned to
/// that enum by `the_request_minimal_stamp_matches_the_policys_own_name`; a
/// rename there fails that test rather than silently disabling a gate.
const REQUEST_MINIMAL_POLICY: &str = "request-minimal";

/// A machine-readable cost report: the profile it was priced at and every
/// compared figure. A `Vec` (not a map) so a figure emitted twice survives
/// deserialization as a duplicate rather than collapsing to last-wins.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CostReport {
    /// The cost-profile stamp; the profile guard reads its effective profile.
    pub profile: ProfileStamp,
    /// The effective logs-fetch policy the run executed under, as
    /// [`ravel_query::LogsFetchPolicy::as_str`] renders it, or `None` on a lane
    /// that does not record one. The plan-shape absolutes are defined only over
    /// a request-minimal run, so without this stamp the tool does not assert
    /// them (see [`Absolutes`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_policy: Option<String>,
    /// Every figure, in emission order.
    pub figures: Vec<Figure>,
}

impl CostReport {
    /// Parse a report from JSON, mapping any malformed input to a typed
    /// refusal rather than a panic.
    pub fn from_json_str(s: &str) -> Result<CostReport, CompareError> {
        serde_json::from_str(s).map_err(|e| CompareError::Malformed(e.to_string()))
    }

    /// Whether this report stamps a request-minimal effective policy. An absent
    /// stamp is NOT request-minimal: the tool cannot know what the run did, so
    /// it does not assert the plan-shape rule against it.
    fn is_request_minimal(&self) -> bool {
        self.effective_policy.as_deref() == Some(REQUEST_MINIMAL_POLICY)
    }

    /// The request surface a real measurement always carries: at least one
    /// [`FigureClass::DataGets`] figure. A legacy report written before request
    /// counts existed carries none, and comparing it would vacuously pass every
    /// request band it never emitted.
    fn has_request_surface(&self) -> bool {
        self.figures
            .iter()
            .any(|f| f.class == FigureClass::DataGets)
    }
}

/// A tiny relative epsilon so float noise at a *ranged* band edge does not read
/// as a regression. Applied only to the `Percent`/`PercentTwoSided`/`Absolute`
/// arms and to the absolute gates; an `Exact` band never uses it (see
/// [`Band::regresses`]).
fn edge_eps(x: f64) -> f64 {
    x.abs() * 1e-9 + f64::EPSILON
}

/// A per-figure band. The comparison loop reads only this, never a hardcoded
/// constant, so every threshold is configuration.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Band {
    /// Candidate must equal baseline exactly. Any difference, up or down,
    /// fails. Used for deterministic geometry (object count, ranged opens) and
    /// for request counts by default (a request floor is an exact figure).
    Exact,
    /// Candidate may exceed baseline by at most `allowance` percent. A decrease
    /// is an improvement and passes.
    Percent { allowance: f64 },
    /// Candidate must stay within `allowance` percent of baseline in EITHER
    /// direction: a decrease past the band fails too. Used for a redundant
    /// cross-check figure (modeled request cost) where a drop under an
    /// unchanged stamp signals a model bug, not an improvement.
    PercentTwoSided { allowance: f64 },
    /// Candidate may exceed baseline by at most `allowance` in the figure's own
    /// units. A decrease passes.
    Absolute { allowance: f64 },
}

impl Band {
    /// Whether `candidate` regresses past this band relative to `baseline`.
    ///
    /// For `Percent`/`Absolute` only an INCREASE past the allowance is a
    /// regression; a decrease is an improvement. `PercentTwoSided` fails on a
    /// move past the allowance in either direction. `Exact` means EXACT at all
    /// magnitudes: any difference fails, with no epsilon (a relative epsilon
    /// would silently admit drift at large baselines, e.g. `2e9` admitting
    /// `2e9 + 1`). Whole f64 values compare exactly, so an integer-valued exact
    /// figure is unaffected.
    fn regresses(self, baseline: f64, candidate: f64) -> bool {
        match self {
            Band::Exact => candidate != baseline,
            Band::Percent { allowance } => {
                let threshold = baseline * (1.0 + allowance / 100.0);
                candidate - threshold > edge_eps(threshold)
            }
            Band::PercentTwoSided { allowance } => {
                let margin = baseline.abs() * (allowance / 100.0);
                (candidate - baseline).abs() - margin > edge_eps(baseline)
            }
            Band::Absolute { allowance } => {
                let threshold = baseline + allowance;
                candidate - threshold > edge_eps(threshold)
            }
        }
    }

    /// A compact rendering for the report table, e.g. `"exact"`, `"+5%"`,
    /// `"±1%"`, `"+0.05"`.
    fn render(self) -> String {
        match self {
            Band::Exact => "exact".to_string(),
            Band::Percent { allowance } => format!("+{allowance}%"),
            Band::PercentTwoSided { allowance } => format!("±{allowance}%"),
            Band::Absolute { allowance } => format!("+{allowance}"),
        }
    }
}

/// Absolute, baseline-independent gates (ADR-0996 verification plan). Unlike a
/// [`Band`], which judges a candidate against a baseline, these judge EACH
/// report on its own: a physical ceiling/floor that no comparison can excuse.
///
/// - `range_amplification_ceiling` / `range_amplification_floor`: full-scan
///   amplification is bounded above (`<= 1.05`) and floored at the physical
///   `1.0`. A value below the floor is a broken measurement (you cannot move
///   fewer bytes than the data holds), so it FAILS rather than passing as an
///   improvement.
/// - `ranged_opens_max`: a request-minimal full scan opens zero ranged
///   segments. Enforced only when the report stamps a request-minimal effective
///   policy ([`CostReport::effective_policy`]); without the stamp the tool
///   cannot know the run was request-minimal, so it does not assert it.
/// - `cold_gets_max` / `modeled_cost_max_nanodollars`: corpus-scale absolutes.
///   Corpus-specific, so the checked-in defaults leave them unset (`None`) and
///   the TOML ships them commented out with the corpus named; setting one gates
///   the [`FigureClass::DataGets`] / [`FigureClass::ModeledRequestCost`]
///   figures against that ceiling.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Absolutes {
    /// Upper bound on any range-amplification figure. `None` disables.
    pub range_amplification_ceiling: Option<f64>,
    /// Physical floor; a value below it is a broken measurement. `None`
    /// disables.
    pub range_amplification_floor: Option<f64>,
    /// Max ranged opens under a request-minimal effective policy stamp. `None`
    /// disables.
    pub ranged_opens_max: Option<f64>,
    /// Corpus-scale max cold GETs. `None` (default) disables.
    pub cold_gets_max: Option<f64>,
    /// Corpus-scale max modeled request cost, nanodollars. `None` (default)
    /// disables.
    pub modeled_cost_max_nanodollars: Option<f64>,
}

impl Absolutes {
    /// The checked-in defaults: the ADR figures for amplification and ranged
    /// opens; the corpus-scale ceilings unset, since they must not fail a corpus
    /// other than the one they were measured on.
    pub fn defaults() -> Absolutes {
        Absolutes {
            range_amplification_ceiling: Some(1.05),
            range_amplification_floor: Some(1.0),
            ranged_opens_max: Some(0.0),
            cold_gets_max: None,
            modeled_cost_max_nanodollars: None,
        }
    }

    /// Judge ONE value of `class` from ONE report on its own, with no baseline
    /// in play, and return the reason a gate refuses it.
    ///
    /// `request_minimal` is that report's own effective-policy stamp
    /// ([`CostReport::effective_policy`]): the ranged-opens gate states the
    /// plan-shape rule for a request-minimal run and says nothing about any
    /// other policy, so an unstamped or differently-stamped report skips it.
    /// The relative [`edge_eps`] applies here for the same reason it applies to
    /// the ranged bands: these are ratio and cost figures, not exact geometry.
    fn violation(
        &self,
        class: FigureClass,
        name: &str,
        value: f64,
        request_minimal: bool,
    ) -> Option<String> {
        match class {
            FigureClass::RangeAmplification => {
                if let Some(ceiling) = self.range_amplification_ceiling
                    && value - ceiling > edge_eps(ceiling)
                {
                    return Some(format!(
                        "range amplification {} exceeds the absolute ceiling {}",
                        render_number(value),
                        render_number(ceiling),
                    ));
                }
                let floor = self.range_amplification_floor?;
                (floor - value > edge_eps(floor)).then(|| {
                    format!(
                        "range amplification {} is below the physical floor {}: a scan cannot \
                         move fewer bytes than the data holds, so this is a broken measurement, \
                         never an improvement",
                        render_number(value),
                        render_number(floor),
                    )
                })
            }
            // Gated on the stamp, not asserted blindly: a byte-minimal run opens
            // ranged segments on purpose and is not a regression.
            FigureClass::RangedOpens if request_minimal => {
                let max = self.ranged_opens_max?;
                (value - max > edge_eps(max)).then(|| {
                    format!(
                        "{} ranged opens under a `{REQUEST_MINIMAL_POLICY}` effective policy, \
                         which reads every object whole and so opens at most {}",
                        render_number(value),
                        render_number(max),
                    )
                })
            }
            // Both corpus ceilings are scoped by figure NAME, not only class:
            // `data_get_calls` shares the DataGets class as the diagnostic
            // beside the billed attempts, and the transfer/retrieval cost terms
            // share ModeledRequestCost as unsummable siblings. A ceiling
            // measured for one quantity must judge exactly that quantity.
            FigureClass::DataGets if name == "data_gets" => {
                let max = self.cold_gets_max?;
                (value - max > edge_eps(max)).then(|| {
                    format!(
                        "{} data GETs exceeds the corpus-scale ceiling {}",
                        render_number(value),
                        render_number(max),
                    )
                })
            }
            FigureClass::ModeledRequestCost if name == "modeled_request_cost" => {
                let max = self.modeled_cost_max_nanodollars?;
                (value - max > edge_eps(max)).then(|| {
                    format!(
                        "modeled request cost {} nanodollars exceeds the corpus-scale ceiling {}",
                        render_number(value),
                        render_number(max),
                    )
                })
            }
            _ => None,
        }
    }
}

/// The band for every figure class, whether the class is expected present, and
/// the absolute gates. The comparison loop resolves a figure's band through
/// `Self::for_class`; defaults live in [`Self::defaults`] and are mirrored by
/// the checked-in TOML, so a threshold is data with a named home, never a magic
/// number in the loop.
#[derive(Debug, Clone, PartialEq)]
pub struct Bands {
    bands: BTreeMap<FigureClass, Band>,
    /// Per class, whether at least one figure of that class must appear in both
    /// reports. A class expected but absent from both is a failure naming it,
    /// not a silent skip.
    expected: BTreeMap<FigureClass, bool>,
    /// Baseline-independent gates applied to each report.
    absolutes: Absolutes,
}

/// One band table in the TOML: `kind` plus the `allowance` its kind needs, plus
/// the optional per-class `expected` flag. A dedicated struct (not
/// `Option<Band>`) so `deny_unknown_fields` refuses a stray key INSIDE the
/// table too -- an internally-tagged enum cannot carry `deny_unknown_fields`,
/// so a misspelled `allowanace` would otherwise slip through as a default band.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BandToml {
    kind: BandKind,
    #[serde(default)]
    allowance: Option<f64>,
    #[serde(default)]
    expected: Option<bool>,
}

/// The band kinds a TOML table may name. Kept separate from [`Band`] so the
/// wire form is a flat `kind = "..."` string plus an `allowance`, validated
/// into a [`Band`] where a missing allowance is a typed refusal.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum BandKind {
    Exact,
    Percent,
    PercentTwoSided,
    Absolute,
}

impl BandToml {
    /// Validate this table into a [`Band`], refusing a kind whose required
    /// `allowance` is missing.
    fn to_band(&self, class: FigureClass) -> Result<Band, CompareError> {
        let allowance = || {
            self.allowance.ok_or_else(|| {
                CompareError::MalformedBands(format!(
                    "band `{class}` requires an `allowance` for its kind"
                ))
            })
        };
        Ok(match self.kind {
            BandKind::Exact => {
                if self.allowance.is_some() {
                    return Err(CompareError::MalformedBands(format!(
                        "band `{class}` is `exact` and takes no `allowance`: an exact band \
                         admits zero drift, so a supplied allowance is a misconfiguration, \
                         not a widening"
                    )));
                }
                Band::Exact
            }
            BandKind::Percent => Band::Percent {
                allowance: allowance()?,
            },
            BandKind::PercentTwoSided => Band::PercentTwoSided {
                allowance: allowance()?,
            },
            BandKind::Absolute => Band::Absolute {
                allowance: allowance()?,
            },
        })
    }
}

/// The overridable TOML surface: one optional band table per class plus an
/// optional `[absolute]` section. A document that sets a subset leaves the rest
/// at their default; an unknown key is refused so a misspelled figure name
/// fails loudly instead of leaving a default silently in force.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct BandsToml {
    object_count: Option<BandToml>,
    write_class_requests: Option<BandToml>,
    data_gets: Option<BandToml>,
    range_amplification: Option<BandToml>,
    modeled_request_cost: Option<BandToml>,
    bytes: Option<BandToml>,
    latency_p50: Option<BandToml>,
    latency_p95: Option<BandToml>,
    peak_memory: Option<BandToml>,
    ranged_opens: Option<BandToml>,
    #[serde(default)]
    absolute: Option<AbsolutesToml>,
}

/// The `[absolute]` TOML table. Every key optional; an omitted key keeps its
/// compiled default (TOML has no null, so a present key is always an override).
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct AbsolutesToml {
    range_amplification_ceiling: Option<f64>,
    range_amplification_floor: Option<f64>,
    ranged_opens_max: Option<f64>,
    cold_gets_max: Option<f64>,
    modeled_cost_max_nanodollars: Option<f64>,
}

impl Bands {
    /// The default band per figure class.
    ///
    /// Rationale (documented here and mirrored in `cost_regression_bands.toml`):
    ///
    /// - `object_count`, `ranged_opens`: EXACT. Object geometry is
    ///   deterministic and the plan-shape rule pins a request-minimal full scan
    ///   to zero ranged opens; any drift is a real regression, not noise.
    /// - `write_class_requests`, `data_gets`: EXACT. The request floor is an
    ///   exact figure (ADR-0996 verification plan: "requests and object counts
    ///   exact or configured %"), and a read-policy change must not move the
    ///   write class at all. Override to a percent band in the TOML for a lane
    ///   whose request count legitimately varies.
    /// - `range_amplification`: ABSOLUTE +0.05. The ADR bands full-scan
    ///   amplification at <= 1.05 over the 1.0 floor; a comparison allows the
    ///   candidate the same +0.05 headroom over the baseline.
    /// - `modeled_request_cost`: PERCENT TWO-SIDED ±1%. Modeled from billed
    ///   attempts, so it tracks the request count; 1% absorbs profile-rounding.
    ///   Two-sided because the figure is a redundant cross-check: under an
    ///   unchanged stamp and unchanged request counts the modeled cost must not
    ///   move, so a DROP (a model bug that underprices) fails the same as a
    ///   rise, rather than passing as an improvement.
    /// - `bytes`: PERCENT +5%. Wire bytes carry coalesced holes that shift with
    ///   object geometry; 5% covers that jitter.
    /// - `latency_p50`: PERCENT +1%. The repo's hot-path noise floor is
    ///   ~0.2-1%; p50 is a hot/median figure, so 1% is the tightest band that
    ///   does not flake.
    /// - `latency_p95`: PERCENT +5%. The cold/tail noise floor is 3-5%; p95 is
    ///   a tail figure, so 5% is the matching floor.
    /// - `peak_memory`: PERCENT +10%. The ADR peak-residency band is +/-10% vs
    ///   baseline; the regression gate keeps the +10% upper half.
    ///
    /// Expected presence: every class defaults to expected, EXCEPT the four the
    /// shipping [`crate::report::BenchReport`] does not yet carry a figure for
    /// (`object_count`, `range_amplification`, `ranged_opens`, `peak_memory`).
    /// Defaulting those expected would fail the gate on every real report until
    /// their counters exist (task 996-7 STOP-and-report); the checked-in TOML
    /// mirrors this so the gate is usable now on the figures the bench emits.
    pub fn defaults() -> Bands {
        let mut bands = BTreeMap::new();
        bands.insert(FigureClass::ObjectCount, Band::Exact);
        bands.insert(FigureClass::WriteClassRequests, Band::Exact);
        bands.insert(FigureClass::DataGets, Band::Exact);
        bands.insert(
            FigureClass::RangeAmplification,
            Band::Absolute { allowance: 0.05 },
        );
        bands.insert(
            FigureClass::ModeledRequestCost,
            Band::PercentTwoSided { allowance: 1.0 },
        );
        bands.insert(FigureClass::Bytes, Band::Percent { allowance: 5.0 });
        bands.insert(FigureClass::LatencyP50, Band::Percent { allowance: 1.0 });
        bands.insert(FigureClass::LatencyP95, Band::Percent { allowance: 5.0 });
        bands.insert(FigureClass::PeakMemory, Band::Percent { allowance: 10.0 });
        bands.insert(FigureClass::RangedOpens, Band::Exact);

        let mut expected = BTreeMap::new();
        for class in FigureClass::ALL {
            expected.insert(class, true);
        }
        // Not yet emitted by BenchReport (STOP-and-report, task 996-7): do not
        // require their presence or the gate fails on every real report.
        for class in [
            FigureClass::ObjectCount,
            FigureClass::RangeAmplification,
            FigureClass::RangedOpens,
            FigureClass::PeakMemory,
        ] {
            expected.insert(class, false);
        }

        Bands {
            bands,
            expected,
            absolutes: Absolutes::defaults(),
        }
    }

    /// Load bands from a TOML document, filling any class the document omits
    /// from [`Self::defaults`].
    pub fn from_toml_str(s: &str) -> Result<Bands, CompareError> {
        let parsed: BandsToml =
            toml::from_str(s).map_err(|e| CompareError::MalformedBands(e.to_string()))?;
        let mut bands = Bands::defaults();
        let overrides = [
            (FigureClass::ObjectCount, parsed.object_count),
            (FigureClass::WriteClassRequests, parsed.write_class_requests),
            (FigureClass::DataGets, parsed.data_gets),
            (FigureClass::RangeAmplification, parsed.range_amplification),
            (FigureClass::ModeledRequestCost, parsed.modeled_request_cost),
            (FigureClass::Bytes, parsed.bytes),
            (FigureClass::LatencyP50, parsed.latency_p50),
            (FigureClass::LatencyP95, parsed.latency_p95),
            (FigureClass::PeakMemory, parsed.peak_memory),
            (FigureClass::RangedOpens, parsed.ranged_opens),
        ];
        for (class, table) in overrides {
            if let Some(table) = table {
                bands.bands.insert(class, table.to_band(class)?);
                if let Some(expected) = table.expected {
                    bands.expected.insert(class, expected);
                }
            }
        }
        if let Some(abs) = parsed.absolute {
            let base = &mut bands.absolutes;
            if let Some(v) = abs.range_amplification_ceiling {
                base.range_amplification_ceiling = Some(v);
            }
            if let Some(v) = abs.range_amplification_floor {
                base.range_amplification_floor = Some(v);
            }
            if let Some(v) = abs.ranged_opens_max {
                base.ranged_opens_max = Some(v);
            }
            if let Some(v) = abs.cold_gets_max {
                base.cold_gets_max = Some(v);
            }
            if let Some(v) = abs.modeled_cost_max_nanodollars {
                base.modeled_cost_max_nanodollars = Some(v);
            }
        }
        Ok(bands)
    }

    /// The band for `class`. Every class has a default, so this never returns
    /// `None` for a class in [`FigureClass::ALL`].
    fn for_class(&self, class: FigureClass) -> Band {
        // Every class is inserted by `defaults`, and `from_toml_str` only ever
        // overwrites; the fallback keeps this total without an unwrap.
        self.bands
            .get(&class)
            .copied()
            .unwrap_or(Band::Percent { allowance: 0.0 })
    }

    /// Whether a figure of `class` must appear in both reports. Defaults to
    /// true for a class not named (a new class is expected until told
    /// otherwise).
    fn is_expected(&self, class: FigureClass) -> bool {
        self.expected.get(&class).copied().unwrap_or(true)
    }

    /// The absolute gates.
    pub fn absolutes(&self) -> &Absolutes {
        &self.absolutes
    }
}

/// Why a comparison refused to run at all, distinct from a regression verdict.
/// A refusal means the two reports could not be compared on the same basis; a
/// regression means they were compared and a figure moved too far.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompareError {
    /// A report was not valid JSON, or did not match the [`CostReport`] schema.
    Malformed(String),
    /// The band TOML was not valid.
    MalformedBands(String),
    /// A report parsed but carries no request surface (no data-GET figure): a
    /// legacy report from before request counts existed. Comparing it would
    /// vacuously pass every request band it never emitted.
    MissingRequestSurface {
        /// `"baseline"` or `"candidate"`.
        which: String,
    },
    /// The two reports priced under different effective cost profiles. Their
    /// request and cost figures are the wrong basis to compare.
    ProfileMismatch {
        /// Effective profile name in the baseline, with its prices appended
        /// when the two reports share a name but not a price list.
        baseline: String,
        /// Effective profile name in the candidate, likewise.
        candidate: String,
    },
    /// Neither report records the effective profile it was priced under, yet a
    /// modeled-cost figure carries a value. Two absent stamps are not evidence
    /// of an equal basis, so the costs are denominated in unknown and possibly
    /// different currencies.
    UnknownProfileForPricedFigure {
        /// The modeled-cost figures that carry a value nothing can denominate.
        figures: Vec<String>,
    },
}

impl fmt::Display for CompareError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompareError::Malformed(e) => write!(f, "malformed report: {e}"),
            CompareError::MalformedBands(e) => write!(f, "malformed band config: {e}"),
            CompareError::MissingRequestSurface { which } => write!(
                f,
                "{which} report carries no request surface (no data-GET figure): a legacy report \
                 cannot be compared, it would pass every request band vacuously"
            ),
            CompareError::ProfileMismatch {
                baseline,
                candidate,
            } => write!(
                f,
                "refusing to compare reports priced under different effective cost profiles: \
                 baseline `{baseline}` vs candidate `{candidate}`"
            ),
            CompareError::UnknownProfileForPricedFigure { figures } => write!(
                f,
                "refusing to compare: neither report records an effective cost profile, yet \
                 modeled-cost figures carry values ({}); two absent stamps are not an equal \
                 pricing basis",
                figures
                    .iter()
                    .map(|n| format!("`{n}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }
    }
}

impl std::error::Error for CompareError {}

/// The verdict for one compared figure.
#[derive(Debug, Clone, PartialEq)]
pub enum Verdict {
    /// Within band.
    Pass,
    /// Both reports report the value absent (absent-not-zero); an equal
    /// absence, not a regression.
    EquallyAbsent,
    /// A regression, with a human-readable reason.
    Fail(String),
}

impl Verdict {
    /// Whether this verdict fails the gate.
    pub fn is_fail(&self) -> bool {
        matches!(self, Verdict::Fail(_))
    }

    fn render(&self) -> &str {
        match self {
            Verdict::Pass => "pass",
            Verdict::EquallyAbsent => "pass (equally absent)",
            Verdict::Fail(_) => "FAIL",
        }
    }
}

/// One row of the comparison table: the figure, its two values, the band, and
/// the verdict. Emitted for EVERY compared figure, pass or fail, so a human
/// reads the whole comparison without the JSON open.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    /// The figure name.
    pub name: String,
    /// The figure class.
    pub class: FigureClass,
    /// The baseline value, or `None` if absent there.
    pub baseline: Option<f64>,
    /// The candidate value, or `None` if absent there.
    pub candidate: Option<f64>,
    /// The unit, if the figure carried one (byte kind).
    pub unit: Option<String>,
    /// The band applied.
    pub band: Band,
    /// The verdict.
    pub verdict: Verdict,
}

/// A figure class the bands declare expected that a report carries no figure
/// of. Not a row (there is no figure to put in one), and not a silent skip: an
/// expected class nobody emitted would otherwise pass the gate by never being
/// compared.
#[derive(Debug, Clone, PartialEq)]
pub struct MissingClass {
    /// The class no figure carried.
    pub class: FigureClass,
    /// Where it is missing: `"both reports"`, `"the baseline report"`, or
    /// `"the candidate report"`.
    pub which: String,
}

/// The full comparison result: one row per compared figure, plus any expected
/// class that no figure carried.
#[derive(Debug, Clone, PartialEq)]
pub struct Comparison {
    /// Rows, sorted by figure name for a stable table.
    pub rows: Vec<Row>,
    /// Expected classes absent from a report. Non-empty fails the gate.
    pub missing_expected: Vec<MissingClass>,
}

impl Comparison {
    /// Whether any figure regressed, or an expected class was absent.
    pub fn regressed(&self) -> bool {
        self.rows.iter().any(|r| r.verdict.is_fail()) || !self.missing_expected.is_empty()
    }

    /// Render the comparison as an aligned table a human reads.
    pub fn render_table(&self) -> String {
        let fmt_val = |v: &Option<f64>| match v {
            None => "absent".to_string(),
            Some(x) => render_number(*x),
        };
        // A byte figure's unit names WHICH bytes it counts (wire, charged,
        // decompressed). Two kinds never compare, so the table shows the kind
        // beside the values rather than leaving a reader to guess the basis.
        let fmt_unit = |u: &Option<String>| u.clone().unwrap_or_else(|| "-".to_string());
        let name_h = "figure";
        let class_h = "class";
        let base_h = "baseline";
        let cand_h = "candidate";
        let unit_h = "unit";
        let band_h = "band";
        let verdict_h = "verdict";

        let mut name_w = name_h.len();
        let mut class_w = class_h.len();
        let mut base_w = base_h.len();
        let mut cand_w = cand_h.len();
        let mut unit_w = unit_h.len();
        let mut band_w = band_h.len();
        for r in &self.rows {
            name_w = name_w.max(r.name.len());
            class_w = class_w.max(r.class.as_str().len());
            base_w = base_w.max(fmt_val(&r.baseline).len());
            cand_w = cand_w.max(fmt_val(&r.candidate).len());
            unit_w = unit_w.max(fmt_unit(&r.unit).len());
            band_w = band_w.max(r.band.render().len());
        }

        let mut out = String::new();
        out.push_str(&format!(
            "{name_h:<name_w$}  {class_h:<class_w$}  {base_h:>base_w$}  {cand_h:>cand_w$}  \
             {unit_h:<unit_w$}  {band_h:<band_w$}  {verdict_h}\n"
        ));
        for r in &self.rows {
            let base = fmt_val(&r.baseline);
            let cand = fmt_val(&r.candidate);
            let unit = fmt_unit(&r.unit);
            let band = r.band.render();
            out.push_str(&format!(
                "{:<name_w$}  {:<class_w$}  {:>base_w$}  {:>cand_w$}  {:<unit_w$}  {:<band_w$}  \
                 {}\n",
                r.name,
                r.class.as_str(),
                base,
                cand,
                unit,
                band,
                r.verdict.render(),
            ));
            // Surface the reason inline for a failing row so the table is
            // self-explanatory.
            if let Verdict::Fail(reason) = &r.verdict {
                out.push_str(&format!("{:width$}  ^ {reason}\n", "", width = name_w));
            }
        }
        // An expected class nobody emitted has no row to fail, so it is named
        // under the table rather than left to a reader noticing an absence.
        for missing in &self.missing_expected {
            out.push_str(&format!(
                "MISSING: the bands declare `{}` expected, but no figure of that class appears in \
                 {}\n",
                missing.class, missing.which,
            ));
        }
        out
    }
}

/// Render an f64 that is integer-valued as an integer, otherwise with enough
/// precision to distinguish a band edge.
fn render_number(x: f64) -> String {
    if x.fract() == 0.0 && x.abs() < 1e15 {
        format!("{}", x as i64)
    } else {
        format!("{x}")
    }
}

/// A figure keyed for lookup, plus whether it was duplicated in its report.
struct FigureIndex<'a> {
    /// name -> figure. On a duplicate the name is recorded in `duplicates`.
    by_name: BTreeMap<&'a str, &'a Figure>,
    duplicates: BTreeMap<&'a str, FigureClass>,
}

impl<'a> FigureIndex<'a> {
    fn build(report: &'a CostReport) -> FigureIndex<'a> {
        let mut by_name = BTreeMap::new();
        let mut duplicates = BTreeMap::new();
        for fig in &report.figures {
            if by_name.insert(fig.name.as_str(), fig).is_some() {
                duplicates.insert(fig.name.as_str(), fig.class);
            }
        }
        FigureIndex {
            by_name,
            duplicates,
        }
    }
}

/// Names of the modeled-cost figures that carry a value in either report,
/// sorted and deduplicated. Used only when neither report knows its effective
/// profile, to name what cannot be denominated.
fn unpriced_modeled_costs(baseline: &CostReport, candidate: &CostReport) -> Vec<String> {
    let mut figures: Vec<String> = baseline
        .figures
        .iter()
        .chain(candidate.figures.iter())
        .filter(|f| f.class == FigureClass::ModeledRequestCost && f.value.is_some())
        .map(|f| f.name.clone())
        .collect();
    figures.sort();
    figures.dedup();
    figures
}

/// Compare a candidate report against a baseline under `bands`.
///
/// Refuses (returns `Err`) when the two cannot be compared on the same basis:
/// either report missing its request surface, or the two priced under
/// different effective profiles. Otherwise returns a [`Comparison`] whose
/// [`Comparison::regressed`] is the gate result; the comparison always carries
/// one row per compared figure, pass or fail.
///
/// Three independent checks feed that verdict:
///
/// 1. the per-figure [`Band`], candidate against baseline;
/// 2. the [`Absolutes`], judging EACH report's value on its own, so a physical
///    ceiling or floor cannot be excused by a baseline that shares the defect;
/// 3. expected-class presence ([`Comparison::missing_expected`]), so a class
///    the bands declare expected cannot pass by never being emitted.
pub fn compare(
    baseline: &CostReport,
    candidate: &CostReport,
    bands: &Bands,
) -> Result<Comparison, CompareError> {
    if !baseline.has_request_surface() {
        return Err(CompareError::MissingRequestSurface {
            which: "baseline".to_string(),
        });
    }
    if !candidate.has_request_surface() {
        return Err(CompareError::MissingRequestSurface {
            which: "candidate".to_string(),
        });
    }

    // Profile guard: the effective profile is the one that governed pricing.
    // Two reports whose effective profiles differ cannot be compared. Equality
    // is over the whole profile, not its name: a profile edited in place keeps
    // its name while repricing every figure stamped with it.
    if baseline.profile.effective != candidate.profile.effective {
        let (baseline, candidate) = profile_labels(&baseline.profile, &candidate.profile);
        return Err(CompareError::ProfileMismatch {
            baseline,
            candidate,
        });
    }

    // Equal stamps that are equally ABSENT are not an equal basis: neither run
    // knows what it priced at, so a modeled cost carrying a value cannot be
    // compared to another. Refuse rather than pass it vacuously.
    if baseline.profile.effective.is_none() {
        let figures = unpriced_modeled_costs(baseline, candidate);
        if !figures.is_empty() {
            return Err(CompareError::UnknownProfileForPricedFigure { figures });
        }
    }

    let ctx = CompareContext {
        base_index: FigureIndex::build(baseline),
        cand_index: FigureIndex::build(candidate),
        bands,
        base_request_minimal: baseline.is_request_minimal(),
        cand_request_minimal: candidate.is_request_minimal(),
    };

    // The union of figure names across both reports, sorted for a stable table.
    let mut names: Vec<&str> = ctx
        .base_index
        .by_name
        .keys()
        .chain(ctx.cand_index.by_name.keys())
        .copied()
        .collect();
    names.sort_unstable();
    names.dedup();

    let mut rows = Vec::with_capacity(names.len());
    for name in names {
        let base_fig = ctx.base_index.by_name.get(name).copied();
        let cand_fig = ctx.cand_index.by_name.get(name).copied();
        rows.push(compare_one(&ctx, name, base_fig, cand_fig));
    }

    Ok(Comparison {
        rows,
        missing_expected: missing_expected_classes(baseline, candidate, bands),
    })
}

/// Every class the bands declare expected that carries no figure in one or both
/// reports. A class expected and never emitted passes every band it never
/// produced a row for, which is exactly the vacuous pass the present-exactly-
/// once discipline forbids.
fn missing_expected_classes(
    baseline: &CostReport,
    candidate: &CostReport,
    bands: &Bands,
) -> Vec<MissingClass> {
    let mut missing = Vec::new();
    for class in FigureClass::ALL {
        if !bands.is_expected(class) {
            continue;
        }
        let carries = |report: &CostReport| report.figures.iter().any(|f| f.class == class);
        let which = match (carries(baseline), carries(candidate)) {
            (true, true) => continue,
            (false, false) => "both reports",
            (false, true) => "the baseline report",
            (true, false) => "the candidate report",
        };
        missing.push(MissingClass {
            class,
            which: which.to_string(),
        });
    }
    missing
}

/// Everything `compare_one` reads that is the same for every figure: the two
/// reports' indexes, the bands, and each report's own effective-policy stamp
/// (the absolute gates judge each report on its own, so the two stamps do not
/// collapse into one flag).
struct CompareContext<'a> {
    base_index: FigureIndex<'a>,
    cand_index: FigureIndex<'a>,
    bands: &'a Bands,
    base_request_minimal: bool,
    cand_request_minimal: bool,
}

fn compare_one(
    ctx: &CompareContext<'_>,
    name: &str,
    base_fig: Option<&Figure>,
    cand_fig: Option<&Figure>,
) -> Row {
    let (base_index, cand_index, bands) = (&ctx.base_index, &ctx.cand_index, ctx.bands);
    // Class is whichever report carries the figure; if both carry it and the
    // classes disagree, that is itself a wrong-basis failure.
    let class = base_fig
        .or(cand_fig)
        .map(|f| f.class)
        .unwrap_or(FigureClass::DataGets);
    let band = bands.for_class(class);
    let unit = base_fig
        .and_then(|f| f.unit.clone())
        .or_else(|| cand_fig.and_then(|f| f.unit.clone()));

    let mut row = Row {
        name: name.to_string(),
        class,
        baseline: base_fig.and_then(|f| f.value),
        candidate: cand_fig.and_then(|f| f.value),
        unit,
        band,
        verdict: Verdict::Pass,
    };

    // A figure duplicated in either report fails present-exactly-once.
    if let Some(&dup_class) = base_index.duplicates.get(name) {
        row.class = dup_class;
        row.verdict = Verdict::Fail(format!(
            "figure `{name}` appears more than once in the baseline report"
        ));
        return row;
    }
    if let Some(&dup_class) = cand_index.duplicates.get(name) {
        row.class = dup_class;
        row.verdict = Verdict::Fail(format!(
            "figure `{name}` appears more than once in the candidate report"
        ));
        return row;
    }

    let (base_fig, cand_fig) = match (base_fig, cand_fig) {
        (Some(b), Some(c)) => (b, c),
        (Some(_), None) => {
            row.verdict = Verdict::Fail(format!(
                "figure `{name}` is present in the baseline but absent from the candidate"
            ));
            return row;
        }
        (None, Some(_)) => {
            row.verdict = Verdict::Fail(format!(
                "figure `{name}` is present in the candidate but absent from the baseline"
            ));
            return row;
        }
        // A name in the union always comes from at least one report, so this
        // arm is unreachable through `compare`. It fails closed rather than
        // panicking: a gate that aborts tells a caller less than one that
        // names the figure it could not resolve.
        (None, None) => {
            row.verdict = Verdict::Fail(format!(
                "figure `{name}` resolved to neither report; the comparison index is inconsistent"
            ));
            return row;
        }
    };

    // A figure present in both under different classes prices two different
    // things; comparing them is a wrong-basis error.
    if base_fig.class != cand_fig.class {
        row.verdict = Verdict::Fail(format!(
            "figure `{name}` is class `{}` in the baseline but `{}` in the candidate",
            base_fig.class, cand_fig.class
        ));
        return row;
    }

    // A byte figure names its kind; comparing two kinds is a wrong-basis error.
    if base_fig.unit != cand_fig.unit {
        row.verdict = Verdict::Fail(format!(
            "figure `{name}` unit differs: baseline `{}` vs candidate `{}`",
            base_fig.unit.as_deref().unwrap_or("<none>"),
            cand_fig.unit.as_deref().unwrap_or("<none>"),
        ));
        return row;
    }

    // Absolute gates first: they judge each report on its own, so a value a
    // physical ceiling or floor rejects is a broken measurement, and naming it
    // that way says more than "regressed against a baseline" would. A baseline
    // that violates one fails too -- the gate is not a comparison, and a broken
    // baseline cannot excuse a candidate.
    let absolutes = bands.absolutes();
    for (value, request_minimal, which) in [
        (base_fig.value, ctx.base_request_minimal, "baseline"),
        (cand_fig.value, ctx.cand_request_minimal, "candidate"),
    ] {
        if let Some(value) = value
            && let Some(reason) = absolutes.violation(class, name, value, request_minimal)
        {
            row.verdict = Verdict::Fail(format!("figure `{name}` in the {which} report: {reason}"));
            return row;
        }
    }

    row.verdict = match (base_fig.value, cand_fig.value) {
        // The absent-not-zero convention: equally absent in both reports is an
        // equal absence, not a regression.
        (None, None) => Verdict::EquallyAbsent,
        (Some(_), None) => Verdict::Fail(format!(
            "figure `{name}` has a value in the baseline but is absent in the candidate"
        )),
        (None, Some(_)) => Verdict::Fail(format!(
            "figure `{name}` has a value in the candidate but is absent in the baseline"
        )),
        (Some(baseline), Some(candidate)) => {
            if band.regresses(baseline, candidate) {
                Verdict::Fail(format!(
                    "figure `{name}` regressed: baseline {}, candidate {}, band {}",
                    render_number(baseline),
                    render_number(candidate),
                    band.render(),
                ))
            } else {
                Verdict::Pass
            }
        }
    };
    row
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    fn fig(name: &str, class: FigureClass, value: Option<f64>, unit: Option<&str>) -> Figure {
        Figure {
            name: name.to_string(),
            class,
            value,
            unit: unit.map(str::to_string),
        }
    }

    /// A full baseline report carrying one figure of every class, all present
    /// and priced at the reference profile. No effective-policy stamp: the
    /// request-minimal plan-shape absolute is exercised by
    /// [`request_minimal_report`], not by every case here.
    fn baseline_report() -> CostReport {
        CostReport {
            profile: ProfileStamp {
                requested: StoreCostProfile::reference(),
                effective: Some(StoreCostProfile::reference()),
            },
            effective_policy: None,
            figures: vec![
                fig("object_count", FigureClass::ObjectCount, Some(3469.0), None),
                fig(
                    "write_class_requests",
                    FigureClass::WriteClassRequests,
                    Some(14500.0),
                    None,
                ),
                fig("data_gets", FigureClass::DataGets, Some(149167.0), None),
                fig(
                    "range_amplification",
                    FigureClass::RangeAmplification,
                    Some(1.0),
                    None,
                ),
                fig(
                    "modeled_request_cost",
                    FigureClass::ModeledRequestCost,
                    Some(59667.0),
                    None,
                ),
                fig(
                    "wire_bytes",
                    FigureClass::Bytes,
                    Some(1_000_000.0),
                    Some("wire"),
                ),
                fig("latency_p50", FigureClass::LatencyP50, Some(10.0), None),
                fig("latency_p95", FigureClass::LatencyP95, Some(100.0), None),
                fig(
                    "peak_memory",
                    FigureClass::PeakMemory,
                    Some(1_000_000.0),
                    None,
                ),
                fig("ranged_opens", FigureClass::RangedOpens, Some(0.0), None),
            ],
        }
    }

    /// Replace the value of the named figure in a report, returning the report.
    fn with_value(mut report: CostReport, name: &str, value: Option<f64>) -> CostReport {
        let f = report
            .figures
            .iter_mut()
            .find(|f| f.name == name)
            .expect("figure exists");
        f.value = value;
        report
    }

    #[test]
    fn identical_reports_pass_every_figure_within_band() {
        let base = baseline_report();
        let cand = baseline_report();
        let cmp = compare(&base, &cand, &Bands::defaults()).expect("comparable");
        assert!(!cmp.regressed(), "identical reports must not regress");
        // Every class is present as a row and every row passes. The table lists
        // each compared figure, pass included (deliverable 3).
        assert_eq!(cmp.rows.len(), 10, "one row per figure");
        for row in &cmp.rows {
            assert_eq!(
                row.verdict,
                Verdict::Pass,
                "figure `{}` within band",
                row.name
            );
        }
    }

    #[test]
    fn each_regressed_figure_fails_naming_exactly_that_figure() {
        // Every figure class, moved ONE step past its own band and no further.
        // Exact-band figures regress by +1; percent and absolute figures sit
        // just past the threshold their default band computes, so a band even
        // slightly wider than documented would let the case through.
        let cases: [(&str, f64); 10] = [
            ("object_count", 3470.0),          // exact, +1
            ("write_class_requests", 14501.0), // exact, +1
            ("data_gets", 149168.0),           // exact, +1
            ("range_amplification", 1.0501),   // past +0.05 over 1.0
            ("modeled_request_cost", 60264.0), // past +1% of 59667 (60263.67)
            ("wire_bytes", 1_050_001.0),       // past +5% of 1_000_000
            ("latency_p50", 10.101),           // past +1% of 10
            ("latency_p95", 105.1),            // past +5% of 100
            ("peak_memory", 1_100_001.0),      // past +10% of 1_000_000
            ("ranged_opens", 1.0),             // exact, +1 (plan-shape rule)
        ];
        assert_eq!(
            cases.len(),
            FigureClass::ALL.len(),
            "every figure class carries a one-past-band case"
        );
        for (name, regressed) in cases {
            let base = baseline_report();
            let cand = with_value(baseline_report(), name, Some(regressed));
            let cmp = compare(&base, &cand, &Bands::defaults()).expect("comparable");
            assert!(cmp.regressed(), "`{name}` at {regressed} must regress");
            let failing: Vec<&str> = cmp
                .rows
                .iter()
                .filter(|r| r.verdict.is_fail())
                .map(|r| r.name.as_str())
                .collect();
            assert_eq!(
                failing,
                vec![name],
                "exactly `{name}` fails, got {failing:?}"
            );
        }
    }

    #[test]
    fn absent_figure_asymmetry_fails_and_equally_absent_passes() {
        // Asymmetry: write_class_requests present in the baseline, absent from
        // the candidate's figure list entirely. That is a failure naming it,
        // never a silent skip.
        let base = baseline_report();
        let mut cand = baseline_report();
        cand.figures.retain(|f| f.name != "write_class_requests");
        let cmp = compare(&base, &cand, &Bands::defaults()).expect("comparable");
        assert!(cmp.regressed(), "an asymmetrically-absent figure must fail");
        let failing: Vec<&str> = cmp
            .rows
            .iter()
            .filter(|r| r.verdict.is_fail())
            .map(|r| r.name.as_str())
            .collect();
        assert_eq!(failing, vec!["write_class_requests"]);

        // Equally absent: the absent-not-zero convention. write_class_requests
        // present in BOTH reports but value None (a backend that cannot bill).
        // Passes as equally-absent and says so.
        let base = with_value(baseline_report(), "write_class_requests", None);
        let cand = with_value(baseline_report(), "write_class_requests", None);
        let cmp = compare(&base, &cand, &Bands::defaults()).expect("comparable");
        assert!(!cmp.regressed(), "equally-absent figure must not regress");
        let row = cmp
            .rows
            .iter()
            .find(|r| r.name == "write_class_requests")
            .expect("row present");
        assert_eq!(row.verdict, Verdict::EquallyAbsent);
        assert!(
            row.verdict.render().contains("equally absent"),
            "the verdict says it is equally absent"
        );

        // Value-asymmetry: present-with-value in one, present-but-None in the
        // other. Not an equal absence; a failure.
        let base = baseline_report();
        let cand = with_value(baseline_report(), "write_class_requests", None);
        let cmp = compare(&base, &cand, &Bands::defaults()).expect("comparable");
        assert!(cmp.regressed(), "one-sided absence is a regression");
    }

    #[test]
    fn duplicated_figure_fails_present_exactly_once() {
        let base = baseline_report();
        let mut cand = baseline_report();
        // Emit data_gets twice: the present-exactly-once discipline forbids it.
        cand.figures.push(fig(
            "data_gets",
            FigureClass::DataGets,
            Some(149167.0),
            None,
        ));
        let cmp = compare(&base, &cand, &Bands::defaults()).expect("comparable");
        assert!(cmp.regressed(), "a duplicated figure must fail");
        let row = cmp
            .rows
            .iter()
            .find(|r| r.name == "data_gets")
            .expect("row present");
        match &row.verdict {
            Verdict::Fail(reason) => assert!(
                reason.contains("more than once"),
                "reason names the duplication: {reason}"
            ),
            other => panic!("expected a duplication failure, got {other:?}"),
        }
    }

    #[test]
    fn differing_effective_profiles_refuse_with_both_names() {
        let base = baseline_report();
        let mut other = StoreCostProfile::reference();
        other.name = "gcs-multi-region-2026".to_string();
        other.get_class_nanodollars = 800;
        let mut cand = baseline_report();
        cand.profile.effective = Some(other);

        let err = compare(&base, &cand, &Bands::defaults()).expect_err("must refuse");
        match err {
            CompareError::ProfileMismatch {
                baseline,
                candidate,
            } => {
                assert_eq!(baseline, "s3-intra-region-2026");
                assert_eq!(candidate, "gcs-multi-region-2026");
            }
            other => panic!("expected a profile mismatch, got {other:?}"),
        }
        // The rendered message names both.
        let msg = compare(&base, &cand, &Bands::defaults())
            .expect_err("must refuse")
            .to_string();
        assert!(
            msg.contains("s3-intra-region-2026"),
            "names the baseline profile"
        );
        assert!(
            msg.contains("gcs-multi-region-2026"),
            "names the candidate profile"
        );
    }

    #[test]
    fn one_profile_name_over_two_price_lists_refuses_and_shows_the_prices() {
        // The trap the stamp exists to catch: a profile repriced in place
        // keeps its name, so a name-only refusal message would print the same
        // string twice and identify nothing. The prices disambiguate it.
        let base = baseline_report();
        let mut repriced = StoreCostProfile::reference();
        repriced.get_class_nanodollars = 800;
        let mut cand = baseline_report();
        cand.profile.effective = Some(repriced);

        let err = compare(&base, &cand, &Bands::defaults()).expect_err("must refuse");
        let msg = err.to_string();
        assert!(
            matches!(err, CompareError::ProfileMismatch { .. }),
            "expected a profile mismatch, got {err:?}"
        );
        assert!(msg.contains("get=400"), "shows the baseline price: {msg}");
        assert!(msg.contains("get=800"), "shows the candidate price: {msg}");
    }

    #[test]
    fn two_absent_profile_stamps_refuse_when_a_cost_figure_carries_a_value() {
        // Equal stamps that are equally ABSENT are not an equal basis: neither
        // run knows what it priced at, so comparing modeled costs would pass
        // vacuously on an unknown denominator.
        let mut base = baseline_report();
        let mut cand = baseline_report();
        base.profile.effective = None;
        cand.profile.effective = None;

        let err = compare(&base, &cand, &Bands::defaults()).expect_err("must refuse");
        match &err {
            CompareError::UnknownProfileForPricedFigure { figures } => {
                assert_eq!(figures, &vec!["modeled_request_cost".to_string()]);
            }
            other => panic!("expected an unknown-profile refusal, got {other:?}"),
        }
        assert!(
            err.to_string().contains("modeled_request_cost"),
            "names the figure that cannot be denominated: {err}"
        );

        // Without a valued cost figure there is nothing to denominate, so an
        // unstamped pair compares normally: the guard is scoped to priced
        // figures, it does not ban the Flight lane from the gate outright. Such
        // a lane declares the class unexpected in its bands document; that is
        // what makes dropping the figure a decision on the record rather than an
        // absence the gate cannot see.
        let strip = |mut r: CostReport| {
            r.figures
                .retain(|f| f.class != FigureClass::ModeledRequestCost);
            r.profile.effective = None;
            r
        };
        let unpriced_lane = Bands::from_toml_str(
            "[modeled_request_cost]\nkind = \"percent_two_sided\"\nallowance = 1.0\nexpected = \
             false\n",
        )
        .expect("parse the unpriced-lane bands");
        let cmp = compare(
            &strip(baseline_report()),
            &strip(baseline_report()),
            &unpriced_lane,
        )
        .expect("an unstamped pair with no priced figure is comparable");
        assert!(!cmp.regressed());

        // And the same pair under the SHIPPING defaults, which do expect the
        // class, fails naming it: an expected class nobody emitted must not pass
        // by never being compared.
        let cmp = compare(
            &strip(baseline_report()),
            &strip(baseline_report()),
            &Bands::defaults(),
        )
        .expect("comparable");
        assert!(
            cmp.regressed(),
            "an expected class dropped from both reports"
        );
        assert_eq!(
            cmp.missing_expected,
            vec![MissingClass {
                class: FigureClass::ModeledRequestCost,
                which: "both reports".to_string(),
            }]
        );
    }

    #[test]
    fn the_table_names_the_byte_kind_of_every_byte_figure() {
        // A byte figure's unit says WHICH bytes it counts; a table that omits
        // it leaves the reader to guess the basis of the only rows where the
        // basis is ambiguous.
        let cmp = compare(&baseline_report(), &baseline_report(), &Bands::defaults())
            .expect("comparable");
        let table = cmp.render_table();
        let byte_row = table
            .lines()
            .find(|l| l.starts_with("wire_bytes"))
            .expect("the byte figure has a row");
        // Past the name column: the figure is NAMED `wire_bytes`, so a match
        // anywhere in the row would pass without a unit column existing.
        let after_name = byte_row
            .strip_prefix("wire_bytes")
            .expect("the row starts with the figure name");
        assert!(
            after_name.contains("wire"),
            "the byte row names its kind in the unit column: {byte_row}"
        );
        // Header carries the column, and a unitless figure renders a dash
        // rather than an empty gap that reads as a truncated table.
        assert!(table.lines().next().expect("header").contains("unit"));
        let count_row = table
            .lines()
            .find(|l| l.starts_with("object_count"))
            .expect("object_count has a row");
        assert!(
            count_row.contains(" - "),
            "unitless renders a dash: {count_row}"
        );
    }

    #[test]
    fn legacy_report_missing_request_surface_is_a_typed_refusal() {
        // A well-formed report that parses but carries no data-GET figure: the
        // legacy shape from before request counts existed. Comparing it would
        // vacuously pass every request band it never emitted, so it refuses.
        let legacy = CostReport {
            profile: ProfileStamp {
                requested: StoreCostProfile::reference(),
                effective: Some(StoreCostProfile::reference()),
            },
            effective_policy: None,
            figures: vec![fig(
                "latency_p95",
                FigureClass::LatencyP95,
                Some(100.0),
                None,
            )],
        };
        let base = baseline_report();
        let err = compare(&legacy, &base, &Bands::defaults()).expect_err("must refuse");
        assert!(
            matches!(err, CompareError::MissingRequestSurface { .. }),
            "expected a missing-request-surface refusal, got {err:?}"
        );

        // And a genuinely malformed JSON is a typed refusal too, not a crash.
        let err = CostReport::from_json_str("{ not json").expect_err("malformed");
        assert!(matches!(err, CompareError::Malformed(_)));
    }

    #[test]
    fn each_default_band_admits_its_edge_and_rejects_one_step_past_it() {
        // What pins a band to the value it documents. Every other test would
        // still pass if `latency_p95` were secretly 1% instead of 5%, or
        // `peak_memory` 50% instead of 10%: they only ever check that a big
        // move fails. This brackets each default from both sides -- a
        // candidate exactly AT the band edge passes, and the next meaningful
        // step past it fails -- so widening or narrowing any default breaks
        // exactly one case here.
        //
        // (class, baseline, at-the-edge candidate, one step past the edge)
        let cases: [(FigureClass, f64, f64, f64); 10] = [
            (FigureClass::ObjectCount, 3469.0, 3469.0, 3470.0),
            (FigureClass::WriteClassRequests, 14500.0, 14500.0, 14501.0),
            (FigureClass::DataGets, 149167.0, 149167.0, 149168.0),
            // absolute +0.05
            (FigureClass::RangeAmplification, 1.0, 1.05, 1.0501),
            // +1%: 59667 -> 60263.67
            (FigureClass::ModeledRequestCost, 59667.0, 60263.67, 60264.0),
            // +5%: 1_000_000 -> 1_050_000
            (FigureClass::Bytes, 1_000_000.0, 1_050_000.0, 1_050_001.0),
            // +1%: 1000 -> 1010
            (FigureClass::LatencyP50, 1000.0, 1010.0, 1010.1),
            // +5%: 100 -> 105
            (FigureClass::LatencyP95, 100.0, 105.0, 105.1),
            // +10%: 1_000_000 -> 1_100_000
            (
                FigureClass::PeakMemory,
                1_000_000.0,
                1_100_000.0,
                1_100_001.0,
            ),
            (FigureClass::RangedOpens, 0.0, 0.0, 1.0),
        ];
        assert_eq!(
            cases.len(),
            FigureClass::ALL.len(),
            "every figure class has its band edge pinned"
        );
        let bands = Bands::defaults();
        for (class, baseline, edge, past) in cases {
            let band = bands.for_class(class);
            assert!(
                !band.regresses(baseline, edge),
                "`{class}` band {} must admit its edge {edge} over baseline {baseline}",
                band.render()
            );
            assert!(
                band.regresses(baseline, past),
                "`{class}` band {} must reject {past} over baseline {baseline}",
                band.render()
            );
        }
    }

    #[test]
    fn defaults_and_the_toml_cover_every_figure_class() {
        // `for_class` falls back to a 0% band for a class the map lacks, which
        // would silently make a new class the tightest band in the tool rather
        // than the documented one. Both the compiled defaults and the
        // checked-in TOML must name every class explicitly.
        let defaults = Bands::defaults();
        let toml = include_str!("../cost_regression_bands.toml");
        for class in FigureClass::ALL {
            assert!(
                defaults.bands.contains_key(&class),
                "`{class}` has no compiled default band"
            );
            assert!(
                toml.contains(&format!("[{class}]")),
                "`{class}` has no section in cost_regression_bands.toml"
            );
        }
        assert_eq!(
            defaults.bands.len(),
            FigureClass::ALL.len(),
            "the defaults table carries exactly one band per class"
        );
    }

    #[test]
    fn checked_in_bands_toml_parses_to_the_compiled_defaults() {
        // The defaults and the checked-in TOML must never drift.
        let toml = include_str!("../cost_regression_bands.toml");
        let from_file = Bands::from_toml_str(toml).expect("parse checked-in bands");
        assert_eq!(from_file, Bands::defaults());
    }

    #[test]
    fn a_band_override_replaces_only_the_named_class() {
        // data_gets loosened to a 10% band; a +5% candidate now passes where
        // the exact default would have failed. Every other class keeps its
        // default (object_count still exact).
        let toml = "[data_gets]\nkind = \"percent\"\nallowance = 10.0\n";
        let bands = Bands::from_toml_str(toml).expect("parse override");
        let base = baseline_report();
        let cand = with_value(baseline_report(), "data_gets", Some(149167.0 * 1.05));
        let cmp = compare(&base, &cand, &bands).expect("comparable");
        assert!(
            !cmp.regressed(),
            "the 10% override admits a 5% data_gets rise"
        );
        // object_count keeps its exact default.
        let cand = with_value(baseline_report(), "object_count", Some(3470.0));
        let cmp = compare(&base, &cand, &bands).expect("comparable");
        assert!(
            cmp.regressed(),
            "object_count stays exact under the override"
        );
    }

    #[test]
    fn byte_kind_mismatch_is_a_wrong_basis_failure() {
        // Two byte figures of different KIND cannot be compared (ADR-0996
        // decision 3: a byte column names its kind and two kinds never compare).
        let base = baseline_report();
        let mut cand = baseline_report();
        let f = cand
            .figures
            .iter_mut()
            .find(|f| f.name == "wire_bytes")
            .expect("figure exists");
        f.unit = Some("charged".to_string());
        let cmp = compare(&base, &cand, &Bands::defaults()).expect("comparable");
        assert!(cmp.regressed(), "a byte-kind mismatch must fail");
        let row = cmp
            .rows
            .iter()
            .find(|r| r.name == "wire_bytes")
            .expect("row present");
        match &row.verdict {
            Verdict::Fail(reason) => assert!(reason.contains("unit differs"), "{reason}"),
            other => panic!("expected a unit-mismatch failure, got {other:?}"),
        }
    }

    #[test]
    fn an_improvement_is_not_a_regression_on_a_one_sided_band() {
        // A ONE-SIDED percent/absolute-band figure moving DOWN is an
        // improvement, never a regression. `bytes` (+5%) and `latency_p95`
        // (+5%) are one-sided, so both drops pass.
        //
        // Two classes are deliberately excluded: an EXACT-band figure fails on
        // any drift by design (`Band::Exact` is a drift detector), and
        // `modeled_request_cost` is TWO-SIDED, so its drop fails -- see
        // `a_modeled_cost_drop_fails_the_two_sided_band`.
        let base = baseline_report();
        let cand = with_value(
            with_value(baseline_report(), "wire_bytes", Some(800_000.0)),
            "latency_p95",
            Some(80.0),
        );
        let cmp = compare(&base, &cand, &Bands::defaults()).expect("comparable");
        assert!(
            !cmp.regressed(),
            "one-sided ranged-band figures moving down do not regress:\n{}",
            cmp.render_table()
        );
    }

    #[test]
    fn a_modeled_cost_drop_fails_the_two_sided_band() {
        // The redundant-cross-check rule: under an UNCHANGED profile stamp and
        // unchanged request counts, the modeled cost cannot legitimately fall.
        // A drop is a model bug that underprices the pass, so the two-sided band
        // fails it exactly as it fails a rise, rather than reading it as a
        // saving. Every other figure is unchanged, so the drop is the only
        // candidate explanation.
        let base = baseline_report();
        let cand = with_value(baseline_report(), "modeled_request_cost", Some(50_000.0));
        let cmp = compare(&base, &cand, &Bands::defaults()).expect("comparable");
        let failing: Vec<&str> = cmp
            .rows
            .iter()
            .filter(|r| r.verdict.is_fail())
            .map(|r| r.name.as_str())
            .collect();
        assert_eq!(
            failing,
            vec!["modeled_request_cost"],
            "a modeled-cost DROP is a failure, not an improvement"
        );

        // The band is two-sided, not merely inverted: a rise past the same 1%
        // fails too, and a move INSIDE it in either direction passes.
        for inside in [59_667.0 * 0.995, 59_667.0 * 1.005] {
            let cand = with_value(baseline_report(), "modeled_request_cost", Some(inside));
            assert!(
                !compare(&base, &cand, &Bands::defaults())
                    .expect("comparable")
                    .regressed(),
                "{inside} is inside the +/-1% band"
            );
        }
        let cand = with_value(baseline_report(), "modeled_request_cost", Some(60_264.0));
        assert!(
            compare(&base, &cand, &Bands::defaults())
                .expect("comparable")
                .regressed(),
            "a rise past +1% fails the same band"
        );
    }

    #[test]
    fn an_exact_band_figure_fails_on_a_downward_drift() {
        // object_count dropping is a real event to investigate (objects
        // vanished), so the exact band fails it rather than reading it as an
        // improvement.
        let base = baseline_report();
        let cand = with_value(baseline_report(), "object_count", Some(3468.0));
        let cmp = compare(&base, &cand, &Bands::defaults()).expect("comparable");
        assert!(cmp.regressed(), "an exact band fails on downward drift too");
    }

    /// The hand-copied stamp string the ranged-opens gate matches on must be
    /// the exact rendering of the policy enum; a rename there would otherwise
    /// silently disable the gate rather than fail a test.
    #[test]
    fn the_request_minimal_stamp_matches_the_policys_own_name() {
        assert_eq!(
            REQUEST_MINIMAL_POLICY,
            ravel_query::LogsFetchPolicy::RequestMinimal.as_str(),
            "the absolute gate's stamp string must track the policy enum"
        );
    }

    fn failing_names(cmp: &Comparison) -> Vec<&str> {
        cmp.rows
            .iter()
            .filter(|r| r.verdict.is_fail())
            .map(|r| r.name.as_str())
            .collect()
    }

    /// The physical floor: amplification below 1.0 is a broken measurement,
    /// never an improvement. Both reports carry 0.999 so no relative band can
    /// fire; only the absolute floor can produce the failure. Demonstrated
    /// failing: with `range_amplification_floor` flipped to `None` in
    /// `Absolutes::defaults`, this test's first assertion goes red.
    #[test]
    fn amplification_below_the_physical_floor_fails_as_broken_measurement() {
        let base = with_value(baseline_report(), "range_amplification", Some(0.999));
        let cand = with_value(baseline_report(), "range_amplification", Some(0.999));
        let cmp = compare(&base, &cand, &Bands::defaults()).expect("comparable");
        assert!(cmp.regressed(), "0.999 must fail the 1.0 floor");
        assert_eq!(failing_names(&cmp), vec!["range_amplification"]);
        let row = cmp
            .rows
            .iter()
            .find(|r| r.name == "range_amplification")
            .expect("row present");
        match &row.verdict {
            Verdict::Fail(reason) => assert!(
                reason.contains("broken measurement"),
                "the floor failure names the broken-measurement cause: {reason}"
            ),
            other => panic!("expected Fail, got {other:?}"),
        }
    }

    /// The absolute ceiling fires even when the baseline is equally bad, which
    /// is exactly the case the relative band can never catch.
    #[test]
    fn amplification_past_the_absolute_ceiling_fails_with_an_equal_baseline() {
        let base = with_value(baseline_report(), "range_amplification", Some(1.051));
        let cand = with_value(baseline_report(), "range_amplification", Some(1.051));
        let cmp = compare(&base, &cand, &Bands::defaults()).expect("comparable");
        assert!(cmp.regressed(), "1.051 must fail the 1.05 ceiling");
        assert_eq!(failing_names(&cmp), vec!["range_amplification"]);
        // At the ceiling exactly: passes.
        let base = with_value(baseline_report(), "range_amplification", Some(1.05));
        let cand = with_value(baseline_report(), "range_amplification", Some(1.05));
        let cmp = compare(&base, &cand, &Bands::defaults()).expect("comparable");
        assert!(!cmp.regressed(), "1.05 sits on the ceiling and passes");
    }

    fn with_policy(mut report: CostReport, policy: &str) -> CostReport {
        report.effective_policy = Some(policy.to_string());
        report
    }

    /// The plan-shape rule: one ranged open under a request-minimal stamp
    /// fails absolutely (the baseline carries the same value, so no relative
    /// band can be the cause); the same figures without the stamp pass, since
    /// a byte-minimal run opens ranges on purpose.
    #[test]
    fn ranged_opens_under_request_minimal_fail_and_unstamped_pass() {
        let base = with_policy(
            with_value(baseline_report(), "ranged_opens", Some(1.0)),
            REQUEST_MINIMAL_POLICY,
        );
        let cand = with_policy(
            with_value(baseline_report(), "ranged_opens", Some(1.0)),
            REQUEST_MINIMAL_POLICY,
        );
        let cmp = compare(&base, &cand, &Bands::defaults()).expect("comparable");
        assert!(cmp.regressed(), "one ranged open under request-minimal");
        assert_eq!(failing_names(&cmp), vec!["ranged_opens"]);

        // Unstamped: the gate is policy-scoped and must not fire.
        let base = with_value(baseline_report(), "ranged_opens", Some(1.0));
        let cand = with_value(baseline_report(), "ranged_opens", Some(1.0));
        let cmp = compare(&base, &cand, &Bands::defaults()).expect("comparable");
        assert!(
            !cmp.regressed(),
            "without the stamp the plan-shape gate stays out of the way"
        );
    }

    /// A Flight-lane report (effective profile None) against a stamped one is
    /// the wrong-basis comparison the guard exists for.
    #[test]
    fn a_none_effective_profile_refuses_against_a_stamped_one() {
        let mut base = baseline_report();
        base.profile.effective = None;
        let cand = baseline_report();
        let err = compare(&base, &cand, &Bands::defaults()).expect_err("must refuse");
        let msg = err.to_string();
        assert!(
            msg.contains("<none>"),
            "the refusal renders the absent side as <none>: {msg}"
        );
        assert!(
            msg.contains(&StoreCostProfile::reference().name),
            "the refusal names the stamped side's profile: {msg}"
        );
    }

    /// `deny_unknown_fields` must reach inside a band table, so a misspelled
    /// key in an override fails loudly instead of silently keeping the default.
    #[test]
    fn an_unknown_key_inside_a_band_table_is_refused() {
        let err = Bands::from_toml_str("[data_gets]\nkind = \"exact\"\nallowanec = 10.0\n")
            .expect_err("a misspelled key inside a band table must refuse");
        assert!(
            matches!(err, CompareError::MalformedBands(_)),
            "typed malformed-bands refusal, got {err:?}"
        );
    }

    /// An `allowance` beside `kind = "exact"` is the same misconfiguration
    /// class: the key is structurally valid, so only `to_band` can refuse it.
    #[test]
    fn an_allowance_on_an_exact_band_is_refused() {
        let err = Bands::from_toml_str("[data_gets]\nkind = \"exact\"\nallowance = 10.0\n")
            .expect_err("an exact band with an allowance must refuse");
        assert!(
            matches!(err, CompareError::MalformedBands(_)),
            "typed malformed-bands refusal, got {err:?}"
        );
    }

    /// The corpus ceilings are name-scoped: `data_get_calls` shares the
    /// DataGets class as a diagnostic, and the transfer/retrieval cost terms
    /// share ModeledRequestCost, so a ceiling measured for the billed figure
    /// must not judge its class siblings.
    #[test]
    fn corpus_ceilings_judge_only_their_named_figure() {
        let bands = Bands::from_toml_str(
            "[absolute]\ncold_gets_max = 100.0\nmodeled_cost_max_nanodollars = 1000.0\n",
        )
        .expect("valid absolute overrides");

        let mut base = baseline_report();
        let mut cand = baseline_report();
        for report in [&mut base, &mut cand] {
            // Class siblings far past both ceilings: must pass untouched.
            report.figures.push(fig(
                "data_get_calls",
                FigureClass::DataGets,
                Some(1_000_000.0),
                Some("calls"),
            ));
            report.figures.push(fig(
                "modeled_transfer_cost",
                FigureClass::ModeledRequestCost,
                Some(1_000_000_000.0),
                Some("nanodollars-transfer"),
            ));
            // The named figures inside their ceilings.
            for f in &mut report.figures {
                if f.name == "data_gets" {
                    f.value = Some(90.0);
                }
                if f.name == "modeled_request_cost" {
                    f.value = Some(900.0);
                }
            }
        }
        let cmp = compare(&base, &cand, &bands).expect("comparable");
        assert!(
            !cmp.regressed(),
            "class siblings past the ceilings must not be judged by them: {:?}",
            cmp.rows
                .iter()
                .filter(|r| r.verdict.is_fail())
                .map(|r| &r.name)
                .collect::<Vec<_>>()
        );

        // The named figure past its ceiling still fails (the other named
        // figure held inside its own ceiling so exactly one row fails).
        let base = with_value(
            with_value(baseline_report(), "modeled_request_cost", Some(900.0)),
            "data_gets",
            Some(101.0),
        );
        let cand = base.clone();
        let cmp = compare(&base, &cand, &bands).expect("comparable");
        assert_eq!(
            failing_names(&cmp),
            vec!["data_gets"],
            "the named figure is still gated by its ceiling"
        );
    }

    /// The same figure name carrying different classes in the two reports is a
    /// wrong-basis comparison, not a band question. The mismatched figure is a
    /// latency one so the request-surface guard stays out of the way and the
    /// class check itself produces the failure.
    #[test]
    fn a_class_mismatch_on_one_name_fails_its_row() {
        let base = baseline_report();
        let mut cand = baseline_report();
        for f in &mut cand.figures {
            if f.name == "latency_p95" {
                f.class = FigureClass::LatencyP50;
            }
        }
        let cmp = compare(&base, &cand, &Bands::defaults()).expect("comparable");
        let row = cmp
            .rows
            .iter()
            .find(|r| r.name == "latency_p95")
            .expect("row present");
        assert!(
            row.verdict.is_fail(),
            "a figure whose class differs between the reports must fail its row, got {:?}",
            row.verdict
        );
    }
}
