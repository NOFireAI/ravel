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
//! absent-not-zero convention ([`report::RequestCounts`] renders a backend that
//! cannot bill as `None`, never `0`): a figure whose VALUE is absent in BOTH
//! reports passes as equally-absent and says so.
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

/// A machine-readable cost report: the profile it was priced at and every
/// compared figure. A `Vec` (not a map) so a figure emitted twice survives
/// deserialization as a duplicate rather than collapsing to last-wins.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CostReport {
    /// The cost-profile stamp; the profile guard reads its effective profile.
    pub profile: ProfileStamp,
    /// Every figure, in emission order.
    pub figures: Vec<Figure>,
}

impl CostReport {
    /// Parse a report from JSON, mapping any malformed input to a typed
    /// refusal rather than a panic.
    pub fn from_json_str(s: &str) -> Result<CostReport, CompareError> {
        serde_json::from_str(s).map_err(|e| CompareError::Malformed(e.to_string()))
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

/// A per-figure band. The comparison loop reads only this, never a hardcoded
/// constant, so every threshold is configuration.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Band {
    /// Candidate must equal baseline exactly. Any difference, up or down,
    /// fails. Used for deterministic geometry (object count, ranged opens) and
    /// for request counts by default (a request floor is an exact figure).
    Exact,
    /// Candidate may exceed baseline by at most `allowance` percent. A decrease
    /// is an improvement and passes.
    Percent { allowance: f64 },
    /// Candidate may exceed baseline by at most `allowance` in the figure's own
    /// units. A decrease passes.
    Absolute { allowance: f64 },
}

impl Band {
    /// Whether `candidate` regresses past this band relative to `baseline`.
    /// Only an INCREASE past the allowance is a regression; a decrease is an
    /// improvement. `Exact` treats any difference as a regression.
    ///
    /// The comparison carries a tiny relative epsilon so float noise at the
    /// band edge does not read as a regression; an exact band on integer-valued
    /// figures is unaffected because whole f64 values compare exactly.
    fn regresses(self, baseline: f64, candidate: f64) -> bool {
        let eps = |x: f64| x.abs() * 1e-9 + f64::EPSILON;
        match self {
            Band::Exact => (candidate - baseline).abs() > eps(baseline),
            Band::Percent { allowance } => {
                let threshold = baseline * (1.0 + allowance / 100.0);
                candidate - threshold > eps(threshold)
            }
            Band::Absolute { allowance } => {
                let threshold = baseline + allowance;
                candidate - threshold > eps(threshold)
            }
        }
    }

    /// A compact rendering for the report table, e.g. `"exact"`, `"+5%"`,
    /// `"+0.05"`.
    fn render(self) -> String {
        match self {
            Band::Exact => "exact".to_string(),
            Band::Percent { allowance } => format!("+{allowance}%"),
            Band::Absolute { allowance } => format!("+{allowance}"),
        }
    }
}

/// The band for every figure class. The comparison loop resolves a figure's
/// band through [`Self::for_class`]; defaults live in [`Self::defaults`] and are
/// mirrored by the checked-in TOML, so a threshold is data with a named home,
/// never a magic number in the loop.
#[derive(Debug, Clone, PartialEq)]
pub struct Bands {
    bands: BTreeMap<FigureClass, Band>,
}

/// The overridable TOML surface: one optional band per class. A document that
/// sets a subset leaves the rest at their default; an unknown key is refused so
/// a misspelled figure name fails loudly instead of leaving a default silently
/// in force.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct BandsToml {
    object_count: Option<Band>,
    write_class_requests: Option<Band>,
    data_gets: Option<Band>,
    range_amplification: Option<Band>,
    modeled_request_cost: Option<Band>,
    bytes: Option<Band>,
    latency_p50: Option<Band>,
    latency_p95: Option<Band>,
    peak_memory: Option<Band>,
    ranged_opens: Option<Band>,
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
    /// - `modeled_request_cost`: PERCENT +1%. Modeled from billed attempts, so
    ///   it tracks the request count; 1% absorbs profile-rounding while still
    ///   flagging real growth.
    /// - `bytes`: PERCENT +5%. Wire bytes carry coalesced holes that shift with
    ///   object geometry; 5% covers that jitter.
    /// - `latency_p50`: PERCENT +1%. The repo's hot-path noise floor is
    ///   ~0.2-1%; p50 is a hot/median figure, so 1% is the tightest band that
    ///   does not flake.
    /// - `latency_p95`: PERCENT +5%. The cold/tail noise floor is 3-5%; p95 is
    ///   a tail figure, so 5% is the matching floor.
    /// - `peak_memory`: PERCENT +10%. The ADR peak-residency band is +/-10% vs
    ///   baseline; the regression gate keeps the +10% upper half.
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
            Band::Percent { allowance: 1.0 },
        );
        bands.insert(FigureClass::Bytes, Band::Percent { allowance: 5.0 });
        bands.insert(FigureClass::LatencyP50, Band::Percent { allowance: 1.0 });
        bands.insert(FigureClass::LatencyP95, Band::Percent { allowance: 5.0 });
        bands.insert(FigureClass::PeakMemory, Band::Percent { allowance: 10.0 });
        bands.insert(FigureClass::RangedOpens, Band::Exact);
        Bands { bands }
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
        for (class, band) in overrides {
            if let Some(band) = band {
                bands.bands.insert(class, band);
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

/// The full comparison result: one row per compared figure.
#[derive(Debug, Clone, PartialEq)]
pub struct Comparison {
    /// Rows, sorted by figure name for a stable table.
    pub rows: Vec<Row>,
}

impl Comparison {
    /// Whether any figure regressed.
    pub fn regressed(&self) -> bool {
        self.rows.iter().any(|r| r.verdict.is_fail())
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

    let base_index = FigureIndex::build(baseline);
    let cand_index = FigureIndex::build(candidate);

    // The union of figure names across both reports, sorted for a stable table.
    let mut names: Vec<&str> = base_index
        .by_name
        .keys()
        .chain(cand_index.by_name.keys())
        .copied()
        .collect();
    names.sort_unstable();
    names.dedup();

    let mut rows = Vec::with_capacity(names.len());
    for name in names {
        let base_fig = base_index.by_name.get(name).copied();
        let cand_fig = cand_index.by_name.get(name).copied();
        rows.push(compare_one(
            name,
            base_fig,
            cand_fig,
            &base_index,
            &cand_index,
            bands,
        ));
    }

    Ok(Comparison { rows })
}

fn compare_one(
    name: &str,
    base_fig: Option<&Figure>,
    cand_fig: Option<&Figure>,
    base_index: &FigureIndex<'_>,
    cand_index: &FigureIndex<'_>,
    bands: &Bands,
) -> Row {
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
    /// and priced at the reference profile.
    fn baseline_report() -> CostReport {
        CostReport {
            profile: ProfileStamp {
                requested: StoreCostProfile::reference(),
                effective: Some(StoreCostProfile::reference()),
            },
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
        // figures, it does not ban the Flight lane from the gate outright.
        let strip = |mut r: CostReport| {
            r.figures
                .retain(|f| f.class != FigureClass::ModeledRequestCost);
            r.profile.effective = None;
            r
        };
        let cmp = compare(
            &strip(baseline_report()),
            &strip(baseline_report()),
            &Bands::defaults(),
        )
        .expect("an unstamped pair with no priced figure is comparable");
        assert!(!cmp.regressed());
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
    fn an_improvement_is_not_a_regression_on_a_ranged_band() {
        // A percent/absolute-band figure moving DOWN is an improvement, never a
        // regression. (An EXACT-band figure fails on any drift, up or down, by
        // design: it is a drift detector, not a one-sided guard -- see
        // `Band::Exact`.)
        let base = baseline_report();
        let cand = with_value(
            with_value(baseline_report(), "modeled_request_cost", Some(50_000.0)),
            "latency_p95",
            Some(80.0),
        );
        let cmp = compare(&base, &cand, &Bands::defaults()).expect("comparable");
        assert!(
            !cmp.regressed(),
            "ranged-band figures moving down do not regress"
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
}
