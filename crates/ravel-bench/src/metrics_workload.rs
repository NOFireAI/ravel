//! The MetricsBench workload manifest (ADR-0927 decision 11).
//!
//! One versioned artifact, `benchmarks/metrics/workload.json`, declaring
//! everything the deterministic generator needs and everything a report has to
//! restate: the seed, the generator configuration, the metric families, the
//! label distributions, and the four profiles. It is loaded and gated exactly
//! as the query corpus is ([`crate::promql_corpus`]), so a manifest that no
//! longer describes what the generator would produce is a loud typed error
//! rather than a silently different workload.
//!
//! The `ci` profile's non-comparability is a typed field on the profile
//! ([`Comparability`]), not a sentence in a README: ADR-0927 decision 11 says
//! it "cannot be presented as a performance result", and a reader that loads
//! the artifact can only refuse to publish it if the refusal is in the data.
//! [`Profile::is_publishable`] is that check, and [`gate_workload`] refuses a
//! manifest that marks `ci` comparable at all.

use std::collections::{BTreeSet, HashSet};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// The manifest format version this module reads and writes.
pub const WORKLOAD_FORMAT_VERSION: u32 = 1;

/// The profile names ADR-0927 decision 11 fixes. A manifest declaring a
/// different set is refused: a run over a different profile set is not
/// comparable to a run over this one, and decision 11's whole point is that the
/// three axes vary independently.
pub const REQUIRED_PROFILES: &[&str] = &["cardinality", "history", "churn", "ci"];

/// The profiles ADR-0927 decision 11 marks non-comparable. Checked against the
/// artifact rather than assumed, so a manifest that quietly promotes `ci` to a
/// publishable profile fails the gate.
pub const NON_COMPARABLE_PROFILES: &[&str] = &["ci"];

/// Seconds in one churn epoch. Churn is specified per hour (ADR-0927 decision
/// 11), so the generator retires and creates series on hour boundaries.
pub const CHURN_EPOCH_SECS: u64 = 3600;

/// Whether a profile's figures may appear in a performance or cost table.
///
/// A typed field rather than prose. `Comparable` is a positive claim, and
/// `NonComparable` carries the reason a reader is shown when a publish is
/// refused.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Comparability {
    /// The profile's figures may be compared, subject to the substrate rule
    /// (ADR-0927 decision 10: only the real-S3 lane publishes).
    Comparable,
    /// The profile exercises the code paths but its figures are not a
    /// performance result.
    NonComparable {
        /// Why the profile's figures cannot be published.
        reason: String,
    },
}

impl Comparability {
    /// Whether figures from this profile may be published.
    pub fn is_comparable(&self) -> bool {
        matches!(self, Comparability::Comparable)
    }

    /// The stated reason, or `None` for a comparable profile.
    pub fn reason(&self) -> Option<&str> {
        match self {
            Comparability::Comparable => None,
            Comparability::NonComparable { reason } => Some(reason),
        }
    }
}

/// One workload profile: one axis varied, the others pinned.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    /// The profile name, one of [`REQUIRED_PROFILES`].
    pub name: String,
    /// Series alive at any one instant.
    pub active_series: u64,
    /// Samples each series receives over the profile's duration.
    pub samples_per_series: u64,
    /// Seconds between scrapes.
    pub scrape_interval_secs: u64,
    /// Total wall-clock span the generated data covers.
    pub duration_secs: u64,
    /// Percent of the active set retired and replaced each
    /// [`CHURN_EPOCH_SECS`], in basis points (2000 = 20%/h) so the value is
    /// exact rather than a float that cannot represent it.
    pub churn_basis_points_per_hour: u64,
    /// Total samples the profile nominally generates, restated here so the
    /// artifact carries the figure a report quotes. [`gate_workload`] recomputes
    /// it from `active_series * samples_per_series` and refuses a mismatch.
    pub total_samples: u64,
    /// Whether the profile's figures may be published.
    pub comparability: Comparability,
}

impl Profile {
    /// Whether this profile's figures may appear in a performance or cost
    /// table. A caller that publishes a figure checks this first.
    pub fn is_publishable(&self) -> bool {
        self.comparability.is_comparable()
    }

    /// Churn epochs the profile spans, counting the first (partial) one.
    ///
    /// Measured from the last sample's offset rather than from `duration_secs`,
    /// because that is the step the generator actually reaches: a 240-step run
    /// at 15 s covers 3600 s of wall clock but its last sample lands at 3585 s,
    /// inside the first epoch.
    pub fn churn_epochs(&self) -> u64 {
        if self.samples_per_series == 0 {
            return 1;
        }
        (self.samples_per_series - 1) * self.scrape_interval_secs / CHURN_EPOCH_SECS + 1
    }

    /// Series retired and replaced at each epoch boundary. Truncating, so the
    /// figure is exact and reproducible rather than rounded differently by two
    /// readers.
    pub fn churned_series_per_epoch(&self) -> u64 {
        self.active_series * self.churn_basis_points_per_hour / 10_000
    }

    /// Distinct series the profile creates over its whole duration: the active
    /// set plus one replacement cohort per epoch boundary crossed.
    pub fn total_series_created(&self) -> u64 {
        self.active_series + (self.churn_epochs() - 1) * self.churned_series_per_epoch()
    }
}

/// What a metric family emits.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FamilyKind {
    /// A float that moves up and down.
    Gauge,
    /// A float that only increases, apart from injected resets.
    Counter,
    /// A Prometheus classic histogram: `_bucket{le}` per bound plus `+Inf`,
    /// `_sum`, and `_count`, all monotonic.
    ClassicHistogram,
    /// A native histogram (ADR-0108): one series carrying schema, count, sum,
    /// and bucket deltas.
    NativeHistogram,
}

/// One metric family: its name, what it emits, which label dimensions it
/// carries, and its share of the active-series budget.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricFamily {
    /// The metric name, as PromQL selects it.
    pub name: String,
    /// What the family emits.
    pub kind: FamilyKind,
    /// The fixed label dimensions, by [`LabelDimension::name`]. The scaling
    /// label ([`GeneratorConfig::scaling_label`]) is added by the generator and
    /// must not appear here.
    pub labels: Vec<String>,
    /// The family's share of a profile's active series, in permille. The shares
    /// across all families sum to exactly 1000.
    pub series_permille: u64,
}

/// One label dimension and the exact values it takes.
///
/// Values are listed rather than generated from a prefix so the corpus can
/// select on a literal (`job="metricsbench-api"`) and a test can prove the
/// literal exists in the workload.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LabelDimension {
    /// The label name.
    pub name: String,
    /// Every value the label takes, in the order the generator assigns them.
    pub values: Vec<String>,
}

/// How often an injected anomaly fires, as "one series-step in N". Zero
/// disables that anomaly, which is how the `cardinality` and `history` profiles
/// stay clean where ADR-0927 decision 11 says "churn: none".
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AnomalyRates {
    /// One series-step in N is omitted entirely (a missed scrape).
    pub missing_sample_one_in: u64,
    /// One series-step in N emits a staleness marker instead of a value.
    pub stale_marker_one_in: u64,
    /// One series-step in N resets a counter or histogram instance to zero.
    pub counter_reset_one_in: u64,
    /// One series-step in N is stamped two intervals early, so it reaches the
    /// stream after a sample that is newer than it.
    pub out_of_order_one_in: u64,
}

/// Everything the generator needs beyond the families and the profiles.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GeneratorConfig {
    /// The label whose value scales with the series count. Every family carries
    /// it, and the generator assigns it, so it is declared once here.
    pub scaling_label: String,
    /// The prefix the scaling label's values carry; the generator appends the
    /// instance ordinal. A corpus entry selecting one series names a literal
    /// built from this, so it is data rather than a format string in code.
    pub scaling_label_value_prefix: String,
    /// The `le` bounds of every classic histogram, ascending. `+Inf` is implied
    /// and not listed.
    pub classic_histogram_bounds: Vec<f64>,
    /// The native-histogram schema every native histogram declares (ADR-0108).
    pub native_histogram_schema: i32,
    /// Positive bucket count every native histogram emits.
    pub native_histogram_buckets: u32,
    /// A metric name no family uses, for the corpus' metadata-only entries: a
    /// selector that resolves to nothing reads no sample values, which is what
    /// makes those entries metadata-only rather than merely cheap.
    pub absent_metric_name: String,
    /// The injected anomaly rates.
    pub anomalies: AnomalyRates,
}

/// The parsed manifest.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkloadFile {
    /// The format version, which must equal [`WORKLOAD_FORMAT_VERSION`].
    pub version: u32,
    /// The one seed every generated figure derives from. The same seed and the
    /// same manifest produce byte-identical output.
    pub seed: u64,
    /// Generator configuration.
    pub generator: GeneratorConfig,
    /// The label dimensions families draw from.
    pub label_dimensions: Vec<LabelDimension>,
    /// The metric families.
    pub families: Vec<MetricFamily>,
    /// The four profiles.
    pub profiles: Vec<Profile>,
}

impl WorkloadFile {
    /// The profile named `name`, or `None`.
    pub fn profile(&self, name: &str) -> Option<&Profile> {
        self.profiles.iter().find(|p| p.name == name)
    }

    /// The label dimension named `name`, or `None`.
    pub fn dimension(&self, name: &str) -> Option<&LabelDimension> {
        self.label_dimensions.iter().find(|d| d.name == name)
    }

    /// Time series one instance of `kind` emits: one for a gauge, a counter, or
    /// a native histogram, and one per `le` bound plus `+Inf`, `_sum`, and
    /// `_count` for a classic histogram.
    pub fn series_per_instance(&self, kind: FamilyKind) -> u64 {
        match kind {
            FamilyKind::Gauge | FamilyKind::Counter | FamilyKind::NativeHistogram => 1,
            FamilyKind::ClassicHistogram => {
                self.generator.classic_histogram_bounds.len() as u64 + 3
            }
        }
    }

    /// Time series `family` emits under `profile`.
    pub fn family_series(&self, profile: &Profile, family: &MetricFamily) -> u64 {
        profile.active_series * family.series_permille / 1000
    }

    /// Instances (histograms, or plain series) `family` emits under `profile`.
    pub fn family_instances(&self, profile: &Profile, family: &MetricFamily) -> u64 {
        self.family_series(profile, family) / self.series_per_instance(family.kind)
    }

    /// Every metric name the manifest implies, including the classic-histogram
    /// suffixes. A corpus entry may select any of these and nothing else.
    pub fn emitted_metric_names(&self) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        for family in &self.families {
            match family.kind {
                FamilyKind::ClassicHistogram => {
                    for suffix in ["_bucket", "_sum", "_count"] {
                        out.insert(format!("{}{suffix}", family.name));
                    }
                }
                _ => {
                    out.insert(family.name.clone());
                }
            }
        }
        out
    }
}

/// Everything that can go wrong loading or gating a manifest. Every variant
/// names what is at fault and, where a figure disagrees, both figures.
#[derive(Debug, thiserror::Error)]
pub enum WorkloadError {
    /// The manifest could not be read.
    #[error("workload manifest {path}: {source}")]
    Read {
        /// The path that was read.
        path: PathBuf,
        /// The underlying I/O failure.
        #[source]
        source: std::io::Error,
    },
    /// The manifest is not a valid manifest document.
    #[error("workload manifest {path} is not a valid JSON manifest document: {source}")]
    Parse {
        /// The path that was parsed.
        path: PathBuf,
        /// The underlying `serde_json` failure.
        #[source]
        source: serde_json::Error,
    },
    /// The document declares a format version this module does not read.
    #[error(
        "workload manifest {path} declares format version {found}, but this build reads \
         version {expected}"
    )]
    UnsupportedVersion {
        /// The path that was parsed.
        path: PathBuf,
        /// The version the document declared.
        found: u32,
        /// The version this build reads.
        expected: u32,
    },
    /// The profile set is not the one ADR-0927 decision 11 fixes.
    #[error(
        "workload manifest declares profiles {found:?}, but ADR-0927 decision 11 fixes them at \
         {expected:?}; a run over a different profile set is not comparable to one over this set"
    )]
    ProfileSetMismatch {
        /// The profile names the manifest declared, sorted.
        found: Vec<String>,
        /// The names ADR-0927 decision 11 fixes, sorted.
        expected: Vec<String>,
    },
    /// A profile whose sample grid does not close: the samples, the scrape
    /// interval, and the duration disagree.
    #[error(
        "profile `{profile}` declares {samples_per_series} samples at {scrape_interval_secs}s \
         over {duration_secs}s, but {samples_per_series} * {scrape_interval_secs} = {computed}"
    )]
    SampleGridMismatch {
        /// The offending profile.
        profile: String,
        /// Its declared samples per series.
        samples_per_series: u64,
        /// Its declared scrape interval.
        scrape_interval_secs: u64,
        /// Its declared duration.
        duration_secs: u64,
        /// The duration the other two figures imply.
        computed: u64,
    },
    /// A profile's declared total does not equal the total its own figures
    /// imply.
    #[error(
        "profile `{profile}` declares total_samples {declared}, but active_series * \
         samples_per_series = {computed}"
    )]
    TotalSamplesMismatch {
        /// The offending profile.
        profile: String,
        /// The declared total.
        declared: u64,
        /// The computed total.
        computed: u64,
    },
    /// A profile ADR-0927 marks non-comparable is marked comparable here.
    #[error(
        "profile `{profile}` is marked comparable, but ADR-0927 decision 11 marks it \
         non-comparable: its figures cannot be presented as a performance result, and the \
         refusal has to live in the artifact so a reader that loads it can refuse to publish"
    )]
    MustBeNonComparable {
        /// The offending profile.
        profile: String,
    },
    /// A non-comparable profile states no reason, so a refusal to publish has
    /// nothing to show the reader.
    #[error(
        "profile `{profile}` is marked non-comparable with an empty reason; a refusal to \
         publish must state why"
    )]
    BlankNonComparableReason {
        /// The offending profile.
        profile: String,
    },
    /// The family shares do not partition the active-series budget.
    #[error("metric family series_permille values sum to {total}, not 1000")]
    SeriesShareNotThousand {
        /// The sum the manifest declares.
        total: u64,
    },
    /// Two families share a name.
    #[error("metric family `{name}` is declared more than once")]
    DuplicateFamily {
        /// The duplicated name.
        name: String,
    },
    /// Two label dimensions share a name.
    #[error("label dimension `{name}` is declared more than once")]
    DuplicateDimension {
        /// The duplicated name.
        name: String,
    },
    /// A label dimension carries no values, so it cannot be assigned.
    #[error("label dimension `{name}` declares no values")]
    EmptyDimension {
        /// The offending dimension.
        name: String,
    },
    /// A family names a dimension the manifest does not declare.
    #[error("metric family `{family}` names label dimension `{label}`, which is not declared")]
    UnknownDimension {
        /// The offending family.
        family: String,
        /// The undeclared dimension it named.
        label: String,
    },
    /// The scaling label is declared or listed as an ordinary dimension. It is
    /// assigned by the generator, so a second source for it would collide.
    #[error(
        "the scaling label `{label}` is declared as an ordinary label dimension or listed on a \
         family; the generator assigns it, so it must appear in neither place"
    )]
    ScalingLabelDeclared {
        /// The scaling label.
        label: String,
    },
    /// A family's share of a profile's active series is not a whole number of
    /// series.
    #[error(
        "family `{family}` takes {series_permille} permille of profile `{profile}`'s \
         {active_series} active series, which is not a whole number of series; ADR-0927 \
         decision 11 requires an exact active-series count"
    )]
    InexactSeriesBudget {
        /// The offending profile.
        profile: String,
        /// The offending family.
        family: String,
        /// The profile's active-series count.
        active_series: u64,
        /// The family's share.
        series_permille: u64,
    },
    /// A family's series budget is not a whole number of instances.
    #[error(
        "family `{family}` gets {series} series under profile `{profile}`, which is not a whole \
         number of {series_per_instance}-series instances"
    )]
    InexactInstanceBudget {
        /// The offending profile.
        profile: String,
        /// The offending family.
        family: String,
        /// The family's series budget.
        series: u64,
        /// Series one instance emits.
        series_per_instance: u64,
    },
    /// The classic-histogram bounds are empty or not strictly ascending.
    #[error(
        "classic_histogram_bounds must be a non-empty, strictly ascending list of finite \
         bounds; got {bounds:?}"
    )]
    BadHistogramBounds {
        /// The bounds the manifest declared.
        bounds: Vec<f64>,
    },
    /// A native histogram would carry no buckets.
    #[error("native_histogram_buckets is 0; a native histogram must carry at least one bucket")]
    NoNativeBuckets,
    /// The scaling label's value prefix is blank, so every instance would carry
    /// a bare ordinal.
    #[error("scaling_label_value_prefix is blank; the scaling label's values need a prefix")]
    BlankScalingPrefix,
    /// The absent-metric sentinel collides with a real family name.
    #[error(
        "absent_metric_name `{name}` collides with an emitted metric name; the metadata-only \
         corpus entries select it precisely because nothing emits it"
    )]
    AbsentMetricCollides {
        /// The colliding name.
        name: String,
    },
}

/// Apply every structural check to a manifest, identically for the checked-in
/// artifact and for an external file.
pub fn gate_workload(workload: &WorkloadFile) -> Result<(), WorkloadError> {
    let scaling = workload.generator.scaling_label.as_str();

    // Generator configuration.
    let bounds = &workload.generator.classic_histogram_bounds;
    let ascending = bounds
        .windows(2)
        .all(|w| w[0] < w[1] && w[0].is_finite() && w[1].is_finite());
    if bounds.is_empty() || !ascending || bounds.iter().any(|b| !b.is_finite()) {
        return Err(WorkloadError::BadHistogramBounds {
            bounds: bounds.clone(),
        });
    }
    if workload.generator.native_histogram_buckets == 0 {
        return Err(WorkloadError::NoNativeBuckets);
    }
    if workload
        .generator
        .scaling_label_value_prefix
        .trim()
        .is_empty()
    {
        return Err(WorkloadError::BlankScalingPrefix);
    }

    // Label dimensions.
    let mut seen_dims: HashSet<&str> = HashSet::new();
    for dim in &workload.label_dimensions {
        if dim.name == scaling {
            return Err(WorkloadError::ScalingLabelDeclared {
                label: scaling.to_string(),
            });
        }
        if !seen_dims.insert(dim.name.as_str()) {
            return Err(WorkloadError::DuplicateDimension {
                name: dim.name.clone(),
            });
        }
        if dim.values.is_empty() {
            return Err(WorkloadError::EmptyDimension {
                name: dim.name.clone(),
            });
        }
    }

    // Families.
    let mut seen_families: HashSet<&str> = HashSet::new();
    let mut permille_total = 0u64;
    for family in &workload.families {
        if !seen_families.insert(family.name.as_str()) {
            return Err(WorkloadError::DuplicateFamily {
                name: family.name.clone(),
            });
        }
        for label in &family.labels {
            if label == scaling {
                return Err(WorkloadError::ScalingLabelDeclared {
                    label: scaling.to_string(),
                });
            }
            if workload.dimension(label).is_none() {
                return Err(WorkloadError::UnknownDimension {
                    family: family.name.clone(),
                    label: label.clone(),
                });
            }
        }
        permille_total += family.series_permille;
    }
    if permille_total != 1000 {
        return Err(WorkloadError::SeriesShareNotThousand {
            total: permille_total,
        });
    }
    if workload
        .emitted_metric_names()
        .contains(&workload.generator.absent_metric_name)
    {
        return Err(WorkloadError::AbsentMetricCollides {
            name: workload.generator.absent_metric_name.clone(),
        });
    }

    // Profiles.
    let found: Vec<String> = {
        let mut v: Vec<String> = workload.profiles.iter().map(|p| p.name.clone()).collect();
        v.sort();
        v
    };
    let expected: Vec<String> = {
        let mut v: Vec<String> = REQUIRED_PROFILES.iter().map(|s| (*s).to_string()).collect();
        v.sort();
        v
    };
    if found != expected {
        return Err(WorkloadError::ProfileSetMismatch { found, expected });
    }
    for profile in &workload.profiles {
        let computed = profile.samples_per_series * profile.scrape_interval_secs;
        if computed != profile.duration_secs {
            return Err(WorkloadError::SampleGridMismatch {
                profile: profile.name.clone(),
                samples_per_series: profile.samples_per_series,
                scrape_interval_secs: profile.scrape_interval_secs,
                duration_secs: profile.duration_secs,
                computed,
            });
        }
        let computed = profile.active_series * profile.samples_per_series;
        if computed != profile.total_samples {
            return Err(WorkloadError::TotalSamplesMismatch {
                profile: profile.name.clone(),
                declared: profile.total_samples,
                computed,
            });
        }
        let must_be_non_comparable = NON_COMPARABLE_PROFILES.contains(&profile.name.as_str());
        match &profile.comparability {
            Comparability::Comparable if must_be_non_comparable => {
                return Err(WorkloadError::MustBeNonComparable {
                    profile: profile.name.clone(),
                });
            }
            Comparability::NonComparable { reason } if reason.trim().is_empty() => {
                return Err(WorkloadError::BlankNonComparableReason {
                    profile: profile.name.clone(),
                });
            }
            _ => {}
        }
        for family in &workload.families {
            if profile.active_series * family.series_permille % 1000 != 0 {
                return Err(WorkloadError::InexactSeriesBudget {
                    profile: profile.name.clone(),
                    family: family.name.clone(),
                    active_series: profile.active_series,
                    series_permille: family.series_permille,
                });
            }
            let series = workload.family_series(profile, family);
            let per_instance = workload.series_per_instance(family.kind);
            if !series.is_multiple_of(per_instance) {
                return Err(WorkloadError::InexactInstanceBudget {
                    profile: profile.name.clone(),
                    family: family.name.clone(),
                    series,
                    series_per_instance: per_instance,
                });
            }
        }
    }
    Ok(())
}

/// Read a manifest, parse it, and gate it.
pub fn load_workload(path: impl AsRef<Path>) -> Result<WorkloadFile, WorkloadError> {
    let path = path.as_ref();
    let text = std::fs::read_to_string(path).map_err(|source| WorkloadError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    let workload: WorkloadFile =
        serde_json::from_str(&text).map_err(|source| WorkloadError::Parse {
            path: path.to_path_buf(),
            source,
        })?;
    if workload.version != WORKLOAD_FORMAT_VERSION {
        return Err(WorkloadError::UnsupportedVersion {
            path: path.to_path_buf(),
            found: workload.version,
            expected: WORKLOAD_FORMAT_VERSION,
        });
    }
    gate_workload(&workload)?;
    Ok(workload)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    /// A manifest small enough to read, shaped exactly like the checked-in one:
    /// the same families and shares, and the four required profiles reduced to
    /// the `ci` scale so a test can name every figure.
    fn fixture() -> WorkloadFile {
        let profile = |name: &str, comparability: Comparability| Profile {
            name: name.to_string(),
            active_series: 1_000,
            samples_per_series: 120,
            scrape_interval_secs: 15,
            duration_secs: 1_800,
            churn_basis_points_per_hour: 0,
            total_samples: 120_000,
            comparability,
        };
        WorkloadFile {
            version: WORKLOAD_FORMAT_VERSION,
            seed: 7,
            generator: GeneratorConfig {
                scaling_label: "instance".to_string(),
                scaling_label_value_prefix: "mb-instance-".to_string(),
                classic_histogram_bounds: vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5],
                native_histogram_schema: 2,
                native_histogram_buckets: 8,
                absent_metric_name: "metricsbench_absent_metric".to_string(),
                anomalies: AnomalyRates {
                    missing_sample_one_in: 500,
                    stale_marker_one_in: 900,
                    counter_reset_one_in: 700,
                    out_of_order_one_in: 600,
                },
            },
            label_dimensions: vec![
                LabelDimension {
                    name: "job".to_string(),
                    values: vec!["a".to_string(), "b".to_string()],
                },
                LabelDimension {
                    name: "region".to_string(),
                    values: vec!["r1".to_string()],
                },
            ],
            families: vec![
                MetricFamily {
                    name: "mb_gauge".to_string(),
                    kind: FamilyKind::Gauge,
                    labels: vec!["job".to_string(), "region".to_string()],
                    series_permille: 450,
                },
                MetricFamily {
                    name: "mb_counter".to_string(),
                    kind: FamilyKind::Counter,
                    labels: vec!["job".to_string()],
                    series_permille: 300,
                },
                MetricFamily {
                    name: "mb_classic".to_string(),
                    kind: FamilyKind::ClassicHistogram,
                    labels: vec!["job".to_string()],
                    series_permille: 150,
                },
                MetricFamily {
                    name: "mb_native".to_string(),
                    kind: FamilyKind::NativeHistogram,
                    labels: vec!["job".to_string()],
                    series_permille: 100,
                },
            ],
            profiles: vec![
                profile("cardinality", Comparability::Comparable),
                profile("history", Comparability::Comparable),
                profile("churn", Comparability::Comparable),
                profile(
                    "ci",
                    Comparability::NonComparable {
                        reason: "1,000 series over 30 minutes exercises the code paths and \
                                 measures nothing"
                            .to_string(),
                    },
                ),
            ],
        }
    }

    #[test]
    fn the_fixture_manifest_gates_clean_and_its_derived_figures_are_exact() {
        let w = fixture();
        gate_workload(&w).expect("fixture manifest gates clean");
        let ci = w.profile("ci").expect("ci profile present");
        // 7 declared bounds plus +Inf, _sum and _count: 10 series per classic
        // histogram instance.
        assert_eq!(w.series_per_instance(FamilyKind::ClassicHistogram), 10);
        assert_eq!(w.series_per_instance(FamilyKind::Gauge), 1);
        assert_eq!(w.series_per_instance(FamilyKind::NativeHistogram), 1);
        let series: Vec<u64> = w.families.iter().map(|f| w.family_series(ci, f)).collect();
        assert_eq!(series, vec![450, 300, 150, 100]);
        assert_eq!(series.iter().sum::<u64>(), ci.active_series);
        let instances: Vec<u64> = w
            .families
            .iter()
            .map(|f| w.family_instances(ci, f))
            .collect();
        assert_eq!(instances, vec![450, 300, 15, 100]);
        assert_eq!(
            w.emitted_metric_names().into_iter().collect::<Vec<_>>(),
            vec![
                "mb_classic_bucket".to_string(),
                "mb_classic_count".to_string(),
                "mb_classic_sum".to_string(),
                "mb_counter".to_string(),
                "mb_gauge".to_string(),
                "mb_native".to_string(),
            ]
        );
    }

    #[test]
    fn a_comparable_ci_profile_is_refused() {
        let mut w = fixture();
        for p in &mut w.profiles {
            if p.name == "ci" {
                p.comparability = Comparability::Comparable;
            }
        }
        let err = gate_workload(&w).expect_err("a comparable ci profile must fail");
        assert!(
            matches!(&err, WorkloadError::MustBeNonComparable { profile } if profile == "ci"),
            "wrong error variant: {err:?}"
        );
        assert!(err.to_string().contains("refuse to publish"), "{err}");
    }

    #[test]
    fn a_blank_non_comparable_reason_is_refused() {
        let mut w = fixture();
        for p in &mut w.profiles {
            if p.name == "ci" {
                p.comparability = Comparability::NonComparable {
                    reason: "  ".to_string(),
                };
            }
        }
        let err = gate_workload(&w).expect_err("a blank reason must fail");
        assert!(
            matches!(&err, WorkloadError::BlankNonComparableReason { profile } if profile == "ci"),
            "wrong error variant: {err:?}"
        );
    }

    #[test]
    fn a_missing_or_extra_profile_is_refused() {
        let mut w = fixture();
        w.profiles.retain(|p| p.name != "churn");
        let err = gate_workload(&w).expect_err("a missing profile must fail");
        match &err {
            WorkloadError::ProfileSetMismatch { found, expected } => {
                assert_eq!(
                    found,
                    &vec![
                        "cardinality".to_string(),
                        "ci".to_string(),
                        "history".to_string()
                    ]
                );
                assert_eq!(
                    expected,
                    &vec![
                        "cardinality".to_string(),
                        "churn".to_string(),
                        "ci".to_string(),
                        "history".to_string()
                    ]
                );
            }
            other => panic!("wrong error variant: {other:?}"),
        }
    }

    #[test]
    fn a_sample_grid_that_does_not_close_is_refused() {
        let mut w = fixture();
        w.profiles[0].duration_secs = 1_801;
        let err = gate_workload(&w).expect_err("an inconsistent grid must fail");
        match &err {
            WorkloadError::SampleGridMismatch {
                profile, computed, ..
            } => {
                assert_eq!(profile, "cardinality");
                assert_eq!(*computed, 1_800);
            }
            other => panic!("wrong error variant: {other:?}"),
        }
    }

    #[test]
    fn a_restated_total_that_disagrees_with_its_own_figures_is_refused() {
        let mut w = fixture();
        w.profiles[1].total_samples = 119_999;
        let err = gate_workload(&w).expect_err("a wrong total must fail");
        assert!(
            matches!(&err, WorkloadError::TotalSamplesMismatch { profile, declared, computed }
                if profile == "history" && *declared == 119_999 && *computed == 120_000),
            "wrong error variant: {err:?}"
        );
    }

    #[test]
    fn shares_that_do_not_partition_the_budget_are_refused() {
        let mut w = fixture();
        w.families[0].series_permille = 400;
        let err = gate_workload(&w).expect_err("shares summing to 950 must fail");
        assert!(
            matches!(&err, WorkloadError::SeriesShareNotThousand { total } if *total == 950),
            "wrong error variant: {err:?}"
        );
    }

    #[test]
    fn an_inexact_series_or_instance_budget_is_refused() {
        // 500 * 451 / 1000 is not a whole number of series.
        let mut w = fixture();
        w.profiles[0].active_series = 500;
        w.profiles[0].total_samples = 60_000;
        w.families[0].series_permille = 451;
        w.families[1].series_permille = 299;
        let err = gate_workload(&w).expect_err("an inexact series budget must fail");
        assert!(
            matches!(&err, WorkloadError::InexactSeriesBudget { family, .. } if family == "mb_gauge"),
            "wrong error variant: {err:?}"
        );

        // 150 series over 11-series instances (8 bounds) does not divide.
        let mut w = fixture();
        w.generator.classic_histogram_bounds.push(1.0);
        let err = gate_workload(&w).expect_err("an inexact instance budget must fail");
        assert!(
            matches!(&err, WorkloadError::InexactInstanceBudget { family, series, series_per_instance, .. }
                if family == "mb_classic" && *series == 150 && *series_per_instance == 11),
            "wrong error variant: {err:?}"
        );
    }

    #[test]
    fn a_family_naming_an_undeclared_dimension_is_refused() {
        let mut w = fixture();
        w.families[1].labels.push("method".to_string());
        let err = gate_workload(&w).expect_err("an undeclared dimension must fail");
        assert!(
            matches!(&err, WorkloadError::UnknownDimension { family, label }
                if family == "mb_counter" && label == "method"),
            "wrong error variant: {err:?}"
        );
    }

    #[test]
    fn declaring_the_scaling_label_twice_is_refused() {
        let mut w = fixture();
        w.label_dimensions.push(LabelDimension {
            name: "instance".to_string(),
            values: vec!["i0".to_string()],
        });
        let err = gate_workload(&w).expect_err("a declared scaling label must fail");
        assert!(
            matches!(&err, WorkloadError::ScalingLabelDeclared { label } if label == "instance"),
            "wrong error variant: {err:?}"
        );

        let mut w = fixture();
        w.families[0].labels.push("instance".to_string());
        let err = gate_workload(&w).expect_err("a family listing the scaling label must fail");
        assert!(
            matches!(&err, WorkloadError::ScalingLabelDeclared { .. }),
            "wrong error variant: {err:?}"
        );
    }

    #[test]
    fn an_absent_metric_name_that_collides_is_refused() {
        let mut w = fixture();
        w.generator.absent_metric_name = "mb_classic_sum".to_string();
        let err = gate_workload(&w).expect_err("a colliding sentinel must fail");
        assert!(
            matches!(&err, WorkloadError::AbsentMetricCollides { name } if name == "mb_classic_sum"),
            "wrong error variant: {err:?}"
        );
    }

    #[test]
    fn bad_histogram_bounds_and_zero_native_buckets_are_refused() {
        let mut w = fixture();
        w.generator.classic_histogram_bounds = vec![0.1, 0.05];
        assert!(
            matches!(
                gate_workload(&w),
                Err(WorkloadError::BadHistogramBounds { .. })
            ),
            "descending bounds must fail"
        );

        let mut w = fixture();
        w.generator.classic_histogram_bounds = Vec::new();
        assert!(
            matches!(
                gate_workload(&w),
                Err(WorkloadError::BadHistogramBounds { .. })
            ),
            "empty bounds must fail"
        );

        let mut w = fixture();
        w.generator.native_histogram_buckets = 0;
        assert!(
            matches!(gate_workload(&w), Err(WorkloadError::NoNativeBuckets)),
            "zero native buckets must fail"
        );
    }

    #[test]
    fn churn_arithmetic_is_exact_per_profile() {
        let mut w = fixture();
        // The ADR-0927 churn profile: 50,000 concurrent, 36 h, 20%/h.
        let churn = w
            .profiles
            .iter_mut()
            .find(|p| p.name == "churn")
            .expect("churn profile");
        churn.active_series = 50_000;
        churn.samples_per_series = 8_640;
        churn.duration_secs = 129_600;
        churn.total_samples = 432_000_000;
        churn.churn_basis_points_per_hour = 2_000;
        gate_workload(&w).expect("the real churn figures gate clean");
        let churn = w.profile("churn").expect("churn profile");
        assert_eq!(churn.churn_epochs(), 36);
        assert_eq!(churn.churned_series_per_epoch(), 10_000);
        // 50,000 alive at once, plus a 10,000-series cohort at each of the 35
        // epoch boundaries the 36-hour run crosses.
        assert_eq!(churn.total_series_created(), 400_000);
        // Churn does not change the sample count: exactly `active_series` series
        // are alive at every step.
        assert_eq!(churn.total_samples, 8_640 * 50_000);

        // A profile shorter than one epoch crosses no boundary, so it creates
        // exactly its active set however high its churn rate is.
        let ci = w.profile("ci").expect("ci profile");
        assert_eq!(ci.churn_epochs(), 1);
        assert_eq!(ci.total_series_created(), 1_000);
    }

    #[test]
    fn an_unknown_manifest_key_is_a_deserialization_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("workload.json");
        let mut doc = serde_json::to_value(fixture()).expect("fixture serializes");
        doc.as_object_mut()
            .expect("object")
            .insert("sed".to_string(), serde_json::json!(11));
        std::fs::write(&path, doc.to_string()).expect("write fixture");
        let err = load_workload(&path).expect_err("an unknown key must fail");
        assert!(matches!(&err, WorkloadError::Parse { .. }), "{err:?}");
        assert!(err.to_string().contains("sed"), "{err}");
    }

    #[test]
    fn a_manifest_round_trips_and_an_unknown_version_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("workload.json");
        let w = fixture();
        std::fs::write(
            &path,
            serde_json::to_string_pretty(&w).expect("fixture serializes"),
        )
        .expect("write fixture");
        let back = load_workload(&path).expect("the round-tripped manifest loads and gates");
        assert_eq!(back, w);

        let mut doc = serde_json::to_value(&w).expect("serializes");
        doc.as_object_mut()
            .expect("object")
            .insert("version".to_string(), serde_json::json!(9));
        std::fs::write(&path, doc.to_string()).expect("write fixture");
        let err = load_workload(&path).expect_err("an unknown version must fail");
        assert!(
            matches!(err, WorkloadError::UnsupportedVersion { found: 9, .. }),
            "{err:?}"
        );
    }

    #[test]
    fn a_missing_manifest_is_a_read_error_naming_the_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let missing = dir.path().join("nope.json");
        let err = load_workload(&missing).expect_err("a missing manifest must fail");
        assert!(
            matches!(&err, WorkloadError::Read { path, .. } if path == &missing),
            "{err:?}"
        );
    }
}
