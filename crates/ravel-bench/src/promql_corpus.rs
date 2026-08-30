//! The MetricsBench PromQL query corpus (ADR-0927 decision 5).
//!
//! A versioned set of workload-shaped PromQL queries against the metric
//! families `benchmarks/metrics/workload.json` declares. Each entry carries the
//! constructs it exercises, named exactly as
//! [`ravel_promql_difftest::scoring::REGISTRY`] names them, and the cost class
//! it falls into. The gate reproduces the five ordered checks
//! [`crate::sql_corpus::gate_corpus`] applies to the ClickBench corpus, in the
//! same order, against the PromQL registry instead of the SQL one:
//!
//! 1. all-or-none classification,
//! 2. unique ids,
//! 3. non-empty construct list,
//! 4. non-blank modification reason,
//! 5. every named construct known to the registry.
//!
//! Check 5 differs from the SQL corpus in one deliberate way. The SQL gate
//! requires each construct to be `SupportedAndCovered`; ADR-0927 decision 5
//! requires each construct to be *known to the registry*, because ADR-0927
//! decision 6 keeps queries Ravel refuses or does not implement in the corpus,
//! reported as `unsupported_construct` or `refused` and still counted in the
//! corpus denominator. A supported-only gate would delete exactly the entries
//! ADR-0927's rejected-alternatives section says must stay.
//!
//! This module is data plus its gate. It never executes a query: running the
//! corpus against an engine is the harness's job (ADR-0927 decision 1).
//!
//! [`Modification`] restates [`crate::sql_corpus::Modification`] rather than
//! reusing it: that module is behind the `sql-latency` feature, which pulls
//! datafusion, and this corpus loads in the default build.
//!
//! ## Corpus file format
//!
//! JSON, one document per corpus:
//!
//! ```json
//! {
//!   "version": 1,
//!   "entries": [
//!     {
//!       "id": "mb_fanout_total_rate",
//!       "class": "high_fan_out",
//!       "promql": "sum(rate(metricsbench_requests_total[5m]))",
//!       "eval": "range",
//!       "constructs": ["aggregate expression", "sum", "rate"],
//!       "profiles": ["cardinality", "ci"],
//!       "upstream_id": null,
//!       "modified": {"modified": {"reason": "..."}}
//!     }
//!   ]
//! }
//! ```
//!
//! `eval`, `profiles`, `upstream_id`, and `modified` may be omitted; they
//! default to [`EvalKind::Instant`], every profile, no upstream id, and
//! [`Modification::Verbatim`].

use std::collections::{BTreeSet, HashSet};
use std::path::{Path, PathBuf};

use ravel_promql_difftest::scoring::REGISTRY;
use serde::{Deserialize, Serialize};

/// The corpus format version this module reads and writes. A file declaring any
/// other version is rejected rather than parsed optimistically: the field exists
/// so a future format change is a loud error, not a silently misread document.
pub const CORPUS_FORMAT_VERSION: u32 = 1;

/// Whether a query was rewritten in a way that changes what it computes.
///
/// A disclosure obligation, so it is a typed field rather than a comment:
/// re-pointing a query at Ravel's metric names is *not* a modification,
/// changing the computed result is, and a modified query cannot exist without a
/// stated reason.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Modification {
    /// The query computes what its upstream original computes.
    #[default]
    Verbatim,
    /// The query was rewritten in a way that changes its result.
    Modified {
        /// What the rewrite changed about the computation, and why.
        reason: String,
    },
}

impl Modification {
    /// Whether this query computes something other than its upstream original.
    pub fn is_modified(&self) -> bool {
        matches!(self, Modification::Modified { .. })
    }

    /// The stated reason, or `None` for a verbatim query.
    pub fn reason(&self) -> Option<&str> {
        match self {
            Modification::Verbatim => None,
            Modification::Modified { reason } => Some(reason),
        }
    }
}

/// Which Prometheus HTTP query endpoint an entry is run against.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvalKind {
    /// `/api/v1/query`: one evaluation timestamp.
    #[default]
    Instant,
    /// `/api/v1/query_range`: a start/end/step grid. The step is a property of
    /// the profile being run, not of the query, so it is not carried here.
    Range,
}

/// The physical work a query is expected to do (ADR-0927 decision 5).
///
/// A typed enum, not a bare string and not a comment. [`CorpusEntry`] carries
/// `deny_unknown_fields`, so the two halves of the hole are closed separately:
/// a misspelled class VALUE (`"high_fanout"`) fails this deserializer naming
/// the bad value, and a misspelled class KEY (`"clas"`) fails the entry's
/// deserializer naming the bad field. Without the second, a corpus where every
/// entry carried the same key typo would class nothing and pass the
/// all-or-none check by having nothing left to check.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CostClass {
    /// Answerable from series metadata alone: the selector resolves and no
    /// sample values are read.
    MetadataOnly,
    /// A fully qualified selector matching exactly one series.
    SingleSeries,
    /// A selective label predicate matching a small fraction of the series.
    SelectiveMultiSeries,
    /// An unrestricted selector over a whole metric family.
    HighFanOut,
    /// Reads every sample of the matched series across the profile's whole
    /// generated range.
    FullRange,
    /// Vector matching between two selectors (`on`/`ignoring`, with or without
    /// `group_left`/`group_right`).
    Join,
    /// Reads classic or native histogram data.
    Histogram,
    /// A window or subquery spanning multiple days, so it crosses far more
    /// stored objects than a scrape-scale window.
    LongRange,
}

impl CostClass {
    /// Every cost class, in declaration order. A reader that groups a report by
    /// class iterates this rather than re-listing the variants, so a new class
    /// cannot be silently dropped from a table.
    pub const ALL: &'static [CostClass] = &[
        CostClass::MetadataOnly,
        CostClass::SingleSeries,
        CostClass::SelectiveMultiSeries,
        CostClass::HighFanOut,
        CostClass::FullRange,
        CostClass::Join,
        CostClass::Histogram,
        CostClass::LongRange,
    ];

    /// The slug this class serializes as, for report keys and log lines.
    pub fn slug(self) -> &'static str {
        match self {
            CostClass::MetadataOnly => "metadata_only",
            CostClass::SingleSeries => "single_series",
            CostClass::SelectiveMultiSeries => "selective_multi_series",
            CostClass::HighFanOut => "high_fan_out",
            CostClass::FullRange => "full_range",
            CostClass::Join => "join",
            CostClass::Histogram => "histogram",
            CostClass::LongRange => "long_range",
        }
    }
}

/// One corpus query and everything a reader needs to judge it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CorpusEntry {
    /// A stable short identifier, unique within a corpus. Report rows are keyed
    /// by it, so it must not change when the query is edited in place.
    pub id: String,
    /// The query text, as it will be handed to the engine.
    pub promql: String,
    /// The constructs the query exercises, named exactly as
    /// [`ravel_promql_difftest::scoring::Construct::name`] names them.
    pub constructs: Vec<String>,
    /// The cost class, or `None` for a corpus that predates cost classes. A
    /// corpus that classes *any* entry must class *every* entry, which
    /// [`gate_corpus`] enforces, so an unclassified entry can never silently
    /// read as some default class where the classes are consumed.
    #[serde(default)]
    pub class: Option<CostClass>,
    /// Which endpoint the query runs against.
    #[serde(default)]
    pub eval: EvalKind,
    /// The workload profiles this query belongs to, by
    /// [`crate::metrics_workload::Profile::name`]. Empty means every profile:
    /// most queries are profile-independent, and only the ones that need a
    /// range no shorter profile generates (a 7-day window) restrict themselves.
    #[serde(default)]
    pub profiles: Vec<String>,
    /// The id this query carries in the external suite it came from, so it can
    /// be diffed against its original.
    #[serde(default)]
    pub upstream_id: Option<String>,
    /// Whether the query was rewritten in a way that changes what it computes.
    #[serde(default)]
    pub modified: Modification,
}

impl CorpusEntry {
    /// Whether this entry runs under the profile named `profile`.
    pub fn runs_under(&self, profile: &str) -> bool {
        self.profiles.is_empty() || self.profiles.iter().any(|p| p == profile)
    }
}

/// A parsed corpus document: a format version plus its entries.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CorpusFile {
    /// The format version, which must equal [`CORPUS_FORMAT_VERSION`].
    pub version: u32,
    /// The queries, in the order the harness runs them.
    pub entries: Vec<CorpusEntry>,
}

/// Everything that can go wrong loading or gating a corpus. Every variant names
/// the entry (and where applicable the construct) at fault: the message is the
/// signal a reader acts on, so it is part of the contract.
#[derive(Debug, thiserror::Error)]
pub enum CorpusError {
    /// The corpus file could not be read.
    #[error("promql corpus file {path}: {source}")]
    Read {
        /// The path that was read.
        path: PathBuf,
        /// The underlying I/O failure.
        #[source]
        source: std::io::Error,
    },
    /// The corpus file is not a valid corpus document.
    #[error("promql corpus file {path} is not a valid JSON corpus document: {source}")]
    Parse {
        /// The path that was parsed.
        path: PathBuf,
        /// The underlying `serde_json` failure, which carries the line, column,
        /// and what was expected there.
        #[source]
        source: serde_json::Error,
    },
    /// The document declares a format version this module does not read.
    #[error(
        "promql corpus file {path} declares format version {found}, but this build reads \
         version {expected}"
    )]
    UnsupportedVersion {
        /// The path that was parsed.
        path: PathBuf,
        /// The version the document declared.
        found: u32,
        /// The version this build reads ([`CORPUS_FORMAT_VERSION`]).
        expected: u32,
    },
    /// A corpus that classifies some entries left this one unclassified. A
    /// corpus uses cost classes for every entry or for none: a mix would let an
    /// unclassified entry read as a default class where the classes are
    /// consumed.
    #[error(
        "promql corpus entry `{entry_id}` has no cost class, but other entries in this corpus \
         do; a corpus that classes any entry must class every entry"
    )]
    UnclassifiedEntry {
        /// The id of the unclassified entry.
        entry_id: String,
    },
    /// Two entries share an id, so their report rows would collide.
    #[error("promql corpus entry id `{entry_id}` appears more than once; ids must be unique")]
    DuplicateId {
        /// The duplicated id.
        entry_id: String,
    },
    /// An entry names no constructs at all, so the gate would pass it
    /// vacuously.
    #[error(
        "promql corpus entry `{entry_id}` names no constructs; every entry must name at \
         least one"
    )]
    NoConstructs {
        /// The id of the offending entry.
        entry_id: String,
    },
    /// An entry is flagged modified with an empty reason, which discloses
    /// nothing.
    #[error(
        "promql corpus entry `{entry_id}` is flagged as a modified query with an empty reason; \
         a modified query must state what its rewrite changed about the computation"
    )]
    EmptyModificationReason {
        /// The id of the offending entry.
        entry_id: String,
    },
    /// An entry names a construct the PromQL conformance registry does not
    /// carry. This is the misspelling and drift signal ADR-0927 decision 5
    /// exists to produce, so it names both the construct and the entry.
    #[error(
        "promql corpus entry `{entry_id}` names construct `{construct}`, which is not in \
         ravel_promql_difftest::scoring::REGISTRY: no registry construct carries that name. \
         Either the construct name is misspelled, or it names something outside the scored \
         PromQL surface, which the registry deliberately does not enumerate."
    )]
    UnknownConstruct {
        /// The id of the entry that named it.
        entry_id: String,
        /// The construct name the registry does not carry.
        construct: String,
    },
}

/// Every construct name the PromQL conformance registry carries, in any state.
///
/// ADR-0927 decision 5's check is membership in the registry, not membership in
/// its supported subset: a corpus entry naming an intentionally-rejected or
/// unclassified construct is exactly the entry the report records as
/// `unsupported_construct` or `refused`, and it stays in the corpus
/// denominator.
pub fn known_construct_names() -> BTreeSet<&'static str> {
    REGISTRY.iter().map(|c| c.name).collect()
}

/// Apply the five ordered checks to a corpus, identically for the checked-in
/// artifact and for an external file.
pub fn gate_corpus(entries: &[CorpusEntry]) -> Result<(), CorpusError> {
    let known = known_construct_names();
    // A corpus either classes every entry or none. If it classes any, an
    // unclassified entry is a hard error here where the classes are consumed,
    // not a silent fall-through to a default class.
    let uses_cost_classes = entries.iter().any(|e| e.class.is_some());
    let mut seen_ids: HashSet<&str> = HashSet::new();
    for entry in entries {
        if uses_cost_classes && entry.class.is_none() {
            return Err(CorpusError::UnclassifiedEntry {
                entry_id: entry.id.clone(),
            });
        }
        if !seen_ids.insert(entry.id.as_str()) {
            return Err(CorpusError::DuplicateId {
                entry_id: entry.id.clone(),
            });
        }
        if entry.constructs.is_empty() {
            return Err(CorpusError::NoConstructs {
                entry_id: entry.id.clone(),
            });
        }
        if let Modification::Modified { reason } = &entry.modified
            && reason.trim().is_empty()
        {
            return Err(CorpusError::EmptyModificationReason {
                entry_id: entry.id.clone(),
            });
        }
        for construct in &entry.constructs {
            if !known.contains(construct.as_str()) {
                return Err(CorpusError::UnknownConstruct {
                    entry_id: entry.id.clone(),
                    construct: construct.clone(),
                });
            }
        }
    }
    Ok(())
}

/// Read a corpus document, parse it, and gate it. A malformed file is a typed
/// error naming what failed to parse; it is never a panic and never a silently
/// skipped entry.
pub fn load_corpus(path: impl AsRef<Path>) -> Result<Vec<CorpusEntry>, CorpusError> {
    let path = path.as_ref();
    let text = std::fs::read_to_string(path).map_err(|source| CorpusError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    let file: CorpusFile = serde_json::from_str(&text).map_err(|source| CorpusError::Parse {
        path: path.to_path_buf(),
        source,
    })?;
    if file.version != CORPUS_FORMAT_VERSION {
        return Err(CorpusError::UnsupportedVersion {
            path: path.to_path_buf(),
            found: file.version,
            expected: CORPUS_FORMAT_VERSION,
        });
    }
    gate_corpus(&file.entries)?;
    Ok(file.entries)
}

/// How many corpus entries fall in each cost class, over every class rather
/// than only the ones present, so an empty class reads as `0` instead of being
/// absent from the table.
pub fn class_counts(entries: &[CorpusEntry]) -> Vec<(CostClass, usize)> {
    CostClass::ALL
        .iter()
        .map(|class| {
            let n = entries.iter().filter(|e| e.class == Some(*class)).count();
            (*class, n)
        })
        .collect()
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    fn write_corpus(dir: &tempfile::TempDir, name: &str, body: &str) -> PathBuf {
        let path = dir.path().join(name);
        std::fs::write(&path, body).expect("write corpus fixture");
        path
    }

    fn entry(id: &str, class: Option<CostClass>) -> CorpusEntry {
        CorpusEntry {
            id: id.to_string(),
            promql: "sum(rate(metricsbench_requests_total[5m]))".to_string(),
            constructs: vec!["sum".to_string(), "rate".to_string()],
            class,
            eval: EvalKind::Instant,
            profiles: Vec::new(),
            upstream_id: None,
            modified: Modification::Verbatim,
        }
    }

    /// ACCEPTANCE TEST (issue #933): a corpus that classes some entries and
    /// leaves one unclassified is refused, naming the unclassified entry. This
    /// is check 1 of the five, and the check that stops a partially classed
    /// corpus from reading as "no classes here" downstream.
    #[test]
    fn gate_rejects_an_unclassified_corpus() {
        let classed = entry("classed", Some(CostClass::HighFanOut));
        let unclassed = entry("unclassed", None);
        let err = gate_corpus(&[classed.clone(), unclassed])
            .expect_err("a mix of classed and unclassed entries must fail");
        assert!(
            matches!(&err, CorpusError::UnclassifiedEntry { entry_id } if entry_id == "unclassed"),
            "wrong error variant: {err:?}"
        );
        assert!(
            err.to_string().contains("unclassed"),
            "the error must name the unclassified entry: {err}"
        );
        // The same corpus with every entry classed gates clean, so the check
        // rejects the mix rather than rejecting classes.
        gate_corpus(&[classed, entry("also_classed", Some(CostClass::Join))])
            .expect("a fully classed corpus gates clean");
        // And a corpus with no classes at all gates clean too: the all-or-none
        // rule triggers only once one entry carries a class.
        gate_corpus(&[entry("a", None), entry("b", None)])
            .expect("a fully unclassed corpus gates clean");
    }

    #[test]
    fn gate_rejects_a_duplicate_id_naming_it() {
        let base = entry("dup", Some(CostClass::SingleSeries));
        let err = gate_corpus(&[base.clone(), base]).expect_err("duplicate ids must fail");
        assert!(
            matches!(&err, CorpusError::DuplicateId { entry_id } if entry_id == "dup"),
            "wrong error variant: {err:?}"
        );
        assert!(err.to_string().contains("must be unique"), "{err}");
    }

    #[test]
    fn gate_rejects_an_entry_naming_no_constructs() {
        let mut bare = entry("bare", Some(CostClass::MetadataOnly));
        bare.constructs.clear();
        let err = gate_corpus(&[bare]).expect_err("an entry naming no constructs must fail");
        assert!(
            matches!(&err, CorpusError::NoConstructs { entry_id } if entry_id == "bare"),
            "wrong error variant: {err:?}"
        );
    }

    #[test]
    fn gate_rejects_a_modified_entry_with_a_blank_reason() {
        let mut modified = entry("blank_reason", Some(CostClass::FullRange));
        modified.modified = Modification::Modified {
            reason: "   \t ".to_string(),
        };
        let err = gate_corpus(&[modified]).expect_err("a blank modification reason must fail");
        assert!(
            matches!(&err, CorpusError::EmptyModificationReason { entry_id }
                if entry_id == "blank_reason"),
            "wrong error variant: {err:?}"
        );

        // A stated reason passes, so the check rejects the blank rather than
        // the flag.
        let mut disclosed = entry("disclosed", Some(CostClass::FullRange));
        disclosed.modified = Modification::Modified {
            reason: "widened from 5m to 1h: the ci profile generates 30 minutes of data"
                .to_string(),
        };
        gate_corpus(&[disclosed]).expect("a modified entry with a reason gates clean");
    }

    #[test]
    fn gate_rejects_a_construct_the_registry_does_not_carry() {
        let mut bad = entry("unknown_construct", Some(CostClass::HighFanOut));
        // `limitk` is a real Prometheus aggregator, deliberately outside the
        // scored surface (it is experimental), so the registry does not carry
        // it. That makes it the exact shape this check catches: a plausible
        // name that is not a registry construct.
        bad.constructs = vec!["sum".to_string(), "limitk".to_string()];
        let err = gate_corpus(&[bad]).expect_err("an unknown construct must fail");
        assert!(
            matches!(&err, CorpusError::UnknownConstruct { entry_id, construct }
                if entry_id == "unknown_construct" && construct == "limitk"),
            "wrong error variant: {err:?}"
        );
        let message = err.to_string();
        assert!(
            message.contains("limitk"),
            "error must name the construct: {message}"
        );
        assert!(
            message.contains("unknown_construct"),
            "error must name the entry id: {message}"
        );
        assert!(
            message.contains("REGISTRY"),
            "error must say which registry was consulted: {message}"
        );
    }

    #[test]
    fn the_registry_check_admits_a_construct_ravel_refuses() {
        // ADR-0927 decision 6 keeps queries Ravel does not implement in the
        // corpus, reported `unsupported_construct` and still counted in the
        // corpus denominator. `histogram_stddev` is registered
        // IntentionallyRejected, so a supported-only gate (the SQL corpus'
        // rule) would delete this entry; the registry-membership gate admits
        // it.
        let mut refused = entry("native_hist_stddev", Some(CostClass::Histogram));
        refused.promql = "histogram_stddev(metricsbench_latency_native)".to_string();
        refused.constructs = vec!["histogram_stddev".to_string(), "function call".to_string()];
        gate_corpus(&[refused])
            .expect("a registered but unsupported construct stays in the corpus");
    }

    #[test]
    fn a_misspelled_class_key_is_a_deserialization_error_not_an_unclassified_entry() {
        // The typed enum catches a bad class VALUE. It cannot catch a bad class
        // KEY: without `deny_unknown_fields`, `"clas"` is dropped and the entry
        // reads as unclassified, and a corpus where every entry carries the
        // same typo classes nothing and sails through check 1 by seeing "none".
        let typo = serde_json::json!({
            "id": "a",
            "promql": "up",
            "constructs": ["vector selector"],
            "clas": "high_fan_out",
        });
        let err = serde_json::from_value::<CorpusEntry>(typo)
            .expect_err("a misspelled class key must fail to deserialize");
        assert!(
            err.to_string().contains("clas"),
            "the error must name the offending field, got: {err}"
        );

        // The bad VALUE, caught by the typed enum rather than by the field set.
        let bad_value = serde_json::json!({
            "id": "a",
            "promql": "up",
            "constructs": ["vector selector"],
            "class": "high_fanout",
        });
        let err = serde_json::from_value::<CorpusEntry>(bad_value)
            .expect_err("a misspelled class value must fail to deserialize");
        assert!(
            err.to_string().contains("high_fanout"),
            "the error must name the offending value, got: {err}"
        );

        // The correct spelling parses, so the guards reject typos rather than
        // rejecting the field.
        let ok = serde_json::json!({
            "id": "a",
            "promql": "up",
            "constructs": ["vector selector"],
            "class": "high_fan_out",
        });
        let parsed: CorpusEntry = serde_json::from_value(ok).expect("the correct spelling parses");
        assert_eq!(parsed.class, Some(CostClass::HighFanOut));
        assert_eq!(parsed.eval, EvalKind::Instant);
        assert_eq!(parsed.profiles, Vec::<String>::new());
        assert!(!parsed.modified.is_modified());
    }

    #[test]
    fn an_unknown_top_level_key_is_a_deserialization_error() {
        // The same hole one level up: a document carrying `"entires"` would
        // otherwise parse as a corpus with zero entries, and a zero-entry
        // corpus passes all five checks vacuously.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = write_corpus(&dir, "typo.json", r#"{"version": 1, "entires": []}"#);
        let err = load_corpus(&path).expect_err("a misspelled `entries` key must fail");
        assert!(matches!(&err, CorpusError::Parse { .. }), "{err:?}");
        assert!(err.to_string().contains("entires"), "{err}");
    }

    #[test]
    fn a_malformed_corpus_is_a_typed_parse_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = write_corpus(
            &dir,
            "corpus.json",
            "{\"version\": 1, \"entries\": [{\"id\":",
        );
        let err = load_corpus(&path).expect_err("malformed JSON must fail");
        match &err {
            CorpusError::Parse { path: p, source } => {
                assert_eq!(p, &path);
                assert!(
                    source.to_string().contains("line"),
                    "parse error should locate the failure: {source}"
                );
            }
            other => panic!("wrong error variant: {other:?}"),
        }
        assert!(err.to_string().contains("not a valid JSON corpus document"));
    }

    #[test]
    fn a_missing_corpus_is_a_read_error_naming_the_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let missing = dir.path().join("absent.json");
        let err = load_corpus(&missing).expect_err("a missing file must fail");
        assert!(
            matches!(&err, CorpusError::Read { path, .. } if path == &missing),
            "{err:?}"
        );
        assert!(err.to_string().contains("absent.json"), "{err}");
    }

    #[test]
    fn an_unknown_format_version_is_rejected() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = write_corpus(&dir, "corpus.json", r#"{"version": 7, "entries": []}"#);
        let err = load_corpus(&path).expect_err("an unknown version must fail");
        assert!(
            matches!(
                err,
                CorpusError::UnsupportedVersion {
                    found: 7,
                    expected: 1,
                    ..
                }
            ),
            "{err:?}"
        );
    }

    #[test]
    fn the_loader_gates_what_it_returns() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = write_corpus(
            &dir,
            "corpus.json",
            r#"{
              "version": 1,
              "entries": [
                {
                  "id": "mb_a",
                  "class": "single_series",
                  "promql": "metricsbench_gauge_cpu_percent{instance=\"metricsbench-instance-0\"}",
                  "eval": "instant",
                  "constructs": ["vector selector", "label matcher ="]
                },
                {
                  "id": "mb_b",
                  "class": "long_range",
                  "promql": "avg_over_time(metricsbench_gauge_cpu_percent[7d])",
                  "eval": "range",
                  "profiles": ["history"],
                  "constructs": ["avg_over_time", "matrix selector"],
                  "modified": {"modified": {"reason": "window widened to 7d; the 30m ci profile has no 7 day range"}}
                }
              ]
            }"#,
        );
        let entries = load_corpus(&path).expect("the fixture corpus loads and gates");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].eval, EvalKind::Instant);
        assert_eq!(entries[1].eval, EvalKind::Range);
        assert_eq!(entries[1].profiles, vec!["history".to_string()]);
        assert!(entries[1].runs_under("history"));
        assert!(!entries[1].runs_under("ci"));
        assert!(
            entries[0].runs_under("ci"),
            "an empty profile list is every profile"
        );
        assert!(
            entries[1]
                .modified
                .reason()
                .expect("modified entry states a reason")
                .contains("7d")
        );

        // The loader really gated what it returned: the identical corpus with
        // one construct renamed to a name the registry does not carry is
        // refused. Without this pair, a loader that skipped the gate would
        // still pass this test.
        gate_corpus(&entries).expect("loaded entries gate clean");
        let mut tampered = entries.clone();
        tampered[0].constructs[0] = "vector selektor".to_string();
        assert!(
            matches!(
                gate_corpus(&tampered),
                Err(CorpusError::UnknownConstruct { .. })
            ),
            "the gate must refuse an unknown construct"
        );
    }

    #[test]
    fn class_counts_reports_every_class_including_the_empty_ones() {
        let entries = vec![
            entry("a", Some(CostClass::Join)),
            entry("b", Some(CostClass::Join)),
            entry("c", Some(CostClass::MetadataOnly)),
        ];
        let counts = class_counts(&entries);
        assert_eq!(
            counts.len(),
            8,
            "all eight ADR-0927 cost classes must appear in the table"
        );
        let lookup = |class: CostClass| -> usize {
            counts
                .iter()
                .find(|(c, _)| *c == class)
                .map(|(_, n)| *n)
                .unwrap_or_else(|| panic!("class {} missing from the table", class.slug()))
        };
        assert_eq!(lookup(CostClass::Join), 2);
        assert_eq!(lookup(CostClass::MetadataOnly), 1);
        assert_eq!(lookup(CostClass::HighFanOut), 0);
        assert_eq!(lookup(CostClass::LongRange), 0);
        assert_eq!(counts.iter().map(|(_, n)| *n).sum::<usize>(), 3);
    }

    #[test]
    fn every_cost_class_slug_round_trips_through_serde() {
        // The slug a report keys by and the slug the artifact stores must be
        // the same string, or a class renamed in one place reads as a different
        // class in the other.
        for class in CostClass::ALL {
            let json = serde_json::to_string(class).expect("class serializes");
            assert_eq!(
                json,
                format!("\"{}\"", class.slug()),
                "slug and serde name disagree for {class:?}"
            );
            let back: CostClass = serde_json::from_str(&json).expect("class round-trips");
            assert_eq!(back, *class);
        }
    }
}
