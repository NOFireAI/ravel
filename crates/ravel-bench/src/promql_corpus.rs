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
//! 5. every named construct known to the registry,
//! 6. non-blank reason on every intended-empty entry,
//! 7. every ULP tolerance non-zero and on a modified entry.
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
//! [`gate_corpus_against_workload`] adds the checks that need the workload
//! manifest (ADR-0927, 2026-10-26 amendment, decision E): every selector of an
//! entry not marked intended-empty matches at least one generated series,
//! every `group_left`/`group_right` key is unique on the one side, and every
//! range entry's evaluation window plus its longest lookback fits inside each
//! profile it runs under. It reads the query text, not a parse tree; its
//! limits are stated on each check.
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
//!   "version": 3,
//!   "entries": [
//!     {
//!       "id": "mb_fanout_total_rate",
//!       "class": "high_fan_out",
//!       "promql": "sum(rate(metricsbench_requests_total[5m]))",
//!       "eval": "range",
//!       "constructs": ["aggregate expression", "sum", "rate"],
//!       "profiles": ["cardinality", "ci"],
//!       "upstream_id": null,
//!       "modified": {"modified": {"reason": "..."}},
//!       "expected": {"intended_empty": false},
//!       "tolerance_ulps": 1048576
//!     }
//!   ]
//! }
//! ```
//!
//! `eval`, `profiles`, `upstream_id`, `modified`, `expected` and
//! `tolerance_ulps` may be omitted; they default to [`EvalKind::Instant`],
//! every profile, no upstream id, [`Modification::Verbatim`], an answer that
//! must not be empty, and bit-exact comparison.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::{Path, PathBuf};

use ravel_promql_difftest::scoring::REGISTRY;
use serde::{Deserialize, Serialize};

use crate::metrics_gen::{Generator, LabelSet};
use crate::metrics_workload::{FamilyKind, Profile, WorkloadFile};

/// The corpus format version this module reads and writes. A file declaring any
/// other version is rejected rather than parsed optimistically: the field exists
/// so a future format change is a loud error, not a silently misread document.
pub const CORPUS_FORMAT_VERSION: u32 = 3;

/// Whether a query was rewritten in a way that changes what it computes.
///
/// A disclosure obligation, so it is a typed field rather than a comment:
/// re-pointing a query at Ravel's metric names is *not* a modification,
/// changing the computed result is, and a modified query cannot exist without a
/// stated reason.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
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

/// What an entry's oracle answer is expected to be.
///
/// An empty oracle answer is an invalid fixture unless the entry says it is
/// intended (ADR-0927, 2026-10-26 amendment, decision E), and the saying needs
/// a reason, which [`gate_corpus`] check 6 requires.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Expected {
    /// The answer is expected to be empty on the generated data.
    #[serde(default)]
    pub intended_empty: bool,
    /// Why, when `intended_empty` is set.
    #[serde(default)]
    pub reason: Option<String>,
}

/// Which Prometheus HTTP query endpoint an entry is run against.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvalKind {
    /// `/api/v1/query`: one evaluation timestamp.
    #[default]
    Instant,
    /// `/api/v1/query_range`: a start/end/step grid. The grid is a property of
    /// the profile being run, not of the query, so it is not carried here: it
    /// is [`crate::metrics_workload::Profile::range_eval`].
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
    /// What the oracle answer is expected to be.
    #[serde(default)]
    pub expected: Expected,
    /// The ADR-0025 ULP tolerance the comparator applies to this entry's
    /// sample values, or `None` for bit-exact comparison. [`gate_corpus`]
    /// check 7 refuses `Some(0)`, which is bit-exact spelled a second way,
    /// and a tolerance on an entry whose `modified` field states no reason.
    #[serde(default)]
    pub tolerance_ulps: Option<u32>,
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
    /// An entry is marked intended-empty without a reason.
    #[error(
        "promql corpus entry `{entry_id}` is marked intended_empty with no reason; an empty \
         answer is an invalid fixture unless the entry states why it is intended"
    )]
    EmptyIntendedEmptyReason {
        /// The id of the offending entry.
        entry_id: String,
    },
    /// An entry declares a ULP tolerance of zero. Bit-exact comparison is
    /// spelled by leaving the field out, so there is one spelling to read.
    #[error(
        "promql corpus entry `{entry_id}` declares tolerance_ulps 0; bit-exact comparison is \
         spelled by omitting tolerance_ulps"
    )]
    ZeroToleranceUlps {
        /// The id of the offending entry.
        entry_id: String,
    },
    /// An entry declares a ULP tolerance without a `modified` reason, so the
    /// loosened comparison would be undisclosed.
    #[error(
        "promql corpus entry `{entry_id}` declares tolerance_ulps {tolerance_ulps} but is not \
         flagged modified; a tolerance must be disclosed with its basis in the modified reason"
    )]
    UndisclosedTolerance {
        /// The id of the offending entry.
        entry_id: String,
        /// The tolerance it declares.
        tolerance_ulps: u32,
    },
    /// The textual reader could not take an entry apart. The gate refuses
    /// rather than skipping the entry, so a query outside what it reads cannot
    /// pass unchecked.
    #[error("promql corpus entry `{entry_id}` cannot be checked against the workload: {detail}")]
    Unreadable {
        /// The id of the offending entry.
        entry_id: String,
        /// What the reader could not handle.
        detail: String,
    },
    /// A selector of an entry not marked intended-empty matches no series the
    /// workload generates under a profile the entry runs under.
    #[error(
        "promql corpus entry `{entry_id}` selects `{selector}`, which matches no series profile \
         `{profile}` generates; an entry whose answer is empty must be marked intended_empty \
         with a reason"
    )]
    SelectsNothing {
        /// The id of the offending entry.
        entry_id: String,
        /// The profile under which nothing matches.
        profile: String,
        /// The selector, as written.
        selector: String,
    },
    /// A one-to-many join whose one side (the right side of a `group_left`
    /// match, the left side of a `group_right` one) holds two series for one
    /// `on (...)` key, so every engine refuses the match.
    #[error(
        "promql corpus entry `{entry_id}` joins with {modifier}, but under profile `{profile}` \
         its one side `{selector}` holds {series} series for the on key {key}; the one side of \
         a {modifier} match must be unique per key"
    )]
    JoinKeyNotUnique {
        /// The id of the offending entry.
        entry_id: String,
        /// The profile under which the key repeats.
        profile: String,
        /// `group_left` or `group_right`.
        modifier: &'static str,
        /// The one-side selector, as written.
        selector: String,
        /// The repeated key, as `label="value"` pairs in `on (...)` order.
        key: String,
        /// How many one-side series carried it when the check stopped: the
        /// check stops at the first key it sees twice, so this is 2 unless
        /// that changes.
        series: usize,
    },
    /// A range entry's evaluation window plus its longest lookback reaches back
    /// past the first sample of a profile it runs under.
    #[error(
        "promql corpus entry `{entry_id}` needs a {window_secs}s range evaluation window plus a \
         {lookback_secs}s lookback under profile `{profile}`, whose stream spans only \
         {span_secs}s from its first sample to its newest"
    )]
    WindowExceedsProfile {
        /// The id of the offending entry.
        entry_id: String,
        /// The profile it does not fit.
        profile: String,
        /// The profile's `range_eval` start offset.
        window_secs: u64,
        /// The entry's longest lookback.
        lookback_secs: u64,
        /// The profile's stream span, first sample to newest
        /// ([`crate::metrics_workload::Profile::stream_span_secs`]).
        span_secs: u64,
    },
    /// An entry restricts itself to a profile the manifest does not declare.
    #[error("promql corpus entry `{entry_id}` names profile `{profile}`, which is not declared")]
    UnknownProfile {
        /// The id of the offending entry.
        entry_id: String,
        /// The undeclared profile.
        profile: String,
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

/// Apply the seven ordered checks to a corpus, identically for the checked-in
/// artifact and for an external file.
pub fn gate_corpus(entries: &[CorpusEntry]) -> Result<(), CorpusError> {
    let known = known_construct_names();

    // Each check runs across the WHOLE corpus before the next begins, in the
    // order the module contract states. Sharing one loop lets a later check on
    // an earlier entry win over an earlier check on a later entry (a duplicate
    // id at entry 1 reported ahead of an unclassified entry at entry 5),
    // contradicting the stated precedence.

    // 1. All-or-none classification. A corpus either classes every entry or
    //    none; a mix would let an unclassified entry read as a default class
    //    where the classes are consumed.
    let uses_cost_classes = entries.iter().any(|e| e.class.is_some());
    if uses_cost_classes {
        for entry in entries {
            if entry.class.is_none() {
                return Err(CorpusError::UnclassifiedEntry {
                    entry_id: entry.id.clone(),
                });
            }
        }
    }

    // 2. Unique ids.
    let mut seen_ids: HashSet<&str> = HashSet::new();
    for entry in entries {
        if !seen_ids.insert(entry.id.as_str()) {
            return Err(CorpusError::DuplicateId {
                entry_id: entry.id.clone(),
            });
        }
    }

    // 3. Non-empty construct list.
    for entry in entries {
        if entry.constructs.is_empty() {
            return Err(CorpusError::NoConstructs {
                entry_id: entry.id.clone(),
            });
        }
    }

    // 4. Non-blank modification reason.
    for entry in entries {
        if let Modification::Modified { reason } = &entry.modified
            && reason.trim().is_empty()
        {
            return Err(CorpusError::EmptyModificationReason {
                entry_id: entry.id.clone(),
            });
        }
    }

    // 5. Every named construct known to the registry.
    for entry in entries {
        for construct in &entry.constructs {
            if !known.contains(construct.as_str()) {
                return Err(CorpusError::UnknownConstruct {
                    entry_id: entry.id.clone(),
                    construct: construct.clone(),
                });
            }
        }
    }

    // 6. Non-blank reason on every intended-empty entry.
    for entry in entries {
        if entry.expected.intended_empty
            && entry
                .expected
                .reason
                .as_deref()
                .is_none_or(|r| r.trim().is_empty())
        {
            return Err(CorpusError::EmptyIntendedEmptyReason {
                entry_id: entry.id.clone(),
            });
        }
    }

    // 7. A ULP tolerance is non-zero and disclosed. Check 4 already refused a
    //    blank reason, so `is_modified` here means a stated one.
    for entry in entries {
        if let Some(tolerance_ulps) = entry.tolerance_ulps {
            if tolerance_ulps == 0 {
                return Err(CorpusError::ZeroToleranceUlps {
                    entry_id: entry.id.clone(),
                });
            }
            if !entry.modified.is_modified() {
                return Err(CorpusError::UndisclosedTolerance {
                    entry_id: entry.id.clone(),
                    tolerance_ulps,
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

/// How a label matcher compares a value.
#[derive(Clone, Debug, PartialEq, Eq)]
enum MatchOp {
    Eq,
    Ne,
    Re,
    Nre,
}

/// One alternative of a regex matcher the reader understands: a literal, or a
/// literal followed by `.*`.
#[derive(Clone, Debug, PartialEq, Eq)]
struct RegexAlt {
    literal: String,
    any_suffix: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Matcher {
    label: String,
    op: MatchOp,
    value: String,
    /// The value read as a regex, for `=~` and `!~`.
    alternatives: Vec<RegexAlt>,
}

impl Matcher {
    fn matches(&self, labels: &[(String, String)]) -> bool {
        let value = labels
            .iter()
            .find(|(k, _)| *k == self.label)
            .map(|(_, v)| v.as_str())
            .unwrap_or("");
        let regex_hit = || {
            self.alternatives.iter().any(|alt| {
                if alt.any_suffix {
                    value.starts_with(&alt.literal)
                } else {
                    value == alt.literal
                }
            })
        };
        match self.op {
            MatchOp::Eq => value == self.value,
            MatchOp::Ne => value != self.value,
            MatchOp::Re => regex_hit(),
            MatchOp::Nre => !regex_hit(),
        }
    }
}

/// A vector selector as the textual reader finds it in a query.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Selector {
    metric: String,
    matchers: Vec<Matcher>,
    /// The selector as written, metric name through closing brace.
    text: String,
    /// The function whose sole argument this selector is, when it is written
    /// directly inside `name(`.
    argument_of: Option<String>,
}

const MODIFIER_KEYWORDS: &[&str] = &[
    "by",
    "without",
    "on",
    "ignoring",
    "group_left",
    "group_right",
];
const OPERATOR_KEYWORDS: &[&str] = &["and", "or", "unless", "bool", "atan2"];
/// Aggregation operators, which may be followed by `by`/`without` rather than
/// directly by `(`.
const AGGREGATORS: &[&str] = &[
    "sum",
    "avg",
    "min",
    "max",
    "count",
    "group",
    "stddev",
    "stdvar",
    "topk",
    "bottomk",
    "quantile",
    "count_values",
    "limitk",
    "limit_ratio",
];
const REGEX_META: &[char] = &[
    '.', '*', '+', '?', '(', ')', '[', ']', '{', '}', '^', '$', '\\', '|',
];

fn is_ident_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_' || c == ':'
}

fn is_ident_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == ':'
}

fn skip_ws(chars: &[char], mut i: usize) -> usize {
    while i < chars.len() && chars[i].is_whitespace() {
        i += 1;
    }
    i
}

/// The index just past the bracket that closes the one at `open`.
fn close_of(chars: &[char], open: usize) -> Result<usize, String> {
    let (o, c) = match chars[open] {
        '(' => ('(', ')'),
        '[' => ('[', ']'),
        '{' => ('{', '}'),
        other => return Err(format!("internal: `{other}` is not an opening bracket")),
    };
    let mut depth = 0usize;
    let mut i = open;
    while i < chars.len() {
        if chars[i] == '"' {
            i = skip_string(chars, i)?;
            continue;
        }
        if chars[i] == o {
            depth += 1;
        } else if chars[i] == c {
            depth -= 1;
            if depth == 0 {
                return Ok(i + 1);
            }
        }
        i += 1;
    }
    Err(format!("unbalanced `{o}`"))
}

/// The index just past the double-quoted string starting at `open`.
fn skip_string(chars: &[char], open: usize) -> Result<usize, String> {
    let mut i = open + 1;
    while i < chars.len() {
        match chars[i] {
            '\\' => i += 2,
            '"' => return Ok(i + 1),
            _ => i += 1,
        }
    }
    Err("unterminated string".to_string())
}

fn read_regex(pattern: &str) -> Result<Vec<RegexAlt>, String> {
    pattern
        .split('|')
        .map(|alt| {
            let (literal, any_suffix) = match alt.strip_suffix(".*") {
                Some(prefix) => (prefix, true),
                None => (alt, false),
            };
            if literal.contains(REGEX_META) {
                return Err(format!(
                    "regex `{pattern}` is outside what the reader handles (literal \
                     alternatives, each optionally followed by `.*`)"
                ));
            }
            Ok(RegexAlt {
                literal: literal.to_string(),
                any_suffix,
            })
        })
        .collect()
}

/// The matchers in the braces opening at `open`, and the index past them.
fn read_matchers(chars: &[char], open: usize) -> Result<(Vec<Matcher>, usize), String> {
    let end = close_of(chars, open)?;
    let mut out = Vec::new();
    let mut i = skip_ws(chars, open + 1);
    while i < end - 1 {
        let start = i;
        while i < end && is_ident_char(chars[i]) {
            i += 1;
        }
        let label: String = chars[start..i].iter().collect();
        if label.is_empty() {
            return Err("a matcher with no label name".to_string());
        }
        i = skip_ws(chars, i);
        let two: String = chars[i..(i + 2).min(end)].iter().collect();
        let (op, width) = match two.as_str() {
            "=~" => (MatchOp::Re, 2),
            "!~" => (MatchOp::Nre, 2),
            "!=" => (MatchOp::Ne, 2),
            _ if chars[i] == '=' => (MatchOp::Eq, 1),
            _ => return Err(format!("matcher on `{label}` has no operator")),
        };
        i = skip_ws(chars, i + width);
        if chars.get(i) != Some(&'"') {
            return Err(format!("matcher on `{label}` has no double-quoted value"));
        }
        let close = skip_string(chars, i)?;
        let raw: String = chars[i + 1..close - 1].iter().collect();
        let value = raw.replace("\\\"", "\"").replace("\\\\", "\\");
        let alternatives = match op {
            MatchOp::Re | MatchOp::Nre => read_regex(&value)?,
            MatchOp::Eq | MatchOp::Ne => Vec::new(),
        };
        if label == "__name__" {
            return Err("a `__name__` matcher".to_string());
        }
        out.push(Matcher {
            label,
            op,
            value,
            alternatives,
        });
        i = skip_ws(chars, close);
        if chars.get(i) == Some(&',') {
            i = skip_ws(chars, i + 1);
        }
    }
    Ok((out, end))
}

/// The identifier ending just before `end`, skipping whitespace.
fn ident_before(chars: &[char], end: usize) -> Option<String> {
    let mut i = end;
    while i > 0 && chars[i - 1].is_whitespace() {
        i -= 1;
    }
    let stop = i;
    while i > 0 && is_ident_char(chars[i - 1]) {
        i -= 1;
    }
    (i < stop).then(|| chars[i..stop].iter().collect())
}

/// Every vector selector in `promql`.
///
/// A textual scan, not a PromQL parse. An identifier is a metric name unless it
/// is a keyword or an aggregation operator, is followed by `(` (a function),
/// or sits inside a
/// `by`/`without`/`on`/`ignoring`/`group_*` label list, a `[...]` range, or a
/// matcher block. It refuses, rather than misreads, what it does not handle:
/// a selector with no metric name, a `__name__` matcher, the `offset` and `@`
/// modifiers, and any regex other than literal alternatives with an optional
/// trailing `.*`.
fn read_selectors(promql: &str) -> Result<Vec<Selector>, String> {
    let chars: Vec<char> = promql.chars().collect();
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < chars.len() {
        let c = chars[i];
        if c == '"' {
            i = skip_string(&chars, i)?;
        } else if c == '[' {
            i = close_of(&chars, i)?;
        } else if c == '{' {
            return Err("a selector with no metric name".to_string());
        } else if c == '@' {
            return Err("the `@` modifier".to_string());
        } else if c.is_ascii_digit()
            || (c == '.' && chars.get(i + 1).is_some_and(char::is_ascii_digit))
        {
            while i < chars.len() && (is_ident_char(chars[i]) || chars[i] == '.') {
                i += 1;
            }
        } else if is_ident_start(c) {
            let start = i;
            while i < chars.len() && is_ident_char(chars[i]) {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();
            let next = skip_ws(&chars, i);
            if word == "offset" {
                return Err("the `offset` modifier".to_string());
            }
            if MODIFIER_KEYWORDS.contains(&word.as_str()) {
                if chars.get(next) == Some(&'(') {
                    i = close_of(&chars, next)?;
                }
                continue;
            }
            if OPERATOR_KEYWORDS.contains(&word.as_str())
                || AGGREGATORS.contains(&word.as_str())
                || word.eq_ignore_ascii_case("inf")
                || word.eq_ignore_ascii_case("nan")
                || chars.get(next) == Some(&'(')
            {
                continue;
            }
            let (matchers, end) = if chars.get(next) == Some(&'{') {
                read_matchers(&chars, next)?
            } else {
                (Vec::new(), i)
            };
            let mut before = start;
            while before > 0 && chars[before - 1].is_whitespace() {
                before -= 1;
            }
            let argument_of = (before > 0 && chars[before - 1] == '(')
                .then(|| ident_before(&chars, before - 1))
                .flatten();
            out.push(Selector {
                metric: word,
                matchers,
                text: chars[start..end].iter().collect(),
                argument_of,
            });
            i = end;
        } else {
            i += 1;
        }
    }
    Ok(out)
}

/// A Prometheus duration literal (`5m`, `1d`, `1h30m`) in seconds.
fn parse_duration_secs(text: &str) -> Result<u64, String> {
    let text = text.trim();
    let mut total = 0u64;
    let mut rest = text;
    if rest.is_empty() {
        return Err("an empty duration".to_string());
    }
    while !rest.is_empty() {
        let digits = rest.chars().take_while(char::is_ascii_digit).count();
        if digits == 0 {
            return Err(format!("duration `{text}` is not a duration literal"));
        }
        let n: u64 = rest[..digits]
            .parse()
            .map_err(|_| format!("duration `{text}` overflows"))?;
        rest = &rest[digits..];
        let (secs, width) = if rest.starts_with("ms") {
            return Err(format!("duration `{text}` has a millisecond part"));
        } else {
            match rest.chars().next() {
                Some('s') => (1, 1),
                Some('m') => (60, 1),
                Some('h') => (3_600, 1),
                Some('d') => (86_400, 1),
                Some('w') => (604_800, 1),
                Some('y') => (31_536_000, 1),
                _ => return Err(format!("duration `{text}` has no unit")),
            }
        };
        total = n
            .checked_mul(secs)
            .and_then(|part| total.checked_add(part))
            .ok_or_else(|| format!("duration `{text}` overflows"))?;
        rest = &rest[width..];
    }
    Ok(total)
}

/// The longest stretch of data before an evaluation timestamp that `promql`
/// reads, in seconds: the largest `[range]` of a matrix selector, and for a
/// subquery `expr[range:step]` its range plus the longest lookback inside
/// `expr`. Textual, like [`read_selectors`]: the operand of a subquery is the
/// bracketed or identifier expression written immediately before its `[`.
fn longest_lookback_secs(promql: &str) -> Result<u64, String> {
    let chars: Vec<char> = promql.chars().collect();
    let mut longest = 0u64;
    let mut i = 0usize;
    while i < chars.len() {
        if chars[i] == '"' {
            i = skip_string(&chars, i)?;
            continue;
        }
        if chars[i] != '[' {
            i += 1;
            continue;
        }
        let end = close_of(&chars, i)?;
        let inner: String = chars[i + 1..end - 1].iter().collect();
        let lookback = match inner.split_once(':') {
            None => parse_duration_secs(&inner)?,
            Some((range, _step)) => {
                let operand_start = subquery_operand_start(&chars, i)?;
                let operand: String = chars[operand_start..i].iter().collect();
                parse_duration_secs(range)?
                    .checked_add(longest_lookback_secs(&operand)?)
                    .ok_or_else(|| format!("subquery `[{inner}]` lookback overflows"))?
            }
        };
        longest = longest.max(lookback);
        i = end;
    }
    Ok(longest)
}

/// Where the operand of the subquery whose `[` is at `open` starts.
fn subquery_operand_start(chars: &[char], open: usize) -> Result<usize, String> {
    let mut i = open;
    while i > 0 && chars[i - 1].is_whitespace() {
        i -= 1;
    }
    if i > 0 && (chars[i - 1] == ')' || chars[i - 1] == '}') {
        let (o, c) = if chars[i - 1] == ')' {
            ('(', ')')
        } else {
            ('{', '}')
        };
        let mut depth = 0usize;
        let mut j = i;
        while j > 0 {
            j -= 1;
            if chars[j] == c {
                depth += 1;
            } else if chars[j] == o {
                depth -= 1;
                if depth == 0 {
                    break;
                }
            }
        }
        if depth != 0 {
            return Err("unbalanced subquery operand".to_string());
        }
        while j > 0 && is_ident_char(chars[j - 1]) {
            j -= 1;
        }
        return Ok(j);
    }
    let stop = i;
    while i > 0 && is_ident_char(chars[i - 1]) {
        i -= 1;
    }
    if i == stop {
        return Err("a subquery with no operand".to_string());
    }
    Ok(i)
}

/// A one-to-many join as the textual reader finds it.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Join {
    modifier: &'static str,
    keys: Vec<String>,
    one_side: Selector,
}

fn word_positions(chars: &[char], word: &str) -> Vec<usize> {
    let w: Vec<char> = word.chars().collect();
    (0..chars.len().saturating_sub(w.len() - 1))
        .filter(|&i| {
            chars[i..i + w.len()] == w[..]
                && (i == 0 || !is_ident_char(chars[i - 1]))
                && chars.get(i + w.len()).is_none_or(|c| !is_ident_char(*c))
        })
        .collect()
}

/// The `group_left`/`group_right` join in `promql`, if it has one.
///
/// Reads one binary operation of the shape `<lhs> <op> on (<keys>)
/// group_left [(<labels>)] <rhs>` (or `group_right`), where the one side,
/// `<rhs>` for `group_left` and `<lhs>` for `group_right`, is a bare vector
/// selector and nothing else. Anything else that names a group modifier is
/// refused: more than one join, `ignoring`, or a one side that is a function,
/// an aggregation, or a parenthesised expression, whose series set the reader
/// cannot derive from the manifest.
fn read_join(promql: &str) -> Result<Option<Join>, String> {
    let chars: Vec<char> = promql.chars().collect();
    let left = word_positions(&chars, "group_left");
    let right = word_positions(&chars, "group_right");
    let (modifier, at) = match (left.as_slice(), right.as_slice()) {
        ([], []) => return Ok(None),
        ([at], []) => ("group_left", *at),
        ([], [at]) => ("group_right", *at),
        _ => return Err("more than one group modifier".to_string()),
    };
    if !word_positions(&chars, "ignoring").is_empty() {
        return Err(format!("{modifier} with `ignoring`"));
    }
    let on = match word_positions(&chars, "on").as_slice() {
        [on] => *on,
        _ => return Err(format!("{modifier} without exactly one `on (...)`")),
    };
    let open = skip_ws(&chars, on + 2);
    if chars.get(open) != Some(&'(') {
        return Err("`on` without a label list".to_string());
    }
    let close = close_of(&chars, open)?;
    let keys: Vec<String> = chars[open + 1..close - 1]
        .iter()
        .collect::<String>()
        .split(',')
        .map(|k| k.trim().to_string())
        .filter(|k| !k.is_empty())
        .collect();
    let mut after = skip_ws(&chars, at + modifier.len());
    if chars.get(after) == Some(&'(') {
        after = close_of(&chars, after)?;
    }
    let one_text: String = if modifier == "group_left" {
        chars[after..].iter().collect()
    } else {
        let mut lhs: String = chars[..on].iter().collect();
        lhs = lhs.trim_end().to_string();
        if let Some(stripped) = lhs.strip_suffix("bool") {
            lhs = stripped.trim_end().to_string();
        }
        let op = [
            "==", "!=", ">=", "<=", "*", "/", "+", "-", "%", "^", ">", "<",
        ]
        .iter()
        .find(|op| lhs.ends_with(**op))
        .ok_or_else(|| "no binary operator before `on`".to_string())?;
        lhs[..lhs.len() - op.len()].to_string()
    };
    let one_text = one_text.trim();
    let one_side = match read_selectors(one_text)?.as_slice() {
        [only] if only.text == one_text => only.clone(),
        _ => {
            return Err(format!(
                "the one side of the {modifier} match, `{one_text}`, is not a bare vector selector"
            ));
        }
    };
    Ok(Some(Join {
        modifier,
        keys,
        one_side,
    }))
}

/// Visit the label sets of the instances that write `selector.metric` under
/// `profile` over its whole declared run, one at a time, generated on demand
/// and never collected, until `visit` returns `false`. `Ok(None)` when nothing
/// writes the metric, else `Ok(Some(n))` with the number of label sets
/// visited. A classic histogram's `_bucket`/`_sum`/`_count` resolve to its
/// instances; a matcher on `le` is refused, since the sets do not carry it.
fn visit_series(
    workload: &WorkloadFile,
    profile: &Profile,
    selector: &Selector,
    mut visit: impl FnMut(&LabelSet) -> bool,
) -> Result<Option<u64>, String> {
    let family = workload.families.iter().find(|f| match f.kind {
        FamilyKind::ClassicHistogram => ["_bucket", "_sum", "_count"]
            .iter()
            .any(|suffix| selector.metric == format!("{}{suffix}", f.name)),
        _ => selector.metric == f.name,
    });
    let Some(family) = family else {
        return Ok(None);
    };
    if selector.matchers.iter().any(|m| m.label == "le") {
        return Err(format!("`{}` matches on `le`", selector.text));
    }
    let generator = Generator::for_profile(workload, profile, 0);
    let Some(sets) = generator.family_label_iter(&family.name, profile.samples_per_series) else {
        return Ok(None);
    };
    let mut visited = 0u64;
    for labels in sets {
        visited += 1;
        if !visit(&labels) {
            break;
        }
    }
    Ok(Some(visited))
}

/// The checks that need the workload manifest, for every profile each entry
/// runs under (ADR-0927, 2026-10-26 amendment, decision E):
///
/// 1. every profile an entry names is declared;
/// 2. an `eval: range` entry's evaluation window (the profile's `range_eval`
///    start offset) plus its longest lookback fits inside the profile's stream
///    span ([`crate::metrics_workload::Profile::stream_span_secs`], one scrape
///    interval less than `duration_secs`), so every grid point's lookback
///    starts at or after the profile's first sample;
/// 3. unless the entry is marked intended-empty, every selector matches at
///    least one series the generator writes under the profile, except a
///    selector written directly as the argument of `absent(` or
///    `absent_over_time(`, which answer with a sample exactly when their
///    argument is empty;
/// 4. the one side of a `group_left`/`group_right` match (the right side for
///    `group_left`, the left for `group_right`) holds at most one series per
///    `on (...)` key, over every series the profile's whole run creates.
///
/// Series identity comes from the generator's own label assignment
/// ([`Generator::family_label_sets`]), not a re-typed formula. The query is
/// read as text (see `read_selectors`, `read_join`, `longest_lookback_secs`
/// for what each handles); a query the reader cannot take apart is refused
/// with [`CorpusError::Unreadable`] rather than passed. Check 3 proves that a
/// selector reaches data, not that the whole expression's answer is
/// non-empty: a threshold no sample crosses still passes it.
pub fn gate_corpus_against_workload(
    entries: &[CorpusEntry],
    workload: &WorkloadFile,
) -> Result<(), CorpusError> {
    for entry in entries {
        for name in &entry.profiles {
            if workload.profile(name).is_none() {
                return Err(CorpusError::UnknownProfile {
                    entry_id: entry.id.clone(),
                    profile: name.clone(),
                });
            }
        }
        let unreadable = |detail: String| CorpusError::Unreadable {
            entry_id: entry.id.clone(),
            detail,
        };
        let selectors = read_selectors(&entry.promql).map_err(unreadable)?;
        let lookback_secs = longest_lookback_secs(&entry.promql).map_err(unreadable)?;
        let join = read_join(&entry.promql).map_err(unreadable)?;

        for profile in workload
            .profiles
            .iter()
            .filter(|p| entry.runs_under(&p.name))
        {
            let window_secs = profile.range_eval.start_offset_secs;
            let span_secs = profile.stream_span_secs();
            if entry.eval == EvalKind::Range
                && window_secs.saturating_add(lookback_secs) > span_secs
            {
                return Err(CorpusError::WindowExceedsProfile {
                    entry_id: entry.id.clone(),
                    profile: profile.name.clone(),
                    window_secs,
                    lookback_secs,
                    span_secs,
                });
            }

            if !entry.expected.intended_empty {
                for selector in &selectors {
                    if matches!(
                        selector.argument_of.as_deref(),
                        Some("absent" | "absent_over_time")
                    ) {
                        continue;
                    }
                    let mut hit = false;
                    visit_series(workload, profile, selector, |labels| {
                        hit = selector.matchers.iter().all(|m| m.matches(labels));
                        !hit
                    })
                    .map_err(unreadable)?;
                    if !hit {
                        return Err(CorpusError::SelectsNothing {
                            entry_id: entry.id.clone(),
                            profile: profile.name.clone(),
                            selector: selector.text.clone(),
                        });
                    }
                }
            }

            if let Some(join) = &join {
                if join.one_side.metric.ends_with("_bucket") {
                    return Err(unreadable(format!(
                        "the one side `{}` is a classic-histogram bucket series",
                        join.one_side.text
                    )));
                }
                // Bounded by the one side's matching series, not the family:
                // only matching label sets are keyed, and the visit stops at
                // the first key seen twice.
                let mut per_key: BTreeMap<Vec<String>, usize> = BTreeMap::new();
                let mut repeated: Option<Vec<String>> = None;
                visit_series(workload, profile, &join.one_side, |labels| {
                    if !join.one_side.matchers.iter().all(|m| m.matches(labels)) {
                        return true;
                    }
                    let key: Vec<String> = join
                        .keys
                        .iter()
                        .map(|k| {
                            labels
                                .iter()
                                .find(|(name, _)| name == k)
                                .map(|(_, v)| v.clone())
                                .unwrap_or_default()
                        })
                        .collect();
                    let n = per_key.entry(key.clone()).or_default();
                    *n += 1;
                    if *n > 1 {
                        repeated = Some(key);
                        return false;
                    }
                    true
                })
                .map_err(unreadable)?;
                if let Some(key) = repeated {
                    let series = per_key.get(&key).copied().unwrap_or(2);
                    return Err(CorpusError::JoinKeyNotUnique {
                        entry_id: entry.id.clone(),
                        profile: profile.name.clone(),
                        modifier: join.modifier,
                        selector: join.one_side.text.clone(),
                        key: join
                            .keys
                            .iter()
                            .zip(key)
                            .map(|(k, v)| format!("{k}=\"{v}\""))
                            .collect::<Vec<_>>()
                            .join(", "),
                        series,
                    });
                }
            }
        }
    }
    Ok(())
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
            expected: Expected::default(),
            tolerance_ulps: None,
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
    fn the_classification_check_runs_across_the_whole_corpus_before_the_id_check() {
        // A corpus with an EARLIER duplicate id and a LATER unclassified entry
        // is invalid two ways. The contract (module docs, checks 1..5) runs the
        // all-or-none classification check across the whole corpus first, so the
        // classification failure must win regardless of entry order. A shared
        // loop that checks id-then-class per entry returns DuplicateId instead,
        // contradicting the stated order.
        let a = entry("dup", Some(CostClass::HighFanOut));
        let b = entry("dup", Some(CostClass::Join)); // duplicate id, earlier
        let c = entry("later", None); // unclassified, later
        let err = gate_corpus(&[a, b, c]).expect_err("the corpus is invalid two ways");
        assert!(
            matches!(&err, CorpusError::UnclassifiedEntry { entry_id } if entry_id == "later"),
            "classification must be checked across the whole corpus before the id check, got: {err:?}"
        );
    }

    #[test]
    fn a_modification_with_an_unknown_field_is_a_deserialization_error() {
        // The nested-struct instance of the deny_unknown_fields hole the
        // top-level `CorpusEntry`/`CorpusFile` already close: without the
        // attribute on `Modification`, `{"reason":"x","reasno":"x"}` drops
        // `reasno` silently and the entry reads as a disclosed modification with
        // the wrong intent recorded.
        let typo = serde_json::json!({
            "id": "a",
            "promql": "up",
            "constructs": ["vector selector"],
            "modified": {"modified": {"reason": "x", "reasno": "x"}}
        });
        let err = serde_json::from_value::<CorpusEntry>(typo)
            .expect_err("an unknown field inside `modified` must fail to deserialize");
        assert!(
            err.to_string().contains("reasno"),
            "the error must name the offending field, got: {err}"
        );

        // The same entry without the typo parses and records the modification.
        let ok = serde_json::json!({
            "id": "a",
            "promql": "up",
            "constructs": ["vector selector"],
            "modified": {"modified": {"reason": "x"}}
        });
        let parsed: CorpusEntry = serde_json::from_value(ok).expect("the correct shape parses");
        assert!(parsed.modified.is_modified());
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
        let path = write_corpus(&dir, "typo.json", r#"{"version": 3, "entires": []}"#);
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
            "{\"version\": 3, \"entries\": [{\"id\":",
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
                    expected: 3,
                    ..
                }
            ),
            "{err:?}"
        );
    }

    #[test]
    fn a_version_2_corpus_is_refused_after_the_full_range_rewrite() {
        // Version 3 changed what `mb_full_range_avg_over_time` computes, so a
        // version-2 file is a different corpus and must not load as this one.
        assert_eq!(CORPUS_FORMAT_VERSION, 3);
        let dir = tempfile::tempdir().expect("tempdir");
        let path = write_corpus(&dir, "corpus.json", r#"{"version": 2, "entries": []}"#);
        let err = load_corpus(&path).expect_err("a version-2 corpus must fail");
        assert!(
            matches!(
                err,
                CorpusError::UnsupportedVersion {
                    found: 2,
                    expected: 3,
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
              "version": 3,
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
    fn the_lookback_reader_reads_ranges_compound_durations_and_nested_subqueries() {
        let cases: &[(&str, u64)] = &[
            ("metricsbench_gauge_cpu_percent", 0),
            ("rate(m[5m])", 300),
            ("rate(m[1h30m]) / rate(m[5m])", 5_400),
            ("max_over_time(sum(rate(m[5m]))[1d:5m])", 86_700),
            ("max_over_time(sum(rate(m[5m]))[1d:])", 86_700),
            (
                "avg_over_time(max_over_time(rate(m[5m])[1h:1m])[1d:5m])",
                90_300,
            ),
            ("predict_linear(m{job=\"a\"}[6h], 4 * 3600)", 21_600),
            ("avg_over_time(m[7d])", 604_800),
            ("avg_over_time(m[2w])", 1_209_600),
        ];
        for (promql, expected) in cases {
            assert_eq!(longest_lookback_secs(promql), Ok(*expected), "{promql}");
        }
        for bad in ["rate(m[5])", "rate(m[500ms])", "rate(m[])", "rate(m[5x])"] {
            assert!(longest_lookback_secs(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn the_selector_reader_finds_metric_names_and_skips_everything_else() {
        let selectors = read_selectors(
            "sum by (job) (rate(a_total{job=\"x\",status!=\"500\"}[5m])) / on (job) \
             group_left (version) b{region=~\"eu-.*|us-east-1\"} and absent(c)",
        )
        .expect("readable");
        let names: Vec<&str> = selectors.iter().map(|s| s.metric.as_str()).collect();
        assert_eq!(names, vec!["a_total", "b", "c"]);
        assert_eq!(selectors[0].text, "a_total{job=\"x\",status!=\"500\"}");
        assert_eq!(selectors[0].argument_of.as_deref(), Some("rate"));
        assert_eq!(selectors[2].argument_of.as_deref(), Some("absent"));
        let labels = |pairs: &[(&str, &str)]| -> Vec<(String, String)> {
            pairs
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect()
        };
        let region = &selectors[1].matchers[0];
        assert!(region.matches(&labels(&[("region", "eu-west-1")])));
        assert!(region.matches(&labels(&[("region", "us-east-1")])));
        assert!(!region.matches(&labels(&[("region", "us-east-2")])));
        assert!(!region.matches(&labels(&[])));
        let status = &selectors[0].matchers[1];
        assert!(status.matches(&labels(&[("status", "200")])));
        assert!(!status.matches(&labels(&[("status", "500")])));

        for bad in [
            "{job=\"x\"}",
            "m offset 5m",
            "m @ 100",
            "m{__name__=\"x\"}",
            "m{job=~\"a+\"}",
            "m{job=\"x\"",
        ] {
            assert!(read_selectors(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn the_join_reader_finds_the_one_side_of_either_modifier() {
        let left = read_join("a * on (job, instance) group_left (version) b{version=\"1\"}")
            .expect("readable")
            .expect("a join");
        assert_eq!(left.modifier, "group_left");
        assert_eq!(left.keys, vec!["job".to_string(), "instance".to_string()]);
        assert_eq!(left.one_side.text, "b{version=\"1\"}");

        let right = read_join("a{x=\"1\"} > bool on (job) group_right b")
            .expect("readable")
            .expect("a join");
        assert_eq!(right.modifier, "group_right");
        assert_eq!(right.one_side.text, "a{x=\"1\"}");

        assert_eq!(read_join("a and on (job) b"), Ok(None));
        for bad in [
            "a * ignoring (x) group_left b",
            "a * on (job) group_left sum by (job) (b)",
            "a * group_left b",
            "a * on (job) group_left b * on (job) group_left c",
        ] {
            assert!(read_join(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn an_intended_empty_entry_needs_a_reason() {
        let mut marked = entry("marked", Some(CostClass::Histogram));
        marked.expected = Expected {
            intended_empty: true,
            reason: None,
        };
        let err = gate_corpus(&[marked.clone()]).expect_err("no reason must fail");
        assert!(
            matches!(&err, CorpusError::EmptyIntendedEmptyReason { entry_id } if entry_id == "marked"),
            "{err:?}"
        );
        marked.expected.reason = Some("  ".to_string());
        assert!(
            gate_corpus(&[marked.clone()]).is_err(),
            "a blank reason must fail"
        );
        marked.expected.reason = Some("no native histogram on the wire yet".to_string());
        gate_corpus(&[marked]).expect("a stated reason gates clean");
    }

    fn checked_in_workload() -> crate::metrics_workload::WorkloadFile {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../benchmarks/metrics/workload.json");
        crate::metrics_workload::load_workload(path).expect("the checked-in manifest loads")
    }

    #[test]
    fn the_full_range_entry_collapses_its_answer_and_says_why() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../benchmarks/metrics/metrics.corpus.json");
        let entries = load_corpus(path).expect("the checked-in corpus loads");
        let entry = entries
            .iter()
            .find(|e| e.id == "mb_full_range_avg_over_time")
            .expect("entry present");
        assert_eq!(
            entry.promql,
            "avg(avg_over_time(metricsbench_gauge_cpu_percent[30m]))"
        );
        assert_eq!(entry.class, Some(CostClass::FullRange));
        assert_eq!(entry.eval, EvalKind::Range);
        assert_eq!(entry.profiles, vec!["cardinality", "history", "churn"]);
        assert_eq!(
            entry.constructs,
            vec![
                "aggregate expression",
                "avg",
                "function call",
                "avg_over_time",
                "matrix selector",
                "vector selector"
            ]
        );
        let reason = entry
            .modified
            .reason()
            .expect("the rewrite is disclosed as a modification");
        assert!(!reason.trim().is_empty());
        assert!(reason.contains("256 MiB"), "{reason}");

        // Answer size on cardinality: the scan still selects every gauge
        // series. Uncollapsed, the answer held one series per selected series;
        // `avg` without `by` answers one series over the same 121-point grid.
        let workload = checked_in_workload();
        let profile = workload
            .profile("cardinality")
            .expect("cardinality declared");
        let selectors = read_selectors(&entry.promql).expect("selectors read");
        assert_eq!(selectors.len(), 1);
        let selected = visit_series(&workload, profile, &selectors[0], |_| true)
            .expect("visit runs")
            .expect("the family exists");
        let points = profile.range_eval.points();
        assert_eq!(selected, 400_000);
        assert_eq!(points, 121);
        assert_eq!(selected * points, 48_400_000, "the uncollapsed answer");
    }

    #[test]
    fn the_selector_check_stops_at_the_first_matching_series() {
        // A selector that pins the first instance must be answered after one
        // label set, not after the cardinality profile's 400,000 gauge
        // instances: the check streams the family and stops on the first
        // match rather than materializing it.
        let workload = checked_in_workload();
        let profile = workload
            .profile("cardinality")
            .expect("cardinality declared");
        let selectors =
            read_selectors(r#"metricsbench_gauge_cpu_percent{instance="metricsbench-instance-0"}"#)
                .expect("selector parses");
        let selector = &selectors[0];
        let visited = visit_series(&workload, profile, selector, |labels| {
            !selector.matchers.iter().all(|m| m.matches(labels))
        })
        .expect("visit runs")
        .expect("the family exists");
        assert_eq!(
            visited, 1,
            "the first instance matches, so one label set is enough"
        );
        // Visiting to the end reaches every instance, so the bound above is
        // the early exit, not a small family.
        let all = visit_series(&workload, profile, selector, |_| true)
            .expect("visit runs")
            .expect("the family exists");
        assert_eq!(all, 400_000);
    }

    #[test]
    fn a_range_window_fits_the_stream_span_not_the_duration() {
        // ci: 120 steps at 15 s span 1,785 s from first sample to newest.
        // A 5 m lookback plus a window ending exactly at the first sample
        // passes; one scrape interval more reaches before the first sample
        // and is refused, though it still fits inside duration_secs (1,800).
        let mut workload = checked_in_workload();
        let ci = workload
            .profiles
            .iter_mut()
            .find(|p| p.name == "ci")
            .expect("ci declared");
        assert_eq!(ci.stream_span_secs(), 1_785);
        let mut range = entry("fits", Some(CostClass::SingleSeries));
        range.eval = EvalKind::Range;
        range.profiles = vec!["ci".to_string()];
        ci.range_eval.start_offset_secs = 1_485;
        gate_corpus_against_workload(&[range.clone()], &workload)
            .expect("window plus lookback equal to the span fits");
        let ci = workload
            .profiles
            .iter_mut()
            .find(|p| p.name == "ci")
            .expect("ci declared");
        ci.range_eval.start_offset_secs = 1_500;
        let err = gate_corpus_against_workload(&[range], &workload)
            .expect_err("one interval past the span must be refused");
        assert!(
            matches!(
                &err,
                CorpusError::WindowExceedsProfile {
                    window_secs: 1_500,
                    lookback_secs: 300,
                    span_secs: 1_785,
                    ..
                }
            ),
            "{err:?}"
        );
    }

    #[test]
    fn a_duration_too_large_for_seconds_is_a_typed_error() {
        let err = parse_duration_secs("1000000000000y").expect_err("must overflow");
        assert!(err.contains("overflows"), "{err}");
        let err = longest_lookback_secs("max_over_time(x[1000000000000y:5m])")
            .expect_err("a subquery range that overflows must be refused");
        assert!(err.contains("overflows"), "{err}");
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
