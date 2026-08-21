//! The analytical SQL query corpus (ADR-0100 decision 3).
//!
//! A versioned set of workload-shaped statements against the `logs` table,
//! covering the four shapes ADR-0100 decision 3 names: filtered aggregates,
//! string search, `GROUP BY`, and `ORDER BY` + `LIMIT`. Each entry carries the
//! constructs it exercises, and every named construct is cross-checked against
//! [`ravel_sql::conformance::registry`]'s supported set, so the corpus seeds
//! from the supported-construct list rather than re-enumerating it.
//!
//! The same gate applies to the checked-in corpus ([`default_corpus`]) and to
//! an external file ([`load_external_corpus`]): an external corpus is exempt
//! from being checked in, not from the gate. A statement naming a construct the
//! registry does not support fails with the construct and the entry id named,
//! which is the signal that says which capability is missing.
//!
//! This module is data plus its gate. It never executes a statement: running
//! the corpus is the `sql_latency_bench` harness's job (ADR-0100 decision 4).
//!
//! ## External corpus format
//!
//! JSON (`serde_json` is already a plain dependency of this crate), one
//! document per corpus:
//!
//! ```json
//! {
//!   "version": 1,
//!   "entries": [
//!     {
//!       "id": "q1_error_rate",
//!       "sql": "SELECT count(*) FROM logs WHERE severity_num >= 17",
//!       "constructs": ["logs -> Signal::Logs", "Filter (WHERE)", "count"],
//!       "expected_rows": 1,
//!       "upstream_id": "Q1",
//!       "modified": {"modified": {"reason": "counts errors, not all rows"}}
//!     }
//!   ]
//! }
//! ```
//!
//! `expected_rows`, `upstream_id`, and `modified` may be omitted; `modified`
//! then defaults to [`Modification::Verbatim`].

use std::collections::{BTreeSet, HashSet};
use std::path::{Path, PathBuf};

use ravel_sql::conformance::{Classification, registry};
use serde::{Deserialize, Serialize};

/// The corpus format version this module reads and writes. An external file
/// declaring any other version is rejected rather than parsed optimistically:
/// the field exists so a future format change is a loud error, not a silently
/// misread document.
pub const CORPUS_FORMAT_VERSION: u32 = 1;

/// Whether a statement was rewritten in a way that changes what it computes.
///
/// This is a disclosure obligation from ADR-0100 decision 3, so it is a typed
/// field rather than a comment: renaming a table or a column is *not* a
/// modification, changing the computed result is, and a modified statement
/// cannot exist without a stated reason.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Modification {
    /// The statement computes what its upstream original computes (it may still
    /// have been re-pointed at Ravel's table and column names).
    #[default]
    Verbatim,
    /// The statement was rewritten in a way that changes its result.
    Modified {
        /// What the rewrite changed about the computation, and why.
        reason: String,
    },
}

impl Modification {
    /// The bool half of the flag: whether this statement computes something
    /// other than its upstream original.
    pub fn is_modified(&self) -> bool {
        matches!(self, Modification::Modified { .. })
    }

    /// The stated reason, or `None` for a verbatim statement.
    pub fn reason(&self) -> Option<&str> {
        match self {
            Modification::Verbatim => None,
            Modification::Modified { reason } => Some(reason),
        }
    }
}

/// One corpus statement and everything a reader needs to judge it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CorpusEntry {
    /// A stable short identifier, unique within a corpus. Report rows are keyed
    /// by it, so it must not change when the statement is edited in place.
    pub id: String,
    /// The statement text, as it will be handed to the executor.
    pub sql: String,
    /// The constructs the statement exercises, named exactly as
    /// [`ravel_sql::conformance::Construct::name`] names them.
    pub constructs: Vec<String>,
    /// The row count the statement is expected to return, where it is a
    /// property of the statement rather than of the dataset.
    #[serde(default)]
    pub expected_rows: Option<usize>,
    /// The id this statement carries in the external suite it came from, so a
    /// statement can be diffed against its original.
    #[serde(default)]
    pub upstream_id: Option<String>,
    /// Whether the statement was rewritten in a way that changes what it
    /// computes.
    #[serde(default)]
    pub modified: Modification,
}

/// A parsed corpus document: a format version plus its entries.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CorpusFile {
    /// The format version, which must equal [`CORPUS_FORMAT_VERSION`].
    pub version: u32,
    /// The statements, in the order the harness runs them.
    pub entries: Vec<CorpusEntry>,
}

/// Everything that can go wrong loading or gating a corpus. Every variant names
/// the entry (and where applicable the construct) at fault: the message is the
/// signal a reader acts on, so it is part of the contract.
#[derive(Debug, thiserror::Error)]
pub enum CorpusError {
    /// The corpus file could not be read.
    #[error("corpus file {path}: {source}")]
    Read {
        /// The path that was read.
        path: PathBuf,
        /// The underlying I/O failure.
        #[source]
        source: std::io::Error,
    },
    /// The corpus file is not a valid corpus document.
    #[error("corpus file {path} is not a valid JSON corpus document: {source}")]
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
        "corpus file {path} declares format version {found}, but this build reads \
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
    /// An entry names a construct the conformance registry does not classify as
    /// supported. This is the capability-gap signal ADR-0100 decision 3 exists
    /// to produce, so it names both the construct and the entry.
    #[error(
        "corpus entry `{entry_id}` names construct `{construct}`, which is not in the \
         supported set of ravel_sql::conformance::registry(): no registry construct \
         with that name is classified SupportedAndCovered. Either the construct name \
         is misspelled, or the capability it names is not supported yet and the \
         statement cannot run until it is."
    )]
    UnsupportedConstruct {
        /// The id of the entry that named it.
        entry_id: String,
        /// The construct name that is not supported.
        construct: String,
    },
    /// An entry names no constructs at all, so the gate would pass it
    /// vacuously.
    #[error("corpus entry `{entry_id}` names no constructs; every entry must name at least one")]
    NoConstructs {
        /// The id of the offending entry.
        entry_id: String,
    },
    /// Two entries share an id, so their report rows would collide.
    #[error("corpus entry id `{entry_id}` appears more than once; ids must be unique")]
    DuplicateId {
        /// The duplicated id.
        entry_id: String,
    },
    /// An entry is flagged modified with an empty reason, which discloses
    /// nothing.
    #[error(
        "corpus entry `{entry_id}` is flagged as a modified query with an empty reason; \
         a modified query must state what its rewrite changed about the computation"
    )]
    EmptyModificationReason {
        /// The id of the offending entry.
        entry_id: String,
    },
}

/// The set of construct names the conformance registry classifies as supported.
///
/// Matching is by name over the supported subset, which is what a corpus entry
/// can reasonably carry: the registry's own category is an internal grouping,
/// and one construct name is unique enough to identify a capability.
pub fn supported_construct_names() -> BTreeSet<String> {
    registry()
        .into_iter()
        .filter(|c| matches!(c.classification, Classification::SupportedAndCovered { .. }))
        .map(|c| c.name)
        .collect()
}

/// Apply the construct gate and the structural checks to a corpus, identically
/// for the checked-in set and for an external file.
pub fn gate_corpus(entries: &[CorpusEntry]) -> Result<(), CorpusError> {
    let supported = supported_construct_names();
    let mut seen_ids: HashSet<&str> = HashSet::new();
    for entry in entries {
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
            if !supported.contains(construct) {
                return Err(CorpusError::UnsupportedConstruct {
                    entry_id: entry.id.clone(),
                    construct: construct.clone(),
                });
            }
        }
    }
    Ok(())
}

/// The checked-in corpus, gated. This is what the harness runs when no
/// `--corpus` path is given.
pub fn checked_default_corpus() -> Result<Vec<CorpusEntry>, CorpusError> {
    let entries = default_corpus();
    gate_corpus(&entries)?;
    Ok(entries)
}

/// Read an external corpus document, parse it, and gate it. A malformed file is
/// a typed error naming what failed to parse; it is never a panic and never a
/// silently skipped entry.
pub fn load_external_corpus(path: impl AsRef<Path>) -> Result<Vec<CorpusEntry>, CorpusError> {
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

fn entry(id: &str, sql: &str, constructs: &[&str], expected_rows: Option<usize>) -> CorpusEntry {
    CorpusEntry {
        id: id.to_string(),
        sql: sql.to_string(),
        constructs: constructs.iter().map(|c| c.to_string()).collect(),
        expected_rows,
        // The checked-in statements are Ravel-native: they are written against
        // the `logs` table directly rather than adapted from an external suite,
        // so there is no upstream id to diff against and nothing to disclose.
        upstream_id: None,
        modified: Modification::Verbatim,
    }
}

/// The checked-in corpus, ungated. Prefer [`checked_default_corpus`]; this
/// exists so a test can gate it explicitly.
///
/// The statements target the `logs` table's nine fixed columns (`ts`,
/// `observed_ts`, `severity_num`, `severity_text`, `body`, `trace_id`,
/// `span_id`, `flags`, `attrs`) plus the declared typed attribute column
/// `duration_ms`, which the harness declares for the generated lane (ADR-0090
/// decision 1 names a declared column by its attribute key verbatim).
///
/// Row counts are only stated where they are a property of the statement: an
/// ungrouped aggregate returns exactly one row whatever the dataset holds,
/// while a `GROUP BY` returns as many rows as the data has groups.
pub fn default_corpus() -> Vec<CorpusEntry> {
    vec![
        // --- Filtered aggregates -----------------------------------------
        entry(
            "filtered_error_count",
            "SELECT count(*) FROM logs WHERE severity_num >= 17",
            &["logs -> Signal::Logs", "Filter (WHERE)", "count"],
            Some(1),
        ),
        entry(
            "filtered_time_span",
            "SELECT min(ts), max(ts) FROM logs WHERE severity_text = 'ERROR'",
            &["Filter (WHERE)", "min", "max"],
            Some(1),
        ),
        entry(
            "distinct_severity_count",
            "SELECT count(DISTINCT severity_text) FROM logs",
            &["count(DISTINCT)"],
            Some(1),
        ),
        entry(
            "typed_duration_threshold_count",
            "SELECT count(*) FROM logs WHERE duration_ms >= 1000",
            &["declared i64 typed comparison", "Filter (WHERE)", "count"],
            Some(1),
        ),
        entry(
            "typed_duration_sum",
            "SELECT sum(duration_ms) FROM logs WHERE severity_num >= 9",
            &["declared i64 typed aggregate", "Filter (WHERE)", "sum"],
            Some(1),
        ),
        // --- String search ------------------------------------------------
        entry(
            "body_word_search",
            "SELECT count(*) FROM logs WHERE has_word(body, 'timeout')",
            &["has_word", "Filter (WHERE)", "count"],
            Some(1),
        ),
        entry(
            "severity_case_insensitive_match",
            "SELECT count(*) FROM logs WHERE upper(severity_text) = 'ERROR'",
            &["upper", "Filter (WHERE)", "count"],
            Some(1),
        ),
        entry(
            "body_shape_histogram",
            "SELECT regexp_replace(body, '[0-9]+', 'N'), count(*) FROM logs \
             GROUP BY 1 ORDER BY 2 DESC LIMIT 10",
            &[
                "regexp_replace",
                "GROUP BY ordinal",
                "count",
                "ORDER BY",
                "LIMIT",
            ],
            None,
        ),
        // --- GROUP BY ------------------------------------------------------
        entry(
            "count_by_severity",
            "SELECT severity_text, count(*) FROM logs GROUP BY severity_text \
             ORDER BY severity_text",
            &["GROUP BY", "count", "ORDER BY"],
            None,
        ),
        entry(
            "busy_hours",
            "SELECT date_trunc('hour', ts), count(*) FROM logs GROUP BY 1 \
             HAVING count(*) > 100 ORDER BY 1",
            &[
                "DATE_TRUNC",
                "GROUP BY ordinal",
                "HAVING",
                "count",
                "ORDER BY",
            ],
            None,
        ),
        entry(
            "severity_bucket_split",
            "SELECT CASE WHEN severity_num >= 17 THEN 'error' ELSE 'other' END, count(*) \
             FROM logs WHERE severity_text IN ('ERROR', 'FATAL', 'WARN') \
             GROUP BY 1 ORDER BY 1",
            &[
                "CASE",
                "IN list",
                "GROUP BY ordinal",
                "count",
                "ORDER BY",
                "Filter (WHERE)",
            ],
            None,
        ),
        // --- ORDER BY + LIMIT ----------------------------------------------
        entry(
            "recent_errors_top_n",
            "SELECT ts, severity_text, body FROM logs WHERE severity_num >= 17 \
             ORDER BY ts DESC LIMIT 20",
            &["Projection", "Filter (WHERE)", "ORDER BY", "LIMIT"],
            None,
        ),
    ]
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

    #[test]
    fn every_corpus_construct_is_in_the_conformance_registry() {
        let supported = supported_construct_names();
        let corpus = default_corpus();
        assert!(
            (8..=12).contains(&corpus.len()),
            "corpus is {} entries; ADR-0100 decision 3 sizes it at roughly 8 to 12",
            corpus.len()
        );
        for entry in &corpus {
            assert!(
                !entry.constructs.is_empty(),
                "entry `{}` names no constructs",
                entry.id
            );
            for construct in &entry.constructs {
                assert!(
                    supported.contains(construct),
                    "entry `{}` names construct `{construct}`, which the conformance \
                     registry does not classify as supported",
                    entry.id
                );
            }
        }
        // The same check through the shipped gate, so the gate itself is what a
        // caller relies on rather than this test's open-coded loop.
        checked_default_corpus().expect("checked-in corpus passes its own gate");
    }

    #[test]
    fn the_corpus_covers_all_four_adr_0100_shapes() {
        let named: BTreeSet<String> = default_corpus()
            .into_iter()
            .flat_map(|e| e.constructs)
            .collect();
        for construct in ["count", "has_word", "GROUP BY", "ORDER BY", "LIMIT"] {
            assert!(named.contains(construct), "no entry exercises {construct}");
        }
    }

    #[test]
    fn external_corpus_file_parses_and_is_construct_gated() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = write_corpus(
            &dir,
            "corpus.json",
            r#"{
              "version": 1,
              "entries": [
                {
                  "id": "ext_group_by",
                  "sql": "SELECT severity_text, count(*) FROM logs GROUP BY severity_text",
                  "constructs": ["GROUP BY", "count"],
                  "upstream_id": "Q13"
                },
                {
                  "id": "ext_top_n",
                  "sql": "SELECT ts, body FROM logs ORDER BY ts DESC LIMIT 10",
                  "constructs": ["Projection", "ORDER BY", "LIMIT"],
                  "expected_rows": 10,
                  "upstream_id": "Q14",
                  "modified": {"modified": {"reason": "upstream counts distinct users; Ravel has no user column, so this returns raw rows instead"}}
                }
              ]
            }"#,
        );

        let entries = load_external_corpus(&path).expect("external corpus loads and gates");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].upstream_id.as_deref(), Some("Q13"));
        assert_eq!(entries[0].expected_rows, None);
        assert!(!entries[0].modified.is_modified());
        // The loader gated what it returned: gating the same entries again is a
        // no-op, while the identical corpus with one construct renamed to a name
        // the registry does not carry is refused. Without this pair, a loader
        // that skipped the gate would still pass this test.
        gate_corpus(&entries).expect("loaded entries are gated");
        let mut tampered = entries.clone();
        tampered[0].constructs[0] = "GROUP BY CUBE".to_string();
        assert!(
            gate_corpus(&tampered).is_err(),
            "gate must refuse an unsupported construct"
        );
        assert_eq!(entries[1].expected_rows, Some(10));
        assert!(entries[1].modified.is_modified());
        assert!(
            entries[1]
                .modified
                .reason()
                .expect("modified entry states a reason")
                .contains("no user column")
        );
    }

    #[test]
    fn an_external_corpus_naming_an_unsupported_construct_fails_with_the_construct_named() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = write_corpus(
            &dir,
            "corpus.json",
            r#"{
              "version": 1,
              "entries": [
                {
                  "id": "ext_quantile",
                  "sql": "SELECT approx_percentile_cont(duration_ms, 0.9) FROM logs",
                  "constructs": ["count", "approx_percentile_cont"],
                  "upstream_id": "Q29"
                }
              ]
            }"#,
        );

        let err =
            load_external_corpus(&path).expect_err("unsupported construct must fail the gate");
        assert!(
            matches!(&err, CorpusError::UnsupportedConstruct { entry_id, construct }
                if entry_id == "ext_quantile" && construct == "approx_percentile_cont"),
            "wrong error variant: {err:?}"
        );
        let message = err.to_string();
        assert!(
            message.contains("approx_percentile_cont"),
            "error must name the construct: {message}"
        );
        assert!(
            message.contains("ext_quantile"),
            "error must name the entry id: {message}"
        );
        assert!(
            message.contains("SupportedAndCovered"),
            "error must say which registry classification was required: {message}"
        );
    }

    #[test]
    fn a_malformed_external_corpus_is_a_typed_parse_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = write_corpus(
            &dir,
            "corpus.json",
            "{\"version\": 1, \"entries\": [{\"id\":",
        );

        let err = load_external_corpus(&path).expect_err("malformed JSON must fail");
        match &err {
            CorpusError::Parse { path: p, source } => {
                assert_eq!(p, &path);
                // serde_json's message carries where parsing failed, which is
                // the part a reader fixes the file by.
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
    fn an_entry_with_a_wrong_field_type_is_a_typed_parse_error_not_a_skip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = write_corpus(
            &dir,
            "corpus.json",
            r#"{"version": 1, "entries": [
                 {"id": "ext", "sql": "SELECT 1", "constructs": "GROUP BY"}
               ]}"#,
        );

        let err = load_external_corpus(&path).expect_err("a non-list `constructs` must fail");
        assert!(matches!(err, CorpusError::Parse { .. }), "{err:?}");
    }

    #[test]
    fn a_missing_external_corpus_is_a_read_error_naming_the_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let missing = dir.path().join("absent.json");
        let err = load_external_corpus(&missing).expect_err("a missing file must fail");
        assert!(
            matches!(&err, CorpusError::Read { path, .. } if path == &missing),
            "{err:?}"
        );
        assert!(err.to_string().contains("absent.json"));
    }

    #[test]
    fn an_unknown_format_version_is_rejected() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = write_corpus(&dir, "corpus.json", r#"{"version": 2, "entries": []}"#);
        let err = load_external_corpus(&path).expect_err("an unknown version must fail");
        assert!(
            matches!(err, CorpusError::UnsupportedVersion { found: 2, .. }),
            "{err:?}"
        );
    }

    #[test]
    fn a_modified_entry_without_a_reason_is_rejected() {
        let entries = vec![CorpusEntry {
            id: "ext".to_string(),
            sql: "SELECT count(*) FROM logs".to_string(),
            constructs: vec!["count".to_string()],
            expected_rows: Some(1),
            upstream_id: Some("Q1".to_string()),
            modified: Modification::Modified {
                reason: "   ".to_string(),
            },
        }];
        let err = gate_corpus(&entries).expect_err("an empty reason must fail");
        assert!(
            matches!(err, CorpusError::EmptyModificationReason { .. }),
            "{err:?}"
        );
    }

    #[test]
    fn duplicate_ids_and_empty_construct_lists_are_rejected() {
        let base = entry("dup", "SELECT count(*) FROM logs", &["count"], Some(1));
        let err = gate_corpus(&[base.clone(), base.clone()]).expect_err("duplicate ids must fail");
        assert!(matches!(err, CorpusError::DuplicateId { .. }), "{err:?}");

        let empty = entry("bare", "SELECT count(*) FROM logs", &[], Some(1));
        let err = gate_corpus(&[empty]).expect_err("an entry naming no constructs must fail");
        assert!(matches!(err, CorpusError::NoConstructs { .. }), "{err:?}");
    }
}
