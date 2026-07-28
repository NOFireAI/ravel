//! Corpus file format (docs/promql-evaluator-plan.md section 5.3):
//! blank-line-separated entries, each a small set of `key: value` lines.
//! Lines starting with `#` are comments. Every offset is relative to the
//! dataset's `base_ts_ms`, so a corpus file never encodes an absolute
//! timestamp (CLAUDE.md "time is injected").
//!
//! ```text
//! name: selector_simple_match
//! query: diff_gauge_walk
//! kind: instant
//! time: +0s
//! mode: unordered
//!
//! name: selector_range_window
//! query: diff_gauge_walk[5m]
//! kind: range
//! start: +0s
//! end: +600s
//! step: 30s
//! mode: unordered
//!
//! name: bad_paren
//! query: sum(
//! kind: instant
//! time: +0s
//! mode: error
//!
//! name: atanh_off_by_a_few_ulps
//! query: atanh(diff_gauge_walk)
//! kind: instant
//! time: +0s
//! mode: unordered
//! tolerance: 256
//! ```
//!
//! `tolerance` is optional (ADR-0025's allowlist): a non-negative integer
//! count of representable f64 steps the comparator accepts between the two
//! sides' values, in place of the default bit-exact comparison. Every use
//! needs a comment above the entry explaining why exact bits are
//! unattainable and where the tolerance number came from; see
//! docs/adrs/0025-promql-differential-float-precision-residue.md.

use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonMode {
    /// Sort both result vectors by full label set before comparing.
    Unordered,
    /// Compare results in the order each side returned them.
    Ordered,
    /// Both sides must reject the query; compared by `errorType` class.
    ExpectError,
}

#[derive(Debug, Clone, Copy)]
pub enum Eval {
    Instant {
        time_offset_ms: i64,
    },
    Range {
        start_offset_ms: i64,
        end_offset_ms: i64,
        step_ms: i64,
    },
}

#[derive(Debug, Clone)]
pub struct CorpusEntry {
    pub name: String,
    pub query: String,
    pub eval: Eval,
    pub mode: ComparisonMode,
    /// ADR-0025's allowlist: an optional, named ULP tolerance for this
    /// entry's value comparison, from an optional `tolerance:` field.
    /// Absent by default, meaning the usual bit-exact comparison; present
    /// only on entries with a written justification in the corpus file
    /// itself for why exact bits are unattainable.
    pub tolerance_ulps: Option<u32>,
}

#[derive(Debug, thiserror::Error)]
pub enum CorpusError {
    #[error("entry {index} (line {line}): {message}")]
    Malformed {
        index: usize,
        line: usize,
        message: String,
    },
}

fn err(index: usize, line: usize, message: impl fmt::Display) -> CorpusError {
    CorpusError::Malformed {
        index,
        line,
        message: message.to_string(),
    }
}

/// Parses a signed offset like `+300s` or `-30s` into milliseconds.
fn parse_offset_ms(s: &str, index: usize, line: usize) -> Result<i64, CorpusError> {
    let s = s.trim();
    let (sign, rest) = match s.strip_prefix('-') {
        Some(rest) => (-1i64, rest),
        None => (1i64, s.strip_prefix('+').unwrap_or(s)),
    };
    let secs = parse_duration_secs(rest, index, line)?;
    Ok(sign * secs * 1000)
}

/// Parses an unsigned duration like `30s` or `5m` into whole seconds.
fn parse_duration_secs(s: &str, index: usize, line: usize) -> Result<i64, CorpusError> {
    let s = s.trim();
    let (num, unit) = s.split_at(s.len().saturating_sub(1));
    let value: i64 = num
        .parse()
        .map_err(|_| err(index, line, format!("invalid duration '{s}'")))?;
    match unit {
        "s" => Ok(value),
        "m" => Ok(value * 60),
        "h" => Ok(value * 3600),
        _ => Err(err(index, line, format!("unknown duration unit in '{s}'"))),
    }
}

/// Parses a duration like `30s` into milliseconds (for `step`).
fn parse_duration_ms(s: &str, index: usize, line: usize) -> Result<i64, CorpusError> {
    Ok(parse_duration_secs(s, index, line)? * 1000)
}

pub fn parse_corpus(text: &str) -> Result<Vec<CorpusEntry>, CorpusError> {
    let mut entries = Vec::new();
    let mut fields: Vec<(String, String, usize)> = Vec::new();
    let mut index = 0usize;

    let flush = |fields: &mut Vec<(String, String, usize)>,
                 index: usize|
     -> Result<Option<CorpusEntry>, CorpusError> {
        if fields.is_empty() {
            return Ok(None);
        }
        let taken = std::mem::take(fields);
        Ok(Some(build_entry(index, taken)?))
    };

    for (line_no, raw_line) in text.lines().enumerate() {
        let line_no = line_no + 1;
        let line = raw_line.trim();
        if line.is_empty() {
            if let Some(entry) = flush(&mut fields, index)? {
                index += 1;
                entries.push(entry);
            }
            continue;
        }
        if line.starts_with('#') {
            continue;
        }
        let (key, value) = line.split_once(':').ok_or_else(|| {
            err(
                index,
                line_no,
                format!("expected 'key: value', got '{line}'"),
            )
        })?;
        fields.push((key.trim().to_string(), value.trim().to_string(), line_no));
    }
    if let Some(entry) = flush(&mut fields, index)? {
        entries.push(entry);
    }
    Ok(entries)
}

fn build_entry(
    index: usize,
    fields: Vec<(String, String, usize)>,
) -> Result<CorpusEntry, CorpusError> {
    let last_line = fields.last().map(|(_, _, l)| *l).unwrap_or(0);
    let get = |key: &str| -> Option<(&str, usize)> {
        fields
            .iter()
            .find(|(k, _, _)| k == key)
            .map(|(_, v, l)| (v.as_str(), *l))
    };

    let (name, _) = get("name").ok_or_else(|| err(index, last_line, "missing 'name'"))?;
    let (query, _) = get("query").ok_or_else(|| err(index, last_line, "missing 'query'"))?;
    let (kind, kind_line) = get("kind").ok_or_else(|| err(index, last_line, "missing 'kind'"))?;

    let eval = match kind {
        "instant" => {
            let (time, line) =
                get("time").ok_or_else(|| err(index, kind_line, "instant entry missing 'time'"))?;
            Eval::Instant {
                time_offset_ms: parse_offset_ms(time, index, line)?,
            }
        }
        "range" => {
            let (start, start_line) =
                get("start").ok_or_else(|| err(index, kind_line, "range entry missing 'start'"))?;
            let (end, end_line) =
                get("end").ok_or_else(|| err(index, kind_line, "range entry missing 'end'"))?;
            let (step, step_line) =
                get("step").ok_or_else(|| err(index, kind_line, "range entry missing 'step'"))?;
            Eval::Range {
                start_offset_ms: parse_offset_ms(start, index, start_line)?,
                end_offset_ms: parse_offset_ms(end, index, end_line)?,
                step_ms: parse_duration_ms(step, index, step_line)?,
            }
        }
        other => {
            return Err(err(
                index,
                kind_line,
                format!("unknown kind '{other}', expected 'instant' or 'range'"),
            ));
        }
    };

    let (mode, mode_line) = get("mode").ok_or_else(|| err(index, last_line, "missing 'mode'"))?;
    let mode = match mode {
        "unordered" => ComparisonMode::Unordered,
        "ordered" => ComparisonMode::Ordered,
        "error" => ComparisonMode::ExpectError,
        other => {
            return Err(err(
                index,
                mode_line,
                format!("unknown mode '{other}', expected 'unordered', 'ordered', or 'error'"),
            ));
        }
    };

    let tolerance_ulps = match get("tolerance") {
        Some((value, line)) => Some(value.parse::<u32>().map_err(|_| {
            err(
                index,
                line,
                format!("invalid tolerance '{value}', expected a non-negative integer"),
            )
        })?),
        None => None,
    };

    Ok(CorpusEntry {
        name: name.to_string(),
        query: query.to_string(),
        eval,
        mode,
        tolerance_ulps,
    })
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn parses_an_instant_entry() {
        let entries =
            parse_corpus("name: simple\nquery: up\nkind: instant\ntime: +30s\nmode: unordered\n")
                .expect("parse");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "simple");
        assert_eq!(entries[0].query, "up");
        match entries[0].eval {
            Eval::Instant { time_offset_ms } => assert_eq!(time_offset_ms, 30_000),
            _ => panic!("expected instant"),
        }
        assert_eq!(entries[0].mode, ComparisonMode::Unordered);
        assert_eq!(entries[0].tolerance_ulps, None);
    }

    #[test]
    fn parses_an_optional_tolerance_field() {
        let entries = parse_corpus(
            "name: simple\nquery: up\nkind: instant\ntime: +30s\nmode: unordered\ntolerance: 256\n",
        )
        .expect("parse");
        assert_eq!(entries[0].tolerance_ulps, Some(256));
    }

    #[test]
    fn rejects_a_non_integer_tolerance() {
        let err = parse_corpus(
            "name: simple\nquery: up\nkind: instant\ntime: +30s\nmode: unordered\ntolerance: not-a-number\n",
        )
        .expect_err("must reject a non-integer tolerance");
        assert!(matches!(err, CorpusError::Malformed { .. }));
    }

    #[test]
    fn parses_a_negative_offset_and_range_entry() {
        let entries = parse_corpus(
            "name: r\nquery: up[5m]\nkind: range\nstart: -300s\nend: +0s\nstep: 30s\nmode: ordered\n",
        )
        .expect("parse");
        match entries[0].eval {
            Eval::Range {
                start_offset_ms,
                end_offset_ms,
                step_ms,
            } => {
                assert_eq!(start_offset_ms, -300_000);
                assert_eq!(end_offset_ms, 0);
                assert_eq!(step_ms, 30_000);
            }
            _ => panic!("expected range"),
        }
    }

    #[test]
    fn parses_multiple_entries_and_skips_comments() {
        let text = "\
# a comment
name: first
query: up
kind: instant
time: +0s
mode: unordered

# another comment
name: second
query: down
kind: instant
time: +0s
mode: error
";
        let entries = parse_corpus(text).expect("parse");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].name, "first");
        assert_eq!(entries[1].name, "second");
        assert_eq!(entries[1].mode, ComparisonMode::ExpectError);
    }

    #[test]
    fn missing_field_is_a_typed_error_not_a_panic() {
        let err = parse_corpus("name: broken\nquery: up\nkind: instant\nmode: unordered\n")
            .expect_err("must fail");
        assert!(matches!(err, CorpusError::Malformed { .. }));
    }

    #[test]
    fn unknown_kind_is_a_typed_error() {
        let err =
            parse_corpus("name: broken\nquery: up\nkind: nonsense\ntime: +0s\nmode: unordered\n")
                .expect_err("must fail");
        assert!(matches!(err, CorpusError::Malformed { .. }));
    }

    #[test]
    fn binop_corpus_file_parses_cleanly() {
        let entries =
            parse_corpus(include_str!("../corpus/binop.txt")).expect("parse binop corpus");
        assert!(entries.len() >= 20);
        assert!(
            entries
                .iter()
                .any(|e| e.mode == ComparisonMode::ExpectError)
        );
    }

    #[test]
    fn histogram_native_corpus_file_parses_cleanly() {
        let entries = parse_corpus(include_str!("../corpus/histogram_native.txt"))
            .expect("parse histogram_native corpus");
        assert!(entries.len() >= 8);
        assert!(
            entries
                .iter()
                .any(|e| e.query.starts_with("histogram_count"))
        );
        assert!(entries.iter().any(|e| e.query.contains("rate(")));
    }

    #[test]
    fn aggregate_corpus_file_parses_cleanly() {
        let entries =
            parse_corpus(include_str!("../corpus/aggregate.txt")).expect("parse aggregate corpus");
        assert!(entries.len() >= 15);
        // No entry here uses `mode: ordered` (issue #177 moved the two
        // topk/bottomk tie-boundary entries to `unordered`: this dataset's
        // only values produce a tie at both the top and bottom, so there is
        // no tie-free query left to assert a specific rank order against;
        // rank-order correctness itself stays covered by aggregate.rs's own
        // unit tests, e.g. topk_returns_original_series_identity_in_
        // descending_order).
        assert!(entries.iter().any(|e| e.mode == ComparisonMode::Unordered));
    }
}
