//! Drift check for the numeric defaults `docs/consistency-model.md` states
//! against the constants the server derives them from (issue #1311). Both
//! sides are read rather than restated: the code side is the constant itself,
//! the doc side is the figure parsed out of the sentence that makes the claim.
//! Editing either one alone fails here, which is the point. The document is
//! normative for acknowledgement, visibility, and erasure bounds, so a stale
//! figure in it is read as a guarantee: the query-deadline row was wrong by a
//! factor of 22 in the direction that under-sizes a DSAR drain window.
//!
//! Each claim is anchored on the exact prose that introduces its figure, and
//! every test asserts how many occurrences of its anchor the document must
//! have, so a rewrite that drops one of the two places a quantity is stated
//! fails rather than passing on the survivor.
#![allow(clippy::expect_used)]

use std::path::{Path, PathBuf};

use ravel_maintain::config::DEFAULT_MAX_QUERY_DURATION_NS;
use ravel_server::config::{DERIVED_MAX_SEGMENTS, DERIVED_QUERY_DEADLINE};

const NS_PER_HOUR: i64 = 3_600 * 1_000_000_000;

/// The prose that introduces the engine's query deadline, in both the section
/// body and the erasure table.
const DEADLINE_ANCHOR: &str = "the query deadline, the engine's enforced query timeout, ";

/// The prose that introduces the GC protection budget the deadline fits under,
/// in the same two places.
const CEILING_ANCHOR: &str = "`max_query_duration`, the GC protection budget it fits under, ";

/// The prose that introduces the sealed-set fan-out cap.
const SEGMENTS_ANCHOR: &str = "`max_segments` (default ";

/// `docs/consistency-model.md`, resolved from this crate's manifest directory
/// rather than the process working directory, which differs between a
/// crate-scoped `cargo test` and one run from the workspace root.
fn doc_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("docs")
        .join("consistency-model.md")
}

/// The document with every run of whitespace collapsed to a single space, so a
/// claim the 80-column wrap split across two lines still matches one anchor.
fn normalized_doc() -> String {
    let path = doc_path();
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The figure and its unit word immediately after every occurrence of
/// `anchor`. One entry per occurrence, so a caller can require the count it
/// expects instead of trusting the first hit. Thousands separators are
/// accepted and stripped; the unit is the alphabetic run after the digits, and
/// is empty when a delimiter follows them directly.
fn figures_after(doc: &str, anchor: &str) -> Vec<(u64, String)> {
    let mut found = Vec::new();
    let mut rest = doc;
    while let Some(at) = rest.find(anchor) {
        let tail = &rest[at + anchor.len()..];
        let digits: String = tail
            .chars()
            .take_while(|c| c.is_ascii_digit() || *c == ',' || *c == '_')
            .collect();
        let value = digits
            .replace([',', '_'], "")
            .parse::<u64>()
            .unwrap_or_else(|e| {
                panic!(
                    "docs/consistency-model.md states no figure after {anchor:?}: \
                     read {digits:?} ({e}). The anchor and the figure must stay \
                     adjacent, or this check silently stops covering the claim."
                )
            });
        let unit: String = tail[digits.len()..]
            .trim_start()
            .chars()
            .take_while(|c| c.is_ascii_alphabetic())
            .collect();
        found.push((value, unit));
        rest = tail;
    }
    found
}

/// `value` with comma thousands separators, the grouping the repository's own
/// prose uses for this magnitude.
fn grouped(value: usize) -> String {
    let digits = value.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, ch) in digits.char_indices() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

/// The query deadline the document states is the deadline
/// [`DERIVED_QUERY_DEADLINE`] gives an operator who sets no flag. Stated in
/// two places, and both must agree with the constant.
#[test]
fn query_deadline_figure_matches_the_derived_constant() {
    let doc = normalized_doc();
    let figures = figures_after(&doc, DEADLINE_ANCHOR);
    assert_eq!(
        figures.len(),
        2,
        "docs/consistency-model.md must state the query deadline in both the \
         erasure section body and the erasure table, using the phrase \
         {DEADLINE_ANCHOR:?}; found {} occurrence(s)",
        figures.len()
    );

    let seconds = DERIVED_QUERY_DEADLINE.as_secs();
    assert_eq!(
        seconds % 60,
        0,
        "DERIVED_QUERY_DEADLINE is {DERIVED_QUERY_DEADLINE:?}, no longer a \
         whole number of minutes, so the document cannot state it in minutes; \
         change the unit in both places and here together"
    );
    let expected = seconds / 60;

    for (value, unit) in &figures {
        assert_eq!(
            unit, "min",
            "the query deadline in docs/consistency-model.md must be stated in \
             minutes to match DERIVED_QUERY_DEADLINE, not in {unit:?}"
        );
        assert_eq!(
            *value, expected,
            "docs/consistency-model.md states a query deadline of {value} min \
             but ravel-server derives DERIVED_QUERY_DEADLINE = {expected} min. \
             A compliance owner sizes an erasure drain window from that table, \
             so the doc figure must track the constant."
        );
    }
}

/// The `max_query_duration` ceiling the same two places state is the `sys/gc`
/// default, and the deadline really does fit under it.
#[test]
fn max_query_duration_ceiling_figure_matches_the_gc_default() {
    let doc = normalized_doc();
    let figures = figures_after(&doc, CEILING_ANCHOR);
    assert_eq!(
        figures.len(),
        2,
        "docs/consistency-model.md must state the max_query_duration ceiling \
         beside the query deadline in both places, using the phrase \
         {CEILING_ANCHOR:?}; found {} occurrence(s)",
        figures.len()
    );

    assert_eq!(
        DEFAULT_MAX_QUERY_DURATION_NS % NS_PER_HOUR,
        0,
        "DEFAULT_MAX_QUERY_DURATION_NS is {DEFAULT_MAX_QUERY_DURATION_NS} ns, \
         no longer a whole number of hours, so the document cannot state it in \
         hours"
    );
    let expected = DEFAULT_MAX_QUERY_DURATION_NS / NS_PER_HOUR;

    for (value, unit) in &figures {
        assert_eq!(
            unit, "h",
            "the max_query_duration ceiling in docs/consistency-model.md must \
             be stated in hours to match DEFAULT_MAX_QUERY_DURATION_NS, not in \
             {unit:?}"
        );
        assert_eq!(
            i64::try_from(*value).expect("the stated ceiling fits in i64"),
            expected,
            "docs/consistency-model.md states a max_query_duration ceiling of \
             {value} h but ravel-maintain's DEFAULT_MAX_QUERY_DURATION_NS is \
             {expected} h"
        );
    }

    // The document says the deadline fits under the ceiling. That ordering is
    // what makes the GC protection budget cover a query the engine has not yet
    // cancelled, so it is a claim, not a restatement.
    let ceiling_ns =
        u128::try_from(DEFAULT_MAX_QUERY_DURATION_NS).expect("the default ceiling is positive");
    assert!(
        DERIVED_QUERY_DEADLINE.as_nanos() < ceiling_ns,
        "the derived query deadline {DERIVED_QUERY_DEADLINE:?} must fit under \
         the default max_query_duration of {DEFAULT_MAX_QUERY_DURATION_NS} ns, \
         which is what docs/consistency-model.md tells an operator"
    );
}

/// The sealed-set fan-out cap the document states is the one an unset
/// `--max-segments` reaches the engine as, [`DERIVED_MAX_SEGMENTS`], not the
/// engine crate's much smaller compiled-in `EngineConfig::default()`.
#[test]
fn max_segments_figure_matches_the_derived_constant() {
    let doc = normalized_doc();
    let figures = figures_after(&doc, SEGMENTS_ANCHOR);
    assert_eq!(
        figures.len(),
        1,
        "docs/consistency-model.md must state the max_segments default exactly \
         once, using the phrase {SEGMENTS_ANCHOR:?}; found {} occurrence(s)",
        figures.len()
    );

    let (value, _unit) = &figures[0];
    assert_eq!(
        usize::try_from(*value).expect("the stated cap fits in usize"),
        DERIVED_MAX_SEGMENTS,
        "docs/consistency-model.md states a max_segments default of {value} but \
         ravel-server derives DERIVED_MAX_SEGMENTS = {DERIVED_MAX_SEGMENTS}"
    );

    // The rendering is pinned too, so the figure stays readable at this
    // magnitude instead of drifting to an ungrouped digit run.
    let rendered = format!("{SEGMENTS_ANCHOR}{})", grouped(DERIVED_MAX_SEGMENTS));
    assert!(
        doc.contains(&rendered),
        "docs/consistency-model.md must render the max_segments default with \
         comma thousands separators, as {rendered:?}"
    );
}

/// The claim the anchors rest on: the document really was read, and the
/// erasure table that carries two of the three figures is still in it. A
/// missing file or a renamed section would otherwise make every assertion
/// above vacuous by never finding an anchor to check.
#[test]
fn the_document_and_its_erasure_table_are_present() {
    let doc = normalized_doc();
    assert!(
        doc.len() > 10_000,
        "docs/consistency-model.md read back as {} bytes, so the path is wrong \
         or the file was truncated",
        doc.len()
    );
    assert!(
        doc.contains("## Selective subject erasure"),
        "docs/consistency-model.md is missing the selective subject erasure \
         section that carries the erasure stage bounds table"
    );
    assert!(
        doc.contains("| Stage | Guarantee | Worst-case bound (defaults) |"),
        "docs/consistency-model.md is missing the erasure stage bounds table \
         header, so the table the deadline row lives in has been restructured"
    );
}
