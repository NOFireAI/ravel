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
use ravel_server::config::{Cli, DERIVED_MAX_SEGMENTS, DERIVED_QUERY_DEADLINE};

const NS_PER_HOUR: i64 = 3_600 * 1_000_000_000;

/// The prose that introduces the engine's query deadline, in both the section
/// body and the erasure table.
const DEADLINE_ANCHOR: &str = "the query deadline, the engine's enforced query timeout, ";

/// The prose that introduces the GC protection budget the deadline fits under,
/// in the same two places.
const CEILING_ANCHOR: &str = "`max_query_duration`, the GC protection budget it fits under, ";

/// The prose that introduces the sealed-set fan-out cap.
const SEGMENTS_ANCHOR: &str = "`max_segments` (default ";

/// The prose in `docs/query-engine.md` that introduces the derived per-query
/// S3 request budget. A stock server's figure follows it directly.
const REQUEST_BUDGET_ANCHOR: &str = "the derived default is ";

/// The prose in `docs/guides/ingest.md` that introduces the abandonment budget
/// a buffered flush gets for its own store calls. The figure follows it
/// directly, in seconds, the unit the constant is declared in.
const INGEST_LIFETIME_ANCHOR: &str =
    "`max_flush_lifetime`, the budget for those store calls, defaults to ";

/// The sentence `docs/guides/ingest.md` used to bound buffered-mode loss with,
/// byte for byte. It named the flush-trigger cadence as the loss bound and a
/// crash as the only way to lose an acked row, which the consistency model
/// contradicts on both counts: the trigger delay does not bound when a flush
/// completes, and an abandoned flush drops already-acked rows with no crash.
const INGEST_STALE_LOSS_SENTENCE: &str = "A crash between the ack and the next flush loses that buffered window, \
     bounded by `max_flush_delay` (2s default).";

/// The sentence `README.md` used to close its buffered paragraph with, byte for
/// byte. A clean shutdown draining the window does not make loss specific to a
/// crash: a flush whose store calls exhaust `max_flush_lifetime` is abandoned
/// and its already-acked rows are dropped while the process keeps running.
const README_STALE_SHUTDOWN_SENTENCE: &str =
    "A clean shutdown drains the window, so the loss is specific to a crash.";

/// The clause that bounded the buffered window by the flush delay, which only
/// starts a flush and never ends one.
const README_STALE_BOUND_CLAUSE: &str = "bounded by the maximum flush delay";

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

/// `docs/query-engine.md`, resolved from this crate's manifest directory the
/// same way [`doc_path`] resolves the consistency model.
fn query_engine_doc_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("docs")
        .join("query-engine.md")
}

/// `docs/guides/ingest.md`, resolved from this crate's manifest directory the
/// same way [`doc_path`] resolves the consistency model.
fn ingest_guide_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("docs")
        .join("guides")
        .join("ingest.md")
}

/// `README.md`, resolved from this crate's manifest directory the same way
/// [`doc_path`] resolves the consistency model.
fn readme_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("README.md")
}

/// The document with every run of whitespace collapsed to a single space, so a
/// claim the 80-column wrap split across two lines still matches one anchor.
fn normalized_doc() -> String {
    normalized(&doc_path())
}

/// The whitespace-collapsed form of `docs/query-engine.md`, matching
/// [`normalized_doc`].
fn normalized_query_engine_doc() -> String {
    normalized(&query_engine_doc_path())
}

/// The whitespace-collapsed form of `docs/guides/ingest.md`, matching
/// [`normalized_doc`].
fn normalized_ingest_guide() -> String {
    normalized(&ingest_guide_path())
}

/// The whitespace-collapsed form of `README.md`, matching [`normalized_doc`].
fn normalized_readme() -> String {
    normalized(&readme_path())
}

/// Read `path` and collapse every run of whitespace to a single space.
fn normalized(path: &Path) -> String {
    let text =
        std::fs::read_to_string(path).unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The figure and its unit word immediately after every occurrence of
/// `anchor`. One entry per occurrence, so a caller can require the count it
/// expects instead of trusting the first hit. Thousands separators are
/// accepted and stripped; the unit is the alphabetic run after the digits, and
/// is empty when a delimiter follows them directly.
///
/// `doc_name` is the document `doc` was read from, so the panic below names the
/// file whose anchor lost its figure rather than one fixed document.
fn figures_after(doc: &str, doc_name: &str, anchor: &str) -> Vec<(u64, String)> {
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
                    "{doc_name} states no figure after {anchor:?}: \
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
    let figures = figures_after(&doc, "docs/consistency-model.md", DEADLINE_ANCHOR);
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
    let figures = figures_after(&doc, "docs/consistency-model.md", CEILING_ANCHOR);
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
    let figures = figures_after(&doc, "docs/consistency-model.md", SEGMENTS_ANCHOR);
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

/// The per-query S3 request budget `docs/query-engine.md` states as the
/// derived default is the budget a stock server enforces through
/// `resolve_max_s3_requests`, which derives it with `derive_max_s3_requests`
/// from the default `--shards` and the default ingest flush cadence. The
/// figure is computed here from that derivation, never restated, so a default
/// or a derivation change fails this rather than leaving the doc claiming
/// 48,200 while a stock server derives 15,800.
#[test]
fn request_budget_figure_matches_the_derived_default() {
    use clap::Parser;

    let doc = normalized_query_engine_doc();
    let figures = figures_after(&doc, "docs/query-engine.md", REQUEST_BUDGET_ANCHOR);
    assert_eq!(
        figures.len(),
        1,
        "docs/query-engine.md must state the derived S3 request budget exactly \
         once, using the phrase {REQUEST_BUDGET_ANCHOR:?}; found {} occurrence(s)",
        figures.len()
    );

    let cli = Cli::try_parse_from(["ravel-server"]).expect("server defaults parse");
    assert_eq!(
        cli.shards, 4,
        "guards the shard default the documented figure is derived at"
    );
    let flush = ravel_ingest::IngestConfig::default().max_flush_delay;
    let expected = ravel_query::derive_max_s3_requests(cli.shards, flush);

    // The figure must be exactly what a stock server enforces through the real
    // resolve path, not just what the standalone derivation returns.
    assert_eq!(
        cli.resolve_max_s3_requests()
            .expect("server defaults resolve a bounded budget"),
        ravel_query::RequestLimit::Bounded(expected),
        "the budget the running server enforces must match derive_max_s3_requests"
    );

    let (value, _unit) = &figures[0];
    assert_eq!(
        *value, expected,
        "docs/query-engine.md states a derived S3 request budget of {value} but \
         ravel-server derives derive_max_s3_requests({}, {flush:?}) = {expected}. \
         An operator sizes a query tier from that figure, so it must track the \
         derivation.",
        cli.shards
    );

    // The rendering is pinned too, so the figure keeps its comma grouping
    // instead of drifting to an ungrouped digit run.
    let rendered = format!(
        "{REQUEST_BUDGET_ANCHOR}{}",
        grouped(usize::try_from(expected).expect("the derived budget fits in usize"))
    );
    assert!(
        doc.contains(&rendered),
        "docs/query-engine.md must render the derived S3 request budget with \
         comma thousands separators, as {rendered:?}"
    );
}

/// The buffered-mode loss bound `docs/guides/ingest.md` gives a reader is the
/// one `docs/consistency-model.md` is normative for. Two claims are pinned.
/// The guide no longer bounds the loss by the flush-trigger cadence and no
/// longer reads as if only a crash can lose an acked row, so the sentence that
/// said both is gone byte for byte. And the budget it puts in its place is the
/// real one: `max_flush_lifetime`, read from the constant rather than restated,
/// which is what an abandoned flush's own store calls have to finish inside.
#[test]
fn buffered_mode_loss_bound_in_the_ingest_guide_matches_the_consistency_model() {
    let doc = normalized_ingest_guide();
    assert!(
        doc.contains("## Strict vs. buffered acknowledgement"),
        "docs/guides/ingest.md is missing the acknowledgement-mode section the \
         buffered loss bound lives in, so the path is wrong or the guide has \
         been restructured"
    );

    assert_eq!(
        doc.matches(INGEST_STALE_LOSS_SENTENCE).count(),
        0,
        "docs/guides/ingest.md still states {INGEST_STALE_LOSS_SENTENCE:?}. \
         max_flush_delay bounds when a flush is triggered, not when it \
         completes, and a crash is not the only way a buffered row is lost: an \
         abandoned flush drops already-acked rows with no crash at all."
    );

    let figures = figures_after(&doc, "docs/guides/ingest.md", INGEST_LIFETIME_ANCHOR);
    assert_eq!(
        figures.len(),
        1,
        "docs/guides/ingest.md must state the buffered abandonment budget \
         exactly once, using the phrase {INGEST_LIFETIME_ANCHOR:?}; found {} \
         occurrence(s)",
        figures.len()
    );

    let (value, unit) = &figures[0];
    let expected = ravel_ingest::IngestConfig::default()
        .max_flush_lifetime
        .as_secs();
    assert_eq!(
        unit, "s",
        "the abandonment budget in docs/guides/ingest.md must be stated in \
         seconds to match IngestConfig::default().max_flush_lifetime, not in \
         {unit:?}"
    );
    assert_eq!(
        *value, expected,
        "docs/guides/ingest.md states an abandonment budget of {value} s but \
         ravel-ingest defaults max_flush_lifetime to {expected} s. That figure \
         is how long a buffered flush's own store calls may take before its \
         already-acked rows are dropped, so the guide must track the constant."
    );
}

/// `README.md` does not tell a reader that a clean shutdown makes buffered loss
/// specific to a crash. It is not: a flush whose store calls exceed
/// `max_flush_lifetime` is abandoned and drops rows Ravel already acknowledged,
/// with every process still running.
#[test]
fn readme_does_not_claim_a_clean_shutdown_makes_buffered_loss_crash_only() {
    let readme = normalized_readme();
    assert!(
        readme.contains("Buffered acknowledgement is opt-in per request."),
        "README.md is missing the buffered acknowledgement paragraph, so the \
         path is wrong or the section has been restructured"
    );
    assert_eq!(
        readme.matches(README_STALE_SHUTDOWN_SENTENCE).count(),
        0,
        "README.md still states {README_STALE_SHUTDOWN_SENTENCE:?}. An \
         abandoned flush drops already-acked buffered rows without any crash, \
         so loss is not specific to one."
    );
    assert_eq!(
        readme.matches(README_STALE_BOUND_CLAUSE).count(),
        0,
        "README.md still says the buffered window is {README_STALE_BOUND_CLAUSE:?}. \
         The flush delay bounds when a flush is triggered, not when it completes."
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
