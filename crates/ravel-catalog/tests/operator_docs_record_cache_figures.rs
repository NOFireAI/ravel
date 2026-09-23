//! Pins the operator-facing record-cache figures in docs/guides/caching.md,
//! docs/guides/operations.md and docs/catalog-and-mvcc.md to the constants
//! in `crate::config` they are derived from (issue #1927): the per-tenant
//! capacity floor and cap, the per-entry byte rate, and the per-tenant byte
//! budget (both halves and combined). Each figure is computed here from the
//! constant, never copied from the doc text, so a constant change fails this
//! test in this file instead of surfacing later as stale prose.
//!
//! A figure that recurs in a doc is pinned by an occurrence count, not just
//! `contains`: `assert_doc_states` asserts that the bare quantity (e.g. "18
//! MB") appears exactly as many times in the doc as this test has needles
//! for it. `contains` alone lets any restatement this test does not name
//! drift silently, whether that is one of the copies below going stale or a
//! new paragraph nobody added a needle for; the count assertion fails loudly
//! in both directions and names the doc and the figure so the next author
//! knows to add (or remove) a needle here.
//!
//! Doc text is read from the repo at test time, the way
//! `shipped_rules_name_emitted_metrics.rs` reads the shipped alert rules,
//! rather than duplicated into this file. Whitespace in both the doc text
//! and the search needles is collapsed before matching, so a paragraph
//! rewrap that changes nothing semantic does not make this test flaky.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::fs;

const CACHING_GUIDE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/guides/caching.md");
const OPERATIONS_GUIDE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../docs/guides/operations.md"
);
const CATALOG_MVCC_DOC: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../docs/catalog-and-mvcc.md"
);

const CONSTANTS_NAMED: &str = "RECORD_CACHE_ENTRY_BYTES, RECORD_CACHES_PER_TENANT, \
     MAX_RECORD_CACHE_BYTES_PER_TENANT, DEFAULT_CACHE_CAPACITY_PER_TENANT and \
     MAX_CACHE_CAPACITY_PER_TENANT in crates/ravel-catalog/src/config.rs";

fn read(path: &str) -> String {
    fs::read_to_string(path).unwrap_or_else(|e| panic!("{path} must be readable: {e}"))
}

/// Collapses all whitespace runs to a single space, so a needle does not
/// break when Markdown rewraps a paragraph across a different line.
fn normalize(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Formats a count the way these docs write one: thousands separated by
/// commas.
fn with_commas(n: u64) -> String {
    let mut groups: Vec<String> = Vec::new();
    let mut rest = n;
    loop {
        groups.push(format!("{:03}", rest % 1000));
        rest /= 1000;
        if rest == 0 {
            break;
        }
    }
    groups.reverse();
    groups[0] = groups[0].trim_start_matches('0').to_string();
    if groups[0].is_empty() {
        groups[0] = "0".to_string();
    }
    groups.join(",")
}

/// Formats a byte count scaled down by `scale` (1e6 for MB, 1e9 for GB) with
/// at most one decimal place, trimming a trailing ".0". Formatting from the
/// byte count directly (rather than from an already-truncated MB integer)
/// means a future non-round constant prints the value an author would
/// actually write ("45.9 MB"), not one truncated to "45 MB".
fn format_scaled(bytes: u64, scale: f64) -> String {
    let value = bytes as f64 / scale;
    let rounded = (value * 10.0).round() / 10.0;
    if (rounded - rounded.trunc()).abs() < 1e-9 {
        format!("{:.0}", rounded)
    } else {
        format!("{:.1}", rounded)
    }
}

fn format_mb(bytes: u64) -> String {
    format_scaled(bytes, 1_000_000.0)
}

fn format_gb(bytes: u64) -> String {
    format_scaled(bytes, 1_000_000_000.0)
}

/// Counts non-overlapping occurrences of `marker` in `haystack` that do not
/// continue a longer number, so "45 MB" does not also match inside "145 MB"
/// and "10,000" does not match inside "110,000" or "1.10,000".
///
/// `.` and `,` are rejected alongside a digit because both are digit context
/// in these docs: a thousands separator and a decimal point. No current
/// marker collides that way, so this is a guard against the next one.
fn count_figure(haystack: &str, marker: &str) -> usize {
    let bytes = haystack.as_bytes();
    let mut count = 0;
    let mut start = 0;
    while let Some(pos) = haystack[start..].find(marker) {
        let idx = start + pos;
        let continues_a_number = idx > 0
            && (bytes[idx - 1].is_ascii_digit()
                || bytes[idx - 1] == b'.'
                || bytes[idx - 1] == b',');
        if !continues_a_number {
            count += 1;
        }
        start = idx + marker.len().max(1);
    }
    count
}

/// [`count_figure`] over a haystack with `exclusions` removed first, for a
/// figure a doc also states in a sentence that must NOT track the constant.
/// `docs/guides/caching.md` has exactly one: "the old cache held a flat
/// 10,000 records", a historical value describing the cache this one
/// replaced. Counting it would make the entry-count assertion demand that
/// history change whenever the constant does.
fn count_figure_excluding(haystack: &str, marker: &str, exclusions: &[&str]) -> usize {
    let mut scanned = haystack.to_string();
    for phrase in exclusions {
        assert!(
            scanned.contains(phrase),
            "the exclusion {phrase:?} is not in the doc, so it excludes nothing and \
             the count below silently changed meaning; update or drop it"
        );
        scanned = scanned.replace(phrase, "");
    }
    count_figure(&scanned, marker)
}

/// Asserts that `needle` (an exact restatement of a figure) is present in
/// the doc, AND that the bare `marker` for that figure (e.g. "18 MB") occurs
/// exactly `expected_count` times in the whole doc -- the count this test
/// accounts for with needles. A mismatch means either an existing
/// restatement drifted off the constant-derived value (count too low) or a
/// restatement was added/removed without updating this test (count differs
/// from the number of needles below).
fn assert_doc_states(
    doc_path: &str,
    normalized_doc: &str,
    needle: &str,
    marker: &str,
    expected_count: usize,
    quantity: &str,
) {
    assert!(
        normalized_doc.contains(needle),
        "{doc_path} must state {quantity} as {needle:?}, computed from the ravel-catalog \
         record-cache constants ({CONSTANTS_NAMED}); the doc text has drifted from the \
         constants it describes"
    );
    let actual = count_figure(normalized_doc, marker);
    assert_eq!(
        actual, expected_count,
        "{doc_path} mentions {quantity} (matched via {marker:?}) {actual} time(s), but this \
         test only has needles for {expected_count}; a restatement of this figure was added or \
         removed without a matching needle in \
         crates/ravel-catalog/tests/operator_docs_record_cache_figures.rs -- add (or remove) a \
         needle here so every occurrence stays pinned to the ravel-catalog record-cache \
         constants ({CONSTANTS_NAMED})"
    );
}

#[test]
fn record_cache_figures_match_the_constants_they_are_derived_from() {
    let floor =
        u64::try_from(ravel_catalog::DEFAULT_CACHE_CAPACITY_PER_TENANT).expect("floor fits u64");
    let entry_bytes = ravel_catalog::RECORD_CACHE_ENTRY_BYTES;
    let caches_per_tenant = ravel_catalog::RECORD_CACHES_PER_TENANT;
    let budget = ravel_catalog::MAX_RECORD_CACHE_BYTES_PER_TENANT;

    // Deliverable 4: derive the entry cap from the constants here, rather
    // than restating MAX_CACHE_CAPACITY_PER_TENANT, so a change to either
    // input this test reads fails it even if config.rs's own constant
    // definition were edited to match.
    let derived_cap = budget / (entry_bytes * caches_per_tenant);
    assert_eq!(
        derived_cap,
        u64::try_from(ravel_catalog::MAX_CACHE_CAPACITY_PER_TENANT).expect("cap fits u64"),
        "MAX_CACHE_CAPACITY_PER_TENANT must equal MAX_RECORD_CACHE_BYTES_PER_TENANT / \
         (RECORD_CACHE_ENTRY_BYTES * RECORD_CACHES_PER_TENANT)"
    );

    let floor_str = with_commas(floor);
    let cap_str = with_commas(derived_cap);

    let budget_mb = format_mb(budget);
    let half_budget_bytes = budget / caches_per_tenant;
    let half_budget_mb = format_mb(half_budget_bytes);
    let floor_share_bytes = floor * entry_bytes;
    let floor_share_mb = format_mb(floor_share_bytes);
    let floor_total_bytes = floor_share_bytes * caches_per_tenant;
    let floor_total_mb = format_mb(floor_total_bytes);
    let hundred_tenant_bytes = budget * 100;
    let hundred_tenant_gb = format_gb(hundred_tenant_bytes);

    let marker_budget = format!("{budget_mb} MB");
    let marker_half_budget = format!("{half_budget_mb} MB");
    let marker_floor_share = format!("{floor_share_mb} MB");
    let marker_floor_total = format!("{floor_total_mb} MB");
    let marker_hundred_gb = format!("{hundred_tenant_gb} GB");

    // ---- docs/guides/caching.md ----
    let caching = normalize(&read(CACHING_GUIDE));

    // The entry counts get the same occurrence protection as the MB and GB
    // markers. Without it a restatement in different words is unpinned: the
    // whole-sentence needles below matched three of the four places this guide
    // states the floor, and the fourth could drift to a different number with
    // this test still green -- the partial-update drift #1904 produced.
    //
    // "the old cache held a flat 10,000 records" is excluded: it describes the
    // cache this one replaced, so it must NOT track the constant.
    const HISTORICAL_FLOOR_SENTENCE: &str = "the old cache held a flat 10,000 records";
    let floor_mentions = count_figure_excluding(&caching, &floor_str, &[HISTORICAL_FLOOR_SENTENCE]);
    assert_eq!(
        floor_mentions, 3,
        "docs/guides/caching.md states the capacity floor {floor_mentions} time(s) \
         (excluding the historical sentence), but this test accounts for 3 with \
         needles; add or remove a needle so every live restatement stays pinned \
         to DEFAULT_CACHE_CAPACITY_PER_TENANT"
    );
    let cap_mentions = count_figure(&caching, &cap_str);
    assert_eq!(
        cap_mentions, 1,
        "docs/guides/caching.md states the capacity cap {cap_mentions} time(s), but \
         this test accounts for 1 with a needle; add or remove a needle so every \
         restatement stays pinned to the derived cap"
    );

    assert_doc_states(
        CACHING_GUIDE,
        &caching,
        &format!("floored at {floor_str} entries and capped at {cap_str}."),
        &format!("floored at {floor_str} entries and capped at {cap_str}."),
        1,
        "the record-cache capacity floor and cap",
    );
    assert!(
        caching.contains(&format!(
            "at the {floor_str}-entry floor rather than the derived value"
        )),
        "docs/guides/caching.md must state the disabled-cache floor as \
         \"at the {floor_str}-entry floor rather than the derived value\", computed \
         from DEFAULT_CACHE_CAPACITY_PER_TENANT; this is the third live restatement \
         of the floor and the one the whole-sentence needles missed"
    );
    assert_doc_states(
        CACHING_GUIDE,
        &caching,
        &format!(
            "byte budget of `capacity x {entry_bytes} bytes`, {half_budget_mb} MB per tenant \
             at the cap"
        ),
        &marker_half_budget,
        1,
        "the per-entry byte rate and the per-cache byte share at the cap",
    );
    assert_doc_states(
        CACHING_GUIDE,
        &caching,
        &format!("The {entry_bytes} bytes is a PLANNING rate the capacity is derived against"),
        &format!("The {entry_bytes} bytes is a PLANNING rate the capacity is derived against"),
        1,
        "the per-entry planning rate",
    );
    assert_doc_states(
        CACHING_GUIDE,
        &caching,
        &format!("So the worst case is {budget_mb} MB per actively-queried tenant"),
        &marker_budget,
        2,
        "the per-tenant memory budget",
    );
    assert_doc_states(
        CACHING_GUIDE,
        &caching,
        &format!(
            "Budget it as {budget_mb} MB times the number of tenants queried concurrently: \
             100 of them is {hundred_tenant_gb} GB worst case,"
        ),
        &marker_budget,
        2,
        "the per-tenant memory budget (operator sizing line)",
    );
    assert_doc_states(
        CACHING_GUIDE,
        &caching,
        &format!(
            "Budget it as {budget_mb} MB times the number of tenants queried concurrently: \
             100 of them is {hundred_tenant_gb} GB worst case,"
        ),
        &marker_hundred_gb,
        1,
        "the 100-tenant sizing product",
    );
    assert_doc_states(
        CACHING_GUIDE,
        &caching,
        &format!(
            "held at their {floor_str}-entry floor rather than the derived capacity, about \
             {floor_total_mb} MB per actively-queried tenant."
        ),
        &marker_floor_total,
        3,
        "the byte budget at the disabled-cache floor (--disable-cache flag row)",
    );
    assert_doc_states(
        CACHING_GUIDE,
        &caching,
        &format!("cost about {floor_total_mb} MB per actively-queried tenant under it"),
        &marker_floor_total,
        3,
        "the byte budget at the disabled-cache floor (prose restatement)",
    );
    assert_doc_states(
        CACHING_GUIDE,
        &caching,
        &format!(
            "about {floor_total_mb} MB per actively-queried tenant: a {floor_share_mb} MB byte \
             budget for each of the two caches, both enforced."
        ),
        &marker_floor_total,
        3,
        "the byte budget per cache and combined at the disabled-cache floor",
    );
    assert_doc_states(
        CACHING_GUIDE,
        &caching,
        &format!(
            "about {floor_total_mb} MB per actively-queried tenant: a {floor_share_mb} MB byte \
             budget for each of the two caches, both enforced."
        ),
        &marker_floor_share,
        1,
        "the per-cache byte budget at the disabled-cache floor",
    );

    // ---- docs/guides/operations.md ----
    let operations = normalize(&read(OPERATIONS_GUIDE));
    let operations_needle = format!(
        "cost up to {budget_mb} MB per actively-queried tenant on top of the carved \
         read-cache shares. Both halves are enforced in bytes, {half_budget_mb} MB each"
    );
    assert_doc_states(
        OPERATIONS_GUIDE,
        &operations,
        &operations_needle,
        &marker_budget,
        1,
        "the per-tenant memory budget",
    );
    assert_doc_states(
        OPERATIONS_GUIDE,
        &operations,
        &operations_needle,
        &marker_half_budget,
        1,
        "the per-cache byte share",
    );

    // ---- docs/catalog-and-mvcc.md ----
    let catalog_mvcc = normalize(&read(CATALOG_MVCC_DOC));
    let catalog_mvcc_needle_cap = format!(
        "returns the {cap_str}-entry cap, so the budget is {half_budget_mb} MB per \
         actively-queried tenant; at the {floor_str}-entry floor (which is also what \
         `--disable-cache` holds) it is {floor_share_mb} MB."
    );
    assert_doc_states(
        CATALOG_MVCC_DOC,
        &catalog_mvcc,
        &catalog_mvcc_needle_cap,
        &marker_half_budget,
        2,
        "the derived cap and its byte budget",
    );
    assert_doc_states(
        CATALOG_MVCC_DOC,
        &catalog_mvcc,
        &catalog_mvcc_needle_cap,
        &marker_floor_share,
        1,
        "the floor and its byte budget",
    );
    let catalog_mvcc_needle_combined =
        format!("({half_budget_mb} MB each at the {cap_str}-entry cap, {budget_mb} MB together)");
    assert_doc_states(
        CATALOG_MVCC_DOC,
        &catalog_mvcc,
        &catalog_mvcc_needle_combined,
        &marker_half_budget,
        2,
        "the per-cache byte budget at the cap",
    );
    assert_doc_states(
        CATALOG_MVCC_DOC,
        &catalog_mvcc,
        &catalog_mvcc_needle_combined,
        &marker_budget,
        1,
        "the combined byte budget at the cap",
    );
}
