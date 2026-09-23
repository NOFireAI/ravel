//! Pins the operator-facing record-cache figures in docs/guides/caching.md,
//! docs/guides/operations.md and docs/catalog-and-mvcc.md to the constants
//! in `crate::config` they are derived from (issue #1927): the per-tenant
//! capacity floor and cap, the per-entry byte rate, and the per-tenant byte
//! budget (both halves and combined). Each figure is computed here from the
//! constant, never copied from the doc text, so a constant change fails this
//! test in this file instead of surfacing later as stale prose.
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

fn assert_doc_states(doc_path: &str, normalized_doc: &str, needle: &str, quantity: &str) {
    assert!(
        normalized_doc.contains(needle),
        "{doc_path} must state {quantity} as {needle:?}, computed from the ravel-catalog \
         record-cache constants (RECORD_CACHE_ENTRY_BYTES, MAX_RECORD_CACHE_BYTES_PER_TENANT, \
         DEFAULT_CACHE_CAPACITY_PER_TENANT, MAX_CACHE_CAPACITY_PER_TENANT in \
         crates/ravel-catalog/src/config.rs); the doc text has drifted from the constants it \
         describes"
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
    let budget_mb = budget / 1_000_000;
    let half_budget_mb = budget as f64 / caches_per_tenant as f64 / 1_000_000.0;
    let floor_share_bytes = floor * entry_bytes;
    let floor_share_mb = floor_share_bytes / 1_000_000;
    let floor_total_mb = floor_share_mb * caches_per_tenant;

    // ---- docs/guides/caching.md ----
    let caching = normalize(&read(CACHING_GUIDE));
    assert_doc_states(
        CACHING_GUIDE,
        &caching,
        &format!("floored at {floor_str} entries and capped at {cap_str}."),
        "the record-cache capacity floor and cap",
    );
    assert_doc_states(
        CACHING_GUIDE,
        &caching,
        &format!(
            "byte budget of `capacity x {entry_bytes} bytes`, {half_budget_mb} MB per tenant \
             at the cap"
        ),
        "the per-entry byte rate and the per-cache byte share at the cap",
    );
    assert_doc_states(
        CACHING_GUIDE,
        &caching,
        &format!("The {entry_bytes} bytes is a PLANNING rate the capacity is derived against"),
        "the per-entry planning rate",
    );
    assert_doc_states(
        CACHING_GUIDE,
        &caching,
        &format!("So the worst case is {budget_mb} MB per actively-queried tenant"),
        "the per-tenant memory budget",
    );
    assert_doc_states(
        CACHING_GUIDE,
        &caching,
        &format!("Budget it as {budget_mb} MB times the number of tenants queried concurrently"),
        "the per-tenant memory budget (operator sizing line)",
    );
    assert_doc_states(
        CACHING_GUIDE,
        &caching,
        &format!(
            "about {floor_total_mb} MB per actively-queried tenant: a {floor_share_mb} MB byte \
             budget for each of the two caches, both enforced."
        ),
        "the byte budget per cache and combined at the disabled-cache floor",
    );

    // ---- docs/guides/operations.md ----
    let operations = normalize(&read(OPERATIONS_GUIDE));
    assert_doc_states(
        OPERATIONS_GUIDE,
        &operations,
        &format!(
            "cost up to {budget_mb} MB per actively-queried tenant on top of the carved \
             read-cache shares. Both halves are enforced in bytes, {half_budget_mb} MB each"
        ),
        "the per-tenant memory budget and the per-cache byte share",
    );

    // ---- docs/catalog-and-mvcc.md ----
    let catalog_mvcc = normalize(&read(CATALOG_MVCC_DOC));
    assert_doc_states(
        CATALOG_MVCC_DOC,
        &catalog_mvcc,
        &format!(
            "returns the {cap_str}-entry cap, so the budget is {half_budget_mb} MB per \
             actively-queried tenant; at the {floor_str}-entry floor (which is also what \
             `--disable-cache` holds) it is {floor_share_mb} MB."
        ),
        "the derived cap and its byte budget, and the floor and its byte budget",
    );
    assert_doc_states(
        CATALOG_MVCC_DOC,
        &catalog_mvcc,
        &format!("({half_budget_mb} MB each at the {cap_str}-entry cap, {budget_mb} MB together)"),
        "the per-cache and combined byte budgets at the cap",
    );
}
