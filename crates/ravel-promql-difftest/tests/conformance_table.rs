//! PromQL conformance scoring (ADR-0035).
//!
//! Unlike `difftest_selectors.rs`, none of this needs the pinned Prometheus
//! binary or `RAVEL_DIFFTEST=1`: every assertion here is about Ravel's own
//! behaviour, either a corpus entry answered by the in-process Ravel stack or
//! an intentionally-rejected construct refused by it. So the conformance gate
//! runs in a plain `cargo test -p ravel-promql-difftest`, in every
//! environment, and the generated table in `docs/query-engine.md` is
//! reproducible without network egress.
//!
//! Regenerate the table with:
//!
//! ```sh
//! RAVEL_UPDATE_CONFORMANCE_TABLE=1 \
//!   cargo test -p ravel-promql-difftest --test conformance_table
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::path::{Path, PathBuf};

use ravel_promql_difftest::corpus::{CorpusEntry, parse_corpus};
use ravel_promql_difftest::generator::{DatasetConfig, generate};
use ravel_promql_difftest::ravel_stack::RavelStack;
use ravel_promql_difftest::scoring::{
    BEGIN_MARKER, ConformanceReport, ConstructState, CorpusFile, END_MARKER, REGISTRY,
    REJECTION_CASES, RejectionEval, TYPED_ERROR_TAGS, TypedErrorTag, UNIT_TEST_PREFIX,
    declared_typed_error, scan_test_fn_names, splice_generated_block,
};
use ravel_types::TenantId;
use serde_json::Value as Json;

/// Every corpus file, with the path a registry row cites. Adding a corpus
/// file here is what makes its entries count as evidence.
const CORPUS_FILES: &[(&str, &str)] = &[
    (
        "corpus/selectors.txt",
        include_str!("../corpus/selectors.txt"),
    ),
    ("corpus/errors.txt", include_str!("../corpus/errors.txt")),
    ("corpus/rate.txt", include_str!("../corpus/rate.txt")),
    (
        "corpus/transform.txt",
        include_str!("../corpus/transform.txt"),
    ),
    ("corpus/binop.txt", include_str!("../corpus/binop.txt")),
    (
        "corpus/aggregate.txt",
        include_str!("../corpus/aggregate.txt"),
    ),
    (
        "corpus/over_time.txt",
        include_str!("../corpus/over_time.txt"),
    ),
    (
        "corpus/subquery.txt",
        include_str!("../corpus/subquery.txt"),
    ),
    (
        "corpus/histogram_classic.txt",
        include_str!("../corpus/histogram_classic.txt"),
    ),
    (
        "corpus/histogram_native.txt",
        include_str!("../corpus/histogram_native.txt"),
    ),
];

/// Environment variable that switches the doc check into a rewrite.
const UPDATE_ENV: &str = "RAVEL_UPDATE_CONFORMANCE_TABLE";

#[allow(clippy::expect_used)]
fn parsed_corpus() -> Vec<(&'static str, Vec<CorpusEntry>)> {
    CORPUS_FILES
        .iter()
        .map(|(path, text)| {
            let entries = parse_corpus(text).expect("every corpus file must parse");
            (*path, entries)
        })
        .collect()
}

fn corpus_files<'a>(parsed: &'a [(&'static str, Vec<CorpusEntry>)]) -> Vec<CorpusFile<'a>> {
    parsed
        .iter()
        .map(|(path, entries)| CorpusFile {
            path,
            entries: entries.as_slice(),
        })
        .collect()
}

/// `docs/query-engine.md`, resolved from this crate's manifest directory
/// rather than the process' working directory (which differs between `cargo
/// test` and a `cargo test` run from the workspace root).
#[allow(clippy::expect_used)]
fn query_engine_doc() -> PathBuf {
    let manifest = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is always set");
    Path::new(&manifest)
        .join("..")
        .join("..")
        .join("docs")
        .join("query-engine.md")
}

/// The `ravel-promql` crate directory, whose test function names are the
/// evidence a `ravel-promql:<fn>` citation resolves against.
#[allow(clippy::expect_used)]
fn ravel_promql_crate() -> PathBuf {
    let manifest = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is always set");
    Path::new(&manifest).join("..").join("ravel-promql")
}

/// Every test function name the `ravel-promql` crate defines.
#[allow(clippy::expect_used)]
fn ravel_promql_test_names() -> BTreeSet<String> {
    scan_test_fn_names(&ravel_promql_crate()).expect("scanning ravel-promql for test names")
}

/// ADR-0035 decision: "each construct gets exactly one state". A Rust enum
/// gives that for free per row; what this test proves is the part an enum
/// cannot, that the table has no duplicate row silently carrying a second
/// state for the same construct, and that the state-2 verification cases map
/// onto real rows in both directions.
#[test]
#[allow(clippy::expect_used)]
fn every_registry_entry_has_exactly_one_state() {
    ConformanceReport::validate_registry().expect("registry must be well formed");

    let mut seen: BTreeSet<&str> = BTreeSet::new();
    for construct in REGISTRY {
        assert!(
            seen.insert(construct.name),
            "construct '{}' appears more than once, so it carries more than \
             one state",
            construct.name
        );
    }

    // Every intentionally-rejected construct needs a case proving the
    // rejection, and every case needs a construct: an unpaired case would
    // test nothing and an unpaired construct would publish an unverified
    // state-2 claim.
    let rejected: BTreeSet<&str> = REGISTRY
        .iter()
        .filter(|c| matches!(c.state, ConstructState::IntentionallyRejected { .. }))
        .map(|c| c.name)
        .collect();
    let covered: BTreeSet<&str> = REJECTION_CASES.iter().map(|c| c.construct).collect();
    let uncovered: Vec<&&str> = rejected.difference(&covered).collect();
    assert!(
        uncovered.is_empty(),
        "intentionally-rejected constructs with no rejection case: {uncovered:?}"
    );
    let stray: Vec<&&str> = covered.difference(&rejected).collect();
    assert!(
        stray.is_empty(),
        "rejection cases for constructs that are not intentionally rejected: \
         {stray:?}"
    );

    // Every `Supported` row must cite a test in one of the two recognized
    // forms, or the published-state derivation cannot check the citation.
    for construct in REGISTRY {
        if let ConstructState::Supported { test } = construct.state {
            assert!(
                test.starts_with("corpus/") || test.starts_with("ravel-promql:"),
                "construct '{}' cites '{test}', which is neither a corpus \
                 file nor a ravel-promql unit test",
                construct.name
            );
        }
    }

    // ADR-0035 limits hand-written prose in the table to state-2 rows and the
    // accepted-divergence records; a note anywhere else would be prose the
    // generator cannot verify.
    for construct in REGISTRY {
        if construct.note.is_empty() {
            continue;
        }
        assert!(
            matches!(
                construct.state,
                ConstructState::IntentionallyRejected { .. }
                    | ConstructState::AcceptedDivergence { .. }
                    | ConstructState::Unclassified
            ),
            "construct '{}' carries a hand-written note but is neither \
             rejected, an accepted divergence, nor unclassified",
            construct.name
        );
    }
}

/// The acceptance test for this work, and the point of the whole table per
/// ADR-0035's Consequences: a construct marked "intentionally unsupported"
/// that actually panics, or answers with wrong data instead of a typed error,
/// is a bug the table must catch.
///
/// "Never a panic" is enforced structurally: a panic anywhere in Ravel's
/// evaluator during these queries aborts this test.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::expect_used)]
async fn intentionally_rejected_constructs_return_typed_errors() {
    let results = run_rejection_cases().await;
    let failures: Vec<&RejectionResult> = results.iter().filter(|r| !r.ok).collect();
    assert!(
        failures.is_empty(),
        "intentionally-rejected constructs that did not reject cleanly:\n{}",
        failures
            .iter()
            .map(|r| format!("- {} ({}): {}", r.construct, r.query, r.detail))
            .collect::<Vec<_>>()
            .join("\n")
    );
    assert_eq!(
        results.len(),
        REJECTION_CASES.len(),
        "every rejection case must have been exercised"
    );
}

/// The rejection check compares the registry's *declared*
/// status and `errorType` against the observed response, so a construct that
/// regresses from one typed-error variant to another is caught.
///
/// The synthetic regression is a deliberately wrong declared typed error fed
/// against a real, well-formed 422 `execution` rejection: the shape is
/// impeccable and only the identity is wrong, which is exactly what the old
/// check could not see.
#[test]
fn a_rejection_that_does_not_match_the_declared_typed_error_is_flagged() {
    let body = serde_json::json!({
        "status": "error",
        "errorType": "execution",
        "error": "Unsupported: histogram_stddev",
    });
    let declared = TypedErrorTag {
        status: 422,
        error_type: "execution",
    };

    // Control: the observed rejection matches what the row declares.
    let ok = check_rejection(
        "histogram_stddev",
        "histogram_stddev(diff_native_hist)",
        "histogram_stddev",
        declared,
        422,
        &body,
    );
    assert!(ok.ok, "{}", ok.detail);

    // The same rejection against a row declaring 500 internal: a regression
    // from one typed-error variant to another.
    let wrong_tag = check_rejection(
        "histogram_stddev",
        "histogram_stddev(diff_native_hist)",
        "histogram_stddev",
        TypedErrorTag {
            status: 500,
            error_type: "internal",
        },
        422,
        &body,
    );
    assert!(!wrong_tag.ok);
    assert!(
        wrong_tag.detail.contains("is not the declared 'internal'")
            && wrong_tag
                .detail
                .contains("HTTP 422 is not the declared 500"),
        "detail was '{}'",
        wrong_tag.detail
    );

    // Status alone regressing is caught too: same errorType, 500 instead of
    // the declared 422.
    let wrong_status = check_rejection(
        "histogram_stddev",
        "histogram_stddev(diff_native_hist)",
        "histogram_stddev",
        declared,
        500,
        &body,
    );
    assert!(!wrong_status.ok);
    assert!(
        wrong_status
            .detail
            .contains("HTTP 500 is not the declared 422"),
        "detail was '{}'",
        wrong_status.detail
    );
}

/// Every rejection case's construct declares a typed error the check can
/// compare against, and every declared tag is a stable one.
#[test]
#[allow(clippy::expect_used)]
fn every_rejection_case_has_a_declared_typed_error_to_compare_against() {
    for case in REJECTION_CASES {
        let tag = declared_typed_error(case.construct)
            .unwrap_or_else(|e| panic!("{}: {e}", case.construct));
        assert!(
            TYPED_ERROR_TAGS.contains(&tag.error_type),
            "{} declares errorType '{}', which the HTTP layer never emits",
            case.construct,
            tag.error_type
        );
    }
}

/// A `ravel-promql:<fn>` citation is only evidence while
/// that test exists, so the scan must find every cited name in the real crate.
#[test]
fn every_cited_ravel_promql_unit_test_exists() {
    let names = ravel_promql_test_names();
    assert!(
        names.len() > 50,
        "the scan found only {} test names in ravel-promql, so it did not run",
        names.len()
    );
    let missing: Vec<&str> = REGISTRY
        .iter()
        .filter_map(|c| match c.state {
            ConstructState::Supported { test } => test.strip_prefix(UNIT_TEST_PREFIX),
            _ => None,
        })
        .filter(|unit_test| !names.contains(*unit_test))
        .collect();
    assert!(
        missing.is_empty(),
        "registry rows cite `ravel-promql` unit tests that no longer exist: \
         {missing:?}"
    );
}

/// The generated table in `docs/query-engine.md` must match what a run
/// produces. Because the table is generated, a regression from supported to
/// unclassified shows up as a diff in the same change that caused it, and
/// cannot be edited away in prose (ADR-0035 Consequences).
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::expect_used)]
async fn query_engine_doc_table_matches_a_real_run() {
    let report = build_report().await;
    let block = report.to_markdown();

    let path = query_engine_doc();
    let doc = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
    let updated = splice_generated_block(&doc, &block).expect("doc must carry the table markers");

    if env::var(UPDATE_ENV).as_deref() == Ok("1") {
        if updated != doc {
            std::fs::write(&path, &updated)
                .unwrap_or_else(|e| panic!("writing {}: {e}", path.display()));
            eprintln!("rewrote the generated table in {}", path.display());
        }
        return;
    }

    assert_eq!(
        updated, doc,
        "the generated PromQL conformance table in docs/query-engine.md is \
         stale. Regenerate it with:\n  {UPDATE_ENV}=1 cargo test -p \
         ravel-promql-difftest --test conformance_table\nMarkers: \
         {BEGIN_MARKER} .. {END_MARKER}"
    );
}

/// The score itself, printed so a reader of the test output sees the same
/// numbers the doc publishes, plus the state-3 bucket spelled out. Every row
/// here is a ticket, per ADR-0035's one-time-triage decision.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::expect_used)]
async fn the_unclassified_bucket_is_reported_not_hidden() {
    let report = build_report().await;
    let counts = report.counts();
    eprintln!(
        "PromQL conformance: {} supported, {} intentionally rejected, {} \
         accepted divergence, {} unclassified ({} of {} = {}%)",
        counts.supported,
        counts.intentionally_rejected,
        counts.accepted_divergence,
        counts.unclassified,
        counts.conformant(),
        counts.total(),
        counts.score_percent()
    );
    for outcome in report.unclassified() {
        eprintln!(
            "  unclassified: {} ({})",
            outcome.construct.name, outcome.construct.category
        );
    }
    assert_eq!(
        counts.total(),
        REGISTRY.len(),
        "every registry row must be classified exactly once"
    );
    // The corpus must actually have run: an empty run would publish an
    // all-unclassified table that technically satisfies every other
    // assertion here.
    assert!(
        counts.supported > 0,
        "no construct came out supported, so the run did not happen"
    );
}

/// Builds the report from a real run: the corpus probed against the registry,
/// every entry executed against the in-process Ravel stack, and every
/// state-2 rejection case verified.
#[allow(clippy::expect_used)]
async fn build_report() -> ConformanceReport {
    let parsed = parsed_corpus();
    let files = corpus_files(&parsed);
    let mut report = ConformanceReport::from_corpus(&files);

    let config = DatasetConfig::default();
    let dataset = generate(&config);
    let tenant = TenantId::new("difftest");
    let now_ns = config.base_ts_ms * 1_000_000;
    let ravel = RavelStack::ingest(tenant, &dataset, now_ns)
        .await
        .expect("ingest dataset into ravel stack");

    let outcomes = ravel_promql_difftest::scoring::run_ravel_only(
        &files,
        config.base_ts_ms,
        &ravel.app,
        ravel.token,
    )
    .await;
    report.apply_ravel_outcomes(&outcomes);
    report.apply_unit_test_names(&ravel_promql_test_names());

    let rejection_results = run_rejection_cases().await;
    let mut per_construct: BTreeMap<&str, bool> = BTreeMap::new();
    for result in &rejection_results {
        let entry = per_construct.entry(result.construct).or_insert(true);
        *entry = *entry && result.ok;
    }
    report.apply_rejection_results(&per_construct);
    report
}

/// What one [`REJECTION_CASES`] entry did.
struct RejectionResult {
    construct: &'static str,
    query: &'static str,
    ok: bool,
    detail: String,
}

/// Runs every rejection case against a freshly ingested Ravel stack.
#[allow(clippy::expect_used)]
async fn run_rejection_cases() -> Vec<RejectionResult> {
    let config = DatasetConfig::default();
    let dataset = generate(&config);
    let tenant = TenantId::new("difftest");
    let now_ns = config.base_ts_ms * 1_000_000;
    let ravel = RavelStack::ingest(tenant, &dataset, now_ns)
        .await
        .expect("ingest dataset into ravel stack");

    let mut results = Vec::new();
    for case in REJECTION_CASES {
        let at_ms = config.base_ts_ms + case.time_offset_ms;
        let uri = match case.eval {
            RejectionEval::Instant => format!(
                "/api/v1/query?query={}&time={}",
                encode(case.query),
                seconds(at_ms)
            ),
            RejectionEval::Range => format!(
                "/api/v1/query_range?query={}&start={}&end={}&step=60000ms",
                encode(case.query),
                seconds(at_ms),
                seconds(at_ms + 300_000)
            ),
        };
        let (status, body) = get(&ravel.app, ravel.token, &uri).await;
        // The registry's own declared typed error is the expectation, so a
        // construct that regresses from one typed-error variant to another
        // fails here instead of passing as "some typed error came back".
        let expected = declared_typed_error(case.construct)
            .unwrap_or_else(|e| panic!("{}: {e}", case.construct));
        results.push(check_rejection(
            case.construct,
            case.query,
            case.message_contains,
            expected,
            status,
            &body,
        ));
    }
    results
}

/// The typed-rejection contract, in one place: not a 2xx, a Prometheus error
/// envelope, the exact HTTP status and `errorType` the registry row declares, a
/// non-empty message, no `data` payload, and the expected message substring
/// when the case names one.
///
/// `expected` is the `(status, errorType)` pair parsed out of the construct's
/// own `typed_error` string, which is what makes this an identity check rather
/// than a shape check: a construct whose rejection moves
/// from 422 `execution` to 500 `internal` still rejects cleanly by shape, but
/// no longer matches the error the published table names.
fn check_rejection(
    construct: &'static str,
    query: &'static str,
    message_contains: &str,
    expected: TypedErrorTag,
    status: u16,
    body: &Json,
) -> RejectionResult {
    let envelope_status = body
        .get("status")
        .and_then(Json::as_str)
        .unwrap_or_default();
    let error_type = body
        .get("errorType")
        .and_then(Json::as_str)
        .unwrap_or_default();
    let message = body.get("error").and_then(Json::as_str).unwrap_or_default();

    let mut problems: Vec<String> = Vec::new();
    if (200..300).contains(&status) {
        problems.push(format!("answered with HTTP {status}"));
    }
    if envelope_status != "error" {
        problems.push(format!(
            "envelope status is '{envelope_status}', not 'error'"
        ));
    }
    if !TYPED_ERROR_TAGS.contains(&error_type) {
        problems.push(format!("errorType '{error_type}' is not a stable tag"));
    } else if error_type != expected.error_type {
        problems.push(format!(
            "errorType '{error_type}' is not the declared '{}'",
            expected.error_type
        ));
    }
    if status != expected.status {
        problems.push(format!(
            "HTTP {status} is not the declared {}",
            expected.status
        ));
    }
    if message.is_empty() {
        problems.push("the error message is empty".to_string());
    }
    if body.get("data").is_some() {
        problems.push("a rejection must carry no data payload".to_string());
    }
    if !message_contains.is_empty() && !message.contains(message_contains) {
        problems.push(format!("the message does not mention '{message_contains}'"));
    }

    RejectionResult {
        construct,
        query,
        ok: problems.is_empty(),
        detail: if problems.is_empty() {
            format!("HTTP {status} {error_type}: {message}")
        } else {
            format!(
                "{} [HTTP {status} status='{envelope_status}' errorType='{error_type}' error='{message}']",
                problems.join("; ")
            )
        },
    }
}

#[allow(clippy::expect_used)]
async fn get(app: &axum::Router, token: &str, uri: &str) -> (u16, Json) {
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    let request = Request::builder()
        .method("GET")
        .uri(uri)
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .expect("building the request");
    // `Router`'s `Service::Error` is `Infallible`, so this is an exhaustive
    // (empty) match rather than an unwrap on the `Result`.
    let response = match app.clone().oneshot(request).await {
        Ok(response) => response,
        Err(never) => match never {},
    };
    let status = response.status().as_u16();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("reading the response body");
    let json = serde_json::from_slice(&bytes).unwrap_or(Json::Null);
    (status, json)
}

/// Percent-encodes everything except unreserved characters, matching
/// `runner.rs`'s own encoding (braces, quotes, and `=` all appear in PromQL).
fn encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn seconds(ms: i64) -> String {
    let sign = if ms < 0 { "-" } else { "" };
    let abs = ms.unsigned_abs();
    format!("{sign}{}.{:03}", abs / 1000, abs % 1000)
}
