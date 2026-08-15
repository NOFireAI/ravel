//! Gated differential test: runs the selector, error, and counter/regression
//! function corpora against a real pinned Prometheus binary and the
//! in-process Ravel stack. Skips
//! cleanly unless both `RAVEL_DIFFTEST=1` and `RAVEL_DIFFTEST_PROM_BIN` are
//! set, since the pinned binary is not available in every environment (this
//! sandbox has no network egress to fetch it, for one).

use std::env;
use std::path::PathBuf;
use std::time::Duration;

use ravel_promql_difftest::corpus::parse_corpus;
use ravel_promql_difftest::encode::encode_write_request;
use ravel_promql_difftest::generator::{DatasetConfig, generate};
use ravel_promql_difftest::prometheus_client::PrometheusClient;
use ravel_promql_difftest::prometheus_process::PrometheusProcess;
use ravel_promql_difftest::ravel_stack::RavelStack;
use ravel_promql_difftest::runner::run_corpus;
use ravel_types::TenantId;

const SELECTORS_CORPUS: &str = include_str!("../corpus/selectors.txt");
const ERRORS_CORPUS: &str = include_str!("../corpus/errors.txt");
const RATE_CORPUS: &str = include_str!("../corpus/rate.txt");
const TRANSFORM_CORPUS: &str = include_str!("../corpus/transform.txt");
const BINOP_CORPUS: &str = include_str!("../corpus/binop.txt");
const AGGREGATE_CORPUS: &str = include_str!("../corpus/aggregate.txt");
const SUBQUERY_CORPUS: &str = include_str!("../corpus/subquery.txt");
const HISTOGRAM_NATIVE_CORPUS: &str = include_str!("../corpus/histogram_native.txt");
const OVER_TIME_CORPUS: &str = include_str!("../corpus/over_time.txt");
const HISTOGRAM_CLASSIC_CORPUS: &str = include_str!("../corpus/histogram_classic.txt");

/// Issue #947 exclusion: the exact PromQL queries whose difference from the
/// pinned Prometheus binary is a 1-ULP summation-order residue in the last
/// digit (e.g. `-4.235967390124592` vs `-4.235967390124591`), not a semantic
/// bug. `avg_over_time`'s incremental Kahan mean and `rate()`'s Kahan sum
/// reduce their windows in an order Prometheus does not reproduce bit-for-bit;
/// making the output bit-exact would mean changing Ravel's reduction strategy,
/// a decision epic #947 owns and gates behind a not-yet-written tolerance ADR.
/// Until then these two queries (and only these two) compare within a small
/// ULP tolerance instead of exact bits. Every other corpus entry, in both new
/// files and the existing ones, stays exact-match. Keyed by query text so it
/// applies to whichever `kind` (instant or range) a corpus entry runs the
/// query at; an entry's declared `tolerance:` field, if any, still wins.
const ISSUE_947_ULP_TOLERANCE_QUERIES: &[&str] = &[
    "avg_over_time(diff_gauge_walk[5m])",
    "histogram_quantile(0.9, rate(diff_histogram_bucket{variant=\"wellformed\"}[5m]))",
];

/// The ULP tolerance applied to the [`ISSUE_947_ULP_TOLERANCE_QUERIES`]: small
/// enough that it only ever absorbs a last-digit summation-order residue,
/// never a real numeric divergence. The comparator still refuses any
/// tolerance across the zero boundary, a sign change, or a non-finite value
/// (see `comparator::within_ulps`), so this cannot mask a wrong sign or an
/// Inf/NaN mistake.
///
/// Measured, not guessed, against the real pinned-Prometheus CI run:
/// `avg_over_time(diff_gauge_walk[5m])` differs by 1 ULP
/// (`-4.235967390124592` vs `-4.235967390124591`);
/// `histogram_quantile(0.9, rate(...))` differs by 6 ULPs
/// (`0.4978847724695252` vs `0.49788477246952556`) -- both computed with
/// `f64::to_bits`. 8 is the smallest power-of-two margin above the larger
/// measured gap, leaving headroom for run-to-run FMA/vectorization variance
/// in the reduction order without widening far enough to hide an unrelated
/// bug.
const ISSUE_947_ULP_TOLERANCE: u32 = 8;

/// Apply the issue #947 exclusion in place: any entry whose query is one of
/// [`ISSUE_947_ULP_TOLERANCE_QUERIES`] and that has not already declared its
/// own `tolerance:` gets [`ISSUE_947_ULP_TOLERANCE`]. Returns how many entries
/// were adjusted so the caller can assert the allowlist actually matched
/// something (a renamed query would otherwise silently re-tighten these).
fn apply_issue_947_tolerance(entries: &mut [ravel_promql_difftest::corpus::CorpusEntry]) -> usize {
    let mut applied = 0;
    for entry in entries.iter_mut() {
        if entry.tolerance_ulps.is_none()
            && ISSUE_947_ULP_TOLERANCE_QUERIES.contains(&entry.query.as_str())
        {
            entry.tolerance_ulps = Some(ISSUE_947_ULP_TOLERANCE);
            applied += 1;
        }
    }
    applied
}

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::expect_used)]
async fn selector_and_error_corpus_match_pinned_prometheus() {
    if env::var("RAVEL_DIFFTEST").as_deref() != Ok("1") {
        eprintln!(
            "skipping: set RAVEL_DIFFTEST=1 and RAVEL_DIFFTEST_PROM_BIN=<path to pinned \
             prometheus binary> to run the differential test"
        );
        return;
    }
    let bin_path = env::var("RAVEL_DIFFTEST_PROM_BIN")
        .map(PathBuf::from)
        .expect("RAVEL_DIFFTEST_PROM_BIN must be set when RAVEL_DIFFTEST=1");

    let config = DatasetConfig::default();
    let dataset = generate(&config);

    let prom = PrometheusProcess::spawn(&bin_path).expect("spawn pinned prometheus");
    let client = PrometheusClient::new(prom.base_url.clone());
    client
        .wait_ready(Duration::from_secs(30))
        .await
        .expect("prometheus became ready");

    let body = encode_write_request(&dataset).expect("encode remote-write payload");
    client
        .remote_write(body)
        .await
        .expect("push dataset to prometheus");
    // Remote-writes append straight to the head block, but give the write
    // path a moment to settle before the first query.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let tenant = TenantId::new("difftest");
    let now_ns = config.base_ts_ms * 1_000_000;
    let ravel = RavelStack::ingest(tenant, &dataset, now_ns)
        .await
        .expect("ingest dataset into ravel stack");

    let mut entries = parse_corpus(SELECTORS_CORPUS).expect("parse selector corpus");
    entries.extend(parse_corpus(ERRORS_CORPUS).expect("parse error corpus"));
    entries.extend(parse_corpus(RATE_CORPUS).expect("parse rate corpus"));
    entries.extend(parse_corpus(TRANSFORM_CORPUS).expect("parse transform corpus"));
    entries.extend(parse_corpus(BINOP_CORPUS).expect("parse binop corpus"));
    entries.extend(parse_corpus(AGGREGATE_CORPUS).expect("parse aggregate corpus"));
    entries.extend(parse_corpus(SUBQUERY_CORPUS).expect("parse subquery corpus"));
    entries.extend(parse_corpus(HISTOGRAM_NATIVE_CORPUS).expect("parse histogram_native corpus"));
    entries.extend(parse_corpus(OVER_TIME_CORPUS).expect("parse over_time corpus"));
    entries.extend(parse_corpus(HISTOGRAM_CLASSIC_CORPUS).expect("parse histogram_classic corpus"));

    // Issue #947: two named queries carry a last-digit summation-order residue
    // that is out of scope to make bit-exact here; everything else stays
    // exact-match. Assert the allowlist matched so a future rename cannot
    // silently drop the exclusion and turn this into a spurious failure.
    let applied = apply_issue_947_tolerance(&mut entries);
    assert!(
        applied >= 2,
        "issue #947 float-tolerance allowlist matched {applied} entries, expected at least the \
         two named queries; a corpus query was likely renamed"
    );

    let report = run_corpus(
        &entries,
        config.base_ts_ms,
        &client,
        &ravel.app,
        ravel.token,
    )
    .await;

    if !report.is_clean() {
        let mut msg = format!(
            "{} of {} corpus entries mismatched:\n",
            report.failures.len(),
            report.total
        );
        for failure in &report.failures {
            msg.push_str(&format!(
                "- {} ({}): {}\n  prometheus: {}\n  ravel: {}\n",
                failure.entry_name,
                failure.query,
                failure.detail,
                failure.prometheus_body,
                failure.ravel_body,
            ));
        }
        panic!("{msg}");
    }
}
