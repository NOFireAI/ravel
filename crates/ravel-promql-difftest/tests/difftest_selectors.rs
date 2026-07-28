//! Gated differential test: runs the selector, error, and counter/regression
//! function (P4) corpora against a real pinned Prometheus binary and the
//! in-process Ravel stack (docs/promql-evaluator-plan.md section 5.5). Skips
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
