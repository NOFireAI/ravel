//! ADR-1306 decision 5: the startup log that reports the resolved per-query
//! S3 request budget also reports `covered_span`, "so an operator can see
//! which span their explicit value undercuts".
//!
//! An explicit `--max-s3-requests` is used verbatim, so an operator who sets
//! one below what `covered_span` costs gets queries refused for fold lag
//! before the fold-stall alert reaches them. Nothing else in the process
//! names that span, so this test pins the fields on both paths: the derived
//! default and an explicit value.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::io;
use std::sync::{Arc, Mutex};

use clap::Parser;
use ravel_server::config::Cli;
use ravel_server::log_resolved_request_budget;
use tracing::Level;
use tracing_subscriber::fmt::MakeWriter;

/// A `tracing` writer that appends every emitted byte to a shared buffer so
/// the test can assert what was logged.
#[derive(Clone)]
struct CapturedLog(Arc<Mutex<Vec<u8>>>);

impl io::Write for CapturedLog {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .expect("log buffer lock")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for CapturedLog {
    type Writer = CapturedLog;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Resolves the budget the way `main.rs` does for the given arguments and
/// returns the text the startup log emitted, plus the figures the assertions
/// compare against.
fn capture(args: &[&str]) -> (String, u64, u64) {
    let cli = Cli::try_parse_from(args).expect("arguments parse");
    let seal_margin = ravel_server::query::server_seal_margin();
    let limit = cli
        .resolve_max_s3_requests_with(seal_margin)
        .expect("the budget resolves");
    let budget = match limit {
        ravel_query::RequestLimit::Bounded(n) => n,
        ravel_query::RequestLimit::Unlimited => {
            panic!("the server's budget resolution is always bounded")
        }
    };
    let covered_span_secs = ravel_query::covered_span(seal_margin).as_secs();

    let buffer = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_writer(CapturedLog(buffer.clone()))
        .with_max_level(Level::INFO)
        .with_ansi(false)
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        log_resolved_request_budget(limit, seal_margin, cli.max_s3_requests.is_some());
    });

    let bytes = buffer.lock().expect("log buffer lock").clone();
    let text = String::from_utf8(bytes).expect("log bytes are utf-8");
    (text, budget, covered_span_secs)
}

/// The derived default: the log names the budget, says it was derived, and
/// gives the span it covers.
#[test]
fn derived_budget_is_logged_with_its_covered_span() {
    let (logged, budget, covered_span_secs) = capture(&["ravel-server"]);

    assert!(
        logged.contains("INFO"),
        "expected an INFO-level startup event, got: {logged:?}"
    );
    assert!(
        logged.contains(&format!("max_s3_requests={budget}")),
        "the log must name the resolved budget {budget}, got: {logged:?}"
    );
    assert!(
        logged.contains(&format!("covered_span_secs={covered_span_secs}")),
        "ADR-1306 decision 5 requires covered_span ({covered_span_secs}s) beside the \
         budget, got: {logged:?}"
    );
    assert!(
        logged.contains("source=\"derived\""),
        "an omitted --max-s3-requests must be reported as derived, got: {logged:?}"
    );
}

/// An explicit `--max-s3-requests` is the case decision 5 names: the value is
/// used verbatim, and the span it is measured against is what tells the
/// operator whether they undercut it.
#[test]
fn explicit_budget_is_logged_with_the_same_covered_span() {
    let (logged, budget, covered_span_secs) =
        capture(&["ravel-server", "--max-s3-requests", "25000"]);

    assert_eq!(budget, 25_000, "an explicit value is used verbatim");
    assert!(
        logged.contains("max_s3_requests=25000"),
        "the log must name the explicit budget, got: {logged:?}"
    );
    assert!(
        logged.contains(&format!("covered_span_secs={covered_span_secs}")),
        "an explicit budget must still be logged with the span it is measured against, \
         got: {logged:?}"
    );
    assert!(
        logged.contains("source=\"explicit --max-s3-requests\""),
        "an explicit --max-s3-requests must be reported as explicit, got: {logged:?}"
    );
}

/// Non-vacuity for the two assertions above: 25,000 is the pre-ADR-1306 flat
/// default, and it is far below what `covered_span` costs at the shipped
/// defaults. So the two logged spans are the same number while the two
/// budgets are not, which is exactly the comparison decision 5 puts in front
/// of an operator.
#[test]
fn the_explicit_value_undercuts_the_span_the_log_reports() {
    let (_, derived, derived_span) = capture(&["ravel-server"]);
    let (_, explicit, explicit_span) = capture(&["ravel-server", "--max-s3-requests", "25000"]);

    assert_eq!(
        derived_span, explicit_span,
        "both paths report the span of the catalog this server folds with"
    );
    assert!(
        explicit < derived,
        "the explicit 25,000 must be below the {derived} the span costs, or this test \
         is not exercising the case decision 5 is about"
    );
}
