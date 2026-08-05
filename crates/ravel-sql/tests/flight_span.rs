//! The Flight SQL statement-redemption path carries a request-level tracing
//! span (issue #643, ADR-0044 section 5).
//!
//! `do_get_statement` builds a `flight_sql_statement` span, the Flight SQL
//! counterpart of the HTTP handlers' `sql_query`/`analytics_query` spans. This
//! test drives a real `GetFlightInfo` then `DoGet` round trip through the
//! in-process harness under a scoped subscriber that captures `tracing` output,
//! then asserts three things:
//!
//! - the span exists and carries the bounded fields the allowlist permits
//!   (tenant hash, `workload_class="interactive"`, and the two store-count
//!   fields), and
//! - the store counts are actually recorded once the stream drains, which is
//!   where the streamed path's cost becomes final, and
//! - no statement text or object key ever reaches a span field (ADR-0044
//!   section 4's closed allowlist).
//!
//! It captures `tracing` the same way
//! services/ravel-server/tests/dev_insecure_tenant_header_warns.rs does: a
//! `MakeWriter` over an `Arc<Mutex<Vec<u8>>>`. The runtime is current-thread so
//! the whole round trip runs on the one thread the subscriber is installed on,
//! and the span's `CLOSE` event (which renders the fields recorded on stream
//! end) is captured deterministically.

#![cfg(feature = "flight-sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod util;

use std::io;
use std::sync::{Arc, Mutex};

use tracing::Level;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::fmt::format::FmtSpan;

use util::flight_harness::Harness;
use util::{SegSpec, tenant_id};

const QUERY: &str = "SELECT ts, value FROM samples ORDER BY series_id, ts";

/// A `tracing` writer that appends every emitted byte to a shared buffer so the
/// test can assert what was logged.
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

/// A `GetFlightInfo` then a fully drained `DoGet` for `QUERY`, run under a
/// subscriber that captures span-close events into a buffer. Returns the
/// captured log text.
fn capture_round_trip() -> String {
    let buffer = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_writer(CapturedLog(buffer.clone()))
        .with_max_level(Level::INFO)
        .with_span_events(FmtSpan::CLOSE)
        .with_ansi(false)
        .finish();

    tracing::subscriber::with_default(subscriber, || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("current-thread runtime");
        rt.block_on(async {
            let tenant = tenant_id("acme");
            let seg_specs: Vec<SegSpec> = util::flight_harness::specs();
            let harness = Harness::memory(&[(&tenant, &seg_specs)]).await;

            let ticket = harness
                .get_flight_info("acme", QUERY)
                .await
                .expect("flight info");
            // Draining the DoGet stream to completion drops it, which fires the
            // execution fold and, with it, the span's s3-count records: the
            // recorded counts are the whole RPC, not just its first batch.
            let _batches = harness.do_get("acme", &ticket).await.expect("do get");
        });
    });

    let bytes = buffer.lock().expect("log buffer lock").clone();
    String::from_utf8(bytes).expect("log bytes are utf-8")
}

#[test]
fn do_get_statement_emits_a_bounded_request_span() {
    let logged = capture_round_trip();

    // The request-level span exists and is named per the spec.
    assert!(
        logged.contains("flight_sql_statement"),
        "expected a flight_sql_statement span, got: {logged:?}"
    );

    // Its bounded fields are present: the tenant hash, the interactive workload
    // class label, and the two store-count fields, recorded once the stream
    // drained.
    let tenant_hex = tenant_id("acme").hash().to_hex();
    assert!(
        logged.contains(&tenant_hex),
        "span must carry the tenant hash {tenant_hex:?}, got: {logged:?}"
    );
    assert!(
        logged.contains("workload_class=\"interactive\""),
        "span must carry workload_class=interactive, got: {logged:?}"
    );
    assert!(
        logged.contains("s3_requests="),
        "span must record s3_requests once the stream ends, got: {logged:?}"
    );
    assert!(
        logged.contains("s3_bytes="),
        "span must record s3_bytes once the stream ends, got: {logged:?}"
    );
}

#[test]
fn the_request_span_never_carries_statement_text_or_keys() {
    let logged = capture_round_trip();

    // ADR-0044 section 4's allowlist is closed: no query text and no object key
    // may ever become a span field. The statement is
    // "SELECT ts, value FROM samples ...", so neither its keyword nor its table
    // name may appear anywhere the INFO-level span output reaches.
    assert!(
        !logged.contains("SELECT"),
        "statement text must never reach a span field, got: {logged:?}"
    );
    assert!(
        !logged.contains("samples"),
        "statement text must never reach a span field, got: {logged:?}"
    );
    assert!(
        !logged.contains(".rseg"),
        "object keys must never reach a span field, got: {logged:?}"
    );
}
