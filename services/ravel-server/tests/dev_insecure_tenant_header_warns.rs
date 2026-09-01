//! When
//! `--dev-insecure-tenant-header` is enabled, the server must log a loud
//! startup warning that tenant isolation is bypassed (ADR-0009 mitigation).
//!
//! The test captures the `tracing` output around
//! `ravel_server::warn_dev_insecure_tenant_header` and asserts a WARN event
//! naming the header and the tenant-isolation bypass is emitted when the flag
//! is on, and that nothing is emitted when it is off.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::io;
use std::sync::{Arc, Mutex};

use ravel_server::warn_dev_insecure_tenant_header;
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

/// Runs `warn_dev_insecure_tenant_header(enabled)` under a scoped subscriber
/// that writes into a buffer, and returns the captured log text.
fn capture(enabled: bool) -> String {
    let buffer = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_writer(CapturedLog(buffer.clone()))
        .with_max_level(Level::WARN)
        .with_ansi(false)
        .finish();

    tracing::subscriber::with_default(subscriber, || {
        warn_dev_insecure_tenant_header(enabled);
    });

    let bytes = buffer.lock().expect("log buffer lock").clone();
    String::from_utf8(bytes).expect("log bytes are utf-8")
}

#[test]
fn warns_loudly_when_flag_enabled() {
    let logged = capture(true);

    assert!(
        logged.contains("WARN"),
        "expected a WARN-level event, got: {logged:?}"
    );
    assert!(
        logged.contains("x-ravel-tenant"),
        "warning must name the insecure header, got: {logged:?}"
    );
    assert!(
        logged.contains("Tenant isolation is bypassed"),
        "warning must state tenant isolation is bypassed, got: {logged:?}"
    );
    assert!(
        logged.contains("production"),
        "warning must warn against production use, got: {logged:?}"
    );
}

#[test]
fn silent_when_flag_disabled() {
    let logged = capture(false);

    assert!(
        logged.trim().is_empty(),
        "no warning must be emitted when the flag is off, got: {logged:?}"
    );
}
