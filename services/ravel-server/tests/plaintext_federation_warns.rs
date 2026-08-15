//! Federation TLS is on by default (ADR-0071 amendment: "Federation TLS on by
//! default; plaintext is an explicit, logged choice"), so a `--remote-cluster`
//! configured with TLS off must produce a loud startup warning naming that
//! remote and its endpoint.
//!
//! The test captures the `tracing` output around
//! `ravel_server::warn_plaintext_federation` and asserts a WARN event is
//! emitted for a plaintext remote, and that a TLS remote emits nothing.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ravel_server::config::RemoteClusterConfig;
use ravel_server::warn_plaintext_federation;
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

fn remote(name: &str, endpoint: &str, tls: bool) -> RemoteClusterConfig {
    RemoteClusterConfig {
        name: name.to_string(),
        endpoint: endpoint.to_string(),
        credential: "operator-token".to_string(),
        tls,
        tls_ca_file: None,
        skip_unavailable: false,
        soft_timeout: Duration::from_secs(5),
    }
}

/// Runs `warn_plaintext_federation(clusters)` under a scoped subscriber that
/// writes into a buffer, and returns the captured log text.
fn capture(clusters: &[RemoteClusterConfig]) -> String {
    let buffer = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_writer(CapturedLog(buffer.clone()))
        .with_max_level(Level::WARN)
        .with_ansi(false)
        .finish();

    tracing::subscriber::with_default(subscriber, || {
        warn_plaintext_federation(clusters);
    });

    let bytes = buffer.lock().expect("log buffer lock").clone();
    String::from_utf8(bytes).expect("log bytes are utf-8")
}

#[test]
fn warns_for_a_plaintext_remote() {
    let logged = capture(&[remote("eu", "eu.internal:9443", false)]);

    assert!(
        logged.contains("WARN"),
        "expected a WARN-level event, got: {logged:?}"
    );
    assert!(
        logged.contains("eu") && logged.contains("eu.internal:9443"),
        "warning must name the remote and its endpoint, got: {logged:?}"
    );
    assert!(
        logged.contains("cleartext"),
        "warning must state the traffic is cleartext, got: {logged:?}"
    );
    assert!(
        logged.contains("replay the credential"),
        "warning must state the credential can be replayed, got: {logged:?}"
    );
}

#[test]
fn silent_for_a_tls_remote() {
    let logged = capture(&[remote("eu", "eu.internal:9443", true)]);

    assert!(
        logged.trim().is_empty(),
        "a TLS remote must not warn, got: {logged:?}"
    );
}

/// One warning per plaintext remote, and a TLS remote in the same list stays
/// unmentioned: the operator sees exactly which hops are in cleartext.
#[test]
fn warns_once_per_plaintext_remote() {
    let logged = capture(&[
        remote("eu", "eu.internal:9443", false),
        remote("apac", "apac.internal:9443", true),
        remote("us", "us.internal:9443", false),
    ]);

    assert_eq!(
        logged.matches("SECURITY: --remote-cluster").count(),
        2,
        "expected one warning per plaintext remote, got: {logged:?}"
    );
    assert!(
        !logged.contains("apac"),
        "the TLS remote must not be warned about, got: {logged:?}"
    );
}
