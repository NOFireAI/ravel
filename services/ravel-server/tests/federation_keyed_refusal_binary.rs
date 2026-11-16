//! ADR-2708 D1: on a keyed bucket in a tenant-resolving mode, an unkeyed
//! `--remote-cluster` is refused at startup, and the refusal has to come out of
//! the SHIPPED BINARY.
//!
//! `federation_tenant_isolation.rs` calls `ensure_federation_tenant_mapping`
//! with `durable_auth = true` directly. That test passes against a `main.rs`
//! that passes `false` there instead of `durable_auth_enabled(..)`, because it
//! never goes through `main` at all. This runs the real binary over a fresh
//! keyed memory bucket, so the argument `main` passes is part of what is
//! asserted.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// The built `ravel-server` binary, the same artifact `cargo run -p
/// ravel-server` and the release image start.
const BINARY: &str = env!("CARGO_BIN_EXE_ravel-server");

/// How long the process may take to either exit or report it is listening.
/// Both outcomes come before any query is served, so reaching this means the
/// process hung somewhere in startup.
const STARTUP_DEADLINE: Duration = Duration::from_secs(30);

/// The line `main` logs once every listener is bound, which is after the
/// federation guard has run.
const LISTENING: &str = "ravel-server listening";

/// Text only the unkeyed-remote refusal prints.
const REFUSAL: &str = "names no local tenant";

/// The one static tenant every run configures.
const TENANT: &str = "acme";

/// How a run ended.
enum Outcome {
    /// The process exited on its own with this code (`None` for a signal).
    Exited(Option<i32>),
    /// The process logged [`LISTENING`] and was then killed by the test.
    Listening,
}

/// Writes the deployment key and the remote credential into `dir`, and returns
/// the arguments that start a query-mode process over a fresh keyed memory
/// bucket with one `--tenant-token` and one remote described by `remote_spec`
/// (`credential-file` is appended here).
fn keyed_query_args(dir: &Path, remote_spec: &str) -> Vec<String> {
    let key = dir.join("deployment.key");
    std::fs::write(&key, "11".repeat(32)).expect("write the deployment key");
    let credential = dir.join("eu.token");
    std::fs::write(&credential, "remote-operator-token").expect("write the remote credential");
    vec![
        "--mode".into(),
        "query".into(),
        "--listen-http".into(),
        "127.0.0.1:0".into(),
        "--listen-grpc".into(),
        "127.0.0.1:0".into(),
        "--tenant-hash-key-file".into(),
        key.display().to_string(),
        "--tenant-token".into(),
        format!("acme-token={TENANT}"),
        "--remote-cluster".into(),
        format!("{remote_spec},credential-file={}", credential.display()),
    ]
}

/// Drains one child pipe on its own thread, so a chatty process cannot block
/// on a full pipe while the caller polls for its exit.
fn drain(mut pipe: impl Read + Send + 'static) -> std::thread::JoinHandle<String> {
    std::thread::spawn(move || {
        let mut bytes = Vec::new();
        let _ = pipe.read_to_end(&mut bytes);
        String::from_utf8_lossy(&bytes).into_owned()
    })
}

/// Runs the binary with `args` until it exits or logs [`LISTENING`], and
/// returns the outcome with the captured stdout and stderr. A process that
/// does neither within [`STARTUP_DEADLINE`] is killed and fails the test.
fn run(args: &[String]) -> (Outcome, String, String) {
    let mut child = Command::new(BINARY)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| panic!("the built binary at {BINARY} must be runnable: {e}"));
    let stderr = drain(child.stderr.take().expect("stderr is piped"));
    let stdout_pipe = child.stdout.take().expect("stdout is piped");
    let (listening_tx, listening_rx) = mpsc::channel();
    let stdout = std::thread::spawn(move || {
        let mut captured = String::new();
        for line in BufReader::new(stdout_pipe).lines() {
            let Ok(line) = line else { break };
            if line.contains(LISTENING) {
                let _ = listening_tx.send(());
            }
            captured.push_str(&line);
            captured.push('\n');
        }
        captured
    });

    let deadline = Instant::now() + STARTUP_DEADLINE;
    let outcome = loop {
        if let Some(status) = child.try_wait().expect("poll the child") {
            break Outcome::Exited(status.code());
        }
        if listening_rx.try_recv().is_ok() {
            let _ = child.kill();
            let _ = child.wait();
            break Outcome::Listening;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            let stderr = stderr.join().unwrap_or_default();
            panic!(
                "`ravel-server {}` neither exited nor logged {LISTENING:?} within \
                 {STARTUP_DEADLINE:?}, and was killed; stderr: {stderr}",
                args.join(" ")
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    (
        outcome,
        stdout.join().expect("stdout reader"),
        stderr.join().expect("stderr reader"),
    )
}

/// A fresh keyed bucket, query mode, one `--tenant-token`, and a
/// `--remote-cluster` with no `tenant=`: the binary refuses before any
/// listener binds, exits non-zero, and names durable sys/auth as the reason
/// and `tenant=` as the fix.
///
/// Flip to watch it fail: in `main.rs`, pass `false` to
/// `ensure_federation_tenant_mapping` in place of
/// `ravel_server::durable_auth_enabled(cli.mode, deployment_key.is_some())`.
/// The process then starts and logs [`LISTENING`], and the first assertion
/// below fails.
#[test]
fn the_binary_refuses_an_unkeyed_remote_on_a_keyed_bucket() {
    let dir = tempfile::tempdir().expect("tempdir");
    let args = keyed_query_args(dir.path(), "name=eu,endpoint=127.0.0.1:9");
    let (outcome, stdout, stderr) = run(&args);

    let code = match outcome {
        Outcome::Exited(code) => code,
        Outcome::Listening => panic!(
            "an unkeyed --remote-cluster on a keyed bucket must be refused at startup, but the \
             process started listening; stderr: {stderr}"
        ),
    };
    assert!(
        code.is_some_and(|c| c != 0),
        "the refusal must exit non-zero, got {code:?}; stderr: {stderr}; stdout: {stdout}"
    );
    assert!(
        stderr.contains(REFUSAL) && stderr.contains("'eu'"),
        "stderr must carry the unkeyed-remote refusal naming 'eu', got: {stderr}"
    );
    assert!(
        stderr.contains("sys/auth"),
        "stderr must name sys/auth as the reason, got: {stderr}"
    );
    assert!(
        stderr.contains("tenant="),
        "stderr must name tenant= as the fix, got: {stderr}"
    );
}

/// The same keyed bucket and flags with `tenant=<the static tenant>` on the
/// spec gets past the guard: the process logs [`LISTENING`] and the refusal
/// text appears nowhere.
#[test]
fn the_binary_admits_a_mapped_remote_on_a_keyed_bucket() {
    let dir = tempfile::tempdir().expect("tempdir");
    let args = keyed_query_args(
        dir.path(),
        &format!("name=eu,endpoint=127.0.0.1:9,tenant={TENANT}"),
    );
    let (outcome, stdout, stderr) = run(&args);

    assert!(
        matches!(outcome, Outcome::Listening),
        "a mapped --remote-cluster on a keyed bucket must start, but the process exited; \
         stderr: {stderr}; stdout: {stdout}"
    );
    assert!(
        !stderr.contains(REFUSAL) && !stdout.contains(REFUSAL),
        "the unkeyed-remote refusal must not appear for a mapped spec; stderr: {stderr}"
    );
}
