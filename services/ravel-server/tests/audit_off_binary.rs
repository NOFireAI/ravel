//! ADR-0062 opt-in amendment: what the shipped binary does under the default
//! `--audit-mode off` with audit settings left in its environment.
//!
//! Both cases hang on the process environment (`RAVEL_AUDIT_TOKEN_KEY`, and
//! clap's `env =` bindings for the tuning flags), which a test cannot set
//! in-process, so each runs the real binary with the variables set on the
//! child only.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::io::{BufRead, BufReader, Read};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// The built `ravel-server` binary.
const BINARY: &str = env!("CARGO_BIN_EXE_ravel-server");

/// How long the process may take to either exit or report it is listening.
const STARTUP_DEADLINE: Duration = Duration::from_secs(30);

/// The line `main` logs once every listener is bound, which is after the
/// query-audit stamp.
const LISTENING: &str = "ravel-server listening";

/// Text only the unused-token-key warning prints.
const UNUSED_KEY: &str = "RAVEL_AUDIT_TOKEN_KEY is set but unused";

/// Text the dead-flag refusal prints.
const ENABLE_HINT: &str = "--audit-mode required";

/// A well-formed token key: 64 hex characters.
const TOKEN_KEY: &str = "2222222222222222222222222222222222222222222222222222222222222222";

/// Every variable that changes what the audit resolves to. Each run clears
/// them, so a value in the test runner's own environment cannot decide a case.
const AUDIT_ENV: [&str; 5] = [
    "RAVEL_AUDIT_MODE",
    "RAVEL_AUDIT_TEXT",
    "RAVEL_AUDIT_MAX_BATCH",
    "RAVEL_AUDIT_MAX_AGE",
    "RAVEL_AUDIT_TOKEN_KEY",
];

/// How a run ended.
enum Outcome {
    /// The process exited on its own with this code (`None` for a signal).
    Exited(Option<i32>),
    /// The process logged [`LISTENING`] and was then killed by the test.
    Listening,
}

fn drain(mut pipe: impl Read + Send + 'static) -> std::thread::JoinHandle<String> {
    std::thread::spawn(move || {
        let mut bytes = Vec::new();
        let _ = pipe.read_to_end(&mut bytes);
        String::from_utf8_lossy(&bytes).into_owned()
    })
}

/// Runs the binary in `mode` over the default memory store with an unkeyed
/// tenant hash and ephemeral loopback ports, with `env` set on the child and
/// every other [`AUDIT_ENV`] variable removed. Returns once the process exits
/// or logs [`LISTENING`] (it is killed then), with the captured stdout and
/// stderr. A process that does neither within [`STARTUP_DEADLINE`] is killed
/// and fails the test, so no run leaves a child behind.
fn run(mode: &str, env: &[(&str, &str)]) -> (Outcome, String, String) {
    let mut command = Command::new(BINARY);
    command
        .args([
            "--mode",
            mode,
            "--listen-http",
            "127.0.0.1:0",
            "--listen-grpc",
            "127.0.0.1:0",
            "--tenant-hash-unkeyed",
        ])
        .env("RUST_LOG", "info")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for name in AUDIT_ENV {
        command.env_remove(name);
    }
    command.envs(env.iter().copied());
    let mut child = command
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
                "`ravel-server --mode {mode}` neither exited nor logged {LISTENING:?} within \
                 {STARTUP_DEADLINE:?}, and was killed; stderr: {stderr}"
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    (
        outcome,
        strip_ansi(&stdout.join().expect("stdout reader")),
        strip_ansi(&stderr.join().expect("stderr reader")),
    )
}

/// `text` with ANSI escape sequences removed, so an assertion reads the same
/// whether or not the log formatter colours its output.
fn strip_ansi(text: &str) -> String {
    let mut plain = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            for c in chars.by_ref() {
                if c.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            plain.push(c);
        }
    }
    plain
}

/// The lines of `output` carrying the query-audit stamp.
fn stamp_lines(output: &str) -> Vec<&str> {
    output
        .lines()
        .filter(|line| line.contains("query audit resolved"))
        .collect()
}

/// A query-serving process under the default `off` with
/// `RAVEL_AUDIT_TOKEN_KEY` set starts, and its one stamp line is a WARN that
/// says the key is unused and names `--audit-mode required`.
///
/// Flip to watch it fail: in `lib.rs`, change the stamp's condition
/// `config.audit_pipeline.is_none() && config::audit_token_key_env_present()`
/// to `false`. The stamp is then the plain INFO line and the WARN assertion
/// below fails.
#[test]
fn a_token_key_under_audit_mode_off_warns_that_it_is_unused() {
    let (outcome, stdout, stderr) = run("query", &[("RAVEL_AUDIT_TOKEN_KEY", TOKEN_KEY)]);
    assert!(
        matches!(outcome, Outcome::Listening),
        "a token key under --audit-mode off is a warning, not a refusal, but the process \
         exited; stderr: {stderr}; stdout: {stdout}"
    );
    let output = format!("{stdout}{stderr}");
    let stamps = stamp_lines(&output);
    assert_eq!(
        stamps.len(),
        1,
        "exactly one query-audit stamp line, got: {output}"
    );
    let stamp = stamps[0];
    assert!(
        stamp.contains("WARN")
            && stamp.contains(UNUSED_KEY)
            && stamp.contains(ENABLE_HINT)
            && stamp.contains("audit_mode=\"off\""),
        "the stamp must be a WARN naming the unused key, {ENABLE_HINT} and audit_mode=off, \
         got: {stamp}"
    );
}

/// The same process without `RAVEL_AUDIT_TOKEN_KEY` stamps an INFO line and
/// prints no unused-key warning.
#[test]
fn no_token_key_under_audit_mode_off_prints_no_warning() {
    let (outcome, stdout, stderr) = run("query", &[]);
    assert!(
        matches!(outcome, Outcome::Listening),
        "the default query process must start; stderr: {stderr}; stdout: {stdout}"
    );
    let output = format!("{stdout}{stderr}");
    let stamps = stamp_lines(&output);
    assert_eq!(
        stamps.len(),
        1,
        "exactly one query-audit stamp line, got: {output}"
    );
    assert!(
        stamps[0].contains("INFO") && !stamps[0].contains("WARN"),
        "with no token key the stamp is INFO, got: {}",
        stamps[0]
    );
    assert!(
        !output.contains(UNUSED_KEY),
        "no unused-key warning without the variable, got: {output}"
    );
}

/// An audit tuning flag set through its environment variable under the
/// default `off` is refused in a query-serving mode, the same as the flag:
/// non-zero exit, and the error names `--audit-mode required`.
///
/// Flip to watch it fail: in `config.rs`, change the dead-flag refusal's
/// condition `cli.mode.installs_query_audit_pipeline() &&
/// !cli.query_audit_enabled()` to `false`. The process then starts and logs
/// [`LISTENING`], and the first assertion below fails.
#[test]
fn an_audit_tuning_env_var_under_audit_mode_off_is_refused_in_query_mode() {
    for env in [
        ("RAVEL_AUDIT_MAX_BATCH", "8"),
        ("RAVEL_AUDIT_TEXT", "plaintext"),
    ] {
        let (outcome, stdout, stderr) = run("query", &[env]);
        let code = match outcome {
            Outcome::Exited(code) => code,
            Outcome::Listening => panic!(
                "{}={} under --audit-mode off must be refused in query mode, but the process \
                 started listening; stdout: {stdout}",
                env.0, env.1
            ),
        };
        assert!(
            code.is_some_and(|c| c != 0),
            "the refusal must exit non-zero, got {code:?}; stderr: {stderr}"
        );
        assert!(
            stderr.contains(ENABLE_HINT),
            "the refusal must name {ENABLE_HINT}, got: {stderr}"
        );
    }
}

/// The same environment under `--mode gateway`, which never installs the
/// pipeline, starts: a tier-shared environment must not stop the gateway.
#[test]
fn an_audit_tuning_env_var_does_not_stop_the_gateway() {
    let (outcome, stdout, stderr) = run("gateway", &[("RAVEL_AUDIT_MAX_BATCH", "8")]);
    assert!(
        matches!(outcome, Outcome::Listening),
        "the gateway must start with RAVEL_AUDIT_MAX_BATCH set; stderr: {stderr}; \
         stdout: {stdout}"
    );
    assert!(
        !stderr.contains(ENABLE_HINT),
        "the gateway must not print the dead-flag refusal, got: {stderr}"
    );
}
