//! Issue #4493: a SIGTERM delivered the instant `/readyz` turns 200 drains
//! gracefully and exits 0, instead of killing the process with the signal's
//! default disposition.
//!
//! This drives the real `ravel-server` binary, not the in-process
//! `ravel_server::start`/`Running::shutdown` path the rest of the
//! graceful-shutdown suite exercises: the bug lives in `main`'s ordering of
//! signal-handler installation against readiness, which only the shipped
//! process goes through. `main` installs the SIGTERM/SIGINT handlers before it
//! calls `start`, so a signal arriving in the window between readiness and the
//! drain await is buffered by the already-installed handler and delivered on
//! that await. Before the fix the handler was created only when the await was
//! first polled, so a SIGTERM in that window took its default disposition and
//! killed the process (exit by signal 15, shell code 143).
//!
//! The window is made deterministic with the `RAVEL_SHUTDOWN_WINDOW_TEST_SLEEP_MS`
//! seam `main` reads only under `cfg(debug_assertions)` (and so never in a
//! release build): it sleeps after readiness and before the drain await, and
//! logs a marker line as it enters the sleep. This test waits for that marker,
//! then sends SIGTERM squarely inside the window. The whole file is
//! `cfg(all(unix, debug_assertions))` because the seam, the signal and the
//! `kill` call it uses are all unix- and debug-only.
#![cfg(all(unix, debug_assertions))]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::io::{BufRead, BufReader, Read};
use std::os::unix::process::ExitStatusExt;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// The built `ravel-server` binary.
const BINARY: &str = env!("CARGO_BIN_EXE_ravel-server");

/// How long the seam sleep holds the shutdown window open. Long enough that the
/// test reaches the SIGTERM well inside it on a loaded host, short enough that a
/// buffered-signal pass does not idle the suite for long.
const WINDOW_SLEEP: Duration = Duration::from_secs(3);

/// How long the process may take to reach the window marker, or to exit after
/// the signal. Generous for a cold, loaded CI host.
const DEADLINE: Duration = Duration::from_secs(30);

/// The marker `main`'s seam logs as it enters the hold, after readiness is
/// attached and before the drain await. Its presence proves `/readyz` is 200
/// and the process is sitting in the window.
const WINDOW_MARKER: &str = "test seam: holding the shutdown window open after readiness";

/// The first line `main` logs on the drain path, and the last.
const DRAIN_BEGAN: &str = "shutdown signal received, draining";
const DRAIN_DONE: &str = "shutdown complete";

fn drain_pipe(mut pipe: impl Read + Send + 'static) -> std::thread::JoinHandle<String> {
    std::thread::spawn(move || {
        let mut bytes = Vec::new();
        let _ = pipe.read_to_end(&mut bytes);
        String::from_utf8_lossy(&bytes).into_owned()
    })
}

/// A SIGTERM to `pid` via the `kill` binary: ravel-server has no `libc`/`nix`
/// dependency to send one directly, and this test adds none.
fn send_sigterm(pid: u32) {
    let status = Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status()
        .expect("`kill` must be runnable");
    assert!(status.success(), "`kill -TERM {pid}` failed: {status:?}");
}

/// A SIGTERM sent while the process sits in the window between `/readyz` turning
/// 200 and the drain await must drain gracefully and exit 0, not die by the
/// signal.
///
/// This fails on the pre-#4493 order: move `ShutdownListener::install()` in
/// `main` down to just before `shutdown_listener.recv().await` (its position
/// before the fix, where the handler is created only when the await is first
/// polled) and the process is killed by SIGTERM during the seam sleep, exiting
/// by signal 15 with no drain line and a non-zero code.
#[test]
fn sigterm_in_the_readiness_window_drains_and_exits_zero() {
    let mut child = Command::new(BINARY)
        .args([
            "--mode",
            "gateway",
            "--listen-http",
            "127.0.0.1:0",
            "--listen-grpc",
            "127.0.0.1:0",
            "--tenant-hash-unkeyed",
        ])
        .env("RUST_LOG", "info")
        .env(
            "RAVEL_SHUTDOWN_WINDOW_TEST_SLEEP_MS",
            WINDOW_SLEEP.as_millis().to_string(),
        )
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| panic!("the built binary at {BINARY} must be runnable: {e}"));
    let pid = child.id();

    let stderr = drain_pipe(child.stderr.take().expect("stderr is piped"));
    let stdout_pipe = child.stdout.take().expect("stdout is piped");
    let (marker_tx, marker_rx) = mpsc::channel();
    let stdout = std::thread::spawn(move || {
        let mut captured = String::new();
        for line in BufReader::new(stdout_pipe).lines() {
            let Ok(line) = line else { break };
            if line.contains(WINDOW_MARKER) {
                let _ = marker_tx.send(());
            }
            captured.push_str(&line);
            captured.push('\n');
        }
        captured
    });

    // Wait for the process to enter the window, then signal it inside the hold.
    if marker_rx.recv_timeout(DEADLINE).is_err() {
        let _ = child.kill();
        let _ = child.wait();
        let err = stderr.join().unwrap_or_default();
        panic!("ravel-server never reached the shutdown window within {DEADLINE:?}; stderr: {err}");
    }
    send_sigterm(pid);

    // The process must now drain and exit on its own, well within the deadline.
    let exit_deadline = Instant::now() + DEADLINE;
    let status = loop {
        if let Some(status) = child.try_wait().expect("poll the child") {
            break status;
        }
        if Instant::now() >= exit_deadline {
            let _ = child.kill();
            let _ = child.wait();
            let err = stderr.join().unwrap_or_default();
            panic!("ravel-server did not exit within {DEADLINE:?} of the SIGTERM; stderr: {err}");
        }
        std::thread::sleep(Duration::from_millis(25));
    };

    let out = stdout.join().expect("stdout reader");
    let err = stderr.join().expect("stderr reader");
    let combined = format!("{out}{err}");

    assert_eq!(
        status.code(),
        Some(0),
        "a SIGTERM in the readiness window must drain and exit 0, not die by the signal; got \
         {status:?} (signal {:?}); output:\n{combined}",
        status.signal()
    );
    assert!(
        combined.contains(DRAIN_BEGAN),
        "the drain must start (missing {DRAIN_BEGAN:?}); output:\n{combined}"
    );
    assert!(
        combined.contains(DRAIN_DONE),
        "the drain must complete (missing {DRAIN_DONE:?}); output:\n{combined}"
    );
}
