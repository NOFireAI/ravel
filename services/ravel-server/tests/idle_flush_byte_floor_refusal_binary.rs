//! ADR-1737 decision 1: the shipped binary refuses a bad
//! `--idle-flush-byte-floor` before it touches the object store.
//!
//! `start` refuses a floor at or above `--min-flush-bytes` too, but `main`
//! pins `sys/tenancy`, bootstraps `sys/gc` and validates provisioning before it
//! calls `start`, so a refusal there still leaves durable objects in a fresh
//! bucket. `main` cannot be handed a store a test can inspect, so this pins
//! the ordering through the one startup step that fails on its own: with no
//! tenant-hash key and no `--tenant-hash-unkeyed`, pinning `sys/tenancy` on a
//! fresh bucket refuses to start. A binary that checks the floor first names
//! the floor; one that pins tenancy first names `sys/tenancy` instead.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::io::Read;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// The built `ravel-server` binary.
const BINARY: &str = env!("CARGO_BIN_EXE_ravel-server");

/// How long a refusal may take to exit. Reaching this means the process
/// started up instead.
const EXIT_DEADLINE: Duration = Duration::from_secs(20);

/// Ephemeral loopback ports over the default memory store, so a process that
/// wrongly starts binds nothing another test holds.
const STARTUP_ARGS: [&str; 4] = [
    "--listen-http",
    "127.0.0.1:0",
    "--listen-grpc",
    "127.0.0.1:0",
];

fn drain(mut pipe: impl Read + Send + 'static) -> std::thread::JoinHandle<String> {
    std::thread::spawn(move || {
        let mut bytes = Vec::new();
        let _ = pipe.read_to_end(&mut bytes);
        String::from_utf8_lossy(&bytes).into_owned()
    })
}

/// Runs the binary with `args` plus [`STARTUP_ARGS`] and returns
/// `(status code, stderr)`, failing the test if it is still running at
/// [`EXIT_DEADLINE`].
fn run(args: &[&str]) -> (Option<i32>, String) {
    let mut child = Command::new(BINARY)
        .args(args)
        .args(STARTUP_ARGS)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| panic!("the built binary at {BINARY} must be runnable: {e}"));
    let stderr = drain(child.stderr.take().expect("stderr is piped"));

    let deadline = Instant::now() + EXIT_DEADLINE;
    let status = loop {
        if let Some(status) = child.try_wait().expect("poll the child") {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            let stderr = stderr.join().unwrap_or_default();
            panic!(
                "`ravel-server {}` did not exit within {EXIT_DEADLINE:?}; stderr: {stderr}",
                args.join(" ")
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    (status.code(), stderr.join().expect("stderr reader"))
}

/// A floor equal to the shipped `--min-flush-bytes` (256 KiB) is refused with
/// the floor's own message, and the refusal comes before tenancy pinning, the
/// first step that writes to the bucket.
///
/// Flip to watch it fail: remove the `self.resolve_idle_flush_byte_floor()?`
/// call from `Cli::validate`. The binary then reaches tenancy pinning, which
/// refuses on the missing hash key, and stderr names `sys/tenancy` instead of
/// the floor.
#[test]
fn the_binary_refuses_a_bad_floor_before_it_writes_to_the_bucket() {
    let (code, stderr) = run(&["--idle-flush-byte-floor", "262144"]);
    assert_eq!(
        code,
        Some(1),
        "a bad floor must refuse startup; stderr: {stderr}"
    );
    assert!(
        stderr.contains(
            "invalid ingest configuration: idle_flush_byte_floor (262144 bytes) must be below \
             min_flush_bytes (262144 bytes)"
        ),
        "the refusal must name the floor and the byte figures, got: {stderr}"
    );
    assert!(
        !stderr.contains("sys/tenancy"),
        "the floor must be refused before tenancy is pinned, got: {stderr}"
    );
}

/// The same run with a legal floor gets past the floor check and is refused by
/// tenancy pinning instead, so the test above cannot pass because this
/// configuration refuses before tenancy for some unrelated reason.
#[test]
fn a_legal_floor_reaches_tenancy_pinning() {
    let (code, stderr) = run(&["--idle-flush-byte-floor", "262143"]);
    assert_eq!(
        code,
        Some(1),
        "a fresh bucket with no hash key must refuse startup; stderr: {stderr}"
    );
    assert!(
        stderr.contains("sys/tenancy"),
        "a legal floor must reach tenancy pinning, got: {stderr}"
    );
}
