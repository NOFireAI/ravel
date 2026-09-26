//! ADR-1693: the fold flags are refused in the modes that never schedule a
//! fold, and the refusal has to come out of the SHIPPED BINARY.
//!
//! `config.rs` already tests `Cli::parse_validated_from` directly. That test
//! passes against a `main.rs` that calls plain `Cli::parse()`, because it never
//! goes through `main` at all: reverting that one line would leave every
//! existing test green and every `--mode gateway --disable-fold` process
//! starting up and silently ignoring the flag, which is the exact failure the
//! decision exists to prevent. This runs the real binary instead, so the entry
//! point is part of what is asserted.
//!
//! Exit STATUS, not just a message: an operator's unit file and a container
//! runtime read the code, and a refusal printed at exit 0 restarts nothing.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::io::Read;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// The built `ravel-server` binary, the same artifact `cargo run -p
/// ravel-server` and the release image start.
const BINARY: &str = env!("CARGO_BIN_EXE_ravel-server");

/// How long a refusal may take to exit. A refusal is argument parsing and
/// nothing else, so reaching this means the process started up instead.
const EXIT_DEADLINE: Duration = Duration::from_secs(20);

/// Flags appended to every run. Ephemeral loopback ports, so a process that
/// wrongly starts up binds nothing another test or a local server holds; and
/// an unkeyed tenant hash over the default memory store, so such a process
/// really starts and is caught at [`EXIT_DEADLINE`] rather than exiting 1 on
/// a fresh bucket's missing hash key, which would read as some other failure.
const STARTUP_ARGS: [&str; 5] = [
    "--listen-http",
    "127.0.0.1:0",
    "--listen-grpc",
    "127.0.0.1:0",
    "--tenant-hash-unkeyed",
];

/// Drains one child pipe on its own thread, so a chatty process cannot block
/// on a full pipe while the caller polls for its exit.
fn drain(mut pipe: impl Read + Send + 'static) -> std::thread::JoinHandle<String> {
    std::thread::spawn(move || {
        let mut bytes = Vec::new();
        let _ = pipe.read_to_end(&mut bytes);
        String::from_utf8_lossy(&bytes).into_owned()
    })
}

/// Runs the binary with `args` plus [`STARTUP_ARGS`] and returns
/// `(status code, stdout, stderr)`.
///
/// A signal death has no code; it is reported as `None` so the assertion says
/// "killed by a signal" rather than silently reading as a refusal. A process
/// still running at [`EXIT_DEADLINE`] is killed and fails the test here: it
/// started up instead of refusing, which is itself the regression.
fn run(args: &[&str]) -> (Option<i32>, String, String) {
    let mut child = Command::new(BINARY)
        .args(args)
        .args(STARTUP_ARGS)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| panic!("the built binary at {BINARY} must be runnable: {e}"));
    let stdout = drain(child.stdout.take().expect("stdout is piped"));
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
                "`ravel-server {}` did not exit within {EXIT_DEADLINE:?}: it started up \
                 instead of refusing, and was killed; stderr: {stderr}",
                args.join(" ")
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    (
        status.code(),
        stdout.join().expect("stdout reader"),
        stderr.join().expect("stderr reader"),
    )
}

/// Every fold flag, in every mode that never schedules a fold, refused by the
/// binary with a non-zero exit and an error naming both the flag and the mode.
///
/// Nothing here reaches the object store: the refusal happens in argument
/// parsing, before any listener binds or any store call is made, which is why
/// running the binary costs a process start and nothing else.
///
/// Flip to watch it fail: change `main.rs`'s
/// `Cli::parse_validated_from(std::env::args_os())` back to `Cli::parse()`.
/// The first process below then starts up and serves instead of refusing, and
/// the case fails after [`EXIT_DEADLINE`] with "did not exit", rather than
/// hanging the test run.
#[test]
fn the_binary_refuses_a_fold_flag_in_a_mode_that_never_schedules_a_fold() {
    for mode in ["gateway", "query"] {
        for flag in [
            vec!["--disable-fold"],
            vec!["--fold-interval-secs", "900"],
            vec!["--disable-fold", "--fold-interval-secs", "900"],
        ] {
            let mut argv = vec!["--mode", mode];
            argv.extend_from_slice(&flag);
            let (code, stdout, stderr) = run(&argv);

            assert_eq!(
                code,
                Some(2),
                "`ravel-server {}` must exit with clap's usage-error status, got {code:?}; \
                 stdout: {stdout}; stderr: {stderr}",
                argv.join(" ")
            );
            assert!(
                stderr.contains(flag[0]),
                "the refusal must name {}, got: {stderr}",
                flag[0]
            );
            assert!(
                stderr.contains(mode),
                "the refusal must name the mode {mode}, got: {stderr}"
            );
            assert!(
                stdout.is_empty(),
                "a refusal belongs on stderr, not stdout; stdout was: {stdout}"
            );
        }
    }
}

/// `--help` still wins over the refusal, on stdout and at exit 0.
///
/// The validation runs after clap's own parse, so a help request is answered
/// rather than turned into an argument conflict. Asserted here because the
/// refusal is spelled as a `clap::Error` precisely to keep this true, and it
/// is the one case where a fold flag on a gateway command line must NOT fail.
#[test]
fn help_still_wins_over_the_fold_flag_refusal() {
    let (code, stdout, stderr) = run(&["--mode", "gateway", "--disable-fold", "--help"]);
    assert_eq!(
        code,
        Some(0),
        "--help exits 0 even beside a refused flag, got {code:?}; stderr: {stderr}"
    );
    assert!(
        stdout.contains("--disable-fold"),
        "--help prints the usage on stdout, got: {stdout}"
    );
    assert!(
        stderr.is_empty(),
        "--help belongs on stdout, not stderr; stderr was: {stderr}"
    );
}
