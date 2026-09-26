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

use std::process::Command;

/// The built `ravel-server` binary, the same artifact `cargo run -p
/// ravel-server` and the release image start.
const BINARY: &str = env!("CARGO_BIN_EXE_ravel-server");

/// Runs the binary with `args` and returns `(status code, stdout, stderr)`.
///
/// A signal death has no code; it is reported as `None` so the assertion says
/// "killed by a signal" rather than silently reading as a refusal.
fn run(args: &[&str]) -> (Option<i32>, String, String) {
    let output = Command::new(BINARY)
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("the built binary at {BINARY} must be runnable: {e}"));
    (
        output.status.code(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
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
/// Every process below then starts up instead of refusing, and each case here
/// fails on the exit status.
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
