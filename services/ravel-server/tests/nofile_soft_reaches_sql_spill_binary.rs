//! ADR-3565 bucket spill item 1: the shipped binary raises its soft open-file
//! limit to the hard limit at startup, and the SQL spill retry's descriptor
//! budget is computed from the raised limit, not the inherited one.
//!
//! The limit is a property of the process, which a test cannot set on itself
//! without changing it for every other test, so this runs the real binary
//! under a shell that lowers both limits on the child only.

#![cfg(feature = "sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::io::{BufRead, BufReader, Read};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// The built `ravel-server` binary.
const BINARY: &str = env!("CARGO_BIN_EXE_ravel-server");

/// How long the process may take to either exit or report it is listening.
const STARTUP_DEADLINE: Duration = Duration::from_secs(30);

/// The line `main` logs once every listener is bound, which is after every
/// startup line asserted on here.
const LISTENING: &str = "ravel-server listening";

/// The hard limit the child starts under: below any host's usual hard limit,
/// and not a value a host or a default would give.
const HARD: u64 = 4099;

/// The soft limit the child starts under, below [`HARD`], so the raise has
/// something to do.
const SOFT: u64 = 2053;

/// Lowers the soft and then the hard open-file limit and execs the server
/// with the remaining arguments. The soft limit goes first because the kernel
/// refuses a hard limit below the current soft one. A `ulimit` fails, and the
/// shell exits before the server starts, when the host's hard limit is
/// already below [`HARD`].
const LIMIT_SCRIPT: &str = "ulimit -Sn 2053 && ulimit -Hn 4099 && exec \"$0\" \"$@\"";

/// The spill ceiling handed to the child: 8 GiB, one spill share, so the
/// descriptor budget is `floor(4099 / 2) = 2049`.
const SPILL_MAX_BYTES: &str = "8589934592";

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

/// Runs the binary in `--mode query` over the default memory store with an
/// unkeyed tenant hash and ephemeral loopback ports, under [`LIMIT_SCRIPT`],
/// with `args` appended, `env` set on the child, and every other `RAVEL_`
/// variable removed. Returns once the process exits or logs [`LISTENING`]
/// (it is killed then), with the captured stdout and stderr. A process that
/// does neither within [`STARTUP_DEADLINE`] is killed and fails the test, so
/// no run leaves a child behind.
fn run(args: &[&str], env: &[(&str, &str)]) -> (Outcome, String, String) {
    let mut command = Command::new("sh");
    command
        .args(["-c", LIMIT_SCRIPT, BINARY])
        .args([
            "--mode",
            "query",
            "--listen-http",
            "127.0.0.1:0",
            "--listen-grpc",
            "127.0.0.1:0",
            "--tenant-hash-unkeyed",
        ])
        .args(args)
        .env("RUST_LOG", "info")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("RAVEL_") {
            command.env_remove(name);
        }
    }
    command.envs(env.iter().copied());
    let mut child = command
        .spawn()
        .unwrap_or_else(|e| panic!("sh must run the built binary at {BINARY}: {e}"));
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
                "`ravel-server --mode query` neither exited nor logged {LISTENING:?} within \
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

/// The one line of `output` whose `setting` field is `setting`.
fn setting_line<'a>(output: &'a str, setting: &str) -> &'a str {
    let key = format!(" setting=\"{setting}\"");
    let lines: Vec<&str> = output.lines().filter(|line| line.contains(&key)).collect();
    assert_eq!(
        lines.len(),
        1,
        "exactly one {setting} line, got: {lines:?}\n{output}"
    );
    lines[0]
}

/// The value of ` name=` on `line`, quotes removed.
fn field<'a>(line: &'a str, name: &str) -> &'a str {
    let key = format!(" {name}=");
    let start = line
        .find(&key)
        .unwrap_or_else(|| panic!("no {name} on {line}"))
        + key.len();
    let rest = &line[start..];
    match rest.strip_prefix('"') {
        Some(quoted) => &quoted[..quoted.find('"').expect("closing quote")],
        None => rest.split(' ').next().expect("a value"),
    }
}

/// A server started at a soft limit of 2053 under a hard limit of 4099 raises
/// its soft limit to 4099 and logs both, and the spill retry's descriptor
/// budget and modes lines are computed from the raised 4099 and the
/// operator's `--sql-partition-count 4`.
///
/// Prove-the-test: drop `budgets.sql_spill.nofile_soft = nofile.soft_or_max();`
/// from `main` and the budget line reads `nofile_soft=1024`; hand the spill
/// inputs the soft limit read before the raise and it reads
/// `nofile_soft=2053`.
#[test]
fn the_raised_soft_limit_reaches_the_sql_spill_retry_budget() {
    let spill_dir = tempfile::tempdir().expect("spill dir");
    let spill_dir_arg = spill_dir.path().to_str().expect("utf-8 temp path");
    let (outcome, stdout, stderr) = run(
        &["--sql-partition-count", "4"],
        &[
            ("RAVEL_SQL_SPILL_DIR", spill_dir_arg),
            ("RAVEL_SQL_SPILL_MAX_BYTES", SPILL_MAX_BYTES),
        ],
    );
    if let Outcome::Exited(code) = outcome {
        assert!(
            !stderr.contains("ulimit"),
            "the shell could not lower the open-file limits to hard {HARD} and soft {SOFT} \
             (the host's hard limit is likely below {HARD}), so the server never started; \
             exit {code:?}, stderr: {stderr}"
        );
        panic!("the server exited ({code:?}) before listening; stderr: {stderr}; stdout: {stdout}");
    }
    let output = format!("{stdout}{stderr}");

    let soft = setting_line(&output, "nofile_soft");
    assert_eq!(field(soft, "value"), HARD.to_string(), "{soft}");
    assert_eq!(field(soft, "source"), "raised", "{soft}");

    let hard = setting_line(&output, "nofile_hard");
    assert_eq!(field(hard, "value"), HARD.to_string(), "{hard}");

    let budget = setting_line(&output, "sql_spill_retry_descriptor_budget");
    assert_eq!(field(budget, "nofile_soft"), HARD.to_string(), "{budget}");
    let slots: u64 = field(budget, "slots").parse().expect("slots is a number");
    assert_eq!(
        field(budget, "value"),
        ravel_sql::bucket_spill_descriptor_budget(HARD, slots).to_string(),
        "{budget}"
    );

    let modes = setting_line(&output, "sql_spill_retry_modes");
    assert_eq!(field(modes, "partitions"), "4", "{modes}");
}
