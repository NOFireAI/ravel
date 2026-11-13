//! ADR-4158 decision D7: under `--store azure` every `--s3-*` and `--gcs-*`
//! flag, `--s3-kms-key`, `--tenant-kms-config` and
//! `--require-bucket-protection` is refused, and under `--store s3`, `gcs` or
//! `memory` every `--azure-*` flag is, whether the flag was passed on the
//! command line or set through its environment variable. Each case runs the
//! shipped binary, since an environment variable has to be set on a child
//! process.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::io::Read;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// The built `ravel-server` binary.
const BINARY: &str = env!("CARGO_BIN_EXE_ravel-server");

/// How long a refusal may take to exit. A refusal is argument parsing and
/// nothing else, so reaching this means the process started up instead.
const EXIT_DEADLINE: Duration = Duration::from_secs(20);

const AZURE_STORE: [&str; 6] = [
    "--store",
    "azure",
    "--azure-account",
    "acct",
    "--azure-container",
    "cont",
];

/// The flags `--store azure` refuses, with their environment variable (empty
/// for a flag that has none) and a value the flag parses. Several values
/// equal the flag's default: the refusal reads where a value came from, not
/// what it is.
const REFUSED_UNDER_AZURE: [(&str, &str, &str); 17] = [
    (
        "--s3-endpoint",
        "RAVEL_S3_ENDPOINT",
        "http://127.0.0.1:9000",
    ),
    ("--s3-allow-http", "RAVEL_S3_ALLOW_HTTP", "true"),
    ("--s3-bucket", "RAVEL_S3_BUCKET", "b"),
    ("--s3-region", "RAVEL_S3_REGION", "us-east-1"),
    ("--s3-access-key", "RAVEL_S3_ACCESS_KEY", "k"),
    ("--s3-secret-key", "RAVEL_S3_SECRET_KEY", "s"),
    ("--s3-auth", "RAVEL_S3_AUTH", "static"),
    ("--s3-session-token", "RAVEL_S3_SESSION_TOKEN", "t"),
    ("--s3-credentials-file", "RAVEL_S3_CREDENTIALS_FILE", "/x"),
    (
        "--s3-instance-metadata-endpoint",
        "RAVEL_S3_INSTANCE_METADATA_ENDPOINT",
        "http://169.254.169.254",
    ),
    (
        "--s3-upload-integrity",
        "RAVEL_S3_UPLOAD_INTEGRITY",
        "crc64nvme",
    ),
    (
        "--s3-request-stored-checksum",
        "RAVEL_S3_REQUEST_STORED_CHECKSUM",
        "true",
    ),
    ("--s3-kms-key", "RAVEL_S3_KMS_KEY", "arn:aws:kms:k"),
    ("--tenant-kms-config", "RAVEL_TENANT_KMS_CONFIG", "/x.toml"),
    (
        "--require-bucket-protection",
        "RAVEL_REQUIRE_BUCKET_PROTECTION",
        "true",
    ),
    ("--gcs-bucket", "RAVEL_GCS_BUCKET", "b"),
    ("--gcs-endpoint", "", "http://127.0.0.1:4443"),
];

/// The `--azure-*` flags, refused under every other store.
const AZURE_FLAGS: [(&str, &str, &str); 3] = [
    ("--azure-account", "RAVEL_AZURE_ACCOUNT", "acct"),
    ("--azure-container", "RAVEL_AZURE_CONTAINER", "cont"),
    ("--azure-endpoint", "", "http://127.0.0.1:10000/acct"),
];

fn drain(mut pipe: impl Read + Send + 'static) -> std::thread::JoinHandle<String> {
    std::thread::spawn(move || {
        let mut bytes = Vec::new();
        let _ = pipe.read_to_end(&mut bytes);
        String::from_utf8_lossy(&bytes).into_owned()
    })
}

/// Runs the binary with `args` on ephemeral loopback ports, every variable
/// named above removed and then `env` set, and returns `(status code,
/// stderr)`. A process still running at [`EXIT_DEADLINE`] started up instead
/// of refusing, and fails the test.
fn run(args: &[&str], env: &[(&str, &str)]) -> (Option<i32>, String) {
    let mut command = Command::new(BINARY);
    command
        .args(args)
        .args([
            "--listen-http",
            "127.0.0.1:0",
            "--listen-grpc",
            "127.0.0.1:0",
            "--tenant-hash-unkeyed",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (_, name, _) in REFUSED_UNDER_AZURE.iter().chain(AZURE_FLAGS.iter()) {
        if !name.is_empty() {
            command.env_remove(name);
        }
    }
    command.envs(env.iter().copied());
    let mut child = command
        .spawn()
        .unwrap_or_else(|e| panic!("the built binary at {BINARY} must be runnable: {e}"));
    let _stdout = drain(child.stdout.take().expect("stdout is piped"));
    let stderr = drain(child.stderr.take().expect("stderr is piped"));
    let deadline = Instant::now() + EXIT_DEADLINE;
    let status = loop {
        if let Some(status) = child.try_wait().expect("poll the child") {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!(
                "`ravel-server {args:?}` with {env:?} was still running after {EXIT_DEADLINE:?}: \
                 it started instead of refusing; stderr: {}",
                stderr.join().unwrap_or_default()
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    (status.code(), stderr.join().expect("stderr reader"))
}

/// Asserts the run exited 2 with an error naming `flag` and `store`.
fn assert_refused(case: &str, (code, stderr): (Option<i32>, String), flag: &str, store: &str) {
    assert_eq!(code, Some(2), "{case}: {stderr}");
    assert!(
        stderr.contains(flag) && stderr.contains(store),
        "{case}: the refusal must name {flag} and {store}, got: {stderr}"
    );
}

/// One run of `flag` set to `value`: through its environment variable when it
/// has one, which the in-process tests in `config.rs` cannot reach, and on the
/// command line when it has none.
fn run_with(base: &[&str], flag: &str, name: &str, value: &str) -> (String, (Option<i32>, String)) {
    if name.is_empty() {
        let passed = format!("{flag}={value}");
        let mut args = base.to_vec();
        args.push(&passed);
        let outcome = run(&args, &[]);
        (format!("{base:?} {passed}"), outcome)
    } else {
        (
            format!("{base:?} {name}={value}"),
            run(base, &[(name, value)]),
        )
    }
}

/// Every flag `--store azure` refuses exits 2 with an error naming the flag.
#[test]
fn azure_store_refuses_each_s3_and_gcs_flag() {
    for (flag, name, value) in REFUSED_UNDER_AZURE {
        let (case, outcome) = run_with(&AZURE_STORE, flag, name, value);
        assert_refused(&case, outcome, flag, "--store azure");
    }
}

/// Every `--azure-*` flag exits 2 with an error naming the flag under
/// `--store memory`, `s3` and `gcs`.
#[test]
fn other_stores_refuse_each_azure_flag() {
    for store in [
        &["--store", "memory"][..],
        &["--store", "s3"][..],
        &["--store", "gcs", "--gcs-bucket", "b"][..],
    ] {
        for (flag, name, value) in AZURE_FLAGS {
            let (case, outcome) = run_with(store, flag, name, value);
            assert_refused(&case, outcome, flag, "--store azure");
        }
    }
}

/// A defaulted `--s3-request-stored-checksum` (it defaults to true) is not
/// refused under `--store azure`: the run gets past argument parsing to the
/// startup check of `--azure-endpoint`, which refuses plaintext to a host on
/// the network. `RAVEL_AZURE_ACCOUNT` and `RAVEL_AZURE_CONTAINER` satisfy the
/// two required flags on the way.
#[test]
fn defaulted_s3_flag_is_not_refused_under_azure() {
    let (code, stderr) = run(
        &[
            "--store",
            "azure",
            "--azure-endpoint",
            "http://blob.example.com/acct",
        ],
        &[
            ("RAVEL_AZURE_ACCOUNT", "acct"),
            ("RAVEL_AZURE_CONTAINER", "cont"),
        ],
    );
    assert_eq!(code, Some(1), "{stderr}");
    assert!(
        !stderr.contains("--s3-") && !stderr.contains("requires --azure-"),
        "no S3 flag and no missing Azure flag may be the refusal, got: {stderr}"
    );
    assert!(
        stderr.contains("--azure-endpoint \"http://blob.example.com/acct\" is refused"),
        "the run must reach the endpoint check, got: {stderr}"
    );
}
