//! ADR-3456 decision D6: under `--store gcs` an S3 store flag set through its
//! environment variable is refused, the same as one passed on the command
//! line, and the refusal comes out of the shipped binary.
//!
//! `config.rs` pins the command-line form in-process. The environment form
//! needs the variable set on a child process, which a test cannot do
//! in-process, so each case runs the real binary.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::io::Read;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// The built `ravel-server` binary.
const BINARY: &str = env!("CARGO_BIN_EXE_ravel-server");

/// How long a refusal may take to exit. A refusal is argument parsing and
/// nothing else, so reaching this means the process started up instead.
const EXIT_DEADLINE: Duration = Duration::from_secs(20);

/// Each refused flag and its environment variable, with a value the flag
/// parses. Several values equal the flag's default: the refusal reads where a
/// value came from, not what it is.
const REFUSED: [(&str, &str, &str); 15] = [
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
];

fn drain(mut pipe: impl Read + Send + 'static) -> std::thread::JoinHandle<String> {
    std::thread::spawn(move || {
        let mut bytes = Vec::new();
        let _ = pipe.read_to_end(&mut bytes);
        String::from_utf8_lossy(&bytes).into_owned()
    })
}

/// Runs the binary with `args` on ephemeral loopback ports, every variable in
/// [`REFUSED`] and `RAVEL_GCS_BUCKET` removed and then `env` set, and returns
/// `(status code, stderr)`. A process still running at [`EXIT_DEADLINE`]
/// started up instead of refusing, and fails the test.
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
    for (_, name, _) in REFUSED {
        command.env_remove(name);
    }
    command.env_remove("RAVEL_GCS_BUCKET");
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

/// Every refused flag, set only through its environment variable, exits 2
/// under `--store gcs` with an error naming the flag.
#[test]
fn gcs_store_refuses_each_s3_flag_set_through_the_environment() {
    for (flag, name, value) in REFUSED {
        let (code, stderr) = run(&["--store", "gcs", "--gcs-bucket", "b"], &[(name, value)]);
        assert_eq!(code, Some(2), "{name}={value}: {stderr}");
        assert!(
            stderr.contains(flag) && stderr.contains("--store gcs"),
            "{name}={value}: the refusal must name {flag}, got: {stderr}"
        );
    }
}

/// `RAVEL_GCS_BUCKET` is read as `--gcs-bucket`: it satisfies the required
/// bucket under `--store gcs` (the refusal reached is the S3 flag's, not the
/// missing bucket's), and it is refused under another store.
#[test]
fn gcs_bucket_environment_variable_is_read_as_the_flag() {
    let (code, stderr) = run(
        &["--store", "gcs"],
        &[("RAVEL_GCS_BUCKET", "b"), ("RAVEL_S3_REGION", "us-east-1")],
    );
    assert_eq!(code, Some(2), "{stderr}");
    assert!(stderr.contains("--s3-region"), "{stderr}");
    assert!(!stderr.contains("requires --gcs-bucket"), "{stderr}");

    let (code, stderr) = run(&["--store", "memory"], &[("RAVEL_GCS_BUCKET", "b")]);
    assert_eq!(code, Some(2), "{stderr}");
    assert!(stderr.contains("--gcs-bucket"), "{stderr}");
}
