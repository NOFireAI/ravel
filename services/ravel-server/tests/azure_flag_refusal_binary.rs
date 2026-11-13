//! ADR-4158 decision D7: under `--store azure` every S3 store flag and every
//! GCS flag is refused, and under every other store every Azure flag is
//! refused, whether the flag came from the command line or its environment
//! variable. The refusal comes out of the shipped binary.
//!
//! `config.rs` pins the command-line forms in-process. The environment form
//! needs the variable set on a child process, which a test cannot do
//! in-process, so each case runs the real binary.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::io::Read;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use ravel_object_store::azure::fake::FakeAzure;
use ravel_object_store::conformance::QUALIFICATION_KEY;

/// The built `ravel-server` binary.
const BINARY: &str = env!("CARGO_BIN_EXE_ravel-server");

/// How long a refusal may take to exit. A refusal is argument parsing and
/// nothing else, so reaching this means the process started up instead.
const EXIT_DEADLINE: Duration = Duration::from_secs(20);

const AZURE: [&str; 6] = [
    "--store",
    "azure",
    "--azure-account",
    "acct",
    "--azure-container",
    "cont",
];

/// Each S3 store flag and its environment variable, with a value the flag
/// parses. Several values equal the flag's default: the refusal reads where a
/// value came from, not what it is.
const S3_ONLY: [(&str, &str, &str); 15] = [
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

/// Store variables every run starts without, so the host environment cannot
/// decide a case.
const CLEARED: [&str; 3] = [
    "RAVEL_GCS_BUCKET",
    "RAVEL_AZURE_ACCOUNT",
    "RAVEL_AZURE_CONTAINER",
];

fn drain(mut pipe: impl Read + Send + 'static) -> std::thread::JoinHandle<String> {
    std::thread::spawn(move || {
        let mut bytes = Vec::new();
        let _ = pipe.read_to_end(&mut bytes);
        String::from_utf8_lossy(&bytes).into_owned()
    })
}

/// Runs the binary with `args` on ephemeral loopback ports, every variable in
/// [`S3_ONLY`] and [`CLEARED`] removed and then `env` set, and returns
/// `(status code, stderr)`. A process still running at [`EXIT_DEADLINE`]
/// started up instead of exiting, and fails the test.
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
    for (_, name, _) in S3_ONLY {
        command.env_remove(name);
    }
    for name in CLEARED {
        command.env_remove(name);
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
                 it started instead of exiting; stderr: {}",
                stderr.join().unwrap_or_default()
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    (status.code(), stderr.join().expect("stderr reader"))
}

/// Asserts a refusal: exit 2, and stderr names `flag` and `store`.
fn assert_refused(case: &str, (code, stderr): (Option<i32>, String), flag: &str, store: &str) {
    assert_eq!(code, Some(2), "{case}: {stderr}");
    assert!(
        stderr.contains(flag) && stderr.contains(store),
        "{case}: the refusal must name {flag} and {store}, got: {stderr}"
    );
}

/// Every S3 store flag, set only through its environment variable, exits 2
/// under `--store azure` with an error naming the flag.
#[test]
fn azure_store_refuses_each_s3_flag_set_through_the_environment() {
    for (flag, name, value) in S3_ONLY {
        assert_refused(
            &format!("{name}={value}"),
            run(&AZURE, &[(name, value)]),
            flag,
            "--store azure",
        );
    }
}

/// Both GCS flags exit 2 under `--store azure`: `--gcs-bucket` through
/// `RAVEL_GCS_BUCKET`, and `--gcs-endpoint`, which has no variable, on the
/// command line.
#[test]
fn azure_store_refuses_the_gcs_flags() {
    assert_refused(
        "RAVEL_GCS_BUCKET",
        run(&AZURE, &[("RAVEL_GCS_BUCKET", "b")]),
        "--gcs-bucket",
        "--store gcs",
    );
    let mut args = AZURE.to_vec();
    args.extend(["--gcs-endpoint", "http://127.0.0.1:4443"]);
    assert_refused(
        "--gcs-endpoint",
        run(&args, &[]),
        "--gcs-endpoint",
        "--store gcs",
    );
}

/// Every Azure flag exits 2 under `--store s3`, `--store gcs` and `--store
/// memory`: the account and the container through their variables, and
/// `--azure-endpoint`, which has no variable, on the command line.
#[test]
fn other_stores_refuse_each_azure_flag() {
    let stores: [&[&str]; 3] = [
        &["--store", "s3", "--s3-bucket", "b"],
        &["--store", "gcs", "--gcs-bucket", "b"],
        &["--store", "memory"],
    ];
    for base in stores {
        for (flag, name) in [
            ("--azure-account", "RAVEL_AZURE_ACCOUNT"),
            ("--azure-container", "RAVEL_AZURE_CONTAINER"),
        ] {
            assert_refused(
                &format!("{base:?} {name}"),
                run(base, &[(name, "x")]),
                flag,
                "--store azure",
            );
        }
        let mut args = base.to_vec();
        args.extend(["--azure-endpoint", "http://127.0.0.1:10000/acct"]);
        assert_refused(
            &format!("{base:?} --azure-endpoint"),
            run(&args, &[]),
            "--azure-endpoint",
            "--store azure",
        );
    }
}

/// `RAVEL_AZURE_ACCOUNT` and `RAVEL_AZURE_CONTAINER` are read as their
/// flags: together they satisfy `--store azure` (the refusal reached is the
/// S3 flag's, not a missing account's or container's).
#[test]
fn azure_environment_variables_are_read_as_the_flags() {
    let (code, stderr) = run(
        &["--store", "azure"],
        &[
            ("RAVEL_AZURE_ACCOUNT", "acct"),
            ("RAVEL_AZURE_CONTAINER", "cont"),
            ("RAVEL_S3_REGION", "us-east-1"),
        ],
    );
    assert_eq!(code, Some(2), "{stderr}");
    assert!(stderr.contains("--s3-region"), "{stderr}");
    assert!(!stderr.contains("requires --azure-"), "{stderr}");
}

/// A plain `--store azure` start, where every S3 flag keeps its default
/// (`--s3-request-stored-checksum` defaults to true), passes argument
/// parsing: the process reaches the qualification gate against a fake
/// container and refuses there, reading the record from the fake.
#[test]
fn azure_store_does_not_refuse_a_defaulted_s3_flag() {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let fake = runtime
        .block_on(FakeAzure::start("acct", "cont"))
        .expect("fake Azure starts");
    let mut args = AZURE.to_vec();
    args.extend(["--azure-endpoint", fake.endpoint()]);
    let (code, stderr) = run(&args, &[]);
    assert_ne!(code, Some(2), "{stderr}");
    assert_ne!(code, Some(0), "{stderr}");
    assert!(!stderr.contains("--s3-request-stored-checksum"), "{stderr}");
    assert!(stderr.contains("sys/qualification"), "{stderr}");
    assert!(
        fake.requests()
            .iter()
            .any(|request| request.method == "GET"
                && request.is_blob("acct", "cont", QUALIFICATION_KEY)),
        "the binary read the qualification record from the fake: {:?}",
        fake.requests()
    );
}
