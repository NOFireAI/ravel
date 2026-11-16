//! The binary's tracing subscriber (issue #3854): library tracing reaches the
//! operator on stderr, never on stdout, which carries the reports and JSON
//! documents callers parse.
//!
//! The bucket is the fake S3 endpoint this crate's tests use, since a memory
//! store starts empty in every process and could not carry seeded L0 objects
//! into the binary.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[allow(clippy::expect_used, dead_code)]
#[path = "../src/fake_s3.rs"]
mod fake_s3;

use std::process::{Command, Output};
use std::sync::Arc;

use clap::Parser;
use ravel_cli::store;
use ravel_object_store::ObjectStoreBackend;

const BINARY: &str = env!("CARGO_BIN_EXE_ravel-cli");
const TENANT: &str = "acme";
/// An ingest hour in 1970, sealed against the binary's real wall clock.
const HOUR: u32 = 100;
const MODE_LINE: &str = "rlog merge read mode";

fn store_flags(endpoint: &str) -> Vec<String> {
    [
        "--store",
        "s3",
        "--s3-bucket",
        "ravel-test",
        "--s3-endpoint",
        endpoint,
        "--s3-access-key",
        "test",
        "--s3-secret-key",
        "test",
    ]
    .map(str::to_string)
    .to_vec()
}

/// A fake S3 bucket holding two L0 logs objects in shard 0 of `HOUR`, enough
/// for a compaction to merge them.
async fn seeded_bucket() -> (String, Arc<fake_s3::FakeS3>) {
    let (endpoint, fake) = fake_s3::spawn(fake_s3::Echo::Stored, &[]).await;
    let mut argv = vec!["ravel-cli".to_string()];
    argv.extend(store_flags(&endpoint));
    let args = store::StoreArgs::try_parse_from(argv).expect("store flags parse");
    let plain = store::build_store(&args).expect("plain store");
    fake_s3::seed::two_l0_logs(plain.as_ref() as &dyn ObjectStoreBackend, TENANT, 0, HOUR).await;
    (endpoint, fake)
}

/// Run the binary with the store flags and `command`. `rust_log` of `None`
/// leaves `RUST_LOG` unset.
async fn run_binary(endpoint: &str, command: &[&str], rust_log: Option<&str>) -> Output {
    let mut cmd = Command::new(BINARY);
    cmd.args(store_flags(endpoint))
        .arg("--tenant-hash-unkeyed")
        .args(command)
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .env_remove("RUST_LOG");
    if let Some(filter) = rust_log {
        cmd.env("RUST_LOG", filter);
    }
    let out = tokio::task::spawn_blocking(move || cmd.output())
        .await
        .expect("join")
        .expect("run ravel-cli");
    assert!(
        out.status.success(),
        "ravel-cli {command:?} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    out
}

const COMPACT_TENANT: [&str; 12] = [
    "maintain",
    "compact-tenant",
    "--tenant",
    TENANT,
    "--signal",
    "logs",
    "--shards",
    "1",
    "--from-hour",
    "100",
    "--to-hour",
    "100",
];

const COMPACT_BUCKET: [&str; 10] = [
    "maintain",
    "compact-bucket",
    "--tenant",
    TENANT,
    "--signal",
    "logs",
    "--shard",
    "0",
    "--hour",
    "100",
];

fn text(bytes: &[u8]) -> String {
    String::from_utf8(bytes.to_vec()).expect("utf-8 output")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compact_tenant_logs_the_read_mode_on_stderr_not_stdout_by_default() {
    let (endpoint, fake) = seeded_bucket().await;
    let out = run_binary(&endpoint, &COMPACT_TENANT, None).await;
    let (stdout, stderr) = (text(&out.stdout), text(&out.stderr));
    assert!(
        fake.puts().iter().any(|put| put.key.contains("/l1/")),
        "the run compacted nothing, so it never planned a merge\nstdout:\n{stdout}"
    );
    assert_eq!(
        stderr.matches(MODE_LINE).count(),
        1,
        "stderr:\n{stderr}\nstdout:\n{stdout}"
    );
    assert!(!stdout.contains(MODE_LINE), "stdout:\n{stdout}");
    assert!(stdout.contains("compacted"), "stdout:\n{stdout}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compact_bucket_logs_the_read_mode_on_stderr_not_stdout_by_default() {
    let (endpoint, _fake) = seeded_bucket().await;
    let out = run_binary(&endpoint, &COMPACT_BUCKET, None).await;
    let (stdout, stderr) = (text(&out.stdout), text(&out.stderr));
    assert_eq!(
        stderr.matches(MODE_LINE).count(),
        1,
        "stderr:\n{stderr}\nstdout:\n{stdout}"
    );
    assert!(!stdout.contains(MODE_LINE), "stdout:\n{stdout}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rust_log_off_silences_the_read_mode_line() {
    let (endpoint, fake) = seeded_bucket().await;
    let out = run_binary(&endpoint, &COMPACT_TENANT, Some("off")).await;
    let (stdout, stderr) = (text(&out.stdout), text(&out.stderr));
    assert!(
        fake.puts().iter().any(|put| put.key.contains("/l1/")),
        "the run compacted nothing\nstdout:\n{stdout}"
    );
    assert!(!stderr.contains(MODE_LINE), "stderr:\n{stderr}");
    assert!(!stdout.contains(MODE_LINE), "stdout:\n{stdout}");
}

/// Exactly one JSON document on stdout, whatever the filter lets through.
fn assert_one_json_document(stdout: &[u8]) {
    let docs: Vec<serde_json::Value> = serde_json::Deserializer::from_slice(stdout)
        .into_iter::<serde_json::Value>()
        .collect::<Result<_, _>>()
        .unwrap_or_else(|e| panic!("stdout is not JSON ({e}):\n{}", text(stdout)));
    assert_eq!(docs.len(), 1, "stdout:\n{}", text(stdout));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn json_report_stays_one_document_on_stdout_under_every_filter() {
    let fold = [
        "catalog", "fold", "--tenant", TENANT, "--shards", "1", "--signal", "logs", "--json",
    ];
    for rust_log in [None, Some("trace")] {
        let (endpoint, _fake) = seeded_bucket().await;
        let compact = run_binary(&endpoint, &COMPACT_TENANT, rust_log).await;
        assert!(
            text(&compact.stderr).contains(MODE_LINE),
            "RUST_LOG={rust_log:?} stderr:\n{}",
            text(&compact.stderr)
        );
        let out = run_binary(&endpoint, &fold, rust_log).await;
        assert_one_json_document(&out.stdout);
    }
}
