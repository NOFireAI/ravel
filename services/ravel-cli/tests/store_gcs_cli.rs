//! Subprocess coverage of `--store gcs` through the real binary (ADR-3456 D5,
//! D6), against the in-process fake GCS from ravel-object-store's
//! test-support feature.
//!
//! `store qualify` enters through `main` -> `run` -> `run_logged`, whose
//! `StoreCommand::Qualify` arm builds the store with
//! `store::build_store_handle(&cli.store, Some(list_page_size))` and writes the
//! record through `qualify::qualify_built(.., cli.store.backend_identity(), ..)`.
//! The fake needs no credentials; `RAVEL_GCS_TEST_SKIP_SIGNATURE` (read only by
//! a test-support build of the binary) skips the ADC token fetch.
#![allow(clippy::expect_used)]

use std::process::{Command, Output};

use ravel_object_store::conformance::{
    CONFORMANCE_SUITE_VERSION, QUALIFICATION_KEY, QualificationRecord,
};
use ravel_object_store::gcs::fake::FakeGcs;

const BUCKET: &str = "b";
const LISTING_PATH: &str = "/storage/v1/b/b/o";

fn ravel_cli(args: Vec<String>) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ravel-cli"));
    command
        .args(args)
        .env("RAVEL_GCS_TEST_SKIP_SIGNATURE", "1")
        .env_remove("RAVEL_GCS_BUCKET");
    // Every variable the --store gcs refusal reads, so a developer's or CI's
    // own S3 environment cannot turn a test into a refusal.
    for var in [
        "RAVEL_S3_ACCESS_KEY",
        "RAVEL_S3_ALLOW_HTTP",
        "RAVEL_S3_AUTH",
        "RAVEL_S3_BUCKET",
        "RAVEL_S3_CREDENTIALS_FILE",
        "RAVEL_S3_ENDPOINT",
        "RAVEL_S3_INSTANCE_METADATA_ENDPOINT",
        "RAVEL_S3_REGION",
        "RAVEL_S3_REQUEST_STORED_CHECKSUM",
        "RAVEL_S3_SECRET_KEY",
        "RAVEL_S3_SESSION_TOKEN",
        "RAVEL_S3_UPLOAD_INTEGRITY",
    ] {
        command.env_remove(var);
    }
    command
}

async fn run(command: Command) -> Output {
    let mut command = command;
    tokio::task::spawn_blocking(move || command.output().expect("ravel-cli runs"))
        .await
        .expect("subprocess task joins")
}

fn gcs_flags(fake: &FakeGcs) -> Vec<String> {
    vec![
        "--store".into(),
        "gcs".into(),
        "--gcs-bucket".into(),
        BUCKET.into(),
        "--gcs-endpoint".into(),
        fake.endpoint().into(),
    ]
}

/// `store qualify --store gcs` writes `sys/qualification` with the `gs://`
/// identity, and `--list-page-size` reaches GcsStore as the listing's
/// `maxResults`.
#[tokio::test(flavor = "multi_thread")]
async fn qualify_gcs_writes_gs_identity() {
    let fake = FakeGcs::start(BUCKET).await.expect("fake GCS starts");
    let mut args = gcs_flags(&fake);
    args.extend(["store", "qualify", "--list-page-size", "2"].map(String::from));
    let output = run(ravel_cli(args)).await;
    assert!(
        output.status.success(),
        "store qualify against the fake GCS must pass; stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let data = fake
        .data_of(QUALIFICATION_KEY)
        .expect("sys/qualification is written");
    let record: QualificationRecord =
        serde_json::from_slice(&data).expect("sys/qualification decodes");
    assert_eq!(record.suite_version, CONFORMANCE_SUITE_VERSION);
    assert_eq!(
        record.backend_identity,
        format!("gs://{BUCKET}@{}", fake.endpoint())
    );

    let listings: Vec<_> = fake
        .requests()
        .into_iter()
        .filter(|request| request.method == "GET" && request.path == LISTING_PATH)
        .collect();
    assert!(!listings.is_empty(), "qualify lists the bucket");
    for listing in &listings {
        assert_eq!(
            listing.query_param("maxResults"),
            Some("2"),
            "every listing carries the page size: {listing:?}"
        );
    }
}

/// `store verify-protection --store gcs` refuses with exit 2 before building
/// the store: the error names the reason, no report is printed, and the fake
/// sees no request.
#[tokio::test(flavor = "multi_thread")]
async fn verify_protection_refuses_under_gcs() {
    let fake = FakeGcs::start(BUCKET).await.expect("fake GCS starts");
    let mut args = gcs_flags(&fake);
    args.extend(
        [
            "store",
            "verify-protection",
            "--expected-noncurrent-days",
            "30",
        ]
        .map(String::from),
    );
    let output = run(ravel_cli(args)).await;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(2), "stderr:\n{stderr}");
    assert!(
        stderr.contains(
            "store verify-protection is refused under --store gcs: GCS bucket protection \
             (retention policy, object versioning, soft delete) is not checked"
        ),
        "stderr:\n{stderr}"
    );
    assert_eq!(stdout, "", "no report is printed");
    let requests = fake.requests();
    assert!(
        requests.is_empty(),
        "no request reaches the bucket: {requests:?}"
    );
}

/// A `--s3-*` flag given on the command line under `--store gcs` refuses
/// before any request; the defaulted `--s3-upload-integrity` and
/// `--s3-request-stored-checksum` do not (the qualify test above runs with
/// both defaulted).
#[tokio::test(flavor = "multi_thread")]
async fn given_s3_flag_is_refused_under_gcs() {
    let fake = FakeGcs::start(BUCKET).await.expect("fake GCS starts");
    let mut args = gcs_flags(&fake);
    args.extend(["--s3-upload-integrity", "sha256", "store", "qualify"].map(String::from));
    let output = run(ravel_cli(args)).await;
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "stderr:\n{stderr}");
    assert!(
        stderr.contains(
            "--store gcs refuses S3 flags, which have no GCS implementation and would \
             otherwise be ignored: --s3-upload-integrity (RAVEL_S3_UPLOAD_INTEGRITY) given"
        ),
        "stderr:\n{stderr}"
    );
    let requests = fake.requests();
    assert!(
        requests.is_empty(),
        "no request reaches the bucket: {requests:?}"
    );
}

/// The same refusal for a `--s3-*` flag set through its environment variable.
#[tokio::test(flavor = "multi_thread")]
async fn s3_flag_from_environment_is_refused_under_gcs() {
    let fake = FakeGcs::start(BUCKET).await.expect("fake GCS starts");
    let mut args = gcs_flags(&fake);
    args.extend(["store", "qualify"].map(String::from));
    let mut command = ravel_cli(args);
    command.env("RAVEL_S3_BUCKET", "leftover");
    let output = run(command).await;
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "stderr:\n{stderr}");
    assert!(
        stderr.contains("--s3-bucket (RAVEL_S3_BUCKET) given"),
        "stderr:\n{stderr}"
    );
    let requests = fake.requests();
    assert!(
        requests.is_empty(),
        "no request reaches the bucket: {requests:?}"
    );
}

/// `--store gcs` without `--gcs-bucket` refuses and names the flag.
#[test]
fn gcs_bucket_is_required_under_gcs() {
    let output = ravel_cli(
        ["--store", "gcs", "store", "qualify"]
            .map(String::from)
            .to_vec(),
    )
    .output()
    .expect("ravel-cli runs");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "stderr:\n{stderr}");
    assert!(
        stderr.contains("--store gcs requires --gcs-bucket (RAVEL_GCS_BUCKET)"),
        "stderr:\n{stderr}"
    );
}

/// Every tenant-KMS command refuses `--tenant-kms-config` under `--store gcs`
/// before any request.
#[tokio::test(flavor = "multi_thread")]
async fn tenant_kms_is_refused_under_gcs() {
    let fake = FakeGcs::start(BUCKET).await.expect("fake GCS starts");
    let commands: [&[&str]; 4] = [
        &[
            "maintain",
            "compact-bucket",
            "--tenant",
            "t",
            "--signal",
            "metrics",
            "--shard",
            "0",
            "--hour",
            "0",
        ],
        &[
            "maintain",
            "compact-tenant",
            "--tenant",
            "t",
            "--signal",
            "metrics",
        ],
        &[
            "maintain", "migrate", "--tenant", "t", "--signal", "metrics",
        ],
        &["catalog", "fold", "--tenant", "t"],
    ];
    for command in commands {
        let mut args = gcs_flags(&fake);
        args.extend(command.iter().map(|arg| (*arg).to_string()));
        args.extend(["--tenant-kms-config", "/nonexistent/kms.json"].map(String::from));
        let output = run(ravel_cli(args)).await;
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!output.status.success(), "{command:?} stderr:\n{stderr}");
        assert!(
            stderr.contains("--tenant-kms-config is refused under --store gcs"),
            "{command:?} stderr:\n{stderr}"
        );
    }
    let requests = fake.requests();
    assert!(
        requests.is_empty(),
        "no request reaches the bucket: {requests:?}"
    );
}
