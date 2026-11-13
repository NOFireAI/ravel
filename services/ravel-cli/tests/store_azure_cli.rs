//! Subprocess coverage of `--store azure` through the real binary (ADR-4158
//! D6, D7), against the in-process fake Azure Blob endpoint from
//! ravel-object-store's test-support feature.
//!
//! `store qualify` enters through `main` -> `run` -> `run_logged`, whose
//! `StoreCommand::Qualify` arm builds the store with
//! `store::build_store_handle(&cli.store, Some(list_page_size))` and writes the
//! record through `qualify::qualify_built(.., cli.store.backend_identity(), ..)`.
//! The fake needs no credentials; `RAVEL_AZURE_TEST_SKIP_SIGNATURE` (read only
//! by a test-support build of the binary) makes the store sign nothing.
#![allow(clippy::expect_used)]

use std::process::{Command, Output};

use ravel_object_store::azure::fake::FakeAzure;
use ravel_object_store::conformance::{
    CONFORMANCE_SUITE_VERSION, QUALIFICATION_KEY, QualificationRecord,
};

const ACCOUNT: &str = "ravelacct";
const CONTAINER: &str = "data";

fn ravel_cli(args: Vec<String>) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ravel-cli"));
    command
        .args(args)
        .env("RAVEL_AZURE_TEST_SKIP_SIGNATURE", "1");
    // Every variable a store flag or the Azure credential lookup reads, so a
    // developer's or CI's own environment cannot turn a test into a refusal.
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
        "RAVEL_GCS_BUCKET",
        "RAVEL_AZURE_ACCOUNT",
        "RAVEL_AZURE_CONTAINER",
        "RAVEL_TENANT_KMS_CONFIG",
        "AZURE_FEDERATED_TOKEN_FILE",
        "AZURE_CLIENT_ID",
        "AZURE_TENANT_ID",
        "AZURE_STORAGE_ACCOUNT_KEY",
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

fn azure_flags_for(fake: &FakeAzure, container: &str) -> Vec<String> {
    vec![
        "--store".into(),
        "azure".into(),
        "--azure-account".into(),
        ACCOUNT.into(),
        "--azure-container".into(),
        container.into(),
        "--azure-endpoint".into(),
        fake.endpoint().into(),
    ]
}

fn azure_flags(fake: &FakeAzure) -> Vec<String> {
    azure_flags_for(fake, CONTAINER)
}

fn args(base: Vec<String>, rest: &[&str]) -> Vec<String> {
    let mut args = base;
    args.extend(rest.iter().map(|arg| (*arg).to_string()));
    args
}

fn qualification_record(fake: &FakeAzure) -> QualificationRecord {
    let data = fake
        .data_of(QUALIFICATION_KEY)
        .expect("sys/qualification is written");
    serde_json::from_slice(&data).expect("sys/qualification decodes")
}

fn assert_success(output: &Output, what: &str) {
    assert!(
        output.status.success(),
        "{what} must pass; stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn assert_refused(output: &Output, expected: &[&str]) {
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "stderr:\n{stderr}");
    for needle in expected {
        assert!(
            stderr.contains(needle),
            "want {needle:?} in stderr:\n{stderr}"
        );
    }
}

fn assert_no_requests(fake: &FakeAzure) {
    let requests = fake.requests();
    assert!(
        requests.is_empty(),
        "no request reaches the container: {requests:?}"
    );
}

/// `store qualify --store azure` passes against the fake and writes
/// `sys/qualification` with the `az://<account>/<container>@<endpoint>`
/// identity.
#[tokio::test(flavor = "multi_thread")]
async fn qualify_azure_writes_az_identity() {
    let fake = FakeAzure::start(ACCOUNT, CONTAINER)
        .await
        .expect("fake Azure starts");
    let output = run(ravel_cli(args(azure_flags(&fake), &["store", "qualify"]))).await;
    assert_success(&output, "store qualify against the fake Azure");

    let record = qualification_record(&fake);
    assert_eq!(record.suite_version, CONFORMANCE_SUITE_VERSION);
    assert_eq!(
        record.backend_identity,
        format!("az://{ACCOUNT}/{CONTAINER}@{}", fake.endpoint())
    );
}

/// `--list-page-size 2` reaches the Azure lister: every List Blobs request
/// qualify sends carries `maxresults=2`.
#[tokio::test(flavor = "multi_thread")]
async fn list_page_size_reaches_the_azure_listing() {
    let fake = FakeAzure::start(ACCOUNT, CONTAINER)
        .await
        .expect("fake Azure starts");
    let output = run(ravel_cli(args(
        azure_flags(&fake),
        &["store", "qualify", "--list-page-size", "2"],
    )))
    .await;
    assert_success(&output, "store qualify --list-page-size 2");

    let listings: Vec<_> = fake
        .requests()
        .into_iter()
        .filter(|request| request.is_list())
        .collect();
    assert!(!listings.is_empty(), "qualify lists the container");
    for listing in &listings {
        assert_eq!(
            listing.query_param("maxresults"),
            Some("2"),
            "every listing carries the page size: {listing:?}"
        );
    }
}

/// `--azure-container` routes the requests: a qualify of each container
/// writes a record naming that container into that container, and a qualify
/// pointed at a container the endpoint does not serve fails, its report
/// naming `ContainerNotFound`, and leaves the served container's record
/// untouched.
#[tokio::test(flavor = "multi_thread")]
async fn qualification_of_one_container_is_not_one_of_another() {
    let data = FakeAzure::start(ACCOUNT, CONTAINER)
        .await
        .expect("fake Azure starts");
    let other = FakeAzure::start(ACCOUNT, "other")
        .await
        .expect("second fake Azure starts");
    for fake in [&data, &other] {
        let output = run(ravel_cli(args(
            azure_flags_for(fake, fake.container()),
            &["store", "qualify"],
        )))
        .await;
        assert_success(&output, "store qualify");
    }
    let data_identity = qualification_record(&data).backend_identity;
    let other_identity = qualification_record(&other).backend_identity;
    assert_eq!(
        data_identity,
        format!("az://{ACCOUNT}/{CONTAINER}@{}", data.endpoint())
    );
    assert_eq!(
        other_identity,
        format!("az://{ACCOUNT}/other@{}", other.endpoint())
    );

    let before = data.data_of(QUALIFICATION_KEY);
    let output = run(ravel_cli(args(
        azure_flags_for(&data, "other"),
        &["store", "qualify"],
    )))
    .await;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "qualify against a container the account does not serve must fail; stdout:\n{stdout}"
    );
    // The error on stderr names the container qualified, and the per-property
    // report on stdout carries the store's reason.
    assert!(
        stderr.contains(&format!(
            "store qualification failed: az://{ACCOUNT}/other@{}",
            data.endpoint()
        )),
        "stderr:\n{stderr}"
    );
    assert!(
        stdout.contains("the Azure container does not exist (ContainerNotFound)"),
        "the property failures name the missing container; stdout:\n{stdout}"
    );
    assert!(
        !stdout.contains(&format!("wrote {QUALIFICATION_KEY}")),
        "stdout:\n{stdout}"
    );
    assert_eq!(
        data.data_of(QUALIFICATION_KEY),
        before,
        "the served container's record is untouched"
    );
}

/// `store verify-protection --store azure` refuses with exit 2 before building
/// the store: the error says Azure container protection is not checked, no
/// report is printed, and the fake sees no request.
#[tokio::test(flavor = "multi_thread")]
async fn verify_protection_refuses_under_azure() {
    let fake = FakeAzure::start(ACCOUNT, CONTAINER)
        .await
        .expect("fake Azure starts");
    let output = run(ravel_cli(args(
        azure_flags(&fake),
        &[
            "store",
            "verify-protection",
            "--expected-noncurrent-days",
            "30",
        ],
    )))
    .await;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(2), "stderr:\n{stderr}");
    assert!(
        stderr.contains(
            "store verify-protection is refused under --store azure: Azure container protection \
             is not checked"
        ),
        "stderr:\n{stderr}"
    );
    assert_eq!(stdout, "", "no report is printed");
    assert_no_requests(&fake);
}

/// A `--s3-*` flag given under `--store azure` refuses before any request,
/// naming the flag; the defaulted `--s3-upload-integrity` and
/// `--s3-request-stored-checksum` do not (the qualify tests run with both
/// defaulted).
#[tokio::test(flavor = "multi_thread")]
async fn given_s3_flag_is_refused_under_azure() {
    let fake = FakeAzure::start(ACCOUNT, CONTAINER)
        .await
        .expect("fake Azure starts");
    let output = run(ravel_cli(args(
        azure_flags(&fake),
        &["--s3-upload-integrity", "sha256", "store", "qualify"],
    )))
    .await;
    assert_refused(
        &output,
        &[
            "--store azure refuses S3 and GCS flags",
            "--s3-upload-integrity (RAVEL_S3_UPLOAD_INTEGRITY) given",
        ],
    );
    assert_no_requests(&fake);
}

/// A `--gcs-*` flag set through its environment variable under `--store
/// azure` refuses the same way.
#[tokio::test(flavor = "multi_thread")]
async fn given_gcs_flag_is_refused_under_azure() {
    let fake = FakeAzure::start(ACCOUNT, CONTAINER)
        .await
        .expect("fake Azure starts");
    let mut command = ravel_cli(args(azure_flags(&fake), &["store", "qualify"]));
    command.env("RAVEL_GCS_BUCKET", "leftover");
    let output = run(command).await;
    assert_refused(
        &output,
        &[
            "--store azure refuses S3 and GCS flags",
            "--gcs-bucket (RAVEL_GCS_BUCKET) given",
        ],
    );
    assert_no_requests(&fake);
}

/// Under `--store s3` a given `--azure-*` flag refuses before the S3 store is
/// built, naming the flag.
#[test]
fn given_azure_flag_is_refused_under_s3() {
    let output = ravel_cli(
        [
            "--store",
            "s3",
            "--s3-bucket",
            "b",
            "--s3-access-key",
            "k",
            "--s3-secret-key",
            "s",
            "--s3-endpoint",
            "http://127.0.0.1:9",
            "--azure-account",
            "acct",
            "store",
            "qualify",
        ]
        .map(String::from)
        .to_vec(),
    )
    .output()
    .expect("ravel-cli runs");
    assert_refused(
        &output,
        &[
            "--store s3 refuses Azure flags",
            "--azure-account (RAVEL_AZURE_ACCOUNT) given",
        ],
    );
}

/// Under `--store gcs` a given `--azure-*` flag, here from its environment
/// variable, refuses before any request, naming the flag.
#[test]
fn given_azure_flag_is_refused_under_gcs() {
    let mut command = ravel_cli(
        [
            "--store",
            "gcs",
            "--gcs-bucket",
            "b",
            "--gcs-endpoint",
            "http://127.0.0.1:9",
            "store",
            "qualify",
        ]
        .map(String::from)
        .to_vec(),
    );
    command.env("RAVEL_AZURE_CONTAINER", "leftover");
    let output = command.output().expect("ravel-cli runs");
    assert_refused(
        &output,
        &[
            "--store gcs refuses Azure flags",
            "--azure-container (RAVEL_AZURE_CONTAINER) given",
        ],
    );
}

/// Under `--store memory` the hidden `--azure-endpoint`, which has no
/// environment variable, is refused and named too.
#[test]
fn given_azure_flag_is_refused_under_memory() {
    let output = ravel_cli(
        [
            "--store",
            "memory",
            "--azure-endpoint",
            "http://127.0.0.1:9",
            "store",
            "qualify",
        ]
        .map(String::from)
        .to_vec(),
    )
    .output()
    .expect("ravel-cli runs");
    assert_refused(
        &output,
        &[
            "--store memory refuses Azure flags",
            "--azure-endpoint given",
        ],
    );
}

/// `--store azure` without `--azure-account` or `--azure-container` refuses
/// and names the missing flag.
#[test]
fn account_and_container_are_required_under_azure() {
    for (flags, missing) in [
        (
            vec!["--azure-container", "c"],
            "--store azure requires --azure-account (RAVEL_AZURE_ACCOUNT)",
        ),
        (
            vec!["--azure-account", "a"],
            "--store azure requires --azure-container (RAVEL_AZURE_CONTAINER)",
        ),
    ] {
        let mut argv = vec!["--store", "azure"];
        argv.extend(flags);
        argv.extend(["store", "qualify"]);
        let output = ravel_cli(argv.into_iter().map(String::from).collect())
            .output()
            .expect("ravel-cli runs");
        assert_refused(&output, &[missing]);
    }
}

/// `agents dev` drives the compose RustFS bucket, so `--store azure` refuses
/// with the shape of its `--store gcs` refusal, before the store is built.
#[tokio::test(flavor = "multi_thread")]
async fn agents_dev_refuses_azure() {
    let fake = FakeAzure::start(ACCOUNT, CONTAINER)
        .await
        .expect("fake Azure starts");
    let output = run(ravel_cli(args(
        azure_flags(&fake),
        &["--tenant-hash-unkeyed", "agents", "dev"],
    )))
    .await;
    assert_refused(
        &output,
        &["agents dev refuses --store azure: it starts, creates and qualifies the compose"],
    );
    assert_no_requests(&fake);
}

/// The tenant-KMS commands refuse `--tenant-kms-config` under `--store azure`
/// before any request.
#[tokio::test(flavor = "multi_thread")]
async fn tenant_kms_is_refused_under_azure() {
    let fake = FakeAzure::start(ACCOUNT, CONTAINER)
        .await
        .expect("fake Azure starts");
    let output = run(ravel_cli(args(
        azure_flags(&fake),
        &[
            "catalog",
            "fold",
            "--tenant",
            "t",
            "--tenant-kms-config",
            "/nonexistent/kms.json",
        ],
    )))
    .await;
    assert_refused(
        &output,
        &["--tenant-kms-config is refused under --store azure"],
    );
    assert_no_requests(&fake);
}
