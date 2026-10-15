//! ADR-2708 D5c (issue #2724): `--format-window-check` run through the SHIPPED
//! BINARY against a bucket whose catalog names an out-of-window entry.
//!
//! The unit tests in `format_window.rs` pin the check and the refuse/warn
//! decision. This pins what only `main` decides: that the check runs at all
//! when asked and not by default, that `refuse` turns into a non-zero exit status before the server comes up,
//! and that `warn` leaves the same bucket starting.
//!
//! The bucket is the fake S3 endpoint ravel-cli's tests use, since a memory
//! store starts empty in every process and could not carry the seeded catalog
//! into the binary.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[allow(clippy::expect_used, dead_code)]
#[path = "../../ravel-cli/src/fake_s3.rs"]
mod fake_s3;

use std::io::Read;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use clap::Parser;
use ravel_catalog::{HEAD_FORMAT_VERSION, encode_head, encode_part};
use ravel_object_store::conformance::{
    CONFORMANCE_SUITE_VERSION, QUALIFICATION_KEY, QualificationRecord, s3_backend_identity,
};
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_proto::catalog::v1::{SnapshotEntry, SnapshotHead, SnapshotPartRef};
use ravel_server::config::Cli;
use ravel_types::{Signal, TenantId};

const BINARY: &str = env!("CARGO_BIN_EXE_ravel-server");
const BUCKET: &str = "format-window";
const TENANT: &str = "acme";

/// How long either run may take to reach the line it is waiting for. Not an
/// assertion on duration: reaching it fails the test with the output so far.
const DEADLINE: Duration = Duration::from_secs(60);

/// The RLOG version one below this build's reader window floor.
fn out_of_window_rlog_version() -> u32 {
    u32::from(ravel_logseg::footer::SUPPORTED_VERSIONS.oldest()) - 1
}

fn s3_args(endpoint: &str) -> Vec<String> {
    [
        "--store",
        "s3",
        "--s3-bucket",
        BUCKET,
        "--s3-endpoint",
        endpoint,
        "--s3-access-key",
        "test",
        "--s3-secret-key",
        "test",
        "--maintain-tenant",
        TENANT,
        "--tenant-hash-unkeyed",
        "--listen-http",
        "127.0.0.1:0",
        "--listen-grpc",
        "127.0.0.1:0",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

/// A qualified bucket whose `acme` alerts catalog holds one entry at an RLOG
/// version outside the reader window.
async fn seeded_bucket() -> (String, Arc<fake_s3::FakeS3>) {
    let (endpoint, fake) = fake_s3::spawn(fake_s3::Echo::Stored, &[]).await;
    let mut argv = vec!["ravel-server".to_string()];
    argv.extend(s3_args(&endpoint));
    let cli = Cli::try_parse_from(argv).expect("flags parse");
    let store =
        ravel_server::store::build_store(&cli, ravel_server::config::DEFAULT_CACHE_MAX_BYTES)
            .expect("build the S3 store")
            .foreground;

    let record = QualificationRecord {
        suite_version: CONFORMANCE_SUITE_VERSION,
        backend_identity: s3_backend_identity(Some(BUCKET), Some(&endpoint)),
        qualified_unix_ns: 1,
        passed_properties: Vec::new(),
    };
    put(
        store.as_ref(),
        QUALIFICATION_KEY,
        serde_json::to_vec(&record).expect("encode qualification"),
    )
    .await;

    let tenant = TenantId::new(TENANT).hash();
    let signal = Signal::Alerts;
    let signal_code = ravel_commit::signal::to_proto(signal) as u32;
    let entry = SnapshotEntry {
        level: 0,
        shard: 0,
        ingest_hour_bucket: 10,
        writer_id: vec![0xAA; 16],
        writer_epoch: 1,
        writer_seq: 1,
        content_hash: vec![0xBB; 32],
        object_size: 100,
        min_event_ts_ns: 0,
        max_event_ts_ns: 100,
        sample_count: 1,
        series_count: 1,
        segment_format_version: out_of_window_rlog_version(),
        created_unix_ns: 1_000,
        declared_column_stats: Vec::new(),
    };
    let part = encode_part(tenant.0, signal_code, 1, 10, &[entry]).expect("encode part");
    let part_key = format!(
        "t/{}/catalog/{}/snap/format-window-part",
        tenant.to_hex(),
        signal.key_prefix()
    );
    let head = SnapshotHead {
        format_version: HEAD_FORMAT_VERSION,
        tenant_hash: tenant.0.to_vec(),
        signal: signal_code,
        shard_count: 1,
        watermark_hour: 10,
        parts: vec![SnapshotPartRef {
            key: part_key.clone(),
            blake3: blake3::hash(&part).as_bytes().to_vec(),
            size: part.len() as u64,
            entry_count: 1,
            watermark_hour: 10,
            min_hour: 0,
            column_stats: None,
            key_index: Vec::new(),
        }],
        postings: None,
        folder_id: vec![0x33; 16],
        created_unix_ns: 0,
        shard_generation_count: 1,
    };
    put(store.as_ref(), &part_key, part).await;
    put(
        store.as_ref(),
        &format!("t/{}/catalog/{}/HEAD", tenant.to_hex(), signal.key_prefix()),
        encode_head(&head).expect("encode head"),
    )
    .await;
    (endpoint, fake)
}

async fn put(store: &dyn ObjectStoreBackend, key: &str, body: Vec<u8>) {
    store
        .put(key, Bytes::from(body), PutOptions::default())
        .await
        .unwrap_or_else(|e| panic!("seed {key}: {e}"));
}

/// Drains one child pipe on its own thread into a shared buffer the caller
/// can read while the process still runs.
fn drain(mut pipe: impl Read + Send + 'static) -> Pipe {
    let out = Arc::new(Mutex::new(String::new()));
    let sink = Arc::clone(&out);
    let reader = std::thread::spawn(move || {
        let mut buf = [0u8; 4096];
        while let Ok(n) = pipe.read(&mut buf) {
            if n == 0 {
                break;
            }
            sink.lock()
                .expect("pipe buffer")
                .push_str(&String::from_utf8_lossy(&buf[..n]));
        }
    });
    Pipe { out, reader }
}

struct Pipe {
    out: Arc<Mutex<String>>,
    reader: std::thread::JoinHandle<()>,
}

impl Pipe {
    fn read(&self) -> String {
        self.out.lock().expect("pipe buffer").clone()
    }

    /// Everything the pipe carried; returns once the process closed it.
    fn finish(self) -> String {
        let _ = self.reader.join();
        self.out.lock().expect("pipe buffer").clone()
    }
}

/// `mode` of `None` passes no `--format-window-check`, so the default applies.
fn spawn_binary(endpoint: &str, mode: Option<&str>) -> (Child, Pipe, Pipe) {
    let mode_args: Vec<&str> = mode
        .map(|mode| vec!["--format-window-check", mode])
        .unwrap_or_default();
    let mut child = Command::new(BINARY)
        .args(s3_args(endpoint))
        .args(mode_args)
        .env("NO_COLOR", "1")
        .env("RUST_LOG", "info")
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| panic!("the built binary at {BINARY} must be runnable: {e}"));
    let stdout = drain(child.stdout.take().expect("stdout is piped"));
    let stderr = drain(child.stderr.take().expect("stderr is piped"));
    (child, stdout, stderr)
}

/// With `refuse`, the binary exits non-zero before it serves, and stderr
/// names the signal and the version.
///
/// Flip to watch it fail: make `format_window::enforce` return `Ok(())` under
/// `Refuse` as well (log, then start). The process then serves instead of
/// exiting and this fails at [`DEADLINE`] with "did not exit".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refuse_exits_non_zero_naming_signal_and_version() {
    let (endpoint, _fake) = seeded_bucket().await;
    let version = out_of_window_rlog_version();
    let (code, stdout, stderr) = tokio::task::spawn_blocking(move || {
        let (mut child, stdout, stderr) = spawn_binary(&endpoint, Some("refuse"));
        let deadline = Instant::now() + DEADLINE;
        let status = loop {
            if let Some(status) = child.try_wait().expect("poll the child") {
                break status;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!(
                    "`--format-window-check refuse` did not exit: it started up instead of \
                     refusing; stdout: {}; stderr: {}",
                    stdout.read(),
                    stderr.read()
                );
            }
            std::thread::sleep(Duration::from_millis(50));
        };
        (status.code(), stdout.finish(), stderr.finish())
    })
    .await
    .expect("the binary run");

    assert!(
        matches!(code, Some(c) if c != 0),
        "refuse must exit non-zero, got {code:?}; stdout: {stdout}; stderr: {stderr}"
    );
    assert!(
        stderr.contains(&format!("signal alerts version {version}")),
        "stderr must name the signal and version; stderr: {stderr}"
    );
    assert!(
        !stdout.contains("ravel-server listening"),
        "refuse must stop before the server listens; stdout: {stdout}"
    );
}

/// Runs the binary with `mode` until it reports listening, and returns its
/// stdout up to that point. Fails if it exits or never listens.
async fn run_until_listening(endpoint: String, mode: Option<&'static str>) -> String {
    tokio::task::spawn_blocking(move || {
        let (mut child, stdout, stderr) = spawn_binary(&endpoint, mode);
        let deadline = Instant::now() + DEADLINE;
        loop {
            let out = stdout.read();
            if out.contains("ravel-server listening") {
                let _ = child.kill();
                let _ = child.wait();
                return out;
            }
            if let Some(status) = child.try_wait().expect("poll the child") {
                panic!(
                    "`--format-window-check {mode:?}` exited ({status}) instead of starting; \
                     stdout: {out}; stderr: {}",
                    stderr.read()
                );
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!(
                    "`--format-window-check {mode:?}` never reported listening; stdout: {out}; \
                     stderr: {}",
                    stderr.read()
                );
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    })
    .await
    .expect("the binary run")
}

/// With no flag, the check is off: the same bucket that `refuse` stops
/// starts, and no format-window check line is logged at info, which the
/// check emits on every path it runs (a finding, an unchecked pair, a clean
/// report, or no tenant).
///
/// Flip to watch it fail: make `format_window::should_run` return `true` for
/// `Off`. The check then runs and logs the out-of-window warning.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_default_starts_without_running_the_check() {
    let (endpoint, _fake) = seeded_bucket().await;
    let stdout = run_until_listening(endpoint, None).await;
    assert!(
        !stdout.contains("format-window check"),
        "the default must not run the check; stdout: {stdout}"
    );
}

/// With `warn`, the same bucket starts, and the warning names the signal and
/// version.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn warn_starts_and_logs_the_out_of_window_entry() {
    let (endpoint, _fake) = seeded_bucket().await;
    let version = out_of_window_rlog_version();
    let stdout = run_until_listening(endpoint, Some("warn")).await;

    let warning = stdout
        .lines()
        .find(|line| line.contains("outside this build's reader window"))
        .unwrap_or_else(|| panic!("warn must log the out-of-window entry; stdout: {stdout}"));
    assert!(warning.contains("WARN"), "{warning}");
    assert!(warning.contains("signal=\"alerts\""), "{warning}");
    assert!(warning.contains(&format!("version={version}")), "{warning}");
    assert!(warning.contains("entries=1"), "{warning}");
}
