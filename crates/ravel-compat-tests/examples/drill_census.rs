//! Asks main's `compat certify` census and late-write pass whether they flag
//! the data-bearing keys a released binary wrote into a fenced drill bucket
//! (issue #4243, ADR-4233 decisions 7 and 9). Used by
//! `scripts/compat/released-binary-drill.sh` against its isolated RustFS
//! bucket; never point it at a shared or production bucket, since it writes
//! `sys/compat` there.
//!
//! Run:
//!
//! ```text
//! cargo run --manifest-path crates/ravel-compat-tests/Cargo.toml --example drill_census -- \
//!     --endpoint http://127.0.0.1:9000 --bucket drill --tenant-hash-key-file key.hex \
//!     --activated-unix-s N --fence-complete-unix-s N --keys FILE [--region us-east-1]
//! ```
//!
//! It writes a `sys/compat` record (create-if-absent) whose rseg, rlog and
//! rspan entries are active at the drill versions, one above what v0.33.0
//! writes (RSEG 8, RLOG 6, RSPAN 6), activated at `--activated-unix-s` (when
//! the drill's fence began) at epoch 1, the epoch the drill fenced with, and
//! with the fence walk complete at `--fence-complete-unix-s`. It then runs
//! `certify` at that target and `late_write_pass` per family at the real
//! current time. `--keys` lists the data-bearing keys (one per line) to
//! judge. Output, one line each:
//!
//! ```text
//! compat outcome=created|existing
//! certify family=<f> target=<v> report=<Debug of CertifyReport or error>
//! late-write family=<f> certified=<bool> examined=<n> late_writes=<n> report=<Debug or error>
//! key <key> flagged=contradicted|held|late-write|no
//! census keys=<n> flagged=<n> not_flagged=<n>
//! ```
//!
//! A key is flagged only when a report names it: `Contradicted` or
//! `HeldMember` with that `member_key`, or a late write with that
//! `record_key`. The census stops at its first finding and the late-write
//! pass reads only commit records once a certificate exists, so a key no
//! report names prints `flagged=no`.

use std::process::ExitCode;

use ravel_catalog::compat::{
    COMPAT_FORMAT_VERSION, CompatError, CompatKeys, FAMILY_RLOG, FAMILY_RSEG, FAMILY_RSPAN,
    create_compat,
};
use ravel_catalog::{DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_MAX_FLUSH_LIFETIME_NS};
use ravel_maintain::{CertifyReport, CertifyTimings, certify, late_write_pass};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::s3::{S3AuthMode, S3Config, S3Store};
use ravel_proto::sys::v1::{CompatFamily, CompatRecord};

/// One above what v0.33.0 writes: the drill versions `format-drill` admits.
const DRILL_TARGETS: [(&str, u32); 3] = [(FAMILY_RSEG, 8), (FAMILY_RLOG, 6), (FAMILY_RSPAN, 6)];
const SECOND_NS: i64 = 1_000_000_000;
/// v0.33.0's S3 request timeout is 20 s; the drill's margin is three of them.
const T_REQ_NS: i64 = 60 * SECOND_NS;

struct Args {
    endpoint: String,
    bucket: String,
    region: String,
    key_file: String,
    activated_unix_s: i64,
    fence_complete_unix_s: i64,
    keys: String,
}

const USAGE: &str = "usage: drill_census --endpoint URL --bucket NAME --tenant-hash-key-file PATH \
     --activated-unix-s N --fence-complete-unix-s N --keys FILE [--region R]";

fn parse_args(raw: &[String]) -> Result<Args, String> {
    let mut endpoint = None;
    let mut bucket = None;
    let mut region = "us-east-1".to_string();
    let mut key_file = None;
    let mut activated = None;
    let mut fence_complete = None;
    let mut keys = None;
    let mut it = raw.iter();
    while let Some(flag) = it.next() {
        let value = it
            .next()
            .ok_or_else(|| format!("{flag} needs a value"))?
            .clone();
        let seconds = |v: &str| v.parse::<i64>().map_err(|e| format!("{flag} {v}: {e}"));
        match flag.as_str() {
            "--endpoint" => endpoint = Some(value),
            "--bucket" => bucket = Some(value),
            "--region" => region = value,
            "--tenant-hash-key-file" => key_file = Some(value),
            "--activated-unix-s" => activated = Some(seconds(&value)?),
            "--fence-complete-unix-s" => fence_complete = Some(seconds(&value)?),
            "--keys" => keys = Some(value),
            other => return Err(format!("unknown flag {other}")),
        }
    }
    Ok(Args {
        endpoint: endpoint.ok_or("--endpoint is required")?,
        bucket: bucket.ok_or("--bucket is required")?,
        region,
        key_file: key_file.ok_or("--tenant-hash-key-file is required")?,
        activated_unix_s: activated.ok_or("--activated-unix-s is required")?,
        fence_complete_unix_s: fence_complete.ok_or("--fence-complete-unix-s is required")?,
        keys: keys.ok_or("--keys is required")?,
    })
}

/// The deployment key, read the way `ravel-server --tenant-hash-key-file`
/// reads it: 64 hex characters (surrounding whitespace ignored) or exactly 32
/// raw bytes.
fn read_deployment_key(path: &str) -> Result<[u8; 32], String> {
    let raw = std::fs::read(path).map_err(|e| format!("read {path}: {e}"))?;
    if let Ok(text) = std::str::from_utf8(&raw) {
        let trimmed = text.trim();
        if trimmed.len() == 64 && trimmed.bytes().all(|b| b.is_ascii_hexdigit()) {
            let bytes = hex::decode(trimmed).map_err(|e| format!("{path}: {e}"))?;
            return bytes
                .try_into()
                .map_err(|_| format!("{path}: hex key did not decode to 32 bytes"));
        }
    }
    raw.try_into().map_err(|raw: Vec<u8>| {
        format!(
            "{path}: expected 64 hex characters or 32 raw bytes, got {} bytes",
            raw.len()
        )
    })
}

fn env(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|_| format!("{name} must be set"))
}

fn now_ns() -> Result<i64, String> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| format!("clock: {e}"))
        .and_then(|d| i64::try_from(d.as_nanos()).map_err(|e| format!("clock: {e}")))
}

fn drill_record(activated_ns: i64, fence_complete_ns: i64) -> CompatRecord {
    CompatRecord {
        format_version: COMPAT_FORMAT_VERSION,
        baseline_release_major: 0,
        baseline_release_minor: 33,
        families: DRILL_TARGETS
            .iter()
            .map(|(family, version)| CompatFamily {
                family: (*family).to_string(),
                active_write_version: *version,
                activated_epoch: 1,
                activated_unix_ns: activated_ns,
                activated_by: "released-binary-drill".to_string(),
                certificate: None,
                fence_complete_unix_ns: fence_complete_ns,
                late_writes_verified_through_unix_ns: 0,
                contradicted: false,
            })
            .collect(),
        compat_epoch: 1,
        max_publish_lifetime_ns: DEFAULT_MAX_FLUSH_LIFETIME_NS,
        adopted: Vec::new(),
        mac: Vec::new(),
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Flag {
    Contradicted,
    Held,
    LateWrite,
}

impl Flag {
    fn name(self) -> &'static str {
        match self {
            Flag::Contradicted => "contradicted",
            Flag::Held => "held",
            Flag::LateWrite => "late-write",
        }
    }
}

async fn run(args: Args) -> Result<(), String> {
    let deployment_key = read_deployment_key(&args.key_file)?;
    let keys = CompatKeys::from_file_keys(&[deployment_key]).map_err(|e| format!("keys: {e}"))?;
    let judged: Vec<String> = std::fs::read_to_string(&args.keys)
        .map_err(|e| format!("read {}: {e}", args.keys))?
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .collect();
    let config = S3Config {
        bucket: args.bucket.clone(),
        region: args.region.clone(),
        endpoint: Some(args.endpoint.clone()),
        access_key_id: env("AWS_ACCESS_KEY_ID")?,
        secret_access_key: env("AWS_SECRET_ACCESS_KEY")?,
        allow_http: args.endpoint.starts_with("http://"),
        force_path_style: true,
        kms_key_id: None,
        session_token: None,
        credentials_file: None,
        auth: S3AuthMode::Static,
        instance_metadata_endpoint: None,
    };
    let s3 = S3Store::new(config).map_err(|e| format!("open bucket: {e}"))?;
    let store: &dyn ObjectStoreBackend = &s3;

    let record = drill_record(
        args.activated_unix_s.saturating_mul(SECOND_NS),
        args.fence_complete_unix_s.saturating_mul(SECOND_NS),
    );
    match create_compat(store, &keys, record).await {
        Ok(_) => println!("compat outcome=created"),
        Err(CompatError::AlreadyExists { .. }) => println!("compat outcome=existing"),
        Err(e) => return Err(format!("create sys/compat: {e}")),
    }

    let timings = CertifyTimings {
        t_req_ns: T_REQ_NS,
        max_flush_lifetime_ns: DEFAULT_MAX_FLUSH_LIFETIME_NS,
        clock_skew_allowance_ns: DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
    };
    let mut flagged: Vec<(String, Flag)> = Vec::new();
    for (family, target) in DRILL_TARGETS {
        match certify(store, &keys, now_ns()?, family, target, timings, None).await {
            Ok(report) => {
                match &report {
                    CertifyReport::Contradicted { member_key, .. } => {
                        flagged.push((member_key.clone(), Flag::Contradicted))
                    }
                    CertifyReport::HeldMember { member_key, .. } => {
                        flagged.push((member_key.clone(), Flag::Held))
                    }
                    _ => {}
                }
                println!("certify family={family} target={target} report={report:?}");
            }
            Err(e) => println!("certify family={family} target={target} report=error: {e}"),
        }
    }
    for (family, _) in DRILL_TARGETS {
        match late_write_pass(
            store,
            &keys,
            now_ns()?,
            family,
            DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
        )
        .await
        {
            Ok(report) => {
                for late in &report.late_writes {
                    flagged.push((late.record_key.clone(), Flag::LateWrite));
                }
                println!(
                    "late-write family={family} certified={} examined={} late_writes={} report={report:?}",
                    report.certified,
                    report.examined,
                    report.late_writes.len()
                );
            }
            Err(e) => println!("late-write family={family} report=error: {e}"),
        }
    }

    let mut hits = 0usize;
    for key in &judged {
        match flagged.iter().find(|(k, _)| k == key) {
            Some((_, flag)) => {
                hits += 1;
                println!("key {key} flagged={}", flag.name());
            }
            None => println!("key {key} flagged=no"),
        }
    }
    println!(
        "census keys={} flagged={hits} not_flagged={}",
        judged.len(),
        judged.len() - hits
    );
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    let raw: Vec<String> = std::env::args().skip(1).collect();
    let args = match parse_args(&raw) {
        Ok(args) => args,
        Err(e) => {
            eprintln!("{e}\n{USAGE}");
            return ExitCode::from(64);
        }
    };
    match run(args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("drill_census: {e}");
            ExitCode::FAILURE
        }
    }
}
