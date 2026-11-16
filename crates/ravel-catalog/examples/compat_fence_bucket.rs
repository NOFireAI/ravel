//! Fences one (tenant, signal)'s provisioning record exactly as
//! `compat activate` does (ADR-4233 decision 4), and optionally parks the
//! tenant's alert lease the same way. Used by the released-binary fence drill
//! (`scripts/compat/released-binary-drill.sh`) against an isolated local
//! bucket; never point it at a shared or production bucket.
//!
//! Run:
//!
//! ```text
//! cargo run -p ravel-catalog --example compat_fence_bucket -- \
//!     --endpoint http://127.0.0.1:9000 --bucket drill --tenant acme \
//!     --tenant-hash-key-file key.hex --epoch 1 \
//!     [--signal metrics|logs|spans] [--park-alert-lease] \
//!     [--region us-east-1] [--now-ns N]
//! ```
//!
//! Credentials come from `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.
//! `--signal` fences that signal's record through
//! [`ravel_catalog::raise_compat_fence`]; `--park-alert-lease`
//! parks `t/<h>/a/alert-lease` through [`ravel_alerting::park_lease`]. At least
//! one of the two is required. Every printed line is `key=value` pairs, one
//! line per state, so the drill can assert each figure:
//!
//! ```text
//! prov-before key=t/<h>/m/prov format_version=3 compat_epoch=0 compat_fenced_unix_ns=0
//! prov-outcome outcome=raised previous_epoch=0 compat_epoch=1 compat_fenced_unix_ns=<ns>
//! prov-after key=t/<h>/m/prov format_version=4 compat_epoch=1 compat_fenced_unix_ns=<ns>
//! lease-before key=t/<h>/a/alert-lease body=<json or absent>
//! lease-outcome outcome=replaced
//! lease-after key=t/<h>/a/alert-lease body=<json>
//! ```

use std::process::ExitCode;
use std::sync::Arc;

use prost::Message;
use ravel_catalog::{CompatFenceOutcome, provisioning_key, raise_compat_fence};
use ravel_object_store::s3::{S3AuthMode, S3Config, S3Store};
use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError};
use ravel_proto::sys::v1::ProvisioningRecord;
use ravel_types::{Signal, TenantHash, TenantHashScheme, TenantId};

struct Args {
    endpoint: String,
    bucket: String,
    region: String,
    tenant: String,
    key_file: String,
    epoch: u64,
    signal: Option<Signal>,
    park_alert_lease: bool,
    now_ns: Option<i64>,
}

const USAGE: &str = "usage: compat_fence_bucket --endpoint URL --bucket NAME --tenant ID \
     --tenant-hash-key-file PATH --epoch N [--signal metrics|logs|spans] \
     [--park-alert-lease] [--region R] [--now-ns N]";

fn parse_signal(s: &str) -> Result<Signal, String> {
    match s {
        "metrics" => Ok(Signal::Metrics),
        "logs" => Ok(Signal::Logs),
        "spans" => Ok(Signal::Spans),
        other => Err(format!("--signal {other}: expected metrics, logs or spans")),
    }
}

fn parse_args(raw: &[String]) -> Result<Args, String> {
    let mut endpoint = None;
    let mut bucket = None;
    let mut region = "us-east-1".to_string();
    let mut tenant = None;
    let mut key_file = None;
    let mut epoch = None;
    let mut signal = None;
    let mut park_alert_lease = false;
    let mut now_ns = None;
    let mut it = raw.iter();
    while let Some(flag) = it.next() {
        if flag == "--park-alert-lease" {
            park_alert_lease = true;
            continue;
        }
        let value = it
            .next()
            .ok_or_else(|| format!("{flag} needs a value"))?
            .clone();
        match flag.as_str() {
            "--endpoint" => endpoint = Some(value),
            "--bucket" => bucket = Some(value),
            "--region" => region = value,
            "--tenant" => tenant = Some(value),
            "--tenant-hash-key-file" => key_file = Some(value),
            "--epoch" => {
                epoch = Some(
                    value
                        .parse::<u64>()
                        .map_err(|e| format!("--epoch {value}: {e}"))?,
                )
            }
            "--signal" => signal = Some(parse_signal(&value)?),
            "--now-ns" => {
                now_ns = Some(
                    value
                        .parse::<i64>()
                        .map_err(|e| format!("--now-ns {value}: {e}"))?,
                )
            }
            other => return Err(format!("unknown flag {other}")),
        }
    }
    let args = Args {
        endpoint: endpoint.ok_or("--endpoint is required")?,
        bucket: bucket.ok_or("--bucket is required")?,
        region,
        tenant: tenant.ok_or("--tenant is required")?,
        key_file: key_file.ok_or("--tenant-hash-key-file is required")?,
        epoch: epoch.ok_or("--epoch is required")?,
        signal,
        park_alert_lease,
        now_ns,
    };
    if args.epoch == 0 {
        return Err("--epoch must be at least 1: epoch 0 is the unfenced state".to_string());
    }
    if args.signal.is_none() && !args.park_alert_lease {
        return Err("nothing to do: pass --signal, --park-alert-lease, or both".to_string());
    }
    Ok(args)
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

async fn print_record(
    store: &dyn ObjectStoreBackend,
    label: &str,
    key: &str,
) -> Result<(), String> {
    match store.get(key, GetRange::Full).await {
        Ok(got) => {
            let record = ProvisioningRecord::decode(got.data.as_ref())
                .map_err(|e| format!("{key}: decode: {e}"))?;
            println!(
                "{label} key={key} format_version={} compat_epoch={} compat_fenced_unix_ns={}",
                record.format_version, record.compat_epoch, record.compat_fenced_unix_ns
            );
            Ok(())
        }
        Err(StoreError::NotFound) => {
            println!("{label} key={key} absent");
            Ok(())
        }
        Err(e) => Err(format!("{key}: {e}")),
    }
}

async fn print_lease(store: &dyn ObjectStoreBackend, label: &str, key: &str) -> Result<(), String> {
    match store.get(key, GetRange::Full).await {
        Ok(got) => {
            println!(
                "{label} key={key} body={}",
                String::from_utf8_lossy(got.data.as_ref())
            );
            Ok(())
        }
        Err(StoreError::NotFound) => {
            println!("{label} key={key} body=absent");
            Ok(())
        }
        Err(e) => Err(format!("{key}: {e}")),
    }
}

async fn run(args: Args) -> Result<(), String> {
    let key = read_deployment_key(&args.key_file)?;
    let tenant_hash: TenantHash =
        TenantHashScheme::v2_from_deployment_key(&key).hash(&TenantId::new(args.tenant.clone()));
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
    let store: Arc<dyn ObjectStoreBackend> =
        Arc::new(S3Store::new(config).map_err(|e| format!("open bucket: {e}"))?);
    println!(
        "tenant={} tenant_hash={}",
        args.tenant,
        tenant_hash.to_hex()
    );

    if let Some(signal) = args.signal {
        let prov_key = provisioning_key(&tenant_hash, signal);
        print_record(store.as_ref(), "prov-before", &prov_key).await?;
        let now_ns = match args.now_ns {
            Some(ns) => ns,
            None => std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|e| format!("clock: {e}"))
                .and_then(|d| i64::try_from(d.as_nanos()).map_err(|e| format!("clock: {e}")))?,
        };
        let outcome = raise_compat_fence(store.as_ref(), &tenant_hash, signal, args.epoch, now_ns)
            .await
            .map_err(|e| format!("raise_compat_fence: {e}"))?;
        match outcome {
            CompatFenceOutcome::AlreadyFenced { compat_epoch } => {
                println!("prov-outcome outcome=already-fenced compat_epoch={compat_epoch}")
            }
            CompatFenceOutcome::Raised {
                previous_epoch,
                compat_epoch,
                compat_fenced_unix_ns,
            } => println!(
                "prov-outcome outcome=raised previous_epoch={previous_epoch} \
                 compat_epoch={compat_epoch} compat_fenced_unix_ns={compat_fenced_unix_ns}"
            ),
        }
        print_record(store.as_ref(), "prov-after", &prov_key).await?;
    }

    if args.park_alert_lease {
        let lease_key = ravel_alerting::alert_lease_key(&tenant_hash);
        print_lease(store.as_ref(), "lease-before", &lease_key).await?;
        let outcome = ravel_alerting::park_lease(store.as_ref(), &tenant_hash, args.epoch)
            .await
            .map_err(|e| format!("park_lease: {e}"))?;
        let outcome = match outcome {
            ravel_alerting::ParkOutcome::Created => "created".to_string(),
            ravel_alerting::ParkOutcome::Replaced => "replaced".to_string(),
            ravel_alerting::ParkOutcome::AlreadyParked { compat_epoch } => {
                format!("already-parked compat_epoch={compat_epoch}")
            }
        };
        println!("lease-outcome outcome={outcome}");
        print_lease(store.as_ref(), "lease-after", &lease_key).await?;
    }
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
            eprintln!("compat_fence_bucket: {e}");
            ExitCode::FAILURE
        }
    }
}
