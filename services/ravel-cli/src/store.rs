//! Object store construction, sharing `RAVEL_S3_*` env vars with `ravel-server`.

use std::path::Path;
use std::sync::Arc;

use clap::{Parser, ValueEnum};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::s3::{S3Config, S3Store};
use ravel_object_store::{GetRange, ObjectStoreBackend};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum StoreKind {
    Memory,
    #[value(name = "s3")]
    S3,
}

#[derive(Debug, Parser)]
pub struct StoreArgs {
    #[arg(long, value_enum, default_value = "memory")]
    pub store: StoreKind,

    #[arg(long, env = "RAVEL_S3_ENDPOINT")]
    pub s3_endpoint: Option<String>,

    #[arg(long, env = "RAVEL_S3_BUCKET")]
    pub s3_bucket: Option<String>,

    #[arg(long, env = "RAVEL_S3_REGION")]
    pub s3_region: Option<String>,

    #[arg(long, env = "RAVEL_S3_ACCESS_KEY")]
    pub s3_access_key: Option<String>,

    #[arg(long, env = "RAVEL_S3_SECRET_KEY")]
    pub s3_secret_key: Option<String>,
}

pub fn build_store(args: &StoreArgs) -> anyhow::Result<Arc<dyn ObjectStoreBackend>> {
    match args.store {
        StoreKind::Memory => Ok(Arc::new(MemoryStore::new())),
        StoreKind::S3 => {
            let bucket = args
                .s3_bucket
                .clone()
                .ok_or_else(|| anyhow::anyhow!("--store s3 requires RAVEL_S3_BUCKET"))?;
            let region = args
                .s3_region
                .clone()
                .unwrap_or_else(|| "us-east-1".to_string());
            let access_key_id = args
                .s3_access_key
                .clone()
                .ok_or_else(|| anyhow::anyhow!("--store s3 requires RAVEL_S3_ACCESS_KEY"))?;
            let secret_access_key = args
                .s3_secret_key
                .clone()
                .ok_or_else(|| anyhow::anyhow!("--store s3 requires RAVEL_S3_SECRET_KEY"))?;
            let endpoint = args.s3_endpoint.clone();
            let allow_http = endpoint.is_some();
            let config = S3Config {
                bucket,
                region,
                endpoint,
                access_key_id,
                secret_access_key,
                allow_http,
                force_path_style: true,
                kms_key_id: None,
            };
            let store = S3Store::new(config)
                .map_err(|err| anyhow::anyhow!("failed to build S3 store: {err}"))?;
            Ok(Arc::new(store))
        }
    }
}

/// Reads `key_or_path` from the local filesystem if it names an existing
/// file, otherwise fetches it as a key from the configured object store.
pub async fn read_bytes(args: &StoreArgs, key_or_path: &str) -> anyhow::Result<Vec<u8>> {
    if Path::new(key_or_path).is_file() {
        return tokio::fs::read(key_or_path)
            .await
            .map_err(|err| anyhow::anyhow!("failed to read {key_or_path}: {err}"));
    }
    let store = build_store(args)?;
    let outcome = store
        .get(key_or_path, GetRange::Full)
        .await
        .map_err(|err| anyhow::anyhow!("failed to fetch {key_or_path}: {err}"))?;
    Ok(outcome.data.to_vec())
}
