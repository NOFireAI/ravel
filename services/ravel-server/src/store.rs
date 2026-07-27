//! Builds the configured `ObjectStoreBackend` (memory or S3/MinIO).

use std::sync::Arc;

use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::s3::{S3Config, S3Store};

use crate::config::{Cli, StoreKind};

pub fn build_store(cli: &Cli) -> anyhow::Result<Arc<dyn ObjectStoreBackend>> {
    match cli.store {
        StoreKind::Memory => Ok(Arc::new(MemoryStore::new())),
        StoreKind::S3 => {
            let bucket = cli
                .s3_bucket
                .clone()
                .ok_or_else(|| anyhow::anyhow!("--store s3 requires RAVEL_S3_BUCKET"))?;
            let region = cli
                .s3_region
                .clone()
                .unwrap_or_else(|| "us-east-1".to_string());
            let access_key_id = cli
                .s3_access_key
                .clone()
                .ok_or_else(|| anyhow::anyhow!("--store s3 requires RAVEL_S3_ACCESS_KEY"))?;
            let secret_access_key = cli
                .s3_secret_key
                .clone()
                .ok_or_else(|| anyhow::anyhow!("--store s3 requires RAVEL_S3_SECRET_KEY"))?;
            let endpoint = cli.s3_endpoint.clone();
            let allow_http = endpoint.is_some();

            let config = S3Config {
                bucket,
                region,
                endpoint,
                access_key_id,
                secret_access_key,
                allow_http,
                force_path_style: true,
            };
            let store = S3Store::new(config)
                .map_err(|err| anyhow::anyhow!("failed to build S3 store: {err}"))?;
            Ok(Arc::new(store))
        }
    }
}
