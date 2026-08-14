//! Shared object-store construction for end-to-end bench bins. Extracted from
//! `ingest_bench`'s `s3_config_from_env`/`StoreKind` so every bin that needs a
//! `--store memory|s3` flag shares one copy of the `RAVEL_S3_*` env-var
//! convention instead of re-deriving it (`ingest_bench.rs` and
//! `catalog_resolve_bench.rs` predate this module and keep their own inline
//! copies; migrating them is unrelated to the bin this module was extracted
//! for).
#![allow(clippy::expect_used)]

use std::sync::Arc;

use clap::ValueEnum;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::s3::{S3Config, S3Store};

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum StoreKind {
    Memory,
    S3,
}

impl std::fmt::Display for StoreKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreKind::Memory => write!(f, "memory"),
            StoreKind::S3 => write!(f, "s3"),
        }
    }
}

/// Builds an `S3Config` from the `RAVEL_S3_*` env vars: `RAVEL_S3_BUCKET`,
/// `RAVEL_S3_REGION`, `RAVEL_S3_ENDPOINT` (optional), `RAVEL_S3_ACCESS_KEY_ID`,
/// `RAVEL_S3_SECRET_ACCESS_KEY`, `RAVEL_S3_ALLOW_HTTP` (default false),
/// `RAVEL_S3_FORCE_PATH_STYLE` (default true). Same convention as
/// `ingest_bench`'s `--store s3`; not the `RAVEL_MINIO_*` convention used by
/// `ravel-object-store`'s contract suite, which gates a fixed local MinIO
/// rather than configuring an arbitrary S3-compatible endpoint.
pub fn s3_config_from_env() -> S3Config {
    let get = |key: &str| std::env::var(key).unwrap_or_default();
    S3Config {
        bucket: get("RAVEL_S3_BUCKET"),
        region: get("RAVEL_S3_REGION"),
        endpoint: std::env::var("RAVEL_S3_ENDPOINT").ok(),
        access_key_id: get("RAVEL_S3_ACCESS_KEY_ID"),
        secret_access_key: get("RAVEL_S3_SECRET_ACCESS_KEY"),
        allow_http: std::env::var("RAVEL_S3_ALLOW_HTTP").as_deref() == Ok("true"),
        force_path_style: std::env::var("RAVEL_S3_FORCE_PATH_STYLE").as_deref() != Ok("false"),
        kms_key_id: None,
        session_token: None,
        credentials_file: None,
    }
}

/// Builds the store backing a bench bin's `--store` flag: an in-process
/// `MemoryStore`, or a real `S3Store` configured from `RAVEL_S3_*`.
pub fn store_from_env(kind: StoreKind) -> Arc<dyn ObjectStoreBackend> {
    match kind {
        StoreKind::Memory => Arc::new(MemoryStore::new()),
        StoreKind::S3 => {
            Arc::new(S3Store::new(s3_config_from_env()).expect("build S3Store from RAVEL_S3_* env"))
        }
    }
}
