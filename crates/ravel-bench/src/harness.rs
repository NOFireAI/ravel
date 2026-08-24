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
use ravel_object_store::s3::{S3AuthMode, S3Config, S3Store};

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
///
/// `RAVEL_S3_AUTH=instance-role` (ADR-0106) selects
/// [`ravel_object_store::s3::S3AuthMode::InstanceRole`]: any value other than
/// exactly `instance-role` (including unset) keeps the default `Static`
/// mode, so the access/secret key env vars stay required exactly as before
/// for every caller that does not opt in. `RAVEL_S3_INSTANCE_METADATA_ENDPOINT`
/// (optional) points instance-role mode at a mock IMDS in tests, or an
/// unusual deployment; ignored under `Static`, matching
/// [`S3Config::instance_metadata_endpoint`]'s own contract.
pub fn s3_config_from_env() -> S3Config {
    s3_config_from_lookup(|key| std::env::var(key).ok())
}

/// [`s3_config_from_env`]'s logic over an injected lookup instead of the real
/// process environment, so `RAVEL_S3_AUTH=instance-role` selection is testable
/// without `std::env::set_var` (`unsafe` under the 2024 edition, and process
/// env vars are global state that races across parallel tests regardless).
fn s3_config_from_lookup(lookup: impl Fn(&str) -> Option<String>) -> S3Config {
    let get = |key: &str| lookup(key).unwrap_or_default();
    let auth = if lookup("RAVEL_S3_AUTH").as_deref() == Some("instance-role") {
        S3AuthMode::InstanceRole
    } else {
        S3AuthMode::default()
    };
    S3Config {
        bucket: get("RAVEL_S3_BUCKET"),
        region: get("RAVEL_S3_REGION"),
        endpoint: lookup("RAVEL_S3_ENDPOINT"),
        access_key_id: get("RAVEL_S3_ACCESS_KEY_ID"),
        secret_access_key: get("RAVEL_S3_SECRET_ACCESS_KEY"),
        allow_http: lookup("RAVEL_S3_ALLOW_HTTP").as_deref() == Some("true"),
        force_path_style: lookup("RAVEL_S3_FORCE_PATH_STYLE").as_deref() != Some("false"),
        kms_key_id: None,
        session_token: None,
        credentials_file: None,
        auth,
        instance_metadata_endpoint: lookup("RAVEL_S3_INSTANCE_METADATA_ENDPOINT"),
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A fixed map, not the real process environment: `RAVEL_S3_AUTH`
    /// selection must be provable without `std::env::set_var` (`unsafe`
    /// under the 2024 edition) and without racing other tests over global
    /// process env state.
    fn lookup(pairs: &'static [(&'static str, &'static str)]) -> impl Fn(&str) -> Option<String> {
        move |key| {
            pairs
                .iter()
                .find(|(k, _)| *k == key)
                .map(|(_, v)| v.to_string())
        }
    }

    /// Issue #546: `RAVEL_S3_AUTH=instance-role` selects
    /// `S3AuthMode::InstanceRole` and carries
    /// `RAVEL_S3_INSTANCE_METADATA_ENDPOINT` through unchanged; the access
    /// and secret key vars stay absent (not required under instance-role).
    #[test]
    fn instance_role_env_selects_instance_role_config() {
        let cfg = s3_config_from_lookup(lookup(&[
            ("RAVEL_S3_BUCKET", "ravel-bench"),
            ("RAVEL_S3_REGION", "us-east-1"),
            ("RAVEL_S3_AUTH", "instance-role"),
            (
                "RAVEL_S3_INSTANCE_METADATA_ENDPOINT",
                "http://127.0.0.1:9999",
            ),
        ]));
        assert_eq!(cfg.auth, S3AuthMode::InstanceRole);
        assert_eq!(
            cfg.instance_metadata_endpoint.as_deref(),
            Some("http://127.0.0.1:9999")
        );
        assert_eq!(
            cfg.access_key_id, "",
            "instance-role mode must not require an inline access key"
        );
        assert_eq!(
            cfg.secret_access_key, "",
            "instance-role mode must not require an inline secret key"
        );
    }

    /// The default (unset `RAVEL_S3_AUTH`) must keep selecting `Static`, byte-
    /// identical to the pre-#546 behavior, so every existing bench bin that
    /// never sets this var is unaffected.
    #[test]
    fn missing_auth_env_defaults_to_static() {
        let cfg = s3_config_from_lookup(lookup(&[
            ("RAVEL_S3_BUCKET", "ravel-bench"),
            ("RAVEL_S3_REGION", "us-east-1"),
        ]));
        assert_eq!(cfg.auth, S3AuthMode::Static);
        assert_eq!(cfg.instance_metadata_endpoint, None);
    }

    /// A value other than exactly `instance-role` -- a typo, a different
    /// case -- must not silently select instance-role mode. Fail-closed to
    /// the default rather than guessing at what the operator meant.
    #[test]
    fn unrecognized_auth_value_defaults_to_static() {
        let cfg = s3_config_from_lookup(lookup(&[("RAVEL_S3_AUTH", "Instance-Role")]));
        assert_eq!(cfg.auth, S3AuthMode::Static);
    }
}
