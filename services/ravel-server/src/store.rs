//! Builds the configured `ObjectStoreBackend` (memory or S3/MinIO) and
//! enforces the mandatory-capability contract before the backend is used.

use std::sync::Arc;

use ravel_object_store::memory::MemoryStore;
use ravel_object_store::s3::{S3Config, S3Store};
use ravel_object_store::{Capabilities, ObjectStoreBackend};

use crate::config::{Cli, StoreKind};

/// A constructed backend under-reports a capability Ravel's commit protocol
/// and catalog require in production. Startup aborts here rather than trusting
/// a durability guarantee the backend does not provide: the check is not
/// decorative. See docs/object-store-contract.md "Mandatory capabilities".
#[derive(Debug)]
pub struct UnsatisfiedCapabilities {
    /// Comma-separated mandatory flags the backend reports as unsupported.
    pub missing: String,
}

impl std::fmt::Display for UnsatisfiedCapabilities {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "object store backend does not satisfy mandatory capabilities \
             (unsupported: {}); refusing to start. Every flag in the \
             mandatory-capabilities table must be supported by the durable \
             backend (docs/object-store-contract.md \"Mandatory capabilities\").",
            self.missing
        )
    }
}

impl std::error::Error for UnsatisfiedCapabilities {}

/// The mandatory flags a backend reports as unsupported, for the error text.
/// Mirrors [`Capabilities::satisfies`]; kept in sync field-for-field so the
/// diagnostic never disagrees with the gate that produced it.
fn missing_mandatory(caps: &Capabilities) -> Vec<&'static str> {
    let required = Capabilities::mandatory();
    let mut missing = Vec::new();
    if required.consistent_read && !caps.consistent_read {
        missing.push("consistent_read");
    }
    if required.consistent_list && !caps.consistent_list {
        missing.push("consistent_list");
    }
    if required.create_if_absent && !caps.create_if_absent {
        missing.push("create_if_absent");
    }
    if required.cas_version && !caps.cas_version {
        missing.push("cas_version");
    }
    if required.suffix_range && !caps.suffix_range {
        missing.push("suffix_range");
    }
    if required.upload_checksum && !caps.upload_checksum {
        missing.push("upload_checksum");
    }
    if required.prefix_list && !caps.prefix_list {
        missing.push("prefix_list");
    }
    if required.multipart && !caps.multipart {
        missing.push("multipart");
    }
    missing
}

/// Enforce the mandatory-capability contract against a constructed backend.
///
/// A backend that under-reports any mandatory capability (see
/// [`Capabilities::mandatory`]) must not carry production durability, so this
/// returns [`UnsatisfiedCapabilities`] listing the offending flags instead of
/// letting the server start. This is the enforcement the struct doc on
/// [`Capabilities`] refers to.
pub fn check_mandatory_capabilities(
    backend: &dyn ObjectStoreBackend,
) -> Result<(), UnsatisfiedCapabilities> {
    let caps = backend.capabilities();
    if caps.satisfies(&Capabilities::mandatory()) {
        Ok(())
    } else {
        Err(UnsatisfiedCapabilities {
            missing: missing_mandatory(&caps).join(", "),
        })
    }
}

pub fn build_store(cli: &Cli) -> anyhow::Result<Arc<dyn ObjectStoreBackend>> {
    let store: Arc<dyn ObjectStoreBackend> = match cli.store {
        StoreKind::Memory => Arc::new(MemoryStore::new()),
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
            Arc::new(store)
        }
    };

    check_mandatory_capabilities(store.as_ref())?;
    Ok(store)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    use async_trait::async_trait;
    use bytes::Bytes;
    use ravel_object_store::{
        DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, PageToken, PutOptions,
        PutOutcome, StoreError,
    };

    /// A backend that returns whatever capabilities the test asks for. Only
    /// `capabilities()` is exercised by these tests; the data-plane methods
    /// exist to satisfy the trait and must never be reached.
    struct StubBackend {
        caps: Capabilities,
    }

    #[async_trait]
    impl ObjectStoreBackend for StubBackend {
        async fn put(
            &self,
            _key: &str,
            _data: Bytes,
            _opts: PutOptions,
        ) -> Result<PutOutcome, StoreError> {
            Err(StoreError::Permanent("stub".to_string()))
        }

        async fn get(&self, _key: &str, _range: GetRange) -> Result<GetOutcome, StoreError> {
            Err(StoreError::Permanent("stub".to_string()))
        }

        async fn head(&self, _key: &str) -> Result<ObjectMeta, StoreError> {
            Err(StoreError::Permanent("stub".to_string()))
        }

        async fn list(
            &self,
            _prefix: &str,
            _page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            Err(StoreError::Permanent("stub".to_string()))
        }

        async fn list_delimited(&self, _prefix: &str) -> Result<DelimitedList, StoreError> {
            Err(StoreError::Permanent("stub".to_string()))
        }

        async fn delete(&self, _key: &str) -> Result<(), StoreError> {
            Err(StoreError::Permanent("stub".to_string()))
        }

        fn capabilities(&self) -> Capabilities {
            self.caps
        }
    }

    #[test]
    fn backend_meeting_all_mandatory_flags_passes() {
        let backend = StubBackend {
            caps: Capabilities::mandatory(),
        };
        check_mandatory_capabilities(&backend).expect("mandatory-satisfying backend must start");
    }

    #[test]
    fn memory_store_satisfies_mandatory() {
        // The reference backend the default `--store memory` path builds must
        // pass the gate build_store applies.
        check_mandatory_capabilities(&MemoryStore::new())
            .expect("MemoryStore must satisfy mandatory capabilities");
    }

    #[test]
    fn backend_lying_about_mandatory_flag_fails_startup() {
        // A backend that reports a mandatory flag as unsupported (here
        // upload_checksum, exactly as S3Store does today) must abort startup
        // with a clear, actionable error rather than starting silently.
        let mut caps = Capabilities::mandatory();
        caps.upload_checksum = false;
        let backend = StubBackend { caps };

        let err = check_mandatory_capabilities(&backend)
            .expect_err("backend missing a mandatory flag must fail the check");
        assert!(
            err.missing.contains("upload_checksum"),
            "error must name the offending flag, got: {}",
            err.missing
        );
        let rendered = err.to_string();
        assert!(
            rendered.contains("mandatory capabilities") && rendered.contains("upload_checksum"),
            "error message must be actionable, got: {rendered}"
        );
    }

    #[test]
    fn multiple_missing_flags_are_all_reported() {
        let mut caps = Capabilities::mandatory();
        caps.cas_version = false;
        caps.prefix_list = false;
        let backend = StubBackend { caps };

        let err = check_mandatory_capabilities(&backend)
            .expect_err("backend missing mandatory flags must fail the check");
        assert!(err.missing.contains("cas_version"), "got: {}", err.missing);
        assert!(err.missing.contains("prefix_list"), "got: {}", err.missing);
    }

    #[test]
    fn build_store_memory_starts() {
        // End-to-end through the production construction path: the default
        // memory backend is built and passes the capability gate.
        use clap::Parser;
        let cli = Cli::try_parse_from(["ravel-server"]).expect("defaults parse");
        assert!(matches!(cli.store, StoreKind::Memory));
        build_store(&cli).expect("memory backend must build and satisfy the capability gate");
    }
}
