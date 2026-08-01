//! Builds the configured `ObjectStoreBackend` (memory or S3/MinIO) and
//! enforces the mandatory-capability contract before the backend is used.

use std::sync::Arc;

use ravel_object_store::memory::MemoryStore;
use ravel_object_store::s3::{S3Config, S3Store};
use ravel_object_store::{Capabilities, InstrumentedStore, ObjectStoreBackend, StoreMetrics};

use crate::config::{Cli, Mode, StoreKind};

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

/// The capability set required for `mode`. Every mode requires
/// [`Capabilities::mandatory`]; [`Mode::Maintain`] additionally requires
/// `multipart`, because compaction is the only path that writes multipart
/// objects (large L1/L2 parts) and ingest/query never need it. Multipart is
/// NOT made globally mandatory: a gateway/query/all deployment keeps today's
/// `mandatory()` set exactly (docs/compaction-retention-plan.md P8; the
/// `Capabilities::mandatory` "mandatory from Phase 2" note).
///
/// `upload_checksum` is not required by any mode, including maintain, and no
/// mode may add it. Unlike `multipart`, which some backend could supply and
/// which one mode genuinely needs, `upload_checksum` is permanently
/// unsatisfiable by S3, the only durable backend: the `object_store` client
/// cannot put a caller-supplied CRC32C on the wire at all (issue #251, and
/// the `Capabilities::mandatory` doc). Gating startup on it rejected every
/// S3-compatible endpoint in every mode. Read-time integrity still comes
/// from the segment crc32c hierarchy and `put()`'s local pre-flight check.
pub fn required_capabilities(mode: Mode) -> Capabilities {
    let mut required = Capabilities::mandatory();
    if matches!(mode, Mode::Maintain) {
        required.multipart = true;
    }
    required
}

/// The required flags a backend reports as unsupported, for the error text.
/// Mirrors [`Capabilities::satisfies`]; kept in sync field-for-field so the
/// diagnostic never disagrees with the gate that produced it.
fn missing_capabilities(caps: &Capabilities, required: &Capabilities) -> Vec<&'static str> {
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

/// Enforce the capability contract for `mode` against a constructed backend.
///
/// A backend that under-reports any required capability (see
/// [`required_capabilities`]) must not carry production durability, so this
/// returns [`UnsatisfiedCapabilities`] listing the offending flags instead of
/// letting the server start. Every mode enforces [`Capabilities::mandatory`];
/// [`Mode::Maintain`] additionally enforces `multipart`. This is the
/// enforcement the struct doc on [`Capabilities`] refers to.
pub fn check_capabilities(
    backend: &dyn ObjectStoreBackend,
    mode: Mode,
) -> Result<(), UnsatisfiedCapabilities> {
    let required = required_capabilities(mode);
    let caps = backend.capabilities();
    if caps.satisfies(&required) {
        Ok(())
    } else {
        Err(UnsatisfiedCapabilities {
            missing: missing_capabilities(&caps, &required).join(", "),
        })
    }
}

/// Backward-compatible mandatory-only check (every non-maintain mode). Kept as
/// a thin wrapper over [`check_capabilities`] for callers and tests that only
/// care about the always-mandatory set.
pub fn check_mandatory_capabilities(
    backend: &dyn ObjectStoreBackend,
) -> Result<(), UnsatisfiedCapabilities> {
    check_capabilities(backend, Mode::All)
}

/// Build the configured backend, wrap it in the instrumentation decorator, and
/// enforce the capability contract for `cli.mode`.
///
/// Returns the store the whole process shares plus the metrics handle the
/// decorator counts into. Every backend is wrapped, unconditionally and in
/// every mode: the decorator is observability only, never
/// correctness-bearing, and wrapping is a zero behavior change (results
/// forward verbatim and `capabilities()` passes through), so there is no
/// configuration to get wrong and no "instrumented vs not" pair of behaviors
/// to reason about. Because capabilities pass through, the gate below still
/// checks the real backend's declaration.
///
/// Nothing surfaces the handle yet (no scrape endpoint, no exporter); it is
/// returned so the caller can hold it for that later work.
pub fn build_store(cli: &Cli) -> anyhow::Result<(Arc<dyn ObjectStoreBackend>, Arc<StoreMetrics>)> {
    let (store, metrics): (Arc<dyn ObjectStoreBackend>, Arc<StoreMetrics>) = match cli.store {
        StoreKind::Memory => {
            let instrumented = InstrumentedStore::new(MemoryStore::new());
            let metrics = instrumented.metrics();
            (Arc::new(instrumented), metrics)
        }
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
                kms_key_id: None,
            };
            let store = S3Store::new(config)
                .map_err(|err| anyhow::anyhow!("failed to build S3 store: {err}"))?;
            let instrumented = InstrumentedStore::new(store);
            let metrics = instrumented.metrics();
            (Arc::new(instrumented), metrics)
        }
    };

    // Runs against the decorator, which passes `capabilities()` straight
    // through, so this still gates on the wrapped backend's own declaration.
    check_capabilities(store.as_ref(), cli.mode)?;
    Ok((store, metrics))
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
        // suffix_range, which footer-first segment reads cannot work without)
        // must abort startup with a clear, actionable error rather than
        // starting silently.
        let mut caps = Capabilities::mandatory();
        caps.suffix_range = false;
        let backend = StubBackend { caps };

        let err = check_mandatory_capabilities(&backend)
            .expect_err("backend missing a mandatory flag must fail the check");
        assert!(
            err.missing.contains("suffix_range"),
            "error must name the offending flag, got: {}",
            err.missing
        );
        let rendered = err.to_string();
        assert!(
            rendered.contains("mandatory capabilities") && rendered.contains("suffix_range"),
            "error message must be actionable, got: {rendered}"
        );
    }

    /// Regression test for issue #251: `ravel-server --store s3` could not
    /// start in any mode against any S3-compatible endpoint, because
    /// `Capabilities::mandatory()` required `upload_checksum`, which
    /// `S3Store` permanently reports as unsupported (`object_store` 0.14 has
    /// no way to put a caller-supplied CRC32C on the wire). This stub
    /// reports exactly the S3Store/MemoryStore-shaped set: every mandatory
    /// flag, no `upload_checksum`, no `multipart`. It must pass the
    /// non-maintain gate, and `upload_checksum` must not be requestable by
    /// any mode.
    #[test]
    fn s3_shaped_backend_missing_only_upload_checksum_and_multipart_now_starts() {
        let caps = Capabilities {
            consistent_read: true,
            consistent_list: true,
            create_if_absent: true,
            cas_version: true,
            suffix_range: true,
            upload_checksum: false,
            prefix_list: true,
            multipart: false,
        };
        let backend = StubBackend { caps };

        check_mandatory_capabilities(&backend)
            .expect("an S3-shaped backend without upload_checksum must start (issue #251)");
        for mode in [Mode::All, Mode::Gateway, Mode::Query] {
            check_capabilities(&backend, mode).unwrap_or_else(|e| {
                panic!("{mode:?} must not require upload_checksum (issue #251), got: {e}")
            });
        }
        for mode in [Mode::All, Mode::Gateway, Mode::Query, Mode::Maintain] {
            assert!(
                !required_capabilities(mode).upload_checksum,
                "no mode may require upload_checksum, {mode:?} does"
            );
        }
    }

    /// The real `S3Store`, not a stub: the backend `--store s3` builds must
    /// satisfy the startup gate for every non-maintain mode. `S3Store::new`
    /// only validates configuration, so this needs no endpoint.
    #[test]
    fn s3_store_satisfies_mandatory_capabilities() {
        let store = S3Store::new(S3Config {
            bucket: "ravel-test".to_string(),
            region: "us-east-1".to_string(),
            endpoint: Some("http://127.0.0.1:9000".to_string()),
            access_key_id: "test".to_string(),
            secret_access_key: "test".to_string(),
            allow_http: true,
            force_path_style: true,
            kms_key_id: None,
        })
        .expect("dummy S3 config must build without network access");
        assert!(
            !store.capabilities().upload_checksum,
            "precondition: S3Store reports upload_checksum unsupported"
        );
        check_mandatory_capabilities(&store)
            .expect("S3Store must satisfy mandatory capabilities (issue #251)");
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
    fn maintain_mode_requires_multipart_and_fails_without_it() {
        // A backend meeting every always-mandatory flag but not multipart
        // (exactly what MemoryStore and S3Store report today) must fail startup
        // in maintain mode, naming multipart as the offending flag.
        let backend = StubBackend {
            caps: Capabilities::mandatory(),
        };
        assert!(
            !backend.caps.multipart,
            "precondition: multipart unsupported"
        );
        let err = check_capabilities(&backend, Mode::Maintain)
            .expect_err("maintain mode must reject a non-multipart backend");
        assert!(
            err.missing.contains("multipart"),
            "error must name multipart, got: {}",
            err.missing
        );
    }

    #[test]
    fn maintain_mode_passes_with_multipart() {
        let mut caps = Capabilities::mandatory();
        caps.multipart = true;
        let backend = StubBackend { caps };
        check_capabilities(&backend, Mode::Maintain)
            .expect("maintain mode must start on a multipart-capable backend");
    }

    #[test]
    fn non_maintain_modes_do_not_require_multipart() {
        // multipart is NOT globally mandatory: the always-mandatory set (no
        // multipart) must satisfy every non-maintain mode.
        let backend = StubBackend {
            caps: Capabilities::mandatory(),
        };
        for mode in [Mode::All, Mode::Gateway, Mode::Query] {
            check_capabilities(&backend, mode)
                .unwrap_or_else(|e| panic!("{mode:?} must not require multipart, got: {e}"));
        }
        assert!(
            !required_capabilities(Mode::All).multipart,
            "multipart must not be globally required"
        );
        assert!(
            required_capabilities(Mode::Maintain).multipart,
            "maintain mode must require multipart"
        );
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

    /// The built store is instrumented, and the instrumentation is invisible
    /// to the capability gate: `MemoryStore` supports `upload_checksum` on the
    /// wire, so the decorator must report that flag too rather than flattening
    /// the set to `mandatory()` (issue #272). The counters are proven to be
    /// the returned handle's by driving one operation through the store.
    #[tokio::test]
    async fn build_store_wraps_the_backend_and_passes_capabilities_through() {
        use clap::Parser;
        use ravel_object_store::PutOptions;

        let cli = Cli::try_parse_from(["ravel-server"]).expect("defaults parse");
        let (store, metrics) = build_store(&cli).expect("memory backend must build");
        assert_eq!(
            store.capabilities(),
            MemoryStore::new().capabilities(),
            "the decorator must report the wrapped backend's capabilities verbatim"
        );
        assert_eq!(
            metrics.snapshot(),
            ravel_object_store::instrument::StoreMetricsSnapshot::default(),
            "a freshly built store has recorded nothing"
        );

        store
            .put("t/k", Bytes::from_static(b"abc"), PutOptions::default())
            .await
            .expect("put through the instrumented store");
        let snap = metrics.snapshot();
        assert_eq!(
            snap.put.calls, 1,
            "the returned handle must be the live one"
        );
        assert_eq!(snap.put.ok, 1);
        assert_eq!(snap.put.bytes, 3);
    }
}
