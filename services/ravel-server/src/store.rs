//! Builds the configured `ObjectStoreBackend` (memory or S3/MinIO) and
//! enforces the mandatory-capability contract before the backend is used.

use std::sync::Arc;

use bytes::Bytes;
use ravel_cache::{Cache, CacheLimits};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::s3::{S3Config, S3Store};
use ravel_object_store::{
    Capabilities, ClassedStore, DelimitedList, GetOutcome, GetRange, InstrumentedStore,
    KmsRoutingStore, ListPage, MultipartUpload, ObjectMeta, ObjectStoreBackend, PageToken,
    PutOptions, PutOutcome, SchedulerConfig, StoreError, StoreMetrics,
};
use ravel_query::CacheFetchError;

use crate::config::{Cli, Mode, StoreKind};

/// RAM cache single-entry cap (ADR-0046): comfortably larger than any one
/// planned byte range this process fetches (`ravel_query::fetcher`'s
/// suffix/coalesce/whole-object thresholds all stay well under this), so a
/// legitimate cache admission is never rejected for size.
const CACHE_MAX_ENTRY_BYTES: u64 = 64 * 1024 * 1024;

/// RAM cache entry-count cap (ADR-0046): high enough that `--cache-max-bytes`,
/// not this count, is what drives eviction in practice.
const CACHE_MAX_ENTRIES: usize = 1_000_000;

/// Build the ADR-0046 RAM read cache from CLI config, or `None` when
/// `--disable-cache` is set. `None` must leave query behavior byte-for-byte
/// identical to a build with no cache wiring at all: callers pass it to
/// `SegmentFetcher`/`LogSegmentFetcher`/`QueryEngine`'s `with_cache` only when
/// `Some`.
pub fn build_cache(cli: &Cli) -> Option<Arc<Cache<CacheFetchError>>> {
    if cli.disable_cache {
        return None;
    }
    Some(Arc::new(Cache::new(CacheLimits::new(
        cli.cache_max_bytes,
        CACHE_MAX_ENTRIES,
        CACHE_MAX_ENTRY_BYTES,
    ))))
}

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

/// Build the configured backend, wrap it in the instrumentation decorator,
/// enforce the capability contract for `cli.mode`, and build the ADR-0046
/// read cache from the same CLI config.
///
/// Returns the store the whole process shares, the metrics handle the
/// decorator counts into, and the cache (`None` when `--disable-cache`).
/// Every backend is wrapped, unconditionally and in every mode: the decorator
/// is observability only, never correctness-bearing, and wrapping is a zero
/// behavior change (results forward verbatim and `capabilities()` passes
/// through), so there is no configuration to get wrong and no "instrumented
/// vs not" pair of behaviors to reason about. Because capabilities pass
/// through, the gate below still checks the real backend's declaration.
///
/// The store metrics handle is not yet exposed on any scrape endpoint; it is
/// returned so the caller can hold it for that later work. The cache is
/// exposed: the caller attaches it to the query fetchers via their existing
/// `with_cache` builders.
///
/// The two store handles ([`Self::foreground`], [`Self::background`]) come from
/// a [`ClassedStore`] (ADR-0070 decision 1) wrapping the instrumented backend
/// chain. In the default passthrough construction (decision 2, when
/// `--store-scheduling` is off) both are the same `Arc` as the instrumented
/// store verbatim: `Arc::ptr_eq(&foreground, &background)` holds and there is
/// no scheduling, no added latency, and no per-class metrics. When
/// `--store-scheduling` is on they are two distinct scheduled handles sharing
/// one `RequestScheduler`, and [`Self::classed`] exposes the per-class metrics.
pub struct BuiltStore {
    /// The foreground, ack-bearing store handle: ingest, query, and catalog
    /// paths use this. In passthrough it is the instrumented backend verbatim.
    pub foreground: Arc<dyn ObjectStoreBackend>,
    /// The background maintenance store handle: the maintain, fold, and scrub
    /// loops use this. In passthrough it is the same `Arc` as `foreground`.
    pub background: Arc<dyn ObjectStoreBackend>,
    /// The metrics handle the instrumentation decorator counts every op into,
    /// served at `GET /metrics`. Shared by both class handles (the decorator
    /// sits under the [`ClassedStore`], so it counts foreground and background
    /// traffic alike). Distinct from the per-class metrics on [`Self::classed`].
    pub metrics: Arc<StoreMetrics>,
    /// The ADR-0046 read cache, `None` when `--disable-cache` is set. The caller
    /// attaches it to the query fetchers via their existing `with_cache`
    /// builders.
    pub cache: Option<Arc<Cache<CacheFetchError>>>,
    /// A handle onto the live [`KmsRoutingStore`] embedded in the backend chain,
    /// `Some` only when `--tenant-kms-config` is set on `--store s3`.
    ///
    /// It exists because `build_store` runs before the tenant-hash scheme is
    /// installed (see `main.rs`'s ordering note on its `build_store` call):
    /// hashing a tenant name to register its key with
    /// `KmsRoutingStore::set_tenant_key` needs `TenantId::hash()`, which is not
    /// valid to call yet at this point in startup. `main.rs` holds this handle
    /// and calls [`crate::tenant_kms::configure_tenant_kms`] on it once the
    /// scheme is installed. `None` when no `KmsRoutingStore` was inserted at all
    /// (either `--store memory`, or `--store s3` with no `--tenant-kms-config`).
    pub kms: Option<Arc<KmsRoutingStore>>,
    /// The [`ClassedStore`] both handles were drawn from. Held so the per-class
    /// [`ClassedStore::metrics`] blocks (the `{class}` metric dimension) stay
    /// reachable; wiring them onto the `/metrics` scrape is later work
    /// (E5-T3/T4). `None`-valued per-class metrics in passthrough mode.
    pub classed: Arc<ClassedStore>,
}

/// Delegates every [`ObjectStoreBackend`] method to a shared handle. Exists
/// only so the same [`KmsRoutingStore`] instance can be both the backend
/// [`InstrumentedStore`] wraps for live traffic, and a handle `build_store`
/// returns separately for `main.rs` to call `set_tenant_key` on later:
/// `InstrumentedStore<S>` owns its `S` by value, so wrapping `KmsRoutingStore`
/// directly would leave no other way to reach the same instance afterward.
struct SharedKmsStore(Arc<KmsRoutingStore>);

#[async_trait::async_trait]
impl ObjectStoreBackend for SharedKmsStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.0.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.0.get(key, range).await
    }

    async fn put_multipart<'a>(
        &'a self,
        key: &str,
    ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
        self.0.put_multipart(key).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.0.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.0.list(prefix, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.0.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.0.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        self.0.capabilities()
    }
}

pub fn build_store(cli: &Cli) -> anyhow::Result<BuiltStore> {
    let (store, metrics, kms): (
        Arc<dyn ObjectStoreBackend>,
        Arc<StoreMetrics>,
        Option<Arc<KmsRoutingStore>>,
    ) = match cli.store {
        StoreKind::Memory => {
            let instrumented = InstrumentedStore::new(MemoryStore::new());
            let metrics = instrumented.metrics();
            (Arc::new(instrumented), metrics, None)
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
                kms_key_id: cli.s3_kms_key.clone(),
                session_token: None,
                credentials_file: None,
            };
            let store = S3Store::new(config.clone())
                .map_err(|err| anyhow::anyhow!("failed to build S3 store: {err}"))?;

            // EL-7 (issue #764, ADR-0062 decision 1, ADR-0072 decision 2): off
            // by default. Without --tenant-kms-config this builds exactly
            // today's store, byte-for-byte, no KmsRoutingStore in the chain.
            if cli.tenant_kms_config.is_some() {
                let kms = Arc::new(KmsRoutingStore::new(
                    Arc::new(store) as Arc<dyn ObjectStoreBackend>,
                    config,
                ));
                let instrumented = InstrumentedStore::new(SharedKmsStore(kms.clone()));
                let metrics = instrumented.metrics();
                (Arc::new(instrumented), metrics, Some(kms))
            } else {
                let instrumented = InstrumentedStore::new(store);
                let metrics = instrumented.metrics();
                (Arc::new(instrumented), metrics, None)
            }
        }
    };

    // Runs against the decorator, which passes `capabilities()` straight
    // through, so this still gates on the wrapped backend's own declaration.
    // Checked here, before `store` is moved into the `ClassedStore`; the
    // `ClassedStore` handles also pass `capabilities()` straight through
    // (passthrough returns the inner store verbatim, and each scheduled handle
    // delegates), so the class wrapping does not hide the backend's real caps.
    check_capabilities(store.as_ref(), cli.mode)?;

    // Two-class request scheduling (ADR-0070). The `ClassedStore` wraps the
    // fully-instrumented backend chain, preserving the KMS-routing and
    // instrumentation wrapping order above: the decorator still counts every
    // op, and (when scheduling is on) each class adds its own per-class metrics
    // on top. Off by default (decision 2): `passthrough` hands both callers the
    // instrumented store verbatim, byte-for-byte today's behavior. When
    // `--store-scheduling` is set, `scheduled` installs a shared
    // `RequestScheduler` with a background floor of 1 -- the value that makes
    // ADR-0070's "a foreground acquire is never delayed by more than one
    // in-flight background request" guarantee hold; it is deliberately not a
    // CLI knob.
    let classed = Arc::new(if cli.store_scheduling {
        ClassedStore::scheduled(
            store,
            SchedulerConfig::new(cli.store_fg_permits, cli.store_bg_permits, 1),
        )
    } else {
        ClassedStore::passthrough(store)
    });
    let foreground = classed.foreground();
    let background = classed.background();

    let cache = build_cache(cli);
    Ok(BuiltStore {
        foreground,
        background,
        metrics,
        cache,
        kms,
        classed,
    })
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
            session_token: None,
            credentials_file: None,
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
        let BuiltStore {
            foreground: store,
            metrics,
            kms,
            ..
        } = build_store(&cli).expect("memory backend must build");
        assert!(
            kms.is_none(),
            "no --tenant-kms-config means no KmsRoutingStore handle"
        );
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

    /// EL-7's off-by-default guarantee: `--store s3` with no
    /// `--tenant-kms-config` builds exactly today's chain, no
    /// `KmsRoutingStore` anywhere in it.
    #[test]
    fn build_store_s3_without_tenant_kms_config_inserts_no_kms_routing() {
        use clap::Parser;

        let cli = Cli::try_parse_from([
            "ravel-server",
            "--store",
            "s3",
            "--s3-bucket",
            "ravel-test",
            "--s3-endpoint",
            "http://127.0.0.1:9000",
            "--s3-access-key",
            "test",
            "--s3-secret-key",
            "test",
        ])
        .expect("flags parse");
        let kms = build_store(&cli)
            .expect("dummy S3 config must build without network access")
            .kms;
        assert!(
            kms.is_none(),
            "no --tenant-kms-config must yield no KmsRoutingStore handle"
        );
    }

    /// EL-7's reachability: `--tenant-kms-config` on `--store s3` inserts a
    /// live `KmsRoutingStore` between the raw `S3Store` and the outermost
    /// `InstrumentedStore`, and the returned handle is that same instance
    /// (proven by `set_tenant_key` on the handle changing routing decisions
    /// the returned `store` handle's `put` calls make).
    #[tokio::test]
    async fn build_store_s3_with_tenant_kms_config_inserts_kms_routing() {
        use clap::Parser;
        use std::io::Write;

        let mut file = tempfile::NamedTempFile::new().expect("create temp tenant-kms-config file");
        file.write_all(
            br#"
                [tenants]
                acme = "arn:aws:kms:us-east-1:111122223333:key/acme"
            "#,
        )
        .expect("write temp file");

        let cli = Cli::try_parse_from([
            "ravel-server",
            "--store",
            "s3",
            "--s3-bucket",
            "ravel-test",
            "--s3-endpoint",
            "http://127.0.0.1:9000",
            "--s3-access-key",
            "test",
            "--s3-secret-key",
            "test",
            "--tenant-kms-config",
            file.path().to_str().expect("temp path is valid utf-8"),
        ])
        .expect("flags parse");
        let BuiltStore {
            foreground: store,
            kms,
            ..
        } = build_store(&cli).expect("dummy S3 config must build without network access");
        let kms = kms.expect("--tenant-kms-config must yield a KmsRoutingStore handle");

        assert_eq!(
            store.capabilities(),
            kms.capabilities(),
            "the returned store handle must be (or wrap) the same KmsRoutingStore instance"
        );
    }

    /// E5-T2 (ADR-0070, epic #810): the reachability acceptance test for the
    /// two-class scheduler wiring, driven end-to-end through `build_store` from
    /// a `Cli`.
    ///
    /// (a) `--store-scheduling` OFF (the default) is passthrough: the
    ///     foreground and background handles are the SAME store (`Arc::ptr_eq`),
    ///     byte-for-byte today's single-handle behavior, and there are no
    ///     per-class metrics. This assertion is non-vacuous: it fails if the
    ///     scheduled variant is ever wired by default, because scheduled hands
    ///     out two distinct wrapper handles. The line that keeps it passing is
    ///     the `else { ClassedStore::passthrough(store) }` arm in `build_store`
    ///     (paired with `store_scheduling` defaulting to `false`); flip either
    ///     and this case fails.
    /// (b) `--store-scheduling` ON: the two handles are DISTINCT scheduled
    ///     handles (`Arc::ptr_eq` is false).
    /// (c) a foreground op and a background op each record under their own
    ///     class: the `{class}` dimension is realized as one `StoreMetrics`
    ///     block per class, so the foreground block sees exactly the foreground
    ///     op and the background block exactly the background op.
    #[tokio::test]
    async fn scheduler_wiring_passthrough_and_scheduled() {
        use clap::Parser;
        use ravel_object_store::RequestClass;

        // (a) Off by default: passthrough hands out one shared store.
        let cli = Cli::try_parse_from(["ravel-server"]).expect("defaults parse");
        assert!(
            !cli.store_scheduling,
            "the store scheduler must default off (ADR-0070 decision 2)"
        );
        let built = build_store(&cli).expect("memory backend must build");
        assert!(
            Arc::ptr_eq(&built.foreground, &built.background),
            "passthrough: the foreground and background handles must be the same store"
        );
        assert!(
            built.classed.metrics(RequestClass::Foreground).is_none(),
            "passthrough adds no per-class metrics"
        );

        // (b) Scheduling on: two distinct scheduled handles over one scheduler.
        let cli = Cli::try_parse_from(["ravel-server", "--store-scheduling"])
            .expect("scheduling flag parses");
        assert!(cli.store_scheduling);
        let built = build_store(&cli).expect("scheduled memory backend must build");
        assert!(
            !Arc::ptr_eq(&built.foreground, &built.background),
            "scheduled: the foreground and background handles must be distinct"
        );

        // (c) Each class records its own op under its own `{class}` label.
        built
            .foreground
            .put("t/fg", Bytes::from_static(b"abc"), PutOptions::default())
            .await
            .expect("foreground put");
        built
            .background
            .put("t/bg", Bytes::from_static(b"de"), PutOptions::default())
            .await
            .expect("background put");
        let fg = built
            .classed
            .metrics(RequestClass::Foreground)
            .expect("scheduled mode has foreground metrics");
        let bg = built
            .classed
            .metrics(RequestClass::Background)
            .expect("scheduled mode has background metrics");
        let (fg, bg) = (fg.snapshot(), bg.snapshot());
        assert_eq!(
            fg.put.calls, 1,
            "class=\"foreground\" must record exactly the foreground op"
        );
        assert_eq!(
            bg.put.calls, 1,
            "class=\"background\" must record exactly the background op"
        );
        // Non-vacuous label separation: neither class absorbed the other's op.
        assert_eq!(
            fg.put.bytes, 3,
            "foreground op's payload size, not the bg op's"
        );
        assert_eq!(
            bg.put.bytes, 2,
            "background op's payload size, not the fg op's"
        );
    }
}
