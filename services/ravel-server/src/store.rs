//! Builds the configured `ObjectStoreBackend` (memory or S3/MinIO) and
//! enforces the mandatory-capability contract before the backend is used.

use std::sync::Arc;

use crate::config::{Cli, Mode, StoreKind};
use bytes::Bytes;
use ravel_cache::{Cache, CacheLimits, DiskCache, TieredCache};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::s3::{S3AuthMode, S3Config, S3Store};
use ravel_object_store::{
    Capabilities, ClassedStore, DelimitedList, GetOutcome, GetRange, InstrumentedStore,
    KmsRoutingStore, ListPage, MultipartUpload, ObjectMeta, ObjectStoreBackend, PageToken,
    PutOptions, PutOutcome, SchedulerConfig, StoreError, StoreMetrics,
};

/// RAM cache single-entry cap (ADR-0046): comfortably larger than any one
/// planned byte range this process fetches (`ravel_query::fetcher`'s
/// suffix/coalesce/whole-object thresholds all stay well under this), so a
/// legitimate cache admission is never rejected for size.
const CACHE_MAX_ENTRY_BYTES: u64 = 64 * 1024 * 1024;

/// RAM cache entry-count cap (ADR-0046): high enough that `--cache-max-bytes`,
/// not this count, is what drives eviction in practice.
const CACHE_MAX_ENTRIES: usize = 1_000_000;

/// Build the ADR-0046 read cache from CLI config as a [`ravel_query::ReadCache`]
/// (RAM-only or RAM-over-disk), or `None` when `--disable-cache` is set. `None`
/// must leave query behavior byte-for-byte identical to a build with no cache
/// wiring at all: callers pass it to
/// `SegmentFetcher`/`LogSegmentFetcher`/`QueryEngine`'s `with_cache` only when
/// `Some`.
///
/// With no `--cache-dir`, this returns [`ravel_query::ReadCache::Ram`], byte-for-byte
/// the pre-#97 RAM-only cache (same `--cache-max-bytes` budget and the same
/// [`CACHE_MAX_ENTRY_BYTES`]/[`CACHE_MAX_ENTRIES`] constants).
///
/// With `--cache-dir` set, this returns [`ravel_query::ReadCache::Tiered`]: the RAM
/// tier at the exact same limits as the RAM-only path, over a [`DiskCache`] at
/// `dir`. There is no dedicated CLI flag for disk-tier capacity, and this task
/// adds none, so the disk tier's [`CacheLimits`] reuse the SAME
/// `--cache-max-bytes` number and the SAME two constants as the RAM tier. A
/// reader must not assume `--cache-max-bytes` bounds only RAM once a disk tier
/// exists: it bounds each tier to that number independently.
///
/// [`DiskCache::new`] spawns the ADR-0064 background age-sweep and so must run
/// inside a Tokio runtime; `build_store`'s caller (`main`) is `#[tokio::main]`,
/// so this holds on every `--cache-dir` startup.
pub fn build_cache(cli: &Cli) -> Option<ravel_query::ReadCache> {
    if cli.disable_cache {
        return None;
    }
    let ram_limits = CacheLimits::new(
        cli.cache_max_bytes,
        CACHE_MAX_ENTRIES,
        CACHE_MAX_ENTRY_BYTES,
    );
    match &cli.cache_dir {
        None => Some(ravel_query::ReadCache::Ram(Arc::new(Cache::new(
            ram_limits,
        )))),
        Some(dir) => {
            // The disk tier mirrors the RAM tier's capacity from the same
            // `--cache-max-bytes` number and the same two constants. The
            // `"store"` namespace (issue #671) keeps this fetcher cache's files
            // in `dir/store/`, disjoint from the catalog byte cache that shares
            // the same `--cache-dir`, so the two never collide, double-count, or
            // evict each other's entries.
            let disk = DiskCache::new_in_namespace(dir.clone(), "store", ram_limits);
            let tiered = TieredCache::new(Cache::new(ram_limits), disk);
            Some(ravel_query::ReadCache::Tiered(Arc::new(tiered)))
        }
    }
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
/// NOT made globally mandatory: a gateway/query/all deployment keeps the base
/// `mandatory()` set exactly (see `Capabilities::mandatory`).
///
/// `upload_checksum` is not required by any mode, including maintain, and no
/// mode may add it. Unlike `multipart`, which some backend could supply and
/// which one mode genuinely needs, `upload_checksum` is permanently
/// unsatisfiable by S3, the only durable backend: the `object_store` client
/// cannot put a caller-supplied CRC32C on the wire at all (and
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
    /// The ADR-0046 read cache (RAM-only, or RAM-over-disk when `--cache-dir` is
    /// set), `None` when `--disable-cache` is set. The caller attaches it to the
    /// query fetchers via their existing `with_cache` builders.
    pub cache: Option<ravel_query::ReadCache>,
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
    /// reachable; wiring them onto the `/metrics` scrape is later work.
    /// `None`-valued per-class metrics in passthrough mode.
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

/// The argument error for `--s3-auth instance-role` combined with an inline
/// credential (ADR-0106), or `None` when no inline credential is set.
///
/// `S3Store::new` rejects the same mix, but its message is written for the
/// `S3Config` field names. Operators set flags, so the CLI names the flags
/// (and the env var clap also reads each one from, since a stray exported
/// `RAVEL_S3_*` is the likelier source of the conflict).
fn instance_role_credential_conflict(cli: &Cli) -> Option<anyhow::Error> {
    let conflicting: Vec<&str> = [
        (
            cli.s3_access_key.is_some(),
            "--s3-access-key (RAVEL_S3_ACCESS_KEY)",
        ),
        (
            cli.s3_secret_key.is_some(),
            "--s3-secret-key (RAVEL_S3_SECRET_KEY)",
        ),
        (
            cli.s3_session_token.is_some(),
            "--s3-session-token (RAVEL_S3_SESSION_TOKEN)",
        ),
        (
            cli.s3_credentials_file.is_some(),
            "--s3-credentials-file (RAVEL_S3_CREDENTIALS_FILE)",
        ),
    ]
    .into_iter()
    .filter_map(|(present, name)| present.then_some(name))
    .collect();
    if conflicting.is_empty() {
        return None;
    }
    Some(anyhow::anyhow!(
        "--s3-auth instance-role conflicts with {}: under instance-role every \
         credential comes from the EC2 instance metadata service, so those \
         must be unset (or select --s3-auth static)",
        conflicting.join(", ")
    ))
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
            let auth = cli.s3_auth.mode();
            let (access_key_id, secret_access_key, session_token, credentials_file) = match auth {
                S3AuthMode::Static => (
                    cli.s3_access_key.clone().ok_or_else(|| {
                        anyhow::anyhow!("--store s3 requires RAVEL_S3_ACCESS_KEY")
                    })?,
                    cli.s3_secret_key.clone().ok_or_else(|| {
                        anyhow::anyhow!("--store s3 requires RAVEL_S3_SECRET_KEY")
                    })?,
                    cli.s3_session_token.clone(),
                    cli.s3_credentials_file.clone(),
                ),
                S3AuthMode::InstanceRole => {
                    if let Some(conflict) = instance_role_credential_conflict(cli) {
                        return Err(conflict);
                    }
                    (String::new(), String::new(), None, None)
                }
            };
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
                session_token,
                credentials_file,
                auth,
                instance_metadata_endpoint: cli.s3_instance_metadata_endpoint.clone(),
            };
            // One shared metrics block for the whole S3 chain: the counting HTTP
            // connector inside `S3Store` records billed HTTP requests (attempts,
            // retries included) into it below `object_store`'s retry loop, and
            // the `InstrumentedStore` decorator records completed calls into the
            // same block, so `ravel_store_attempts_total` and
            // `ravel_store_calls_total` come from one snapshot (issue #928). Built
            // before the store so the connector and the decorator share it.
            let metrics = Arc::new(StoreMetrics::default());
            let store = S3Store::with_metrics(config.clone(), Arc::clone(&metrics))
                .map_err(|err| anyhow::anyhow!("failed to build S3 store: {err}"))?;

            // Per-tenant SSE-KMS routing (ADR-0062 decision 1, ADR-0072
            // decision 2): off by default. Without --tenant-kms-config this builds exactly
            // today's store, byte-for-byte, no KmsRoutingStore in the chain.
            if cli.tenant_kms_config.is_some() {
                let kms = Arc::new(KmsRoutingStore::new(
                    Arc::new(store) as Arc<dyn ObjectStoreBackend>,
                    config,
                ));
                let instrumented = InstrumentedStore::with_metrics(
                    SharedKmsStore(kms.clone()),
                    Arc::clone(&metrics),
                );
                (Arc::new(instrumented), metrics, Some(kms))
            } else {
                let instrumented = InstrumentedStore::with_metrics(store, Arc::clone(&metrics));
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

    /// `ravel-server --store s3` must start in every mode against any
    /// S3-compatible endpoint. `Capabilities::mandatory()` must not require
    /// `upload_checksum`, which
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
            .expect("an S3-shaped backend without upload_checksum must start");
        for mode in [Mode::All, Mode::Gateway, Mode::Query] {
            check_capabilities(&backend, mode)
                .unwrap_or_else(|e| panic!("{mode:?} must not require upload_checksum, got: {e}"));
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
            auth: Default::default(),
            instance_metadata_endpoint: None,
        })
        .expect("dummy S3 config must build without network access");
        assert!(
            !store.capabilities().upload_checksum,
            "precondition: S3Store reports upload_checksum unsupported"
        );
        check_mandatory_capabilities(&store).expect("S3Store must satisfy mandatory capabilities");
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
    /// the set to `mandatory()`. The counters are proven to be
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

    /// The SSE-KMS off-by-default guarantee: `--store s3` with no
    /// `--tenant-kms-config` builds exactly the base chain, no
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

    /// SSE-KMS reachability: `--tenant-kms-config` on `--store s3` inserts a
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

    /// `build_store`'s error text, panicking with `context` if it succeeded.
    /// `BuiltStore` is not `Debug`, so `expect_err` is unavailable.
    fn build_store_error(cli: &Cli, context: &str) -> String {
        match build_store(cli) {
            Ok(_) => panic!("{context}"),
            Err(err) => err.to_string(),
        }
    }

    /// Stand up a minimal always-succeeding mock IMDSv2 on an ephemeral
    /// loopback port and return its `http://addr` base. Mirrors
    /// ravel-object-store's own `spawn_ok_imds`; the credential it hands out
    /// is what the mock S3 below expects to see on the wire.
    async fn spawn_mock_imds() -> String {
        use axum::Router;
        use axum::routing::{get, put};

        let app = Router::new()
            .route("/latest/api/token", put(|| async { "mock-token" }))
            .route(
                "/latest/meta-data/iam/security-credentials/",
                get(|| async { "ravel-role" }),
            )
            .route(
                "/latest/meta-data/iam/security-credentials/{role}",
                get(|| async {
                    r#"{"Code":"Success","AccessKeyId":"AKIA_IMDS",
                        "SecretAccessKey":"imds-secret","Token":"imds-token",
                        "Expiration":"2033-11-14T22:13:20Z"}"#
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let endpoint = format!("http://{}", listener.local_addr().expect("addr"));
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        endpoint
    }

    /// A path-style S3 endpoint over an in-memory map: one PUT and one GET,
    /// plus the SigV4 credential the client signed the PUT with, so a test can
    /// prove which credential source the constructed store actually used.
    #[derive(Default)]
    struct MockS3 {
        objects: std::sync::Mutex<std::collections::HashMap<String, Bytes>>,
        signed_with: std::sync::Mutex<Option<(String, String)>>,
    }

    async fn spawn_mock_s3() -> (String, Arc<MockS3>) {
        use axum::Router;
        use axum::extract::{Path as AxumPath, State};
        use axum::http::{HeaderMap, StatusCode, header};
        use axum::response::{IntoResponse, Response};
        use axum::routing::put;

        async fn put_object(
            State(state): State<Arc<MockS3>>,
            AxumPath((_bucket, key)): AxumPath<(String, String)>,
            headers: HeaderMap,
            body: Bytes,
        ) -> Response {
            let header_text = |name: &str| {
                headers
                    .get(name)
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or_default()
                    .to_string()
            };
            *state.signed_with.lock().expect("signed_with lock") = Some((
                header_text("authorization"),
                header_text("x-amz-security-token"),
            ));
            state
                .objects
                .lock()
                .expect("objects lock")
                .insert(key, body);
            (StatusCode::OK, [(header::ETAG, "\"mock-etag\"")], "").into_response()
        }

        /// Serves `Range` the way a real S3-compatible endpoint does: 206 with
        /// a `Content-Range`, or 416 when no part of the range exists. The
        /// adapter reads a whole object as bounded ranged requests, so a mock
        /// that answered 200 here would be rejected as a non-partial response
        /// before its body was ever read.
        async fn get_object(
            State(state): State<Arc<MockS3>>,
            AxumPath((_bucket, key)): AxumPath<(String, String)>,
            headers: HeaderMap,
        ) -> Response {
            let found = state
                .objects
                .lock()
                .expect("objects lock")
                .get(&key)
                .cloned();
            let Some(data) = found else {
                return StatusCode::NOT_FOUND.into_response();
            };
            // All three RFC 7233 byte-range forms, not just the closed one:
            // `S3Store::get` emits `bytes=-N` for `GetRange::Suffix`, which is
            // how every footer in this codebase is read, so a mock that parses
            // only `bytes=A-B` fails such a request with 416 for a reason that
            // has nothing to do with the code under test.
            let requested = headers
                .get(header::RANGE)
                .and_then(|value| value.to_str().ok())
                .and_then(|spec| spec.strip_prefix("bytes=")?.split_once('-'))
                .map(|(start, end)| {
                    let len = data.len();
                    match (start.trim(), end.trim()) {
                        // bytes=-N: the last N bytes.
                        ("", n) => n.parse::<usize>().ok().and_then(|n| {
                            (n > 0 && len > 0).then(|| (len.saturating_sub(n), len - 1))
                        }),
                        // bytes=A-: from A through the end.
                        (s, "") => s
                            .parse::<usize>()
                            .ok()
                            .and_then(|s| (s < len).then(|| (s, len - 1))),
                        // bytes=A-B, inclusive, clamped to the object.
                        (s, e) => match (s.parse::<usize>(), e.parse::<usize>()) {
                            (Ok(s), Ok(e)) if s < len && s <= e => Some((s, e.min(len - 1))),
                            _ => None,
                        },
                    }
                });
            let (status, body, content_range) = match requested {
                None => (StatusCode::OK, data.clone(), None),
                Some(Some((start, end))) => (
                    StatusCode::PARTIAL_CONTENT,
                    data.slice(start..end + 1),
                    Some(format!("bytes {start}-{end}/{}", data.len())),
                ),
                Some(None) => return StatusCode::RANGE_NOT_SATISFIABLE.into_response(),
            };
            let mut response = (
                status,
                [
                    (header::ETAG, "\"mock-etag\"".to_string()),
                    (
                        header::LAST_MODIFIED,
                        "Wed, 21 Oct 2020 07:28:00 GMT".to_string(),
                    ),
                ],
                body,
            )
                .into_response();
            if let Some(value) = content_range
                && let Ok(value) = value.parse()
            {
                response.headers_mut().insert(header::CONTENT_RANGE, value);
            }
            response
        }

        let state = Arc::new(MockS3::default());
        let app = Router::new()
            .route("/{bucket}/{*key}", put(put_object).get(get_object))
            .with_state(Arc::clone(&state));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let endpoint = format!("http://{}", listener.local_addr().expect("addr"));
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (endpoint, state)
    }

    /// The ADR-0106 reachability acceptance test: parsed CLI args go through
    /// the real `build_store` entry point with `--s3-auth instance-role` and no
    /// keys anywhere, the credential comes from a mock IMDS reached via
    /// `--s3-instance-metadata-endpoint`, and the constructed backend serves a
    /// put/get round trip. Without this the provider is unreachable from any
    /// shipped binary.
    ///
    /// Non-vacuous on the credential source, not just on "it built": the mock
    /// S3 records the `Authorization` and `x-amz-security-token` headers, and
    /// they must carry the key id and token the mock IMDS minted. A store that
    /// signed with anything else (or with no token at all) fails here.
    ///
    /// `spawn_blocking`: `S3Store::new` blocks on the eager IMDS fetch, which
    /// has to reach the mock task on this same runtime.
    #[tokio::test(flavor = "multi_thread")]
    async fn instance_role_auth_builds_serving_store() {
        use clap::Parser;

        let imds = spawn_mock_imds().await;
        let (s3_endpoint, mock) = spawn_mock_s3().await;

        let cli = Cli::try_parse_from([
            "ravel-server",
            "--store",
            "s3",
            "--s3-bucket",
            "ravel-test",
            "--s3-endpoint",
            &s3_endpoint,
            "--s3-auth",
            "instance-role",
            "--s3-instance-metadata-endpoint",
            &imds,
        ])
        .expect("instance-role flags parse");
        assert!(
            cli.s3_access_key.is_none() && cli.s3_secret_key.is_none(),
            "precondition: instance-role starts with no inline keys set"
        );

        let built = tokio::task::spawn_blocking(move || build_store(&cli))
            .await
            .expect("join")
            .expect("instance-role build_store must construct against the mock IMDS");

        built
            .foreground
            .put("t/k", Bytes::from_static(b"hello"), PutOptions::default())
            .await
            .expect("put through the instance-role store");
        let got = built
            .foreground
            .get("t/k", GetRange::Full)
            .await
            .expect("get through the instance-role store");
        assert_eq!(
            got.data.as_ref(),
            b"hello",
            "the constructed backend must serve back what it stored"
        );

        let (authorization, token) = mock
            .signed_with
            .lock()
            .expect("signed_with lock")
            .clone()
            .expect("the mock S3 must have seen the signed PUT");
        assert!(
            authorization.contains("AKIA_IMDS"),
            "the request must be signed with the IMDS key id, got: {authorization}"
        );
        assert_eq!(
            token, "imds-token",
            "the request must carry the IMDS session token"
        );
    }

    /// `--s3-auth instance-role` plus any inline credential flag is an
    /// argument error naming both flags, refused before any IMDS contact (no
    /// mock is running here, and the test does not hang or wait on a network
    /// timeout). Each flag is exercised on its own so one dropped from the
    /// check is caught.
    ///
    /// The message assertions are what make this non-vacuous: `S3Store::new`
    /// would reject the same mix, but with the `S3Config` field names, so an
    /// error naming `--s3-access-key` can only have come from the CLI-level
    /// check.
    #[test]
    fn instance_role_auth_rejects_inline_keys() {
        use clap::Parser;

        for (flag, value) in [
            ("--s3-access-key", "AKIA_INLINE"),
            ("--s3-secret-key", "inline-secret"),
            ("--s3-session-token", "inline-token"),
            ("--s3-credentials-file", "/nonexistent/creds.json"),
        ] {
            let cli = Cli::try_parse_from([
                "ravel-server",
                "--store",
                "s3",
                "--s3-bucket",
                "ravel-test",
                "--s3-endpoint",
                "http://127.0.0.1:9000",
                "--s3-auth",
                "instance-role",
                flag,
                value,
            ])
            .expect("flags parse");

            let rendered =
                build_store_error(&cli, &format!("instance-role plus {flag} must be an error"));
            assert!(
                rendered.contains("--s3-auth instance-role") && rendered.contains(flag),
                "the error must name both conflicting flags, got: {rendered}"
            );
        }
    }

    /// `--s3-auth` defaults to `static`, and static mode still requires both
    /// keys with their exact pre-ADR-0106 messages. This is the
    /// no-behavior-change half of the change: the new flag must not have made
    /// any existing deployment's startup differ.
    #[test]
    fn static_auth_is_the_default_and_still_requires_both_keys() {
        use clap::Parser;

        let base = [
            "ravel-server",
            "--store",
            "s3",
            "--s3-bucket",
            "ravel-test",
            "--s3-endpoint",
            "http://127.0.0.1:9000",
        ];
        let cli = Cli::try_parse_from(base).expect("flags parse");
        assert_eq!(
            cli.s3_auth,
            crate::config::S3Auth::Static,
            "--s3-auth must default to static"
        );
        assert!(cli.s3_session_token.is_none() && cli.s3_credentials_file.is_none());
        assert!(cli.s3_instance_metadata_endpoint.is_none());

        assert_eq!(
            build_store_error(&cli, "static mode without keys must fail"),
            "--store s3 requires RAVEL_S3_ACCESS_KEY",
            "the access-key error text must be unchanged"
        );

        let mut with_key = base.to_vec();
        with_key.extend(["--s3-access-key", "test"]);
        let cli = Cli::try_parse_from(with_key).expect("flags parse");
        assert_eq!(
            build_store_error(&cli, "static mode without a secret key must fail"),
            "--store s3 requires RAVEL_S3_SECRET_KEY",
            "the secret-key error text must be unchanged"
        );
    }

    /// The ADR-0072 decision 1 flags reach `S3Config` rather than being parsed
    /// and dropped: `--s3-credentials-file` is read at construction, so a
    /// missing file fails the build, and a valid one builds. That failure is
    /// only reachable if the flag's value was actually placed in
    /// `S3Config::credentials_file`.
    #[test]
    fn session_token_and_credentials_file_flags_reach_the_store() {
        use clap::Parser;
        use std::io::Write;

        let base = [
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
        ];

        let mut with_token = base.to_vec();
        with_token.extend(["--s3-session-token", "sts-token"]);
        let cli = Cli::try_parse_from(with_token).expect("flags parse");
        assert_eq!(cli.s3_session_token.as_deref(), Some("sts-token"));
        build_store(&cli).expect("a session token must not break construction");

        let mut missing_file = base.to_vec();
        missing_file.extend(["--s3-credentials-file", "/nonexistent/ravel-creds.json"]);
        let cli = Cli::try_parse_from(missing_file).expect("flags parse");
        let err = build_store_error(
            &cli,
            "an unreadable --s3-credentials-file must fail construction",
        );
        assert!(
            err.contains("/nonexistent/ravel-creds.json"),
            "the error must name the path the flag carried, got: {err}"
        );

        let mut file = tempfile::NamedTempFile::new().expect("create temp credentials file");
        file.write_all(br#"{"access_key_id":"AKIA_FILE","secret_access_key":"file-secret"}"#)
            .expect("write temp file");
        let mut with_file = base.to_vec();
        let file_path = file.path().to_str().expect("temp path is valid utf-8");
        with_file.extend(["--s3-credentials-file", file_path]);
        let cli = Cli::try_parse_from(with_file).expect("flags parse");
        build_store(&cli).expect("a readable --s3-credentials-file must build");
    }

    /// The reachability acceptance test (ADR-0070) for the
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
