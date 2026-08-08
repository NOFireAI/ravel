//! Per-tenant SSE-KMS key routing decorator (ADR-0062 decision 1a).
//!
//! [`KmsRoutingStore`] wraps a default [`ObjectStoreBackend`] and routes write
//! operations for tenants that have a configured KMS key to a lazily-built,
//! cached per-tenant [`S3Store`] whose builder was given that tenant's
//! `kms_key_id`. Every other operation delegates straight to the default
//! store.
//!
//! # Why the key alone decides
//!
//! Every tenant-scoped object key in this codebase begins with
//! `t/<tenant_hash_hex>/` (see `ravel-commit`'s key builders). Keys outside
//! that shape (`sys/*`, `admission/*`, ...) are never tenant-scoped. Because
//! the object key is passed to every [`ObjectStoreBackend`] method, key
//! selection for a write is decidable per operation from the key alone --- no
//! trait change, no per-tenant handle threaded through call sites (ADR-0062
//! decision 1a; the same "one seam, zero threading" shape ADR-0055 used for
//! authorization).
//!
//! # Reads never select a key
//!
//! SSE-KMS decryption on GET is transparent: S3 records the key in the object's
//! metadata and decrypts server-side given `kms:Decrypt` permission, so the
//! reader needs no client-side key selection. Every read (`get`, `head`,
//! `list`, `list_delimited`), plus `delete` and any key that does not start
//! with `t/`, delegates to the default store unconditionally.
//!
//! # Malformed keys degrade to the default store
//!
//! A key that starts with `t/` but does not have the `t/<non-empty>/...` shape
//! (a bare `t/`, `t//...`, or `t/<hash>` with no trailing segment) parses to no
//! tenant hash and routes to the default store rather than panicking. Content
//! validity of the hash segment is deliberately not checked here: the
//! configured-key map lookup is the authority, so an unrecognized hash falls
//! through to the default store exactly as a structurally malformed one does.
//! This never realistically happens given how this crate's callers build keys,
//! but a decorator on the object-store boundary must not panic on adversarial
//! input.
//!
//! # What this task does not do
//!
//! CLI/config flag parsing (`--s3-kms-key`, `--tenant-kms-key`) and server
//! startup wiring belong to EL-7 and are out of scope here. This module ships
//! the decorator and its construction API; a later task populates it from
//! process arguments without changing this crate's shape.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;

use crate::s3::{S3Config, S3Store};
use crate::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
    ObjectStoreBackend, PageToken, PutOptions, PutOutcome, StoreError,
};

/// Builds a per-tenant backend from a fully-configured [`S3Config`] (the
/// default config cloned with `kms_key_id` overridden). Boxed so tests can
/// inject a fake that records the config it was built with instead of a real
/// [`S3Store`], which has no live endpoint under test. The default
/// implementation (see [`KmsRoutingStore::new`]) builds an [`S3Store`] the same
/// way the default store was built.
type TenantStoreBuilder =
    Box<dyn Fn(&S3Config) -> Result<Box<dyn ObjectStoreBackend>, StoreError> + Send + Sync>;

/// A per-tenant store, cached for the process lifetime. Stored as `&'static`
/// (via [`Box::leak`], see [`KmsRoutingStore::tenant_store`]) so a multipart
/// handle returned from it can satisfy the [`ObjectStoreBackend::put_multipart`]
/// lifetime without a self-referential wrapper. The leak is bounded by the
/// number of distinct tenants with a configured key --- built once per tenant,
/// never rebuilt --- each of which would live for the process lifetime anyway.
type CachedStore = &'static dyn ObjectStoreBackend;

/// Routes tenant writes to per-tenant SSE-KMS stores; everything else goes to
/// the default store. Composes wherever the default `S3Store` sits today
/// (wrapped by `InstrumentedStore` per the existing pattern). See the
/// [module docs](self).
pub struct KmsRoutingStore {
    /// The store every read, delete, non-`t/` key, and unconfigured-tenant
    /// write delegates to. This is the deployment default (shared key, or
    /// bucket-default SSE) --- ADR-0062 decision 1c.
    default: Arc<dyn ObjectStoreBackend>,
    /// The default store's config, cloned and `kms_key_id`-overridden to build
    /// each per-tenant store.
    default_config: S3Config,
    /// `tenant_hash_hex -> key_arn`. Configured via [`Self::set_tenant_key`],
    /// consulted on every write to decide routing.
    tenant_keys: Mutex<HashMap<String, String>>,
    /// Lazily-built per-tenant store cache, keyed by `tenant_hash_hex`. Built
    /// on the first routed write for a tenant and reused thereafter.
    tenant_stores: Mutex<HashMap<String, CachedStore>>,
    /// How a per-tenant store is constructed from its config.
    builder: TenantStoreBuilder,
}

/// Parse the `<tenant_hash_hex>` segment out of a `t/<hash>/...` key.
///
/// Returns `None` (route to the default store) for any key that is not
/// tenant-scoped: no `t/` prefix, an empty hash segment (`t//...`), or no
/// segment after the hash (`t/` or `t/<hash>` with no trailing `/`). The hash's
/// content is not validated (see the module docs on malformed keys).
fn parse_tenant_hash(key: &str) -> Option<&str> {
    let rest = key.strip_prefix("t/")?;
    let (hash, _) = rest.split_once('/')?;
    if hash.is_empty() { None } else { Some(hash) }
}

impl KmsRoutingStore {
    /// Wrap `default` (the deployment-default store) and build per-tenant stores
    /// by cloning `default_config`, overriding only `kms_key_id`, and calling
    /// [`S3Store::new`] --- the same construction path the default store used.
    pub fn new(default: Arc<dyn ObjectStoreBackend>, default_config: S3Config) -> Self {
        Self::with_builder(
            default,
            default_config,
            Box::new(|config: &S3Config| {
                Ok(Box::new(S3Store::new(config.clone())?) as Box<dyn ObjectStoreBackend>)
            }),
        )
    }

    /// [`Self::new`] with an injected per-tenant store builder, for tests that
    /// must observe which config a routed write's store was built with (a real
    /// [`S3Store`] has no live endpoint under test).
    fn with_builder(
        default: Arc<dyn ObjectStoreBackend>,
        default_config: S3Config,
        builder: TenantStoreBuilder,
    ) -> Self {
        KmsRoutingStore {
            default,
            default_config,
            tenant_keys: Mutex::new(HashMap::new()),
            tenant_stores: Mutex::new(HashMap::new()),
            builder,
        }
    }

    /// Register (or overwrite) a tenant's KMS key. Designed so a later task
    /// (EL-7) can wire `--tenant-kms-key` into it without changing this crate's
    /// shape. Registering a key that a store was already built under does not
    /// rebuild that store: the first build wins for the process lifetime (key
    /// rotation and epochs are EL-2's concern, not this decorator's).
    pub fn set_tenant_key(&self, tenant_hash_hex: String, key_arn: String) {
        self.tenant_keys.lock().insert(tenant_hash_hex, key_arn);
    }

    /// The store a write to `key` should go through: the per-tenant store when
    /// the key names a tenant with a configured KMS key, otherwise `None`
    /// (delegate to the default store). Building a missing per-tenant store can
    /// fail (config shape), so this returns a `Result`.
    fn routed_store(&self, key: &str) -> Result<Option<CachedStore>, StoreError> {
        let Some(hash) = parse_tenant_hash(key) else {
            return Ok(None);
        };
        let arn = self.tenant_keys.lock().get(hash).cloned();
        match arn {
            Some(arn) => Ok(Some(self.tenant_store(hash, &arn)?)),
            None => Ok(None),
        }
    }

    /// Get or lazily build the per-tenant store for `tenant_hash_hex`,
    /// configured with `key_arn`. Built at most once per tenant: the cache is
    /// checked and populated under one lock, and the builder is synchronous so
    /// nothing is awaited while the lock is held.
    fn tenant_store(
        &self,
        tenant_hash_hex: &str,
        key_arn: &str,
    ) -> Result<CachedStore, StoreError> {
        let mut cache = self.tenant_stores.lock();
        if let Some(store) = cache.get(tenant_hash_hex) {
            return Ok(*store);
        }
        let mut config = self.default_config.clone();
        config.kms_key_id = Some(key_arn.to_string());
        let built = (self.builder)(&config)?;
        // Leak to `&'static`: the store lives for the process (a per-tenant
        // singleton, built once), and a `&'static` backend lets a multipart
        // handle returned below satisfy `put_multipart`'s lifetime without a
        // self-referential owner. See `CachedStore`.
        let leaked: CachedStore = Box::leak(built);
        cache.insert(tenant_hash_hex.to_string(), leaked);
        Ok(leaked)
    }
}

#[async_trait::async_trait]
impl ObjectStoreBackend for KmsRoutingStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        match self.routed_store(key)? {
            Some(store) => store.put(key, data, opts).await,
            None => self.default.put(key, data, opts).await,
        }
    }

    async fn put_multipart<'a>(
        &'a self,
        key: &str,
    ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
        // The initiation carries the SSE-KMS configuration, so a tenant's
        // multipart upload must start on its per-tenant store to be encrypted
        // under its key. The `&'static` cached store makes the returned handle
        // outlive this call trivially.
        match self.routed_store(key)? {
            Some(store) => store.put_multipart(key).await,
            None => self.default.put_multipart(key).await,
        }
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        // Reads never select a key: SSE-KMS decryption is server-side.
        self.default.get(key, range).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.default.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.default.list(prefix, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.default.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        // Delete is never key-routed: the object is identified by key, and no
        // encryption decision rides on a DELETE.
        self.default.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        // The decorator adds no capability and removes none: routed writes go
        // to stores built from the same config, so the default store's
        // declaration is authoritative (and is what the startup gate must see).
        self.default.capabilities()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// One recorded operation: which store handled it and the key it saw.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Event {
        /// The store's label: `"default"` for the default store, or the
        /// per-tenant store's `kms_key_id` (its configured key ARN).
        store: String,
        op: &'static str,
        key: String,
    }

    /// A fake [`ObjectStoreBackend`] that records every operation it handled
    /// into a shared log, tagged with its own label. Used for both the default
    /// store and each per-tenant store so a test can prove exactly which store a
    /// call reached. No live endpoint, following `s3.rs`'s no-network tests.
    struct RecordingStore {
        label: String,
        events: Arc<Mutex<Vec<Event>>>,
    }

    impl RecordingStore {
        fn record(&self, op: &'static str, key: &str) {
            self.events.lock().push(Event {
                store: self.label.clone(),
                op,
                key: key.to_string(),
            });
        }
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for RecordingStore {
        async fn put(
            &self,
            key: &str,
            _data: Bytes,
            _opts: PutOptions,
        ) -> Result<PutOutcome, StoreError> {
            self.record("put", key);
            Ok(PutOutcome {
                etag: crate::Etag("fake".into()),
                version: crate::Version("fake".into()),
            })
        }

        async fn get(&self, key: &str, _range: GetRange) -> Result<GetOutcome, StoreError> {
            self.record("get", key);
            Ok(GetOutcome {
                data: Bytes::new(),
                etag: crate::Etag("fake".into()),
                version: crate::Version("fake".into()),
                total_size: 0,
            })
        }

        async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
            self.record("head", key);
            Err(StoreError::NotFound)
        }

        async fn list(
            &self,
            prefix: &str,
            _page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            self.record("list", prefix);
            Ok(ListPage {
                objects: Vec::new(),
                next: None,
            })
        }

        async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
            self.record("list_delimited", prefix);
            Ok(DelimitedList {
                objects: Vec::new(),
                common_prefixes: Vec::new(),
            })
        }

        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.record("delete", key);
            Ok(())
        }

        fn capabilities(&self) -> Capabilities {
            Capabilities::mandatory()
        }
    }

    /// Shared test rig: a default `RecordingStore`, a shared event log every
    /// store writes to, a build log capturing each per-tenant build's config,
    /// and a build counter.
    struct Rig {
        store: KmsRoutingStore,
        events: Arc<Mutex<Vec<Event>>>,
        build_configs: Arc<Mutex<Vec<S3Config>>>,
        builds: Arc<AtomicUsize>,
    }

    fn base_config() -> S3Config {
        S3Config {
            bucket: "ravel-test".to_string(),
            region: "us-east-1".to_string(),
            endpoint: Some("http://localhost:0".to_string()),
            access_key_id: "test".to_string(),
            secret_access_key: "test".to_string(),
            allow_http: true,
            force_path_style: true,
            kms_key_id: None,
        }
    }

    fn rig() -> Rig {
        let events = Arc::new(Mutex::new(Vec::new()));
        let build_configs = Arc::new(Mutex::new(Vec::new()));
        let builds = Arc::new(AtomicUsize::new(0));

        let default = Arc::new(RecordingStore {
            label: "default".to_string(),
            events: Arc::clone(&events),
        });

        let builder_events = Arc::clone(&events);
        let builder_configs = Arc::clone(&build_configs);
        let builder_count = Arc::clone(&builds);
        let builder: TenantStoreBuilder = Box::new(move |config: &S3Config| {
            builder_count.fetch_add(1, Ordering::SeqCst);
            builder_configs.lock().push(config.clone());
            // Label the per-tenant fake by the key it was built with, so an
            // event proves both routing and the config the store carries.
            let label = config
                .kms_key_id
                .clone()
                .expect("routed store is always built with a kms_key_id");
            Ok(Box::new(RecordingStore {
                label,
                events: Arc::clone(&builder_events),
            }) as Box<dyn ObjectStoreBackend>)
        });

        let store = KmsRoutingStore::with_builder(default, base_config(), builder);
        Rig {
            store,
            events,
            build_configs,
            builds,
        }
    }

    // Realistic-shaped tenant hashes (hex); any string works, the map keys on
    // the exact segment text.
    const TENANT_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const TENANT_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const KEY_A: &str = "arn:aws:kms:us-east-1:111122223333:key/tenant-a";

    fn events_for(rig: &Rig, store: &str) -> Vec<Event> {
        rig.events
            .lock()
            .iter()
            .filter(|e| e.store == store)
            .cloned()
            .collect()
    }

    /// The core routing proof (ADR-0062 decision 1a): tenant A has a configured
    /// KMS key, tenant B does not. A write under `t/<A>/...` goes through a
    /// store built with A's `kms_key_id`; a write under `t/<B>/...` and every
    /// read for both tenants go through the unmodified default store.
    #[tokio::test]
    async fn writes_route_to_per_tenant_kms_store() {
        let rig = rig();
        rig.store
            .set_tenant_key(TENANT_A.to_string(), KEY_A.to_string());

        let key_a = format!("t/{TENANT_A}/seg/0001");
        let key_b = format!("t/{TENANT_B}/seg/0001");

        rig.store
            .put(&key_a, Bytes::from_static(b"a"), PutOptions::default())
            .await
            .expect("write to tenant A");
        rig.store
            .put(&key_b, Bytes::from_static(b"b"), PutOptions::default())
            .await
            .expect("write to tenant B");
        rig.store
            .get(&key_a, GetRange::Full)
            .await
            .expect("read tenant A");
        rig.store
            .get(&key_b, GetRange::Full)
            .await
            .expect("read tenant B");

        // Tenant A's write --- and only that write --- reached the store built
        // with A's key ARN.
        assert_eq!(
            events_for(&rig, KEY_A),
            vec![Event {
                store: KEY_A.to_string(),
                op: "put",
                key: key_a.clone(),
            }],
            "tenant A's write must route through the store built with A's kms_key_id"
        );

        // The store carrying A's traffic was built with exactly A's key.
        let configs = rig.build_configs.lock();
        assert_eq!(configs.len(), 1, "only tenant A's store is ever built");
        assert_eq!(configs[0].kms_key_id.as_deref(), Some(KEY_A));

        // Tenant B's write and both reads went to the default store, untouched.
        assert_eq!(
            events_for(&rig, "default"),
            vec![
                Event {
                    store: "default".to_string(),
                    op: "put",
                    key: key_b.clone(),
                },
                Event {
                    store: "default".to_string(),
                    op: "get",
                    key: key_a,
                },
                Event {
                    store: "default".to_string(),
                    op: "get",
                    key: key_b,
                },
            ],
            "unconfigured-tenant writes and all reads go to the default store"
        );
    }

    /// Every read operation for a configured tenant delegates to the default
    /// store: SSE-KMS decryption is server-side, so reads never select a key.
    #[tokio::test]
    async fn all_reads_delegate_to_default_even_for_configured_tenant() {
        let rig = rig();
        rig.store
            .set_tenant_key(TENANT_A.to_string(), KEY_A.to_string());
        let key = format!("t/{TENANT_A}/seg/0001");
        let prefix = format!("t/{TENANT_A}/");

        rig.store.get(&key, GetRange::Full).await.expect("get");
        let _ = rig.store.head(&key).await;
        rig.store.list(&prefix, None).await.expect("list");
        rig.store
            .list_delimited(&prefix)
            .await
            .expect("list_delimited");
        rig.store.delete(&key).await.expect("delete");

        let default_ops: Vec<&'static str> =
            events_for(&rig, "default").iter().map(|e| e.op).collect();
        assert_eq!(
            default_ops,
            vec!["get", "head", "list", "list_delimited", "delete"],
        );
        // No per-tenant store was ever built: no write happened.
        assert_eq!(rig.builds.load(Ordering::SeqCst), 0);
    }

    /// A key with no `t/` prefix always routes to the default store, regardless
    /// of any configured tenant keys.
    #[tokio::test]
    async fn non_tenant_keys_route_to_default() {
        let rig = rig();
        rig.store
            .set_tenant_key(TENANT_A.to_string(), KEY_A.to_string());

        for key in ["sys/something", "admission/lease", "prov/x"] {
            rig.store
                .put(key, Bytes::from_static(b"x"), PutOptions::default())
                .await
                .expect("write to non-tenant key");
        }

        assert!(
            events_for(&rig, KEY_A).is_empty(),
            "no non-tenant write may reach a per-tenant store"
        );
        assert_eq!(events_for(&rig, "default").len(), 3);
        assert_eq!(rig.builds.load(Ordering::SeqCst), 0);
    }

    /// The per-tenant store is built lazily (first routed write) and only once
    /// per tenant: N writes to the same tenant build one store and reuse it.
    #[tokio::test]
    async fn per_tenant_store_built_lazily_and_once() {
        let rig = rig();
        // Registering a key builds nothing.
        rig.store
            .set_tenant_key(TENANT_A.to_string(), KEY_A.to_string());
        assert_eq!(
            rig.builds.load(Ordering::SeqCst),
            0,
            "registering a key must not build a store"
        );

        for i in 0..5 {
            let key = format!("t/{TENANT_A}/seg/{i:04}");
            rig.store
                .put(&key, Bytes::from_static(b"x"), PutOptions::default())
                .await
                .expect("routed write");
        }

        assert_eq!(
            rig.builds.load(Ordering::SeqCst),
            1,
            "five writes to one tenant must build its store exactly once"
        );
        assert_eq!(events_for(&rig, KEY_A).len(), 5, "all five writes routed");
    }

    /// A `t/`-prefixed key with a missing or empty tenant-hash segment degrades
    /// safely to the default store rather than panicking.
    #[tokio::test]
    async fn malformed_tenant_key_degrades_to_default() {
        let rig = rig();
        // Even a configured tenant does not save a structurally malformed key
        // from falling through: routing is by the parsed segment.
        rig.store
            .set_tenant_key(TENANT_A.to_string(), KEY_A.to_string());

        // No trailing segment after `t/`, empty hash, and a bare prefix.
        for key in ["t/", "t//seg/0001", "t/onlyhash"] {
            rig.store
                .put(key, Bytes::from_static(b"x"), PutOptions::default())
                .await
                .expect("malformed key must not panic and must write");
        }

        assert!(
            events_for(&rig, KEY_A).is_empty(),
            "a malformed t/ key must not reach a per-tenant store"
        );
        assert_eq!(events_for(&rig, "default").len(), 3);
        assert_eq!(rig.builds.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn parse_tenant_hash_shapes() {
        assert_eq!(parse_tenant_hash("t/abc/seg/1"), Some("abc"));
        assert_eq!(parse_tenant_hash("t/abc/"), Some("abc"));
        assert_eq!(parse_tenant_hash("sys/x"), None);
        assert_eq!(parse_tenant_hash("t/"), None);
        assert_eq!(parse_tenant_hash("t//seg"), None);
        assert_eq!(parse_tenant_hash("t/onlyhash"), None);
        assert_eq!(parse_tenant_hash(""), None);
    }
}
