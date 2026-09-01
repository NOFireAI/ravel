//! Prometheus HTTP API compatibility routes that carry little query semantics:
//! `/api/v1/status/buildinfo` and `/api/v1/metadata`.
//!
//! Grafana's built-in Prometheus datasource probes both on every datasource
//! save and on dashboard load. A 404 there makes the datasource test fail
//! outright, even though every query route works, so these exist purely so
//! that client works against ravel-server.
//!
//! `buildinfo` is genuinely stateless. `/api/v1/metadata` is not: as of
//! ADR-0085 decision 1 it reads [`AppState`] to reach the per-process metadata
//! cache and the tenant resolver, so these routes take `AppState` and are built
//! by [`router`] with the same state the query routes use. When no cache is
//! attached the metadata handler still returns the pre-ADR empty object, so a
//! deployment that has not wired the cache (and every existing test) is
//! unaffected.
//!
//! [`AppState`]: crate::http::AppState

use std::collections::BTreeMap;

use axum::extract::{Request, State};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;

use ravel_catalog::MetricKind;

use crate::http::json::ApiResponse;
use crate::http::params::Params;
use crate::http::{AppState, MetadataSnapshot};

/// Git revision of the build, when the build environment provided one.
///
/// `option_env!` rather than `env!`: a plain `cargo build` from a source
/// tarball has no git metadata, and Prometheus itself reports an empty
/// `revision` in that case rather than failing to build. Nothing in the
/// repository sets `RAVEL_GIT_SHA` today, so this is the empty string until
/// a build script or CI exports it.
const REVISION: &str = match option_env!("RAVEL_GIT_SHA") {
    Some(sha) => sha,
    None => "",
};

/// `/api/v1/status/buildinfo` `data` object, field-for-field as Prometheus
/// renders it. Fields Ravel has no equivalent for are empty strings, not
/// invented values: `goVersion` in particular must stay empty because Ravel
/// is not Go, and `version` is Ravel's own crate version, never a Prometheus
/// version string. A client that gates features on a Prometheus version
/// should see Ravel's version and treat it as unknown, not be told a
/// Prometheus release Ravel does not implement.
#[derive(Debug, Serialize)]
struct BuildInfo {
    version: &'static str,
    revision: &'static str,
    branch: &'static str,
    #[serde(rename = "buildUser")]
    build_user: &'static str,
    #[serde(rename = "buildDate")]
    build_date: &'static str,
    #[serde(rename = "goVersion")]
    go_version: &'static str,
}

/// Router carrying the Prometheus compatibility routes, built with `state` so
/// `/api/v1/metadata` can reach the metadata cache and tenant resolver. Merged
/// into the query router, so any service mounting that router serves these too.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/v1/status/buildinfo", get(buildinfo))
        .route("/api/v1/metadata", get(metadata))
        .with_state(state)
}

/// Ravel's build info. `version` is this workspace's crate version (every
/// crate in the workspace, `ravel-server` included, inherits the single
/// `[workspace.package] version`), so it is the server's version too.
async fn buildinfo() -> Json<ApiResponse<BuildInfo>> {
    Json(ApiResponse::success(BuildInfo {
        version: env!("CARGO_PKG_VERSION"),
        revision: REVISION,
        branch: "",
        build_user: "",
        build_date: "",
        go_version: "",
    }))
}

/// One metric family's metadata, in Prometheus' documented `/api/v1/metadata`
/// shape: `data` maps a family name to an array of these. Ravel stores exactly
/// one tuple per name (ADR-0085 decision 1), so the array always has length 1.
#[derive(Debug, Serialize)]
struct MetadataItem {
    #[serde(rename = "type")]
    metric_type: &'static str,
    help: String,
    unit: String,
}

/// Prometheus' spelling of each [`MetricKind`], lowercase as its API renders it.
fn kind_str(kind: MetricKind) -> &'static str {
    match kind {
        MetricKind::Counter => "counter",
        MetricKind::Gauge => "gauge",
        MetricKind::Histogram => "histogram",
        MetricKind::Summary => "summary",
        MetricKind::Unknown => "unknown",
    }
}

/// The pre-ADR-0085 response: `200 {"status":"success","data":{}}`. Served when
/// no metadata cache is attached, and when a request carries no resolvable
/// tenant (this endpoint never `401`s, ADR-0085 read path).
fn empty_metadata() -> Response {
    Json(ApiResponse::success(
        BTreeMap::<String, Vec<MetadataItem>>::new(),
    ))
    .into_response()
}

/// Metric metadata (ADR-0085 decision 1 read path).
///
/// Behavior:
///
/// - No cache attached ([`AppState::metadata_cache`] is `None`): the pre-ADR
///   empty object, so a deployment that has not wired the cache is unchanged.
/// - A cache is attached but the request carries no resolvable tenant: still the
///   empty object. The endpoint was unauthenticated before this ADR and must
///   not become a `401` for an anonymous probe; a request with no tenant simply
///   has no per-tenant metadata to serve. The tenant is resolved directly
///   through the resolver (the same one `/api/v1/labels` uses) so a failure is a
///   plain "no tenant", never mapped to an error response.
/// - A cache is attached and the tenant resolves: that tenant's cached record,
///   projected into Prometheus' shape, honoring the optional `metric` and
///   `limit` query params.
async fn metadata(State(state): State<AppState>, req: Request) -> Response {
    let Some(cache) = state.metadata_cache.clone() else {
        return empty_metadata();
    };
    // Resolve the tenant directly rather than through the error-mapping
    // `authenticate` wrapper: that wrapper only knows how to turn a failure into
    // an error response, and here a missing/invalid credential must fall back to
    // the empty object, not fail the request.
    let Ok(tenant) = state.tenant_resolver.resolve(req.headers()) else {
        return empty_metadata();
    };
    let params = Params::parse(req.uri().query(), None);
    let metric_filter = params.first("metric").map(str::to_string);
    let limit = params
        .first("limit")
        .and_then(|raw| raw.parse::<usize>().ok());

    let snapshot: MetadataSnapshot = cache.get(tenant.hash()).await;
    let data = project(&snapshot, metric_filter.as_deref(), limit);
    Json(ApiResponse::success(data)).into_response()
}

/// Project a cached record into Prometheus' `data` map: one length-1 array per
/// family name, sorted (a `BTreeMap`) so name order is deterministic. `metric`
/// filters to a single family (an unknown name yields an empty map, still a
/// `200`); `limit` caps the number of names kept, applied over the sorted order
/// so the cap is deterministic too.
fn project(
    snapshot: &[ravel_catalog::MetricMetadataEntry],
    metric_filter: Option<&str>,
    limit: Option<usize>,
) -> BTreeMap<String, Vec<MetadataItem>> {
    let mut data: BTreeMap<String, Vec<MetadataItem>> = BTreeMap::new();
    for entry in snapshot {
        if let Some(name) = metric_filter
            && entry.family_name != name
        {
            continue;
        }
        data.insert(
            entry.family_name.clone(),
            vec![MetadataItem {
                metric_type: kind_str(entry.kind),
                help: entry.help.clone(),
                unit: entry.unit.clone(),
            }],
        );
    }
    match limit {
        Some(n) => data.into_iter().take(n).collect(),
        None => data,
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn buildinfo_version_is_ravels_own_crate_version() {
        let json = serde_json::to_value(ApiResponse::success(BuildInfo {
            version: env!("CARGO_PKG_VERSION"),
            revision: REVISION,
            branch: "",
            build_user: "",
            build_date: "",
            go_version: "",
        }))
        .expect("serializes");
        assert_eq!(json["status"], "success");
        assert_eq!(json["data"]["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(
            json["data"]["goVersion"], "",
            "Ravel is not Go; goVersion stays empty rather than invented"
        );
    }

    // The metadata read path (ADR-0085 decision 1). The single-GET acceptance
    // test, the stale-while-revalidate and single-flight refresh tests, the
    // absent-record test, and the LRU/idle-eviction tests live in
    // `super::super::metadata_cache::tests`, driving the cache directly; the
    // tests here drive the HTTP handler, covering the legacy fall-throughs (no
    // cache, no/bad bearer) and the `metric`/`limit` projection.

    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use axum::body::Body;
    use axum::http::StatusCode;
    use bytes::Bytes;
    use ravel_cache::SystemClock;
    use ravel_catalog::{Catalog, CatalogConfig, MetricMetadataEntry, write_metrics_meta};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{
        Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta,
        ObjectStoreBackend, PageToken, PutOptions, PutOutcome, StoreError,
    };
    use ravel_types::{TenantHash, TenantId};

    use crate::EngineConfig;
    use crate::QueryEngine;
    use crate::http::{MetadataCache, MetadataCacheConfig, StaticBearerTokenResolver};

    /// Wraps a store and counts GETs, so a test can prove a fall-through path did
    /// no object-store read at all.
    struct CountingStore {
        inner: Arc<dyn ObjectStoreBackend>,
        gets: AtomicU64,
    }

    impl CountingStore {
        fn new(inner: Arc<dyn ObjectStoreBackend>) -> Arc<Self> {
            Arc::new(CountingStore {
                inner,
                gets: AtomicU64::new(0),
            })
        }
        fn get_count(&self) -> u64 {
            self.gets.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for CountingStore {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<PutOutcome, StoreError> {
            self.inner.put(key, data, opts).await
        }
        async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            self.inner.get(key, range).await
        }
        async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
            self.inner.head(key).await
        }
        async fn list(
            &self,
            prefix: &str,
            page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            self.inner.list(prefix, page).await
        }
        async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
            self.inner.list_delimited(prefix).await
        }
        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }
        fn capabilities(&self) -> Capabilities {
            self.inner.capabilities()
        }
    }

    const TOKEN: &str = "tenant-a-token";

    fn resolver() -> Arc<StaticBearerTokenResolver> {
        let mut tokens = HashMap::new();
        tokens.insert(TOKEN.to_string(), TenantId::new("tenant-a".to_string()));
        Arc::new(StaticBearerTokenResolver::new(tokens))
    }

    fn tenant_hash() -> TenantHash {
        TenantId::new("tenant-a".to_string()).hash()
    }

    /// A minimal `AppState`: a throwaway engine (the metadata handler never
    /// touches it) plus the token resolver, and `metadata_cache` set per test.
    fn app_state(cache: Option<Arc<MetadataCache>>) -> AppState {
        let engine_store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let catalog = Arc::new(
            Catalog::new(engine_store.clone(), CatalogConfig::default()).expect("catalog"),
        );
        let engine = Arc::new(QueryEngine::new(
            catalog,
            engine_store,
            EngineConfig::default(),
        ));
        let mut state = AppState::new(engine, resolver());
        state.metadata_cache = cache;
        state
    }

    fn entry(name: &str, kind: MetricKind, help: &str, unit: &str) -> MetricMetadataEntry {
        MetricMetadataEntry {
            family_name: name.to_string(),
            kind,
            help: help.to_string(),
            unit: unit.to_string(),
            updated_unix_ns: 1,
        }
    }

    fn cache_over(store: Arc<dyn ObjectStoreBackend>) -> Arc<MetadataCache> {
        Arc::new(MetadataCache::new(
            store,
            MetadataCacheConfig::default(),
            Arc::new(SystemClock),
        ))
    }

    async fn call(
        state: AppState,
        uri: &str,
        bearer: Option<&str>,
    ) -> (StatusCode, serde_json::Value) {
        let mut builder = axum::http::Request::builder().method("GET").uri(uri);
        if let Some(token) = bearer {
            builder = builder.header("authorization", format!("Bearer {token}"));
        }
        let req = builder.body(Body::empty()).expect("request");
        let resp = metadata(State(state), req).await;
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .expect("body");
        let json: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
        (status, json)
    }

    #[tokio::test]
    async fn no_cache_attached_keeps_legacy_empty_response() {
        let (status, json) = call(app_state(None), "/api/v1/metadata", Some(TOKEN)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["status"], "success");
        assert_eq!(json["data"], serde_json::json!({}));
    }

    #[tokio::test]
    async fn no_or_bad_bearer_keeps_legacy_empty_response() {
        // A cache is attached over a counting store; a request with no bearer
        // (and one with a bad bearer) must still get the empty object, and must
        // never reach the store.
        let mem: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let store = CountingStore::new(mem);
        let cache = cache_over(store.clone());

        for bearer in [None, Some("not-a-real-token")] {
            let (status, json) =
                call(app_state(Some(cache.clone())), "/api/v1/metadata", bearer).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(json["data"], serde_json::json!({}), "bearer={bearer:?}");
        }
        assert_eq!(
            store.get_count(),
            0,
            "an unresolved tenant must never read the store"
        );
    }

    #[tokio::test]
    async fn metric_param_filters_and_limit_caps() {
        let mem: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        // CreateIfAbsent seeding does no GET, so a later handler GET is countable.
        write_metrics_meta(
            mem.as_ref(),
            &tenant_hash(),
            &[
                entry("aaa_total", MetricKind::Counter, "a help", ""),
                entry("bbb_bytes", MetricKind::Gauge, "b help", "bytes"),
                entry("ccc_seconds", MetricKind::Histogram, "c help", "seconds"),
            ],
            None,
        )
        .await
        .expect("seed");
        let cache = cache_over(mem);

        // No params: all three families, each a length-1 array in Prometheus
        // shape with the mapped type string.
        let (status, json) = call(
            app_state(Some(cache.clone())),
            "/api/v1/metadata",
            Some(TOKEN),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let data = json["data"].as_object().expect("data object");
        assert_eq!(data.len(), 3);
        assert_eq!(json["data"]["bbb_bytes"][0]["type"], "gauge");
        assert_eq!(json["data"]["bbb_bytes"][0]["unit"], "bytes");
        assert_eq!(json["data"]["ccc_seconds"][0]["help"], "c help");

        // metric= filters to one family.
        let (_, json) = call(
            app_state(Some(cache.clone())),
            "/api/v1/metadata?metric=aaa_total",
            Some(TOKEN),
        )
        .await;
        let data = json["data"].as_object().expect("data object");
        assert_eq!(data.len(), 1);
        assert_eq!(json["data"]["aaa_total"][0]["type"], "counter");

        // limit= caps the number of names, over the deterministic sorted order.
        let (_, json) = call(
            app_state(Some(cache.clone())),
            "/api/v1/metadata?limit=2",
            Some(TOKEN),
        )
        .await;
        let data = json["data"].as_object().expect("data object");
        assert_eq!(data.len(), 2, "limit=2 keeps the first two sorted names");
        assert!(data.contains_key("aaa_total"));
        assert!(data.contains_key("bbb_bytes"));
        assert!(!data.contains_key("ccc_seconds"));

        // An unknown metric name is an empty data object, still 200.
        let (status, json) = call(
            app_state(Some(cache)),
            "/api/v1/metadata?metric=does_not_exist",
            Some(TOKEN),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["data"], serde_json::json!({}));
    }
}
