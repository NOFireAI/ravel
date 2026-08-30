//! Prometheus-compatible HTTP API, exported as a plain axum `Router`
//! (docs/query-engine.md "HTTP API"). Library only: binding a listener and
//! wiring this into a service is left to the caller.

mod compat;
mod error;
mod handlers;
mod json;
mod metadata_cache;
mod params;
pub mod tenant;

use std::sync::Arc;

use axum::Router;
use axum::routing::get;
use ravel_maintain::{NoopQueryAuditSink, QueryAuditSink};
use ravel_types::accounting::{NoopQueryCostRecorder, QueryCostRecorder};

pub use error::{MSG_CORRUPT, MSG_UNAVAILABLE, MSG_UNSATISFIABLE, QueryErrorResponse};
pub use metadata_cache::{
    MetadataCache, MetadataCacheConfig, MetadataCacheCounters, MetadataSnapshot,
};
pub use tenant::{
    AuthError, DevHeaderTenantResolver, MtlsResolver, OidcError, OidcJwksCache, OidcResolver,
    StaticBearerTokenResolver, TenantResolver,
};

use crate::{QueryAdmissionController, QueryConcurrencyLimit, QueryEngine};

const ONE_HOUR_NS: i64 = 60 * 60 * 1_000_000_000;

/// Shared state for every route: the query engine, the tenant resolution
/// strategy, and the per-query cost recorder. There is no `Default`: callers
/// must pick a `TenantResolver` explicitly (default-deny, docs/query-engine.md).
///
/// Construct with [`AppState::new`], which defaults the cost recorder to the
/// no-op, so a caller that mounts the router without a `/metrics` aggregator,
/// and every test, needs no recorder of its own. A deployment attaches a real
/// one with [`AppState::with_cost_recorder`].
#[derive(Clone)]
pub struct AppState {
    pub engine: Arc<QueryEngine>,
    pub tenant_resolver: Arc<dyn TenantResolver>,
    /// Records each completed query's cost (its accounting snapshot and its
    /// pre-execution estimate) into a process aggregate exported at `/metrics`
    /// (ADR-0044 section 4). Defaults to
    /// [`NoopQueryCostRecorder`]; a deployment sets the real aggregator so the
    /// Prometheus-shaped read paths fold into `/metrics` like the SQL path
    /// does.
    pub cost_recorder: Arc<dyn QueryCostRecorder>,
    /// The fleet-global query concurrency ceiling (ADR-0061 decision 2). Every
    /// Prometheus-shaped handler acquires a [`crate::QueryPermit`] from this
    /// before doing any resolve or GET, and is rejected if admitting one more
    /// query would exceed this process's reconciled fleet threshold. Defaults to
    /// an [`QueryConcurrencyLimit::Unlimited`] controller in [`AppState::new`],
    /// so a caller that mounts the router without a ceiling (and every test)
    /// behaves exactly as before this mechanism existed; a deployment attaches
    /// the one shared controller with [`AppState::with_query_admission`].
    pub query_admission: Arc<QueryAdmissionController>,
    /// The evidential audit sink every query surface submits one
    /// [`AuditEvent`](ravel_maintain::AuditEvent) through before releasing its
    /// response (ADR-0062 §2a). Submission awaits the
    /// event's durability, so a completed handler's response is released only
    /// after its audit record is durable (or, in best-effort mode, after the
    /// pipeline decided to release it anyway). Defaults to
    /// [`NoopQueryAuditSink`] in [`AppState::new`], so a caller that mounts the
    /// router without an audit pipeline (and every test) runs unaudited exactly
    /// as before this seam existed; a deployment attaches the one shared
    /// pipeline with [`AppState::with_audit_sink`].
    pub audit_sink: Arc<dyn QueryAuditSink>,
    /// The per-process, per-tenant metric metadata cache backing
    /// `/api/v1/metadata` (ADR-0085 decision 1 read path). Defaults to `None` in
    /// [`AppState::new`]: when it is absent the endpoint keeps its pre-ADR
    /// behavior exactly (a `200` with an empty `data` object), so every existing
    /// caller and `ravel-server`'s [`AppState::new`] call site are unaffected. A
    /// deployment attaches a cache built from its object store with
    /// [`AppState::with_metadata_cache`], at which point the endpoint serves that
    /// process's cached view of each queried tenant's metadata.
    pub metadata_cache: Option<Arc<MetadataCache>>,
}

impl AppState {
    /// State with the given engine and resolver, a no-op cost recorder, and an
    /// unlimited (never-rejecting) query concurrency controller.
    pub fn new(engine: Arc<QueryEngine>, tenant_resolver: Arc<dyn TenantResolver>) -> Self {
        AppState {
            engine,
            tenant_resolver,
            cost_recorder: Arc::new(NoopQueryCostRecorder),
            query_admission: QueryAdmissionController::shared(QueryConcurrencyLimit::Unlimited),
            audit_sink: Arc::new(NoopQueryAuditSink),
            metadata_cache: None,
        }
    }

    /// Set the recorder every completed query folds its cost into. Returns
    /// `self` so it chains off [`AppState::new`].
    pub fn with_cost_recorder(mut self, cost_recorder: Arc<dyn QueryCostRecorder>) -> Self {
        self.cost_recorder = cost_recorder;
        self
    }

    /// Set the shared fleet-global query concurrency controller (ADR-0061
    /// decision 2). Returns `self` so it chains off [`AppState::new`]. A
    /// deployment passes the one controller instance shared with the SQL and
    /// Flight SQL surfaces, so the process holds a single honest in-flight count
    /// across every query transport.
    pub fn with_query_admission(mut self, query_admission: Arc<QueryAdmissionController>) -> Self {
        self.query_admission = query_admission;
        self
    }

    /// Set the evidential audit sink every query surface submits one event
    /// through and awaits durability on before responding (ADR-0062 §2a).
    /// Returns `self` so it chains off [`AppState::new`]. A deployment passes
    /// the one shared [`AuditPipeline`](ravel_maintain::AuditPipeline) instance
    /// so every read surface's audit trail lands through one seam.
    pub fn with_audit_sink(mut self, audit_sink: Arc<dyn QueryAuditSink>) -> Self {
        self.audit_sink = audit_sink;
        self
    }

    /// Attach the per-process metric metadata cache that backs
    /// `/api/v1/metadata` (ADR-0085 decision 1). Returns `self` so it chains off
    /// [`AppState::new`]. Without this the endpoint serves the pre-ADR empty
    /// object; with it, a request carrying a resolvable tenant credential gets
    /// that tenant's cached metadata, and a request with no resolvable tenant
    /// still gets the empty object (the endpoint never `401`s, ADR-0085 read
    /// path). A deployment builds the cache from the same object store the query
    /// engine reads and passes it here.
    pub fn with_metadata_cache(mut self, metadata_cache: Arc<MetadataCache>) -> Self {
        self.metadata_cache = Some(metadata_cache);
        self
    }
}

/// Builds the Prometheus-compatible query API router. The caller is
/// responsible for binding a listener, adding middleware (tracing,
/// compression, timeouts), and nesting this under whatever path prefix
/// their service uses.
pub fn router(state: AppState) -> Router {
    // The compatibility routes are now stateful: `/api/v1/metadata` reads
    // `AppState` to reach the metadata cache and the tenant resolver (ADR-0085
    // read path). `buildinfo` still ignores state. Build the compat router with
    // its own clone of `state`, merged so every service mounting this router
    // serves them without extra wiring of its own.
    let compat = compat::router(state.clone());
    Router::new()
        .route("/api/v1/query", get(handlers::query).post(handlers::query))
        .route(
            "/api/v1/query_range",
            get(handlers::query_range).post(handlers::query_range),
        )
        .route("/api/v1/labels", get(handlers::labels))
        .route("/api/v1/label/{name}/values", get(handlers::label_values))
        .route(
            "/api/v1/series",
            get(handlers::series).post(handlers::series),
        )
        .with_state(state)
        .merge(compat)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::too_many_arguments)]
mod tests {
    //! Endpoint-level reachability proof for native-histogram range results:
    //! a real RSEG v5 segment carrying histogram samples is published to a
    //! `MemoryStore`, queried through the production axum router via
    //! `tower::ServiceExt::oneshot`, and the rendered `histograms` field is
    //! asserted in full. A unit test on the encoder alone cannot show that a
    //! range request actually reaches it; this does.
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use axum::Router;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use ravel_catalog::{Catalog, CatalogConfig};
    use ravel_commit::publish::RetryPolicy;
    use ravel_commit::record::NewCommitRecord;
    use ravel_commit::{keys, publish, record};
    use ravel_object_store::ObjectStoreBackend;
    use ravel_object_store::memory::MemoryStore;
    use ravel_segment::{
        HistogramCounts, HistogramSample, HistogramSpan, HistogramValue, IngestBounds, ResetHint,
        SegmentIdentity, SegmentWriter, SeriesInputV3, SeriesValues, WrittenSegment,
    };
    use ravel_types::{Label, LabelSet, SeriesId, Signal, TenantId};
    use serde_json::Value as JsonValue;
    use tower::ServiceExt;
    use uuid::Uuid;

    use super::{AppState, StaticBearerTokenResolver, router};
    use crate::{EngineConfig, QueryEngine};

    const NS_PER_SEC: i64 = 1_000_000_000;
    const NS_PER_MIN: i64 = 60 * NS_PER_SEC;
    const NS_PER_HOUR: i64 = 60 * NS_PER_MIN;

    fn now_ns() -> i64 {
        let dur = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch");
        let secs = i64::try_from(dur.as_secs()).expect("seconds fit i64");
        secs * NS_PER_SEC
    }

    /// The native histogram both samples carry: total count 9 (zero_count 4,
    /// one positive bucket count 2, one negative bucket count 3), sum 4.5. With
    /// `scale = 0` the positive span at offset 1 is the bucket `(1, 2]`, the
    /// negative span at offset 1 is `(-2, -1]`, and `zero_threshold = 0.5`
    /// makes the zero bucket `(-0.5, 0.5]`. Matches the instant-path element in
    /// `json::tests::instant_vector_renders_native_histogram_element`, so both
    /// endpoints are proven to render the same shape.
    fn histogram_value() -> HistogramValue {
        HistogramValue {
            scale: 0,
            zero_threshold: 0.5,
            sum: Some(4.5),
            custom_values: None,
            positive_spans: vec![HistogramSpan {
                offset: 1,
                length: 1,
            }],
            negative_spans: vec![HistogramSpan {
                offset: 1,
                length: 1,
            }],
            counts: HistogramCounts::Int {
                zero_count: 4,
                count: 9,
                positive: vec![2],
                negative: vec![3],
            },
            reset_hint: ResetHint::Unknown,
        }
    }

    #[tokio::test]
    async fn range_query_endpoint_serves_native_histogram_matrices() {
        let store = Arc::new(MemoryStore::new());
        let tenant_id = TenantId::new("tenant-hist".to_string());
        let tenant_hash = tenant_id.hash();
        // Floor to a whole minute so every grid instant is a whole-second
        // (and whole-minute) value the step assertions can name exactly.
        let now = (now_ns() / NS_PER_MIN) * NS_PER_MIN;
        let hour_bucket = u32::try_from(now / NS_PER_HOUR).expect("hour bucket");

        let metric = "req_latency";
        let labels = LabelSet::new(vec![Label {
            name: "__name__".to_string(),
            value: metric.to_string(),
        }])
        .expect("valid labels");
        let series_id = SeriesId::compute(&tenant_id, metric, &labels).expect("series id");
        let series = vec![SeriesInputV3 {
            series_id,
            labels,
            values: SeriesValues::Histogram(vec![
                HistogramSample {
                    ts_ns: now - 4 * NS_PER_MIN,
                    value: histogram_value(),
                },
                HistogramSample {
                    ts_ns: now - 2 * NS_PER_MIN,
                    value: histogram_value(),
                },
            ]),
        }];

        let writer_id = Uuid::new_v4();
        let identity = SegmentIdentity {
            tenant_hash: tenant_hash.0,
            shard: 0,
            writer_id: writer_id.to_string(),
            writer_epoch: 1,
            writer_seq: 1,
        };
        let bounds = IngestBounds {
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 0,
        };
        let written: WrittenSegment =
            SegmentWriter::write_histograms(series, identity, bounds).expect("write segment");

        let new_record = NewCommitRecord {
            tenant_hash,
            signal: Signal::Metrics,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq: 1,
            object_size: written.bytes.len() as u64,
            content_hash: written.summary.blake3,
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            min_ingest_ts_ns: written.summary.min_event_ts_ns,
            max_ingest_ts_ns: written.summary.max_event_ts_ns,
            segment_format_version: 1,
            created_unix_ns: now,
            ingest_hour_bucket: hour_bucket,
        };
        let rec = record::build(new_record).expect("valid commit record");
        let data_key = keys::reconstruct_data_key(&rec).expect("data key");
        let backend: Arc<dyn ObjectStoreBackend> = store.clone();
        publish::put_data_object(backend.as_ref(), &data_key, written.bytes)
            .await
            .expect("put data object");
        publish::publish(backend.as_ref(), &rec, &RetryPolicy::default())
            .await
            .expect("publish");

        let mut tokens = HashMap::new();
        tokens.insert("secret-hist".to_string(), tenant_id.clone());
        let catalog =
            Arc::new(Catalog::new(backend.clone(), CatalogConfig::default()).expect("catalog"));
        let engine = Arc::new(QueryEngine::new(catalog, backend, EngineConfig::default()));
        let state = AppState::new(engine, Arc::new(StaticBearerTokenResolver::new(tokens)));
        let app: Router = router(state);

        let start = (now - 4 * NS_PER_MIN) / NS_PER_SEC;
        let end = (now - NS_PER_MIN) / NS_PER_SEC;
        let uri = format!("/api/v1/query_range?query={metric}&start={start}&end={end}&step=60s");
        let request = Request::builder()
            .method("GET")
            .uri(&uri)
            .header("authorization", "Bearer secret-hist")
            .body(Body::empty())
            .expect("build request");
        let response = match app.oneshot(request).await {
            Ok(response) => response,
            Err(never) => match never {},
        };
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let json: JsonValue = serde_json::from_slice(&body).expect("parse json");

        assert_eq!(status, StatusCode::OK, "body: {json}");
        assert_eq!(json["status"], "success", "body: {json}");
        assert_eq!(json["data"]["resultType"], "matrix");
        let results = json["data"]["result"]
            .as_array()
            .expect("matrix result array");
        assert_eq!(results.len(), 1, "one series, body: {json}");
        let elem = &results[0];
        assert_eq!(elem["metric"]["__name__"], "req_latency");
        // The float `values` array is absent on a histogram-only series.
        assert!(
            elem.get("values").is_none(),
            "no values field on a histogram-only series, body: {json}"
        );
        let hists = elem["histograms"]
            .as_array()
            .expect("histograms array present");
        // The two source samples (at -4min and -2min) carry across the whole
        // [-4min, -1min] grid by lookback, so every one of the four 60s steps
        // renders a histogram element.
        let expected_ts: Vec<i64> = (1..=4)
            .rev()
            .map(|k| (now - k * NS_PER_MIN) / NS_PER_SEC)
            .collect();
        assert_eq!(hists.len(), 4, "one histogram per grid step, body: {json}");
        // Every rendered step carries the exact shape the source samples encode:
        // its own grid timestamp, count/sum as strings, and the three buckets
        // in cumulative order.
        for (step, want_ts) in hists.iter().zip(expected_ts) {
            assert_eq!(
                step[0].as_i64().expect("integer step timestamp"),
                want_ts,
                "step timestamp, body: {json}"
            );
            assert_eq!(step[1]["count"], "9");
            assert_eq!(step[1]["sum"], "4.5");
            assert_eq!(
                step[1]["buckets"],
                serde_json::json!([
                    [1, "-2", "-1", "3"],
                    [3, "-0.5", "0.5", "4"],
                    [0, "1", "2", "2"],
                ]),
                "bucket contents, body: {json}"
            );
        }

        // Endpoint-level reachability of the per-phase cost split (issue #935):
        // the same real query through the production router carries a
        // `stats.phaseAccounting` array with exactly the four phases, in order,
        // each once. A json.rs unit test proves the encoder; this proves a
        // caller of `/api/v1/query_range` actually sees it.
        let phase_acc = json["data"]["stats"]["phaseAccounting"]
            .as_array()
            .expect("phaseAccounting array present in the endpoint response");
        let phase_names: Vec<&str> = phase_acc
            .iter()
            .map(|p| p["phase"].as_str().expect("phase name string"))
            .collect();
        assert_eq!(
            phase_names,
            vec!["resolve", "plan", "probe", "scan"],
            "four phases, in order, each exactly once, body: {json}"
        );
        // This query resolves a snapshot and scans one published segment, so the
        // resolve phase must have issued at least one catalog request and the
        // scan phase at least one GET; a pooled-only rendering could not show
        // this attribution. The exact per-phase figures depend on the generated
        // object's geometry, so they are pinned in the json.rs unit test on a
        // hand-built snapshot rather than here.
        let by_name = |want: &str| {
            phase_acc
                .iter()
                .find(|p| p["phase"] == want)
                .unwrap_or_else(|| panic!("phase {want} present, body: {json}"))
        };
        let resolve_requests = by_name("resolve")["s3GetRequests"]
            .as_u64()
            .expect("resolve get requests")
            + by_name("resolve")["s3ListRequests"]
                .as_u64()
                .expect("resolve list requests");
        assert!(
            resolve_requests >= 1,
            "resolve phase issued catalog requests, body: {json}"
        );
        assert!(
            by_name("scan")["s3GetRequests"]
                .as_u64()
                .expect("scan get requests")
                >= 1,
            "scan phase issued at least one data GET, body: {json}"
        );
    }
}
