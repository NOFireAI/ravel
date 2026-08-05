//! End-to-end tests: real RSEG segments, written with `ravel_segment`,
//! published with `ravel_commit` onto a `MemoryStore`, queried through the
//! full stack (catalog -> fetcher -> engine -> evaluator -> HTTP handlers)
//! via `tower::ServiceExt::oneshot` (docs/query-engine.md).
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::too_many_arguments)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use bytes::Bytes;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
    ObjectStoreBackend, PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_query::http::{AppState, StaticBearerTokenResolver, router};
use ravel_query::{EngineConfig, QueryEngine};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput, WrittenSegment};
use ravel_types::{CommitToken, Label, LabelSet, Sample, SeriesId, Signal, TenantHash, TenantId};
use serde_json::Value;
use tower::ServiceExt;
use uuid::Uuid;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_MIN: i64 = 60 * NS_PER_SEC;
const NS_PER_HOUR: i64 = 60 * NS_PER_MIN;

/// Current time, floored to a whole second so every derived sample
/// timestamp is an exact-second value (avoids float precision concerns
/// when round-tripping through the `time`/`start`/`end` query params).
fn now_ns() -> i64 {
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch");
    let ns = i64::try_from(dur.as_nanos()).expect("time overflow");
    (ns / NS_PER_SEC) * NS_PER_SEC
}

fn tenant(name: &str) -> TenantId {
    TenantId::new(name.to_string())
}

fn make_labels(metric: &str, extra: &[(&str, &str)]) -> LabelSet {
    let mut pairs = vec![Label {
        name: "__name__".to_string(),
        value: metric.to_string(),
    }];
    for (k, v) in extra {
        pairs.push(Label {
            name: (*k).to_string(),
            value: (*v).to_string(),
        });
    }
    LabelSet::new(pairs).expect("valid labels")
}

fn series_input(
    tenant_id: &TenantId,
    metric: &str,
    extra: &[(&str, &str)],
    samples: &[(i64, f64)],
) -> SeriesInput {
    let label_set = make_labels(metric, extra);
    let series_id = SeriesId::compute(tenant_id, metric, &label_set).expect("series id");
    SeriesInput {
        series_id,
        labels: label_set,
        samples: samples
            .iter()
            .map(|(ts_ns, value)| Sample {
                ts_ns: *ts_ns,
                value: *value,
            })
            .collect(),
    }
}

/// Writes a real RSEG segment and publishes its commit record, using
/// `footer_epoch` for the on-disk segment identity and `record_epoch` for
/// the published commit record's identity. Equal for every normal-path
/// test; deliberately different for the identity-mismatch test.
async fn publish_segment(
    store: &MemoryStore,
    tenant_hash: TenantHash,
    shard: u32,
    writer_id: Uuid,
    footer_epoch: u64,
    record_epoch: u64,
    writer_seq: u64,
    ingest_hour_bucket: u32,
    created_unix_ns: i64,
    series: Vec<SeriesInput>,
) -> (CommitToken, String) {
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard,
        writer_id: writer_id.to_string(),
        writer_epoch: footer_epoch,
        writer_seq,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    };
    let written: WrittenSegment =
        SegmentWriter::write(series, identity, bounds).expect("write segment");

    let new_record = NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard,
        writer_id,
        writer_epoch: record_epoch,
        writer_seq,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: 1,
        created_unix_ns,
        ingest_hour_bucket,
    };
    let rec = record::build(new_record).expect("valid commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    publish::put_data_object(store, &data_key, written.bytes)
        .await
        .expect("put data object");
    let token = publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
    (token, data_key)
}

/// Percent-encodes everything except unreserved characters, so arbitrary
/// PromQL text (braces, quotes, `=`) survives as a URI query component and
/// round-trips through the crate's own hand-rolled decoder.
fn encode_query_param(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn build_app(
    store: Arc<MemoryStore>,
    catalog_config: CatalogConfig,
    engine_config: EngineConfig,
    tokens: HashMap<String, TenantId>,
) -> Router {
    let backend: Arc<dyn ObjectStoreBackend> = store;
    let catalog = Arc::new(Catalog::new(backend.clone(), catalog_config).expect("catalog"));
    let engine = Arc::new(QueryEngine::new(catalog, backend, engine_config));
    let state = AppState::new(engine, Arc::new(StaticBearerTokenResolver::new(tokens)));
    router(state)
}

fn one_tenant_app(
    store: Arc<MemoryStore>,
    engine_config: EngineConfig,
    tenant_id: &TenantId,
    token: &str,
) -> Router {
    let mut tokens = HashMap::new();
    tokens.insert(token.to_string(), tenant_id.clone());
    build_app(store, CatalogConfig::default(), engine_config, tokens)
}

async fn call(app: &Router, uri: &str, auth: Option<&str>) -> (StatusCode, Value) {
    let mut builder = Request::builder().method("GET").uri(uri);
    if let Some(token) = auth {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    let request = builder.body(Body::empty()).expect("build request");
    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("oneshot is infallible");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let json: Value = serde_json::from_slice(&body).expect("parse response json");
    (status, json)
}

fn vector_results(body: &Value) -> &Vec<Value> {
    body["data"]["result"]
        .as_array()
        .expect("vector result array")
}

fn value_string(sample: &Value) -> &str {
    sample["value"][1].as_str().expect("sample value string")
}

#[tokio::test]
async fn instant_query_returns_expected_value() {
    let store = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let th = tid.hash();
    let now = now_ns();
    let hour_bucket = u32::try_from(now / NS_PER_HOUR).expect("hour bucket");

    let series = vec![
        series_input(
            &tid,
            "http_requests_total",
            &[("method", "get")],
            &[(now - NS_PER_MIN, 42.0)],
        ),
        series_input(
            &tid,
            "http_requests_total",
            &[("method", "post")],
            &[(now - NS_PER_MIN, 7.0)],
        ),
    ];
    publish_segment(
        &store,
        th,
        0,
        Uuid::new_v4(),
        1,
        1,
        1,
        hour_bucket,
        now,
        series,
    )
    .await;

    let app = one_tenant_app(store, EngineConfig::default(), &tid, "secret-a");
    let query = encode_query_param("http_requests_total{method=\"get\"}");
    let uri = format!(
        "/api/v1/query?query={query}&time={}",
        (now - NS_PER_MIN) / NS_PER_SEC
    );
    let (status, body) = call(&app, &uri, Some("secret-a")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "success");
    let results = vector_results(&body);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["metric"]["method"], "get");
    assert_eq!(value_string(&results[0]), "42");
}

/// One span's captured identity and per-span count fields (ADR-0044 decision
/// 5). `name` and `tenant_hash` are read from the span's creation attributes;
/// the count fields are `None` until the phase records them via `span.record`
/// and are captured in [`SpanCollector::on_record`].
#[derive(Clone, Default, Debug)]
struct CapturedSpan {
    name: String,
    tenant_hash: Option<String>,
    s3_requests: Option<u64>,
    s3_bytes: Option<u64>,
    decompressed_bytes: Option<u64>,
}

/// Visits a span's fields, capturing only the ones the phase-span assertions
/// care about. The count fields arrive as `u64`; `tenant_hash` is a `Display`
/// value, which `tracing` routes through `record_debug` as the formatted
/// `format_args!` (its `Debug` renders the hex string with no quotes).
struct SpanFieldVisitor<'a>(&'a mut CapturedSpan);

impl tracing::field::Visit for SpanFieldVisitor<'_> {
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        match field.name() {
            "s3_requests" => self.0.s3_requests = Some(value),
            "s3_bytes" => self.0.s3_bytes = Some(value),
            "decompressed_bytes" => self.0.decompressed_bytes = Some(value),
            _ => {}
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "tenant_hash" {
            self.0.tenant_hash = Some(format!("{value:?}"));
        }
    }
}

/// A `tracing` layer that records the name of every span opened while it is the
/// default subscriber, plus the per-span count fields each phase records, so a
/// test can assert both which query-path phase spans fired and that their
/// recorded counts hold plausible values (ADR-0044 decision 5, issue #642).
///
/// `names` keeps every span name for exact-name presence assertions (a
/// substring check would let `catalog_decode` satisfy an assertion for
/// `decode`). `live` tracks each open span's captured fields by id while it is
/// recording; on close the entry moves into `completed`, so a later span that
/// reuses the freed id cannot overwrite a finished span's captured counts. A
/// quantitative test filters `completed` by span name and `tenant_hash` to
/// isolate its own query's spans from sibling tests sharing this one global
/// collector.
#[derive(Clone, Default)]
struct SpanCollector {
    names: Arc<std::sync::Mutex<Vec<String>>>,
    live: Arc<std::sync::Mutex<HashMap<u64, CapturedSpan>>>,
    completed: Arc<std::sync::Mutex<Vec<CapturedSpan>>>,
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for SpanCollector {
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut captured = CapturedSpan {
            name: attrs.metadata().name().to_string(),
            ..CapturedSpan::default()
        };
        attrs.record(&mut SpanFieldVisitor(&mut captured));
        if let Ok(mut names) = self.names.lock() {
            names.push(captured.name.clone());
        }
        if let Ok(mut live) = self.live.lock() {
            live.insert(id.into_u64(), captured);
        }
    }

    fn on_record(
        &self,
        id: &tracing::span::Id,
        values: &tracing::span::Record<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if let Ok(mut live) = self.live.lock()
            && let Some(captured) = live.get_mut(&id.into_u64())
        {
            values.record(&mut SpanFieldVisitor(captured));
        }
    }

    fn on_close(&self, id: tracing::span::Id, _ctx: tracing_subscriber::layer::Context<'_, S>) {
        let taken = self
            .live
            .lock()
            .ok()
            .and_then(|mut live| live.remove(&id.into_u64()));
        if let (Some(captured), Ok(mut completed)) = (taken, self.completed.lock()) {
            completed.push(captured);
        }
    }
}

/// The collector installed as this test binary's global tracing subscriber.
static SPAN_COLLECTOR: std::sync::OnceLock<SpanCollector> = std::sync::OnceLock::new();

/// Installs a collecting subscriber as the process-global default exactly once
/// for this test binary, returning its collector.
///
/// A thread-local `set_default` is not enough here: the other tests in this
/// binary exercise the same query path first, under the default `NoSubscriber`,
/// which both caches each phase span's callsite interest as "never" and leaves
/// the process-global DEBUG max-level gate closed. A scoped default reopens
/// neither deterministically once a sibling has run. `set_global_default`
/// rebuilds the interest cache and the max-level gate against this subscriber,
/// so the phase spans are enabled regardless of test ordering or parallelism.
fn install_span_collector() -> SpanCollector {
    use tracing_subscriber::layer::SubscriberExt;
    SPAN_COLLECTOR
        .get_or_init(|| {
            let collector = SpanCollector::default();
            let subscriber = tracing_subscriber::registry().with(collector.clone());
            tracing::subscriber::set_global_default(subscriber)
                .expect("no other global tracing subscriber in this test binary");
            collector
        })
        .clone()
}

/// ADR-0044 decision 5 (issue #642): a real instant query must open all six
/// query-path phase spans. This asserts each of `catalog_resolve`,
/// `segment_open`, `catalog_decode`, `page_fetch`, `decode`, and `evaluate`
/// fires at least once. Removing any one of the six span attributes makes this
/// fail (the missing name is never collected).
#[tokio::test]
async fn instant_query_emits_all_six_phase_spans() {
    // Installed before the query runs, so its spans dispatch to the collector.
    // Sibling tests may add their own names to the same shared collector; this
    // only asserts presence, never absence, so their concurrent writes are
    // harmless.
    let collector = install_span_collector();

    let store = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let th = tid.hash();
    let now = now_ns();
    let hour_bucket = u32::try_from(now / NS_PER_HOUR).expect("hour bucket");

    let series = vec![series_input(
        &tid,
        "http_requests_total",
        &[("method", "get")],
        &[(now - NS_PER_MIN, 42.0)],
    )];
    publish_segment(
        &store,
        th,
        0,
        Uuid::new_v4(),
        1,
        1,
        1,
        hour_bucket,
        now,
        series,
    )
    .await;

    let app = one_tenant_app(store, EngineConfig::default(), &tid, "secret-a");
    let query = encode_query_param("http_requests_total{method=\"get\"}");
    let uri = format!(
        "/api/v1/query?query={query}&time={}",
        (now - NS_PER_MIN) / NS_PER_SEC
    );

    let (status, body) = call(&app, &uri, Some("secret-a")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "success");

    let names = collector.names.lock().expect("span-name lock").clone();
    for span_name in [
        "catalog_resolve",
        "segment_open",
        "catalog_decode",
        "page_fetch",
        "decode",
        "evaluate",
    ] {
        assert!(
            names.iter().any(|n| n == span_name),
            "expected the `{span_name}` phase span to fire on an instant query; captured spans: {names:?}"
        );
    }
}

/// A store wrapper that sleeps before every `get`, delegating everything else
/// to the inner store unchanged. Used only by
/// [`segment_open_span_bytes_are_per_segment_not_the_shared_query_total`] to
/// widen each segment's GET window so several segments' fetches reliably
/// overlap. That overlap is what distinguishes the fixed per-span accounting
/// (each `segment_open` records only its own segment's GET bytes) from the old
/// shared-`QueryAccounting` delta (which, while one segment's GET is in flight,
/// folds sibling segments' concurrent GETs into that segment's span).
struct DelayStore {
    inner: Arc<dyn ObjectStoreBackend>,
    get_delay: std::time::Duration,
}

#[async_trait::async_trait]
impl ObjectStoreBackend for DelayStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        tokio::time::sleep(self.get_delay).await;
        self.inner.get(key, range).await
    }

    async fn put_multipart<'a>(
        &'a self,
        key: &str,
    ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
        self.inner.put_multipart(key).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
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

/// One tenant, an arbitrary backing store, and a capturing cost recorder. The
/// `store` seam (over the plain `MemoryStore` of [`one_tenant_app_with_recorder`])
/// lets a test interpose a [`DelayStore`].
fn one_tenant_app_with_recorder_and_store(
    backend: Arc<dyn ObjectStoreBackend>,
    engine_config: EngineConfig,
    tenant_id: &TenantId,
    token: &str,
    recorder: Arc<CapturingRecorder>,
) -> Router {
    let catalog =
        Arc::new(Catalog::new(backend.clone(), CatalogConfig::default()).expect("catalog"));
    let engine = Arc::new(QueryEngine::new(catalog, backend, engine_config));
    let mut tokens = HashMap::new();
    tokens.insert(token.to_string(), tenant_id.clone());
    let state = AppState::new(engine, Arc::new(StaticBearerTokenResolver::new(tokens)))
        .with_cost_recorder(recorder);
    router(state)
}

/// ADR-0044 decision 5 (issue #642), the count-value regression F1 was missing:
/// each `segment_open` span's recorded `s3_bytes` must reflect only that
/// segment's own GET, not a delta off the shared `QueryAccounting` handle that
/// concurrent sibling segments also write. The invariant asserted is cheap and
/// one the buggy delta version violates under concurrency: the sum of every
/// `segment_open` span's `s3_bytes` cannot exceed the query's own total S3 GET
/// bytes, because each segment's bytes are counted once. The old delta double-
/// counted -- while segment A's GET was in flight, segments B/C bumped the
/// shared counter, so A's "after - before" delta swept in B's and C's bytes,
/// and summed across all three spans blew well past the true total.
///
/// Three segments (not one) and a [`DelayStore`] are both load-bearing: one
/// segment has no sibling to race, and without the GET delay the in-memory
/// fetches may not overlap, so neither would distinguish the bug from the fix.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn segment_open_span_bytes_are_per_segment_not_the_shared_query_total() {
    let collector = install_span_collector();

    let store = Arc::new(MemoryStore::new());
    // A tenant unique to this test, so its `segment_open` spans can be told
    // apart from sibling tests' spans in the shared global collector by the
    // `tenant_hash` field every `segment_open` span carries.
    let tid = tenant("tenant-segment-open-bytes");
    let th = tid.hash();
    let now = now_ns();
    let hour_bucket = u32::try_from(now / NS_PER_HOUR).expect("hour bucket");

    // Three separate segments, each one series, all within the instant query's
    // lookback. The engine opens all three concurrently.
    for (i, method) in ["get", "post", "put"].iter().enumerate() {
        let series = vec![series_input(
            &tid,
            "http_requests_total",
            &[("method", method)],
            &[(now - NS_PER_MIN, 42.0 + i as f64)],
        )];
        publish_segment(
            &store,
            th,
            0,
            Uuid::new_v4(),
            1,
            1,
            1 + i as u64,
            hour_bucket,
            now,
            series,
        )
        .await;
    }

    let recorder = Arc::new(CapturingRecorder::default());
    let backend: Arc<dyn ObjectStoreBackend> = Arc::new(DelayStore {
        inner: store,
        get_delay: std::time::Duration::from_millis(5),
    });
    let app = one_tenant_app_with_recorder_and_store(
        backend,
        EngineConfig::default(),
        &tid,
        "secret-a",
        Arc::clone(&recorder),
    );
    let query = encode_query_param("http_requests_total");
    let uri = format!(
        "/api/v1/query?query={query}&time={}",
        (now - NS_PER_MIN) / NS_PER_SEC
    );
    let (status, body) = call(&app, &uri, Some("secret-a")).await;
    assert_eq!(status, StatusCode::OK, "query failed: {body}");
    assert_eq!(vector_results(&body).len(), 3, "three series expected");

    // The query's own authoritative total, from the same accounting handle the
    // response body and `/metrics` are fed from.
    let query_total_s3_bytes = {
        let calls = recorder.calls.lock().expect("recorder mutex");
        assert_eq!(calls.len(), 1, "exactly one query recorded");
        calls[0].0.total_s3_bytes()
    };
    assert!(query_total_s3_bytes > 0, "the query fetched real bytes");

    // This query's `segment_open` spans, isolated from any sibling test's by
    // tenant. Read from `completed` so spans closed before this point are
    // preserved even if their ids were reused.
    let want_tenant = th.to_hex();
    let segment_open_bytes: Vec<u64> = {
        let completed = collector.completed.lock().expect("completed lock");
        completed
            .iter()
            .filter(|s| s.name == "segment_open")
            .filter(|s| {
                s.tenant_hash
                    .as_deref()
                    .is_some_and(|t| t.contains(&want_tenant))
            })
            .map(|s| s.s3_bytes.expect("segment_open must record s3_bytes"))
            .collect()
    };

    assert!(
        segment_open_bytes.len() >= 2,
        "expected at least two segment_open spans for this query's tenant, got {}: {segment_open_bytes:?}",
        segment_open_bytes.len()
    );
    for bytes in &segment_open_bytes {
        assert!(
            *bytes > 0,
            "each segment_open span records its own non-zero GET bytes; got {segment_open_bytes:?}"
        );
    }
    let sum: u64 = segment_open_bytes.iter().sum();
    assert!(
        sum <= query_total_s3_bytes,
        "sum of per-segment segment_open s3_bytes ({sum}) must not exceed the query's \
         own total s3 GET bytes ({query_total_s3_bytes}); a value above the total means a \
         span counted bytes belonging to a concurrent sibling segment (ADR-0044 decision 5)"
    );
}

/// One tenant, a capturing recorder, and an ADR-0046 read cache wired into the
/// engine's fetcher exactly as `services/ravel-server/src/query.rs` wires it
/// (`QueryEngine::with_cache` -> `SegmentFetcher::with_cache`). The returned
/// `Router` holds the engine, so the cache persists across every `call` made
/// against it: a first (cold) query populates it, a second (warm) query serves
/// from it.
fn one_tenant_app_with_recorder_and_cache(
    store: Arc<MemoryStore>,
    engine_config: EngineConfig,
    tenant_id: &TenantId,
    token: &str,
    recorder: Arc<CapturingRecorder>,
    cache: Arc<ravel_cache::Cache<ravel_query::CacheFetchError>>,
) -> Router {
    let backend: Arc<dyn ObjectStoreBackend> = store;
    let catalog =
        Arc::new(Catalog::new(backend.clone(), CatalogConfig::default()).expect("catalog"));
    let engine = Arc::new(QueryEngine::new(catalog, backend, engine_config).with_cache(cache));
    let mut tokens = HashMap::new();
    tokens.insert(token.to_string(), tenant_id.clone());
    let state = AppState::new(engine, Arc::new(StaticBearerTokenResolver::new(tokens)))
        .with_cost_recorder(recorder);
    router(state)
}

/// ADR-0044 decision 5 (issue #642), the cache-attribution regression the F1
/// fix's per-span counting introduced: `open_segment`/`ensure_ranges` must count
/// only *store-sourced* bytes on their `segment_open`/`page_fetch` spans, not
/// bytes served from the ADR-0046 read cache. F1's local counters incremented
/// unconditionally for every `guarded_get` that returned, but a cache hit
/// returns bytes with no store round trip at all, so a fully warm query -- which
/// makes zero real S3 GETs -- still reported the segment's whole object size on
/// its `segment_open` span.
///
/// The prior phase-attribution tests configure no cache (the default
/// `SegmentFetcher::new` path), which is why the bug slipped past them. This one
/// wires a cache in and runs the same query twice: cold to populate it, warm to
/// serve from it. On the warm run the query's own `total_s3_bytes` is zero (all
/// hits), and every `segment_open`/`page_fetch` span's recorded `s3_bytes` must
/// sum to that same zero -- not to the segment's real object size. Against the
/// pre-fix code the warm `segment_open` span reports the whole object size while
/// the query total is zero, so `warm_span_bytes_sum <= warm_total` fails.
#[tokio::test]
async fn warm_cache_query_records_zero_store_bytes_on_fetch_spans() {
    let collector = install_span_collector();

    let store = Arc::new(MemoryStore::new());
    // A tenant unique to this test so its spans are separable in the shared
    // global collector, and so this test's warm-run spans can be sliced off the
    // tenant-filtered `completed` list after the cold run's spans.
    let tid = tenant("tenant-warm-cache-zero-store-bytes");
    let th = tid.hash();
    let now = now_ns();
    let hour_bucket = u32::try_from(now / NS_PER_HOUR).expect("hour bucket");

    // One small segment (one series). Small enough that `open_segment` takes the
    // whole-object `GetRange::Full` path, which -- unlike a suffix GET -- is
    // cache-eligible, so a warm run serves the entire segment from cache with no
    // store GET whatsoever.
    let series = vec![series_input(
        &tid,
        "http_requests_total",
        &[("method", "get")],
        &[(now - NS_PER_MIN, 42.0)],
    )];
    publish_segment(
        &store,
        th,
        0,
        Uuid::new_v4(),
        1,
        1,
        1,
        hour_bucket,
        now,
        series,
    )
    .await;

    let recorder = Arc::new(CapturingRecorder::default());
    // Generous limits: admit the whole segment as one entry and keep it resident
    // between the two queries.
    let cache = Arc::new(ravel_cache::Cache::new(ravel_cache::CacheLimits::new(
        64 * 1024 * 1024,
        1024,
        16 * 1024 * 1024,
    )));
    let app = one_tenant_app_with_recorder_and_cache(
        store,
        EngineConfig::default(),
        &tid,
        "secret-warm",
        Arc::clone(&recorder),
        cache,
    );
    let query = encode_query_param("http_requests_total");
    let uri = format!(
        "/api/v1/query?query={query}&time={}",
        (now - NS_PER_MIN) / NS_PER_SEC
    );

    // Cold run: populates the cache. Must make real S3 GETs.
    let (status, body) = call(&app, &uri, Some("secret-warm")).await;
    assert_eq!(status, StatusCode::OK, "cold query failed: {body}");
    assert_eq!(vector_results(&body).len(), 1, "one series expected (cold)");

    let want_tenant = th.to_hex();
    let cold_total_s3_bytes = {
        let calls = recorder.calls.lock().expect("recorder mutex");
        assert_eq!(calls.len(), 1, "exactly one query recorded after cold run");
        calls[0].0.total_s3_bytes()
    };
    assert!(
        cold_total_s3_bytes > 0,
        "the cold query must fetch real bytes from the store"
    );
    // The count of this tenant's completed spans after the cold run: the warm
    // run's spans are everything this tenant pushes past this point.
    let cold_span_count = {
        let completed = collector.completed.lock().expect("completed lock");
        completed
            .iter()
            .filter(|s| s.name == "segment_open" || s.name == "page_fetch")
            .filter(|s| {
                s.tenant_hash
                    .as_deref()
                    .is_some_and(|t| t.contains(&want_tenant))
            })
            .count()
    };

    // Warm run: identical query, now fully served from the populated cache.
    let (status, body) = call(&app, &uri, Some("secret-warm")).await;
    assert_eq!(status, StatusCode::OK, "warm query failed: {body}");
    assert_eq!(vector_results(&body).len(), 1, "one series expected (warm)");

    let warm_total_s3_bytes = {
        let calls = recorder.calls.lock().expect("recorder mutex");
        assert_eq!(
            calls.len(),
            2,
            "exactly two queries recorded after warm run"
        );
        calls[1].0.total_s3_bytes()
    };
    assert_eq!(
        warm_total_s3_bytes, 0,
        "the warm query is fully cache-served: it must make zero store GET bytes"
    );

    // This warm run's fetch spans: the tenant-filtered fetch spans past the cold
    // run's count. Their recorded store-byte counts must sum to the warm query's
    // own zero total -- not to the segment's real object size, which is what the
    // pre-fix unconditional per-`guarded_get` increment recorded.
    let warm_span_bytes: Vec<(String, u64)> = {
        let completed = collector.completed.lock().expect("completed lock");
        completed
            .iter()
            .filter(|s| s.name == "segment_open" || s.name == "page_fetch")
            .filter(|s| {
                s.tenant_hash
                    .as_deref()
                    .is_some_and(|t| t.contains(&want_tenant))
            })
            .skip(cold_span_count)
            .map(|s| {
                (
                    s.name.clone(),
                    s.s3_bytes.expect("fetch span must record s3_bytes"),
                )
            })
            .collect()
    };
    assert!(
        warm_span_bytes.iter().any(|(n, _)| n == "segment_open"),
        "expected a warm-run segment_open span for this tenant; got {warm_span_bytes:?}"
    );
    for (name, bytes) in &warm_span_bytes {
        assert_eq!(
            *bytes, 0,
            "warm-run {name} span served entirely from cache must record zero store bytes, \
             got {bytes} (a non-zero value means it counted cache-served bytes as S3 traffic)"
        );
    }
    let warm_span_sum: u64 = warm_span_bytes.iter().map(|(_, b)| *b).sum();
    assert!(
        warm_span_sum <= warm_total_s3_bytes,
        "sum of warm-run fetch-span s3_bytes ({warm_span_sum}) must not exceed the warm query's \
         own total store GET bytes ({warm_total_s3_bytes}); a value above it means a span counted \
         cache-served bytes as S3 traffic (ADR-0044 decision 5)"
    );
}

#[tokio::test]
async fn range_query_returns_expected_grid() {
    let store = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let th = tid.hash();
    let now = now_ns();
    let hour_bucket = u32::try_from(now / NS_PER_HOUR).expect("hour bucket");

    let series = vec![series_input(
        &tid,
        "queue_depth",
        &[],
        &[(now - 4 * NS_PER_MIN, 10.0), (now - 2 * NS_PER_MIN, 20.0)],
    )];
    publish_segment(
        &store,
        th,
        0,
        Uuid::new_v4(),
        1,
        1,
        1,
        hour_bucket,
        now,
        series,
    )
    .await;

    let app = one_tenant_app(store, EngineConfig::default(), &tid, "secret-a");
    let query = encode_query_param("queue_depth");
    let start = (now - 4 * NS_PER_MIN) / NS_PER_SEC;
    let end = (now - NS_PER_MIN) / NS_PER_SEC;
    let uri = format!("/api/v1/query_range?query={query}&start={start}&end={end}&step=60s");
    let (status, body) = call(&app, &uri, Some("secret-a")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "success");
    let results = body["data"]["result"]
        .as_array()
        .expect("matrix result array");
    assert_eq!(results.len(), 1);
    let values = results[0]["values"].as_array().expect("values array");
    let observed: Vec<&str> = values
        .iter()
        .map(|v| v[1].as_str().expect("value string"))
        .collect();
    assert_eq!(observed, vec!["10", "10", "20", "20"]);
}

#[tokio::test]
async fn lookback_finds_sample_in_an_earlier_segment() {
    let store = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let th = tid.hash();
    let now = now_ns();
    let hour_bucket = u32::try_from(now / NS_PER_HOUR).expect("hour bucket");

    // Segment A (earlier flush): the only sample at or before the query
    // instant.
    let seg_a = vec![series_input(
        &tid,
        "cpu_seconds",
        &[],
        &[(now - 4 * NS_PER_MIN, 1.0)],
    )];
    publish_segment(
        &store,
        th,
        0,
        Uuid::new_v4(),
        1,
        1,
        1,
        hour_bucket,
        now,
        seg_a,
    )
    .await;

    // Segment B (a later, separate flush): its sample is after the query
    // instant, so lookback must not pick it.
    let seg_b = vec![series_input(
        &tid,
        "cpu_seconds",
        &[],
        &[(now - 30 * NS_PER_SEC, 99.0)],
    )];
    publish_segment(
        &store,
        th,
        0,
        Uuid::new_v4(),
        1,
        1,
        1,
        hour_bucket,
        now,
        seg_b,
    )
    .await;

    let app = one_tenant_app(store, EngineConfig::default(), &tid, "secret-a");
    let query = encode_query_param("cpu_seconds");
    let uri = format!(
        "/api/v1/query?query={query}&time={}",
        (now - 2 * NS_PER_MIN) / NS_PER_SEC
    );
    let (status, body) = call(&app, &uri, Some("secret-a")).await;

    assert_eq!(status, StatusCode::OK);
    let results = vector_results(&body);
    assert_eq!(results.len(), 1);
    assert_eq!(value_string(&results[0]), "1");
}

#[tokio::test]
async fn cross_segment_duplicate_sample_resolves_to_greatest_writer_seq() {
    let store = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let th = tid.hash();
    let now = now_ns();
    let hour_bucket = u32::try_from(now / NS_PER_HOUR).expect("hour bucket");
    let writer_id = Uuid::new_v4();
    let ts = now - NS_PER_MIN;

    // Same series, same timestamp, same writer/epoch/created_unix_ns:
    // only writer_seq differs, so it is the deciding tiebreak
    // (ADR-0010 SS5 total order).
    let older = vec![series_input(&tid, "duplicated_metric", &[], &[(ts, 100.0)])];
    publish_segment(&store, th, 0, writer_id, 1, 1, 1, hour_bucket, now, older).await;
    let newer = vec![series_input(&tid, "duplicated_metric", &[], &[(ts, 200.0)])];
    publish_segment(&store, th, 0, writer_id, 1, 1, 2, hour_bucket, now, newer).await;

    let app = one_tenant_app(store, EngineConfig::default(), &tid, "secret-a");
    let query = encode_query_param("duplicated_metric");
    let uri = format!("/api/v1/query?query={query}&time={}", ts / NS_PER_SEC);
    let (status, body) = call(&app, &uri, Some("secret-a")).await;

    assert_eq!(status, StatusCode::OK);
    let results = vector_results(&body);
    assert_eq!(results.len(), 1);
    assert_eq!(value_string(&results[0]), "200");
}

#[tokio::test]
async fn min_commit_token_finds_segment_outside_the_listing_window() {
    let store = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let th = tid.hash();
    let now = now_ns();
    let ts = now - NS_PER_MIN;

    // ingest_hour_bucket 0 (1970-01-01T00) is nowhere near the catalog's
    // real-time-based listing window, so ordinary listing will never find
    // this commit record; only an explicit min_commit_token GET can.
    let (token, _) = publish_segment(
        &store,
        th,
        0,
        Uuid::new_v4(),
        1,
        1,
        1,
        0,
        now,
        vec![series_input(&tid, "delayed_metric", &[], &[(ts, 55.0)])],
    )
    .await;

    let app = one_tenant_app(store, EngineConfig::default(), &tid, "secret-a");
    let query = encode_query_param("delayed_metric");

    let without_token = format!("/api/v1/query?query={query}&time={}", ts / NS_PER_SEC);
    let (status, body) = call(&app, &without_token, Some("secret-a")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        vector_results(&body).len(),
        0,
        "must not be visible without the token"
    );

    let with_token = format!(
        "/api/v1/query?query={query}&time={}&min_commit_token={}",
        ts / NS_PER_SEC,
        token.encode()
    );
    let (status, body) = call(&app, &with_token, Some("secret-a")).await;
    assert_eq!(status, StatusCode::OK);
    let results = vector_results(&body);
    assert_eq!(
        results.len(),
        1,
        "must be visible with the read-your-write token"
    );
    assert_eq!(value_string(&results[0]), "55");
}

#[tokio::test]
async fn exceeding_max_series_budget_returns_422() {
    let store = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let th = tid.hash();
    let now = now_ns();
    let hour_bucket = u32::try_from(now / NS_PER_HOUR).expect("hour bucket");

    let series = vec![
        series_input(&tid, "up", &[("instance", "a")], &[(now - NS_PER_MIN, 1.0)]),
        series_input(&tid, "up", &[("instance", "b")], &[(now - NS_PER_MIN, 1.0)]),
    ];
    publish_segment(
        &store,
        th,
        0,
        Uuid::new_v4(),
        1,
        1,
        1,
        hour_bucket,
        now,
        series,
    )
    .await;

    let config = EngineConfig {
        max_series: 1,
        ..EngineConfig::default()
    };
    let app = one_tenant_app(store, config, &tid, "secret-a");
    let query = encode_query_param("up");
    let uri = format!(
        "/api/v1/query?query={query}&time={}",
        (now - NS_PER_MIN) / NS_PER_SEC
    );
    let (status, body) = call(&app, &uri, Some("secret-a")).await;

    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["status"], "error");
    assert_eq!(body["errorType"], "execution");
}

#[tokio::test]
async fn identity_mismatch_between_commit_record_and_footer_returns_500() {
    let store = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let th = tid.hash();
    let now = now_ns();
    let hour_bucket = u32::try_from(now / NS_PER_HOUR).expect("hour bucket");

    let series = vec![series_input(
        &tid,
        "mismatched_metric",
        &[],
        &[(now - NS_PER_MIN, 1.0)],
    )];
    // Footer declares writer_epoch 7; the published commit record declares
    // writer_epoch 9. The fetcher must detect this and fail hard rather
    // than return wrong data.
    publish_segment(
        &store,
        th,
        0,
        Uuid::new_v4(),
        7,
        9,
        1,
        hour_bucket,
        now,
        series,
    )
    .await;

    let app = one_tenant_app(store, EngineConfig::default(), &tid, "secret-a");
    let query = encode_query_param("mismatched_metric");
    let uri = format!(
        "/api/v1/query?query={query}&time={}",
        (now - NS_PER_MIN) / NS_PER_SEC
    );
    let (status, body) = call(&app, &uri, Some("secret-a")).await;

    // Identity mismatch is a permanent data-integrity fault, not a transient
    // outage: it maps to the non-retryable 500 `internal`, not 503 (a7-F05,
    // #62). A retry re-reads the same mismatched objects and never clears.
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(body["status"], "error");
    assert_eq!(body["errorType"], "internal");
}

#[tokio::test]
async fn corrupt_segment_bytes_return_500_not_wrong_data() {
    let store = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let th = tid.hash();
    let now = now_ns();
    let hour_bucket = u32::try_from(now / NS_PER_HOUR).expect("hour bucket");

    let series = vec![series_input(
        &tid,
        "corrupt_metric",
        &[],
        &[(now - NS_PER_MIN, 1.0)],
    )];
    let (_, data_key) = publish_segment(
        &store,
        th,
        0,
        Uuid::new_v4(),
        1,
        1,
        1,
        hour_bucket,
        now,
        series,
    )
    .await;

    // Flip the last byte of the object: the trailer's final 4 bytes are
    // the RSEG magic, so this deterministically fails footer parsing
    // (`SegmentError::BadMagic`) regardless of internal layout details.
    let existing = store
        .get(&data_key, GetRange::Full)
        .await
        .expect("get object");
    let mut corrupted = existing.data.to_vec();
    let last = corrupted.len() - 1;
    corrupted[last] ^= 0xFF;
    store
        .put(&data_key, Bytes::from(corrupted), PutOptions::default())
        .await
        .expect("overwrite object");

    let app = one_tenant_app(store, EngineConfig::default(), &tid, "secret-a");
    let query = encode_query_param("corrupt_metric");
    let uri = format!(
        "/api/v1/query?query={query}&time={}",
        (now - NS_PER_MIN) / NS_PER_SEC
    );
    let (status, body) = call(&app, &uri, Some("secret-a")).await;

    // Corrupt stored bytes are a permanent server-side data fault: the query
    // fails closed (never wrong data) with the non-retryable 500 `internal`,
    // not the retryable 503 (a7-F05, #62).
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(body["status"], "error");
    assert_eq!(body["errorType"], "internal");
}

#[tokio::test]
async fn auth_rejects_missing_and_unknown_tokens() {
    let store = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let app = one_tenant_app(store, EngineConfig::default(), &tid, "secret-a");
    let query = encode_query_param("anything");
    let uri = format!("/api/v1/query?query={query}&time=0");

    let (status, body) = call(&app, &uri, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["errorType"], "unauthorized");

    let (status, body) = call(&app, &uri, Some("wrong-token")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["errorType"], "unauthorized");
}

#[tokio::test]
async fn cross_tenant_data_is_isolated() {
    let store = Arc::new(MemoryStore::new());
    let tenant_a = tenant("tenant-a");
    let tenant_b = tenant("tenant-b");
    let now = now_ns();
    let hour_bucket = u32::try_from(now / NS_PER_HOUR).expect("hour bucket");

    // Only tenant B has data.
    let series = vec![series_input(
        &tenant_b,
        "secret_metric",
        &[],
        &[(now - NS_PER_MIN, 9.0)],
    )];
    publish_segment(
        &store,
        tenant_b.hash(),
        0,
        Uuid::new_v4(),
        1,
        1,
        1,
        hour_bucket,
        now,
        series,
    )
    .await;

    let mut tokens = HashMap::new();
    tokens.insert("token-a".to_string(), tenant_a);
    tokens.insert("token-b".to_string(), tenant_b);
    let app = build_app(
        store,
        CatalogConfig::default(),
        EngineConfig::default(),
        tokens,
    );

    let query = encode_query_param("secret_metric");
    let uri = format!(
        "/api/v1/query?query={query}&time={}",
        (now - NS_PER_MIN) / NS_PER_SEC
    );

    let (status, body) = call(&app, &uri, Some("token-a")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        vector_results(&body).len(),
        0,
        "tenant A must not see tenant B's series"
    );

    let (status, body) = call(&app, &uri, Some("token-b")).await;
    assert_eq!(status, StatusCode::OK);
    let results = vector_results(&body);
    assert_eq!(results.len(), 1);
    assert_eq!(value_string(&results[0]), "9");
}

#[tokio::test]
async fn series_endpoint_reflects_published_labels() {
    let store = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let th = tid.hash();
    let now = now_ns();
    let hour_bucket = u32::try_from(now / NS_PER_HOUR).expect("hour bucket");

    let series = vec![
        series_input(
            &tid,
            "http_requests_total",
            &[("method", "get")],
            &[(now - NS_PER_MIN, 1.0)],
        ),
        series_input(
            &tid,
            "http_requests_total",
            &[("method", "post")],
            &[(now - NS_PER_MIN, 1.0)],
        ),
    ];
    publish_segment(
        &store,
        th,
        0,
        Uuid::new_v4(),
        1,
        1,
        1,
        hour_bucket,
        now,
        series,
    )
    .await;

    let app = one_tenant_app(store, EngineConfig::default(), &tid, "secret-a");
    let matcher = encode_query_param("http_requests_total");
    let uri = format!("/api/v1/series?match%5B%5D={matcher}");
    let (status, body) = call(&app, &uri, Some("secret-a")).await;

    assert_eq!(status, StatusCode::OK);
    let results = body["data"].as_array().expect("series result array");
    assert_eq!(results.len(), 2);
    let mut methods: Vec<&str> = results
        .iter()
        .map(|s| s["method"].as_str().expect("method label"))
        .collect();
    methods.sort_unstable();
    assert_eq!(methods, vec!["get", "post"]);
}

#[tokio::test]
async fn series_endpoint_requires_match_param() {
    let store = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let app = one_tenant_app(store, EngineConfig::default(), &tid, "secret-a");
    let (status, body) = call(&app, "/api/v1/series", Some("secret-a")).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["errorType"], "bad_data");
}

// ---------------------------------------------------------------------------
// Per-query cost recording (ADR-0044 section 4, issue #425)
//
// These prove the Prometheus-shaped handlers fold each completed query's cost
// into the `QueryCostRecorder` seam, and that the numbers recorded are exactly
// the numbers the response body reports. A capturing recorder stands in for the
// process-global aggregator: what it captures is what `/metrics` would sum.
// ---------------------------------------------------------------------------

/// A [`QueryCostRecorder`] that keeps every recording for inspection.
#[derive(Default)]
struct CapturingRecorder {
    calls: std::sync::Mutex<
        Vec<(
            ravel_types::accounting::QueryAccountingSnapshot,
            ravel_types::accounting::CostEstimate,
            TenantHash,
            ravel_types::accounting::QueryWorkloadClass,
        )>,
    >,
}

impl ravel_types::accounting::QueryCostRecorder for CapturingRecorder {
    fn record(
        &self,
        accounting: &ravel_types::accounting::QueryAccountingSnapshot,
        estimate: &ravel_types::accounting::CostEstimate,
        tenant_hash: TenantHash,
        workload_class: ravel_types::accounting::QueryWorkloadClass,
    ) {
        self.calls.lock().expect("recorder mutex").push((
            *accounting,
            *estimate,
            tenant_hash,
            workload_class,
        ));
    }
}

/// One tenant, plus a capturing cost recorder wired into the router the same
/// way a deployment wires the real aggregator.
fn one_tenant_app_with_recorder(
    store: Arc<MemoryStore>,
    engine_config: EngineConfig,
    tenant_id: &TenantId,
    token: &str,
    recorder: Arc<CapturingRecorder>,
) -> Router {
    let backend: Arc<dyn ObjectStoreBackend> = store;
    let catalog =
        Arc::new(Catalog::new(backend.clone(), CatalogConfig::default()).expect("catalog"));
    let engine = Arc::new(QueryEngine::new(catalog, backend, engine_config));
    let mut tokens = HashMap::new();
    tokens.insert(token.to_string(), tenant_id.clone());
    let state = AppState::new(engine, Arc::new(StaticBearerTokenResolver::new(tokens)))
        .with_cost_recorder(recorder);
    router(state)
}

#[tokio::test]
async fn instant_query_folds_cost_into_recorder_matching_the_response_stats() {
    let store = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let th = tid.hash();
    let now = now_ns();
    let hour_bucket = u32::try_from(now / NS_PER_HOUR).expect("hour bucket");

    let series = vec![series_input(
        &tid,
        "http_requests_total",
        &[("method", "get")],
        &[(now - NS_PER_MIN, 42.0)],
    )];
    publish_segment(
        &store,
        th,
        0,
        Uuid::new_v4(),
        1,
        1,
        1,
        hour_bucket,
        now,
        series,
    )
    .await;

    let recorder = Arc::new(CapturingRecorder::default());
    let app = one_tenant_app_with_recorder(
        store,
        EngineConfig::default(),
        &tid,
        "secret-a",
        Arc::clone(&recorder),
    );
    let query = encode_query_param("http_requests_total{method=\"get\"}");
    let uri = format!(
        "/api/v1/query?query={query}&time={}",
        (now - NS_PER_MIN) / NS_PER_SEC
    );
    let (status, body) = call(&app, &uri, Some("secret-a")).await;
    assert_eq!(status, StatusCode::OK, "query failed: {body}");

    let calls = recorder.calls.lock().expect("recorder mutex");
    assert_eq!(
        calls.len(),
        1,
        "exactly one query is recorded per completed request"
    );
    let (snapshot, _estimate, tenant_hash, workload) = &calls[0];
    assert_eq!(*tenant_hash, th, "recorded under the query's tenant");
    assert_eq!(
        *workload,
        ravel_types::accounting::QueryWorkloadClass::Interactive,
        "an HTTP query is interactive"
    );
    // A genuine segment GET happened, so the recorded actual is non-zero: this
    // is not a zero placeholder folded in regardless of work done.
    assert!(
        snapshot.total_s3_requests() > 0,
        "the query fetched a segment, so recorded s3 requests must be > 0"
    );

    // The recorded snapshot equals, field for field, the accounting the
    // response body reported. This is the assertion a future refactor that
    // re-introduces a second, separately-counted handle would break: the
    // metric and the response body can never disagree.
    let reported = &body["data"]["stats"]["accounting"];
    use ravel_types::accounting::AccountedOp;
    assert_eq!(
        reported["s3GetRequests"].as_u64().expect("s3GetRequests"),
        snapshot.s3_requests(AccountedOp::Get),
        "recorded get requests must equal the response's"
    );
    assert_eq!(
        reported["s3ListRequests"].as_u64().expect("s3ListRequests"),
        snapshot.s3_requests(AccountedOp::List),
    );
    assert_eq!(
        reported["s3GetBytes"].as_u64().expect("s3GetBytes"),
        snapshot.s3_bytes(AccountedOp::Get),
    );
    assert_eq!(
        reported["decompressedBytes"]
            .as_u64()
            .expect("decompressedBytes"),
        snapshot.decompressed_bytes,
    );
    assert_eq!(
        reported["segmentsOpened"].as_u64().expect("segmentsOpened"),
        snapshot.segments_opened,
    );
}

#[tokio::test]
async fn series_endpoint_folds_cost_into_recorder() {
    let store = Arc::new(MemoryStore::new());
    let tid = tenant("tenant-a");
    let th = tid.hash();
    let now = now_ns();
    let hour_bucket = u32::try_from(now / NS_PER_HOUR).expect("hour bucket");

    let series = vec![series_input(
        &tid,
        "http_requests_total",
        &[("method", "get")],
        &[(now - NS_PER_MIN, 1.0)],
    )];
    publish_segment(
        &store,
        th,
        0,
        Uuid::new_v4(),
        1,
        1,
        1,
        hour_bucket,
        now,
        series,
    )
    .await;

    let recorder = Arc::new(CapturingRecorder::default());
    let app = one_tenant_app_with_recorder(
        store,
        EngineConfig::default(),
        &tid,
        "secret-a",
        Arc::clone(&recorder),
    );
    let matcher = encode_query_param("http_requests_total");
    let uri = format!("/api/v1/series?match%5B%5D={matcher}");
    let (status, _body) = call(&app, &uri, Some("secret-a")).await;
    assert_eq!(status, StatusCode::OK);

    let calls = recorder.calls.lock().expect("recorder mutex");
    assert_eq!(
        calls.len(),
        1,
        "one /series request folds its cost exactly once, not once per selector"
    );
    let (snapshot, _estimate, tenant_hash, workload) = &calls[0];
    assert_eq!(*tenant_hash, th);
    assert_eq!(
        *workload,
        ravel_types::accounting::QueryWorkloadClass::Interactive
    );
    // The metadata resolve listed and read the catalog and opened the matching
    // segment, so its cost is real and non-zero. Before this wiring the
    // handler dropped its accounting and this stayed zero.
    assert!(
        snapshot.total_s3_requests() > 0,
        "the /series resolve fetched from the store, so recorded s3 requests must be > 0"
    );
}
