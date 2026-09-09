//! End-to-end acceptance proof for the fleet-global query concurrency ceiling
//! (ADR-0061 decision 2).
//!
//! These tests drive real Prometheus-shaped queries through the full HTTP stack
//! (`ravel_query::http::router` -> `AppState` -> `QueryEngine` -> catalog ->
//! fetcher) against a real object store, exactly as `e2e.rs` does, and prove:
//!
//! 1. When the process is at its (reconciled) ceiling, the next query is
//!    rejected with HTTP 503 **before it does any store work at all** — no
//!    resolve LIST/GET, no data-object GET. This is verified the same way the
//!    cancellation proof verifies non-work: a counting store shows the rejected
//!    query added exactly zero store operations, not merely that it got an error
//!    response.
//!
//! 2. The ceiling that rejects is the fleet-**reconciled** one, computed on
//!    ADR-0057's `2R` staleness rules: a stale sibling snapshot is treated as
//!    contributing zero (so it never freezes this process out), while a fresh
//!    sibling's in-flight count reduces this process's own local threshold and
//!    so rejects a query the raw per-process cap would have admitted.
//!
//! ## Approximating multiple processes in one test process
//!
//! Spinning up genuinely separate OS processes in a unit test is impractical,
//! so a "sibling process" is simulated by writing its snapshot key
//! (`admission/query/<pid>.snapshot`) directly to the shared store and then
//! running one real reconciliation cycle
//! (`reconcile_query_admission_once`). The snapshot body is the module's own
//! JSON shape (`format_version`/`in_flight`/`snapshot_unix_ns`, documented in
//! docs/catalog-and-mvcc.md); writing it by hand is exactly what a second
//! process's reconciliation loop would have written, so the reconciling process
//! under test cannot tell the difference.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::too_many_arguments)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use bytes::Bytes;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_object_store::fault::{FaultPlan, FaultStore, GateHandle, Occurrence, Op};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutMode, PutOptions, PutOutcome, StoreError,
};
use ravel_query::http::{AppState, StaticBearerTokenResolver, router};
use ravel_query::{
    EngineConfig, QueryAdmissionController, QueryConcurrencyLimit, QueryEngine,
    query_admission_snapshot_key, reconcile_query_admission_once,
};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput, WrittenSegment};
use ravel_types::{CommitToken, Label, LabelSet, Sample, SeriesId, Signal, TenantHash, TenantId};
use serde_json::Value;
use tower::ServiceExt;
use uuid::Uuid;

const NS: i64 = 1_000_000_000;
const SAMPLE_TS_NS: i64 = 1_000 * NS;
const METRIC: &str = "m";
const R: Duration = Duration::from_secs(10);
const R_NS: i64 = 10 * NS;
const TOKEN: &str = "secret";

/// Wraps a store and counts every read operation (`get`, `list`, `head`) it is
/// asked to perform, passing each straight through. A query that reaches resolve
/// issues LISTs and GETs; a query rejected by the admission ceiling before it
/// starts issues none, so the counter's delta across a rejected request is
/// exactly zero — the "proved non-work" signal, not merely an error response.
struct CountingStore<S> {
    inner: S,
    reads: Arc<AtomicUsize>,
}

#[async_trait]
impl<S: ObjectStoreBackend> ObjectStoreBackend for CountingStore<S> {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.get(key, range).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.list(prefix, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
}

/// Publishes one real RSEG segment carrying a single `METRIC` sample and returns
/// the read-your-write token that makes it visible. `ingest_hour_bucket` 0 keeps
/// it out of the real-time listing window, so only the token reveals it: catalog
/// resolution reads the commit record, then the fetcher GETs the data object.
async fn publish_segment(
    store: &MemoryStore,
    tenant_id: &TenantId,
    tenant_hash: TenantHash,
) -> CommitToken {
    let writer_id = Uuid::from_u128(7);
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
    let label_set = LabelSet::new(vec![Label {
        name: "__name__".to_string(),
        value: METRIC.to_string(),
    }])
    .expect("valid labels");
    let series_id = SeriesId::compute(tenant_id, METRIC, &label_set).expect("series id");
    let input = SeriesInput {
        series_id,
        labels: label_set,
        samples: vec![Sample {
            ts_ns: SAMPLE_TS_NS,
            value: 1.0,
        }],
    };
    let written: WrittenSegment =
        SegmentWriter::write(vec![input], identity, bounds).expect("write segment");

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
        created_unix_ns: 42,
        ingest_hour_bucket: 0,
    };
    let rec = record::build(new_record).expect("valid commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    publish::put_data_object(store, &data_key, written.bytes)
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish")
}

/// One tenant, one published segment, a ceiling of `ceiling`, and a store stack
/// `FaultStore(CountingStore(MemoryStore))`. The [`GateHandle`] holds every
/// data-object GET (`Op::Get` on a key containing `"rseg"`) *before* it reaches
/// the counting store, so a held query's data fetch is neither completed nor
/// counted while parked — exactly the shape the gated cancellation proof
/// uses. Catalog/commit reads and the `admission/query/*` reconciliation reads
/// are not data objects, so they are never gated.
struct Harness {
    app: Router,
    controller: Arc<QueryAdmissionController>,
    backend: Arc<dyn ObjectStoreBackend>,
    gate: GateHandle,
    reads: Arc<AtomicUsize>,
    token: CommitToken,
}

async fn harness(ceiling: u64) -> Harness {
    let tenant_id = TenantId::new("tenant-a".to_string());
    let tenant_hash = tenant_id.hash();

    let inner = MemoryStore::new();
    let token = publish_segment(&inner, &tenant_id, tenant_hash).await;

    let reads = Arc::new(AtomicUsize::new(0));
    let counting = CountingStore {
        inner,
        reads: Arc::clone(&reads),
    };
    let fault_store = FaultStore::new(counting, FaultPlan::default());
    let gate = fault_store.hold(Op::Get, Some("rseg".to_string()), Occurrence::Always);
    let backend: Arc<dyn ObjectStoreBackend> = Arc::new(fault_store);

    let catalog =
        Arc::new(Catalog::new(backend.clone(), CatalogConfig::default()).expect("catalog"));
    let engine = Arc::new(QueryEngine::new(
        catalog,
        backend.clone(),
        EngineConfig::default(),
    ));
    let controller = QueryAdmissionController::shared(QueryConcurrencyLimit::Bounded(ceiling));

    let mut tokens = std::collections::HashMap::new();
    tokens.insert(TOKEN.to_string(), tenant_id.clone());
    let state = AppState::new(engine, Arc::new(StaticBearerTokenResolver::new(tokens)))
        .with_query_admission(controller.clone());

    Harness {
        app: router(state),
        controller,
        backend,
        gate,
        reads,
        token,
    }
}

/// Build a GET request for the published metric, visible via its commit token.
fn query_request(token: &CommitToken) -> Request<Body> {
    let uri = format!(
        "/api/v1/query?query={METRIC}&time={}&min_commit_token={}",
        SAMPLE_TS_NS / 1_000_000,
        token.encode()
    );
    Request::builder()
        .method("GET")
        .uri(uri)
        .header("authorization", format!("Bearer {TOKEN}"))
        .body(Body::empty())
        .expect("build request")
}

/// Write a sibling process's snapshot directly, as that process's own
/// reconciliation loop would have (see the module doc). The body is the module's
/// JSON shape; a hand-written object with the same field names decodes
/// identically.
async fn put_sibling_snapshot(
    backend: &dyn ObjectStoreBackend,
    process_id: &str,
    in_flight: u64,
    snapshot_unix_ns: i64,
) {
    let body = serde_json::to_vec(&serde_json::json!({
        "format_version": 1,
        "in_flight": in_flight,
        "snapshot_unix_ns": snapshot_unix_ns,
    }))
    .expect("serialize sibling snapshot");
    backend
        .put(
            &query_admission_snapshot_key(process_id),
            body.into(),
            PutOptions {
                mode: PutMode::Overwrite,
                checksum: None,
            },
        )
        .await
        .expect("seed sibling snapshot");
}

/// Drive a rejected query over real HTTP and assert it returned 503 with the
/// concurrency error body AND touched the store zero times.
async fn assert_rejected_without_store_work(h: &Harness) {
    let before = h.reads.load(Ordering::SeqCst);
    let response = h
        .app
        .clone()
        .oneshot(query_request(&h.token))
        .await
        .expect("oneshot infallible");
    assert_eq!(
        response.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "an over-ceiling query must be rejected with 503"
    );
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let json: Value = serde_json::from_slice(&body).expect("json body");
    assert_eq!(json["status"], "error");
    assert!(
        json["error"]
            .as_str()
            .expect("error message")
            .contains("concurrency ceiling"),
        "rejection body must name the concurrency ceiling, got {json}"
    );
    let after = h.reads.load(Ordering::SeqCst);
    assert_eq!(
        after, before,
        "the rejected query must issue ZERO store operations (it was rejected before any \
         resolve or GET): store reads went from {before} to {after}"
    );
}

/// THE epic acceptance test: a real spawned HTTP query surface at a ceiling of 2,
/// driven with 2 concurrent real queries that fill the ceiling and a 3rd that is
/// rejected before it does any store work. A stale sibling snapshot is reconciled
/// in first, proving the ceiling that rejects is the fleet-reconciled one and
/// that a stale sibling is self-corrected to zero (never freezing this process
/// out) per ADR-0057 section 3's `2R` window.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn at_ceiling_the_next_query_is_rejected_before_any_store_work() {
    let h = harness(2).await;

    // A stale sibling (holding 9, written 3*R ago) must NOT drag this process's
    // threshold down: past 2*R it contributes zero (ADR-0057 section 3). After
    // reconciliation the effective threshold is still the full configured 2.
    let now = 1_000 * R_NS;
    put_sibling_snapshot(h.backend.as_ref(), "stale-sibling", 9, now - 3 * R_NS).await;
    reconcile_query_admission_once(h.controller.as_ref(), h.backend.as_ref(), R, now).await;
    assert_eq!(
        h.controller.effective_threshold(),
        2,
        "a stale sibling is treated as zero, so the reconciled ceiling stays at the configured 2"
    );

    // Two concurrent real queries fill the ceiling. Each acquires a permit at the
    // handler entry, resolves the snapshot, then parks on the held data GET, so
    // both permits are held simultaneously.
    let q1 = tokio::spawn(h.app.clone().oneshot(query_request(&h.token)));
    let q2 = tokio::spawn(h.app.clone().oneshot(query_request(&h.token)));

    // Wait until both queries are genuinely in flight (parked on the data GET),
    // so in_flight == 2 == the ceiling.
    h.gate.wait_until_held(2).await;
    assert_eq!(
        h.controller.in_flight(),
        2,
        "both admitted queries hold a permit while parked on the data GET"
    );

    // The 3rd query is rejected before it does any store work.
    assert_rejected_without_store_work(&h).await;

    // Release the held data GETs so the two admitted queries complete; their
    // permits then drop and free the slots.
    for id in h.gate.held() {
        h.gate.release(id);
    }
    let r1 = q1.await.expect("q1 join").expect("q1 response");
    let r2 = q2.await.expect("q2 join").expect("q2 response");
    assert_eq!(r1.status(), StatusCode::OK, "first admitted query succeeds");
    assert_eq!(
        r2.status(),
        StatusCode::OK,
        "second admitted query succeeds"
    );

    // A short spin for both permit `Drop`s to run, then the freed process admits
    // again over real HTTP.
    for _ in 0..100 {
        if h.controller.in_flight() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        h.controller.in_flight(),
        0,
        "completed queries released their slots"
    );
}

/// Multi-process reconciliation over real HTTP: a ceiling of 3 with one *fresh*
/// sibling reported to be holding 2 leaves this process a local share of just 1.
/// A single real query then fills that reduced share, and the next is rejected
/// before any store work — proving a sibling process's fleet usage genuinely
/// tightens this process's admission, not just its own raw per-process cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fresh_sibling_tightens_this_process_local_ceiling() {
    let h = harness(3).await;

    let now = 2_000 * R_NS;
    // A fresh sibling holding 2 of the fleet cap of 3. Reconciled local
    // threshold = own(0) + max(0, 3 - 0 - 2) = 1.
    put_sibling_snapshot(h.backend.as_ref(), "busy-sibling", 2, now).await;
    reconcile_query_admission_once(h.controller.as_ref(), h.backend.as_ref(), R, now).await;
    assert_eq!(
        h.controller.effective_threshold(),
        1,
        "own(0) + max(0, 3 - 0 - 2) = 1: the fresh sibling's fleet usage tightens this process"
    );

    // One real query fills the reduced local share of 1.
    let q1 = tokio::spawn(h.app.clone().oneshot(query_request(&h.token)));
    h.gate.wait_until_held(1).await;
    assert_eq!(h.controller.in_flight(), 1);

    // The next query is rejected before any store work, even though the raw
    // configured cap (3) would have admitted it: the fleet-reconciled threshold
    // (1) is what binds.
    assert_rejected_without_store_work(&h).await;

    for id in h.gate.held() {
        h.gate.release(id);
    }
    let r1 = q1.await.expect("q1 join").expect("q1 response");
    assert_eq!(r1.status(), StatusCode::OK);
}

/// The `AppState::new` default is an unlimited (never-rejecting) controller, so
/// a router mounted without an explicit ceiling behaves exactly as before this
/// mechanism existed: a normal query succeeds with no admission pressure. Guards
/// against the default silently becoming a bounded (and so rejecting) ceiling.
#[tokio::test]
async fn unlimited_default_admits_normally() {
    let tenant_id = TenantId::new("tenant-a".to_string());
    let tenant_hash = tenant_id.hash();
    let store = Arc::new(MemoryStore::new());
    let token = publish_segment(&store, &tenant_id, tenant_hash).await;
    let backend: Arc<dyn ObjectStoreBackend> = store;
    let catalog =
        Arc::new(Catalog::new(backend.clone(), CatalogConfig::default()).expect("catalog"));
    let engine = Arc::new(QueryEngine::new(catalog, backend, EngineConfig::default()));
    let mut tokens = std::collections::HashMap::new();
    tokens.insert(TOKEN.to_string(), tenant_id);
    // AppState::new defaults to an Unlimited controller, so this never rejects.
    let app = router(AppState::new(
        engine,
        Arc::new(StaticBearerTokenResolver::new(tokens)),
    ));
    let response = app
        .oneshot(query_request(&token))
        .await
        .expect("oneshot infallible");
    assert_eq!(response.status(), StatusCode::OK);
}
