//! Per-query cost accounting reaches `/metrics` for the read surfaces #602 did
//! not wire: the Prometheus-shaped query paths, and (under `flight-sql`) Flight
//! SQL (issue #425, ADR-0044 section 4).
//!
//! These drive the real `ravel_query::http` router and the real `GET /metrics`
//! handler over one shared [`QueryAccountingMetrics`], exactly as
//! `ravel_server::start` wires them, so what is asserted is what an operator
//! and a client would actually observe. A real RSEG segment and a real commit
//! record are published to a `MemoryStore`, so a genuine object-store Get
//! happens and the recorded counters are non-trivial.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_ingest::{AdmissionController, AdmissionLimits, SystemClock};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions, StoreMetrics};
use ravel_query::http::{StaticBearerTokenResolver, router as promql_router};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_server::Mode;
use ravel_server::metrics::{MetricsState, QueryAccountingMetrics};
use ravel_server::query::build_app_state;
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantId};
use tower::ServiceExt;
use uuid::Uuid;

const METRIC: &str = "m";

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_MIN: i64 = 60 * NS_PER_SEC;
const NS_PER_HOUR: i64 = 60 * NS_PER_MIN;

fn now_ns() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos(),
    )
    .expect("now fits i64")
}

/// Publish one real RSEG segment plus its commit record for `tenant`, with two
/// samples just after `base_ns`, in the ingest-hour bucket `base_ns` falls in,
/// so a query over a window covering `base_ns` resolves, opens the segment, and
/// fetches real data.
///
/// `base_ns` is a parameter, not a fixed epoch offset, because the PromQL
/// router reads wall-clock `now` (crates/ravel-query/src/http/handlers.rs) and
/// `Catalog::resolve` lists one prefix per (shard, ingest-hour) from the window
/// start up to `now`. Data anchored at the epoch with a `start=0` query would
/// list every hour bucket since 1970 (about half a million) and now trip the
/// issue #635 window-cost ceiling; the PromQL tests place data a few minutes in
/// the past and query a recent window instead, exactly as tests/e2e.rs does.
/// The Flight SQL path injects a fixed clock, so it keeps `base_ns = 0`.
async fn publish_segment(store: &dyn ObjectStoreBackend, tenant: &TenantId, base_ns: i64) {
    let tenant_hash = tenant.hash();
    let label_set = LabelSet::new(vec![Label {
        name: "__name__".to_string(),
        value: METRIC.to_string(),
    }])
    .expect("valid labels");
    let series = vec![SeriesInput {
        series_id: SeriesId::compute(tenant, METRIC, &label_set).expect("series id"),
        labels: label_set,
        samples: vec![
            Sample {
                ts_ns: base_ns + 100_000_000,
                value: 1.0,
            },
            Sample {
                ts_ns: base_ns + 200_000_000,
                value: 2.5,
            },
        ],
    }];

    let writer_id = Uuid::from_u128(4_000);
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq: 1,
    };
    let written = SegmentWriter::write(
        series,
        identity,
        IngestBounds {
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 0,
        },
    )
    .expect("write segment");

    let rec = record::build(NewCommitRecord {
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
        created_unix_ns: base_ns + 300_000_000,
        ingest_hour_bucket: u32::try_from(base_ns / NS_PER_HOUR).expect("hour bucket fits u32"),
    })
    .expect("valid commit record");

    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    store
        .put(&data_key, written.bytes, PutOptions::default())
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
}

/// Publish a segment a few minutes in the past and return the `(start, end)`
/// seconds of a recent PromQL query window that covers it. The PromQL router
/// reads wall-clock `now`, so the window must be anchored on it; a bounded,
/// recent window keeps the resolve's listing to a handful of hour buckets
/// (issue #635), the same shape tests/e2e.rs uses.
async fn publish_recent(store: &dyn ObjectStoreBackend, tenant: &TenantId) -> (i64, i64) {
    let now = now_ns();
    publish_segment(store, tenant, now - 10 * NS_PER_MIN).await;
    ((now - 15 * NS_PER_MIN) / NS_PER_SEC, now / NS_PER_SEC)
}

/// A PromQL router and a `/metrics` router that share one aggregator and one
/// catalog, exactly as `ravel_server::start` wires them.
struct Surfaces {
    promql: Router,
    metrics: Router,
    query_accounting: Arc<QueryAccountingMetrics>,
}

fn surfaces(store: Arc<dyn ObjectStoreBackend>, tenant: &TenantId) -> Surfaces {
    let query_accounting = Arc::new(QueryAccountingMetrics::new(HashSet::new()));
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    let mut tokens = std::collections::HashMap::new();
    tokens.insert("acme-token".to_string(), tenant.clone());

    let app_state = build_app_state(
        Arc::clone(&catalog),
        Arc::clone(&store),
        Arc::new(StaticBearerTokenResolver::new(tokens)),
        None,
        ravel_query::EngineConfig::default(),
        Arc::clone(&query_accounting),
    );
    let promql = promql_router(app_state);

    let metrics = ravel_server::metrics::router(MetricsState {
        mode: Mode::All,
        store_metrics: Arc::new(StoreMetrics::default()),
        ingest_router: None,
        log_ingest_router: None,
        span_ingest_router: None,
        catalog,
        tenant_discovery: None,
        maintenance_safety: None,
        cache_metrics: None,
        admission: Arc::new(AdmissionController::new(
            Arc::new(SystemClock),
            AdmissionLimits::default(),
        )),
        metrics_tenant_labels: false,
        query_accounting: Arc::clone(&query_accounting),
    });

    Surfaces {
        promql,
        metrics,
        query_accounting,
    }
}

async fn get(app: &Router, uri: &str) -> (StatusCode, serde_json::Value) {
    let request = Request::builder()
        .method("GET")
        .uri(uri)
        .header("authorization", "Bearer acme-token")
        .body(Body::empty())
        .expect("build request");
    let response = app.clone().oneshot(request).await.expect("oneshot");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let value = serde_json::from_slice(&bytes)
        .unwrap_or_else(|e| panic!("body not JSON ({e}): {}", String::from_utf8_lossy(&bytes)));
    (status, value)
}

async fn scrape(app: &Router) -> String {
    let request = Request::builder()
        .method("GET")
        .uri("/metrics")
        .body(Body::empty())
        .expect("build request");
    let response = app.clone().oneshot(request).await.expect("oneshot");
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    String::from_utf8(bytes.to_vec()).expect("utf8 metrics")
}

/// The label keys on the first `ravel_query_*` sample line (not a `# HELP` or
/// `# TYPE` comment) that actually carries a `{...}` label set.
fn query_family_label_key_sets(scrape: &str) -> Vec<Vec<String>> {
    scrape
        .lines()
        .filter(|line| line.starts_with("ravel_query_") && line.contains('{'))
        .map(|line| {
            let open = line.find('{').expect("open brace");
            let close = line.find('}').expect("close brace");
            let mut keys: Vec<String> = line[open + 1..close]
                .split(',')
                .filter(|kv| !kv.is_empty())
                .map(|kv| kv.split('=').next().expect("label key").to_string())
                .collect();
            keys.sort();
            keys.dedup();
            keys
        })
        .collect()
}

/// The single aggregated row for a one-tenant, one-query run: exactly one row,
/// folded into `tenant_hash="other"` (no tenant configured), interactive.
fn only_row(
    query_accounting: &QueryAccountingMetrics,
) -> ravel_server::metrics::QueryAccountingRow {
    let rows = query_accounting.snapshot();
    assert_eq!(
        rows.len(),
        1,
        "one tenant bucket, one workload class: one row"
    );
    rows[0]
}

#[tokio::test]
async fn a_promql_range_query_folds_nonzero_cost_into_the_aggregator() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme".to_string());
    let (start, end) = publish_recent(store.as_ref(), &tenant).await;
    let s = surfaces(Arc::clone(&store), &tenant);

    let (status, body) = get(
        &s.promql,
        &format!("/api/v1/query_range?query={METRIC}&start={start}&end={end}&step=60s"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "query failed: {body}");

    let row = only_row(&s.query_accounting);
    assert_eq!(row.counters.queries, 1, "one query recorded");
    assert!(
        row.counters.s3_requests > 0,
        "the query opened a segment, so the aggregator's s3 request count must be > 0"
    );
}

#[tokio::test]
async fn a_promql_series_request_folds_nonzero_cost_into_the_aggregator() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme".to_string());
    let (start, end) = publish_recent(store.as_ref(), &tenant).await;
    let s = surfaces(Arc::clone(&store), &tenant);

    let (status, _body) = get(
        &s.promql,
        &format!("/api/v1/series?match%5B%5D={METRIC}&start={start}&end={end}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let row = only_row(&s.query_accounting);
    assert_eq!(row.counters.queries, 1);
    assert!(
        row.counters.s3_requests > 0,
        "the /series resolve read the store, so its s3 request count must be > 0"
    );
}

#[tokio::test]
async fn the_aggregator_total_equals_the_response_stats_total() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme".to_string());
    let (start, end) = publish_recent(store.as_ref(), &tenant).await;
    let s = surfaces(Arc::clone(&store), &tenant);

    let (status, body) = get(
        &s.promql,
        &format!("/api/v1/query_range?query={METRIC}&start={start}&end={end}&step=60s"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "query failed: {body}");

    // The response's own stats.accounting, summed across operation kinds.
    let acc = &body["data"]["stats"]["accounting"];
    let reported = acc["s3GetRequests"].as_u64().expect("s3GetRequests")
        + acc["s3ListRequests"].as_u64().expect("s3ListRequests")
        + acc["s3HeadRequests"].as_u64().expect("s3HeadRequests");
    assert!(reported > 0, "the response must report a real cost");

    // The aggregator folded exactly the same snapshot, so its total equals the
    // response body's total. This is the assertion a future refactor that
    // re-introduces a second, separately-counted handle would break.
    let row = only_row(&s.query_accounting);
    assert_eq!(
        row.counters.s3_requests, reported,
        "the /metrics aggregate and the response body must never disagree"
    );
}

#[tokio::test]
async fn the_rendered_query_family_uses_only_the_allowlisted_label_keys_after_a_promql_query() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme".to_string());
    let (start, end) = publish_recent(store.as_ref(), &tenant).await;
    let s = surfaces(Arc::clone(&store), &tenant);

    let (status, _body) = get(
        &s.promql,
        &format!("/api/v1/query_range?query={METRIC}&start={start}&end={end}&step=60s"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let scrape = scrape(&s.metrics).await;
    let key_sets = query_family_label_key_sets(&scrape);
    assert!(
        !key_sets.is_empty(),
        "the query family must render at least one sample after a query:\n{scrape}"
    );
    // ADR-0044 section 4's allowlist is {tenant_hash, signal, mode, op,
    // error_kind, workload_class, level}; the query family uses exactly the
    // subset {mode, tenant_hash, workload_class}. No other key may appear.
    let expected = vec![
        "mode".to_string(),
        "tenant_hash".to_string(),
        "workload_class".to_string(),
    ];
    for keys in &key_sets {
        assert_eq!(
            keys, &expected,
            "a ravel_query_* line carried a label key outside the allowlist subset:\n{scrape}"
        );
    }
}

// ---------------------------------------------------------------------------
// Flight SQL shares the same aggregator and the same bounded label set.
//
// The required `-p ravel-server --features sql` gate does not build the Flight
// path (the `flight-sql` feature is off there), so this runs under
// `--features flight-sql`. It proves the fourth surface folds into the same
// process aggregator and renders with only the allowlisted keys.
// ---------------------------------------------------------------------------
#[cfg(feature = "flight-sql")]
mod flight {
    use super::*;

    use std::net::SocketAddr;
    use std::time::Duration;

    use arrow_flight::flight_service_client::FlightServiceClient;
    use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt};
    use arrow_flight::{FlightDescriptor, Ticket};
    use futures::TryStreamExt;
    use prost::Message as _;
    use ravel_ingest::Clock;
    use ravel_query::{LogSegmentFetcher, SegmentFetcher};
    use ravel_server::sql::SqlState;
    use ravel_sql::{SqlConfig, SqlExecutor};
    use tokio::sync::oneshot;
    use tonic::Request;

    struct FixedClock;
    impl Clock for FixedClock {
        fn now_ns(&self) -> i64 {
            // Small on purpose: Catalog::resolve lists one prefix per (shard,
            // ingest-hour) across the window, so a wall-clock value would fan
            // out to hundreds of thousands of LISTs.
            4 * 3_600_000_000_000
        }
    }

    fn sql_state(
        store: Arc<dyn ObjectStoreBackend>,
        tenant: &TenantId,
        query_accounting: Arc<QueryAccountingMetrics>,
    ) -> SqlState {
        let catalog =
            Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
        let executor = SqlExecutor::new(
            catalog,
            SegmentFetcher::new(store.clone()),
            LogSegmentFetcher::new(store.clone()),
            SqlConfig::default(),
            1 << 30,
        );
        let mut tokens = std::collections::HashMap::new();
        tokens.insert("acme-token".to_string(), tenant.clone());
        SqlState {
            executor: Arc::new(executor),
            tenant_resolver: Arc::new(StaticBearerTokenResolver::new(tokens)),
            store,
            clock: Arc::new(FixedClock),
            max_deadline: Duration::from_secs(30),
            query_accounting,
        }
    }

    async fn run_flight_query(state: &SqlState) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr: SocketAddr = listener.local_addr().expect("addr");
        let (tx, rx) = oneshot::channel::<()>();
        // The Flight ticket-TTL ceiling comes from the durable sys/gc object
        // (ADR-0050 section 4, EC4). This test uses the maintain-default
        // ceiling the server would source on a fresh bucket, matching
        // tests/flight_sql.rs. It has no bearing on cost accounting; it is
        // only what `service` needs to build a ticket.
        let ceiling = ravel_server::gc_config::flight_ceiling(
            &ravel_maintain::GcConfigValues::maintain_defaults(),
        );
        let service = ravel_server::flight::service(state, ceiling);
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(
                    tonic::transport::server::TcpIncoming::from(listener),
                    async {
                        let _ = rx.await;
                    },
                )
                .await
                .expect("serve");
        });

        let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
            .expect("uri")
            .connect()
            .await
            .expect("connect");
        let mut client = FlightServiceClient::new(channel);

        let command = CommandStatementQuery {
            query: "SELECT ts, value FROM samples ORDER BY ts".to_string(),
            transaction_id: None,
        };
        let mut descriptor =
            Request::new(FlightDescriptor::new_cmd(command.as_any().encode_to_vec()));
        authed(descriptor.metadata_mut());
        let info = client
            .get_flight_info(descriptor)
            .await
            .expect("flight info")
            .into_inner();
        let ticket: Ticket = info
            .endpoint
            .first()
            .expect("one endpoint")
            .ticket
            .clone()
            .expect("a ticket");

        let mut do_get = Request::new(ticket);
        authed(do_get.metadata_mut());
        let stream = client.do_get(do_get).await.expect("do get").into_inner();
        // Draining to completion drops the stream, firing the DoGet execution
        // fold.
        let _batches = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
            stream.map_err(|s| arrow_flight::error::FlightError::Tonic(Box::new(s))),
        )
        .try_collect::<Vec<_>>()
        .await
        .expect("decode");

        let _ = tx.send(());
        let _ = task.await;
    }

    fn authed(metadata: &mut tonic::metadata::MetadataMap) {
        metadata.insert("authorization", "Bearer acme-token".parse().expect("ascii"));
        metadata.insert("x-ravel-start", "0".parse().expect("ascii"));
        metadata.insert(
            "x-ravel-end",
            format!("{}", (4 * 3_600_000_000_000i64) as f64 / 1e9)
                .parse()
                .expect("ascii"),
        );
    }

    #[tokio::test]
    async fn flight_sql_folds_nonzero_cost_and_renders_only_allowlisted_keys() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let tenant = TenantId::new("acme".to_string());
        // The Flight SQL path injects a fixed clock at 4h (see `FixedClock`),
        // so its listing window is bounded regardless; keep the data at the
        // epoch hour the fixed-clock window `[0, 4h]` covers.
        publish_segment(store.as_ref(), &tenant, 0).await;
        let query_accounting = Arc::new(QueryAccountingMetrics::new(HashSet::new()));
        let state = sql_state(Arc::clone(&store), &tenant, Arc::clone(&query_accounting));

        run_flight_query(&state).await;

        // Both RPCs folded (GetFlightInfo resolve+plan, DoGet execution), so the
        // summed cost is non-zero in the same aggregator the PromQL and SQL
        // paths use.
        let total: u64 = query_accounting
            .snapshot()
            .iter()
            .map(|r| r.counters.s3_requests)
            .sum();
        assert!(
            total > 0,
            "a Flight SQL query read the store, so its aggregated s3 requests must be > 0"
        );

        // Render through the same metrics surface and check no label key
        // outside the allowlist subset appeared for the Flight-recorded rows.
        let metrics = ravel_server::metrics::router(MetricsState {
            mode: Mode::All,
            store_metrics: Arc::new(StoreMetrics::default()),
            ingest_router: None,
            log_ingest_router: None,
            span_ingest_router: None,
            catalog: Arc::new(
                Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"),
            ),
            tenant_discovery: None,
            maintenance_safety: None,
            cache_metrics: None,
            admission: Arc::new(AdmissionController::new(
                Arc::new(SystemClock),
                AdmissionLimits::default(),
            )),
            metrics_tenant_labels: false,
            query_accounting: Arc::clone(&query_accounting),
        });
        let scrape = scrape(&metrics).await;
        let expected = vec![
            "mode".to_string(),
            "tenant_hash".to_string(),
            "workload_class".to_string(),
        ];
        let key_sets = query_family_label_key_sets(&scrape);
        assert!(!key_sets.is_empty(), "query family must render:\n{scrape}");
        for keys in &key_sets {
            assert_eq!(
                keys, &expected,
                "flight query rendered a stray label key:\n{scrape}"
            );
        }
    }
}
