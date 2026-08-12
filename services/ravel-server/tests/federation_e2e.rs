//! Acceptance coverage for the ADR-0071 cross-cluster federation wiring in
//! `ravel-server` (issue #868).
//!
//! Every test drives the real path: a coordinator [`QueryEngine`] with a
//! [`Federation`] whose remotes are genuine in-process [`FragmentService`] gRPC
//! servers, each over its OWN `MemoryStore`, dialed by the production
//! [`FederationSliceFetcher`] over a real tonic channel. The remote resolves its
//! own snapshot, applies its own erasure, and answers a Resolve-scope request
//! through the same fragment surface an intra-cluster slice hits.
//!
//! Three properties (the issue #868 deliverables 2-4):
//!
//! - `remote_failure_fails_query_by_default`: two servers over separate stores
//!   provisioned for different tenants; the remote is unavailable and its
//!   `skip_unavailable` is false, so the whole query fails typed
//!   ([`QueryError::Federation`]).
//! - `federated_query_marks_skipped_cluster_in_warnings`: with
//!   `skip_unavailable = true`, a down cluster is skipped, named by cluster in
//!   the Prometheus-compatible `warnings`, and the stats block records partial
//!   coverage; the healthy remote's series still merge, and that remote saw ONLY
//!   its operator-configured principal (a recording interceptor captures every
//!   `authorization` it was presented).
//! - `federated_query_merges_remote_series`: both clusters healthy; the merged
//!   result equals the union a single store holding both datasets computes
//!   locally (f64 compared as bits), and a tight `max_bytes_scanned` the
//!   local-only fetch clears trips typed on the combined local+remote frames.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_ingest::{Clock, SystemClock};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_promql::{LabelMatcher, MatchOp, Value};
use ravel_query::distrib::proto::series_fetch_server::SeriesFetchServer;
use ravel_query::distrib::{Federation, RemoteCluster};
use ravel_query::http::{StaticBearerTokenResolver, TenantResolver};
use ravel_query::{ByteLimit, EngineConfig, QueryEngine, QueryError};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_server::config::RemoteClusterConfig;
use ravel_server::distrib::{FragmentAdmission, FragmentMetrics, FragmentService};
use ravel_server::query::build_catalog;
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantId, TimeRange};
use tokio::sync::oneshot;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;

const METRIC: &str = "http_requests_total";
/// The operator-configured credential the coordinator presents to a remote. It
/// is baked into the remote's fetcher; no client credential exists on this path.
const OPERATOR_CRED: &str = "operator-east-credential";

const NS_PER_MS: i64 = 1_000_000;
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

/// Publish one real RSEG segment (one series, one sample) for `tenant` under an
/// `instance` label, anchored at `base_ns`. `seq` makes each publish into the
/// same store land on a distinct commit-record key.
async fn publish_series(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    base_ns: i64,
    instance: &str,
    value: f64,
    seq: u64,
) {
    let tenant_hash = tenant.hash();
    let label_set = LabelSet::new(vec![
        Label {
            name: "__name__".to_string(),
            value: METRIC.to_string(),
        },
        Label {
            name: "instance".to_string(),
            value: instance.to_string(),
        },
    ])
    .expect("valid labels");
    let series = vec![SeriesInput {
        series_id: SeriesId::compute(tenant, METRIC, &label_set).expect("series id"),
        labels: label_set,
        samples: vec![Sample {
            ts_ns: base_ns,
            value,
        }],
    }];

    let writer_id = uuid::Uuid::from_u128(u128::from(seq));
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq: seq,
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
        writer_seq: seq,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: 1,
        created_unix_ns: base_ns + NS_PER_MIN,
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

/// A running in-process remote cluster: its fragment `SeriesFetch` endpoint, the
/// list of `authorization` principals it has been presented (via a recording
/// interceptor), and a shutdown handle.
struct Remote {
    endpoint: String,
    seen_auth: Arc<Mutex<Vec<String>>>,
    shutdown: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<()>,
}

impl Remote {
    /// Stop the server and wait for its accept loop to finish, so the port is
    /// closed (and connections refused) on return.
    async fn stop(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        let _ = self.task.await;
    }
}

/// Stand up a real `FragmentService` gRPC server over `store`, recording every
/// `authorization` metadata value it is presented. `credential` is the ordinary
/// tenant credential the remote resolves to `tenant` (ADR-0071 #868: a federated
/// resolve-scope request authenticates through the remote's normal tenant
/// resolver, not the shared fragment token, and the tenant comes from the
/// credential, never the wire).
async fn spawn_remote(
    store: Arc<dyn ObjectStoreBackend>,
    credential: &str,
    tenant: &TenantId,
) -> Remote {
    let catalog = build_catalog(store.clone(), 1, true, 0).expect("catalog");
    let metrics = Arc::new(FragmentMetrics::new());
    let admission = FragmentAdmission::new(8, metrics.clone());
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let tokens = std::collections::HashMap::from([(credential.to_string(), tenant.clone())]);
    let resolver: Arc<dyn TenantResolver> = Arc::new(StaticBearerTokenResolver::new(tokens));
    let service = FragmentService::new(
        credential.to_string(),
        resolver,
        admission,
        catalog,
        store,
        None,
        clock,
        metrics,
    );

    let seen_auth = Arc::new(Mutex::new(Vec::<String>::new()));
    let recorder = Arc::clone(&seen_auth);
    let intercept = move |req: tonic::Request<()>| -> Result<tonic::Request<()>, tonic::Status> {
        let auth = req
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("<none>")
            .to_string();
        recorder.lock().push(auth);
        Ok(req)
    };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind remote");
    let addr = listener.local_addr().expect("remote local addr");
    let (tx, rx) = oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        Server::builder()
            .add_service(SeriesFetchServer::with_interceptor(service, intercept))
            .serve_with_incoming_shutdown(TcpIncoming::from(listener), async {
                let _ = rx.await;
            })
            .await
            .expect("remote serves");
    });

    Remote {
        endpoint: addr.to_string(),
        seen_auth,
        shutdown: Some(tx),
        task,
    }
}

/// An address that was bound then immediately released: a connection to it is
/// refused, so a remote pointed here is unavailable at query time.
async fn dead_endpoint() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind dead");
    let addr = listener.local_addr().expect("dead local addr");
    drop(listener);
    addr.to_string()
}

/// Build a coordinator-side [`RemoteCluster`] dialed by the production
/// [`FederationSliceFetcher`], carrying `credential` as the ONLY principal the
/// remote will see.
fn make_remote_cluster(
    name: &str,
    endpoint: &str,
    credential: &str,
    skip_unavailable: bool,
    soft_timeout: Duration,
) -> RemoteCluster {
    let config = RemoteClusterConfig {
        name: name.to_string(),
        endpoint: endpoint.to_string(),
        credential: credential.to_string(),
        tls: false,
        tls_ca_file: None,
        skip_unavailable,
        soft_timeout,
    };
    let fetcher = ravel_server::distrib::FederationSliceFetcher::connect(&config)
        .expect("federation client connects lazily");
    RemoteCluster {
        name: name.to_string(),
        fetcher: Arc::new(fetcher),
        skip_unavailable,
        soft_timeout,
    }
}

fn label_key(labels: &LabelSet) -> String {
    labels
        .iter()
        .map(|l| format!("{}={}", l.name, l.value))
        .collect::<Vec<_>>()
        .join(",")
}

/// `(label key, value bit pattern)` of an instant vector, sorted. Values are
/// compared as bits so a `-0.0`/NaN divergence cannot pass as equal.
fn vector_bits(value: &Value) -> Vec<(String, u64)> {
    match value {
        Value::Vector(v) => {
            let mut out: Vec<(String, u64)> = v
                .iter()
                .map(|s| (label_key(&s.labels), s.value.to_bits()))
                .collect();
            out.sort();
            out
        }
        other => panic!("expected an instant vector, got {}", other.type_name()),
    }
}

const DEADLINE: Duration = Duration::from_secs(30);

/// A `__name__ == METRIC` matcher, the selector every discovery test resolves.
fn name_matcher() -> LabelMatcher {
    LabelMatcher {
        name: "__name__".to_string(),
        op: MatchOp::Eq,
        value: METRIC.to_string(),
    }
}

/// The sorted `instance` label values of a resolved discovery result. This is
/// what `/api/v1/series` (and, projected onto one label, `/api/v1/labels` and
/// `/api/v1/label/instance/values`) enumerates: a series' identity, not its
/// samples. Sorted so a set comparison is order-independent.
fn instances(series: &[(SeriesId, LabelSet)]) -> Vec<String> {
    let mut out: Vec<String> = series
        .iter()
        .filter_map(|(_, labels)| labels.get("instance").map(str::to_string))
        .collect();
    out.sort();
    out
}

/// Deliverable 1 (issue #891), reachability + union: a federated discovery
/// request (the seam `/api/v1/series` reaches through `resolve_series_with_stats`)
/// returns series from BOTH clusters. The healthy remote's series merge into the
/// local pool exactly as the query path unions them, and a dead cluster under
/// `skip_unavailable = true` degrades to partial coverage with a warning naming
/// only the skipped cluster.
///
/// Flip-line proof for the skipped-cluster warning: `federate_discovery` in
/// crates/ravel-query/src/engine.rs threads `outcome.warnings`/`outcome.partial`
/// out of the `Federation` coordinator and `resolve_series_inner` writes them
/// onto the returned `QueryStats`. Change that thread to drop the federation
/// result -- e.g. `stats.warnings = Vec::new();` in `resolve_series_inner`
/// instead of `stats.warnings = warnings;` -- and the
/// `stats.warnings ... contains("west")` assertion below fails
/// (`assertion failed: ... the skipped discovery cluster must be named`),
/// with `instances == ["east", "local"]` still holding: the union survives, only
/// the partial-coverage signal is lost. That is the non-vacuous proof the
/// warning names the skipped cluster because federation put it there.
#[tokio::test]
async fn federated_series_discovery_returns_union_and_warns_on_skip() {
    let base = now_ns() - 10 * NS_PER_MIN;
    let acme = TenantId::new("acme");

    let local_store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    publish_series(local_store.as_ref(), &acme, base, "local", 1.0, 1).await;

    // Healthy remote "east" over its own store (same canonical tenant, disjoint
    // series identity).
    let east_store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    publish_series(east_store.as_ref(), &acme, base, "east", 2.0, 1).await;
    let east = spawn_remote(east_store, OPERATOR_CRED, &acme).await;

    // A dead cluster "west", skip_unavailable=true.
    let west_endpoint = dead_endpoint().await;

    let clusters = vec![
        make_remote_cluster(
            "east",
            &east.endpoint,
            OPERATOR_CRED,
            true,
            Duration::from_secs(5),
        ),
        make_remote_cluster(
            "west",
            &west_endpoint,
            OPERATOR_CRED,
            true,
            Duration::from_secs(3),
        ),
    ];
    let catalog = build_catalog(local_store.clone(), 1, true, 0).expect("catalog");
    let engine = QueryEngine::new(catalog, local_store, EngineConfig::default())
        .with_federation(Arc::new(Federation::new(clusters)));

    let window = TimeRange {
        start_ns: base - NS_PER_HOUR,
        end_ns: now_ns(),
    };
    let (series, stats) = engine
        .resolve_series_with_stats(
            acme.hash(),
            &[name_matcher()],
            window,
            &[],
            now_ns(),
            DEADLINE,
        )
        .await
        .expect("skip_unavailable=true keeps discovery alive past the down cluster");

    // The healthy remote must have been dialed with ONLY the operator principal
    // (no client credential crosses the boundary), checked before the union so a
    // credential-forwarding regression fails on THIS assertion.
    let seen = east.seen_auth.lock().clone();
    assert!(!seen.is_empty(), "the healthy remote must have been dialed");
    assert!(
        seen.iter().all(|a| a == &format!("Bearer {OPERATOR_CRED}")),
        "the remote saw a non-operator principal (client credential leaked \
         across the cluster boundary): {seen:?}"
    );

    // Union: BOTH clusters' series identities appear (local + east). Before this
    // ticket discovery resolved local-only and would return just ["local"].
    assert_eq!(
        instances(&series),
        vec!["east".to_string(), "local".to_string()],
        "federated discovery must union series from both clusters"
    );

    // Partial coverage is surfaced, naming only the skipped cluster.
    assert!(
        stats.partial,
        "a skipped remote must mark discovery coverage partial"
    );
    assert!(
        stats.warnings.iter().any(|w| w.contains("west")),
        "the skipped discovery cluster must be named in warnings: {:?}",
        stats.warnings
    );
    assert!(
        !stats.warnings.iter().any(|w| w.contains("east")),
        "the healthy cluster must not be warned about: {:?}",
        stats.warnings
    );

    east.stop().await;
}

/// Deliverable 1, default half: a federated discovery request fails typed when a
/// remote is unavailable and its `skip_unavailable` is false -- the SAME
/// `skip_unavailable=false` semantics the query path enforces, now on the
/// discovery seam.
#[tokio::test]
async fn federated_series_discovery_fails_without_skip() {
    let base = now_ns() - 10 * NS_PER_MIN;
    let acme = TenantId::new("acme");
    let local_store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    publish_series(local_store.as_ref(), &acme, base, "local", 10.0, 1).await;

    // A remote provisioned then shut down so it is unreachable at query time.
    let remote_store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    publish_series(remote_store.as_ref(), &acme, base, "remote", 20.0, 1).await;
    let remote = spawn_remote(remote_store, OPERATOR_CRED, &acme).await;
    let endpoint = remote.endpoint.clone();
    remote.stop().await;

    let cluster = make_remote_cluster(
        "east",
        &endpoint,
        OPERATOR_CRED,
        false,
        Duration::from_secs(3),
    );
    let catalog = build_catalog(local_store.clone(), 1, true, 0).expect("catalog");
    let engine = QueryEngine::new(catalog, local_store, EngineConfig::default())
        .with_federation(Arc::new(Federation::new(vec![cluster])));

    let window = TimeRange {
        start_ns: base - NS_PER_HOUR,
        end_ns: now_ns(),
    };
    let err = engine
        .resolve_series_with_stats(
            acme.hash(),
            &[name_matcher()],
            window,
            &[],
            now_ns(),
            DEADLINE,
        )
        .await
        .expect_err("an unavailable remote with skip_unavailable=false must fail discovery");
    assert!(
        matches!(err, QueryError::Federation { .. }),
        "expected a typed QueryError::Federation, got {err:?}"
    );
}

/// Deliverable 4, default half: `skip_unavailable = false` fails the whole query
/// typed when a remote is unavailable. The two clusters are provisioned over
/// separate stores for different tenants; the remote is taken down before the
/// query, so it is genuinely unreachable.
#[tokio::test]
async fn remote_failure_fails_query_by_default() {
    let base = now_ns() - 10 * NS_PER_MIN;
    let acme = TenantId::new("acme");
    let local_store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    publish_series(local_store.as_ref(), &acme, base, "local", 10.0, 1).await;

    // A second cluster, independently provisioned for a DIFFERENT tenant over
    // its own store, then shut down so it is unavailable at query time.
    let beta = TenantId::new("beta");
    let remote_store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    publish_series(remote_store.as_ref(), &beta, base, "remote", 20.0, 1).await;
    let remote = spawn_remote(remote_store, OPERATOR_CRED, &beta).await;
    let endpoint = remote.endpoint.clone();
    remote.stop().await;

    let cluster = make_remote_cluster(
        "east",
        &endpoint,
        OPERATOR_CRED,
        false,
        Duration::from_secs(3),
    );
    let catalog = build_catalog(local_store.clone(), 1, true, 0).expect("catalog");
    let engine = QueryEngine::new(catalog, local_store, EngineConfig::default())
        .with_federation(Arc::new(Federation::new(vec![cluster])));

    let err = engine
        .instant_with_stats(
            acme.hash(),
            METRIC,
            (base + NS_PER_MIN) / NS_PER_MS,
            &[],
            now_ns(),
            DEADLINE,
        )
        .await
        .expect_err("an unavailable remote with skip_unavailable=false must fail the query");
    assert!(
        matches!(err, QueryError::Federation { .. }),
        "expected a typed QueryError::Federation, got {err:?}"
    );
}

/// Deliverable 4, skip half + trust boundary: `skip_unavailable = true` skips a
/// down cluster (named in `warnings`, coverage partial), the healthy remote's
/// series still merge, and that remote saw ONLY its operator principal.
#[tokio::test]
async fn federated_query_marks_skipped_cluster_in_warnings() {
    let base = now_ns() - 10 * NS_PER_MIN;
    let acme = TenantId::new("acme");

    let local_store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    publish_series(local_store.as_ref(), &acme, base, "local", 1.0, 1).await;

    // Healthy remote "east" over its own store (same canonical tenant, disjoint
    // series), recording every principal presented to it.
    let east_store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    publish_series(east_store.as_ref(), &acme, base, "east", 2.0, 1).await;
    let east = spawn_remote(east_store, OPERATOR_CRED, &acme).await;

    // A dead cluster "west", also skip_unavailable=true.
    let west_endpoint = dead_endpoint().await;

    let clusters = vec![
        make_remote_cluster(
            "east",
            &east.endpoint,
            OPERATOR_CRED,
            true,
            Duration::from_secs(5),
        ),
        make_remote_cluster(
            "west",
            &west_endpoint,
            OPERATOR_CRED,
            true,
            Duration::from_secs(3),
        ),
    ];
    let catalog = build_catalog(local_store.clone(), 1, true, 0).expect("catalog");
    let engine = QueryEngine::new(catalog, local_store, EngineConfig::default())
        .with_federation(Arc::new(Federation::new(clusters)));

    let (value, stats) = engine
        .instant_with_stats(
            acme.hash(),
            METRIC,
            (base + NS_PER_MIN) / NS_PER_MS,
            &[],
            now_ns(),
            DEADLINE,
        )
        .await
        .expect("skip_unavailable=true continues past the down cluster");

    // Trust boundary first, independent of the merge: whatever the outcome, the
    // healthy remote must have been dialed and must have seen ONLY the
    // operator-configured principal. Checked before the merge/warning
    // assertions so a credential-forwarding regression fails on THIS assertion
    // (the flipped-line proof in the task report), not indirectly via the
    // remote's own auth rejecting the wrong token.
    let seen = east.seen_auth.lock().clone();
    assert!(!seen.is_empty(), "the healthy remote must have been dialed");
    assert!(
        seen.iter().all(|a| a == &format!("Bearer {OPERATOR_CRED}")),
        "the remote saw a non-operator principal (client credential leaked \
         across the cluster boundary): {seen:?}"
    );

    assert!(
        stats.partial,
        "a skipped remote must mark the query's coverage partial"
    );
    assert!(
        stats.warnings.iter().any(|w| w.contains("west")),
        "the skipped cluster must be named in warnings: {:?}",
        stats.warnings
    );
    assert!(
        !stats.warnings.iter().any(|w| w.contains("east")),
        "the healthy cluster must not be warned about: {:?}",
        stats.warnings
    );

    let bits = vector_bits(&value);
    assert!(
        bits.iter().any(|(k, _)| k.contains("instance=east")),
        "the healthy remote's series must be merged into the result: {bits:?}"
    );

    east.stop().await;
}

/// Deliverable 2/3 correctness: both clusters healthy, the federated union
/// equals a locally-computed union of both datasets (f64 as bits), and a tight
/// `max_bytes_scanned` the local-only fetch clears trips typed on the combined
/// frames.
#[tokio::test]
async fn federated_query_merges_remote_series() {
    let base = now_ns() - 10 * NS_PER_MIN;
    let acme = TenantId::new("acme");
    let t_ms = (base + NS_PER_MIN) / NS_PER_MS;
    let now = now_ns();

    let local_store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    publish_series(local_store.as_ref(), &acme, base, "local", 10.0, 1).await;
    let remote_store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    publish_series(remote_store.as_ref(), &acme, base, "remote", 20.0, 1).await;
    let remote = spawn_remote(remote_store, OPERATOR_CRED, &acme).await;

    // Oracle: one store holding BOTH datasets, queried with no federation.
    let oracle_store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    publish_series(oracle_store.as_ref(), &acme, base, "local", 10.0, 1).await;
    publish_series(oracle_store.as_ref(), &acme, base, "remote", 20.0, 2).await;
    let oracle_engine = QueryEngine::new(
        build_catalog(oracle_store.clone(), 1, true, 0).expect("catalog"),
        oracle_store,
        EngineConfig::default(),
    );
    let (oracle_value, _) = oracle_engine
        .instant_with_stats(acme.hash(), METRIC, t_ms, &[], now, DEADLINE)
        .await
        .expect("oracle query");

    // Federated: local store + one healthy remote.
    let fed_engine = QueryEngine::new(
        build_catalog(local_store.clone(), 1, true, 0).expect("catalog"),
        local_store.clone(),
        EngineConfig::default(),
    )
    .with_federation(Arc::new(Federation::new(vec![make_remote_cluster(
        "east",
        &remote.endpoint,
        OPERATOR_CRED,
        false,
        Duration::from_secs(10),
    )])));
    let (fed_value, fed_stats) = fed_engine
        .instant_with_stats(acme.hash(), METRIC, t_ms, &[], now, DEADLINE)
        .await
        .expect("federated query");

    assert!(
        !fed_stats.partial,
        "both clusters healthy: coverage is complete"
    );
    assert!(
        fed_stats.warnings.is_empty(),
        "no warnings when every remote is healthy"
    );
    assert_eq!(
        vector_bits(&fed_value),
        vector_bits(&oracle_value),
        "the federated union must equal the locally-computed union of both datasets"
    );

    // Budget: the combined frames trip a cap the local-only fetch clears.
    let combined_bytes = fed_stats.accounting.total_s3_bytes();
    let local_engine = QueryEngine::new(
        build_catalog(local_store.clone(), 1, true, 0).expect("catalog"),
        local_store.clone(),
        EngineConfig::default(),
    );
    let (_local_value, local_stats) = local_engine
        .instant_with_stats(acme.hash(), METRIC, t_ms, &[], now, DEADLINE)
        .await
        .expect("local-only query");
    let local_bytes = local_stats.accounting.total_s3_bytes();
    assert!(
        local_bytes < combined_bytes,
        "sanity: the remote's frames add scanned bytes ({local_bytes} < {combined_bytes})"
    );

    // A cap the local path clears (cap >= local_bytes) but the combined
    // local+remote total exceeds (cap < combined_bytes).
    let cap = combined_bytes - 1;
    let capped = EngineConfig {
        max_bytes_scanned: ByteLimit::Bounded(cap),
        ..EngineConfig::default()
    };
    let local_capped = QueryEngine::new(
        build_catalog(local_store.clone(), 1, true, 0).expect("catalog"),
        local_store.clone(),
        capped,
    );
    local_capped
        .instant_with_stats(acme.hash(), METRIC, t_ms, &[], now, DEADLINE)
        .await
        .expect("the local-only fetch clears the cap");

    let fed_capped = QueryEngine::new(
        build_catalog(local_store.clone(), 1, true, 0).expect("catalog"),
        local_store.clone(),
        capped,
    )
    .with_federation(Arc::new(Federation::new(vec![make_remote_cluster(
        "east",
        &remote.endpoint,
        OPERATOR_CRED,
        false,
        Duration::from_secs(10),
    )])));
    let err = fed_capped
        .instant_with_stats(acme.hash(), METRIC, t_ms, &[], now, DEADLINE)
        .await
        .expect_err("the combined frames must trip the byte budget");
    assert!(
        matches!(err, QueryError::TooManyBytesScanned { .. }),
        "expected a typed QueryError::TooManyBytesScanned on the combined spend, got {err:?}"
    );

    remote.stop().await;
}
