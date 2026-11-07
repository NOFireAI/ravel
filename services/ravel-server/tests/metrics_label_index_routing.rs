//! A PromQL range or instant query routes each selector's positive equality values
//! through the metrics `label_value` key-index leaves before any object is
//! opened, and does not read an object a value is proven absent from
//! (ADR-3694 decision 3, issue #3865).
//!
//! Every fixture tenant holds `OBJECTS` sealed metrics objects in one ingest
//! hour, folded into one catalog part by a catalog configured from the
//! server's own `--fold-key-index-leaves` and `--fold-metrics-label-index`
//! flags. The metric name is in every object; the label value `target` is in
//! exactly the `HOLDERS`. Each routed run is compared with a run over a
//! tenant folded without the metrics leaf, which routes nothing.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use bytes::Bytes;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_query::QueryPhase;
use ravel_query::http::{StaticBearerTokenResolver, router as promql_router};
use ravel_query::key_index_route::{
    KeyIndexPartOutcome, key_index_leaf_gets_total, key_index_parts_total,
};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_server::Cli;
use ravel_server::metrics::QueryAccountingMetrics;
use ravel_server::query::{build_app_state, server_catalog_config_base};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantId};
use serde_json::Value;
use tower::ServiceExt;
use uuid::Uuid;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_MIN: i64 = 60 * NS_PER_SEC;
const NS_PER_HOUR: i64 = 60 * NS_PER_MIN;
/// The sealed hour's age. A fold at wall-clock now seals every hour ending
/// at least `max_flush_lifetime + clock_skew_allowance + fold_safety_margin`
/// (1 h 20 min) before it, and this one ends at least 2 h before.
const SEALED_HOURS_AGO: i64 = 3;
const TENANT: &str = "acme";
const TOKEN: &str = "acme-token";
const METRIC: &str = "cpu_seconds";
const TARGET: &str = "target";
/// Sealed objects per tenant.
const OBJECTS: u64 = 6;
/// The objects holding `instance="target"`: K = 2.
const HOLDERS: [u64; 2] = [1, 4];
/// Pseudo-random samples per series, so the objects' data bytes keep the
/// leaf within the default 1% byte cap.
const SAMPLES: usize = 2_000;

/// The key-index router's counters are process-global: every test that runs
/// a query holds this, so one test's routing never lands in another's delta.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn now_ns() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos(),
    )
    .expect("now fits i64")
}

/// A `MemoryStore` that records the key of every GET.
struct RecordingStore {
    inner: MemoryStore,
    gets: Mutex<Vec<String>>,
}

impl RecordingStore {
    fn gets_on(&self, key: &str) -> usize {
        self.gets
            .lock()
            .expect("get log")
            .iter()
            .filter(|k| *k == key)
            .count()
    }

    fn clear(&self) {
        self.gets.lock().expect("get log").clear();
    }
}

#[async_trait::async_trait]
impl ObjectStoreBackend for RecordingStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }
    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.gets.lock().expect("get log").push(key.to_string());
        self.inner.get(key, range).await
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

/// Writes and commits one RSEG object into `hour`, its samples starting at
/// `base_ts`: one series `instance="{instance}"`, plus one
/// `instance="target", object="o{seq}"` series when `holds_target`.
async fn publish_object(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    seq: u64,
    hour: u32,
    base_ts: i64,
    instance: &str,
    holds_target: bool,
) -> String {
    let mut label_sets = vec![vec![("instance", instance.to_string())]];
    if holds_target {
        label_sets.push(vec![
            ("instance", TARGET.to_string()),
            ("object", format!("o{seq}")),
        ]);
    }
    let mut state = (seq + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    let mut inputs = Vec::new();
    for extra in label_sets {
        let mut labels = vec![Label {
            name: METRIC_NAME_LABEL.to_string(),
            value: METRIC.to_string(),
        }];
        labels.extend(extra.into_iter().map(|(name, value)| Label {
            name: name.to_string(),
            value,
        }));
        let labels = LabelSet::new(labels).expect("valid labels");
        let samples = (0..SAMPLES)
            .map(|i| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                Sample {
                    ts_ns: base_ts + i as i64 * 1_000_000,
                    value: (state >> 11) as f64 / (1u64 << 53) as f64,
                }
            })
            .collect();
        inputs.push(SeriesInput {
            series_id: SeriesId::compute(tenant, METRIC, &labels).expect("series id"),
            labels,
            samples,
        });
    }
    let tenant_hash = tenant.hash();
    let writer_id = Uuid::from_u128(3_865_000 + u128::from(seq));
    let max_ts = base_ts + SAMPLES as i64 * 1_000_000;
    let written = SegmentWriter::write(
        inputs,
        SegmentIdentity {
            tenant_hash: tenant_hash.0,
            shard: 0,
            writer_id: writer_id.to_string(),
            writer_epoch: 1,
            writer_seq: 1,
        },
        IngestBounds {
            min_ingest_ts_ns: base_ts,
            max_ingest_ts_ns: max_ts,
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
        min_ingest_ts_ns: base_ts,
        max_ingest_ts_ns: max_ts,
        segment_format_version: 1,
        created_unix_ns: base_ts,
        ingest_hour_bucket: hour,
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
    data_key
}

/// Files a pending metrics erasure request whose predicate matches no
/// fixture series.
async fn put_erasure_request(store: &dyn ObjectStoreBackend, tenant: &TenantId, now: i64) {
    use ravel_proto::commit::v1::{ErasurePredicateMatcher, ErasureRequest};
    let request_id = Uuid::from_u128(0x3865);
    let request = ErasureRequest {
        format_version: 1,
        tenant_hash: tenant.hash().0.to_vec(),
        signal: ravel_commit::signal::to_proto(Signal::Metrics) as i32,
        request_id: request_id.to_string(),
        created_unix_ns: now,
        predicate: vec![ErasurePredicateMatcher {
            key: "instance".to_string(),
            value: "erased".to_string(),
        }],
        window_start_ns: 0,
        window_end_ns: 0,
        reason: String::new(),
    };
    let key = keys::erasure_request_key(&tenant.hash(), Signal::Metrics, request_id)
        .expect("request key");
    store
        .put(
            &key,
            ravel_commit::erasure::encode_request(&request),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put erasure request");
}

/// Writes and commits one RLOG object holding one `service.name="api"` log
/// record at `ts`, in the ingest hour `ts` falls in.
async fn publish_log_object(store: &dyn ObjectStoreBackend, tenant: &TenantId, ts: i64) {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    let tenant_hash = tenant.hash();
    let writer_id = Uuid::from_u128(3_865_100);
    let mut writer = RlogWriter::new(
        RlogConfig::default(),
        ObjectIdentity {
            tenant_hash: tenant_hash.0,
            shard: 0,
            writer_id: *writer_id.as_bytes(),
            writer_epoch: 1,
            writer_seq: 1,
        },
    );
    writer
        .push(LogRecord {
            stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
            stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
            ts_ns: ts,
            observed_ts_ns: ts,
            severity_num: 9,
            severity_text: "INFO".into(),
            body: "line".into(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: Vec::new(),
        })
        .expect("push log record");
    let object = Bytes::from(writer.finish().expect("finish rlog"));
    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Logs,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size: object.len() as u64,
        content_hash: *blake3::hash(&object).as_bytes(),
        sample_count: 1,
        series_count: 1,
        min_event_ts_ns: ts,
        max_event_ts_ns: ts,
        min_ingest_ts_ns: ts,
        max_ingest_ts_ns: ts,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        created_unix_ns: ts,
        ingest_hour_bucket: u32::try_from(ts / NS_PER_HOUR).expect("hour fits u32"),
    })
    .expect("valid logs commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    store
        .put(&data_key, object, PutOptions::default())
        .await
        .expect("put log object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish log record");
}

/// The catalog the server builds from `flags`: the two key-index switches
/// parsed and validated by the server's own `Cli`.
fn catalog_from_flags(store: Arc<dyn ObjectStoreBackend>, flags: &[&str]) -> Arc<Catalog> {
    let mut argv = vec!["ravel-server"];
    argv.extend_from_slice(flags);
    let cli = Cli::parse_validated_from(argv).expect("flags parse");
    Arc::new(
        Catalog::new(
            store,
            CatalogConfig {
                shard_count: 1,
                build_key_index_leaves: cli.fold_key_index_leaves,
                metrics_label_index: cli.fold_metrics_label_index,
                ..server_catalog_config_base()
            },
        )
        .expect("catalog"),
    )
}

const LABEL_INDEX_ON: &[&str] = &["--fold-key-index-leaves", "--fold-metrics-label-index"];
const LABEL_INDEX_OFF: &[&str] = &["--fold-key-index-leaves"];

#[derive(Clone, Copy, Default)]
struct Setup {
    label_index: bool,
    /// Also publish one object into the current, unsealed hour.
    recent: bool,
    /// The recent object also holds `instance="target"`.
    recent_holds_target: bool,
    /// File a pending metrics erasure request after the fold.
    pending_erasure: bool,
}

struct Fixture {
    recorder: Arc<RecordingStore>,
    flags: &'static [&'static str],
    app: Router,
    /// The sealed objects' data keys, by `seq`.
    sealed: Vec<String>,
    recent: Option<String>,
    start_s: i64,
    end_s: i64,
}

/// The PromQL router a server with `flags` and `config` builds over `store`,
/// with a catalog and caches of its own.
fn app_over(
    store: Arc<dyn ObjectStoreBackend>,
    flags: &[&str],
    config: ravel_query::EngineConfig,
) -> Router {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    let app_state = build_app_state(
        catalog_from_flags(Arc::clone(&store), flags),
        store,
        Arc::new(StaticBearerTokenResolver::new(tokens)),
        None,
        config,
        Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
        Arc::new(QueryAccountingMetrics::new(HashSet::new())),
        ravel_query::QueryAdmissionController::shared(
            ravel_query::QueryConcurrencyLimit::Unlimited,
        ),
        None,
        None,
        None,
        Arc::new(ravel_memory::MemoryBudget::unlimited()),
        ravel_server::cpu_gates::CpuGates::new(Default::default()).read,
    );
    promql_router(app_state)
}

async fn serve(setup: Setup) -> Fixture {
    let recorder = Arc::new(RecordingStore {
        inner: MemoryStore::new(),
        gets: Mutex::new(Vec::new()),
    });
    let store: Arc<dyn ObjectStoreBackend> = recorder.clone();
    let tenant = TenantId::new(TENANT);
    let now = now_ns();
    let hour = u32::try_from(now / NS_PER_HOUR - SEALED_HOURS_AGO).expect("hour fits u32");
    let hour_start = i64::from(hour) * NS_PER_HOUR;

    let mut sealed = Vec::new();
    for seq in 0..OBJECTS {
        let base_ts = hour_start + NS_PER_MIN + seq as i64 * 5 * NS_PER_MIN;
        sealed.push(
            publish_object(
                store.as_ref(),
                &tenant,
                seq,
                hour,
                base_ts,
                &format!("obj{seq}"),
                HOLDERS.contains(&seq),
            )
            .await,
        );
    }
    let flags = if setup.label_index {
        LABEL_INDEX_ON
    } else {
        LABEL_INDEX_OFF
    };
    let report = catalog_from_flags(Arc::clone(&store), flags)
        .fold(
            &tenant.hash(),
            Signal::Metrics,
            Uuid::new_v4(),
            now,
            &[],
            None,
        )
        .await
        .expect("fold");
    assert_eq!(
        report.entry_count, OBJECTS,
        "every object folds: {report:?}"
    );
    assert_eq!(report.metrics_label_index_parts_over_cap, 0, "{report:?}");
    assert_eq!(
        report.key_index_leaves_written > 0,
        setup.label_index,
        "the metrics leaf is built exactly when the switch is on: {report:?}"
    );

    let recent = if setup.recent {
        let recent_hour = u32::try_from(now / NS_PER_HOUR).expect("hour fits u32");
        Some(
            publish_object(
                store.as_ref(),
                &tenant,
                OBJECTS,
                recent_hour,
                // Inside the current hour, so the commit record's created
                // time is never in an hour before its ingest hour.
                (now - 2 * NS_PER_MIN).max(i64::from(recent_hour) * NS_PER_HOUR),
                "recent",
                setup.recent_holds_target,
            )
            .await,
        )
    } else {
        None
    };
    if setup.pending_erasure {
        put_erasure_request(store.as_ref(), &tenant, now).await;
    }

    let app = app_over(store, flags, ravel_query::EngineConfig::default());
    recorder.clear();
    Fixture {
        recorder,
        flags,
        app,
        sealed,
        recent,
        start_s: hour_start / NS_PER_SEC,
        end_s: now / NS_PER_SEC,
    }
}

/// Sends a GET for `uri` to `app`: the status and the JSON body.
async fn get(app: &Router, uri: String) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("GET")
        .uri(uri)
        .header("authorization", format!("Bearer {TOKEN}"))
        .body(Body::empty())
        .expect("build request");
    let response = app.clone().oneshot(request).await.expect("oneshot");
    let code = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let value: Value = serde_json::from_slice(&bytes).expect("body is JSON");
    (code, value)
}

impl Fixture {
    fn range_uri(&self, query: &str) -> String {
        format!(
            "/api/v1/query_range?query={}&start={}&end={}&step=60",
            urlencode(query),
            self.start_s,
            self.end_s
        )
    }

    async fn query_range(&self, query: &str) -> Value {
        let (code, value) = get(&self.app, self.range_uri(query)).await;
        assert_eq!(code, StatusCode::OK, "query_range failed: {value}");
        value
    }

    /// The range query over this fixture's store, served by a fresh server
    /// built with `config`.
    async fn query_range_under(
        &self,
        query: &str,
        config: ravel_query::EngineConfig,
    ) -> (StatusCode, Value) {
        let store: Arc<dyn ObjectStoreBackend> = self.recorder.clone();
        let app = app_over(store, self.flags, config);
        get(&app, self.range_uri(query)).await
    }

    /// The range query's `QueryStats`, from a `QueryEngine` over this
    /// fixture's store with a catalog of its own: the HTTP body does not
    /// render `estimate_excludes_spend`.
    async fn range_stats(&self, query: &str) -> ravel_query::QueryStats {
        let store: Arc<dyn ObjectStoreBackend> = self.recorder.clone();
        let engine = ravel_query::QueryEngine::new(
            catalog_from_flags(Arc::clone(&store), self.flags),
            store,
            ravel_query::EngineConfig::default(),
        );
        let (_, stats) = engine
            .range_with_stats(
                TenantId::new(TENANT).hash(),
                query,
                self.start_s * 1_000,
                self.end_s * 1_000,
                60_000,
                &[],
                now_ns(),
                Duration::from_secs(60),
            )
            .await
            .expect("range query");
        stats
    }

    /// An instant query at `time_s`.
    async fn query(&self, query: &str, time_s: i64) -> Value {
        let uri = format!("/api/v1/query?query={}&time={time_s}", urlencode(query));
        let (code, value) = get(&self.app, uri).await;
        assert_eq!(code, StatusCode::OK, "query failed: {value}");
        value
    }

    /// GETs issued on each sealed object's data key, by `seq`.
    fn sealed_gets(&self) -> Vec<usize> {
        self.sealed
            .iter()
            .map(|key| self.recorder.gets_on(key))
            .collect()
    }
}

fn urlencode(raw: &str) -> String {
    raw.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            other => format!("%{other:02X}"),
        })
        .collect()
}

fn stat(value: &Value, field: &str) -> u64 {
    value["data"]["stats"][field]
        .as_u64()
        .unwrap_or_else(|| panic!("data.stats.{field} is a u64 in {value}"))
}

/// Field `field` of the `data.stats.phases` entry for phase `name`.
fn phase(value: &Value, name: &str, field: &str) -> u64 {
    value["data"]["stats"]["phases"]
        .as_array()
        .expect("phases")
        .iter()
        .find(|phase| phase["phase"] == name)
        .unwrap_or_else(|| panic!("no {name} phase in {value}"))[field]
        .as_u64()
        .unwrap_or_else(|| panic!("{name}.{field} is a u64 in {value}"))
}

/// Every figure of the keyIndex phase.
fn key_index_figures(value: &Value) -> [u64; 3] {
    [
        phase(value, "keyIndex", "s3GetRequests"),
        phase(value, "keyIndex", "s3GetWireBytes"),
        phase(value, "keyIndex", "cacheServedBytes"),
    ]
}

type Matrix = Vec<(BTreeMap<String, String>, Vec<(u64, u64)>)>;

/// The matrix result, each point as the bit patterns of its timestamp and
/// value, series sorted by labels.
fn matrix(value: &Value) -> Matrix {
    assert_eq!(value["data"]["resultType"], "matrix", "{value}");
    let mut series: Matrix = value["data"]["result"]
        .as_array()
        .expect("result array")
        .iter()
        .map(|s| {
            let labels = s["metric"]
                .as_object()
                .expect("metric object")
                .iter()
                .map(|(k, v)| (k.clone(), v.as_str().expect("label value").to_string()))
                .collect();
            let points = s["values"]
                .as_array()
                .expect("values")
                .iter()
                .map(|p| {
                    let ts = p[0].as_f64().expect("timestamp");
                    let v: f64 = p[1].as_str().expect("value string").parse().expect("f64");
                    (ts.to_bits(), v.to_bits())
                })
                .collect();
            (labels, points)
        })
        .collect();
    series.sort();
    series
}

type Vector = Vec<(BTreeMap<String, String>, (u64, u64))>;

/// The vector result, each sample as the bit patterns of its timestamp and
/// value, series sorted by labels.
fn vector(value: &Value) -> Vector {
    assert_eq!(value["data"]["resultType"], "vector", "{value}");
    let mut series: Vector = value["data"]["result"]
        .as_array()
        .expect("result array")
        .iter()
        .map(|s| {
            let labels = s["metric"]
                .as_object()
                .expect("metric object")
                .iter()
                .map(|(k, v)| (k.clone(), v.as_str().expect("label value").to_string()))
                .collect();
            let ts = s["value"][0].as_f64().expect("timestamp");
            let v: f64 = s["value"][1]
                .as_str()
                .expect("value string")
                .parse()
                .expect("f64");
            (labels, (ts.to_bits(), v.to_bits()))
        })
        .collect();
    series.sort();
    series
}

/// The query's pooled store requests and bytes, every operation kind.
fn store_totals(value: &Value) -> (u64, u64) {
    let accounting = &value["data"]["stats"]["accounting"];
    let field = |name: &str| {
        accounting[name]
            .as_u64()
            .unwrap_or_else(|| panic!("accounting.{name} is a u64 in {value}"))
    };
    (
        field("s3GetRequests") + field("s3ListRequests") + field("s3HeadRequests"),
        field("s3GetBytes") + field("s3ListBytes") + field("s3HeadBytes"),
    )
}

/// The metrics rows of `ravel_query_key_index_parts_total`, by outcome, and
/// `ravel_query_key_index_leaf_gets_total{signal="metrics"}`.
fn metrics_route_totals() -> (Vec<u64>, u64) {
    (
        KeyIndexPartOutcome::ALL
            .iter()
            .map(|outcome| key_index_parts_total(Signal::Metrics, *outcome))
            .collect(),
        key_index_leaf_gets_total(Signal::Metrics),
    )
}

/// ADR-3694 decision 3, the acceptance test: with both fold switches on, a
/// range query for `instance="target"` reads only the K objects holding it.
/// `segmentsPrunedByLabelIndex` is the non-holder count, every non-holder's
/// data GETs drop to zero while each holder's stay as they were, the leaf
/// GETs are charged to the keyIndex phase, and the result is bit-identical
/// to the run over a tenant folded without the metrics leaf. A recent holder,
/// which no leaf covers, is read and its series answered.
#[tokio::test]
async fn promql_query_range_prunes_non_holders_by_label_index() {
    let _serial = SERIAL.lock().await;
    let query = format!("{METRIC}{{instance=\"{TARGET}\"}}");
    let with_recent_holder = Setup {
        recent: true,
        recent_holds_target: true,
        ..Setup::default()
    };
    let off = serve(with_recent_holder).await;
    let unrouted = off.query_range(&query).await;
    let on = serve(Setup {
        label_index: true,
        ..with_recent_holder
    })
    .await;
    let (_, leaf_gets_before) = metrics_route_totals();
    let routed = on.query_range(&query).await;
    let (_, leaf_gets_after) = metrics_route_totals();

    let non_holders = OBJECTS - HOLDERS.len() as u64;
    assert_eq!(non_holders, 4);
    assert_eq!(stat(&routed, "segmentsPrunedByLabelIndex"), non_holders);
    assert_eq!(stat(&unrouted, "segmentsPrunedByLabelIndex"), 0);
    assert_eq!(
        stat(&routed, "segmentsFetched"),
        stat(&unrouted, "segmentsFetched") - non_holders,
        "a pruned object is not counted as fetched"
    );

    let result = matrix(&routed);
    assert_eq!(
        result.len(),
        HOLDERS.len() + 1,
        "one series per sealed holder and one for the recent holder"
    );
    assert!(result.iter().all(|(_, points)| !points.is_empty()));
    assert_eq!(result, matrix(&unrouted), "routing changes no answer");
    let recent = |fixture: &Fixture| {
        let key = fixture.recent.as_deref().expect("a recent object");
        fixture.recorder.gets_on(key)
    };
    assert!(recent(&off) >= 1);
    assert_eq!(
        recent(&on),
        recent(&off),
        "the recent holder is read as before"
    );

    let off_gets = off.sealed_gets();
    let on_gets = on.sealed_gets();
    for seq in 0..OBJECTS as usize {
        assert!(off_gets[seq] >= 1, "the unrouted run reads object {seq}");
        if HOLDERS.contains(&(seq as u64)) {
            assert_eq!(
                on_gets[seq], off_gets[seq],
                "holder {seq} is read as before"
            );
        } else {
            assert_eq!(on_gets[seq], 0, "non-holder {seq} is never opened");
        }
    }
    let pruned_gets: usize = (0..OBJECTS as usize)
        .filter(|seq| !HOLDERS.contains(&(*seq as u64)))
        .map(|seq| off_gets[seq])
        .sum();
    assert_eq!(
        on_gets.iter().sum::<usize>(),
        off_gets.iter().sum::<usize>() - pruned_gets,
        "data GETs fell by exactly the pruned objects' GETs"
    );

    let [gets, wire, _] = key_index_figures(&routed);
    assert!(gets >= 1, "the leaf reads are charged to keyIndex");
    assert!(wire > 0);
    assert_eq!(
        leaf_gets_after - leaf_gets_before,
        gets,
        "the keyIndex phase carries every metrics leaf GET"
    );
}

/// Over a tenant folded without `--fold-metrics-label-index` no part has a
/// metrics leaf, so the route does not run: nothing is pruned, the keyIndex
/// phase reports zero, and no metrics row of
/// `ravel_query_key_index_parts_total` or the leaf GET total moves.
#[tokio::test]
async fn without_the_switch_the_route_does_not_run() {
    let _serial = SERIAL.lock().await;
    let off = serve(Setup::default()).await;
    let before = metrics_route_totals();
    let value = off
        .query_range(&format!("{METRIC}{{instance=\"{TARGET}\"}}"))
        .await;
    assert_eq!(metrics_route_totals(), before);
    assert_eq!(stat(&value, "segmentsPrunedByLabelIndex"), 0);
    assert_eq!(key_index_figures(&value), [0, 0, 0]);
    assert_eq!(matrix(&value).len(), HOLDERS.len());
    assert!(off.sealed_gets().iter().all(|gets| *gets >= 1));
}

/// While the tenant has a pending metrics erasure request the query routes
/// nothing, even over parts that already carry a metrics leaf: every object
/// is read and the answer equals the unrouted run.
#[tokio::test]
async fn a_pending_erasure_routes_nothing() {
    let _serial = SERIAL.lock().await;
    let query = format!("{METRIC}{{instance=\"{TARGET}\"}}");
    let off = serve(Setup::default()).await;
    let unrouted = off.query_range(&query).await;
    let gated = serve(Setup {
        label_index: true,
        pending_erasure: true,
        ..Setup::default()
    })
    .await;
    let value = gated.query_range(&query).await;
    assert_eq!(stat(&value, "segmentsPrunedByLabelIndex"), 0);
    assert_eq!(key_index_figures(&value), [0, 0, 0]);
    assert!(gated.sealed_gets().iter().all(|gets| *gets >= 1));
    assert_eq!(matrix(&value), matrix(&unrouted));
}

/// A query of two selectors with disjoint equality values routes each over
/// the union window: an object is dropped only when neither selector reads
/// it, and the answer equals the unrouted run.
#[tokio::test]
async fn a_compound_query_with_disjoint_selectors_equals_the_unrouted_run() {
    let _serial = SERIAL.lock().await;
    let query = format!(
        "sum(count_over_time({METRIC}{{instance=\"{TARGET}\"}}[1h])) \
         + sum(count_over_time({METRIC}{{instance=\"obj2\"}}[1h]))"
    );
    let off = serve(Setup::default()).await;
    let unrouted = off.query_range(&query).await;
    let on = serve(Setup {
        label_index: true,
        ..Setup::default()
    })
    .await;
    let routed = on.query_range(&query).await;
    let read: Vec<u64> = vec![1, 2, 4];
    assert_eq!(
        stat(&routed, "segmentsPrunedByLabelIndex"),
        OBJECTS - read.len() as u64
    );
    let on_gets = on.sealed_gets();
    for seq in 0..OBJECTS {
        assert_eq!(
            on_gets[seq as usize] >= 1,
            read.contains(&seq),
            "object {seq} is read exactly when a selector's value is in it"
        );
    }
    let result = matrix(&routed);
    assert_eq!(result.len(), 1);
    assert!(!result[0].1.is_empty());
    assert_eq!(result, matrix(&unrouted));
}

/// An object in the current, unsealed hour has no part and so no leaf: it is
/// always read, and not counted as pruned, though it lacks the value.
#[tokio::test]
async fn an_unfolded_recent_object_is_never_pruned() {
    let _serial = SERIAL.lock().await;
    let fixture = serve(Setup {
        label_index: true,
        recent: true,
        ..Setup::default()
    })
    .await;
    let value = fixture
        .query_range(&format!("{METRIC}{{instance=\"{TARGET}\"}}"))
        .await;
    assert_eq!(
        stat(&value, "segmentsPrunedByLabelIndex"),
        OBJECTS - HOLDERS.len() as u64
    );
    let recent = fixture.recent.as_deref().expect("a recent object");
    assert!(
        fixture.recorder.gets_on(recent) >= 1,
        "the recent object is read"
    );
    assert_eq!(matrix(&value).len(), HOLDERS.len());
}

/// A selector with no positive equality matcher routes nothing: no leaf is
/// read and no object is pruned.
#[tokio::test]
async fn a_regex_only_selector_routes_nothing() {
    let _serial = SERIAL.lock().await;
    let query = format!("{{__name__=~\"{METRIC}\", instance=~\"{TARGET}\"}}");
    let off = serve(Setup::default()).await;
    let unrouted = off.query_range(&query).await;
    let on = serve(Setup {
        label_index: true,
        ..Setup::default()
    })
    .await;
    let (before, leaf_gets_before) = metrics_route_totals();
    let value = on.query_range(&query).await;
    assert_eq!(metrics_route_totals(), (before, leaf_gets_before));
    assert_eq!(stat(&value, "segmentsPrunedByLabelIndex"), 0);
    assert_eq!(key_index_figures(&value), [0, 0, 0]);
    assert!(on.sealed_gets().iter().all(|gets| *gets >= 1));
    assert_eq!(matrix(&value), matrix(&unrouted));
}

/// The routed query moves the metrics rows of
/// `ravel_query_key_index_parts_total`: the one part is settled as covered
/// once per distinct equality value (`cpu_seconds` and `target`), and the
/// leaf GET total moves by the keyIndex phase's GETs.
#[tokio::test]
async fn the_metrics_rows_of_key_index_parts_total_move() {
    let _serial = SERIAL.lock().await;
    let on = serve(Setup {
        label_index: true,
        ..Setup::default()
    })
    .await;
    let (parts_before, leaf_gets_before) = metrics_route_totals();
    let value = on
        .query_range(&format!("{METRIC}{{instance=\"{TARGET}\"}}"))
        .await;
    let (parts_after, leaf_gets_after) = metrics_route_totals();
    let moved: Vec<(KeyIndexPartOutcome, u64)> = KeyIndexPartOutcome::ALL
        .iter()
        .zip(parts_before.iter().zip(&parts_after))
        .map(|(outcome, (before, after))| (*outcome, after - before))
        .filter(|(_, delta)| *delta > 0)
        .collect();
    assert_eq!(moved, vec![(KeyIndexPartOutcome::Covered, 2)]);
    assert_eq!(
        leaf_gets_after - leaf_gets_before,
        phase(&value, "keyIndex", "s3GetRequests")
    );
}

/// An instant query routes as a range query does: the non-holders are
/// pruned and never opened, and the answer equals the run over a tenant
/// folded without the metrics leaf.
#[tokio::test]
async fn an_instant_query_prunes_non_holders_and_equals_the_unrouted_run() {
    let _serial = SERIAL.lock().await;
    let query = format!("count_over_time({METRIC}{{instance=\"{TARGET}\"}}[4h])");
    let off = serve(Setup::default()).await;
    // Both runs evaluate at one time, so their sample timestamps compare.
    let unrouted = off.query(&query, off.end_s).await;
    let on = serve(Setup {
        label_index: true,
        ..Setup::default()
    })
    .await;
    let routed = on.query(&query, off.end_s).await;
    let non_holders = OBJECTS - HOLDERS.len() as u64;
    assert_eq!(stat(&routed, "segmentsPrunedByLabelIndex"), non_holders);
    assert_eq!(stat(&unrouted, "segmentsPrunedByLabelIndex"), 0);
    let on_gets = on.sealed_gets();
    for seq in 0..OBJECTS {
        assert_eq!(
            on_gets[seq as usize] >= 1,
            HOLDERS.contains(&seq),
            "object {seq} is read exactly when it holds the value"
        );
    }
    let result = vector(&routed);
    assert_eq!(result.len(), HOLDERS.len());
    assert_eq!(result, vector(&unrouted), "routing changes no answer");
}

/// The route's leaf GETs count against `max_s3_requests` and
/// `max_bytes_scanned` even when they prune every object, so no fetch batch
/// runs: a budget one short of the query's spend refuses with the budget
/// error, and a budget equal to it answers.
#[tokio::test]
async fn a_route_that_prunes_every_object_is_held_to_the_budgets() {
    let _serial = SERIAL.lock().await;
    let on = serve(Setup {
        label_index: true,
        ..Setup::default()
    })
    .await;
    let query = format!("{METRIC}{{instance=\"absent\"}}");
    let unbounded = on.query_range(&query).await;
    assert_eq!(stat(&unbounded, "segmentsPrunedByLabelIndex"), OBJECTS);
    assert_eq!(stat(&unbounded, "segmentsFetched"), 0);
    assert!(on.sealed_gets().iter().all(|gets| *gets == 0));
    let [leaf_gets, leaf_wire, _] = key_index_figures(&unbounded);
    assert!(leaf_gets >= 1 && leaf_wire >= 1);
    let (requests, bytes) = store_totals(&unbounded);

    let refused = |(code, value): (StatusCode, Value), expected: String| {
        assert_eq!(code, StatusCode::UNPROCESSABLE_ENTITY, "{value}");
        let message = value["error"].as_str().expect("error message");
        assert!(message.contains(&expected), "{message}");
    };
    let under = |requests: u64, bytes: u64| ravel_query::EngineConfig {
        max_s3_requests: ravel_query::RequestLimit::Bounded(requests),
        max_bytes_scanned: ravel_query::ByteLimit::Bounded(bytes),
        ..ravel_query::EngineConfig::default()
    };
    refused(
        on.query_range_under(&query, under(requests - 1, bytes))
            .await,
        format!(
            "query issued {requests} S3 requests, exceeding the budget of {}",
            requests - 1
        ),
    );
    refused(
        on.query_range_under(&query, under(requests, bytes - 1))
            .await,
        format!(
            "query scanned {bytes} bytes, exceeding the budget of {}",
            bytes - 1
        ),
    );
    let (code, value) = on.query_range_under(&query, under(requests, bytes)).await;
    assert_eq!(code, StatusCode::OK, "{value}");
    assert_eq!(store_totals(&value), (requests, bytes));
    assert!(matrix(&value).is_empty());
}

/// Equality matchers on labels no fixture series carries, added to the
/// estimate test's selector: each value is its own route pass and pays its
/// own leaf GETs, enough of them to exceed the estimate's request term. The
/// label names are short so the selector stays under the PromQL complexity
/// limit, which counts the characters outside string literals.
const ABSENT_VALUES: usize = 64;

/// `stats.estimate` is computed before the label-value route runs and has no
/// term for its leaf GETs, so a query whose route issued one says its
/// estimate excludes spend (ADR-0044's escape). Here the leaf GETs alone
/// take the query's requests above the estimate. A query the route does not
/// run on, over a tenant without the metrics leaf or with no positive
/// equality, keeps the flag false and its estimate covering its spend.
#[tokio::test]
async fn a_route_that_issued_a_leaf_get_marks_the_estimate_as_excluding_spend() {
    let _serial = SERIAL.lock().await;
    let absent: String = (0..ABSENT_VALUES)
        .map(|i| format!(",a{i}=\"absent{i}\""))
        .collect();
    let routed_query = format!("{METRIC}{{instance=\"{TARGET}\"{absent}}}");
    let on = serve(Setup {
        label_index: true,
        ..Setup::default()
    })
    .await;
    let routed = on.range_stats(&routed_query).await;
    let leaf_gets = routed
        .phase_accounting
        .phase(QueryPhase::KeyIndex)
        .total_s3_requests();
    assert!(
        leaf_gets >= ABSENT_VALUES as u64,
        "every absent value pays its own leaf GET: {leaf_gets}"
    );
    assert_eq!(routed.segments_pruned_by_label_index, OBJECTS);
    assert!(
        routed.accounting.total_s3_requests() > routed.estimate.estimated_requests,
        "the leaf GETs take the query above its estimate: actual {:?}, estimate {:?}",
        routed.accounting,
        routed.estimate
    );
    assert!(routed.estimate_excludes_spend);

    let off = serve(Setup::default()).await;
    let regex_only = format!("{{__name__=~\"{METRIC}\", instance=~\"{TARGET}\"}}");
    for (fixture, query) in [(&off, &routed_query), (&on, &regex_only)] {
        let stats = fixture.range_stats(query).await;
        assert_eq!(
            stats
                .phase_accounting
                .phase(QueryPhase::KeyIndex)
                .total_s3_requests(),
            0,
            "{query}: the route does not run"
        );
        assert!(!stats.estimate_excludes_spend, "{query}");
        assert!(
            stats.accounting.total_s3_requests() <= stats.estimate.estimated_requests,
            "{query}: actual {:?}, estimate {:?}",
            stats.accounting,
            stats.estimate
        );
    }
}

/// ADR-1103's cumulative segment check counts the metrics segments resolved
/// before the label-value route, not the ones left to fetch after it: a
/// query of a metrics selector and a log selector over `OBJECTS` sealed
/// metrics objects and one log object is refused under a `max_segments` of
/// `OBJECTS` whether or not the route prunes, and answers under one more.
#[tokio::test]
async fn the_cumulative_segment_check_counts_segments_resolved_before_the_route() {
    let _serial = SERIAL.lock().await;
    let query = format!(
        "count_over_time({METRIC}{{instance=\"{TARGET}\"}}[1h]) \
         or count_over_time(ravel_log_lines{{job=\"api\"}}[1h])"
    );
    let capped = |max_segments: usize| ravel_query::EngineConfig {
        max_segments,
        ..ravel_query::EngineConfig::default()
    };
    let tenant = TenantId::new(TENANT);
    let off = serve(Setup::default()).await;
    let on = serve(Setup {
        label_index: true,
        ..Setup::default()
    })
    .await;
    for fixture in [&off, &on] {
        publish_log_object(fixture.recorder.as_ref(), &tenant, now_ns() - NS_PER_MIN).await;
        let (code, value) = fixture
            .query_range_under(&query, capped(OBJECTS as usize))
            .await;
        assert_eq!(code, StatusCode::UNPROCESSABLE_ENTITY, "{value}");
        let message = value["error"].as_str().expect("error message");
        let expected = format!(
            "query matched {} segments, exceeding the limit of {OBJECTS}",
            OBJECTS + 1
        );
        assert!(message.contains(&expected), "{message}");
    }
    let (code, value) = on
        .query_range_under(&query, capped(OBJECTS as usize + 1))
        .await;
    assert_eq!(code, StatusCode::OK, "{value}");
    assert_eq!(
        stat(&value, "segmentsPrunedByLabelIndex"),
        OBJECTS - HOLDERS.len() as u64,
        "the route pruned the non-holders the check still counts"
    );
}
