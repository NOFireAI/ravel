//! A spans `trace_id` lookup whose key-index leaf fetch the process memory
//! budget refuses (issue #3944) falls back to each object's own KEY_IDX and
//! returns the rows the routed lookup returns, and its stats count the
//! refusal under `reservationsRefused`, not `leavesRejected`.

#![cfg(feature = "sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use bytes::Bytes;
use clap::Parser;
use ravel_catalog::{AbsentPolicy, Catalog, CatalogConfig, validate_or_adopt};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_query::http::StaticBearerTokenResolver;
use ravel_server::Cli;
use ravel_server::query::{build_catalog, build_sql_state};
use ravel_server::store::build_cache;
use ravel_types::{Signal, TenantId};
use serde_json::Value;
use tower::ServiceExt;
use uuid::Uuid;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_HOUR: i64 = 3_600 * NS_PER_SEC;
/// Late enough that a fold seals hour 0.
const NOW_NS: i64 = 7 * NS_PER_HOUR;
const SHARDS: u32 = 2;
/// The three hour-0 objects fold into one part.
const PART_MAX_ENTRIES: usize = 3;
const TENANT: &str = "acme";
const TOKEN: &str = "acme-token";
/// Single-span traces per object. Enough blocks that KEY_IDX and SKIP_IDX lie
/// before the 8 KiB footer suffix, so each is a GET of its own.
const DECOYS: u64 = 2_400;
const BLOCK_RECORDS: usize = 4;

struct FixedClock(i64);
impl ravel_ingest::Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        self.0
    }
}

/// A `MemoryStore` that records every GET's key and range.
struct RecordingStore {
    inner: MemoryStore,
    gets: Mutex<Vec<(String, GetRange)>>,
}

impl RecordingStore {
    fn drain(&self) -> Vec<(String, GetRange)> {
        std::mem::take(&mut *self.gets.lock().expect("get log"))
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
        self.gets
            .lock()
            .expect("get log")
            .push((key.to_string(), range));
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

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The looked-up trace. Its first byte is low, so it sorts ahead of every
/// decoy.
fn trace() -> [u8; 16] {
    let mut t = [0x40; 16];
    t[0] = 0x01;
    t
}

/// `trace` with byte 7 moved by `delta`: a key that sorts right beside it, so
/// an object holding both neighbours keeps a SKIP_IDX block covering `trace`
/// and its KEY_IDX is probed at tier 1.
fn neighbour(trace: &[u8; 16], delta: i8) -> [u8; 16] {
    let mut t = *trace;
    t[7] = t[7].wrapping_add_signed(delta);
    t
}

fn bracket(trace: &[u8; 16]) -> Vec<([u8; 16], &'static str)> {
    vec![
        (neighbour(trace, -1), "left"),
        (neighbour(trace, 1), "right"),
    ]
}

/// A uniformly spread trace id with its first byte at or above 0x80.
fn decoy(seed: u64) -> [u8; 16] {
    let mut t = [0u8; 16];
    t.copy_from_slice(&blake3::hash(&seed.to_le_bytes()).as_bytes()[..16]);
    t[0] |= 0x80;
    t
}

struct ObjectSpec {
    shard: u32,
    spans: Vec<([u8; 16], &'static str)>,
}

/// Writes and commits one hour-0 RSPAN object and returns its data key;
/// `seq` keeps writer ids and decoys distinct across objects.
async fn publish_object(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    seq: u64,
    spec: &ObjectSpec,
) -> String {
    let tenant_hash = tenant.hash();
    let writer_id = Uuid::from_u128(3_944_000 + u128::from(seq));
    let mut writer = ravel_rspan::RspanWriter::new(
        ravel_rspan::RspanConfig {
            block_target_records: BLOCK_RECORDS,
            ..ravel_rspan::RspanConfig::default()
        },
        ravel_rspan::ObjectIdentity {
            tenant_hash: tenant_hash.0,
            shard: spec.shard,
            writer_id: writer_id.into_bytes(),
            writer_epoch: 1,
            writer_seq: 1,
        },
    );
    let base = 60 * NS_PER_SEC;
    let named = spec
        .spans
        .iter()
        .map(|(trace, name)| (*trace, name.to_string()));
    let decoys = (0..DECOYS).map(|i| (decoy(seq * 1_000_000 + i), "decoy".to_string()));
    let mut count = 0u64;
    let (mut min_ts, mut max_ts) = (i64::MAX, i64::MIN);
    for (i, (trace_id, name)) in named.chain(decoys).enumerate() {
        let start = base + i as i64;
        min_ts = min_ts.min(start);
        max_ts = max_ts.max(start + 1_000);
        count += 1;
        writer.push(ravel_rspan::SpanRecord {
            trace_id,
            span_id: (i as u64 + 1).to_le_bytes(),
            parent_span_id: None,
            name,
            start_ts_ns: start,
            end_ts_ns: start + 1_000,
            status_code: ravel_rspan::StatusCode::Ok,
            status_message: None,
            attrs: vec![("service.name".to_string(), "checkout".to_string())],
        });
    }
    let bytes = writer.finish().expect("finish rspan object");
    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Spans,
        shard: spec.shard,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size: bytes.len() as u64,
        content_hash: *blake3::hash(&bytes).as_bytes(),
        sample_count: count,
        series_count: count,
        min_event_ts_ns: min_ts,
        max_event_ts_ns: max_ts,
        min_ingest_ts_ns: min_ts,
        max_ingest_ts_ns: max_ts,
        segment_format_version: u32::from(ravel_ingest::SPAN_SEGMENT_FORMAT_VERSION),
        created_unix_ns: min_ts,
        ingest_hour_bucket: 0,
    })
    .expect("valid span commit record");
    let key = keys::reconstruct_data_key(&rec).expect("data key");
    store
        .put(&key, Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put span data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish span commit");
    key
}

/// Three hour-0 objects folded into one part with key-index leaves: the
/// trace's three spans sit in the shard-1 object, and the other two bracket
/// the trace so tier 1 probes their KEY_IDX. Returns the store and the data
/// keys in publish order.
async fn folded_tenant() -> (Arc<RecordingStore>, Vec<String>) {
    let recorder = Arc::new(RecordingStore {
        inner: MemoryStore::new(),
        gets: Mutex::new(Vec::new()),
    });
    let store: Arc<dyn ObjectStoreBackend> = recorder.clone();
    let tenant = TenantId::new(TENANT);
    validate_or_adopt(
        store.as_ref(),
        &tenant.hash(),
        Signal::Spans,
        SHARDS,
        0,
        AbsentPolicy::CreateFromConfig,
    )
    .await
    .expect("generation 0");
    let t = trace();
    let specs = [
        ObjectSpec {
            shard: 0,
            spans: bracket(&t),
        },
        ObjectSpec {
            shard: 1,
            spans: vec![(t, "t-0"), (t, "t-1"), (t, "t-2")],
        },
        ObjectSpec {
            shard: 1,
            spans: bracket(&t),
        },
    ];
    let mut data_keys = Vec::new();
    for (seq, spec) in specs.iter().enumerate() {
        data_keys.push(publish_object(store.as_ref(), &tenant, seq as u64, spec).await);
    }
    let report = Catalog::new(
        store,
        CatalogConfig {
            shard_count: SHARDS,
            byte_cache_max_bytes: 0,
            build_key_index_leaves: true,
            snapshot_part_max_entries: PART_MAX_ENTRIES,
            ..ravel_server::query::server_catalog_config_base()
        },
    )
    .expect("catalog")
    .fold(
        &tenant.hash(),
        Signal::Spans,
        Uuid::from_u128(1),
        NOW_NS,
        &[],
        None,
    )
    .await
    .expect("fold");
    assert_eq!(report.entry_count, 3, "every object folds: {report:?}");
    recorder.drain();
    (recorder, data_keys)
}

/// The SQL route over the recording store, its spans fetcher charged to
/// `budget`.
fn serve(recorder: &Arc<RecordingStore>, budget: Arc<ravel_memory::MemoryBudget>) -> Router {
    let store: Arc<dyn ObjectStoreBackend> = recorder.clone();
    let cli = Cli::try_parse_from(["ravel-server"]).expect("default flags parse");
    let catalog = build_catalog(
        Arc::clone(&store),
        SHARDS,
        cli.disable_cache,
        ravel_server::config::DEFAULT_CACHE_MAX_BYTES,
        None,
        cli.catalog_resolve_concurrency,
        None,
        cli.resolve_flush_cadence()
            .expect("flush cadence resolves")
            .max_flush_delay,
    )
    .expect("catalog");
    let cache = build_cache(&cli, ravel_server::config::DEFAULT_CACHE_MAX_BYTES)
        .expect("cache enabled by default");
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    let mut sql_state = build_sql_state(
        catalog,
        store,
        Arc::new(StaticBearerTokenResolver::new(tokens)),
        Some(cache),
        ravel_query::EngineConfig::default(),
        Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
        ravel_server::query::DEFAULT_MAX_QUERY_BYTES,
        ravel_server::query::DEFAULT_MAX_TENANT_BYTES,
        false,
        Arc::new(ravel_server::metrics::QueryAccountingMetrics::new(
            std::collections::HashSet::new(),
        )),
        ravel_query::QueryAdmissionController::shared(
            ravel_query::QueryConcurrencyLimit::Unlimited,
        ),
        None,
        budget,
    )
    .expect("sql state");
    sql_state.clock = Arc::new(FixedClock(NOW_NS));
    ravel_server::sql::router(sql_state)
}

/// The trace's spans by `POST /api/v1/sql`, which must answer 200.
async fn lookup(app: &Router) -> Value {
    let sql = format!(
        "SELECT span_id, name FROM spans WHERE trace_id = '{}' ORDER BY span_id",
        hex(&trace())
    );
    let payload = serde_json::json!({
        "query": sql,
        "start": 0.0,
        "end": NOW_NS as f64 / NS_PER_SEC as f64,
    })
    .to_string();
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/sql")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
        .body(Body::from(payload))
        .expect("build request");
    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("oneshot is infallible");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let value: Value = serde_json::from_slice(&bytes).expect("body is JSON");
    assert_eq!(status, StatusCode::OK, "lookup failed: {value}");
    value
}

/// Field `field` of `stats.keyIndex`, which must be present.
fn key_index(value: &Value, field: &str) -> u64 {
    value["stats"]["keyIndex"][field]
        .as_u64()
        .unwrap_or_else(|| panic!("stats.keyIndex.{field} is a u64 in {value}"))
}

/// Field `field` of the `stats.phases` entry for phase `name`.
fn phase(value: &Value, name: &str, field: &str) -> u64 {
    value["stats"]["phases"]
        .as_array()
        .expect("phases")
        .iter()
        .find(|phase| phase["phase"] == name)
        .unwrap_or_else(|| panic!("no {name} phase in {value}"))[field]
        .as_u64()
        .unwrap_or_else(|| panic!("{name}.{field} is a u64 in {value}"))
}

fn len(range: &GetRange) -> u64 {
    match *range {
        GetRange::Range(start, end) => end - start,
        other => panic!("expected a bounded range, got {other:?}"),
    }
}

/// The GETs in `gets` on a key-index leaf object.
fn leaf_gets(gets: &[(String, GetRange)]) -> Vec<(String, GetRange)> {
    gets.iter()
        .filter(|(key, _)| key.contains("/catalog/s/idx/"))
        .cloned()
        .collect()
}

/// Bytes held on the budget before the lookup, so its first fetch
/// reservation is refused.
const FILL: u64 = 1 << 30;

/// A process budget whose ledger is full until its first refusal: the
/// evictor declines the first request it is asked about, so that reservation
/// is refused with `ExhaustionCause::Accounted`, and on the next one it
/// releases the fill, so every later reservation is admitted. Returns the
/// budget and the shortfall of each evictor call, which with no slack and the
/// ledger at `FILL` is the requested size.
fn refuse_first_reservation() -> (Arc<ravel_memory::MemoryBudget>, Arc<Mutex<Vec<u64>>>) {
    let budget = Arc::new(ravel_memory::MemoryBudget::new(FILL));
    budget.try_reserve(FILL).expect("fill the ledger");
    let asked = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&asked);
    let weak = Arc::downgrade(&budget);
    budget
        .register_evictor(
            Box::new(move |shortfall, _cause| {
                let mut seen = seen.lock().expect("evictor log");
                seen.push(shortfall);
                if seen.len() == 2 {
                    weak.upgrade().expect("budget alive").release(FILL);
                    FILL
                } else {
                    0
                }
            }),
            0,
        )
        .ok()
        .expect("first evictor");
    (budget, asked)
}

/// A refused spans leaf fetch leaves the rows unchanged and counts as a
/// refused reservation: the lookup whose first leaf range the process budget
/// refuses returns the routed lookup's three spans in the same order, reports
/// `reservationsRefused` 1 and `leavesRejected` 0, issues no leaf GET, and
/// falls back to each object's own KEY_IDX, opening all three segments where
/// the routed lookup opened one.
///
/// FLIP: mapping `SpanFetchError::FetchMemoryExhausted` to
/// `LeafReadError::Failed` in `SpanLeafRangeReader::read_range` reports
/// `reservationsRefused` 0 and `leavesRejected` 1 with the rows unchanged.
#[tokio::test]
async fn a_refused_spans_leaf_fetch_returns_the_routed_rows() {
    let (recorder, data_keys) = folded_tenant().await;

    let routed = lookup(&serve(
        &recorder,
        Arc::new(ravel_memory::MemoryBudget::unlimited()),
    ))
    .await;
    let want_rows = serde_json::json!([
        ["0100000000000000", "t-0"],
        ["0200000000000000", "t-1"],
        ["0300000000000000", "t-2"],
    ]);
    assert_eq!(routed["data"]["rows"], want_rows, "{routed}");
    assert_eq!(key_index(&routed, "partsCovered"), 1, "{routed}");
    assert_eq!(key_index(&routed, "leafGetRequests"), 2, "{routed}");
    assert_eq!(key_index(&routed, "reservationsRefused"), 0, "{routed}");
    assert_eq!(key_index(&routed, "leavesRejected"), 0, "{routed}");
    assert_eq!(phase(&routed, "probe", "segmentsOpened"), 1, "{routed}");
    let routed_gets = recorder.drain();
    let routed_leaf_gets = leaf_gets(&routed_gets);
    assert_eq!(routed_leaf_gets.len(), 2, "{routed_gets:?}");
    assert_eq!(
        routed_gets
            .iter()
            .filter(|(key, _)| data_keys.contains(key))
            .map(|(key, _)| key)
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        1,
        "the routed lookup reads the holder only: {routed_gets:?}"
    );

    let (budget, asked) = refuse_first_reservation();
    let refused = lookup(&serve(&recorder, Arc::clone(&budget))).await;
    assert_eq!(
        asked.lock().expect("evictor log").first().copied(),
        Some(len(&routed_leaf_gets[0].1)),
        "the refused reservation is the first leaf range"
    );
    assert_eq!(budget.accounted_refusals(), 1);

    assert_eq!(refused["data"], routed["data"], "rows unchanged: {refused}");
    assert_eq!(key_index(&refused, "reservationsRefused"), 1, "{refused}");
    assert_eq!(key_index(&refused, "leavesRejected"), 0, "{refused}");
    assert_eq!(key_index(&refused, "partsCovered"), 0, "{refused}");
    assert_eq!(key_index(&refused, "leafGetRequests"), 0, "{refused}");
    assert_eq!(phase(&refused, "probe", "segmentsOpened"), 3, "{refused}");
    let refused_gets = recorder.drain();
    assert_eq!(leaf_gets(&refused_gets), Vec::new(), "{refused_gets:?}");
    // Each object's two KEY_IDX reads, in place of the two leaf GETs.
    assert_eq!(phase(&refused, "keyIndex", "s3GetRequests"), 6, "{refused}");
    for key in &data_keys {
        assert!(
            refused_gets.iter().any(|(k, _)| k == key),
            "{key}: every object is read on the fallback: {refused_gets:?}"
        );
    }
}
